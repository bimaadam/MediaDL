use std::sync::Arc;
use std::time::Duration;

use tracing::Instrument;
use uuid::Uuid;

use crate::domain::{
    Actor, Artifact, ArtifactStatus, CachedArtifact, DomainError, Job, MediaIdentity,
    ProcessingProfile, RetryPolicy, identify_url,
};

use super::error::AppError;
use super::model::*;
use super::ports::*;

/// Application service shared by HTTP and Telegram.
pub struct Engine {
    store: Arc<dyn Store>,
    cache: Arc<dyn Cache>,
    locks: Arc<dyn Locks>,
    guard: Arc<dyn UrlGuard>,
    platforms: Arc<dyn PlatformRegistry>,
    metadata: Arc<dyn MetadataSource>,
    admission: Arc<dyn Admission>,
    storage: Arc<dyn ObjectStorage>,
    ids: Arc<dyn Ids>,
    clock: Arc<dyn Clock>,
    config: Arc<RuntimeConfig>,
}

impl Engine {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        store: Arc<dyn Store>,
        cache: Arc<dyn Cache>,
        locks: Arc<dyn Locks>,
        guard: Arc<dyn UrlGuard>,
        platforms: Arc<dyn PlatformRegistry>,
        metadata: Arc<dyn MetadataSource>,
        admission: Arc<dyn Admission>,
        storage: Arc<dyn ObjectStorage>,
        ids: Arc<dyn Ids>,
        clock: Arc<dyn Clock>,
        config: Arc<RuntimeConfig>,
    ) -> Self {
        Self {
            store,
            cache,
            locks,
            guard,
            platforms,
            metadata,
            admission,
            storage,
            ids,
            clock,
            config,
        }
    }

    pub fn config(&self) -> &RuntimeConfig {
        &self.config
    }

    /// A public Facebook photo post is fetched as a photo. Everything else starts as video.
    pub async fn preferred_profile(&self, raw_url: &str) -> Result<ProcessingProfile, AppError> {
        let url = self.guard.validate(raw_url).await?;
        let identified = self.platforms.identify(&url).await?;
        if self.metadata.has_public_photos(&identified).await? {
            return Ok(ProcessingProfile::best_image());
        }
        Ok(ProcessingProfile::best_video())
    }

    pub async fn resolve(&self, actor: &Actor, raw_url: &str) -> Result<ResolveView, AppError> {
        self.admit(actor).await?;
        let url = self.guard.validate(raw_url).await?;
        let identified = self.platforms.identify(&url).await?;
        let profile = ProcessingProfile::best_video();
        if let Some(cached) =
            self.cache.get_metadata(identified.platform, identified.content_id.as_str()).await?
        {
            metrics::counter!("media_engine_cache_lookups_total", "result" => "hit", "kind" => "metadata")
                .increment(1);
            return Ok(ResolveView {
                platform: identified.platform,
                content_id: identified.content_id.as_str().to_owned(),
                canonical_url: identified.canonical_url,
                metadata: cached,
            });
        }
        metrics::counter!("media_engine_cache_lookups_total", "result" => "miss", "kind" => "metadata")
            .increment(1);
        let metadata = self.metadata.inspect(&identified, &profile).await?;
        self.cache
            .put_metadata(
                identified.platform,
                identified.content_id.as_str(),
                &metadata,
                self.config.metadata_cache_ttl,
            )
            .await?;
        Ok(ResolveView {
            platform: identified.platform,
            content_id: identified.content_id.as_str().to_owned(),
            canonical_url: identified.canonical_url,
            metadata,
        })
    }

    pub async fn submit(&self, command: SubmitCommand) -> Result<SubmitOutcome, AppError> {
        command.profile.validate()?;
        self.admit(&command.actor).await?;
        let url = self.guard.validate(&command.url).await?;
        let identified = self.platforms.identify(&url).await?;
        let identity = MediaIdentity::derive(
            identified.platform,
            identified.content_id.clone(),
            &command.profile,
        )?;
        let span = tracing::info_span!(
            "submit_media",
            platform = %identity.platform(),
            content_id = %identity.content_id(),
            profile = %identity.profile_hash(),
        );
        self.submit_identified(command, identified.canonical_url, identity).instrument(span).await
    }

    async fn submit_identified(
        &self,
        command: SubmitCommand,
        canonical_url: String,
        identity: MediaIdentity,
    ) -> Result<SubmitOutcome, AppError> {
        if let Some(bot_id) = command.channel.bot_id()
            && let Some(file) = self.cache.get_telegram_file(bot_id, &identity).await?
        {
            metrics::counter!("media_engine_telegram_file_id_total", "result" => "hit")
                .increment(1);
            return Ok(SubmitOutcome::TelegramCached { file, identity });
        }

        if let Some(outcome) = self.ready_from_cache(&identity, &command).await? {
            return Ok(outcome);
        }
        if let Some(outcome) = self.ready_from_store(&identity, &command).await? {
            return Ok(outcome);
        }
        if let Some(job) = self.store.find_active_job(&identity).await? {
            self.store.add_subscription(job.id, &command.actor, &command.channel).await?;
            return Ok(SubmitOutcome::Queued {
                job_id: job.id,
                media_id: job.media_id,
                identity,
                attached: true,
            });
        }

        let owner = self.ids.new_id().to_string();
        let acquired =
            self.locks.try_acquire(&identity.lock_key(), &owner, self.config.lock_ttl).await?;
        if !acquired {
            return self.attach_after_contention(&identity, &command, &canonical_url).await;
        }
        let created = self.create_or_attach(&identity, &command, &canonical_url).await;
        if let Err(error) = self.locks.release(&identity.lock_key(), &owner).await {
            tracing::warn!(%error, "media lock release failed; the lease will expire");
        }
        created
    }

    async fn ready_from_cache(
        &self,
        identity: &MediaIdentity,
        command: &SubmitCommand,
    ) -> Result<Option<SubmitOutcome>, AppError> {
        let Some(cached) = self.cache.get_artifact(identity).await? else {
            metrics::counter!("media_engine_cache_lookups_total", "result" => "miss", "kind" => "artifact")
                .increment(1);
            return Ok(None);
        };
        if !cached.is_current() {
            self.cache.invalidate_artifact(identity).await?;
            return Ok(None);
        }
        metrics::counter!("media_engine_cache_lookups_total", "result" => "hit", "kind" => "artifact")
            .increment(1);
        match self.storage.exists(&cached.object_key).await {
            Ok(true) => {}
            Ok(false) => {
                self.cache.invalidate_artifact(identity).await?;
                self.store.mark_artifact_unavailable(cached.artifact_id).await?;
                return Ok(None);
            }
            Err(error) => {
                tracing::warn!(%error, "storage existence check failed; trusting the database next");
                return Ok(None);
            }
        }
        let artifact = Artifact {
            id: cached.artifact_id,
            media_id: cached.media_id,
            profile_hash: identity.profile_hash().as_str().to_owned(),
            version: 0,
            object_key: cached.object_key,
            checksum_sha256: cached.checksum_sha256,
            byte_size: cached.byte_size,
            content_type: cached.content_type,
            status: ArtifactStatus::Ready,
            created_at: self.clock.now(),
        };
        self.audit_hit(&command.actor, "cache_hit", &artifact.id.to_string()).await?;
        Ok(Some(self.ready_outcome(command, identity, artifact).await?))
    }

    async fn ready_from_store(
        &self,
        identity: &MediaIdentity,
        command: &SubmitCommand,
    ) -> Result<Option<SubmitOutcome>, AppError> {
        let Some(artifact) = self.store.find_ready_artifact(identity).await? else {
            return Ok(None);
        };
        if !self.storage.exists(&artifact.object_key).await? {
            self.store.mark_artifact_unavailable(artifact.id).await?;
            return Ok(None);
        }
        let cached = CachedArtifact::from_artifact(&artifact);
        self.cache.put_artifact(identity, &cached, self.config.artifact_cache_ttl).await?;
        self.audit_hit(&command.actor, "store_hit", &artifact.id.to_string()).await?;
        Ok(Some(self.ready_outcome(command, identity, artifact).await?))
    }

    /// A stored Telegram file id is a delivery of the ready artifact, not a new download.
    async fn ready_outcome(
        &self,
        command: &SubmitCommand,
        identity: &MediaIdentity,
        artifact: Artifact,
    ) -> Result<SubmitOutcome, AppError> {
        if let Some(bot_id) = command.channel.bot_id()
            && let Some(file) = self.store.find_telegram_delivery(bot_id, artifact.id).await?
        {
            self.cache
                .put_telegram_file(bot_id, identity, &file, self.config.telegram_file_ttl)
                .await?;
            metrics::counter!("media_engine_telegram_file_id_total", "result" => "hit")
                .increment(1);
            return Ok(SubmitOutcome::TelegramCached { file, identity: identity.clone() });
        }
        Ok(SubmitOutcome::Ready {
            media_id: artifact.media_id,
            artifact,
            identity: identity.clone(),
        })
    }

    async fn attach_after_contention(
        &self,
        identity: &MediaIdentity,
        command: &SubmitCommand,
        canonical_url: &str,
    ) -> Result<SubmitOutcome, AppError> {
        for attempt in 0..8 {
            if let Some(job) = self.store.find_active_job(identity).await? {
                self.store.add_subscription(job.id, &command.actor, &command.channel).await?;
                return Ok(SubmitOutcome::Queued {
                    job_id: job.id,
                    media_id: job.media_id,
                    identity: identity.clone(),
                    attached: true,
                });
            }
            if let Some(outcome) = self.ready_from_store(identity, command).await? {
                return Ok(outcome);
            }
            tokio::time::sleep(Duration::from_millis(20 * (attempt + 1))).await;
        }
        let owner = self.ids.new_id().to_string();
        if self.locks.try_acquire(&identity.lock_key(), &owner, self.config.lock_ttl).await? {
            let created = self.create_or_attach(identity, command, canonical_url).await;
            let _ = self.locks.release(&identity.lock_key(), &owner).await;
            return created;
        }
        Err(AppError::Busy)
    }

    async fn create_or_attach(
        &self,
        identity: &MediaIdentity,
        command: &SubmitCommand,
        canonical_url: &str,
    ) -> Result<SubmitOutcome, AppError> {
        if let Some(job) = self.store.find_active_job(identity).await? {
            self.store.add_subscription(job.id, &command.actor, &command.channel).await?;
            return Ok(SubmitOutcome::Queued {
                job_id: job.id,
                media_id: job.media_id,
                identity: identity.clone(),
                attached: true,
            });
        }
        if let Some(outcome) = self.ready_from_store(identity, command).await? {
            return Ok(outcome);
        }
        let quota_ok = self
            .admission
            .allow(
                "quota",
                &command.actor.user_id.to_string(),
                self.config.daily_quota,
                Duration::from_secs(24 * 3600),
            )
            .await?;
        if !quota_ok {
            return Err(AppError::QuotaExceeded);
        }
        let draft = NewJobDraft {
            job_id: self.ids.new_id(),
            platform: identity.platform(),
            content_id: identity.content_id().as_str().to_owned(),
            canonical_url: canonical_url.to_owned(),
            profile: command.profile,
            profile_hash: identity.profile_hash().as_str().to_owned(),
            actor: command.actor.clone(),
            channel: command.channel.clone(),
            max_attempts: self.config.max_attempts,
        };
        let committed = self.store.commit_new_job(draft).await?;
        let (job_id, media_id, attached) = match committed {
            CommitJob::Created { job_id, media_id } => (job_id, media_id, false),
            CommitJob::Attached { job_id, media_id } => (job_id, media_id, true),
        };
        self.audit_hit(
            &command.actor,
            if attached { "job_attached" } else { "job_created" },
            &job_id.to_string(),
        )
        .await?;
        Ok(SubmitOutcome::Queued { job_id, media_id, identity: identity.clone(), attached })
    }

    pub async fn job_for_user(&self, user_id: Uuid, job_id: Uuid) -> Result<Job, AppError> {
        self.store.get_job_for_user(user_id, job_id).await?.ok_or(AppError::NotFound)
    }

    pub async fn media_for_user(
        &self,
        user_id: Uuid,
        media_id: Uuid,
    ) -> Result<MediaRecord, AppError> {
        self.store.get_media_for_user(user_id, media_id).await?.ok_or(AppError::NotFound)
    }

    pub async fn progress(&self, job_id: Uuid) -> Result<Option<JobProgress>, AppError> {
        self.cache.get_progress(job_id).await
    }

    pub async fn cancel(&self, actor: Uuid, job_id: Uuid) -> Result<Job, AppError> {
        match self.store.cancel_job(job_id, actor).await? {
            Some(CancelOutcome::Cancelled) => {
                self.store.get_job(job_id).await?.ok_or(AppError::NotFound)
            }
            Some(CancelOutcome::NotCancellable) => Err(AppError::Conflict),
            None => Err(AppError::NotFound),
        }
    }

    pub async fn ready(&self) -> Result<(), AppError> {
        self.store.ping().await?;
        self.cache.ping().await?;
        self.storage.ping().await
    }

    pub async fn store_user(
        &self,
        kind: crate::domain::UserKind,
        external_ref: &str,
        display_name: Option<&str>,
    ) -> Result<UserRecord, AppError> {
        self.store.upsert_external_user(kind, external_ref, display_name).await
    }

    pub async fn usage(&self, user_id: Uuid) -> Result<u64, AppError> {
        self.store.user_job_count(user_id).await
    }

    pub async fn platforms(&self) -> Result<Vec<PlatformInfo>, AppError> {
        let mut from_store = self.store.platforms().await?;
        if from_store.is_empty() {
            from_store = self.platforms.supported();
        }
        Ok(from_store)
    }

    pub async fn authenticate_api_key(&self, token: &str) -> Result<Actor, AppError> {
        let hash = crate::shared::sha256_hex(token.as_bytes());
        let user =
            self.store.find_user_by_api_key_hash(&hash).await?.ok_or(AppError::Unauthorized)?;
        if user.status != "active" {
            return Err(AppError::Unauthorized);
        }
        Ok(Actor { user_id: user.id, kind: user.kind })
    }

    pub async fn open_artifact(
        &self,
        user_id: Uuid,
        artifact_id: Uuid,
    ) -> Result<ObjectBody, AppError> {
        let artifact = self.authorize_download(user_id, artifact_id).await?;
        self.storage.open(&artifact.object_key).await
    }

    /// Opens an artifact that `submit` just returned to this caller.
    pub async fn open_ready_artifact(&self, artifact: &Artifact) -> Result<ObjectBody, AppError> {
        if artifact.status != ArtifactStatus::Ready {
            return Err(AppError::NotFound);
        }
        self.storage.open(&artifact.object_key).await
    }

    pub async fn authorize_download(
        &self,
        user_id: Uuid,
        artifact_id: Uuid,
    ) -> Result<Artifact, AppError> {
        self.store
            .authorize_artifact(user_id, artifact_id)
            .await?
            .filter(|artifact| artifact.status == ArtifactStatus::Ready)
            .ok_or(AppError::NotFound)
    }

    pub async fn invalidate_telegram_file(
        &self,
        bot_id: &str,
        identity: &MediaIdentity,
        artifact_id: Uuid,
    ) -> Result<(), AppError> {
        self.cache.invalidate_telegram_file(bot_id, identity).await?;
        self.store.invalidate_telegram_delivery(bot_id, artifact_id).await
    }

    pub async fn remember_telegram_file(
        &self,
        bot_id: &str,
        identity: &MediaIdentity,
        file: &TelegramFileRef,
    ) -> Result<(), AppError> {
        self.store.save_telegram_delivery(bot_id, file).await?;
        self.cache.put_telegram_file(bot_id, identity, file, self.config.telegram_file_ttl).await
    }

    pub async fn put_selection(
        &self,
        token: &str,
        selection: &FormatSelection,
    ) -> Result<(), AppError> {
        self.cache.put_selection(token, selection, self.config.selection_ttl).await
    }

    pub async fn take_selection(&self, token: &str) -> Result<Option<FormatSelection>, AppError> {
        self.cache.get_selection(token).await
    }

    async fn admit(&self, actor: &Actor) -> Result<(), AppError> {
        let allowed = self
            .admission
            .allow(
                "rate",
                &actor.user_id.to_string(),
                self.config.rate_limit_per_minute,
                Duration::from_secs(60),
            )
            .await?;
        if allowed { Ok(()) } else { Err(AppError::RateLimited) }
    }

    async fn audit_hit(&self, actor: &Actor, action: &str, target: &str) -> Result<(), AppError> {
        self.store
            .audit(AuditEntry {
                id: self.ids.new_id(),
                actor_user_id: Some(actor.user_id),
                action: action.to_owned(),
                target_type: "media".to_owned(),
                target_id: Some(target.to_owned()),
                request_id: None,
                details: serde_json::json!({}),
            })
            .await
    }

    pub async fn idempotent_hit(
        &self,
        user_id: Uuid,
        key: &str,
        request_hash: &str,
    ) -> Result<Option<IdempotencyRecord>, AppError> {
        let Some(record) = self.store.idempotency_get(user_id, key).await? else {
            return Ok(None);
        };
        if record.request_hash != request_hash {
            return Err(AppError::IdempotencyConflict);
        }
        Ok(Some(record))
    }

    pub async fn remember_response(&self, record: NewIdempotency) -> Result<(), AppError> {
        match self.store.idempotency_put(record).await? {
            IdempotencyPut::Stored => Ok(()),
            IdempotencyPut::Conflict => Err(AppError::IdempotencyConflict),
        }
    }

    pub fn retry_policy(&self) -> RetryPolicy {
        RetryPolicy {
            max_attempts: self.config.max_attempts,
            base: self.config.retry_base,
            cap: self.config.retry_cap,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ResolveView {
    pub platform: crate::domain::PlatformKind,
    pub content_id: String,
    pub canonical_url: String,
    pub metadata: MediaMetadata,
}

/// Pure helper so tests can show that URL identity does not depend on tracking parameters.
pub fn preview_identity(
    raw_url: &str,
    profile: &ProcessingProfile,
) -> Result<MediaIdentity, AppError> {
    let url = UrlGuardNone::parse(raw_url)?;
    let identified = identify_url(&url)?.ok_or(DomainError::UnsupportedPlatform)?;
    Ok(MediaIdentity::derive(identified.platform, identified.content_id, profile)?)
}

/// Syntax-only guard used by the pure preview helper. The production guard also checks DNS.
struct UrlGuardNone;

impl UrlGuardNone {
    fn parse(raw: &str) -> Result<url::Url, AppError> {
        let url = url::Url::parse(raw).map_err(|_| DomainError::InvalidUrl)?;
        if !matches!(url.scheme(), "http" | "https") {
            return Err(DomainError::InvalidUrl.into());
        }
        Ok(url)
    }
}

impl std::fmt::Debug for Engine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Engine")
    }
}
