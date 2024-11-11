#![cfg(test)]

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use tokio::sync::Mutex;
use url::Url;
use uuid::Uuid;

use crate::application::{
    Admission, AppError, AuditEntry, Cache, CancelOutcome, ClaimedJob, CommitJob, DownloadedMedia,
    FormatSelection, IdempotencyPut, IdempotencyRecord, JobCompletion, JobProgress, JobQueue,
    JobSettlement, Locks, MediaExtractor, MediaMetadata, MediaProcessor, MediaRecord,
    MetadataSource, NewArtifact, NewAttempt, NewIdempotency, NewJobDraft, ObjectBody,
    ObjectStorage, OutboxEvent, PendingNotification, PlatformInfo, PlatformRegistry, QueueDelivery,
    QueueMessage, RuntimeConfig, Store, Subscription, SystemClock, TelegramFileRef, UrlGuard,
    UserRecord, UuidIds,
};
use crate::domain::{
    Artifact, ArtifactStatus, CachedArtifact, DeliveryChannel, FailureClass, IdentifiedContent,
    Job, JobStatus, MediaIdentity, PlatformKind, ProcessingProfile, UserKind, identify_url,
    ip_is_public,
};

struct LockEntry {
    owner: String,
    expires: tokio::time::Instant,
}

#[derive(Default)]
pub struct MemoryLocks {
    inner: Mutex<HashMap<String, LockEntry>>,
}

#[async_trait]
impl Locks for MemoryLocks {
    async fn try_acquire(&self, key: &str, owner: &str, ttl: Duration) -> Result<bool, AppError> {
        let mut guard = self.inner.lock().await;
        let now = tokio::time::Instant::now();
        if let Some(existing) = guard.get(key)
            && existing.expires > now
            && existing.owner != owner
        {
            return Ok(false);
        }
        guard.insert(key.to_owned(), LockEntry { owner: owner.to_owned(), expires: now + ttl });
        Ok(true)
    }

    async fn renew(&self, key: &str, owner: &str, ttl: Duration) -> Result<bool, AppError> {
        let mut guard = self.inner.lock().await;
        let Some(existing) = guard.get_mut(key) else {
            return Ok(false);
        };
        if existing.owner != owner || existing.expires <= tokio::time::Instant::now() {
            return Ok(false);
        }
        existing.expires = tokio::time::Instant::now() + ttl;
        Ok(true)
    }

    async fn release(&self, key: &str, owner: &str) -> Result<bool, AppError> {
        let mut guard = self.inner.lock().await;
        match guard.get(key) {
            Some(existing) if existing.owner == owner => {
                guard.remove(key);
                Ok(true)
            }
            _ => Ok(false),
        }
    }
}

/// Lock that never excludes anyone. Used to prove the store constraint still deduplicates.
pub struct OpenLock;

#[async_trait]
impl Locks for OpenLock {
    async fn try_acquire(&self, _: &str, _: &str, _: Duration) -> Result<bool, AppError> {
        Ok(true)
    }
    async fn renew(&self, _: &str, _: &str, _: Duration) -> Result<bool, AppError> {
        Ok(true)
    }
    async fn release(&self, _: &str, _: &str) -> Result<bool, AppError> {
        Ok(true)
    }
}

struct CacheEntry<T> {
    value: T,
    expires: tokio::time::Instant,
}

pub struct MemoryCache {
    artifacts: Mutex<HashMap<String, CacheEntry<CachedArtifact>>>,
    metadata: Mutex<HashMap<String, CacheEntry<MediaMetadata>>>,
    telegram: Mutex<HashMap<String, CacheEntry<TelegramFileRef>>>,
    progress: Mutex<HashMap<Uuid, CacheEntry<JobProgress>>>,
    selections: Mutex<HashMap<String, CacheEntry<FormatSelection>>>,
}

impl Default for MemoryCache {
    fn default() -> Self {
        Self {
            artifacts: Mutex::new(HashMap::new()),
            metadata: Mutex::new(HashMap::new()),
            telegram: Mutex::new(HashMap::new()),
            progress: Mutex::new(HashMap::new()),
            selections: Mutex::new(HashMap::new()),
        }
    }
}

fn fresh<T: Clone>(entry: &CacheEntry<T>) -> Option<T> {
    if entry.expires > tokio::time::Instant::now() { Some(entry.value.clone()) } else { None }
}

#[async_trait]
impl Cache for MemoryCache {
    async fn get_artifact(
        &self,
        identity: &MediaIdentity,
    ) -> Result<Option<CachedArtifact>, AppError> {
        let guard = self.artifacts.lock().await;
        Ok(guard.get(&identity.cache_key()).and_then(fresh))
    }
    async fn put_artifact(
        &self,
        identity: &MediaIdentity,
        artifact: &CachedArtifact,
        ttl: Duration,
    ) -> Result<(), AppError> {
        self.artifacts.lock().await.insert(
            identity.cache_key(),
            CacheEntry { value: artifact.clone(), expires: tokio::time::Instant::now() + ttl },
        );
        Ok(())
    }
    async fn invalidate_artifact(&self, identity: &MediaIdentity) -> Result<(), AppError> {
        self.artifacts.lock().await.remove(&identity.cache_key());
        Ok(())
    }
    async fn get_metadata(
        &self,
        platform: PlatformKind,
        content_id: &str,
    ) -> Result<Option<MediaMetadata>, AppError> {
        let key = format!("meta:{platform}:{content_id}");
        Ok(self.metadata.lock().await.get(&key).and_then(fresh))
    }
    async fn put_metadata(
        &self,
        platform: PlatformKind,
        content_id: &str,
        meta: &MediaMetadata,
        ttl: Duration,
    ) -> Result<(), AppError> {
        let key = format!("meta:{platform}:{content_id}");
        self.metadata.lock().await.insert(
            key,
            CacheEntry { value: meta.clone(), expires: tokio::time::Instant::now() + ttl },
        );
        Ok(())
    }
    async fn get_telegram_file(
        &self,
        bot_id: &str,
        identity: &MediaIdentity,
    ) -> Result<Option<TelegramFileRef>, AppError> {
        Ok(self.telegram.lock().await.get(&identity.telegram_key(bot_id)).and_then(fresh))
    }
    async fn put_telegram_file(
        &self,
        bot_id: &str,
        identity: &MediaIdentity,
        file: &TelegramFileRef,
        ttl: Duration,
    ) -> Result<(), AppError> {
        self.telegram.lock().await.insert(
            identity.telegram_key(bot_id),
            CacheEntry { value: file.clone(), expires: tokio::time::Instant::now() + ttl },
        );
        Ok(())
    }
    async fn invalidate_telegram_file(
        &self,
        bot_id: &str,
        identity: &MediaIdentity,
    ) -> Result<(), AppError> {
        self.telegram.lock().await.remove(&identity.telegram_key(bot_id));
        Ok(())
    }
    async fn put_progress(
        &self,
        job_id: Uuid,
        progress: &JobProgress,
        ttl: Duration,
    ) -> Result<(), AppError> {
        self.progress.lock().await.insert(
            job_id,
            CacheEntry { value: progress.clone(), expires: tokio::time::Instant::now() + ttl },
        );
        Ok(())
    }
    async fn get_progress(&self, job_id: Uuid) -> Result<Option<JobProgress>, AppError> {
        Ok(self.progress.lock().await.get(&job_id).and_then(fresh))
    }
    async fn put_selection(
        &self,
        token: &str,
        selection: &FormatSelection,
        ttl: Duration,
    ) -> Result<(), AppError> {
        self.selections.lock().await.insert(
            token.to_owned(),
            CacheEntry { value: selection.clone(), expires: tokio::time::Instant::now() + ttl },
        );
        Ok(())
    }
    async fn get_selection(&self, token: &str) -> Result<Option<FormatSelection>, AppError> {
        Ok(self.selections.lock().await.get(token).and_then(fresh))
    }
    async fn ping(&self) -> Result<(), AppError> {
        Ok(())
    }
}

struct StoredJob {
    job: Job,
    canonical_url: String,
    locked_by: Option<String>,
    lease_until: Option<DateTime<Utc>>,
    next_attempt_at: Option<DateTime<Utc>>,
}

struct Db {
    media: HashMap<(PlatformKind, String), MediaRecord>,
    jobs: Vec<StoredJob>,
    artifacts: Vec<Artifact>,
    subs: Vec<Subscription>,
    outbox: Vec<OutboxEvent>,
    users: HashMap<Uuid, UserRecord>,
    keys: HashMap<String, Uuid>,
    idempotency: HashMap<(Uuid, String), IdempotencyRecord>,
    telegram: HashMap<(String, Uuid), TelegramFileRef>,
    commits: u64,
    next_outbox: i64,
    next_version: HashMap<(Uuid, String), i32>,
}

impl Db {
    fn active(
        &self,
        platform: PlatformKind,
        content_id: &str,
        profile_hash: &str,
    ) -> Option<&StoredJob> {
        self.jobs.iter().find(|job| {
            job.job.identity.platform() == platform
                && job.job.identity.content_id().as_str() == content_id
                && job.job.identity.profile_hash().as_str() == profile_hash
                && job.job.status.is_active()
        })
    }

    fn insert_job(&mut self, draft: &NewJobDraft) -> (Uuid, Uuid) {
        let media_id =
            if let Some(existing) = self.media.get(&(draft.platform, draft.content_id.clone())) {
                existing.id
            } else {
                let id = Uuid::now_v7();
                self.media.insert(
                    (draft.platform, draft.content_id.clone()),
                    MediaRecord {
                        id,
                        platform: draft.platform,
                        content_id: draft.content_id.clone(),
                        canonical_url: Some(draft.canonical_url.clone()),
                        title: None,
                        duration_seconds: None,
                    },
                );
                id
            };
        let identity = MediaIdentity::derive(
            draft.platform,
            crate::domain::ContentId::new(draft.content_id.clone())
                .expect("content id already validated"),
            &draft.profile,
        )
        .expect("profile already validated");
        let now = Utc::now();
        let job = Job {
            id: draft.job_id,
            media_id,
            identity,
            profile: draft.profile,
            status: JobStatus::Queued,
            attempt_count: 0,
            max_attempts: draft.max_attempts,
            failure_class: None,
            failure_reason: None,
            artifact_id: None,
            requested_by: Some(draft.actor.user_id),
            created_at: now,
            updated_at: now,
        };
        self.subs.push(Subscription {
            id: Uuid::now_v7(),
            job_id: job.id,
            user_id: draft.actor.user_id,
            channel: draft.channel.clone(),
            notify_status: "pending".to_owned(),
        });
        self.next_outbox += 1;
        self.outbox.push(OutboxEvent {
            id: self.next_outbox,
            aggregate_id: job.id,
            event_type: "job.queued".to_owned(),
            payload: serde_json::json!({"job_id": job.id}),
        });
        let job_id = job.id;
        self.jobs.push(StoredJob {
            job,
            canonical_url: draft.canonical_url.clone(),
            locked_by: None,
            lease_until: None,
            next_attempt_at: None,
        });
        self.commits += 1;
        (job_id, media_id)
    }
}

pub struct MemoryStore {
    inner: Mutex<Db>,
    blind: bool,
    pub refuse_renew: AtomicBool,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self::with_blind(false)
    }

    pub fn blind() -> Self {
        Self::with_blind(true)
    }

    fn with_blind(blind: bool) -> Self {
        Self {
            inner: Mutex::new(Db {
                media: HashMap::new(),
                jobs: Vec::new(),
                artifacts: Vec::new(),
                subs: Vec::new(),
                outbox: Vec::new(),
                users: HashMap::new(),
                keys: HashMap::new(),
                idempotency: HashMap::new(),
                telegram: HashMap::new(),
                commits: 0,
                next_outbox: 0,
                next_version: HashMap::new(),
            }),
            blind,
            refuse_renew: AtomicBool::new(false),
        }
    }

    pub async fn commits(&self) -> u64 {
        self.inner.lock().await.commits
    }

    pub async fn subscription_count(&self) -> usize {
        self.inner.lock().await.subs.len()
    }

    pub async fn job_count(&self) -> usize {
        self.inner.lock().await.jobs.len()
    }
}

#[async_trait]
impl Store for MemoryStore {
    async fn find_ready_artifact(
        &self,
        identity: &MediaIdentity,
    ) -> Result<Option<Artifact>, AppError> {
        let db = self.inner.lock().await;
        let media_id = db
            .media
            .get(&(identity.platform(), identity.content_id().as_str().to_owned()))
            .map(|media| media.id);
        Ok(media_id.and_then(|media_id| {
            db.artifacts
                .iter()
                .find(|artifact| {
                    artifact.media_id == media_id
                        && artifact.status == ArtifactStatus::Ready
                        && artifact.profile_hash == identity.profile_hash().as_str()
                })
                .cloned()
        }))
    }

    async fn mark_artifact_unavailable(&self, id: Uuid) -> Result<(), AppError> {
        let mut db = self.inner.lock().await;
        if let Some(artifact) = db.artifacts.iter_mut().find(|artifact| artifact.id == id) {
            artifact.status = ArtifactStatus::Deleted;
        }
        Ok(())
    }

    async fn find_active_job(&self, identity: &MediaIdentity) -> Result<Option<Job>, AppError> {
        let db = self.inner.lock().await;
        Ok(db
            .active(
                identity.platform(),
                identity.content_id().as_str(),
                identity.profile_hash().as_str(),
            )
            .map(|stored| stored.job.clone()))
    }

    async fn add_subscription(
        &self,
        job_id: Uuid,
        actor: &crate::domain::Actor,
        channel: &DeliveryChannel,
    ) -> Result<(), AppError> {
        let mut db = self.inner.lock().await;
        let exists = db.subs.iter().any(|sub| {
            sub.job_id == job_id
                && sub.user_id == actor.user_id
                && sub.channel.chat_id() == channel.chat_id()
        });
        if !exists {
            db.subs.push(Subscription {
                id: Uuid::now_v7(),
                job_id,
                user_id: actor.user_id,
                channel: channel.clone(),
                notify_status: "pending".to_owned(),
            });
        }
        Ok(())
    }

    async fn commit_new_job(&self, draft: NewJobDraft) -> Result<CommitJob, AppError> {
        if self.blind {
            let attached = {
                let db = self.inner.lock().await;
                db.active(draft.platform, &draft.content_id, &draft.profile_hash)
                    .map(|stored| (stored.job.id, stored.job.media_id))
            };
            if let Some((job_id, media_id)) = attached {
                self.add_subscription(job_id, &draft.actor, &draft.channel).await?;
                return Ok(CommitJob::Attached { job_id, media_id });
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
            let mut db = self.inner.lock().await;
            let (job_id, media_id) = db.insert_job(&draft);
            return Ok(CommitJob::Created { job_id, media_id });
        }
        let mut db = self.inner.lock().await;
        if let Some(stored) = db.active(draft.platform, &draft.content_id, &draft.profile_hash) {
            let job_id = stored.job.id;
            let media_id = stored.job.media_id;
            let exists = db
                .subs
                .iter()
                .any(|sub| sub.job_id == job_id && sub.user_id == draft.actor.user_id);
            if !exists {
                db.subs.push(Subscription {
                    id: Uuid::now_v7(),
                    job_id,
                    user_id: draft.actor.user_id,
                    channel: draft.channel.clone(),
                    notify_status: "pending".to_owned(),
                });
            }
            return Ok(CommitJob::Attached { job_id, media_id });
        }
        let (job_id, media_id) = db.insert_job(&draft);
        Ok(CommitJob::Created { job_id, media_id })
    }

    async fn get_job(&self, id: Uuid) -> Result<Option<Job>, AppError> {
        Ok(self
            .inner
            .lock()
            .await
            .jobs
            .iter()
            .find(|job| job.job.id == id)
            .map(|job| job.job.clone()))
    }

    async fn get_job_for_user(&self, user_id: Uuid, id: Uuid) -> Result<Option<Job>, AppError> {
        let db = self.inner.lock().await;
        let allowed = db.subs.iter().any(|sub| sub.job_id == id && sub.user_id == user_id)
            || db.jobs.iter().any(|job| job.job.id == id && job.job.requested_by == Some(user_id));
        if !allowed {
            return Ok(None);
        }
        Ok(db.jobs.iter().find(|job| job.job.id == id).map(|job| job.job.clone()))
    }

    async fn get_media(&self, id: Uuid) -> Result<Option<MediaRecord>, AppError> {
        Ok(self.inner.lock().await.media.values().find(|media| media.id == id).cloned())
    }

    async fn get_media_for_user(
        &self,
        user_id: Uuid,
        id: Uuid,
    ) -> Result<Option<MediaRecord>, AppError> {
        let db = self.inner.lock().await;
        let allowed = db.jobs.iter().any(|job| {
            job.job.media_id == id
                && (job.job.requested_by == Some(user_id)
                    || db.subs.iter().any(|sub| sub.job_id == job.job.id && sub.user_id == user_id))
        });
        if !allowed {
            return Ok(None);
        }
        Ok(db.media.values().find(|media| media.id == id).cloned())
    }

    async fn authorize_artifact(
        &self,
        user_id: Uuid,
        artifact_id: Uuid,
    ) -> Result<Option<Artifact>, AppError> {
        let db = self.inner.lock().await;
        let Some(artifact) =
            db.artifacts.iter().find(|artifact| artifact.id == artifact_id).cloned()
        else {
            return Ok(None);
        };
        let allowed = db.jobs.iter().any(|job| {
            job.job.media_id == artifact.media_id
                && job.job.identity.profile_hash().as_str() == artifact.profile_hash
                && (job.job.requested_by == Some(user_id)
                    || db.subs.iter().any(|sub| sub.job_id == job.job.id && sub.user_id == user_id))
        });
        Ok(allowed.then_some(artifact))
    }

    async fn cancel_job(&self, id: Uuid, actor: Uuid) -> Result<Option<CancelOutcome>, AppError> {
        let mut db = self.inner.lock().await;
        let owns =
            db.jobs.iter().any(|job| job.job.id == id && job.job.requested_by == Some(actor))
                || db.subs.iter().any(|sub| sub.job_id == id && sub.user_id == actor);
        if !owns {
            return Ok(None);
        }
        let Some(stored) = db.jobs.iter_mut().find(|job| job.job.id == id) else {
            return Ok(None);
        };
        if stored.job.status.transition(JobStatus::Cancelled).is_err() {
            return Ok(Some(CancelOutcome::NotCancellable));
        }
        stored.job.status = JobStatus::Cancelled;
        stored.job.updated_at = Utc::now();
        stored.locked_by = None;
        Ok(Some(CancelOutcome::Cancelled))
    }

    async fn claim_job(
        &self,
        id: Uuid,
        worker: &str,
        lease: Duration,
    ) -> Result<Option<ClaimedJob>, AppError> {
        let mut db = self.inner.lock().await;
        let Some(stored) = db.jobs.iter_mut().find(|job| job.job.id == id) else {
            return Ok(None);
        };
        let now = Utc::now();
        let lease_open = stored.lease_until.is_none_or(|until| until <= now);
        let due = stored.next_attempt_at.is_none_or(|at| at <= now);
        if !matches!(stored.job.status, JobStatus::Pending | JobStatus::Queued)
            || !lease_open
            || !due
        {
            return Ok(None);
        }
        stored.job.status = JobStatus::Processing;
        stored.job.attempt_count += 1;
        stored.job.updated_at = now;
        stored.locked_by = Some(worker.to_owned());
        stored.lease_until = Some(now + chrono::Duration::from_std(lease).unwrap_or_default());
        Ok(Some(claimed_from(stored)))
    }

    async fn renew_job_lease(
        &self,
        id: Uuid,
        worker: &str,
        lease: Duration,
    ) -> Result<bool, AppError> {
        let mut db = self.inner.lock().await;
        let Some(stored) = db.jobs.iter_mut().find(|job| job.job.id == id) else {
            return Ok(false);
        };
        if self.refuse_renew.load(Ordering::SeqCst) || stored.locked_by.as_deref() != Some(worker) {
            return Ok(false);
        }
        stored.lease_until =
            Some(Utc::now() + chrono::Duration::from_std(lease).unwrap_or_default());
        Ok(true)
    }

    async fn job_status(&self, id: Uuid) -> Result<Option<JobStatus>, AppError> {
        Ok(self
            .inner
            .lock()
            .await
            .jobs
            .iter()
            .find(|job| job.job.id == id)
            .map(|job| job.job.status))
    }

    async fn record_attempt(&self, _: NewAttempt) -> Result<(), AppError> {
        Ok(())
    }

    async fn mark_uploading(&self, job_id: Uuid, worker: &str) -> Result<bool, AppError> {
        let mut db = self.inner.lock().await;
        let Some(stored) = db.jobs.iter_mut().find(|job| job.job.id == job_id) else {
            return Ok(false);
        };
        if stored.locked_by.as_deref() != Some(worker) || stored.job.status != JobStatus::Processing
        {
            return Ok(false);
        }
        stored.job.status = JobStatus::Uploading;
        stored.job.updated_at = Utc::now();
        Ok(true)
    }

    async fn reserve_artifact(&self, draft: NewArtifact) -> Result<Artifact, AppError> {
        let mut db = self.inner.lock().await;
        let version =
            db.next_version.entry((draft.media_id, draft.profile_hash.clone())).or_insert(0);
        *version += 1;
        let artifact = Artifact {
            id: draft.id,
            media_id: draft.media_id,
            profile_hash: draft.profile_hash,
            version: *version,
            object_key: draft.object_key,
            checksum_sha256: "0".repeat(64),
            byte_size: 0,
            content_type: draft.content_type,
            status: ArtifactStatus::Uploading,
            created_at: Utc::now(),
        };
        db.artifacts.push(artifact.clone());
        Ok(artifact)
    }

    async fn complete_job(&self, completion: JobCompletion) -> Result<bool, AppError> {
        let mut db = self.inner.lock().await;
        let Some(stored) = db.jobs.iter_mut().find(|job| job.job.id == completion.job_id) else {
            return Ok(false);
        };
        if stored.locked_by.as_deref() != Some(completion.worker_id.as_str())
            || stored.job.status != JobStatus::Uploading
        {
            return Ok(false);
        }
        stored.job.status = JobStatus::Completed;
        stored.job.artifact_id = Some(completion.artifact_id);
        stored.job.updated_at = Utc::now();
        stored.locked_by = None;
        if let Some(artifact) =
            db.artifacts.iter_mut().find(|artifact| artifact.id == completion.artifact_id)
        {
            artifact.status = ArtifactStatus::Ready;
            artifact.checksum_sha256 = completion.checksum_sha256;
            artifact.byte_size = completion.byte_size;
        }
        Ok(true)
    }

    async fn settle_job(&self, settlement: JobSettlement) -> Result<(), AppError> {
        let mut db = self.inner.lock().await;
        match settlement {
            JobSettlement::Requeue { job_id, worker_id, delay, reason, .. } => {
                let mut matched = false;
                if let Some(stored) = db.jobs.iter_mut().find(|job| job.job.id == job_id)
                    && stored.locked_by.as_deref() == Some(worker_id.as_str())
                {
                    stored.job.status = JobStatus::Queued;
                    stored.job.failure_class = Some(FailureClass::Transient);
                    stored.job.failure_reason = Some(reason);
                    stored.locked_by = None;
                    stored.lease_until = None;
                    stored.next_attempt_at =
                        Some(Utc::now() + chrono::Duration::from_std(delay).unwrap_or_default());
                    matched = true;
                }
                if matched {
                    db.next_outbox += 1;
                    let id = db.next_outbox;
                    db.outbox.push(OutboxEvent {
                        id,
                        aggregate_id: job_id,
                        event_type: "job.queued".into(),
                        payload: serde_json::json!({"job_id": job_id}),
                    });
                }
            }
            JobSettlement::Fail { job_id, worker_id, class, reason, .. } => {
                if let Some(stored) = db.jobs.iter_mut().find(|job| job.job.id == job_id)
                    && (stored.locked_by.as_deref() == Some(worker_id.as_str())
                        || stored.job.status.is_active())
                {
                    stored.job.status = JobStatus::Failed;
                    stored.job.failure_class = Some(class);
                    stored.job.failure_reason = Some(reason);
                    stored.locked_by = None;
                }
            }
            JobSettlement::Cancel { job_id, .. } => {
                if let Some(stored) = db.jobs.iter_mut().find(|job| job.job.id == job_id) {
                    stored.job.status = JobStatus::Cancelled;
                    stored.locked_by = None;
                }
            }
        }
        Ok(())
    }

    async fn recover_expired_leases(&self, limit: i64) -> Result<Vec<Uuid>, AppError> {
        let mut db = self.inner.lock().await;
        let now = Utc::now();
        let mut recovered = Vec::new();
        let mut requeue = Vec::new();
        for stored in &mut db.jobs {
            if requeue.len() as i64 >= limit {
                break;
            }
            let expired = stored.lease_until.is_some_and(|until| until <= now);
            if expired
                && matches!(
                    stored.job.status,
                    JobStatus::Processing | JobStatus::Uploading | JobStatus::Queued
                )
            {
                if stored.job.attempt_count >= stored.job.max_attempts {
                    stored.job.status = JobStatus::Failed;
                    stored.job.failure_class = Some(FailureClass::Permanent);
                    stored.job.failure_reason = Some("abandoned after the attempt budget".into());
                } else {
                    stored.job.status = JobStatus::Queued;
                    requeue.push(stored.job.id);
                }
                stored.locked_by = None;
                stored.lease_until = None;
            }
        }
        for job_id in requeue {
            db.next_outbox += 1;
            let id = db.next_outbox;
            db.outbox.push(OutboxEvent {
                id,
                aggregate_id: job_id,
                event_type: "job.queued".into(),
                payload: serde_json::json!({"job_id": job_id}),
            });
            recovered.push(job_id);
        }
        Ok(recovered)
    }

    async fn expire_stale_jobs(&self, older_than: Duration, limit: i64) -> Result<u64, AppError> {
        let mut db = self.inner.lock().await;
        let cutoff = Utc::now() - chrono::Duration::from_std(older_than).unwrap_or_default();
        let mut count = 0u64;
        for stored in &mut db.jobs {
            if count >= limit as u64 {
                break;
            }
            if stored.job.status.is_active() && stored.job.created_at < cutoff {
                stored.job.status = JobStatus::Expired;
                stored.locked_by = None;
                count += 1;
            }
        }
        Ok(count)
    }

    async fn list_stale_uploads(
        &self,
        older_than: Duration,
        limit: i64,
    ) -> Result<Vec<Artifact>, AppError> {
        let db = self.inner.lock().await;
        let cutoff = Utc::now() - chrono::Duration::from_std(older_than).unwrap_or_default();
        Ok(db
            .artifacts
            .iter()
            .filter(|artifact| {
                artifact.status == ArtifactStatus::Uploading && artifact.created_at < cutoff
            })
            .take(limit as usize)
            .cloned()
            .collect())
    }

    async fn delete_artifact_row(&self, id: Uuid) -> Result<(), AppError> {
        self.mark_artifact_unavailable(id).await
    }

    async fn upsert_external_user(
        &self,
        kind: UserKind,
        external_ref: &str,
        display_name: Option<&str>,
    ) -> Result<UserRecord, AppError> {
        let mut db = self.inner.lock().await;
        if let Some(user) = db
            .users
            .values()
            .find(|user| user.kind == kind && user.external_ref == external_ref)
            .cloned()
        {
            return Ok(user);
        }
        let user = UserRecord {
            id: Uuid::now_v7(),
            kind,
            external_ref: external_ref.to_owned(),
            display_name: display_name.map(str::to_owned),
            status: "active".into(),
        };
        db.users.insert(user.id, user.clone());
        Ok(user)
    }

    async fn find_user_by_api_key_hash(&self, hash: &str) -> Result<Option<UserRecord>, AppError> {
        let db = self.inner.lock().await;
        Ok(db.keys.get(hash).and_then(|id| db.users.get(id)).cloned())
    }

    async fn create_api_user(&self, name: &str, key_hash: &str) -> Result<UserRecord, AppError> {
        let mut db = self.inner.lock().await;
        let user = UserRecord {
            id: Uuid::now_v7(),
            kind: UserKind::Api,
            external_ref: name.to_owned(),
            display_name: Some(name.to_owned()),
            status: "active".into(),
        };
        db.keys.insert(key_hash.to_owned(), user.id);
        db.users.insert(user.id, user.clone());
        Ok(user)
    }

    async fn audit(&self, _: AuditEntry) -> Result<(), AppError> {
        Ok(())
    }

    async fn idempotency_get(
        &self,
        user_id: Uuid,
        key: &str,
    ) -> Result<Option<IdempotencyRecord>, AppError> {
        Ok(self.inner.lock().await.idempotency.get(&(user_id, key.to_owned())).cloned())
    }

    async fn idempotency_put(&self, record: NewIdempotency) -> Result<IdempotencyPut, AppError> {
        let mut db = self.inner.lock().await;
        let map_key = (record.user_id, record.key.clone());
        if let Some(existing) = db.idempotency.get(&map_key) {
            return Ok(if existing.request_hash == record.request_hash {
                IdempotencyPut::Stored
            } else {
                IdempotencyPut::Conflict
            });
        }
        db.idempotency.insert(
            map_key,
            IdempotencyRecord {
                request_hash: record.request_hash,
                status: record.status,
                body: record.body,
                expires_at: record.expires_at,
            },
        );
        Ok(IdempotencyPut::Stored)
    }

    async fn idempotency_purge(&self, now: DateTime<Utc>) -> Result<u64, AppError> {
        let mut db = self.inner.lock().await;
        let before = db.idempotency.len();
        db.idempotency.retain(|_, record| record.expires_at > now);
        Ok((before - db.idempotency.len()) as u64)
    }

    async fn save_metadata(&self, media_id: Uuid, meta: &MediaMetadata) -> Result<(), AppError> {
        let mut db = self.inner.lock().await;
        if let Some(media) = db.media.values_mut().find(|media| media.id == media_id) {
            media.title = meta.title.clone();
            media.duration_seconds = meta.duration_seconds;
        }
        Ok(())
    }

    async fn fetch_outbox(&self, limit: i64) -> Result<Vec<OutboxEvent>, AppError> {
        let db = self.inner.lock().await;
        Ok(db.outbox.iter().take(limit as usize).cloned().collect())
    }

    async fn mark_outbox_published(&self, ids: &[i64]) -> Result<(), AppError> {
        let mut db = self.inner.lock().await;
        db.outbox.retain(|event| !ids.contains(&event.id));
        Ok(())
    }

    async fn heartbeat(&self, _: &str, _: &str) -> Result<(), AppError> {
        Ok(())
    }

    async fn platforms(&self) -> Result<Vec<PlatformInfo>, AppError> {
        Ok(PlatformKind::all()
            .iter()
            .map(|platform| PlatformInfo {
                code: platform.code().to_owned(),
                display_name: platform.display_name().to_owned(),
                enabled: true,
            })
            .collect())
    }

    async fn ping(&self) -> Result<(), AppError> {
        Ok(())
    }

    async fn pending_notifications(
        &self,
        limit: i64,
    ) -> Result<Vec<PendingNotification>, AppError> {
        let db = self.inner.lock().await;
        let mut notes = Vec::new();
        for sub in db.subs.iter().filter(|sub| sub.notify_status == "pending").take(limit as usize)
        {
            let Some(stored) = db.jobs.iter().find(|job| job.job.id == sub.job_id) else {
                continue;
            };
            if stored.job.status != JobStatus::Completed {
                continue;
            }
            let Some(artifact_id) = stored.job.artifact_id else {
                continue;
            };
            let Some(artifact) =
                db.artifacts.iter().find(|artifact| artifact.id == artifact_id).cloned()
            else {
                continue;
            };
            let DeliveryChannel::Telegram { bot_id, chat_id, message_id } = &sub.channel else {
                continue;
            };
            notes.push(PendingNotification {
                subscription_id: sub.id,
                job_id: sub.job_id,
                chat_id: *chat_id,
                message_id: *message_id,
                bot_id: bot_id.clone(),
                artifact,
                identity: stored.job.identity.clone(),
            });
        }
        Ok(notes)
    }

    async fn mark_notified(&self, subscription_id: Uuid, status: &str) -> Result<(), AppError> {
        let mut db = self.inner.lock().await;
        if let Some(sub) = db.subs.iter_mut().find(|sub| sub.id == subscription_id) {
            sub.notify_status = status.to_owned();
        }
        Ok(())
    }

    async fn save_telegram_delivery(
        &self,
        bot_id: &str,
        file: &TelegramFileRef,
    ) -> Result<(), AppError> {
        self.inner
            .lock()
            .await
            .telegram
            .insert((bot_id.to_owned(), file.artifact_id), file.clone());
        Ok(())
    }

    async fn find_telegram_delivery(
        &self,
        bot_id: &str,
        artifact_id: Uuid,
    ) -> Result<Option<TelegramFileRef>, AppError> {
        Ok(self.inner.lock().await.telegram.get(&(bot_id.to_owned(), artifact_id)).cloned())
    }

    async fn invalidate_telegram_delivery(
        &self,
        bot_id: &str,
        artifact_id: Uuid,
    ) -> Result<(), AppError> {
        self.inner.lock().await.telegram.remove(&(bot_id.to_owned(), artifact_id));
        Ok(())
    }

    async fn media_id_for_identity(
        &self,
        identity: &MediaIdentity,
    ) -> Result<Option<Uuid>, AppError> {
        Ok(self
            .inner
            .lock()
            .await
            .media
            .get(&(identity.platform(), identity.content_id().as_str().to_owned()))
            .map(|media| media.id))
    }
}

fn claimed_from(stored: &StoredJob) -> ClaimedJob {
    ClaimedJob {
        id: stored.job.id,
        media_id: stored.job.media_id,
        platform: stored.job.identity.platform(),
        content_id: stored.job.identity.content_id().as_str().to_owned(),
        canonical_url: stored.canonical_url.clone(),
        profile: stored.job.profile,
        profile_hash: stored.job.identity.profile_hash().as_str().to_owned(),
        attempt_count: stored.job.attempt_count,
        max_attempts: stored.job.max_attempts,
        status: stored.job.status,
    }
}

pub struct AllowAll;

#[async_trait]
impl Admission for AllowAll {
    async fn allow(&self, _: &str, _: &str, _: u32, _: Duration) -> Result<bool, AppError> {
        Ok(true)
    }
}

pub struct MemoryAdmission {
    inner: Mutex<HashMap<(String, String), (u32, tokio::time::Instant)>>,
}

impl Default for MemoryAdmission {
    fn default() -> Self {
        Self { inner: Mutex::new(HashMap::new()) }
    }
}

#[async_trait]
impl Admission for MemoryAdmission {
    async fn allow(
        &self,
        bucket: &str,
        subject: &str,
        limit: u32,
        window: Duration,
    ) -> Result<bool, AppError> {
        let mut guard = self.inner.lock().await;
        let key = (bucket.to_owned(), subject.to_owned());
        let now = tokio::time::Instant::now();
        let entry = guard.entry(key).or_insert((0, now));
        if now.duration_since(entry.1) >= window {
            *entry = (0, now);
        }
        entry.0 += 1;
        Ok(entry.0 <= limit)
    }
}

pub struct TestGuard;

#[async_trait]
impl UrlGuard for TestGuard {
    async fn validate(&self, raw: &str) -> Result<Url, AppError> {
        let url = Url::parse(raw).map_err(|_| crate::domain::DomainError::InvalidUrl)?;
        if !matches!(url.scheme(), "http" | "https")
            || !url.username().is_empty()
            || url.password().is_some()
        {
            return Err(crate::domain::DomainError::InvalidUrl.into());
        }
        if let Some(host) = url.host_str() {
            if crate::domain::host_is_blocked_name(host) {
                return Err(crate::domain::DomainError::BlockedDestination.into());
            }
            if let Ok(ip) = host.parse()
                && !ip_is_public(ip)
            {
                return Err(crate::domain::DomainError::BlockedDestination.into());
            }
        }
        Ok(url)
    }
}

pub struct Registry;

#[async_trait]
impl PlatformRegistry for Registry {
    async fn identify(&self, url: &Url) -> Result<IdentifiedContent, AppError> {
        identify_url(url)?.ok_or_else(|| {
            AppError::Validation("short link could not be resolved in this test".into())
        })
    }
    fn supported(&self) -> Vec<PlatformInfo> {
        PlatformKind::all()
            .iter()
            .map(|platform| PlatformInfo {
                code: platform.code().to_owned(),
                display_name: platform.display_name().to_owned(),
                enabled: true,
            })
            .collect()
    }
}

pub struct FixedMetadata;

#[async_trait]
impl MetadataSource for FixedMetadata {
    async fn inspect(
        &self,
        _: &IdentifiedContent,
        profile: &ProcessingProfile,
    ) -> Result<MediaMetadata, AppError> {
        Ok(MediaMetadata {
            title: Some("fixture".into()),
            duration_seconds: Some(12),
            profiles: vec![*profile, ProcessingProfile::audio_mp3()],
        })
    }
}

pub struct PresentStorage;

pub struct FailingStorage;

#[async_trait]
impl ObjectStorage for FailingStorage {
    async fn upload_file(
        &self,
        _: &str,
        _: &Path,
        _: &str,
        _: &str,
        _: u64,
    ) -> Result<(), AppError> {
        Err(AppError::Dependency("upload failed".into()))
    }
    async fn exists(&self, _: &str) -> Result<bool, AppError> {
        Ok(false)
    }
    async fn delete(&self, _: &str) -> Result<(), AppError> {
        Ok(())
    }
    async fn open(&self, _: &str) -> Result<ObjectBody, AppError> {
        Err(AppError::NotFound)
    }
    async fn ping(&self) -> Result<(), AppError> {
        Ok(())
    }
}

#[async_trait]
impl ObjectStorage for PresentStorage {
    async fn upload_file(
        &self,
        _: &str,
        _: &Path,
        _: &str,
        _: &str,
        _: u64,
    ) -> Result<(), AppError> {
        Ok(())
    }
    async fn exists(&self, _: &str) -> Result<bool, AppError> {
        Ok(true)
    }
    async fn delete(&self, _: &str) -> Result<(), AppError> {
        Ok(())
    }
    async fn open(&self, _: &str) -> Result<ObjectBody, AppError> {
        Err(AppError::NotFound)
    }
    async fn ping(&self) -> Result<(), AppError> {
        Ok(())
    }
}

pub struct CountingExtractor {
    pub calls: AtomicU64,
    pub in_flight: AtomicU64,
    pub max_in_flight: AtomicU64,
}

impl CountingExtractor {
    pub fn new() -> Self {
        Self {
            calls: AtomicU64::new(0),
            in_flight: AtomicU64::new(0),
            max_in_flight: AtomicU64::new(0),
        }
    }
}

#[async_trait]
impl MediaExtractor for CountingExtractor {
    async fn download(
        &self,
        _: &ClaimedJob,
        directory: &Path,
        _: &RuntimeConfig,
        _: tokio::sync::mpsc::Sender<JobProgress>,
    ) -> Result<DownloadedMedia, AppError> {
        let current = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_in_flight.fetch_max(current, Ordering::SeqCst);
        self.calls.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(30)).await;
        tokio::fs::create_dir_all(directory)
            .await
            .map_err(|error| AppError::Dependency(error.to_string()))?;
        let path = directory.join("media.bin");
        tokio::fs::write(&path, b"media-bytes")
            .await
            .map_err(|error| AppError::Dependency(error.to_string()))?;
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
        Ok(DownloadedMedia { path, byte_size: 11 })
    }
}

pub struct NoopProcessor;

#[async_trait]
impl MediaProcessor for NoopProcessor {
    async fn process(
        &self,
        input: &Path,
        _: &ProcessingProfile,
        output: &Path,
        _: Duration,
    ) -> Result<(), AppError> {
        tokio::fs::copy(input, output)
            .await
            .map_err(|error| AppError::Dependency(error.to_string()))?;
        Ok(())
    }
    async fn probe(&self, path: &Path, _: Duration) -> Result<(), AppError> {
        let meta = tokio::fs::metadata(path)
            .await
            .map_err(|error| AppError::Dependency(error.to_string()))?;
        if meta.len() == 0 {
            return Err(AppError::Validation("empty media output".into()));
        }
        Ok(())
    }
}

pub struct MemoryQueue {
    inner: Mutex<std::collections::VecDeque<QueueDelivery>>,
    seq: AtomicU64,
}

impl Default for MemoryQueue {
    fn default() -> Self {
        Self { inner: Mutex::new(std::collections::VecDeque::new()), seq: AtomicU64::new(1) }
    }
}

#[async_trait]
impl JobQueue for MemoryQueue {
    async fn publish(&self, message: QueueMessage) -> Result<(), AppError> {
        let id = self.seq.fetch_add(1, Ordering::SeqCst).to_string();
        self.inner.lock().await.push_back(QueueDelivery { id, message });
        Ok(())
    }
    async fn receive(
        &self,
        _: &str,
        max: usize,
        _: Duration,
    ) -> Result<Vec<QueueDelivery>, AppError> {
        let mut guard = self.inner.lock().await;
        let mut batch = Vec::new();
        while batch.len() < max {
            let Some(item) = guard.pop_front() else { break };
            batch.push(item);
        }
        Ok(batch)
    }
    async fn ack(&self, _: &str) -> Result<(), AppError> {
        Ok(())
    }
    async fn dead_letter(&self, _: QueueMessage, _: &str) -> Result<(), AppError> {
        Ok(())
    }
    async fn depth(&self) -> Result<u64, AppError> {
        Ok(self.inner.lock().await.len() as u64)
    }
    async fn reclaim_stale(
        &self,
        _: &str,
        _: Duration,
        _: usize,
    ) -> Result<Vec<QueueDelivery>, AppError> {
        Ok(Vec::new())
    }
}

pub fn engine_parts(
    blind: bool,
    open_lock: bool,
) -> (Arc<crate::application::Engine>, Arc<MemoryStore>) {
    let store = Arc::new(if blind { MemoryStore::blind() } else { MemoryStore::new() });
    let locks: Arc<dyn Locks> =
        if open_lock { Arc::new(OpenLock) } else { Arc::new(MemoryLocks::default()) };
    let engine = Arc::new(crate::application::Engine::new(
        store.clone(),
        Arc::new(MemoryCache::default()),
        locks,
        Arc::new(TestGuard),
        Arc::new(Registry),
        Arc::new(FixedMetadata),
        Arc::new(AllowAll),
        Arc::new(PresentStorage),
        Arc::new(UuidIds),
        Arc::new(SystemClock),
        Arc::new(RuntimeConfig::default()),
    ));
    (engine, store)
}

pub fn actor(id: Uuid) -> crate::domain::Actor {
    crate::domain::Actor { user_id: id, kind: UserKind::Api }
}

#[tokio::test]
async fn memory_admission_stops_at_the_limit() {
    let admission = MemoryAdmission::default();
    let window = Duration::from_secs(60);
    assert!(admission.allow("rate", "user", 2, window).await.expect("allow"));
    assert!(admission.allow("rate", "user", 2, window).await.expect("allow"));
    assert!(!admission.allow("rate", "user", 2, window).await.expect("allow"));
}
