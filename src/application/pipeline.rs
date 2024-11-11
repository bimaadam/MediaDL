//! One media job, from claim through artifact publication.
//!
//! External tools run as child processes with `kill_on_drop`. Dropping this
//! future on a lost lease stops the download. The lease is extended from the
//! database clock, so a stalled worker cannot keep a job forever.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio::time::MissedTickBehavior;
use uuid::Uuid;

use crate::domain::{
    ArtifactStatus, CachedArtifact, FailureClass, JobStatus, MediaIdentity, MediaKind,
    RetryDecision,
};
use crate::shared::checksum::sha256_file;

use super::error::AppError;
use super::model::*;
use super::ports::*;

pub struct Pipeline {
    store: Arc<dyn Store>,
    cache: Arc<dyn Cache>,
    extractor: Arc<dyn MediaExtractor>,
    processor: Arc<dyn MediaProcessor>,
    storage: Arc<dyn ObjectStorage>,
    ids: Arc<dyn Ids>,
    config: Arc<RuntimeConfig>,
}

impl Pipeline {
    pub fn new(
        store: Arc<dyn Store>,
        cache: Arc<dyn Cache>,
        extractor: Arc<dyn MediaExtractor>,
        processor: Arc<dyn MediaProcessor>,
        storage: Arc<dyn ObjectStorage>,
        ids: Arc<dyn Ids>,
        config: Arc<RuntimeConfig>,
    ) -> Self {
        Self { store, cache, extractor, processor, storage, ids, config }
    }

    pub async fn handle(&self, job_id: Uuid, worker_id: &str) -> Result<HandleResult, AppError> {
        let Some(claimed) = self.store.claim_job(job_id, worker_id, self.config.lease).await?
        else {
            metrics::counter!("media_engine_job_claims_total", "result" => "duplicate")
                .increment(1);
            return Ok(HandleResult::Duplicate);
        };
        let attempt_id = self.ids.new_id();
        self.store
            .record_attempt(NewAttempt {
                id: attempt_id,
                job_id,
                attempt_number: claimed.attempt_count as i32,
                worker_id: worker_id.to_owned(),
            })
            .await?;
        metrics::counter!("media_engine_job_claims_total", "result" => "claimed").increment(1);

        let started = std::time::Instant::now();
        let mut work = std::pin::pin!(self.execute(&claimed, worker_id, attempt_id));
        let mut renew = tokio::time::interval(self.config.lease_renew_every);
        renew.set_missed_tick_behavior(MissedTickBehavior::Skip);
        renew.tick().await;
        let outcome = loop {
            tokio::select! {
                result = &mut work => break result,
                _ = renew.tick() => {
                    let held = self.store.renew_job_lease(job_id, worker_id, self.config.lease).await?;
                    if !held {
                        tracing::warn!(%job_id, "lease lost; stopping this attempt");
                        return Ok(HandleResult::LostLease);
                    }
                }
            }
        };
        metrics::histogram!("media_engine_job_duration_seconds")
            .record(started.elapsed().as_secs_f64());
        outcome
    }

    async fn execute(
        &self,
        claimed: &ClaimedJob,
        worker_id: &str,
        attempt_id: Uuid,
    ) -> Result<HandleResult, AppError> {
        let directory = self.config.tmp_dir.join(claimed.id.to_string());
        let result = self.execute_in(&directory, claimed, worker_id, attempt_id).await;
        if let Err(error) = tokio::fs::remove_dir_all(&directory).await
            && error.kind() != std::io::ErrorKind::NotFound
        {
            tracing::warn!(%error, directory = %directory.display(), "temporary directory cleanup failed");
        }
        result
    }

    async fn execute_in(
        &self,
        directory: &PathBuf,
        claimed: &ClaimedJob,
        worker_id: &str,
        attempt_id: Uuid,
    ) -> Result<HandleResult, AppError> {
        if self.cancelled(claimed.id).await? {
            return self.settle_cancel(claimed.id, worker_id, attempt_id).await;
        }
        tokio::fs::create_dir_all(directory)
            .await
            .map_err(|error| AppError::Dependency(format!("create work directory: {error}")))?;

        let (progress_tx, mut progress_rx) = tokio::sync::mpsc::channel(16);
        let job_id = claimed.id;
        let cache = Arc::clone(&self.cache);
        let progress_task = tokio::spawn(async move {
            while let Some(progress) = progress_rx.recv().await {
                let _ = cache.put_progress(job_id, &progress, Duration::from_secs(3600)).await;
            }
        });

        let downloaded =
            self.extractor.download(claimed, directory, &self.config, progress_tx).await;
        progress_task.abort();
        let downloaded = match downloaded {
            Ok(file) => file,
            Err(error) => return self.fail(claimed, worker_id, attempt_id, error).await,
        };

        if downloaded.byte_size > self.config.max_download_bytes {
            return self.fail(claimed, worker_id, attempt_id, AppError::TooLarge).await;
        }

        let produced = if claimed.profile.needs_ffmpeg() {
            let output = directory.join(format!("out.{}", claimed.profile.container.as_str()));
            if let Err(error) = self
                .processor
                .process(&downloaded.path, &claimed.profile, &output, self.config.tool_timeout)
                .await
            {
                return self.fail(claimed, worker_id, attempt_id, error).await;
            }
            output
        } else {
            downloaded.path
        };

        let content_type = if claimed.profile.kind == MediaKind::Image {
            match image_content_type(&produced).await {
                Ok(value) => value.to_owned(),
                Err(error) => return self.fail(claimed, worker_id, attempt_id, error).await,
            }
        } else if let Err(error) = self.processor.probe(&produced, self.config.tool_timeout).await {
            return self.fail(claimed, worker_id, attempt_id, error).await;
        } else {
            claimed.profile.container.content_type(claimed.profile.kind).to_owned()
        };
        let (checksum, byte_size) = match sha256_file(&produced).await {
            Ok(pair) => pair,
            Err(error) => return self.fail(claimed, worker_id, attempt_id, error).await,
        };
        if byte_size == 0 || byte_size > self.config.max_download_bytes {
            return self.fail(claimed, worker_id, attempt_id, AppError::TooLarge).await;
        }

        let identity = match MediaIdentity::derive(
            claimed.platform,
            content_id(&claimed.content_id)?,
            &claimed.profile,
        ) {
            Ok(identity) => identity,
            Err(error) => return self.fail(claimed, worker_id, attempt_id, error.into()).await,
        };
        let artifact_id = self.ids.new_id();
        let object_key = identity.storage_key(claimed.attempt_count as i32);
        let artifact = match self
            .store
            .reserve_artifact(NewArtifact {
                id: artifact_id,
                media_id: claimed.media_id,
                profile_hash: claimed.profile_hash.clone(),
                object_key: object_key.clone(),
                content_type,
            })
            .await
        {
            Ok(artifact) => artifact,
            Err(error) => return self.fail(claimed, worker_id, attempt_id, error).await,
        };

        if let Err(error) = self
            .storage
            .upload_file(
                &artifact.object_key,
                &produced,
                &artifact.content_type,
                &checksum,
                self.config.multipart_threshold_bytes,
            )
            .await
        {
            let _ = self.storage.delete(&artifact.object_key).await;
            return self.fail(claimed, worker_id, attempt_id, error).await;
        }

        // Uploading is a point of no cancellation. Reach it only after the object exists.
        if !self.store.mark_uploading(claimed.id, worker_id).await? {
            let _ = self.storage.delete(&artifact.object_key).await;
            return Ok(HandleResult::LostLease);
        }

        let completed = self
            .store
            .complete_job(JobCompletion {
                job_id: claimed.id,
                worker_id: worker_id.to_owned(),
                attempt_id,
                artifact_id: artifact.id,
                checksum_sha256: checksum.clone(),
                byte_size: i64::try_from(byte_size).unwrap_or(i64::MAX),
            })
            .await?;
        if !completed {
            let _ = self.storage.delete(&artifact.object_key).await;
            return Ok(HandleResult::LostLease);
        }

        let ready = crate::domain::Artifact {
            id: artifact.id,
            media_id: artifact.media_id,
            profile_hash: artifact.profile_hash,
            version: artifact.version,
            object_key: artifact.object_key,
            checksum_sha256: checksum,
            byte_size: i64::try_from(byte_size).unwrap_or(i64::MAX),
            content_type: artifact.content_type,
            status: ArtifactStatus::Ready,
            created_at: artifact.created_at,
        };
        self.cache
            .put_artifact(
                &identity,
                &CachedArtifact::from_artifact(&ready),
                self.config.artifact_cache_ttl,
            )
            .await?;
        metrics::counter!("media_engine_jobs_total", "result" => "completed", "platform" => claimed.platform.code()).increment(1);
        Ok(HandleResult::Completed { artifact_id: artifact.id })
    }

    async fn cancelled(&self, job_id: Uuid) -> Result<bool, AppError> {
        Ok(self.store.job_status(job_id).await? == Some(JobStatus::Cancelled))
    }

    async fn settle_cancel(
        &self,
        job_id: Uuid,
        worker_id: &str,
        attempt_id: Uuid,
    ) -> Result<HandleResult, AppError> {
        self.store
            .settle_job(JobSettlement::Cancel {
                job_id,
                worker_id: worker_id.to_owned(),
                attempt_id,
            })
            .await?;
        Ok(HandleResult::Failed)
    }

    async fn fail(
        &self,
        claimed: &ClaimedJob,
        worker_id: &str,
        attempt_id: Uuid,
        error: AppError,
    ) -> Result<HandleResult, AppError> {
        if matches!(error, AppError::Cancelled) {
            return self.settle_cancel(claimed.id, worker_id, attempt_id).await;
        }
        let class = classify(&error);
        let reason = error.to_string();
        tracing::warn!(job_id = %claimed.id, platform = %claimed.platform, class = class.as_str(), %reason, "media attempt failed");
        metrics::counter!(
            "media_engine_job_failures_total",
            "platform" => claimed.platform.code(),
            "class" => class.as_str(),
        )
        .increment(1);
        let policy = RetryPolicyView {
            max_attempts: claimed.max_attempts,
            base: self.config.retry_base,
            cap: self.config.retry_cap,
        };
        let decision = policy.decide(claimed.attempt_count, class, claimed.id.as_u128() as u64);
        match decision {
            RetryDecision::Retry { delay } => {
                self.store
                    .settle_job(JobSettlement::Requeue {
                        job_id: claimed.id,
                        worker_id: worker_id.to_owned(),
                        attempt_id,
                        delay,
                        reason,
                    })
                    .await?;
                Ok(HandleResult::RetryScheduled)
            }
            RetryDecision::Permanent => {
                self.store
                    .settle_job(JobSettlement::Fail {
                        job_id: claimed.id,
                        worker_id: worker_id.to_owned(),
                        attempt_id,
                        class,
                        reason,
                    })
                    .await?;
                Ok(HandleResult::Failed)
            }
        }
    }
}

struct RetryPolicyView {
    max_attempts: u32,
    base: Duration,
    cap: Duration,
}

impl RetryPolicyView {
    fn decide(&self, attempt: u32, class: FailureClass, jitter_key: u64) -> RetryDecision {
        crate::domain::RetryPolicy {
            max_attempts: self.max_attempts,
            base: self.base,
            cap: self.cap,
        }
        .decide(attempt, class, jitter_key)
    }
}

fn classify(error: &AppError) -> FailureClass {
    match error {
        AppError::Restricted => FailureClass::Restricted,
        AppError::Cancelled => FailureClass::Cancelled,
        AppError::TooLarge
        | AppError::Validation(_)
        | AppError::Domain(_)
        | AppError::NotFound
        | AppError::Forbidden
        | AppError::Unauthorized => FailureClass::Permanent,
        _ => FailureClass::Transient,
    }
}

async fn image_content_type(path: &std::path::Path) -> Result<&'static str, AppError> {
    use tokio::io::AsyncReadExt;
    let mut file = tokio::fs::File::open(path)
        .await
        .map_err(|error| AppError::Dependency(error.to_string()))?;
    let mut header = [0u8; 4];
    let read =
        file.read(&mut header).await.map_err(|error| AppError::Dependency(error.to_string()))?;
    if read >= 3 && header[0] == 0xFF && header[1] == 0xD8 && header[2] == 0xFF {
        Ok("image/jpeg")
    } else if read >= 4 && &header == b"PK\x03\x04" {
        Ok("application/zip")
    } else {
        Err(AppError::Validation("photo download was not a jpeg".into()))
    }
}

fn content_id(value: &str) -> Result<crate::domain::ContentId, crate::domain::DomainError> {
    crate::domain::ContentId::new(value)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use uuid::Uuid;

    use crate::application::{
        Engine, RuntimeConfig, Store, SubmitCommand, SubmitOutcome, SystemClock, UuidIds,
    };
    use crate::domain::{DeliveryChannel, JobStatus, ProcessingProfile};
    use crate::testkit::{
        AllowAll, CountingExtractor, FailingStorage, FixedMetadata, MemoryCache, MemoryLocks,
        MemoryStore, NoopProcessor, PresentStorage, Registry, TestGuard, actor,
    };

    use super::*;

    fn config() -> RuntimeConfig {
        RuntimeConfig {
            lease_renew_every: Duration::from_secs(3600),
            tmp_dir: std::env::temp_dir().join(format!("media-engine-test-{}", Uuid::now_v7())),
            ..RuntimeConfig::default()
        }
    }

    async fn queued_job(store: Arc<MemoryStore>, storage: Arc<dyn ObjectStorage>) -> Uuid {
        let engine = Engine::new(
            store,
            Arc::new(MemoryCache::default()),
            Arc::new(MemoryLocks::default()),
            Arc::new(TestGuard),
            Arc::new(Registry),
            Arc::new(FixedMetadata),
            Arc::new(AllowAll),
            storage,
            Arc::new(UuidIds),
            Arc::new(SystemClock),
            Arc::new(config()),
        );
        let outcome = engine
            .submit(SubmitCommand {
                url: "https://youtu.be/dQw4w9WgXcQ".into(),
                profile: ProcessingProfile::video_mp4(720).expect("profile"),
                actor: actor(Uuid::now_v7()),
                channel: DeliveryChannel::Http,
            })
            .await
            .expect("submit");
        match outcome {
            SubmitOutcome::Queued { job_id, .. } => job_id,
            other => panic!("expected a queued job, got {other:?}"),
        }
    }

    fn pipeline(
        store: Arc<MemoryStore>,
        storage: Arc<dyn ObjectStorage>,
        extractor: Arc<CountingExtractor>,
        config: RuntimeConfig,
    ) -> Pipeline {
        Pipeline::new(
            store,
            Arc::new(MemoryCache::default()),
            extractor,
            Arc::new(NoopProcessor),
            storage,
            Arc::new(UuidIds),
            Arc::new(config),
        )
    }

    #[tokio::test]
    async fn one_claim_completes_and_the_second_is_a_duplicate() {
        let store = Arc::new(MemoryStore::new());
        let job_id = queued_job(store.clone(), Arc::new(PresentStorage)).await;
        let extractor = Arc::new(CountingExtractor::new());
        let pipeline =
            pipeline(store.clone(), Arc::new(PresentStorage), extractor.clone(), config());
        let first = pipeline.handle(job_id, "worker-a").await.expect("handle");
        assert!(matches!(first, HandleResult::Completed { .. }));
        let second = pipeline.handle(job_id, "worker-b").await.expect("duplicate");
        assert_eq!(second, HandleResult::Duplicate);
        assert_eq!(extractor.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        let status = store.job_status(job_id).await.expect("status");
        assert_eq!(status, Some(JobStatus::Completed));
    }

    #[tokio::test]
    async fn upload_failure_requeues_without_a_second_claim() {
        let store = Arc::new(MemoryStore::new());
        let job_id = queued_job(store.clone(), Arc::new(PresentStorage)).await;
        let pipeline = pipeline(
            store.clone(),
            Arc::new(FailingStorage),
            Arc::new(CountingExtractor::new()),
            config(),
        );
        let result = pipeline.handle(job_id, "worker-a").await.expect("handle");
        assert_eq!(result, HandleResult::RetryScheduled);
        assert_eq!(store.job_status(job_id).await.expect("status"), Some(JobStatus::Queued));
    }

    #[tokio::test]
    async fn lost_lease_drops_the_attempt() {
        let store = Arc::new(MemoryStore::new());
        store.refuse_renew.store(true, std::sync::atomic::Ordering::SeqCst);
        let job_id = queued_job(store.clone(), Arc::new(PresentStorage)).await;
        let mut config = config();
        config.lease_renew_every = Duration::from_millis(10);
        let extractor = Arc::new(CountingExtractor::new());
        let pipeline = pipeline(store, Arc::new(PresentStorage), extractor, config);
        let result = pipeline.handle(job_id, "worker-a").await.expect("handle");
        assert_eq!(result, HandleResult::LostLease);
    }
}
