use std::path::Path;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use tokio::io::AsyncRead;
use url::Url;
use uuid::Uuid;

use crate::domain::{
    Actor, Artifact, DeliveryChannel, IdentifiedContent, Job, JobStatus, MediaIdentity,
    ProcessingProfile,
};

use super::error::AppError;
use super::model::*;

#[async_trait]
pub trait Store: Send + Sync {
    async fn find_ready_artifact(
        &self,
        identity: &MediaIdentity,
    ) -> Result<Option<Artifact>, AppError>;
    async fn mark_artifact_unavailable(&self, id: Uuid) -> Result<(), AppError>;
    async fn find_active_job(&self, identity: &MediaIdentity) -> Result<Option<Job>, AppError>;
    async fn add_subscription(
        &self,
        job_id: Uuid,
        actor: &Actor,
        channel: &DeliveryChannel,
    ) -> Result<(), AppError>;
    async fn commit_new_job(&self, draft: NewJobDraft) -> Result<CommitJob, AppError>;
    async fn get_job(&self, id: Uuid) -> Result<Option<Job>, AppError>;
    async fn get_job_for_user(&self, user_id: Uuid, id: Uuid) -> Result<Option<Job>, AppError>;
    async fn get_media(&self, id: Uuid) -> Result<Option<MediaRecord>, AppError>;
    async fn get_media_for_user(
        &self,
        user_id: Uuid,
        id: Uuid,
    ) -> Result<Option<MediaRecord>, AppError>;
    async fn authorize_artifact(
        &self,
        user_id: Uuid,
        artifact_id: Uuid,
    ) -> Result<Option<Artifact>, AppError>;
    async fn cancel_job(&self, id: Uuid, actor: Uuid) -> Result<Option<CancelOutcome>, AppError>;
    async fn claim_job(
        &self,
        id: Uuid,
        worker: &str,
        lease: Duration,
    ) -> Result<Option<ClaimedJob>, AppError>;
    async fn renew_job_lease(
        &self,
        id: Uuid,
        worker: &str,
        lease: Duration,
    ) -> Result<bool, AppError>;
    async fn job_status(&self, id: Uuid) -> Result<Option<JobStatus>, AppError>;
    async fn record_attempt(&self, attempt: NewAttempt) -> Result<(), AppError>;
    async fn mark_uploading(&self, job_id: Uuid, worker: &str) -> Result<bool, AppError>;
    async fn reserve_artifact(&self, draft: NewArtifact) -> Result<Artifact, AppError>;
    async fn complete_job(&self, completion: JobCompletion) -> Result<bool, AppError>;
    async fn settle_job(&self, settlement: JobSettlement) -> Result<(), AppError>;
    async fn recover_expired_leases(&self, limit: i64) -> Result<Vec<Uuid>, AppError>;
    async fn expire_stale_jobs(&self, older_than: Duration, limit: i64) -> Result<u64, AppError>;
    async fn list_stale_uploads(
        &self,
        older_than: Duration,
        limit: i64,
    ) -> Result<Vec<Artifact>, AppError>;
    async fn delete_artifact_row(&self, id: Uuid) -> Result<(), AppError>;
    async fn upsert_external_user(
        &self,
        kind: crate::domain::UserKind,
        external_ref: &str,
        display_name: Option<&str>,
    ) -> Result<UserRecord, AppError>;
    async fn find_user_by_api_key_hash(&self, hash: &str) -> Result<Option<UserRecord>, AppError>;
    async fn create_api_user(&self, name: &str, key_hash: &str) -> Result<UserRecord, AppError>;
    async fn audit(&self, entry: AuditEntry) -> Result<(), AppError>;
    async fn idempotency_get(
        &self,
        user_id: Uuid,
        key: &str,
    ) -> Result<Option<IdempotencyRecord>, AppError>;
    async fn idempotency_put(&self, record: NewIdempotency) -> Result<IdempotencyPut, AppError>;
    async fn idempotency_purge(&self, now: DateTime<Utc>) -> Result<u64, AppError>;
    async fn save_metadata(&self, media_id: Uuid, meta: &MediaMetadata) -> Result<(), AppError>;
    async fn fetch_outbox(&self, limit: i64) -> Result<Vec<OutboxEvent>, AppError>;
    async fn mark_outbox_published(&self, ids: &[i64]) -> Result<(), AppError>;
    async fn heartbeat(&self, worker_id: &str, role: &str) -> Result<(), AppError>;
    async fn platforms(&self) -> Result<Vec<PlatformInfo>, AppError>;
    async fn ping(&self) -> Result<(), AppError>;
    async fn pending_notifications(&self, limit: i64)
    -> Result<Vec<PendingNotification>, AppError>;
    async fn pending_status_notices(&self, _limit: i64) -> Result<Vec<StatusNotice>, AppError> {
        Ok(Vec::new())
    }
    async fn mark_notified(&self, subscription_id: Uuid, status: &str) -> Result<(), AppError>;
    async fn save_telegram_delivery(
        &self,
        bot_id: &str,
        file: &TelegramFileRef,
    ) -> Result<(), AppError>;
    async fn find_telegram_delivery(
        &self,
        bot_id: &str,
        artifact_id: Uuid,
    ) -> Result<Option<TelegramFileRef>, AppError>;
    async fn invalidate_telegram_delivery(
        &self,
        bot_id: &str,
        artifact_id: Uuid,
    ) -> Result<(), AppError>;
    async fn media_id_for_identity(
        &self,
        identity: &MediaIdentity,
    ) -> Result<Option<Uuid>, AppError>;

    async fn user_job_count(&self, _user_id: Uuid) -> Result<u64, AppError> {
        Ok(0)
    }
}

#[async_trait]
pub trait Cache: Send + Sync {
    async fn get_artifact(
        &self,
        identity: &MediaIdentity,
    ) -> Result<Option<crate::domain::CachedArtifact>, AppError>;
    async fn put_artifact(
        &self,
        identity: &MediaIdentity,
        artifact: &crate::domain::CachedArtifact,
        ttl: Duration,
    ) -> Result<(), AppError>;
    async fn invalidate_artifact(&self, identity: &MediaIdentity) -> Result<(), AppError>;
    async fn get_metadata(
        &self,
        platform: crate::domain::PlatformKind,
        content_id: &str,
    ) -> Result<Option<MediaMetadata>, AppError>;
    async fn put_metadata(
        &self,
        platform: crate::domain::PlatformKind,
        content_id: &str,
        meta: &MediaMetadata,
        ttl: Duration,
    ) -> Result<(), AppError>;
    async fn get_telegram_file(
        &self,
        bot_id: &str,
        identity: &MediaIdentity,
    ) -> Result<Option<TelegramFileRef>, AppError>;
    async fn put_telegram_file(
        &self,
        bot_id: &str,
        identity: &MediaIdentity,
        file: &TelegramFileRef,
        ttl: Duration,
    ) -> Result<(), AppError>;
    async fn invalidate_telegram_file(
        &self,
        bot_id: &str,
        identity: &MediaIdentity,
    ) -> Result<(), AppError>;
    async fn put_progress(
        &self,
        job_id: Uuid,
        progress: &JobProgress,
        ttl: Duration,
    ) -> Result<(), AppError>;
    async fn get_progress(&self, job_id: Uuid) -> Result<Option<JobProgress>, AppError>;
    async fn put_selection(
        &self,
        token: &str,
        selection: &FormatSelection,
        ttl: Duration,
    ) -> Result<(), AppError>;
    async fn get_selection(&self, token: &str) -> Result<Option<FormatSelection>, AppError>;
    async fn ping(&self) -> Result<(), AppError>;
}

#[async_trait]
pub trait Locks: Send + Sync {
    async fn try_acquire(&self, key: &str, owner: &str, ttl: Duration) -> Result<bool, AppError>;
    async fn renew(&self, key: &str, owner: &str, ttl: Duration) -> Result<bool, AppError>;
    async fn release(&self, key: &str, owner: &str) -> Result<bool, AppError>;
}

#[async_trait]
pub trait JobQueue: Send + Sync {
    async fn publish(&self, message: QueueMessage) -> Result<(), AppError>;
    async fn receive(
        &self,
        consumer: &str,
        max: usize,
        block: Duration,
    ) -> Result<Vec<QueueDelivery>, AppError>;
    async fn ack(&self, delivery_id: &str) -> Result<(), AppError>;
    async fn dead_letter(&self, message: QueueMessage, reason: &str) -> Result<(), AppError>;
    async fn depth(&self) -> Result<u64, AppError>;
    async fn reclaim_stale(
        &self,
        consumer: &str,
        min_idle: Duration,
        max: usize,
    ) -> Result<Vec<QueueDelivery>, AppError>;
}

#[async_trait]
pub trait Admission: Send + Sync {
    /// Returns true when the subject is under the limit.
    async fn allow(
        &self,
        bucket: &str,
        subject: &str,
        limit: u32,
        window: Duration,
    ) -> Result<bool, AppError>;
}

#[async_trait]
pub trait UrlGuard: Send + Sync {
    async fn validate(&self, raw: &str) -> Result<Url, AppError>;
}

#[async_trait]
pub trait PlatformRegistry: Send + Sync {
    async fn identify(&self, url: &Url) -> Result<IdentifiedContent, AppError>;
    fn supported(&self) -> Vec<PlatformInfo>;
}

#[async_trait]
pub trait MetadataSource: Send + Sync {
    async fn inspect(
        &self,
        content: &IdentifiedContent,
        profile: &ProcessingProfile,
    ) -> Result<MediaMetadata, AppError>;

    /// Public photo attachments, with no video stream on the same page.
    async fn has_public_photos(&self, _content: &IdentifiedContent) -> Result<bool, AppError> {
        Ok(false)
    }
}

#[async_trait]
pub trait MediaExtractor: Send + Sync {
    async fn download(
        &self,
        claimed: &ClaimedJob,
        directory: &Path,
        limits: &RuntimeConfig,
        progress: tokio::sync::mpsc::Sender<JobProgress>,
    ) -> Result<DownloadedMedia, AppError>;
}

#[async_trait]
pub trait MediaProcessor: Send + Sync {
    async fn process(
        &self,
        input: &Path,
        profile: &ProcessingProfile,
        output: &Path,
        timeout: Duration,
    ) -> Result<(), AppError>;
    async fn probe(&self, path: &Path, timeout: Duration) -> Result<(), AppError>;
}

pub struct ObjectBody {
    pub content_type: String,
    pub content_length: Option<u64>,
    pub reader: Box<dyn AsyncRead + Unpin + Send>,
}

#[async_trait]
pub trait ObjectStorage: Send + Sync {
    async fn upload_file(
        &self,
        key: &str,
        path: &Path,
        content_type: &str,
        checksum_hex: &str,
        multipart_threshold: u64,
    ) -> Result<(), AppError>;
    async fn exists(&self, key: &str) -> Result<bool, AppError>;
    async fn delete(&self, key: &str) -> Result<(), AppError>;
    async fn open(&self, key: &str) -> Result<ObjectBody, AppError>;
    async fn ping(&self) -> Result<(), AppError>;
}

pub trait Clock: Send + Sync {
    fn now(&self) -> DateTime<Utc>;
}

pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

pub trait Ids: Send + Sync {
    fn new_id(&self) -> Uuid;
}

pub struct UuidIds;

impl Ids for UuidIds {
    fn new_id(&self) -> Uuid {
        Uuid::now_v7()
    }
}

#[async_trait]
pub trait TelegramSender: Send + Sync {
    async fn send_text(&self, chat_id: i64, text: &str) -> Result<(), AppError>;
    async fn send_file(
        &self,
        chat_id: i64,
        path: &Path,
        kind: &str,
        caption: &str,
    ) -> Result<TelegramFileRef, AppError>;
    async fn send_file_id(
        &self,
        chat_id: i64,
        kind: &str,
        file_id: &str,
        caption: &str,
    ) -> Result<(), AppError>;
    async fn send_file_menu(
        &self,
        chat_id: i64,
        path: &Path,
        kind: &str,
        caption: &str,
        choices: &[TelegramChoice],
    ) -> Result<TelegramFileRef, AppError> {
        let _ = choices;
        self.send_file(chat_id, path, kind, caption).await
    }
    async fn send_file_id_menu(
        &self,
        chat_id: i64,
        kind: &str,
        file_id: &str,
        caption: &str,
        choices: &[TelegramChoice],
    ) -> Result<(), AppError> {
        let _ = choices;
        self.send_file_id(chat_id, kind, file_id, caption).await
    }
    async fn edit_text(&self, chat_id: i64, message_id: i32, text: &str) -> Result<(), AppError>;
}
