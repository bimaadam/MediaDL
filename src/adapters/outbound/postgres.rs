//! PostgreSQL is the source of truth for users, jobs, artifacts, and the outbox.
//! Creating a job and claiming the active-media slot happen in one transaction.

use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

use crate::application::{
    AppError, AuditEntry, CancelOutcome, ClaimedJob, CommitJob, IdempotencyPut, IdempotencyRecord,
    JobCompletion, JobSettlement, MediaRecord, NewArtifact, NewAttempt, NewIdempotency,
    NewJobDraft, PendingNotification, PlatformInfo, StatusNotice, Store, TelegramFileRef,
    UserRecord,
};
use crate::domain::{
    Actor, Artifact, ArtifactStatus, DeliveryChannel, FailureClass, Job, JobStatus, MediaIdentity,
    PlatformKind, ProcessingProfile, UserKind,
};

#[derive(Clone)]
pub struct PostgresStore {
    pool: PgPool,
}

impl PostgresStore {
    pub async fn connect(url: &str, max_connections: u32) -> Result<Self, AppError> {
        let pool = PgPoolOptions::new()
            .max_connections(max_connections.max(1))
            .connect(url)
            .await
            .map_err(db_err)?;
        Ok(Self { pool })
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub fn connect_pool(pool: PgPool) -> Self {
        Self { pool }
    }
}

fn db_err(error: sqlx::Error) -> AppError {
    if error.as_database_error().is_some_and(|db| db.is_unique_violation()) {
        return AppError::Conflict;
    }
    AppError::Dependency(format!("database: {error}"))
}

fn secs(duration: Duration) -> i32 {
    i32::try_from(duration.as_secs()).unwrap_or(i32::MAX)
}

const JOB_SELECT: &str = r#"
    j.id, j.media_id, m.platform_id, m.canonical_content_id AS content_id,
    COALESCE(m.canonical_url, '') AS canonical_url, j.profile, j.profile_hash, j.status,
    j.attempt_count, j.max_attempts, j.failure_class, j.failure_reason, j.artifact_id,
    j.requested_by, j.created_at, j.updated_at
"#;

/// `JOB_SELECT` is a fixed column list in this module. Every value is a bind parameter.
fn static_sql(sql: String) -> sqlx::AssertSqlSafe<String> {
    sqlx::AssertSqlSafe(sql)
}

#[derive(sqlx::FromRow)]
struct JobRow {
    id: Uuid,
    media_id: Uuid,
    platform_id: i16,
    content_id: String,
    canonical_url: String,
    profile: serde_json::Value,
    profile_hash: String,
    status: String,
    attempt_count: i32,
    max_attempts: i32,
    failure_class: Option<String>,
    failure_reason: Option<String>,
    artifact_id: Option<Uuid>,
    requested_by: Option<Uuid>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

impl JobRow {
    fn into_job(self) -> Result<Job, AppError> {
        let profile = profile_from_json(&self.profile)?;
        let platform = PlatformKind::from_db_id(self.platform_id)
            .ok_or_else(|| AppError::Dependency("unknown platform".into()))?;
        let identity = MediaIdentity::derive(
            platform,
            crate::domain::ContentId::new(self.content_id)?,
            &profile,
        )?;
        if identity.profile_hash().as_str() != self.profile_hash {
            return Err(AppError::Dependency(
                "stored profile hash does not match the profile".into(),
            ));
        }
        Ok(Job {
            id: self.id,
            media_id: self.media_id,
            identity,
            profile,
            status: JobStatus::parse(&self.status)
                .ok_or_else(|| AppError::Dependency("unknown job status".into()))?,
            attempt_count: u32::try_from(self.attempt_count).unwrap_or(0),
            max_attempts: u32::try_from(self.max_attempts).unwrap_or(1),
            failure_class: parse_failure(&self.failure_class),
            failure_reason: self.failure_reason,
            artifact_id: self.artifact_id,
            requested_by: self.requested_by,
            created_at: self.created_at,
            updated_at: self.updated_at,
        })
    }

    fn into_claimed(self) -> Result<ClaimedJob, AppError> {
        let job = self.into_job()?;
        Ok(ClaimedJob {
            id: job.id,
            media_id: job.media_id,
            platform: job.identity.platform(),
            content_id: job.identity.content_id().as_str().to_owned(),
            canonical_url: String::new(),
            profile: job.profile,
            profile_hash: job.identity.profile_hash().as_str().to_owned(),
            attempt_count: job.attempt_count,
            max_attempts: job.max_attempts,
            status: job.status,
        })
    }
}

fn profile_from_json(value: &serde_json::Value) -> Result<ProcessingProfile, AppError> {
    let profile: ProcessingProfile = serde_json::from_value(value.clone())
        .map_err(|error| AppError::Dependency(format!("profile json: {error}")))?;
    profile.validate()?;
    Ok(profile)
}

fn parse_failure(value: &Option<String>) -> Option<FailureClass> {
    match value.as_deref() {
        Some("transient") => Some(FailureClass::Transient),
        Some("permanent") => Some(FailureClass::Permanent),
        Some("restricted") => Some(FailureClass::Restricted),
        Some("cancelled") => Some(FailureClass::Cancelled),
        _ => None,
    }
}

#[derive(sqlx::FromRow)]
struct ArtifactRow {
    id: Uuid,
    media_id: Uuid,
    profile_hash: String,
    version: i32,
    object_key: String,
    checksum_sha256: String,
    byte_size: i64,
    content_type: String,
    status: String,
    created_at: DateTime<Utc>,
}

impl ArtifactRow {
    fn into_artifact(self) -> Result<Artifact, AppError> {
        Ok(Artifact {
            id: self.id,
            media_id: self.media_id,
            profile_hash: self.profile_hash,
            version: self.version,
            object_key: self.object_key,
            checksum_sha256: self.checksum_sha256,
            byte_size: self.byte_size,
            content_type: self.content_type,
            status: ArtifactStatus::parse(&self.status)
                .ok_or_else(|| AppError::Dependency("unknown artifact status".into()))?,
            created_at: self.created_at,
        })
    }
}

#[async_trait]
impl Store for PostgresStore {
    async fn find_ready_artifact(
        &self,
        identity: &MediaIdentity,
    ) -> Result<Option<Artifact>, AppError> {
        let row = sqlx::query_as::<_, ArtifactRow>(
            r#"
            SELECT a.id, a.media_id, a.profile_hash, a.version, a.object_key, a.checksum_sha256,
                   a.byte_size, a.content_type, a.status, a.created_at
            FROM media_artifacts a
            JOIN media m ON m.id = a.media_id
            WHERE m.platform_id = $1 AND m.canonical_content_id = $2
              AND a.profile_hash = $3 AND a.status = 'ready'
            LIMIT 1
            "#,
        )
        .bind(identity.platform().db_id())
        .bind(identity.content_id().as_str())
        .bind(identity.profile_hash().as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(db_err)?;
        row.map(ArtifactRow::into_artifact).transpose()
    }

    async fn mark_artifact_unavailable(&self, id: Uuid) -> Result<(), AppError> {
        sqlx::query("UPDATE media_artifacts SET status = 'deleted' WHERE id = $1")
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(db_err)?;
        Ok(())
    }

    async fn find_active_job(&self, identity: &MediaIdentity) -> Result<Option<Job>, AppError> {
        let sql = format!(
            r#"
            SELECT {JOB_SELECT}
            FROM active_media_jobs active
            JOIN download_jobs j ON j.id = active.job_id
            JOIN media m ON m.id = j.media_id
            WHERE m.platform_id = $1 AND m.canonical_content_id = $2 AND active.profile_hash = $3
              AND j.status IN ('pending', 'queued', 'processing', 'uploading')
            "#
        );
        let row = sqlx::query_as::<_, JobRow>(static_sql(sql))
            .bind(identity.platform().db_id())
            .bind(identity.content_id().as_str())
            .bind(identity.profile_hash().as_str())
            .fetch_optional(&self.pool)
            .await
            .map_err(db_err)?;
        row.map(JobRow::into_job).transpose()
    }

    async fn add_subscription(
        &self,
        job_id: Uuid,
        actor: &Actor,
        channel: &DeliveryChannel,
    ) -> Result<(), AppError> {
        let mut tx = self.pool.begin().await.map_err(db_err)?;
        insert_subscription(&mut tx, job_id, actor, channel).await?;
        tx.commit().await.map_err(db_err)?;
        Ok(())
    }

    async fn commit_new_job(&self, draft: NewJobDraft) -> Result<CommitJob, AppError> {
        let mut tx = self.pool.begin().await.map_err(db_err)?;
        let media_id: Uuid = sqlx::query_scalar(
            r#"
            INSERT INTO media (id, platform_id, canonical_content_id, canonical_url)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (platform_id, canonical_content_id)
            DO UPDATE SET updated_at = now(),
                          canonical_url = COALESCE(media.canonical_url, EXCLUDED.canonical_url)
            RETURNING id
            "#,
        )
        .bind(Uuid::now_v7())
        .bind(draft.platform.db_id())
        .bind(&draft.content_id)
        .bind(&draft.canonical_url)
        .fetch_one(&mut *tx)
        .await
        .map_err(db_err)?;

        let profile_json = serde_json::to_value(draft.profile)
            .map_err(|error| AppError::Dependency(error.to_string()))?;
        sqlx::query(
            r#"
            INSERT INTO download_jobs
                (id, media_id, profile_hash, profile, status, max_attempts, requested_by)
            VALUES ($1, $2, $3, $4, 'queued', $5, $6)
            "#,
        )
        .bind(draft.job_id)
        .bind(media_id)
        .bind(&draft.profile_hash)
        .bind(profile_json)
        .bind(draft.max_attempts as i32)
        .bind(draft.actor.user_id)
        .execute(&mut *tx)
        .await
        .map_err(db_err)?;

        let won: Option<Uuid> = sqlx::query_scalar(
            r#"
            INSERT INTO active_media_jobs (media_id, profile_hash, job_id)
            VALUES ($1, $2, $3)
            ON CONFLICT (media_id, profile_hash) DO NOTHING
            RETURNING job_id
            "#,
        )
        .bind(media_id)
        .bind(&draft.profile_hash)
        .bind(draft.job_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db_err)?;

        if won.is_none() {
            sqlx::query("DELETE FROM download_jobs WHERE id = $1")
                .bind(draft.job_id)
                .execute(&mut *tx)
                .await
                .map_err(db_err)?;
            let existing: Uuid = sqlx::query_scalar(
                "SELECT job_id FROM active_media_jobs WHERE media_id = $1 AND profile_hash = $2",
            )
            .bind(media_id)
            .bind(&draft.profile_hash)
            .fetch_one(&mut *tx)
            .await
            .map_err(db_err)?;
            insert_subscription(&mut tx, existing, &draft.actor, &draft.channel).await?;
            tx.commit().await.map_err(db_err)?;
            return Ok(CommitJob::Attached { job_id: existing, media_id });
        }

        insert_subscription(&mut tx, draft.job_id, &draft.actor, &draft.channel).await?;
        insert_outbox(&mut tx, draft.job_id, "job.queued").await?;
        tx.commit().await.map_err(db_err)?;
        Ok(CommitJob::Created { job_id: draft.job_id, media_id })
    }

    async fn get_job(&self, id: Uuid) -> Result<Option<Job>, AppError> {
        self.load_job(id, None).await
    }

    async fn get_job_for_user(&self, user_id: Uuid, id: Uuid) -> Result<Option<Job>, AppError> {
        self.load_job(id, Some(user_id)).await
    }

    async fn get_media(&self, id: Uuid) -> Result<Option<MediaRecord>, AppError> {
        load_media(&self.pool, id, None).await
    }

    async fn get_media_for_user(
        &self,
        user_id: Uuid,
        id: Uuid,
    ) -> Result<Option<MediaRecord>, AppError> {
        load_media(&self.pool, id, Some(user_id)).await
    }

    async fn authorize_artifact(
        &self,
        user_id: Uuid,
        artifact_id: Uuid,
    ) -> Result<Option<Artifact>, AppError> {
        let row = sqlx::query_as::<_, ArtifactRow>(
            r#"
            SELECT a.id, a.media_id, a.profile_hash, a.version, a.object_key, a.checksum_sha256,
                   a.byte_size, a.content_type, a.status, a.created_at
            FROM media_artifacts a
            WHERE a.id = $1
              AND EXISTS (
                SELECT 1 FROM download_jobs j
                WHERE j.media_id = a.media_id AND j.profile_hash = a.profile_hash
                  AND (j.requested_by = $2 OR EXISTS (
                    SELECT 1 FROM job_subscriptions s WHERE s.job_id = j.id AND s.user_id = $2
                  ))
              )
            "#,
        )
        .bind(artifact_id)
        .bind(user_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(db_err)?;
        row.map(ArtifactRow::into_artifact).transpose()
    }

    async fn cancel_job(&self, id: Uuid, actor: Uuid) -> Result<Option<CancelOutcome>, AppError> {
        let Some(job) = self.load_job(id, Some(actor)).await? else {
            return Ok(None);
        };
        if job.status.transition(JobStatus::Cancelled).is_err() {
            return Ok(Some(CancelOutcome::NotCancellable));
        }
        let mut tx = self.pool.begin().await.map_err(db_err)?;
        let updated = sqlx::query(
            r#"
            UPDATE download_jobs
            SET status = 'cancelled', locked_by = NULL, lease_until = NULL, updated_at = now()
            WHERE id = $1 AND status = $2
            "#,
        )
        .bind(id)
        .bind(job.status.as_str())
        .execute(&mut *tx)
        .await
        .map_err(db_err)?;
        if updated.rows_affected() == 0 {
            tx.rollback().await.map_err(db_err)?;
            return Ok(Some(CancelOutcome::NotCancellable));
        }
        sqlx::query("DELETE FROM active_media_jobs WHERE job_id = $1")
            .bind(id)
            .execute(&mut *tx)
            .await
            .map_err(db_err)?;
        tx.commit().await.map_err(db_err)?;
        Ok(Some(CancelOutcome::Cancelled))
    }

    async fn claim_job(
        &self,
        id: Uuid,
        worker: &str,
        lease: Duration,
    ) -> Result<Option<ClaimedJob>, AppError> {
        let sql = format!(
            r#"
            UPDATE download_jobs j
            SET status = 'processing',
                attempt_count = j.attempt_count + 1,
                locked_by = $2,
                lease_until = now() + make_interval(secs => $3::integer),
                updated_at = now()
            FROM media m
            WHERE j.id = $1 AND j.media_id = m.id
              AND j.status IN ('pending', 'queued')
              AND (j.lease_until IS NULL OR j.lease_until <= now())
              AND (j.next_attempt_at IS NULL OR j.next_attempt_at <= now())
            RETURNING {JOB_SELECT}
            "#
        );
        let row = sqlx::query_as::<_, JobRow>(static_sql(sql))
            .bind(id)
            .bind(worker)
            .bind(secs(lease))
            .fetch_optional(&self.pool)
            .await
            .map_err(db_err)?;
        match row {
            None => Ok(None),
            Some(row) => {
                let url = row.canonical_url.clone();
                let mut claimed = row.into_claimed()?;
                claimed.canonical_url = url;
                Ok(Some(claimed))
            }
        }
    }

    async fn renew_job_lease(
        &self,
        id: Uuid,
        worker: &str,
        lease: Duration,
    ) -> Result<bool, AppError> {
        let updated = sqlx::query(
            r#"
            UPDATE download_jobs
            SET lease_until = now() + make_interval(secs => $3::integer), updated_at = now()
            WHERE id = $1 AND locked_by = $2 AND status IN ('processing', 'uploading')
            "#,
        )
        .bind(id)
        .bind(worker)
        .bind(secs(lease))
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(updated.rows_affected() == 1)
    }

    async fn job_status(&self, id: Uuid) -> Result<Option<JobStatus>, AppError> {
        let status: Option<String> =
            sqlx::query_scalar("SELECT status FROM download_jobs WHERE id = $1")
                .bind(id)
                .fetch_optional(&self.pool)
                .await
                .map_err(db_err)?;
        Ok(status.and_then(|status| JobStatus::parse(&status)))
    }

    async fn record_attempt(&self, attempt: NewAttempt) -> Result<(), AppError> {
        sqlx::query(
            r#"
            INSERT INTO processing_attempts (id, job_id, attempt_number, worker_id)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (job_id, attempt_number) DO NOTHING
            "#,
        )
        .bind(attempt.id)
        .bind(attempt.job_id)
        .bind(attempt.attempt_number)
        .bind(attempt.worker_id)
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(())
    }

    async fn mark_uploading(&self, job_id: Uuid, worker: &str) -> Result<bool, AppError> {
        let updated = sqlx::query(
            r#"
            UPDATE download_jobs
            SET status = 'uploading', updated_at = now()
            WHERE id = $1 AND locked_by = $2 AND status = 'processing'
            "#,
        )
        .bind(job_id)
        .bind(worker)
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(updated.rows_affected() == 1)
    }

    async fn reserve_artifact(&self, draft: NewArtifact) -> Result<Artifact, AppError> {
        let row = sqlx::query_as::<_, ArtifactRow>(
            r#"
            INSERT INTO media_artifacts
                (id, media_id, profile_hash, version, object_key, checksum_sha256, byte_size, content_type, status)
            VALUES (
                $1, $2, $3,
                COALESCE((SELECT MAX(version) FROM media_artifacts WHERE media_id = $2 AND profile_hash = $3), 0) + 1,
                $4, $5, 0, $6, 'uploading'
            )
            RETURNING id, media_id, profile_hash, version, object_key, checksum_sha256, byte_size, content_type, status, created_at
            "#,
        )
        .bind(draft.id)
        .bind(draft.media_id)
        .bind(&draft.profile_hash)
        .bind(&draft.object_key)
        .bind("0".repeat(64))
        .bind(&draft.content_type)
        .fetch_one(&self.pool)
        .await
        .map_err(db_err)?;
        row.into_artifact()
    }

    async fn complete_job(&self, completion: JobCompletion) -> Result<bool, AppError> {
        let mut tx = self.pool.begin().await.map_err(db_err)?;
        let artifact = sqlx::query(
            r#"
            UPDATE media_artifacts
            SET status = 'ready', checksum_sha256 = $2, byte_size = $3
            WHERE id = $1 AND status = 'uploading'
            "#,
        )
        .bind(completion.artifact_id)
        .bind(&completion.checksum_sha256)
        .bind(completion.byte_size)
        .execute(&mut *tx)
        .await
        .map_err(db_err)?;
        let job = sqlx::query(
            r#"
            UPDATE download_jobs
            SET status = 'completed', artifact_id = $2, locked_by = NULL, lease_until = NULL, updated_at = now()
            WHERE id = $1 AND locked_by = $3 AND status = 'uploading'
            "#,
        )
        .bind(completion.job_id)
        .bind(completion.artifact_id)
        .bind(&completion.worker_id)
        .execute(&mut *tx)
        .await
        .map_err(db_err)?;
        if artifact.rows_affected() != 1 || job.rows_affected() != 1 {
            tx.rollback().await.map_err(db_err)?;
            return Ok(false);
        }
        sqlx::query("DELETE FROM active_media_jobs WHERE job_id = $1")
            .bind(completion.job_id)
            .execute(&mut *tx)
            .await
            .map_err(db_err)?;
        finish_attempt(&mut tx, completion.attempt_id, "completed", None).await?;
        insert_outbox(&mut tx, completion.job_id, "job.completed").await?;
        tx.commit().await.map_err(db_err)?;
        Ok(true)
    }

    async fn settle_job(&self, settlement: JobSettlement) -> Result<(), AppError> {
        let mut tx = self.pool.begin().await.map_err(db_err)?;
        match settlement {
            JobSettlement::Requeue { job_id, worker_id, attempt_id, delay, reason } => {
                sqlx::query(
                    r#"
                    UPDATE download_jobs
                    SET status = 'queued', locked_by = NULL, lease_until = NULL,
                        next_attempt_at = now() + make_interval(secs => $3::integer),
                        failure_class = 'transient', failure_reason = $4, updated_at = now()
                    WHERE id = $1 AND locked_by = $2 AND status = 'processing'
                    "#,
                )
                .bind(job_id)
                .bind(&worker_id)
                .bind(secs(delay))
                .bind(&reason)
                .execute(&mut *tx)
                .await
                .map_err(db_err)?;
                finish_attempt(&mut tx, attempt_id, "requeued", Some(&reason)).await?;
                insert_outbox(&mut tx, job_id, "job.queued").await?;
            }
            JobSettlement::Fail { job_id, worker_id, attempt_id, class, reason } => {
                sqlx::query(
                    r#"
                    UPDATE download_jobs
                    SET status = 'failed', locked_by = NULL, lease_until = NULL,
                        failure_class = $3, failure_reason = $4, updated_at = now()
                    WHERE id = $1 AND locked_by = $2 AND status IN ('processing', 'uploading', 'queued')
                    "#,
                )
                .bind(job_id)
                .bind(&worker_id)
                .bind(class.as_str())
                .bind(&reason)
                .execute(&mut *tx)
                .await
                .map_err(db_err)?;
                sqlx::query("DELETE FROM active_media_jobs WHERE job_id = $1")
                    .bind(job_id)
                    .execute(&mut *tx)
                    .await
                    .map_err(db_err)?;
                finish_attempt(&mut tx, attempt_id, "failed", Some(&reason)).await?;
                insert_outbox(&mut tx, job_id, "job.failed").await?;
            }
            JobSettlement::Cancel { job_id, worker_id, attempt_id } => {
                sqlx::query(
                    r#"
                    UPDATE download_jobs
                    SET status = 'cancelled', locked_by = NULL, lease_until = NULL,
                        failure_class = 'cancelled', updated_at = now()
                    WHERE id = $1 AND (locked_by = $2 OR locked_by IS NULL)
                      AND status IN ('pending', 'queued', 'processing')
                    "#,
                )
                .bind(job_id)
                .bind(&worker_id)
                .execute(&mut *tx)
                .await
                .map_err(db_err)?;
                sqlx::query("DELETE FROM active_media_jobs WHERE job_id = $1")
                    .bind(job_id)
                    .execute(&mut *tx)
                    .await
                    .map_err(db_err)?;
                finish_attempt(&mut tx, attempt_id, "cancelled", None).await?;
            }
        }
        tx.commit().await.map_err(db_err)?;
        Ok(())
    }

    async fn recover_expired_leases(&self, limit: i64) -> Result<Vec<Uuid>, AppError> {
        let mut tx = self.pool.begin().await.map_err(db_err)?;
        let rows: Vec<(Uuid, String, i32, i32)> = sqlx::query_as(
            r#"
            SELECT id, status, attempt_count, max_attempts
            FROM download_jobs
            WHERE status IN ('processing', 'uploading', 'queued')
              AND lease_until IS NOT NULL AND lease_until <= now()
            ORDER BY lease_until
            LIMIT $1
            FOR UPDATE SKIP LOCKED
            "#,
        )
        .bind(limit)
        .fetch_all(&mut *tx)
        .await
        .map_err(db_err)?;
        let mut recovered = Vec::new();
        for (id, _status, attempts, max_attempts) in rows {
            if attempts >= max_attempts {
                sqlx::query(
                    r#"
                    UPDATE download_jobs
                    SET status = 'failed', failure_class = 'permanent',
                        failure_reason = 'abandoned after the attempt budget',
                        locked_by = NULL, lease_until = NULL, updated_at = now()
                    WHERE id = $1
                    "#,
                )
                .bind(id)
                .execute(&mut *tx)
                .await
                .map_err(db_err)?;
                sqlx::query("DELETE FROM active_media_jobs WHERE job_id = $1")
                    .bind(id)
                    .execute(&mut *tx)
                    .await
                    .map_err(db_err)?;
            } else {
                sqlx::query(
                    r#"
                    UPDATE download_jobs
                    SET status = 'queued', locked_by = NULL, lease_until = NULL, updated_at = now()
                    WHERE id = $1
                    "#,
                )
                .bind(id)
                .execute(&mut *tx)
                .await
                .map_err(db_err)?;
                insert_outbox(&mut tx, id, "job.queued").await?;
                recovered.push(id);
            }
        }
        tx.commit().await.map_err(db_err)?;
        Ok(recovered)
    }

    async fn expire_stale_jobs(&self, older_than: Duration, limit: i64) -> Result<u64, AppError> {
        let mut tx = self.pool.begin().await.map_err(db_err)?;
        let ids: Vec<Uuid> = sqlx::query_scalar(
            r#"
            UPDATE download_jobs
            SET status = 'expired', locked_by = NULL, lease_until = NULL, updated_at = now()
            WHERE id IN (
                SELECT id FROM download_jobs
                WHERE status IN ('pending', 'queued', 'processing', 'uploading')
                  AND created_at < now() - make_interval(secs => $1::integer)
                  AND (lease_until IS NULL OR lease_until < now())
                LIMIT $2
            )
            RETURNING id
            "#,
        )
        .bind(secs(older_than))
        .bind(limit)
        .fetch_all(&mut *tx)
        .await
        .map_err(db_err)?;
        if !ids.is_empty() {
            sqlx::query("DELETE FROM active_media_jobs WHERE job_id = ANY($1)")
                .bind(&ids)
                .execute(&mut *tx)
                .await
                .map_err(db_err)?;
        }
        let count = ids.len() as u64;
        tx.commit().await.map_err(db_err)?;
        Ok(count)
    }

    async fn list_stale_uploads(
        &self,
        older_than: Duration,
        limit: i64,
    ) -> Result<Vec<Artifact>, AppError> {
        let rows = sqlx::query_as::<_, ArtifactRow>(
            r#"
            SELECT id, media_id, profile_hash, version, object_key, checksum_sha256, byte_size,
                   content_type, status, created_at
            FROM media_artifacts
            WHERE status = 'uploading' AND created_at < now() - make_interval(secs => $1::integer)
            ORDER BY created_at
            LIMIT $2
            "#,
        )
        .bind(secs(older_than))
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(db_err)?;
        rows.into_iter().map(ArtifactRow::into_artifact).collect()
    }

    async fn delete_artifact_row(&self, id: Uuid) -> Result<(), AppError> {
        sqlx::query("DELETE FROM media_artifacts WHERE id = $1 AND status = 'uploading'")
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(db_err)?;
        Ok(())
    }

    async fn upsert_external_user(
        &self,
        kind: UserKind,
        external_ref: &str,
        display_name: Option<&str>,
    ) -> Result<UserRecord, AppError> {
        let row = sqlx::query_as::<_, UserRow>(
            r#"
            INSERT INTO users (id, kind, external_ref, display_name)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (kind, external_ref)
            DO UPDATE SET display_name = COALESCE(EXCLUDED.display_name, users.display_name), updated_at = now()
            RETURNING id, kind, external_ref, display_name, status
            "#,
        )
        .bind(Uuid::now_v7())
        .bind(kind.as_str())
        .bind(external_ref)
        .bind(display_name)
        .fetch_one(&self.pool)
        .await
        .map_err(db_err)?;
        row.into_user()
    }

    async fn find_user_by_api_key_hash(&self, hash: &str) -> Result<Option<UserRecord>, AppError> {
        let row = sqlx::query_as::<_, UserRow>(
            r#"
            SELECT u.id, u.kind, u.external_ref, u.display_name, u.status
            FROM api_credentials c
            JOIN users u ON u.id = c.user_id
            WHERE c.key_hash = $1 AND c.revoked_at IS NULL AND u.status = 'active'
            "#,
        )
        .bind(hash)
        .fetch_optional(&self.pool)
        .await
        .map_err(db_err)?;
        row.map(UserRow::into_user).transpose()
    }

    async fn create_api_user(&self, name: &str, key_hash: &str) -> Result<UserRecord, AppError> {
        let mut tx = self.pool.begin().await.map_err(db_err)?;
        let user_id = Uuid::now_v7();
        sqlx::query(
            r#"
            INSERT INTO users (id, kind, external_ref, display_name)
            VALUES ($1, 'api', $2, $3)
            "#,
        )
        .bind(user_id)
        .bind(user_id.to_string())
        .bind(name)
        .execute(&mut *tx)
        .await
        .map_err(db_err)?;
        sqlx::query("INSERT INTO api_credentials (id, user_id, key_hash) VALUES ($1, $2, $3)")
            .bind(Uuid::now_v7())
            .bind(user_id)
            .bind(key_hash)
            .execute(&mut *tx)
            .await
            .map_err(db_err)?;
        tx.commit().await.map_err(db_err)?;
        Ok(UserRecord {
            id: user_id,
            kind: UserKind::Api,
            external_ref: user_id.to_string(),
            display_name: Some(name.to_owned()),
            status: "active".into(),
        })
    }

    async fn audit(&self, entry: AuditEntry) -> Result<(), AppError> {
        sqlx::query(
            r#"
            INSERT INTO audit_logs (id, actor_user_id, action, target_type, target_id, request_id, details)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            "#,
        )
        .bind(entry.id)
        .bind(entry.actor_user_id)
        .bind(entry.action)
        .bind(entry.target_type)
        .bind(entry.target_id)
        .bind(entry.request_id)
        .bind(entry.details)
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(())
    }

    async fn idempotency_get(
        &self,
        user_id: Uuid,
        key: &str,
    ) -> Result<Option<IdempotencyRecord>, AppError> {
        let row = sqlx::query_as::<_, IdempotencyRow>(
            r#"
            SELECT request_hash, status, body, expires_at
            FROM idempotency_records
            WHERE user_id = $1 AND idempotency_key = $2 AND expires_at > now()
            "#,
        )
        .bind(user_id)
        .bind(key)
        .fetch_optional(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(row.map(|row| IdempotencyRecord {
            request_hash: row.request_hash,
            status: row.status,
            body: row.body,
            expires_at: row.expires_at,
        }))
    }

    async fn idempotency_put(&self, record: NewIdempotency) -> Result<IdempotencyPut, AppError> {
        let inserted: Option<Uuid> = sqlx::query_scalar(
            r#"
            INSERT INTO idempotency_records (id, user_id, idempotency_key, request_hash, status, body, expires_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (user_id, idempotency_key) DO NOTHING
            RETURNING id
            "#,
        )
        .bind(record.id)
        .bind(record.user_id)
        .bind(&record.key)
        .bind(&record.request_hash)
        .bind(record.status)
        .bind(&record.body)
        .bind(record.expires_at)
        .fetch_optional(&self.pool)
        .await
        .map_err(db_err)?;
        if inserted.is_some() {
            return Ok(IdempotencyPut::Stored);
        }
        let existing: Option<String> = sqlx::query_scalar(
            "SELECT request_hash FROM idempotency_records WHERE user_id = $1 AND idempotency_key = $2",
        )
        .bind(record.user_id)
        .bind(&record.key)
        .fetch_optional(&self.pool)
        .await
        .map_err(db_err)?;
        if existing.as_deref() == Some(record.request_hash.as_str()) {
            Ok(IdempotencyPut::Stored)
        } else {
            Ok(IdempotencyPut::Conflict)
        }
    }

    async fn idempotency_purge(&self, now: DateTime<Utc>) -> Result<u64, AppError> {
        let result = sqlx::query("DELETE FROM idempotency_records WHERE expires_at <= $1")
            .bind(now)
            .execute(&self.pool)
            .await
            .map_err(db_err)?;
        Ok(result.rows_affected())
    }

    async fn save_metadata(
        &self,
        media_id: Uuid,
        meta: &crate::application::MediaMetadata,
    ) -> Result<(), AppError> {
        let mut tx = self.pool.begin().await.map_err(db_err)?;
        sqlx::query(
            "UPDATE media SET title = $2, duration_seconds = $3, updated_at = now() WHERE id = $1",
        )
        .bind(media_id)
        .bind(&meta.title)
        .bind(meta.duration_seconds)
        .execute(&mut *tx)
        .await
        .map_err(db_err)?;
        for profile in &meta.profiles {
            let code = format!(
                "{}:{}:{}",
                profile.kind_label(),
                profile.quality_label(),
                profile.container.as_str()
            );
            sqlx::query(
                r#"
                INSERT INTO media_formats (id, media_id, format_code, kind, quality, container)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (media_id, format_code) DO NOTHING
                "#,
            )
            .bind(Uuid::now_v7())
            .bind(media_id)
            .bind(&code)
            .bind(profile.kind_label())
            .bind(profile.quality_label())
            .bind(profile.container.as_str())
            .execute(&mut *tx)
            .await
            .map_err(db_err)?;
        }
        tx.commit().await.map_err(db_err)?;
        Ok(())
    }

    async fn fetch_outbox(
        &self,
        limit: i64,
    ) -> Result<Vec<crate::application::OutboxEvent>, AppError> {
        let rows = sqlx::query_as::<_, OutboxRow>(
            r#"
            SELECT id, aggregate_id, event_type, payload
            FROM outbox_events
            WHERE published_at IS NULL
            ORDER BY id
            LIMIT $1
            "#,
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(rows
            .into_iter()
            .map(|row| crate::application::OutboxEvent {
                id: row.id,
                aggregate_id: row.aggregate_id,
                event_type: row.event_type,
                payload: row.payload,
            })
            .collect())
    }

    async fn mark_outbox_published(&self, ids: &[i64]) -> Result<(), AppError> {
        if ids.is_empty() {
            return Ok(());
        }
        sqlx::query("UPDATE outbox_events SET published_at = now() WHERE id = ANY($1)")
            .bind(ids)
            .execute(&self.pool)
            .await
            .map_err(db_err)?;
        Ok(())
    }

    async fn heartbeat(&self, worker_id: &str, role: &str) -> Result<(), AppError> {
        sqlx::query(
            r#"
            INSERT INTO worker_heartbeats (worker_id, role, last_seen_at)
            VALUES ($1, $2, now())
            ON CONFLICT (worker_id) DO UPDATE SET role = EXCLUDED.role, last_seen_at = now()
            "#,
        )
        .bind(worker_id)
        .bind(role)
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(())
    }

    async fn platforms(&self) -> Result<Vec<PlatformInfo>, AppError> {
        let rows = sqlx::query_as::<_, (String, String, bool)>(
            "SELECT code, display_name, enabled FROM platforms ORDER BY id",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(rows
            .into_iter()
            .map(|(code, display_name, enabled)| PlatformInfo { code, display_name, enabled })
            .collect())
    }

    async fn ping(&self) -> Result<(), AppError> {
        sqlx::query("SELECT 1").execute(&self.pool).await.map_err(db_err)?;
        Ok(())
    }

    async fn pending_notifications(
        &self,
        limit: i64,
    ) -> Result<Vec<PendingNotification>, AppError> {
        let rows = sqlx::query_as::<_, NoticeRow>(
            r#"
            SELECT s.id AS subscription_id, s.job_id, s.telegram_chat_id, s.telegram_message_id, s.telegram_bot_id,
                   a.id, a.media_id, a.profile_hash, a.version, a.object_key, a.checksum_sha256, a.byte_size,
                   a.content_type, a.status, a.created_at, m.platform_id, m.canonical_content_id, j.profile
            FROM job_subscriptions s
            JOIN download_jobs j ON j.id = s.job_id
            JOIN media_artifacts a ON a.id = j.artifact_id
            JOIN media m ON m.id = j.media_id
            WHERE s.channel = 'telegram' AND s.notify_status = 'pending' AND j.status = 'completed'
            ORDER BY s.created_at
            LIMIT $1
            "#,
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(db_err)?;
        rows.into_iter().map(NoticeRow::into_notice).collect()
    }

    async fn pending_status_notices(&self, limit: i64) -> Result<Vec<StatusNotice>, AppError> {
        let rows = sqlx::query_as::<_, (Uuid, i64, Option<i32>, String)>(
            r#"
            SELECT s.id, s.telegram_chat_id, s.telegram_message_id, j.status
            FROM job_subscriptions s
            JOIN download_jobs j ON j.id = s.job_id
            WHERE s.channel = 'telegram' AND s.notify_status = 'pending'
              AND j.status IN ('failed', 'cancelled', 'expired')
            ORDER BY s.created_at
            LIMIT $1
            "#,
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(rows
            .into_iter()
            .map(|(subscription_id, chat_id, message_id, status)| StatusNotice {
                subscription_id,
                chat_id,
                message_id,
                text: format!("Download {status}."),
            })
            .collect())
    }

    async fn mark_notified(&self, subscription_id: Uuid, status: &str) -> Result<(), AppError> {
        sqlx::query("UPDATE job_subscriptions SET notify_status = $2 WHERE id = $1")
            .bind(subscription_id)
            .bind(status)
            .execute(&self.pool)
            .await
            .map_err(db_err)?;
        Ok(())
    }

    async fn save_telegram_delivery(
        &self,
        bot_id: &str,
        file: &TelegramFileRef,
    ) -> Result<(), AppError> {
        sqlx::query(
            r#"
            INSERT INTO telegram_deliveries (id, bot_id, media_artifact_id, file_id, file_unique_id, kind)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (bot_id, media_artifact_id)
            DO UPDATE SET file_id = EXCLUDED.file_id, file_unique_id = EXCLUDED.file_unique_id,
                          kind = EXCLUDED.kind, invalidated_at = NULL
            "#,
        )
        .bind(Uuid::now_v7())
        .bind(bot_id)
        .bind(file.artifact_id)
        .bind(&file.file_id)
        .bind(&file.file_unique_id)
        .bind(&file.kind)
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(())
    }

    async fn find_telegram_delivery(
        &self,
        bot_id: &str,
        artifact_id: Uuid,
    ) -> Result<Option<TelegramFileRef>, AppError> {
        let row = sqlx::query_as::<_, (String, Option<String>, String)>(
            r#"
            SELECT file_id, file_unique_id, kind
            FROM telegram_deliveries
            WHERE bot_id = $1 AND media_artifact_id = $2 AND invalidated_at IS NULL
            "#,
        )
        .bind(bot_id)
        .bind(artifact_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(row.map(|(file_id, file_unique_id, kind)| TelegramFileRef {
            file_id,
            file_unique_id,
            kind,
            artifact_id,
        }))
    }

    async fn invalidate_telegram_delivery(
        &self,
        bot_id: &str,
        artifact_id: Uuid,
    ) -> Result<(), AppError> {
        sqlx::query(
            "UPDATE telegram_deliveries SET invalidated_at = now() WHERE bot_id = $1 AND media_artifact_id = $2",
        )
        .bind(bot_id)
        .bind(artifact_id)
        .execute(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(())
    }

    async fn media_id_for_identity(
        &self,
        identity: &MediaIdentity,
    ) -> Result<Option<Uuid>, AppError> {
        let id = sqlx::query_scalar(
            "SELECT id FROM media WHERE platform_id = $1 AND canonical_content_id = $2",
        )
        .bind(identity.platform().db_id())
        .bind(identity.content_id().as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(db_err)?;
        Ok(id)
    }

    async fn user_job_count(&self, user_id: Uuid) -> Result<u64, AppError> {
        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM download_jobs WHERE requested_by = $1")
                .bind(user_id)
                .fetch_one(&self.pool)
                .await
                .map_err(db_err)?;
        Ok(u64::try_from(count).unwrap_or(0))
    }
}

impl PostgresStore {
    async fn load_job(&self, id: Uuid, user_id: Option<Uuid>) -> Result<Option<Job>, AppError> {
        let sql = if user_id.is_some() {
            format!(
                r#"
                SELECT {JOB_SELECT}
                FROM download_jobs j
                JOIN media m ON m.id = j.media_id
                WHERE j.id = $1 AND (
                    j.requested_by = $2 OR EXISTS (
                        SELECT 1 FROM job_subscriptions s WHERE s.job_id = j.id AND s.user_id = $2
                    )
                )
                "#
            )
        } else {
            format!(
                r#"
                SELECT {JOB_SELECT}
                FROM download_jobs j
                JOIN media m ON m.id = j.media_id
                WHERE j.id = $1
                "#
            )
        };
        let mut query = sqlx::query_as::<_, JobRow>(static_sql(sql)).bind(id);
        if let Some(user_id) = user_id {
            query = query.bind(user_id);
        }
        let row = query.fetch_optional(&self.pool).await.map_err(db_err)?;
        row.map(JobRow::into_job).transpose()
    }
}

async fn load_media(
    pool: &PgPool,
    id: Uuid,
    user_id: Option<Uuid>,
) -> Result<Option<MediaRecord>, AppError> {
    let sql = if user_id.is_some() {
        r#"
        SELECT m.id, m.platform_id, m.canonical_content_id, m.canonical_url, m.title, m.duration_seconds
        FROM media m
        WHERE m.id = $1 AND EXISTS (
            SELECT 1 FROM download_jobs j
            WHERE j.media_id = m.id AND (
                j.requested_by = $2 OR EXISTS (
                    SELECT 1 FROM job_subscriptions s WHERE s.job_id = j.id AND s.user_id = $2
                )
            )
        )
        "#
    } else {
        r#"
        SELECT m.id, m.platform_id, m.canonical_content_id, m.canonical_url, m.title, m.duration_seconds
        FROM media m WHERE m.id = $1
        "#
    };
    let mut query = sqlx::query_as::<_, MediaRow>(sql).bind(id);
    if let Some(user_id) = user_id {
        query = query.bind(user_id);
    }
    let row = query.fetch_optional(pool).await.map_err(db_err)?;
    Ok(row.map(MediaRow::into_record))
}

#[derive(sqlx::FromRow)]
struct MediaRow {
    id: Uuid,
    platform_id: i16,
    canonical_content_id: String,
    canonical_url: Option<String>,
    title: Option<String>,
    duration_seconds: Option<i32>,
}

impl MediaRow {
    fn into_record(self) -> MediaRecord {
        MediaRecord {
            id: self.id,
            platform: PlatformKind::from_db_id(self.platform_id).unwrap_or(PlatformKind::Direct),
            content_id: self.canonical_content_id,
            canonical_url: self.canonical_url,
            title: self.title,
            duration_seconds: self.duration_seconds,
        }
    }
}

#[derive(sqlx::FromRow)]
struct UserRow {
    id: Uuid,
    kind: String,
    external_ref: String,
    display_name: Option<String>,
    status: String,
}

impl UserRow {
    fn into_user(self) -> Result<UserRecord, AppError> {
        Ok(UserRecord {
            id: self.id,
            kind: UserKind::parse(&self.kind)
                .ok_or_else(|| AppError::Dependency("unknown user kind".into()))?,
            external_ref: self.external_ref,
            display_name: self.display_name,
            status: self.status,
        })
    }
}

#[derive(sqlx::FromRow)]
struct IdempotencyRow {
    request_hash: String,
    status: i32,
    body: serde_json::Value,
    expires_at: DateTime<Utc>,
}

#[derive(sqlx::FromRow)]
struct OutboxRow {
    id: i64,
    aggregate_id: Uuid,
    event_type: String,
    payload: serde_json::Value,
}

#[derive(sqlx::FromRow)]
struct NoticeRow {
    subscription_id: Uuid,
    job_id: Uuid,
    telegram_chat_id: i64,
    telegram_message_id: Option<i32>,
    telegram_bot_id: Option<String>,
    id: Uuid,
    media_id: Uuid,
    profile_hash: String,
    version: i32,
    object_key: String,
    checksum_sha256: String,
    byte_size: i64,
    content_type: String,
    status: String,
    created_at: DateTime<Utc>,
    platform_id: i16,
    canonical_content_id: String,
    profile: serde_json::Value,
}

impl NoticeRow {
    fn into_notice(self) -> Result<PendingNotification, AppError> {
        let platform = PlatformKind::from_db_id(self.platform_id)
            .ok_or_else(|| AppError::Dependency("unknown platform".into()))?;
        let profile = profile_from_json(&self.profile)?;
        let identity = MediaIdentity::derive(
            platform,
            crate::domain::ContentId::new(self.canonical_content_id)?,
            &profile,
        )?;
        let artifact = Artifact {
            id: self.id,
            media_id: self.media_id,
            profile_hash: self.profile_hash,
            version: self.version,
            object_key: self.object_key,
            checksum_sha256: self.checksum_sha256,
            byte_size: self.byte_size,
            content_type: self.content_type,
            status: ArtifactStatus::parse(&self.status)
                .ok_or_else(|| AppError::Dependency("unknown artifact status".into()))?,
            created_at: self.created_at,
        };
        Ok(PendingNotification {
            subscription_id: self.subscription_id,
            job_id: self.job_id,
            chat_id: self.telegram_chat_id,
            message_id: self.telegram_message_id,
            bot_id: self.telegram_bot_id.unwrap_or_default(),
            artifact,
            identity,
        })
    }
}

async fn insert_subscription(
    tx: &mut Transaction<'_, Postgres>,
    job_id: Uuid,
    actor: &Actor,
    channel: &DeliveryChannel,
) -> Result<(), AppError> {
    let (bot_id, message_id) = match channel {
        DeliveryChannel::Http => (None, None),
        DeliveryChannel::Telegram { bot_id, message_id, .. } => {
            (Some(bot_id.as_str()), *message_id)
        }
    };
    sqlx::query(
        r#"
        INSERT INTO job_subscriptions
            (id, job_id, user_id, channel, telegram_bot_id, telegram_chat_id, telegram_message_id)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (job_id, user_id, channel, telegram_chat_id) DO NOTHING
        "#,
    )
    .bind(Uuid::now_v7())
    .bind(job_id)
    .bind(actor.user_id)
    .bind(channel.as_str())
    .bind(bot_id)
    .bind(channel.chat_id())
    .bind(message_id)
    .execute(&mut **tx)
    .await
    .map_err(db_err)?;
    Ok(())
}

async fn insert_outbox(
    tx: &mut Transaction<'_, Postgres>,
    job_id: Uuid,
    event_type: &str,
) -> Result<(), AppError> {
    sqlx::query(
        r#"
        INSERT INTO outbox_events (aggregate_id, event_type, payload)
        VALUES ($1, $2, $3)
        "#,
    )
    .bind(job_id)
    .bind(event_type)
    .bind(serde_json::json!({ "job_id": job_id }))
    .execute(&mut **tx)
    .await
    .map_err(db_err)?;
    Ok(())
}

async fn finish_attempt(
    tx: &mut Transaction<'_, Postgres>,
    attempt_id: Uuid,
    outcome: &str,
    error: Option<&str>,
) -> Result<(), AppError> {
    sqlx::query(
        "UPDATE processing_attempts SET finished_at = now(), outcome = $2, error = $3 WHERE id = $1",
    )
    .bind(attempt_id)
    .bind(outcome)
    .bind(error)
    .execute(&mut **tx)
    .await
    .map_err(db_err)?;
    Ok(())
}

trait KindLabel {
    fn kind_label(self) -> &'static str;
}

impl KindLabel for ProcessingProfile {
    fn kind_label(self) -> &'static str {
        match self.kind {
            crate::domain::MediaKind::Video => "video",
            crate::domain::MediaKind::Audio => "audio",
            crate::domain::MediaKind::Image => "image",
        }
    }
}
