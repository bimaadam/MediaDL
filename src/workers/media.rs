//! Bounded worker pool. A permit is held for each in-flight job, and each
//! platform has its own permit pool. Messages are acked only after the job
//! reaches a terminal claim result. A lost lease stays pending for reclaim.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::application::{
    AppError, HandleResult, JobQueue, Pipeline, QueueDelivery, RuntimeConfig, Store,
};
use crate::domain::PlatformKind;

pub struct WorkerDeps {
    pub store: Arc<dyn Store>,
    pub queue: Arc<dyn JobQueue>,
    pub pipeline: Arc<Pipeline>,
    pub config: Arc<RuntimeConfig>,
    pub worker_id: String,
}

pub async fn run_media_worker(deps: Arc<WorkerDeps>, cancel: CancellationToken) {
    let global = Arc::new(Semaphore::new(deps.config.worker_concurrency.max(1)));
    let per_platform = Arc::new(PlatformPermits::new(deps.config.per_platform_concurrency.max(1)));
    let mut tasks = tokio::task::JoinSet::new();
    while !cancel.is_cancelled() {
        let permit = tokio::select! {
            _ = cancel.cancelled() => break,
            permit = global.clone().acquire_owned() => match permit {
                Ok(permit) => permit,
                Err(_) => break,
            },
        };
        let received = deps.queue.receive(&deps.worker_id, 1, Duration::from_secs(2)).await;
        let delivery = match received {
            Ok(mut batch) => batch.pop(),
            Err(error) => {
                tracing::warn!(%error, "queue receive failed");
                drop(permit);
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };
        let Some(delivery) = delivery else {
            drop(permit);
            continue;
        };
        let task_deps = Arc::clone(&deps);
        let permits = Arc::clone(&per_platform);
        tasks.spawn(async move {
            let _permit = permit;
            if let Err(error) = process_one(&task_deps, &permits, delivery).await {
                tracing::warn!(%error, "job processing failed before settlement");
            }
        });
        while tasks.len() > deps.config.worker_concurrency {
            tasks.join_next().await;
        }
    }
    while tasks.join_next().await.is_some() {}
}

async fn process_one(
    deps: &WorkerDeps,
    permits: &PlatformPermits,
    delivery: QueueDelivery,
) -> Result<(), AppError> {
    let job_id = delivery.message.job_id;
    let platform_permit = platform_permit(deps, permits, job_id).await?;
    let result = deps.pipeline.handle(job_id, &deps.worker_id).await?;
    drop(platform_permit);
    match result {
        HandleResult::LostLease => Ok(()),
        HandleResult::Failed => {
            deps.queue.dead_letter(delivery.message, "permanent failure").await?;
            deps.queue.ack(&delivery.id).await
        }
        HandleResult::Completed { .. } | HandleResult::Duplicate | HandleResult::RetryScheduled => {
            deps.queue.ack(&delivery.id).await
        }
    }
}

async fn platform_permit(
    deps: &WorkerDeps,
    permits: &PlatformPermits,
    job_id: Uuid,
) -> Result<OwnedSemaphorePermit, AppError> {
    let platform = match deps.store.get_job(job_id).await? {
        Some(job) => job.identity.platform(),
        None => PlatformKind::Direct,
    };
    Ok(permits.acquire(platform).await)
}

pub async fn handle_delivery(
    deps: &WorkerDeps,
    delivery: QueueDelivery,
) -> Result<HandleResult, AppError> {
    let result = deps.pipeline.handle(delivery.message.job_id, &deps.worker_id).await?;
    match result {
        HandleResult::LostLease => Ok(HandleResult::LostLease),
        HandleResult::Failed => {
            deps.queue.dead_letter(delivery.message, "permanent failure").await?;
            deps.queue.ack(&delivery.id).await?;
            Ok(HandleResult::Failed)
        }
        other => {
            deps.queue.ack(&delivery.id).await?;
            Ok(other)
        }
    }
}

struct PlatformPermits {
    permits: [Arc<Semaphore>; 7],
}

impl PlatformPermits {
    fn new(limit: usize) -> Self {
        Self { permits: std::array::from_fn(|_| Arc::new(Semaphore::new(limit))) }
    }

    async fn acquire(&self, platform: PlatformKind) -> OwnedSemaphorePermit {
        let index = platform.db_id().saturating_sub(1).clamp(0, 6) as usize;
        let semaphore = Arc::clone(&self.permits[index]);
        match semaphore.acquire_owned().await {
            Ok(permit) => permit,
            Err(_) => {
                tracing::error!("platform semaphore was closed; continuing without that limit");
                let fallback = Arc::new(Semaphore::new(1));
                loop {
                    if let Ok(permit) = Arc::clone(&fallback).acquire_owned().await {
                        return permit;
                    }
                }
            }
        }
    }
}
