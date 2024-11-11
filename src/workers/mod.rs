//! Background loops. API and worker processes share this code and scale separately.

mod dispatcher;
mod maintenance;
mod media;

pub use dispatcher::publish_batch;
pub use maintenance::{cleanup_once, notify_once, recover_once};
pub use media::{WorkerDeps, handle_delivery, run_media_worker};

use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::application::{JobQueue, ObjectStorage, Pipeline, RuntimeConfig, Store, TelegramSender};

pub struct WorkerSet {
    pub store: Arc<dyn Store>,
    pub queue: Arc<dyn JobQueue>,
    pub pipeline: Arc<Pipeline>,
    pub storage: Arc<dyn ObjectStorage>,
    pub sender: Option<Arc<dyn TelegramSender>>,
    pub config: Arc<RuntimeConfig>,
    pub worker_id: String,
}

pub async fn run(set: WorkerSet, cancel: CancellationToken) {
    let WorkerSet { store, queue, pipeline, storage, sender, config, worker_id } = set;
    let deps = Arc::new(WorkerDeps {
        store: Arc::clone(&store),
        queue: Arc::clone(&queue),
        pipeline,
        config: Arc::clone(&config),
        worker_id: worker_id.clone(),
    });
    let batch = config.outbox_batch;
    tokio::join!(
        run_media_worker(deps, cancel.clone()),
        loop_dispatch(
            Arc::clone(&store),
            Arc::clone(&queue),
            batch,
            worker_id.clone(),
            cancel.clone()
        ),
        loop_recover(Arc::clone(&store), batch, worker_id.clone(), cancel.clone()),
        loop_cleanup(
            Arc::clone(&store),
            Arc::clone(&storage),
            Arc::clone(&config),
            worker_id.clone(),
            cancel.clone()
        ),
        loop_notify(store, storage, sender, config, worker_id, cancel),
    );
}

async fn loop_dispatch(
    store: Arc<dyn Store>,
    queue: Arc<dyn JobQueue>,
    batch: i64,
    worker_id: String,
    cancel: CancellationToken,
) {
    while !cancel.is_cancelled() {
        if let Err(error) = publish_batch(store.as_ref(), queue.as_ref(), batch).await {
            tracing::warn!(%error, "outbox publish failed");
        }
        let _ = store.heartbeat(&worker_id, "dispatcher").await;
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = tokio::time::sleep(Duration::from_millis(500)) => {}
        }
    }
}

async fn loop_recover(
    store: Arc<dyn Store>,
    batch: i64,
    worker_id: String,
    cancel: CancellationToken,
) {
    while !cancel.is_cancelled() {
        match recover_once(store.as_ref(), batch).await {
            Ok(ids) if !ids.is_empty() => {
                tracing::info!(count = ids.len(), "recovered abandoned jobs")
            }
            Ok(_) => {}
            Err(error) => tracing::warn!(%error, "lease recovery failed"),
        }
        let _ = store.heartbeat(&worker_id, "recovery").await;
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = tokio::time::sleep(Duration::from_secs(15)) => {}
        }
    }
}

async fn loop_cleanup(
    store: Arc<dyn Store>,
    storage: Arc<dyn ObjectStorage>,
    config: Arc<RuntimeConfig>,
    worker_id: String,
    cancel: CancellationToken,
) {
    while !cancel.is_cancelled() {
        if let Err(error) = cleanup_once(store.as_ref(), storage.as_ref(), &config).await {
            tracing::warn!(%error, "cleanup failed");
        }
        let _ = store.heartbeat(&worker_id, "cleanup").await;
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = tokio::time::sleep(Duration::from_secs(60)) => {}
        }
    }
}

async fn loop_notify(
    store: Arc<dyn Store>,
    storage: Arc<dyn ObjectStorage>,
    sender: Option<Arc<dyn TelegramSender>>,
    config: Arc<RuntimeConfig>,
    worker_id: String,
    cancel: CancellationToken,
) {
    let Some(sender) = sender else {
        return;
    };
    while !cancel.is_cancelled() {
        if let Err(error) =
            notify_once(store.as_ref(), storage.as_ref(), sender.as_ref(), &config).await
        {
            tracing::warn!(%error, "notification delivery failed");
        }
        let _ = store.heartbeat(&worker_id, "notifier").await;
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = tokio::time::sleep(Duration::from_secs(2)) => {}
        }
    }
}
