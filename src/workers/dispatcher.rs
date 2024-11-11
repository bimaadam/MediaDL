use crate::application::{AppError, JobQueue, OutboxEvent, QueueMessage, Store};
use uuid::Uuid;

/// Publish unpublished `job.queued` events, then mark the whole successful prefix.
pub async fn publish_batch(
    store: &dyn Store,
    queue: &dyn JobQueue,
    limit: i64,
) -> Result<usize, AppError> {
    let events = store.fetch_outbox(limit).await?;
    let mut published = Vec::new();
    for event in events {
        if event.event_type == "job.queued" {
            queue.publish(QueueMessage { job_id: job_id_of(&event) }).await?;
        }
        published.push(event.id);
    }
    if !published.is_empty() {
        store.mark_outbox_published(&published).await?;
    }
    let depth = queue.depth().await?;
    metrics::gauge!("media_engine_queue_depth").set(depth as f64);
    Ok(published.len())
}

fn job_id_of(event: &OutboxEvent) -> Uuid {
    event
        .payload
        .get("job_id")
        .and_then(|value| value.as_str())
        .and_then(|value| Uuid::parse_str(value).ok())
        .unwrap_or(event.aggregate_id)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::application::{SubmitCommand, SubmitOutcome};
    use crate::domain::{DeliveryChannel, ProcessingProfile};
    use crate::testkit::{MemoryQueue, actor, engine_parts};
    use uuid::Uuid;

    use super::*;

    #[tokio::test]
    async fn an_outbox_row_becomes_one_queue_message() {
        let (engine, store) = engine_parts(false, false);
        let outcome = engine
            .submit(SubmitCommand {
                url: "https://youtu.be/dQw4w9WgXcQ".into(),
                profile: ProcessingProfile::video_mp4(720).expect("profile"),
                actor: actor(Uuid::now_v7()),
                channel: DeliveryChannel::Http,
            })
            .await
            .expect("submit");
        assert!(matches!(outcome, SubmitOutcome::Queued { .. }));
        let queue = MemoryQueue::default();
        let published = publish_batch(store.as_ref(), &queue, 10).await.expect("publish");
        assert_eq!(published, 1);
        assert_eq!(queue.depth().await.expect("depth"), 1);
        let again = publish_batch(store.as_ref(), &queue, 10).await.expect("republish");
        assert_eq!(again, 0);
        let _ = Arc::new(queue);
    }
}
