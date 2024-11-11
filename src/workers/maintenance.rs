//! Recovery, orphan cleanup, and subscriber notification.

use std::path::Path;

use tokio::io::AsyncWriteExt;
use uuid::Uuid;

use crate::application::{
    AppError, ObjectStorage, RuntimeConfig, StatusNotice, Store, TelegramFileRef, TelegramSender,
    delivery_kind, format_menu_token, root_menu_choices, upload_filename,
};

pub async fn recover_once(store: &dyn Store, limit: i64) -> Result<Vec<Uuid>, AppError> {
    store.recover_expired_leases(limit).await
}

pub async fn cleanup_once(
    store: &dyn Store,
    storage: &dyn ObjectStorage,
    config: &RuntimeConfig,
) -> Result<(), AppError> {
    let stale = store.list_stale_uploads(config.uploading_grace, config.outbox_batch).await?;
    for artifact in stale {
        storage.delete(&artifact.object_key).await?;
        store.delete_artifact_row(artifact.id).await?;
        metrics::counter!("media_engine_orphaned_objects_total").increment(1);
    }
    let expired = store.expire_stale_jobs(config.job_expiry, config.outbox_batch).await?;
    if expired > 0 {
        tracing::info!(expired, "expired abandoned jobs");
    }
    store.idempotency_purge(chrono::Utc::now()).await?;
    Ok(())
}

pub async fn notify_once(
    store: &dyn Store,
    storage: &dyn ObjectStorage,
    sender: &dyn TelegramSender,
    config: &RuntimeConfig,
) -> Result<(), AppError> {
    let notes = store.pending_notifications(config.outbox_batch).await?;
    for note in notes {
        let result = deliver_artifact(store, storage, sender, config, &note).await;
        match result {
            Ok(()) => store.mark_notified(note.subscription_id, "sent").await?,
            Err(error) => {
                tracing::warn!(%error, subscription = %note.subscription_id, "telegram delivery failed");
                store.mark_notified(note.subscription_id, "failed").await?;
            }
        }
    }
    let texts = store.pending_status_notices(config.outbox_batch).await?;
    for notice in texts {
        send_notice(store, sender, notice).await?;
    }
    Ok(())
}

async fn send_notice(
    store: &dyn Store,
    sender: &dyn TelegramSender,
    notice: StatusNotice,
) -> Result<(), AppError> {
    let sent = if let Some(message_id) = notice.message_id {
        sender.edit_text(notice.chat_id, message_id, &notice.text).await
    } else {
        sender.send_text(notice.chat_id, &notice.text).await
    };
    match sent {
        Ok(()) => store.mark_notified(notice.subscription_id, "sent").await,
        Err(error) => {
            tracing::warn!(%error, "status notice failed");
            store.mark_notified(notice.subscription_id, "failed").await
        }
    }
}

async fn deliver_artifact(
    store: &dyn Store,
    storage: &dyn ObjectStorage,
    sender: &dyn TelegramSender,
    config: &RuntimeConfig,
    note: &crate::application::PendingNotification,
) -> Result<(), AppError> {
    let kind = delivery_kind(&note.artifact.content_type);
    let choices = root_menu_choices(&format_menu_token(note.job_id, note.chat_id));
    if let Some(file) = store.find_telegram_delivery(&note.bot_id, note.artifact.id).await? {
        if sender.send_file_id_menu(note.chat_id, kind, &file.file_id, "", &choices).await.is_ok() {
            return Ok(());
        }
        store.invalidate_telegram_delivery(&note.bot_id, note.artifact.id).await?;
    }
    if note.artifact.byte_size as u64 > config.telegram_max_upload_bytes
        || note.artifact.byte_size < 0
    {
        return sender
            .send_text(
                note.chat_id,
                &format!(
                    "This file is larger than Telegram accepts. Download it with your API key: GET /api/v1/artifacts/{}/content",
                    note.artifact.id
                ),
            )
            .await;
    }
    let directory = config.tmp_dir.join(format!("deliver-{}", note.subscription_id));
    tokio::fs::create_dir_all(&directory)
        .await
        .map_err(|error| AppError::Dependency(error.to_string()))?;
    let path = directory.join(upload_filename(&note.artifact.content_type));
    let result = download_to(storage, &note.artifact.object_key, &path).await;
    let sent = match result {
        Ok(()) => sender.send_file_menu(note.chat_id, &path, kind, "", &choices).await,
        Err(error) => Err(error),
    };
    let _ = tokio::fs::remove_dir_all(&directory).await;
    let file = sent?;
    if kind != "album" {
        let stored = TelegramFileRef {
            file_id: file.file_id,
            file_unique_id: file.file_unique_id,
            kind: kind.to_owned(),
            artifact_id: note.artifact.id,
        };
        store.save_telegram_delivery(&note.bot_id, &stored).await?;
    }
    Ok(())
}

async fn download_to(storage: &dyn ObjectStorage, key: &str, path: &Path) -> Result<(), AppError> {
    let mut object = storage.open(key).await?;
    let mut file = tokio::fs::File::create(path)
        .await
        .map_err(|error| AppError::Dependency(error.to_string()))?;
    tokio::io::copy(&mut object.reader, &mut file)
        .await
        .map_err(|error| AppError::Dependency(format!("copy artifact: {error}")))?;
    file.flush().await.map_err(|error| AppError::Dependency(error.to_string()))?;
    Ok(())
}
