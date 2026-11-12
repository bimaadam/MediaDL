//! Recovery, orphan cleanup, and subscriber notification.

use std::path::Path;

use tokio::io::AsyncWriteExt;
use uuid::Uuid;

use crate::application::{
    AppError, Cache, DeliveredFile, ObjectStorage, RuntimeConfig, StatusNotice, Store,
    TelegramFileRef, TelegramSender, delivery_kind, format_menu_token, messages_to_remove,
    root_menu_choices, upload_filename,
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
    cache: &dyn Cache,
    storage: &dyn ObjectStorage,
    sender: &dyn TelegramSender,
    config: &RuntimeConfig,
) -> Result<(), AppError> {
    let notes = store.pending_notifications(config.outbox_batch).await?;
    for note in notes {
        let result = deliver_artifact(store, cache, storage, sender, config, &note).await;
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
    cache: &dyn Cache,
    storage: &dyn ObjectStorage,
    sender: &dyn TelegramSender,
    config: &RuntimeConfig,
    note: &crate::application::PendingNotification,
) -> Result<(), AppError> {
    let kind = delivery_kind(&note.artifact.content_type);
    let token = format_menu_token(note.job_id, note.chat_id);
    let choices = root_menu_choices(&token);
    let selection = cache.get_selection(&token).await.ok().flatten();
    let replace = selection.as_ref().and_then(|item| item.replace_message_id);
    let previous = selection.as_ref().map(|item| item.drop_message_ids.clone()).unwrap_or_default();
    if let Some(file) = store.find_telegram_delivery(&note.bot_id, note.artifact.id).await? {
        let sent = match replace.filter(|_| kind != "album") {
            Some(message_id) => {
                sender
                    .edit_file_id_menu(note.chat_id, message_id, kind, &file.file_id, &choices)
                    .await
            }
            None => sender.send_file_id_menu(note.chat_id, kind, &file.file_id, "", &choices).await,
        };
        if let Ok(sent) = sent {
            finish_notice(
                sender,
                cache,
                config,
                note,
                &Replacement { token: &token, replace, previous: &previous, sent: &sent },
            )
            .await;
            return Ok(());
        }
        store.invalidate_telegram_delivery(&note.bot_id, note.artifact.id).await?;
    }
    // An album is several messages. The archive can exceed one upload; each piece is capped later.
    if note.artifact.byte_size < 0
        || (kind != "album" && note.artifact.byte_size as u64 > config.telegram_max_upload_bytes)
    {
        return sender.send_text(note.chat_id, crate::application::TELEGRAM_OVERSIZED).await;
    }
    let directory = config.tmp_dir.join(format!("deliver-{}", note.subscription_id));
    tokio::fs::create_dir_all(&directory)
        .await
        .map_err(|error| AppError::Dependency(error.to_string()))?;
    let path = directory.join(upload_filename(&note.artifact.content_type));
    let downloaded = download_to(storage, &note.artifact.object_key, &path).await;
    let sent = match downloaded {
        Ok(()) => place_notice(sender, note.chat_id, &path, kind, &choices, replace).await,
        Err(error) => Err(error),
    };
    let _ = tokio::fs::remove_dir_all(&directory).await;
    let sent = match sent {
        Err(error) if error.is_oversized_notice() => return Ok(()),
        other => other?,
    };
    if kind != "album" {
        let stored = TelegramFileRef {
            file_id: sent.file.file_id.clone(),
            file_unique_id: sent.file.file_unique_id.clone(),
            kind: kind.to_owned(),
            artifact_id: note.artifact.id,
        };
        store.save_telegram_delivery(&note.bot_id, &stored).await?;
    }
    finish_notice(
        sender,
        cache,
        config,
        note,
        &Replacement { token: &token, replace, previous: &previous, sent: &sent },
    )
    .await;
    Ok(())
}

struct Replacement<'a> {
    token: &'a str,
    replace: Option<i32>,
    previous: &'a [i32],
    sent: &'a DeliveredFile,
}

async fn place_notice(
    sender: &dyn TelegramSender,
    chat_id: i64,
    path: &Path,
    kind: &str,
    choices: &[crate::application::TelegramChoice],
    replace: Option<i32>,
) -> Result<DeliveredFile, AppError> {
    if let Some(message_id) = replace.filter(|_| kind != "album")
        && let Ok(sent) = sender.edit_file_menu(chat_id, message_id, path, kind, choices).await
    {
        return Ok(sent);
    }
    sender.send_file_menu(chat_id, path, kind, "", choices).await
}

async fn finish_notice(
    sender: &dyn TelegramSender,
    cache: &dyn Cache,
    config: &RuntimeConfig,
    note: &crate::application::PendingNotification,
    replacement: &Replacement<'_>,
) {
    if let Some(replaced) = replacement.replace {
        let keep = replacement.sent.message_ids.contains(&replaced);
        for message_id in messages_to_remove(replacement.previous, replaced, keep) {
            let _ = sender.delete_message(note.chat_id, message_id).await;
        }
    }
    if let Ok(Some(mut selection)) = cache.get_selection(replacement.token).await {
        selection.drop_message_ids = replacement.sent.message_ids.clone();
        selection.replace_message_id = None;
        let _ = cache.put_selection(replacement.token, &selection, config.selection_ttl).await;
    }
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
