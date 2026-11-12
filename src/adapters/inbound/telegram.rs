//! Telegram delivery adapter. Callback payloads stay short; the format list lives in Redis.

use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::io::AsyncWriteExt;
use tokio::sync::watch;

use teloxide::dispatching::UpdateFilterExt;
use teloxide::prelude::*;
use teloxide::types::{
    CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, InputFile, InputMedia,
    InputMediaAudio, InputMediaDocument, InputMediaPhoto, InputMediaVideo, Message, MessageId,
};
use teloxide::utils::command::BotCommands;
use tokio_util::sync::CancellationToken;

use crate::adapters::inbound::http::selection_token;
use crate::application::{
    AppError, DeliveredFile, Engine, FormatSelection, SubmitCommand, SubmitOutcome, TelegramChoice,
    TelegramFileRef, download_status_text, format_menu_token, messages_to_remove,
    resolution_menu_choices, root_menu_choices,
};
use crate::domain::{
    Actor, Artifact, DeliveryChannel, JobStatus, MediaIdentity, ProcessingProfile, UserKind,
};

#[derive(BotCommands, Clone)]
#[command(rename_rule = "lowercase", description = "Media download commands")]
enum Command {
    #[command(description = "show this help")]
    Start,
    #[command(description = "show this help")]
    Help,
    #[command(description = "show how many downloads you have requested")]
    Stats,
    #[command(description = "show a job")]
    Status { job: String },
    #[command(description = "cancel a job you joined")]
    Cancel { job: String },
}

pub async fn run(token: &str, bot_id: String, engine: Arc<Engine>, cancel: CancellationToken) {
    let bot = Bot::new(token);
    let handler = dptree::entry()
        .branch(Update::filter_message().endpoint(on_message))
        .branch(Update::filter_callback_query().endpoint(on_callback));
    let mut dispatcher =
        Dispatcher::builder(bot, handler).dependencies(dptree::deps![engine, bot_id]).build();
    let shutdown = dispatcher.shutdown_token();
    tokio::select! {
        _ = dispatcher.dispatch() => {}
        _ = cancel.cancelled() => {
            if let Ok(done) = shutdown.shutdown() {
                done.await;
            }
        }
    }
}

async fn on_message(
    bot: Bot,
    message: Message,
    engine: Arc<Engine>,
    bot_id: String,
) -> ResponseResult<()> {
    let Some(text) = message.text() else {
        return Ok(());
    };
    let chat_id = message.chat.id;
    if let Ok(command) = Command::parse(text, "") {
        if let Err(error) =
            handle_command(&bot, &engine, &bot_id, chat_id.0, &message, command).await
        {
            let _ = bot.send_message(chat_id, error.to_string()).await;
        }
        return Ok(());
    }
    if let Some(url) = first_url(text)
        && let Err(error) =
            fetch_link(&bot, &engine, &bot_id, chat_id.0, message.id.0, &message, url).await
    {
        tracing::warn!(%error, "telegram link failed");
        let _ = bot.send_message(chat_id, error.to_string()).await;
    }
    Ok(())
}

async fn on_callback(
    bot: Bot,
    query: CallbackQuery,
    engine: Arc<Engine>,
    bot_id: String,
) -> ResponseResult<()> {
    let _ = bot.answer_callback_query(query.id.clone()).await;
    let Some(data) = query.data.clone() else {
        return Ok(());
    };
    let Some(chat) = query.message.as_ref().map(|message| message.chat().id) else {
        return Ok(());
    };
    if let Err(error) = handle_callback(&bot, &engine, &bot_id, chat.0, &query, &data).await {
        tracing::warn!(%error, "telegram format choice failed");
        let _ = bot.send_message(chat, error.to_string()).await;
    }
    Ok(())
}

async fn handle_command(
    bot: &Bot,
    engine: &Engine,
    bot_id: &str,
    chat_id: i64,
    message: &Message,
    command: Command,
) -> Result<(), AppError> {
    let actor = actor_for(engine, message).await?;
    match command {
        Command::Start | Command::Help => {
            bot.send_message(ChatId(chat_id), Command::descriptions().to_string())
                .await
                .map_err(|error| AppError::Dependency(error.to_string()))?;
        }
        Command::Stats => {
            let count = engine.usage(actor.user_id).await?;
            bot.send_message(ChatId(chat_id), format!("Requested downloads: {count}"))
                .await
                .map_err(|error| AppError::Dependency(error.to_string()))?;
        }
        Command::Status { job } => {
            let job_id = parse_uuid(&job)?;
            let job = engine.job_for_user(actor.user_id, job_id).await?;
            let progress = engine.progress(job_id).await?;
            let percent = progress
                .and_then(|item| item.percent)
                .map(|value| format!(" {value}%"))
                .unwrap_or_default();
            bot.send_message(ChatId(chat_id), format!("Job {job_id} is {}{percent}.", job.status))
                .await
                .map_err(|error| AppError::Dependency(error.to_string()))?;
        }
        Command::Cancel { job } => {
            let job_id = parse_uuid(&job)?;
            let job = engine.cancel(actor.user_id, job_id).await?;
            bot.send_message(ChatId(chat_id), format!("Job {} is {}.", job.id, job.status))
                .await
                .map_err(|error| AppError::Dependency(error.to_string()))?;
        }
    }
    let _ = bot_id;
    Ok(())
}

async fn fetch_link(
    bot: &Bot,
    engine: &Arc<Engine>,
    bot_id: &str,
    chat_id: i64,
    message_id: i32,
    message: &Message,
    url: &str,
) -> Result<(), AppError> {
    let actor = actor_for(engine, message).await?;
    let (pulse, status_message) =
        begin_animation(bot, engine, actor.user_id, chat_id, AnimSignal::Spin).await?;
    let submitted = async {
        let profile = engine.preferred_profile(url).await?;
        engine
            .submit(SubmitCommand {
                url: url.to_owned(),
                profile,
                actor: actor.clone(),
                channel: DeliveryChannel::Telegram {
                    bot_id: bot_id.to_owned(),
                    chat_id,
                    message_id: Some(message_id),
                },
            })
            .await
    }
    .await;
    let outcome = match submitted {
        Ok(outcome) => outcome,
        Err(error) => {
            let _ = pulse.send(AnimSignal::Fail(status_failure(&error.to_string())));
            tracing::warn!(%error, "telegram link failed");
            return Ok(());
        }
    };
    let target = DeliveryTarget {
        bot_id,
        chat_id,
        actor: &actor,
        url,
        replace_message_id: None,
        drop_message_ids: &[],
    };
    match deliver_outcome(bot, engine, &target, outcome).await {
        Ok(follow) => {
            apply_follow(bot, engine, actor.user_id, chat_id, Some((pulse, status_message)), follow)
                .await
        }
        Err(error) => {
            let _ = pulse.send(AnimSignal::Fail(status_failure(&error.to_string())));
            tracing::warn!(%error, "telegram delivery failed");
            Ok(())
        }
    }
}

async fn handle_callback(
    bot: &Bot,
    engine: &Arc<Engine>,
    bot_id: &str,
    chat_id: i64,
    query: &CallbackQuery,
    data: &str,
) -> Result<(), AppError> {
    if let Some(token) = data.strip_prefix("m:") {
        return show_menu(bot, engine, chat_id, query, token, false).await;
    }
    if let Some(token) = data.strip_prefix("b:") {
        return show_menu(bot, engine, chat_id, query, token, true).await;
    }
    let Some(rest) = data.strip_prefix("s:") else {
        return Ok(());
    };
    let Some((token, index_raw)) = rest.rsplit_once(':') else {
        return Err(AppError::Validation("unknown format choice".into()));
    };
    let Ok(index) = index_raw.parse::<usize>() else {
        return Err(AppError::Validation("unknown format choice".into()));
    };
    let selection = authorize(engine, chat_id, query, token).await?;
    let Some(profile) = selection.profiles.get(index).copied() else {
        return Err(AppError::Validation("unknown format choice".into()));
    };
    let actor = Actor { user_id: selection.user_id, kind: UserKind::Telegram };
    let replace_message_id = query.message.as_ref().map(|message| message.id().0);
    let outcome = engine
        .submit(SubmitCommand {
            url: selection.url.clone(),
            profile,
            actor: actor.clone(),
            channel: DeliveryChannel::Telegram {
                bot_id: bot_id.to_owned(),
                chat_id,
                message_id: None,
            },
        })
        .await?;
    let target = DeliveryTarget {
        bot_id,
        chat_id,
        actor: &actor,
        url: &selection.url,
        replace_message_id,
        drop_message_ids: &selection.drop_message_ids,
    };
    let follow = deliver_outcome(bot, engine, &target, outcome).await?;
    apply_follow(bot, engine, actor.user_id, chat_id, None, follow).await
}

async fn show_menu(
    bot: &Bot,
    engine: &Engine,
    chat_id: i64,
    query: &CallbackQuery,
    token: &str,
    root: bool,
) -> Result<(), AppError> {
    let mut selection = authorize(engine, chat_id, query, token).await?;
    if !root {
        selection.profiles = match engine.available_profiles(&selection.url).await {
            Ok(profiles) if !profiles.is_empty() => profiles,
            _ => vec![
                ProcessingProfile::best_video().within(engine.config().telegram_max_upload_bytes),
            ],
        };
        engine.put_selection(token, &selection).await?;
    }
    let choices = if root {
        root_menu_choices(token)
    } else {
        resolution_menu_choices(token, &selection.profiles)
    };
    let Some(markup) = menu_markup(&choices) else {
        return Ok(());
    };
    let Some(message) = query.message.as_ref() else {
        return Ok(());
    };
    match bot.edit_message_reply_markup(ChatId(chat_id), message.id()).reply_markup(markup).await {
        Ok(_) => Ok(()),
        Err(error) if unchanged_message(&error.to_string()) => Ok(()),
        Err(error) => Err(AppError::Dependency(error.to_string())),
    }
}

async fn authorize(
    engine: &Engine,
    chat_id: i64,
    query: &CallbackQuery,
    token: &str,
) -> Result<FormatSelection, AppError> {
    if token.is_empty() || token.len() > 62 {
        return Err(AppError::Validation("unknown format choice".into()));
    }
    let Some(selection) = engine.take_selection(token).await? else {
        return Err(AppError::Validation("that choice expired".into()));
    };
    if selection.chat_id != chat_id {
        return Err(AppError::Forbidden);
    }
    let name = query.from.full_name();
    let clicker = engine
        .store_user(UserKind::Telegram, &query.from.id.0.to_string(), Some(name.as_str()))
        .await?;
    if clicker.id != selection.user_id {
        return Err(AppError::Validation(
            "that menu belongs to the person who sent the link".into(),
        ));
    }
    Ok(selection)
}

enum FollowUp {
    Done,
    Track(uuid::Uuid),
    Tell(String),
}

struct DeliveryTarget<'a> {
    bot_id: &'a str,
    chat_id: i64,
    actor: &'a Actor,
    url: &'a str,
    replace_message_id: Option<i32>,
    drop_message_ids: &'a [i32],
}

async fn deliver_outcome(
    bot: &Bot,
    engine: &Arc<Engine>,
    target: &DeliveryTarget<'_>,
    outcome: SubmitOutcome,
) -> Result<FollowUp, AppError> {
    match outcome {
        SubmitOutcome::TelegramCached { file, identity } => {
            let token = selection_token();
            remember_menu(engine, &token, target, None, Vec::new()).await?;
            let choices = root_menu_choices(&token);
            let sent = match target.replace_message_id {
                Some(message_id) if file.kind != "album" => {
                    match edit_file_id(bot, target.chat_id, message_id, &file, &choices).await {
                        Ok(sent) => Ok(sent),
                        Err(error) => {
                            tracing::warn!(%error, "cached file did not replace the message");
                            send_cached(bot, target.chat_id, &file, &choices).await
                        }
                    }
                }
                _ => send_cached(bot, target.chat_id, &file, &choices).await,
            };
            match sent {
                Ok(sent) => {
                    finish_replacement(bot, target, &sent.message_ids).await;
                    stamp_messages(engine, &token, sent.message_ids).await?;
                    Ok(FollowUp::Done)
                }
                Err(_) => {
                    engine
                        .invalidate_telegram_file(target.bot_id, &identity, file.artifact_id)
                        .await?;
                    Ok(FollowUp::Tell(
                        "The cached Telegram file expired. Ask again to deliver it from storage."
                            .to_owned(),
                    ))
                }
            }
        }
        SubmitOutcome::Ready { artifact, identity, .. } => {
            let token = selection_token();
            remember_menu(engine, &token, target, None, Vec::new()).await?;
            let sent = deliver_ready(
                bot,
                engine,
                target,
                &artifact,
                &identity,
                &root_menu_choices(&token),
            )
            .await?;
            stamp_messages(engine, &token, sent).await?;
            Ok(FollowUp::Done)
        }
        SubmitOutcome::Queued { job_id, .. } => {
            let token = format_menu_token(job_id, target.chat_id);
            remember_menu(
                engine,
                &token,
                target,
                target.replace_message_id,
                target.drop_message_ids.to_vec(),
            )
            .await?;
            Ok(FollowUp::Track(job_id))
        }
    }
}

async fn apply_follow(
    bot: &Bot,
    engine: &Arc<Engine>,
    user_id: uuid::Uuid,
    chat_id: i64,
    pulse: Option<(watch::Sender<AnimSignal>, MessageId)>,
    follow: FollowUp,
) -> Result<(), AppError> {
    match follow {
        FollowUp::Done => {
            if let Some((tx, _)) = pulse {
                let _ = tx.send(AnimSignal::Clear);
            }
            Ok(())
        }
        FollowUp::Track(job_id) => {
            let (tx, status_message) = match pulse {
                Some(pair) => pair,
                None => {
                    begin_animation(bot, engine, user_id, chat_id, AnimSignal::Track(job_id))
                        .await?
                }
            };
            let _ = engine.point_status_message(job_id, user_id, status_message.0).await;
            let _ = tx.send(AnimSignal::Track(job_id));
            Ok(())
        }
        FollowUp::Tell(text) => {
            if let Some((tx, _)) = pulse {
                let _ = tx.send(AnimSignal::Fail(text));
                Ok(())
            } else {
                bot.send_message(ChatId(chat_id), text)
                    .await
                    .map(|_| ())
                    .map_err(|error| AppError::Dependency(error.to_string()))
            }
        }
    }
}

async fn remember_menu(
    engine: &Engine,
    token: &str,
    target: &DeliveryTarget<'_>,
    replace_message_id: Option<i32>,
    drop_message_ids: Vec<i32>,
) -> Result<(), AppError> {
    engine
        .put_selection(
            token,
            &FormatSelection {
                url: target.url.to_owned(),
                user_id: target.actor.user_id,
                chat_id: target.chat_id,
                profiles: vec![
                    ProcessingProfile::best_video()
                        .within(engine.config().telegram_max_upload_bytes),
                ],
                replace_message_id,
                drop_message_ids,
            },
        )
        .await
}

async fn stamp_messages(
    engine: &Engine,
    token: &str,
    message_ids: Vec<i32>,
) -> Result<(), AppError> {
    let Some(mut selection) = engine.take_selection(token).await? else {
        return Ok(());
    };
    selection.drop_message_ids = message_ids;
    selection.replace_message_id = None;
    engine.put_selection(token, &selection).await
}

async fn finish_replacement(bot: &Bot, target: &DeliveryTarget<'_>, kept: &[i32]) {
    let Some(replaced) = target.replace_message_id else {
        return;
    };
    let keep_replaced = kept.contains(&replaced);
    for message_id in messages_to_remove(target.drop_message_ids, replaced, keep_replaced) {
        let _ = bot.delete_message(ChatId(target.chat_id), MessageId(message_id)).await;
    }
}

async fn deliver_ready(
    bot: &Bot,
    engine: &Engine,
    target: &DeliveryTarget<'_>,
    artifact: &Artifact,
    identity: &MediaIdentity,
    choices: &[TelegramChoice],
) -> Result<Vec<i32>, AppError> {
    let kind = crate::application::delivery_kind(&artifact.content_type);
    let chat_id = target.chat_id;
    // An album is several messages. The archive can exceed one upload; each piece is capped later.
    if artifact.byte_size < 0
        || (kind != "album"
            && artifact.byte_size as u64 > engine.config().telegram_max_upload_bytes)
    {
        bot.send_message(ChatId(chat_id), crate::application::TELEGRAM_OVERSIZED)
            .await
            .map_err(|error| AppError::Dependency(error.to_string()))?;
        return Ok(Vec::new());
    }
    let directory = engine.config().tmp_dir.join(format!("deliver-{}-{chat_id}", artifact.id));
    tokio::fs::create_dir_all(&directory)
        .await
        .map_err(|error| AppError::Dependency(error.to_string()))?;
    let path = directory.join(crate::application::upload_filename(&artifact.content_type));
    let sent = async {
        let mut object = engine.open_ready_artifact(artifact).await?;
        let mut file = tokio::fs::File::create(&path)
            .await
            .map_err(|error| AppError::Dependency(error.to_string()))?;
        tokio::io::copy(&mut object.reader, &mut file)
            .await
            .map_err(|error| AppError::Dependency(format!("copy artifact: {error}")))?;
        file.flush().await.map_err(|error| AppError::Dependency(error.to_string()))?;
        drop(file);
        let mut sent = match place_file(bot, target, &path, kind, choices).await {
            Err(error) if error.is_oversized_notice() => return Ok(Vec::new()),
            other => other?,
        };
        if kind != "album" {
            sent.file.artifact_id = artifact.id;
            engine.remember_telegram_file(target.bot_id, identity, &sent.file).await?;
        }
        Ok(sent.message_ids)
    }
    .await;
    let _ = tokio::fs::remove_dir_all(&directory).await;
    sent
}

async fn send_cached(
    bot: &Bot,
    chat_id: i64,
    file: &TelegramFileRef,
    choices: &[TelegramChoice],
) -> Result<DeliveredFile, AppError> {
    let filename = part_name(&file.kind, None);
    let message = send_input(
        bot,
        chat_id,
        InputFile::file_id(file.file_id.clone().into()),
        &file.kind,
        &filename,
        "",
        choices,
    )
    .await?;
    Ok(DeliveredFile { message_ids: vec![message.id.0], file: file.clone() })
}

/// Edit the message the user already has. A failed edit sends once, then the old bubbles go away.
async fn place_file(
    bot: &Bot,
    target: &DeliveryTarget<'_>,
    path: &std::path::Path,
    kind: &str,
    choices: &[TelegramChoice],
) -> Result<DeliveredFile, AppError> {
    if let Some(message_id) = target.replace_message_id
        && kind != "album"
        && let Ok(sent) = edit_path(bot, target.chat_id, message_id, path, kind, choices).await
    {
        finish_replacement(bot, target, &sent.message_ids).await;
        return Ok(sent);
    }
    let sent = send_path(bot, target.chat_id, path, kind, "", choices).await?;
    if target.replace_message_id.is_some() {
        finish_replacement(bot, target, &[]).await;
    }
    Ok(sent)
}

pub async fn send_path(
    bot: &Bot,
    chat_id: i64,
    path: &std::path::Path,
    kind: &str,
    caption: &str,
    choices: &[TelegramChoice],
) -> Result<DeliveredFile, AppError> {
    if kind == "album" {
        return send_album(bot, chat_id, path, choices).await;
    }
    let filename = part_name(kind, Some(path));
    let message =
        send_input(bot, chat_id, InputFile::file(path), kind, &filename, caption, choices).await?;
    Ok(DeliveredFile { file: file_ref(&message, kind)?, message_ids: vec![message.id.0] })
}

async fn edit_path(
    bot: &Bot,
    chat_id: i64,
    message_id: i32,
    path: &std::path::Path,
    kind: &str,
    choices: &[TelegramChoice],
) -> Result<DeliveredFile, AppError> {
    let filename = part_name(kind, Some(path));
    let message = edit_input(
        bot,
        chat_id,
        MessageId(message_id),
        InputFile::file(path),
        kind,
        &filename,
        choices,
    )
    .await?;
    Ok(DeliveredFile { file: file_ref(&message, kind)?, message_ids: vec![message.id.0] })
}

async fn edit_file_id(
    bot: &Bot,
    chat_id: i64,
    message_id: i32,
    file: &TelegramFileRef,
    choices: &[TelegramChoice],
) -> Result<DeliveredFile, AppError> {
    let filename = part_name(&file.kind, None);
    let message = edit_input(
        bot,
        chat_id,
        MessageId(message_id),
        InputFile::file_id(file.file_id.clone().into()),
        &file.kind,
        &filename,
        choices,
    )
    .await?;
    Ok(DeliveredFile { message_ids: vec![message.id.0], file: file.clone() })
}

fn menu_markup(choices: &[TelegramChoice]) -> Option<InlineKeyboardMarkup> {
    let rows: Vec<Vec<InlineKeyboardButton>> = choices
        .iter()
        .filter(|choice| choice.data.len() <= 64 && !choice.label.is_empty())
        .map(|choice| {
            vec![InlineKeyboardButton::callback(choice.label.clone(), choice.data.clone())]
        })
        .collect();
    if rows.is_empty() { None } else { Some(InlineKeyboardMarkup::new(rows)) }
}

async fn send_input(
    bot: &Bot,
    chat_id: i64,
    input: InputFile,
    kind: &str,
    filename: &str,
    caption: &str,
    choices: &[TelegramChoice],
) -> Result<Message, AppError> {
    let chat = ChatId(chat_id);
    let markup = menu_markup(choices);
    // A file id has no multipart part, so this name only reaches a fresh upload.
    // Phones render from it. `media.bin` is what they refuse to draw.
    let name = if filename.is_empty() { upload_name(kind) } else { filename };
    let input = input.file_name(name.to_owned());
    let message = match kind {
        "audio" => {
            let request = bot.send_audio(chat, input).caption(caption);
            match markup {
                Some(markup) => request.reply_markup(markup).await,
                None => request.await,
            }
        }
        "photo" => {
            let request = bot.send_photo(chat, input).caption(caption);
            match markup {
                Some(markup) => request.reply_markup(markup).await,
                None => request.await,
            }
        }
        "video" => {
            let request = bot.send_video(chat, input).caption(caption).supports_streaming(true);
            match markup {
                Some(markup) => request.reply_markup(markup).await,
                None => request.await,
            }
        }
        _ => {
            let request = bot.send_document(chat, input).caption(caption);
            match markup {
                Some(markup) => request.reply_markup(markup).await,
                None => request.await,
            }
        }
    }
    .map_err(|error| AppError::Dependency(error.to_string()))?;
    Ok(message)
}

async fn send_album(
    bot: &Bot,
    chat_id: i64,
    path: &std::path::Path,
    choices: &[TelegramChoice],
) -> Result<DeliveredFile, AppError> {
    let bytes =
        tokio::fs::read(path).await.map_err(|error| AppError::Dependency(error.to_string()))?;
    let stored = crate::shared::stored_zip::read_stored_zip(&bytes)?;
    let files = match fitting_pieces(stored, crate::application::TELEGRAM_FILE_BYTES) {
        Ok(files) => files,
        Err(error) if error.is_oversized_notice() => {
            bot.send_message(ChatId(chat_id), crate::application::TELEGRAM_OVERSIZED)
                .await
                .map_err(|error| AppError::Dependency(error.to_string()))?;
            return Err(error);
        }
        Err(error) => return Err(error),
    };
    if files.len() == 1 {
        let (name, data) = files
            .into_iter()
            .next()
            .ok_or_else(|| AppError::Validation("photo archive is empty".into()))?;
        let kind = piece_kind(&name);
        let message =
            send_input(bot, chat_id, InputFile::memory(data), kind, &name, "", choices).await?;
        return Ok(DeliveredFile {
            file: file_ref(&message, kind)?,
            message_ids: vec![message.id.0],
        });
    }
    let mut first = None;
    let mut message_ids = Vec::new();
    for chunk in files.chunks(10) {
        let Some((name, data)) = chunk.first() else {
            continue;
        };
        if chunk.len() < 2 {
            let kind = piece_kind(name);
            let message =
                send_input(bot, chat_id, InputFile::memory(data.clone()), kind, name, "", &[])
                    .await?;
            message_ids.push(message.id.0);
            if first.is_none() {
                first = Some(file_ref(&message, kind)?);
            }
            continue;
        }
        let media =
            chunk.iter().map(|(name, data)| album_input(name, data.clone())).collect::<Vec<_>>();
        let messages = bot
            .send_media_group(ChatId(chat_id), media)
            .await
            .map_err(|error| AppError::Dependency(error.to_string()))?;
        message_ids.extend(messages.iter().map(|message| message.id.0));
        if first.is_none() {
            let message = messages
                .first()
                .ok_or_else(|| AppError::Dependency("telegram returned an empty album".into()))?;
            first = Some(file_ref(message, "album")?);
        }
    }
    // A media group cannot carry an inline keyboard. The menu is the next message.
    if let Some(markup) = menu_markup(choices) {
        match bot.send_message(ChatId(chat_id), "Media").reply_markup(markup).await {
            Ok(message) => message_ids.push(message.id.0),
            Err(error) => tracing::warn!(%error, "album menu was not attached"),
        }
    }
    let file = first.ok_or_else(|| AppError::Validation("photo archive is empty".into()))?;
    Ok(DeliveredFile { file, message_ids })
}

fn fitting_pieces(
    files: Vec<(String, Vec<u8>)>,
    limit: u64,
) -> Result<Vec<(String, Vec<u8>)>, AppError> {
    if files.is_empty() {
        return Err(AppError::Validation("photo archive is empty".into()));
    }
    let fit = files.into_iter().filter(|(_, data)| data.len() as u64 <= limit).collect::<Vec<_>>();
    if fit.is_empty() {
        Err(AppError::Validation(crate::application::TELEGRAM_OVERSIZED.into()))
    } else {
        Ok(fit)
    }
}

fn album_input(name: &str, data: Vec<u8>) -> InputMedia {
    let file = InputFile::memory(data).file_name(name.to_owned());
    if piece_kind(name) == "video" {
        InputMedia::Video(InputMediaVideo::new(file).supports_streaming(true))
    } else {
        InputMedia::Photo(InputMediaPhoto::new(file))
    }
}

fn piece_kind(name: &str) -> &'static str {
    if name.rsplit('.').next() == Some("mp4") { "video" } else { "photo" }
}

async fn edit_input(
    bot: &Bot,
    chat_id: i64,
    message_id: MessageId,
    input: InputFile,
    kind: &str,
    filename: &str,
    choices: &[TelegramChoice],
) -> Result<Message, AppError> {
    let name = if filename.is_empty() { upload_name(kind) } else { filename };
    let input = input.file_name(name.to_owned());
    let media = match kind {
        "audio" => InputMedia::Audio(InputMediaAudio::new(input)),
        "photo" => InputMedia::Photo(InputMediaPhoto::new(input)),
        "video" => InputMedia::Video(InputMediaVideo::new(input).supports_streaming(true)),
        _ => InputMedia::Document(InputMediaDocument::new(input)),
    };
    let request = bot.edit_message_media(ChatId(chat_id), message_id, media);
    match menu_markup(choices) {
        Some(markup) => request.reply_markup(markup).await,
        None => request.await,
    }
    .map_err(|error| AppError::Dependency(error.to_string()))
}

fn file_ref(message: &Message, kind: &str) -> Result<TelegramFileRef, AppError> {
    let meta = message
        .photo()
        .and_then(|photos| photos.last())
        .map(|photo| &photo.file)
        .or_else(|| message.video().map(|video| &video.file))
        .or_else(|| message.audio().map(|audio| &audio.file))
        .or_else(|| message.document().map(|document| &document.file))
        .ok_or_else(|| AppError::Dependency("telegram did not return a file id".into()))?;
    Ok(TelegramFileRef {
        file_id: meta.id.0.clone(),
        file_unique_id: Some(meta.unique_id.0.clone()),
        kind: kind.to_owned(),
        artifact_id: uuid::Uuid::nil(),
    })
}

async fn actor_for(engine: &Engine, message: &Message) -> Result<Actor, AppError> {
    let external = message
        .from
        .as_ref()
        .map(|user| user.id.0.to_string())
        .unwrap_or_else(|| message.chat.id.0.to_string());
    let name = message.from.as_ref().map(|user| user.full_name());
    let user = engine.store_user(UserKind::Telegram, &external, name.as_deref()).await?;
    Ok(Actor { user_id: user.id, kind: UserKind::Telegram })
}

fn first_url(text: &str) -> Option<&str> {
    text.split_whitespace().find(|part| part.starts_with("http://") || part.starts_with("https://"))
}

fn parse_uuid(value: &str) -> Result<uuid::Uuid, AppError> {
    uuid::Uuid::parse_str(value).map_err(|_| AppError::Validation("job id must be a uuid".into()))
}

pub struct TeloxideSender {
    bot: Bot,
}

impl TeloxideSender {
    pub fn new(token: &str) -> Self {
        Self { bot: Bot::new(token) }
    }
}

#[async_trait::async_trait]
impl crate::application::TelegramSender for TeloxideSender {
    async fn send_text(&self, chat_id: i64, text: &str) -> Result<(), AppError> {
        self.bot
            .send_message(ChatId(chat_id), text)
            .await
            .map(|_| ())
            .map_err(|error| AppError::Dependency(error.to_string()))
    }

    async fn send_file(
        &self,
        chat_id: i64,
        path: &std::path::Path,
        kind: &str,
        caption: &str,
    ) -> Result<TelegramFileRef, AppError> {
        send_path(&self.bot, chat_id, path, kind, caption, &[]).await.map(|sent| sent.file)
    }

    async fn send_file_id(
        &self,
        chat_id: i64,
        kind: &str,
        file_id: &str,
        caption: &str,
    ) -> Result<(), AppError> {
        let filename = part_name(kind, None);
        send_input(
            &self.bot,
            chat_id,
            InputFile::file_id(file_id.to_owned().into()),
            kind,
            &filename,
            caption,
            &[],
        )
        .await
        .map(|_| ())
    }

    async fn send_file_menu(
        &self,
        chat_id: i64,
        path: &std::path::Path,
        kind: &str,
        caption: &str,
        choices: &[TelegramChoice],
    ) -> Result<DeliveredFile, AppError> {
        send_path(&self.bot, chat_id, path, kind, caption, choices).await
    }

    async fn send_file_id_menu(
        &self,
        chat_id: i64,
        kind: &str,
        file_id: &str,
        caption: &str,
        choices: &[TelegramChoice],
    ) -> Result<DeliveredFile, AppError> {
        let filename = part_name(kind, None);
        let message = send_input(
            &self.bot,
            chat_id,
            InputFile::file_id(file_id.to_owned().into()),
            kind,
            &filename,
            caption,
            choices,
        )
        .await?;
        Ok(DeliveredFile { file: file_ref(&message, kind)?, message_ids: vec![message.id.0] })
    }

    async fn edit_file_menu(
        &self,
        chat_id: i64,
        message_id: i32,
        path: &std::path::Path,
        kind: &str,
        choices: &[TelegramChoice],
    ) -> Result<DeliveredFile, AppError> {
        edit_path(&self.bot, chat_id, message_id, path, kind, choices).await
    }

    async fn edit_file_id_menu(
        &self,
        chat_id: i64,
        message_id: i32,
        kind: &str,
        file_id: &str,
        choices: &[TelegramChoice],
    ) -> Result<DeliveredFile, AppError> {
        edit_file_id(
            &self.bot,
            chat_id,
            message_id,
            &TelegramFileRef {
                file_id: file_id.to_owned(),
                file_unique_id: None,
                kind: kind.to_owned(),
                artifact_id: uuid::Uuid::nil(),
            },
            choices,
        )
        .await
    }

    async fn delete_message(&self, chat_id: i64, message_id: i32) -> Result<(), AppError> {
        self.bot
            .delete_message(ChatId(chat_id), MessageId(message_id))
            .await
            .map(|_| ())
            .map_err(|error| AppError::Dependency(error.to_string()))
    }

    async fn edit_text(&self, chat_id: i64, message_id: i32, text: &str) -> Result<(), AppError> {
        match self.bot.edit_message_text(ChatId(chat_id), MessageId(message_id), text).await {
            Ok(_) => Ok(()),
            Err(error) if unchanged_message(&error.to_string()) => Ok(()),
            Err(error) => Err(AppError::Dependency(error.to_string())),
        }
    }
}

pub fn profile_label(profile: ProcessingProfile) -> String {
    format!("{} {}", profile.quality_label(), profile.container.as_str())
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum AnimSignal {
    Spin,
    Track(uuid::Uuid),
    Clear,
    Fail(String),
}

const FRAME: Duration = Duration::from_millis(900);
const ANIMATION_CAP: Duration = Duration::from_secs(12 * 60);

async fn begin_animation(
    bot: &Bot,
    engine: &Arc<Engine>,
    user_id: uuid::Uuid,
    chat_id: i64,
    initial: AnimSignal,
) -> Result<(watch::Sender<AnimSignal>, MessageId), AppError> {
    let status = bot
        .send_message(ChatId(chat_id), download_status_text(0, None))
        .await
        .map_err(|error| AppError::Dependency(error.to_string()))?;
    let (tx, rx) = watch::channel(initial);
    let bot = bot.clone();
    let engine = Arc::clone(engine);
    let message_id = status.id;
    tokio::spawn(async move {
        animate(bot, engine, user_id, chat_id, message_id, rx).await;
    });
    Ok((tx, message_id))
}

async fn animate(
    bot: Bot,
    engine: Arc<Engine>,
    user_id: uuid::Uuid,
    chat_id: i64,
    message_id: MessageId,
    mut rx: watch::Receiver<AnimSignal>,
) {
    let started = Instant::now();
    let mut tick = 1u32;
    let mut misses = 0u8;
    let mut quiet = false;
    let mut sender_open = true;
    loop {
        let signal = rx.borrow().clone();
        if matches!(signal, AnimSignal::Spin | AnimSignal::Track(_))
            && started.elapsed() >= ANIMATION_CAP
        {
            let _ = edit_status(
                &bot,
                chat_id,
                message_id,
                "Still downloading. I'll send it when it's ready.",
            )
            .await;
            return;
        }
        match signal {
            AnimSignal::Clear => {
                dismiss_status(&bot, chat_id, message_id).await;
                return;
            }
            AnimSignal::Fail(text) => {
                let _ = edit_status(&bot, chat_id, message_id, &text).await;
                return;
            }
            AnimSignal::Spin => {
                if !quiet {
                    misses = note_paint(paint(&bot, chat_id, message_id, tick, None).await, misses);
                    quiet = misses >= 3;
                }
            }
            AnimSignal::Track(job_id) => match engine.job_for_user(user_id, job_id).await {
                Ok(job) if job.status.is_terminal() => {
                    if job.status == JobStatus::Completed {
                        dismiss_status(&bot, chat_id, message_id).await;
                    } else {
                        let _ = engine.silence_status_notice(job_id, user_id).await;
                        let text = job_terminal_text(job.status, job.failure_reason.as_deref());
                        let _ = edit_status(&bot, chat_id, message_id, &text).await;
                    }
                    return;
                }
                Ok(_) => {
                    if !quiet {
                        let percent = engine
                            .progress(job_id)
                            .await
                            .ok()
                            .flatten()
                            .and_then(|item| item.percent);
                        misses = note_paint(
                            paint(&bot, chat_id, message_id, tick, percent).await,
                            misses,
                        );
                        quiet = misses >= 3;
                    }
                }
                Err(error) => {
                    tracing::debug!(%error, "download status poll failed");
                    if !quiet {
                        misses =
                            note_paint(paint(&bot, chat_id, message_id, tick, None).await, misses);
                        quiet = misses >= 3;
                    }
                }
            },
        }
        tick = tick.wrapping_add(1);
        if sender_open {
            tokio::select! {
                _ = tokio::time::sleep(FRAME) => {}
                changed = rx.changed() => {
                    if changed.is_err() {
                        sender_open = false;
                    }
                }
            }
        } else {
            tokio::time::sleep(FRAME).await;
        }
    }
}

fn note_paint(painted: Result<(), ()>, misses: u8) -> u8 {
    if painted.is_ok() { 0 } else { misses.saturating_add(1) }
}

async fn paint(
    bot: &Bot,
    chat_id: i64,
    message_id: MessageId,
    tick: u32,
    percent: Option<u8>,
) -> Result<(), ()> {
    edit_status(bot, chat_id, message_id, &download_status_text(tick, percent)).await
}

async fn edit_status(bot: &Bot, chat_id: i64, message_id: MessageId, text: &str) -> Result<(), ()> {
    match bot.edit_message_text(ChatId(chat_id), message_id, text).await {
        Ok(_) => Ok(()),
        Err(error) if unchanged_message(&error.to_string()) => Ok(()),
        Err(error) => {
            tracing::debug!(%error, "status edit failed");
            Err(())
        }
    }
}

async fn dismiss_status(bot: &Bot, chat_id: i64, message_id: MessageId) {
    if bot.delete_message(ChatId(chat_id), message_id).await.is_err() {
        let _ = edit_status(bot, chat_id, message_id, "Sent.").await;
    }
}

fn part_name(kind: &str, path: Option<&std::path::Path>) -> String {
    let from_path = path
        .and_then(|path| path.file_name())
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty() && *name != "media.bin");
    match from_path {
        Some(name) => name.to_owned(),
        None => upload_name(kind).to_owned(),
    }
}

fn upload_name(kind: &str) -> &'static str {
    match kind {
        "photo" => "photo.jpg",
        "video" => "video.mp4",
        "audio" => "audio.mp3",
        _ => "file.bin",
    }
}

fn unchanged_message(error: &str) -> bool {
    error.to_ascii_lowercase().contains("message is not modified")
}

fn status_failure(text: &str) -> String {
    truncate_chars(&text.split_whitespace().collect::<Vec<_>>().join(" "), 240)
}

fn job_terminal_text(status: JobStatus, reason: Option<&str>) -> String {
    let line = match status {
        JobStatus::Cancelled => "Download cancelled.".to_owned(),
        JobStatus::Expired => "Download expired.".to_owned(),
        JobStatus::Failed => match reason.map(str::trim).filter(|text| !text.is_empty()) {
            Some(text) => text.to_owned(),
            None => "Download failed.".to_owned(),
        },
        _ => "Download failed.".to_owned(),
    };
    status_failure(&line)
}

fn truncate_chars(text: &str, max_chars: usize) -> String {
    if text.chars().count() <= max_chars {
        return text.to_owned();
    }
    let end = text
        .char_indices()
        .nth(max_chars.saturating_sub(1))
        .map(|(index, _)| index)
        .unwrap_or(text.len());
    format!("{}…", &text[..end])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failure_text_is_one_short_line() {
        let raw = "yt-dlp\nexited\n1";
        assert_eq!(status_failure(raw), "yt-dlp exited 1");
        let long = "x".repeat(300);
        let trimmed = status_failure(&long);
        assert_eq!(trimmed.chars().count(), 240);
        assert!(trimmed.ends_with('…'));
        assert_eq!(job_terminal_text(JobStatus::Cancelled, Some("ignored")), "Download cancelled.");
        assert_eq!(job_terminal_text(JobStatus::Failed, None), "Download failed.");
    }

    #[test]
    fn upload_names_use_extensions_phones_render() {
        assert_eq!(upload_name("photo"), "photo.jpg");
        assert_eq!(upload_name("video"), "video.mp4");
        assert_eq!(upload_name("audio"), "audio.mp3");
        assert_eq!(piece_kind("01.jpg"), "photo");
        assert_eq!(piece_kind("02.mp4"), "video");
        assert_eq!(piece_kind("10.jpg"), "photo");
    }

    #[test]
    fn album_drops_a_piece_telegram_cannot_accept() {
        let photo = vec![0xFF, 0xD8, 0xFF];
        let video = vec![0, 0, 0, 0x18, b'f', b't', b'y', b'p'];
        let kept = fitting_pieces(
            vec![("01.jpg".into(), photo.clone()), ("02.mp4".into(), video.clone())],
            4,
        )
        .expect("pieces");
        assert_eq!(kept, vec![("01.jpg".into(), photo)]);
        let oversized = fitting_pieces(vec![("01.mp4".into(), video)], 4).expect_err("too big");
        assert!(oversized.is_oversized_notice());
        let empty = fitting_pieces(Vec::new(), 4).expect_err("empty");
        assert!(!empty.is_oversized_notice());
    }

    #[test]
    fn part_name_drops_media_bin() {
        let stored = std::path::Path::new("/tmp/deliver/media.bin");
        assert_eq!(part_name("video", Some(stored)), "video.mp4");
        assert_eq!(part_name("photo", Some(stored)), "photo.jpg");
        assert_eq!(part_name("audio", Some(stored)), "audio.mp3");
        assert_eq!(part_name("document", Some(stored)), "file.bin");
        assert_ne!(part_name("document", Some(stored)), "media.bin");
        let webm = std::path::Path::new("/tmp/deliver/video.webm");
        assert_eq!(part_name("video", Some(webm)), "video.webm");
    }
}
