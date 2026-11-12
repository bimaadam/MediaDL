use std::path::PathBuf;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::domain::{
    Actor, Artifact, DeliveryChannel, FailureClass, JobStatus, MediaIdentity, PlatformKind,
    ProcessingProfile,
};

#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    pub max_download_bytes: u64,
    pub download_timeout: Duration,
    pub tool_timeout: Duration,
    pub lock_ttl: Duration,
    pub lease: Duration,
    pub lease_renew_every: Duration,
    pub artifact_cache_ttl: Duration,
    pub metadata_cache_ttl: Duration,
    pub telegram_file_ttl: Duration,
    pub selection_ttl: Duration,
    pub max_attempts: u32,
    pub retry_base: Duration,
    pub retry_cap: Duration,
    pub rate_limit_per_minute: u32,
    pub daily_quota: u32,
    pub telegram_max_upload_bytes: u64,
    pub multipart_threshold_bytes: u64,
    pub job_expiry: Duration,
    pub uploading_grace: Duration,
    pub tmp_dir: PathBuf,
    pub outbox_batch: i64,
    pub worker_concurrency: usize,
    pub per_platform_concurrency: usize,
    pub ytdlp_path: PathBuf,
    pub ffmpeg_path: PathBuf,
    pub ffprobe_path: PathBuf,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            max_download_bytes: 2 * 1024 * 1024 * 1024,
            download_timeout: Duration::from_secs(15 * 60),
            tool_timeout: Duration::from_secs(10 * 60),
            lock_ttl: Duration::from_secs(30),
            lease: Duration::from_secs(60),
            lease_renew_every: Duration::from_secs(20),
            artifact_cache_ttl: Duration::from_secs(7 * 24 * 3600),
            metadata_cache_ttl: Duration::from_secs(6 * 3600),
            telegram_file_ttl: Duration::from_secs(30 * 24 * 3600),
            selection_ttl: Duration::from_secs(15 * 60),
            max_attempts: 5,
            retry_base: Duration::from_secs(2),
            retry_cap: Duration::from_secs(300),
            rate_limit_per_minute: 20,
            daily_quota: 100,
            telegram_max_upload_bytes: TELEGRAM_FILE_BYTES,
            multipart_threshold_bytes: 64 * 1024 * 1024,
            job_expiry: Duration::from_secs(24 * 3600),
            uploading_grace: Duration::from_secs(2 * 3600),
            tmp_dir: std::env::temp_dir().join("media-engine"),
            outbox_batch: 50,
            worker_concurrency: 4,
            per_platform_concurrency: 2,
            ytdlp_path: PathBuf::from("yt-dlp"),
            ffmpeg_path: PathBuf::from("ffmpeg"),
            ffprobe_path: PathBuf::from("ffprobe"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SubmitCommand {
    pub url: String,
    pub profile: ProcessingProfile,
    pub actor: Actor,
    pub channel: DeliveryChannel,
}

#[derive(Debug, Clone)]
pub enum SubmitOutcome {
    Ready { media_id: Uuid, artifact: Artifact, identity: MediaIdentity },
    Queued { job_id: Uuid, media_id: Uuid, identity: MediaIdentity, attached: bool },
    TelegramCached { file: TelegramFileRef, identity: MediaIdentity },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelegramFileRef {
    pub file_id: String,
    pub file_unique_id: Option<String>,
    pub kind: String,
    pub artifact_id: Uuid,
}

#[derive(Debug, Clone)]
pub struct NewJobDraft {
    pub job_id: Uuid,
    pub platform: PlatformKind,
    pub content_id: String,
    pub canonical_url: String,
    pub profile: ProcessingProfile,
    pub profile_hash: String,
    pub actor: Actor,
    pub channel: DeliveryChannel,
    pub max_attempts: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitJob {
    Created { job_id: Uuid, media_id: Uuid },
    Attached { job_id: Uuid, media_id: Uuid },
}

#[derive(Debug, Clone)]
pub struct MediaRecord {
    pub id: Uuid,
    pub platform: PlatformKind,
    pub content_id: String,
    pub canonical_url: Option<String>,
    pub title: Option<String>,
    pub duration_seconds: Option<i32>,
}

#[derive(Debug, Clone)]
pub struct Subscription {
    pub id: Uuid,
    pub job_id: Uuid,
    pub user_id: Uuid,
    pub channel: DeliveryChannel,
    pub notify_status: String,
}

#[derive(Debug, Clone)]
pub struct ClaimedJob {
    pub id: Uuid,
    pub media_id: Uuid,
    pub platform: PlatformKind,
    pub content_id: String,
    pub canonical_url: String,
    pub profile: ProcessingProfile,
    pub profile_hash: String,
    pub attempt_count: u32,
    pub max_attempts: u32,
    pub status: JobStatus,
}

#[derive(Debug, Clone)]
pub struct NewAttempt {
    pub id: Uuid,
    pub job_id: Uuid,
    pub attempt_number: i32,
    pub worker_id: String,
}

#[derive(Debug, Clone)]
pub enum JobSettlement {
    Requeue { job_id: Uuid, worker_id: String, attempt_id: Uuid, delay: Duration, reason: String },
    Fail { job_id: Uuid, worker_id: String, attempt_id: Uuid, class: FailureClass, reason: String },
    Cancel { job_id: Uuid, worker_id: String, attempt_id: Uuid },
}

#[derive(Debug, Clone)]
pub struct NewArtifact {
    pub id: Uuid,
    pub media_id: Uuid,
    pub profile_hash: String,
    pub object_key: String,
    pub content_type: String,
}

#[derive(Debug, Clone)]
pub struct JobCompletion {
    pub job_id: Uuid,
    pub worker_id: String,
    pub attempt_id: Uuid,
    pub artifact_id: Uuid,
    pub checksum_sha256: String,
    pub byte_size: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MediaMetadata {
    pub title: Option<String>,
    pub duration_seconds: Option<i32>,
    pub profiles: Vec<ProcessingProfile>,
}

#[derive(Debug, Clone)]
pub struct OutboxEvent {
    pub id: i64,
    pub aggregate_id: Uuid,
    pub event_type: String,
    pub payload: serde_json::Value,
}

#[derive(Debug, Clone)]
pub struct QueueMessage {
    pub job_id: Uuid,
}

#[derive(Debug, Clone)]
pub struct QueueDelivery {
    pub id: String,
    pub message: QueueMessage,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobProgress {
    pub phase: String,
    pub percent: Option<u8>,
}

#[derive(Debug, Clone)]
pub struct TelegramChoice {
    pub label: String,
    pub data: String,
}

pub fn delivery_kind(content_type: &str) -> &'static str {
    if content_type.starts_with("audio/") {
        "audio"
    } else if content_type.starts_with("video/") {
        "video"
    } else if content_type.starts_with("image/") {
        "photo"
    } else if content_type == "application/zip" {
        "album"
    } else {
        "document"
    }
}

/// Shown when a stored file is still over Telegram's upload limit.
/// The chat does not get an API content URL.
pub const TELEGRAM_OVERSIZED: &str = "This video is too large for Telegram. Choose a smaller size.";

/// One Telegram `sendPhoto` / `sendVideo` / media-group item. Bot API rejects
/// a larger upload. An album archive may be bigger because each item is sent alone.
pub const TELEGRAM_FILE_BYTES: u64 = 50 * 1024 * 1024;

/// Multipart name Telegram clients render. Never `media.bin`: phones use this
/// name, and an unrecognized extension shows up as a file instead of a photo.
pub fn upload_filename(content_type: &str) -> &'static str {
    match content_type {
        "video/webm" => "video.webm",
        "video/x-matroska" => "video.mkv",
        "audio/mp4" => "audio.m4a",
        "image/png" => "photo.png",
        "image/webp" => "photo.webp",
        "image/gif" => "photo.gif",
        "application/zip" => "album.zip",
        _ => match delivery_kind(content_type) {
            "photo" => "photo.jpg",
            "video" => "video.mp4",
            "audio" => "audio.mp3",
            _ => "file.bin",
        },
    }
}

pub fn format_menu_token(job_id: Uuid, chat_id: i64) -> String {
    format!("{job_id}:{chat_id}")
}

pub fn format_menu_choices(token: &str) -> Vec<TelegramChoice> {
    ProcessingProfile::format_menu()
        .into_iter()
        .enumerate()
        .map(|(index, profile)| TelegramChoice {
            label: profile.menu_label(),
            data: format!("s:{token}:{index}"),
        })
        .collect()
}

/// The button attached to a delivered file. Resolutions stay behind it.
pub fn root_menu_choices(token: &str) -> Vec<TelegramChoice> {
    vec![TelegramChoice { label: "Menu".to_owned(), data: format!("m:{token}") }]
}

/// Rows for the profiles the platform actually published, plus Back.
pub fn resolution_menu_choices(token: &str, profiles: &[ProcessingProfile]) -> Vec<TelegramChoice> {
    let mut choices = profiles
        .iter()
        .enumerate()
        .map(|(index, profile)| TelegramChoice {
            label: profile.menu_label(),
            data: format!("s:{token}:{index}"),
        })
        .collect::<Vec<_>>();
    choices.push(TelegramChoice { label: "Back".to_owned(), data: format!("b:{token}") });
    choices
}

/// Spinner plus a bar. Each tick changes the text so a message edit is accepted.
pub fn download_status_text(tick: u32, percent: Option<u8>) -> String {
    const SPIN: [char; 8] = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧'];
    let spin = SPIN[(tick as usize) % SPIN.len()];
    let percent = percent.map(|value| value.min(100));
    let bar = match percent {
        Some(value) => filled_bar(value),
        None => sliding_bar(tick),
    };
    match percent {
        Some(value) => format!("{spin} Downloading {bar} {value}%"),
        None => format!("{spin} Downloading {bar}"),
    }
}

fn filled_bar(percent: u8) -> String {
    const WIDTH: usize = 10;
    let filled = (usize::from(percent) * WIDTH) / 100;
    (0..WIDTH).map(|index| if index < filled { '█' } else { '░' }).collect()
}

fn sliding_bar(tick: u32) -> String {
    const WIDTH: usize = 10;
    const BLOCK: usize = 3;
    let head = (tick as usize) % (WIDTH + BLOCK);
    (0..WIDTH).map(|index| if index <= head && head < index + BLOCK { '▓' } else { '░' }).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_menu_callbacks_fit_telegram() {
        let token = format_menu_token(Uuid::now_v7(), i64::MIN);
        let choices = format_menu_choices(&token);
        let labels: Vec<_> = choices.iter().map(|choice| choice.label.as_str()).collect();
        assert_eq!(labels, ["Best", "360p", "480p", "720p", "1080p", "Audio"]);
        for choice in &choices {
            assert!(choice.data.len() <= 64, "{}", choice.data);
            assert!(choice.data.starts_with("s:"));
        }
        let root = root_menu_choices(&token);
        assert_eq!(root.len(), 1);
        assert_eq!(root[0].label, "Menu");
        assert!(root[0].data.len() <= 64, "{}", root[0].data);
        assert!(root[0].data.starts_with("m:"));
        let published = vec![
            ProcessingProfile::best_video(),
            ProcessingProfile::video_mp4(576).expect("height"),
            ProcessingProfile::audio_mp3(),
        ];
        let opened = resolution_menu_choices(&token, &published);
        let opened_labels: Vec<_> = opened.iter().map(|choice| choice.label.as_str()).collect();
        assert_eq!(opened_labels, ["Best", "576p", "Audio", "Back"]);
        assert!(!opened_labels.contains(&"360p"));
        assert_eq!(opened.len(), 4);
        for choice in &opened {
            assert!(choice.data.len() <= 64, "{}", choice.data);
        }
        assert!(opened[3].data.starts_with("b:"));
    }

    #[test]
    fn download_status_changes_every_tick() {
        let first = download_status_text(0, None);
        let second = download_status_text(1, None);
        assert_ne!(first, second);
        assert!(first.contains("Downloading"));
        let holding = download_status_text(3, Some(40));
        assert_ne!(holding, download_status_text(4, Some(40)));
        assert!(holding.contains("40%"));
        assert!(download_status_text(0, Some(250)).contains("100%"));
    }

    #[test]
    fn upload_filename_is_never_media_bin() {
        for content_type in [
            "video/mp4",
            "video/webm",
            "video/x-matroska",
            "audio/mpeg",
            "audio/mp4",
            "image/jpeg",
            "image/png",
            "application/zip",
            "application/octet-stream",
        ] {
            let name = upload_filename(content_type);
            assert_ne!(name, "media.bin", "{content_type}");
            assert!(name.contains('.'), "{content_type}");
        }
        assert_eq!(upload_filename("video/mp4"), "video.mp4");
        assert_eq!(upload_filename("video/webm"), "video.webm");
        assert_eq!(upload_filename("audio/mpeg"), "audio.mp3");
        assert_eq!(upload_filename("image/jpeg"), "photo.jpg");
        assert_eq!(upload_filename("application/zip"), "album.zip");
    }

    #[test]
    fn replacement_removes_the_old_bubbles_only() {
        assert_eq!(messages_to_remove(&[10, 11, 12], 11, true), vec![10, 12]);
        assert_eq!(messages_to_remove(&[11], 11, true), Vec::<i32>::new());
        assert_eq!(messages_to_remove(&[4, 5], 5, false), vec![4, 5]);
        assert_eq!(messages_to_remove(&[], 9, true), Vec::<i32>::new());
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FormatSelection {
    pub url: String,
    pub user_id: Uuid,
    pub chat_id: i64,
    pub profiles: Vec<ProcessingProfile>,
    /// Message a resolution change edits. Empty for the first send.
    #[serde(default)]
    pub replace_message_id: Option<i32>,
    /// Bubbles from the previous delivery. A replacement deletes these.
    #[serde(default)]
    pub drop_message_ids: Vec<i32>,
}

/// Bubbles to delete after a resolution change.
/// Keeping the edited message stops the new file from becoming a second post.
pub fn messages_to_remove(previous: &[i32], replaced: i32, keep_replaced: bool) -> Vec<i32> {
    let mut ids: Vec<i32> = previous.iter().copied().filter(|id| *id != replaced).collect();
    if !keep_replaced {
        ids.push(replaced);
    }
    ids.sort_unstable();
    ids.dedup();
    ids
}

#[derive(Debug, Clone)]
pub struct DeliveredFile {
    pub file: TelegramFileRef,
    pub message_ids: Vec<i32>,
}

#[derive(Debug, Clone)]
pub struct IdempotencyRecord {
    pub request_hash: String,
    pub status: i32,
    pub body: serde_json::Value,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct NewIdempotency {
    pub id: Uuid,
    pub user_id: Uuid,
    pub key: String,
    pub request_hash: String,
    pub status: i32,
    pub body: serde_json::Value,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct AuditEntry {
    pub id: Uuid,
    pub actor_user_id: Option<Uuid>,
    pub action: String,
    pub target_type: String,
    pub target_id: Option<String>,
    pub request_id: Option<Uuid>,
    pub details: serde_json::Value,
}

#[derive(Debug, Clone)]
pub struct UserRecord {
    pub id: Uuid,
    pub kind: crate::domain::UserKind,
    pub external_ref: String,
    pub display_name: Option<String>,
    pub status: String,
}

#[derive(Debug, Clone)]
pub struct StatusNotice {
    pub subscription_id: Uuid,
    pub chat_id: i64,
    pub message_id: Option<i32>,
    pub text: String,
}

#[derive(Debug, Clone)]
pub struct PendingNotification {
    pub subscription_id: Uuid,
    pub job_id: Uuid,
    pub chat_id: i64,
    pub message_id: Option<i32>,
    pub bot_id: String,
    pub artifact: Artifact,
    pub identity: MediaIdentity,
}

#[derive(Debug, Clone)]
pub struct PlatformInfo {
    pub code: String,
    pub display_name: String,
    pub enabled: bool,
}

#[derive(Debug, Clone)]
pub struct DownloadedMedia {
    pub path: PathBuf,
    pub byte_size: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HandleResult {
    Completed { artifact_id: Uuid },
    Duplicate,
    RetryScheduled,
    Failed,
    LostLease,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelOutcome {
    Cancelled,
    NotCancellable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdempotencyPut {
    Stored,
    Conflict,
}
