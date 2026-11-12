//! Platform extraction. yt-dlp receives an argument vector built from the
//! processing profile. Direct files are fetched with the pinned HTTP client.
//! This adapter does not accept cookies, credentials, or DRM options.

use std::path::{Path, PathBuf};
use std::time::Duration;

use async_trait::async_trait;
use tokio::io::AsyncWriteExt;
use url::Url;

use crate::application::{
    AppError, ClaimedJob, DownloadedMedia, JobProgress, MediaExtractor, MediaMetadata,
    MetadataSource, PlatformInfo, PlatformRegistry, RuntimeConfig,
};
use crate::domain::{IdentifiedContent, MediaKind, PlatformKind, ProcessingProfile, identify_url};
use crate::shared::command::{self, parse_download_percent};

use super::guard::SafeHttp;

pub struct YtDlpExtractor {
    program: PathBuf,
    http: std::sync::Arc<SafeHttp>,
}

impl YtDlpExtractor {
    pub fn new(program: PathBuf, http: std::sync::Arc<SafeHttp>) -> Self {
        Self { program, http }
    }
}

pub fn download_args(
    profile: ProcessingProfile,
    directory: &Path,
    max_bytes: u64,
    url: &str,
) -> Vec<String> {
    let output = directory.join("media.%(ext)s");
    vec![
        "--no-playlist".into(),
        "--no-progress".into(),
        "--newline".into(),
        "--restrict-filenames".into(),
        "--no-mtime".into(),
        "-f".into(),
        profile.ytdlp_format(),
        "--max-filesize".into(),
        max_bytes.to_string(),
        "-o".into(),
        output.display().to_string(),
        "--".into(),
        url.to_owned(),
    ]
}

fn host_matches(platform: PlatformKind, host: &str) -> bool {
    let host = host.trim_end_matches('.').to_ascii_lowercase();
    match platform {
        PlatformKind::YouTube => {
            host == "youtu.be"
                || host == "youtube.com"
                || host.ends_with(".youtube.com")
                || host == "youtube-nocookie.com"
                || host.ends_with(".youtube-nocookie.com")
        }
        PlatformKind::TikTok => host == "tiktok.com" || host.ends_with(".tiktok.com"),
        PlatformKind::Facebook => {
            host == "facebook.com" || host.ends_with(".facebook.com") || host == "fb.watch"
        }
        PlatformKind::Instagram => host == "instagram.com" || host.ends_with(".instagram.com"),
        PlatformKind::X => {
            host == "x.com"
                || host.ends_with(".x.com")
                || host == "twitter.com"
                || host.ends_with(".twitter.com")
        }
        PlatformKind::Reddit => {
            host == "reddit.com"
                || host.ends_with(".reddit.com")
                || host == "v.redd.it"
                || host == "redd.it"
        }
        PlatformKind::Direct => true,
    }
}

fn classify_stderr(stderr: &str, status: i32) -> AppError {
    let lower = stderr.to_ascii_lowercase();
    if lower.contains("cannot parse data") {
        return AppError::Validation(
            "the extractor could not read a public video from this post".into(),
        );
    }
    // A missing height is a property of the post, not a blip. Retrying it leaves
    // the chat on "Downloading" until the attempt budget is spent.
    if lower.contains("requested format is not available") {
        return AppError::Validation(
            "That resolution is not available for this video. Try Best or another size.".into(),
        );
    }
    // Anonymous rate limits and login walls are not retried, and no cookie file is read.
    if lower.contains("rate-limit")
        || lower.contains("rate limit")
        || lower.contains("redirected to the login page")
        || lower.contains("cookies-from-browser")
    {
        return AppError::Validation(
            "The site is blocking anonymous downloads right now. Try again later.".into(),
        );
    }
    // A post with no video is permanent. Retrying reprints the extractor's
    // bug-report tail into the chat.
    if lower.contains("no video formats found") {
        return AppError::Validation("This post has no public video.".into());
    }
    if lower.contains("drm")
        || lower.contains("sign in")
        || lower.contains("login required")
        || lower.contains("private video")
        || lower.contains("this video is private")
    {
        return AppError::Restricted;
    }
    let end = stderr.len().saturating_sub(400);
    AppError::Dependency(format!("yt-dlp exited {status}: {}", stderr[end..].trim()))
}

#[async_trait]
impl MetadataSource for YtDlpExtractor {
    async fn inspect(
        &self,
        content: &IdentifiedContent,
        profile: &ProcessingProfile,
    ) -> Result<MediaMetadata, AppError> {
        let url = Url::parse(&content.canonical_url)
            .map_err(|_| AppError::Validation("canonical url is invalid".into()))?;
        let host = url.host_str().unwrap_or("");
        if !host_matches(content.platform, host) {
            return Err(AppError::Validation(
                "refusing to inspect a host outside the platform allowlist".into(),
            ));
        }
        let args = vec![
            "--no-playlist".into(),
            "--skip-download".into(),
            "--dump-single-json".into(),
            "--".into(),
            content.canonical_url.clone(),
        ];
        let result = command::run(&self.program, &args, Duration::from_secs(60), None).await?;
        if result.status != 0 {
            return Err(classify_stderr(&result.stderr, result.status));
        }
        let parsed: serde_json::Value = serde_json::from_str(&result.stdout)
            .map_err(|error| AppError::Dependency(format!("yt-dlp metadata: {error}")))?;
        let title = parsed.get("title").and_then(|value| value.as_str()).map(str::to_owned);
        let duration_seconds =
            parsed.get("duration").and_then(|value| value.as_f64()).map(|seconds| seconds as i32);
        let profiles = parsed
            .get("formats")
            .and_then(|value| value.as_array())
            .map(|formats| profiles_from_formats(formats))
            .filter(|profiles| !profiles.is_empty())
            .unwrap_or_else(|| vec![*profile]);
        Ok(MediaMetadata { title, duration_seconds, profiles })
    }

    async fn has_public_photos(&self, content: &IdentifiedContent) -> Result<bool, AppError> {
        if content.platform != PlatformKind::Facebook {
            return Ok(false);
        }
        let url = Url::parse(&content.canonical_url)
            .map_err(|_| AppError::Validation("canonical url is invalid".into()))?;
        if !host_matches(content.platform, url.host_str().unwrap_or("")) {
            return Err(AppError::Validation(
                "refusing to inspect a host outside the platform allowlist".into(),
            ));
        }
        // A share link that still redirects is not an album. Video download handles it.
        let html = match fetch_text(&self.http, &url, 3_000_000).await {
            Ok(html) => html,
            Err(AppError::Dependency(text)) if text.starts_with("page status") => {
                return Ok(false);
            }
            Err(error) => return Err(error),
        };
        Ok(!facebook_album_media(&html).is_empty())
    }
}

/// Menu rows from the extractor's format list. Heights that are not present
/// are not offered. `Best` is the extractor's own choice, not a guessed ladder.
pub fn profiles_from_formats(formats: &[serde_json::Value]) -> Vec<ProcessingProfile> {
    let mut heights = std::collections::BTreeSet::new();
    let mut audio = false;
    for format in formats {
        if storyboard(format) {
            continue;
        }
        let acodec = codec(format, "acodec");
        if acodec != "none" {
            audio = true;
        }
        let vcodec = codec(format, "vcodec");
        if vcodec == "none" {
            continue;
        }
        if let Some(height) = format_height(format) {
            heights.insert(height);
        }
    }
    let mut profiles = vec![ProcessingProfile::best_video()];
    let heights: Vec<u16> = heights.into_iter().collect();
    let start = heights.len().saturating_sub(10);
    for height in heights.into_iter().skip(start) {
        if let Ok(profile) = ProcessingProfile::video_mp4(height) {
            profiles.push(profile);
        }
    }
    if audio {
        profiles.push(ProcessingProfile::audio_mp3());
    }
    profiles
}

fn codec(format: &serde_json::Value, field: &str) -> String {
    format.get(field).and_then(|value| value.as_str()).unwrap_or("none").trim().to_ascii_lowercase()
}

fn storyboard(format: &serde_json::Value) -> bool {
    let id = format.get("format_id").and_then(|value| value.as_str()).unwrap_or("");
    let note = format.get("format_note").and_then(|value| value.as_str()).unwrap_or("");
    id.to_ascii_lowercase().contains("storyboard")
        || note.to_ascii_lowercase().contains("storyboard")
}

fn format_height(format: &serde_json::Value) -> Option<u16> {
    let raw = format.get("height")?;
    let height = raw.as_u64().or_else(|| raw.as_f64().map(|value| value.round() as u64))?;
    u16::try_from(height).ok().filter(|height| (1..=4320).contains(height))
}

#[async_trait]
impl MediaExtractor for YtDlpExtractor {
    async fn download(
        &self,
        claimed: &ClaimedJob,
        directory: &Path,
        limits: &RuntimeConfig,
        progress: tokio::sync::mpsc::Sender<JobProgress>,
    ) -> Result<DownloadedMedia, AppError> {
        let url = Url::parse(&claimed.canonical_url)
            .map_err(|_| AppError::Validation("canonical url is invalid".into()))?;
        let host = url.host_str().unwrap_or("");
        if !host_matches(claimed.platform, host) {
            return Err(AppError::Validation(
                "refusing to download a host outside the platform allowlist".into(),
            ));
        }
        if claimed.platform == PlatformKind::Direct {
            return self.download_direct(&url, directory, limits.max_download_bytes).await;
        }
        if claimed.profile.kind == MediaKind::Image {
            return match claimed.platform {
                PlatformKind::Facebook => {
                    self.download_facebook_photos(
                        &url,
                        directory,
                        limits.max_download_bytes,
                        limits.telegram_max_upload_bytes,
                    )
                    .await
                }
                PlatformKind::Instagram => {
                    self.download_instagram_photo(&url, directory, limits.max_download_bytes).await
                }
                _ => Err(AppError::Validation(
                    "photo download is only supported for a public Facebook or Instagram post"
                        .into(),
                )),
            };
        }
        let _ = identify_url(&url)?;
        let args =
            download_args(claimed.profile, directory, limits.max_download_bytes, url.as_str());
        let (line_tx, mut lines) = tokio::sync::mpsc::channel::<String>(32);
        let forward = progress.clone();
        let reporter = tokio::spawn(async move {
            while let Some(line) = lines.recv().await {
                if let Some(percent) = parse_download_percent(&line) {
                    let _ = forward
                        .try_send(JobProgress { phase: "download".into(), percent: Some(percent) });
                }
            }
        });
        let result =
            command::run(&self.program, &args, limits.download_timeout, Some(line_tx)).await?;
        reporter.abort();
        if result.status != 0 {
            let error = classify_stderr(&result.stderr, result.status);
            // A photo post has no video stream. Use the public preview instead of the error.
            if claimed.platform == PlatformKind::Instagram
                && is_missing_video(&error)
                && let Ok(photo) =
                    self.download_instagram_photo(&url, directory, limits.max_download_bytes).await
            {
                return Ok(photo);
            }
            return Err(error);
        }
        let path = newest_file(directory).await?;
        let byte_size = tokio::fs::metadata(&path)
            .await
            .map_err(|error| AppError::Dependency(error.to_string()))?
            .len();
        if byte_size > limits.max_download_bytes {
            return Err(AppError::TooLarge);
        }
        let _ = progress.try_send(JobProgress { phase: "download".into(), percent: Some(100) });
        Ok(DownloadedMedia { path, byte_size })
    }
}

impl YtDlpExtractor {
    async fn download_direct(
        &self,
        url: &Url,
        directory: &Path,
        max_bytes: u64,
    ) -> Result<DownloadedMedia, AppError> {
        let response = self
            .http
            .pinned_get(url)
            .await?
            .send()
            .await
            .map_err(|error| AppError::Dependency(format!("direct download: {error}")))?;
        if !response.status().is_success() {
            return Err(AppError::Dependency(format!(
                "direct download status {}",
                response.status()
            )));
        }
        if response.content_length().is_some_and(|length| length > max_bytes) {
            return Err(AppError::TooLarge);
        }
        let path = directory.join("media.bin");
        let mut file = tokio::fs::File::create(&path)
            .await
            .map_err(|error| AppError::Dependency(error.to_string()))?;
        let mut stream = response.bytes_stream();
        use futures::StreamExt;
        let mut total = 0_u64;
        while let Some(chunk) = stream.next().await {
            let chunk =
                chunk.map_err(|error| AppError::Dependency(format!("direct read: {error}")))?;
            total = total.saturating_add(chunk.len() as u64);
            if total > max_bytes {
                return Err(AppError::TooLarge);
            }
            file.write_all(&chunk)
                .await
                .map_err(|error| AppError::Dependency(error.to_string()))?;
        }
        file.flush().await.map_err(|error| AppError::Dependency(error.to_string()))?;
        Ok(DownloadedMedia { path, byte_size: total })
    }

    async fn download_facebook_photos(
        &self,
        url: &Url,
        directory: &Path,
        max_bytes: u64,
        telegram_max: u64,
    ) -> Result<DownloadedMedia, AppError> {
        let html = fetch_text(&self.http, url, 3_000_000).await?;
        let items = facebook_album_media(&html);
        if items.is_empty() {
            return Err(AppError::Validation("this Facebook post has no public photo".into()));
        }
        let mut files = Vec::new();
        let mut total = 0u64;
        let mut last_error = None;
        for (index, item) in items.iter().enumerate() {
            let room = max_bytes
                .saturating_sub(total)
                .min(crate::shared::stored_zip::ALBUM_MAX_BYTES.saturating_sub(total));
            let cap = room.min(telegram_max);
            if cap < 3 {
                break;
            }
            let media_url = match Url::parse(&item.url) {
                Ok(parsed) => parsed,
                Err(_) => {
                    last_error = Some(AppError::Validation("photo url is invalid".into()));
                    continue;
                }
            };
            if !host_is_fbcdn(media_url.host_str().unwrap_or("")) {
                last_error = Some(AppError::Validation("photo host is not a public cdn".into()));
                continue;
            }
            match fetch_album_file(&self.http, &media_url, cap, item.video).await {
                Ok(bytes) => {
                    total = total.saturating_add(bytes.len() as u64);
                    let ext = if item.video { "mp4" } else { "jpg" };
                    files.push((format!("{:02}.{ext}", index + 1), bytes));
                }
                Err(error) => last_error = Some(error),
            }
        }
        if files.is_empty() {
            return Err(last_error.unwrap_or_else(|| {
                AppError::Validation("this Facebook post has no public photo".into())
            }));
        }
        if files.len() == 1 && files[0].0.ends_with(".jpg") {
            let path = directory.join("photo.jpg");
            tokio::fs::write(&path, &files[0].1)
                .await
                .map_err(|error| AppError::Dependency(error.to_string()))?;
            return Ok(DownloadedMedia { path, byte_size: files[0].1.len() as u64 });
        }
        let zipped = crate::shared::stored_zip::write_stored_zip(&files)?;
        let path = directory.join("album.zip");
        let byte_size = zipped.len() as u64;
        tokio::fs::write(&path, &zipped)
            .await
            .map_err(|error| AppError::Dependency(error.to_string()))?;
        Ok(DownloadedMedia { path, byte_size })
    }

    /// One public preview from Instagram's oembed endpoint. No cookie and no session.
    /// The original carousel file stays behind the login wall; this is the JPEG they still serve.
    async fn download_instagram_photo(
        &self,
        url: &Url,
        directory: &Path,
        max_bytes: u64,
    ) -> Result<DownloadedMedia, AppError> {
        let endpoint = instagram_oembed_endpoint(url)?;
        let body = fetch_text(&self.http, &endpoint, 200_000).await?;
        let photo_url = instagram_oembed_thumbnail(&body)?;
        let parsed = Url::parse(&photo_url)
            .map_err(|_| AppError::Validation("public photo url is invalid".into()))?;
        let bytes = fetch_jpeg(&self.http, &parsed, max_bytes).await?;
        let path = directory.join("photo.jpg");
        tokio::fs::write(&path, &bytes)
            .await
            .map_err(|error| AppError::Dependency(error.to_string()))?;
        Ok(DownloadedMedia { path, byte_size: bytes.len() as u64 })
    }
}

/// Maps a public URL onto a platform. Short links are expanded by the URL guard
/// before they reach this registry.
pub struct ProviderRegistry;

#[async_trait]
impl PlatformRegistry for ProviderRegistry {
    async fn identify(&self, url: &Url) -> Result<IdentifiedContent, AppError> {
        identify_url(url)?
            .ok_or_else(|| AppError::Validation("no supported platform matched this url".into()))
    }

    fn supported(&self) -> Vec<PlatformInfo> {
        PlatformKind::all()
            .iter()
            .copied()
            .map(|platform| PlatformInfo {
                code: platform.code().to_owned(),
                display_name: platform.display_name().to_owned(),
                enabled: true,
            })
            .collect()
    }
}

async fn fetch_text(http: &SafeHttp, url: &Url, max_bytes: usize) -> Result<String, AppError> {
    let response = http
        .pinned_get(url)
        .await?
        .send()
        .await
        .map_err(|error| AppError::Dependency(format!("page request failed: {error}")))?;
    if !response.status().is_success() {
        return Err(AppError::Dependency(format!("page status {}", response.status())));
    }
    let bytes = response
        .bytes()
        .await
        .map_err(|error| AppError::Dependency(format!("page read failed: {error}")))?;
    if bytes.len() > max_bytes {
        return Err(AppError::TooLarge);
    }
    Ok(String::from_utf8_lossy(&bytes).into_owned())
}

async fn fetch_jpeg(http: &SafeHttp, url: &Url, max_bytes: u64) -> Result<Vec<u8>, AppError> {
    if max_bytes < 3 {
        return Err(AppError::TooLarge);
    }
    let response = http
        .pinned_get(url)
        .await?
        .send()
        .await
        .map_err(|error| AppError::Dependency(format!("photo request failed: {error}")))?;
    if !response.status().is_success() {
        return Err(AppError::Dependency(format!("photo status {}", response.status())));
    }
    if response.content_length().is_some_and(|length| length > max_bytes) {
        return Err(AppError::TooLarge);
    }
    let bytes = response
        .bytes()
        .await
        .map_err(|error| AppError::Dependency(format!("photo read failed: {error}")))?;
    if bytes.len() as u64 > max_bytes {
        return Err(AppError::TooLarge);
    }
    if bytes.len() < 3 || bytes[0] != 0xFF || bytes[1] != 0xD8 || bytes[2] != 0xFF {
        return Err(AppError::Validation("public photo was not a jpeg".into()));
    }
    Ok(bytes.to_vec())
}

const ALBUM_FETCH_TIMEOUT: Duration = Duration::from_secs(120);

async fn fetch_album_file(
    http: &SafeHttp,
    url: &Url,
    max_bytes: u64,
    video: bool,
) -> Result<Vec<u8>, AppError> {
    let minimum: u64 = if video { 12 } else { 3 };
    if max_bytes < minimum {
        return Err(AppError::TooLarge);
    }
    let response = http
        .pinned_get(url)
        .await?
        .timeout(ALBUM_FETCH_TIMEOUT)
        .send()
        .await
        .map_err(|error| AppError::Dependency(format!("media request failed: {error}")))?;
    if !response.status().is_success() {
        return Err(AppError::Dependency(format!("media status {}", response.status())));
    }
    if response.content_length().is_some_and(|length| length > max_bytes) {
        return Err(AppError::TooLarge);
    }
    use futures::StreamExt;
    let mut stream = response.bytes_stream();
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk =
            chunk.map_err(|error| AppError::Dependency(format!("media read failed: {error}")))?;
        if bytes.len() as u64 + chunk.len() as u64 > max_bytes {
            return Err(AppError::TooLarge);
        }
        bytes.extend_from_slice(&chunk);
    }
    let matches_magic = if video {
        bytes.len() >= 12 && &bytes[4..8] == b"ftyp"
    } else {
        bytes.len() >= 3 && bytes[0] == 0xFF && bytes[1] == 0xD8 && bytes[2] == 0xFF
    };
    if !matches_magic {
        let reason =
            if video { "public video was not an mp4" } else { "public photo was not a jpeg" };
        return Err(AppError::Validation(reason.into()));
    }
    Ok(bytes)
}

/// One public attachment from a Facebook post. `video` is an mp4; otherwise a jpeg.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlbumItem {
    pub url: String,
    pub video: bool,
}

/// Attachments a swipeable post publishes without a login.
///
/// Two or more `all_subattachments` nodes are the album, even when the page
/// also names a video URL. One video node, or one photo sitting next to a real
/// video URL, stays empty so yt-dlp can size that single video. A reel with no
/// attachment list stays empty. Null video fields do not count.
pub fn facebook_album_media(html: &str) -> Vec<AlbumItem> {
    let mut items = Vec::new();
    let mut search = 0usize;
    while items.len() < crate::shared::stored_zip::ALBUM_MAX_FILES {
        let Some(rel) = html[search..].find("\"all_subattachments\":") else {
            break;
        };
        let at = search + rel;
        let Some(nodes_rel) = html[at..].find("\"nodes\":[") else {
            break;
        };
        let open = at + nodes_rel + "\"nodes\":".len();
        let Some(end) = json_value_end(&html[open..]) else {
            break;
        };
        collect_album_items(&html[open..open + end], &mut items);
        search = open + end;
    }
    match items.as_slice() {
        [item] if item.video || page_has_video_stream(html) => Vec::new(),
        _ => items,
    }
}

fn page_has_video_stream(html: &str) -> bool {
    html.contains("\"playable_url\":\"")
        || html.contains("\"playable_url_quality_hd\":\"")
        || html.contains("\"browser_native_hd_url\":\"")
        || html.contains("\"browser_native_sd_url\":\"")
}

fn collect_album_items(slice: &str, items: &mut Vec<AlbumItem>) {
    let bytes = slice.as_bytes();
    if bytes.first().copied() != Some(b'[') {
        return;
    }
    let mut index = 1usize;
    while index < bytes.len() && items.len() < crate::shared::stored_zip::ALBUM_MAX_FILES {
        match bytes[index] {
            b' ' | b'\n' | b'\r' | b'\t' | b',' => index += 1,
            b'{' => {
                let Some(end) = json_value_end(&slice[index..]) else {
                    return;
                };
                if let Some(item) = album_item(&slice[index..index + end]) {
                    push_album_item(items, item);
                }
                index += end;
            }
            _ => return,
        }
    }
}

fn album_item(object: &str) -> Option<AlbumItem> {
    let video_at = object.find("\"__typename\":\"Video\"");
    let photo_at = object.find("\"__typename\":\"Photo\"");
    let video = match (video_at, photo_at) {
        (Some(video), Some(photo)) => video < photo,
        (Some(_), None) => true,
        (None, Some(_)) => false,
        (None, None) => return None,
    };
    if video {
        video_uri(object).map(|url| AlbumItem { url, video: true })
    } else {
        photo_uri(object).map(|url| AlbumItem { url, video: false })
    }
}

fn push_album_item(items: &mut Vec<AlbumItem>, item: AlbumItem) {
    let name = filename_of(&item.url);
    if name.is_empty() {
        return;
    }
    if items.iter().any(|existing| filename_of(&existing.url) == name) {
        return;
    }
    items.push(item);
}

fn filename_of(url: &str) -> &str {
    let path = match url.split_once('?') {
        Some((path, _)) => path,
        None => url,
    };
    match path.rsplit_once('/') {
        Some((_, name)) => name,
        None => path,
    }
}

fn photo_uri(window: &str) -> Option<String> {
    for key in ["\"viewer_image\":{", "\"image\":{"] {
        let Some(start) = window.find(key) else {
            continue;
        };
        if let Some(url) = fbcdn_string(&window[start..], "\"uri\":\"") {
            return Some(url);
        }
    }
    None
}

fn video_uri(window: &str) -> Option<String> {
    for key in [
        "\"browser_native_hd_url\":\"",
        "\"playable_url_quality_hd\":\"",
        "\"browser_native_sd_url\":\"",
        "\"playable_url\":\"",
    ] {
        if let Some(url) = fbcdn_string(window, key) {
            return Some(url);
        }
    }
    None
}

fn fbcdn_string(window: &str, key: &str) -> Option<String> {
    let start = window.find(key)?;
    let from = start + key.len();
    let end = window[from..].find('"')?;
    let raw = window[from..from + end].replace("\\/", "/").replace("\\u0026", "&");
    let url = Url::parse(&raw).ok()?;
    (url.scheme() == "https" && host_is_fbcdn(url.host_str().unwrap_or(""))).then_some(raw)
}

fn is_missing_video(error: &AppError) -> bool {
    matches!(error, AppError::Validation(text) if text.contains("no public video"))
}

fn instagram_oembed_endpoint(post: &Url) -> Result<Url, AppError> {
    if !host_matches(PlatformKind::Instagram, post.host_str().unwrap_or("")) {
        return Err(AppError::Validation(
            "refusing to read a photo from a host outside the platform allowlist".into(),
        ));
    }
    let mut clean = post.clone();
    clean.set_query(None);
    clean.set_fragment(None);
    let mut endpoint = Url::parse("https://www.instagram.com/api/v1/oembed/")
        .map_err(|_| AppError::Dependency("instagram oembed endpoint is invalid".into()))?;
    endpoint.query_pairs_mut().append_pair("url", clean.as_str());
    Ok(endpoint)
}

/// Thumbnail published by oembed. Only a public image CDN host is accepted.
pub fn instagram_oembed_thumbnail(body: &str) -> Result<String, AppError> {
    let parsed: serde_json::Value = serde_json::from_str(body)
        .map_err(|_| AppError::Validation("instagram did not return a public photo".into()))?;
    let raw = parsed
        .get("thumbnail_url")
        .and_then(|value| value.as_str())
        .ok_or_else(|| AppError::Validation("instagram did not return a public photo".into()))?;
    let url =
        Url::parse(raw).map_err(|_| AppError::Validation("public photo url is invalid".into()))?;
    if url.scheme() != "https" || !host_is_instagram_cdn(url.host_str().unwrap_or("")) {
        return Err(AppError::Validation("photo host is not a public cdn".into()));
    }
    Ok(raw.to_owned())
}

fn host_is_instagram_cdn(host: &str) -> bool {
    let host = host.trim_end_matches('.').to_ascii_lowercase();
    host_is_fbcdn(&host) || host == "cdninstagram.com" || host.ends_with(".cdninstagram.com")
}

fn host_is_fbcdn(host: &str) -> bool {
    let host = host.trim_end_matches('.').to_ascii_lowercase();
    host == "fbcdn.net" || host.ends_with(".fbcdn.net")
}

fn json_value_end(text: &str) -> Option<usize> {
    let bytes = text.as_bytes();
    match bytes.first().copied() {
        Some(b'[') | Some(b'{') => {}
        _ => return None,
    }
    let mut depth = 0i32;
    let mut in_string = false;
    let mut escape = false;
    for (index, byte) in bytes.iter().copied().enumerate() {
        if in_string {
            if escape {
                escape = false;
            } else if byte == b'\\' {
                escape = true;
            } else if byte == b'"' {
                in_string = false;
            }
            continue;
        }
        match byte {
            b'"' => in_string = true,
            b'[' | b'{' => depth += 1,
            b']' | b'}' => {
                depth -= 1;
                if depth == 0 {
                    return Some(index + 1);
                }
            }
            _ => {}
        }
    }
    None
}

async fn newest_file(directory: &Path) -> Result<PathBuf, AppError> {
    let mut entries = tokio::fs::read_dir(directory)
        .await
        .map_err(|error| AppError::Dependency(error.to_string()))?;
    let mut found = None;
    while let Some(entry) =
        entries.next_entry().await.map_err(|error| AppError::Dependency(error.to_string()))?
    {
        let path = entry.path();
        if path.is_file() {
            found = Some(path);
        }
    }
    found.ok_or_else(|| AppError::Dependency("yt-dlp produced no file".into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn download_arguments_do_not_invoke_a_shell() {
        let profile = ProcessingProfile::video_mp4(720).expect("profile");
        let url = "https://www.youtube.com/watch?v=dQw4w9WgXcQ&evil=;rm%20-rf";
        let args = download_args(profile, Path::new("/tmp/job"), 1024, url);
        assert!(args.iter().any(|arg| arg == "--no-playlist"));
        assert!(args.iter().any(|arg| arg == "bv*[height=720]+ba/b[height=720]"));
        assert!(!args.iter().any(|arg| arg == "sh" || arg == "-c" || arg.contains("cookies")));
        assert_eq!(args.last().map(String::as_str), Some(url));
        assert_eq!(args.iter().filter(|arg| arg.as_str() == url).count(), 1);
    }

    #[test]
    fn facebook_album_keeps_the_full_photo_and_ignores_video_pages() {
        let html = r#"{"all_subattachments":{"count":1,"nodes":[{"media":{"__typename":"Photo","is_playable":false,"image":{"uri":"https:\/\/scontent.xx.fbcdn.net\/v\/thumb.jpg?x=1","height":1,"width":1},"viewer_image":{"height":2,"width":2,"uri":"https:\/\/scontent.xx.fbcdn.net\/v\/full.jpg?y=2"},"id":"1"}}]}}"#;
        let items = facebook_album_media(html);
        assert_eq!(
            items,
            [AlbumItem {
                url: "https://scontent.xx.fbcdn.net/v/full.jpg?y=2".into(),
                video: false
            }]
        );
        let video = format!("{html}\"playable_url\":\"https://example.invalid/v.mp4\"");
        assert!(facebook_album_media(&video).is_empty());
        let null_field = format!("{html}\"browser_native_hd_url\":null");
        assert_eq!(facebook_album_media(&null_field), items);
    }

    #[test]
    fn facebook_carousel_returns_every_photo_and_video() {
        let html = r#"{"playable_url":"https://video.xx.fbcdn.net/v/page.mp4","browser_native_hd_url":"https://video.xx.fbcdn.net/v/page-hd.mp4","all_subattachments":{"nodes":[{"media":{"__typename":"Photo","image":{"uri":"https:\/\/scontent.xx.fbcdn.net\/v\/thumb.jpg?x=1"},"viewer_image":{"uri":"https:\/\/scontent.xx.fbcdn.net\/v\/full.jpg?y=2\u0026z=1"}}},{"media":{"__typename":"Video","image":{"uri":"https:\/\/scontent.xx.fbcdn.net\/v\/cover.jpg"},"browser_native_hd_url":"https:\/\/example.invalid\/nope.mp4","browser_native_sd_url":"https:\/\/video.xx.fbcdn.net\/v\/low.mp4","playable_url":"https:\/\/video.xx.fbcdn.net\/v\/play.mp4"}}]}}"#;
        let items = facebook_album_media(html);
        assert_eq!(
            items,
            [
                AlbumItem {
                    url: "https://scontent.xx.fbcdn.net/v/full.jpg?y=2&z=1".into(),
                    video: false
                },
                AlbumItem { url: "https://video.xx.fbcdn.net/v/low.mp4".into(), video: true }
            ]
        );
    }

    #[test]
    fn facebook_two_photos_stay_an_album_beside_a_page_video_url() {
        let html = r#"{"playable_url":"https://example.invalid/v.mp4","all_subattachments":{"nodes":[{"media":{"__typename":"Photo","viewer_image":{"uri":"https:\/\/scontent.xx.fbcdn.net\/v\/a.jpg?y=1"}}},{"media":{"__typename":"Photo","viewer_image":{"uri":"https:\/\/scontent.xx.fbcdn.net\/v\/b.jpg?y=2"}}}]}}"#;
        let items = facebook_album_media(html);
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].url, "https://scontent.xx.fbcdn.net/v/a.jpg?y=1");
        assert_eq!(items[1].url, "https://scontent.xx.fbcdn.net/v/b.jpg?y=2");
        assert!(items.iter().all(|item| !item.video));
    }

    #[test]
    fn facebook_reel_and_single_video_are_not_albums() {
        let reel = r#"{"browser_native_hd_url":"https://video.xx.fbcdn.net/v/reel.mp4","browser_native_sd_url":"https://video.xx.fbcdn.net/v/reel-sd.mp4","playable_url":"https://video.xx.fbcdn.net/v/reel-play.mp4"}"#;
        assert!(facebook_album_media(reel).is_empty());
        let one = r#"{"all_subattachments":{"nodes":[{"media":{"__typename":"Video","browser_native_hd_url":"https://video.xx.fbcdn.net/v/only.mp4","image":{"uri":"https://scontent.xx.fbcdn.net/v/cover.jpg"}}}]}}"#;
        assert!(facebook_album_media(one).is_empty());
        let hd = r#"{"all_subattachments":{"nodes":[{"media":{"__typename":"Photo","viewer_image":{"uri":"https://scontent.xx.fbcdn.net/v/a.jpg"}}},{"media":{"__typename":"Video","browser_native_hd_url":"https:\/\/video.xx.fbcdn.net\/v\/high.mp4?e=1","browser_native_sd_url":"https://video.xx.fbcdn.net/v/low.mp4","playable_url":"https://video.xx.fbcdn.net/v/play.mp4","image":{"uri":"https://scontent.xx.fbcdn.net/v/cover.jpg"}}}]}}"#;
        let items = facebook_album_media(hd);
        assert_eq!(items.len(), 2);
        assert_eq!(items[1].url, "https://video.xx.fbcdn.net/v/high.mp4?e=1");
        assert!(items[1].video);
    }

    #[test]
    fn facebook_album_stops_at_the_file_cap() {
        let mut nodes = String::new();
        for index in 1..=25 {
            if index > 1 {
                nodes.push(',');
            }
            nodes.push_str(&format!(
                r#"{{"media":{{"__typename":"Photo","viewer_image":{{"uri":"https://scontent.xx.fbcdn.net/v/{index}.jpg"}}}}}}"#
            ));
        }
        let html = format!(r#"{{"all_subattachments":{{"nodes":[{nodes}]}}}}"#);
        assert_eq!(facebook_album_media(&html).len(), crate::shared::stored_zip::ALBUM_MAX_FILES);
    }

    #[test]
    fn menu_lists_only_heights_the_platform_published() {
        let formats = serde_json::json!([
            {"format_id": "audio", "vcodec": "none", "acodec": "aac", "height": null},
            {"format_id": "sb0", "vcodec": "none", "acodec": "none", "format_note": "storyboard", "height": 1080},
            {"format_id": "h264", "vcodec": "h264", "acodec": "aac", "height": 576},
            {"format_id": "h264-low", "vcodec": "h264", "acodec": "none", "height": 576}
        ]);
        let profiles = profiles_from_formats(formats.as_array().expect("array"));
        let labels: Vec<_> = profiles.iter().map(|profile| profile.menu_label()).collect();
        assert_eq!(labels, ["Best", "576p", "Audio"]);
        assert!(!labels.iter().any(|label| label == "360p" || label == "720p" || label == "1080p"));
    }

    #[test]
    fn menu_keeps_the_tallest_published_heights() {
        let formats: Vec<_> = (1..=11)
            .map(|index| {
                serde_json::json!({
                    "format_id": format!("v{index}"),
                    "vcodec": "h264",
                    "acodec": "none",
                    "height": index * 100
                })
            })
            .collect();
        let labels: Vec<_> =
            profiles_from_formats(&formats).iter().map(|profile| profile.menu_label()).collect();
        assert_eq!(labels.first().map(String::as_str), Some("Best"));
        assert!(!labels.iter().any(|label| label == "100p"));
        assert!(labels.iter().any(|label| label == "1100p"));
        assert_eq!(labels.len(), 11);
    }

    #[test]
    fn a_missing_resolution_is_permanent() {
        let error = classify_stderr(
            "ERROR: [TikTok] 1: Requested format is not available. Use --list-formats",
            1,
        );
        assert!(matches!(error, AppError::Validation(_)));
        let text = error.to_string();
        assert!(text.contains("not available"));
        assert!(!text.contains("yt-dlp"));
    }

    #[test]
    fn an_anonymous_rate_limit_is_one_sentence_and_permanent() {
        let error = classify_stderr(
            "ERROR: [Instagram] 1: The webpage request was redirected to the login page. \
             You have exceeded the rate-limit for accessing posts anonymously. \
             Use --cookies-from-browser or --cookies",
            1,
        );
        assert!(matches!(error, AppError::Validation(_)));
        let text = error.to_string();
        assert_eq!(text, "The site is blocking anonymous downloads right now. Try again later.");
        assert!(!text.contains("yt-dlp"));
        assert!(!text.contains("cookies"));
    }

    #[test]
    fn an_instagram_oembed_preview_must_be_a_public_cdn_jpeg() {
        let body = r#"{"thumbnail_url":"https://instagram.fcgk41-1.fna.fbcdn.net/v/t51/preview.jpg","thumbnail_width":640}"#;
        let url = instagram_oembed_thumbnail(body).expect("thumbnail");
        assert!(url.contains("fbcdn.net"));
        let evil = r#"{"thumbnail_url":"https://evil.example/preview.jpg"}"#;
        assert!(instagram_oembed_thumbnail(evil).is_err());
        assert!(instagram_oembed_thumbnail("not json").is_err());
    }

    #[test]
    fn a_post_with_no_video_is_one_sentence_and_permanent() {
        let error = classify_stderr(
            "ERROR: [Instagram] DfMiunXJ8Av: No video formats found!; \
             please report this issue on https://github.com/yt-dlp/yt-dlp/issues?q= , \
             filling out the appropriate issue template. Confirm you are on the latest version using yt-dlp -U",
            1,
        );
        assert!(matches!(error, AppError::Validation(_)));
        assert_eq!(error.to_string(), "This post has no public video.");
        assert!(!error.to_string().contains("yt-dlp"));
        assert!(!error.to_string().contains("github.com"));
    }

    #[test]
    fn platform_hosts_are_allowlisted() {
        assert!(host_matches(PlatformKind::YouTube, "www.youtube.com"));
        assert!(!host_matches(PlatformKind::YouTube, "evil.example"));
        assert!(host_matches(PlatformKind::X, "twitter.com"));
    }
}
