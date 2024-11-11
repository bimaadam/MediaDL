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
        profile.ytdlp_format().to_owned(),
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
    if lower.contains("drm")
        || lower.contains("sign in")
        || lower.contains("login required")
        || lower.contains("private video")
        || lower.contains("this video is private")
        || lower.contains("requested format is not available") && lower.contains("login")
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
        Ok(MediaMetadata { title, duration_seconds, profiles: offered_profiles(profile) })
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
        let html = fetch_text(&self.http, &url, 3_000_000).await?;
        Ok(!facebook_album_urls(&html).is_empty())
    }
}

fn offered_profiles(requested: &ProcessingProfile) -> Vec<ProcessingProfile> {
    let mut profiles = vec![ProcessingProfile::best_video(), ProcessingProfile::audio_mp3()];
    for height in [360_u16, 480, 720, 1080] {
        if let Ok(profile) = ProcessingProfile::video_mp4(height) {
            profiles.push(profile);
        }
    }
    if !profiles.contains(requested) {
        profiles.insert(0, *requested);
    }
    profiles
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
            if claimed.platform != PlatformKind::Facebook {
                return Err(AppError::Validation(
                    "photo download is only supported for a public Facebook post".into(),
                ));
            }
            return self.download_facebook_photos(&url, directory, limits.max_download_bytes).await;
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
            return Err(classify_stderr(&result.stderr, result.status));
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
    ) -> Result<DownloadedMedia, AppError> {
        let html = fetch_text(&self.http, url, 3_000_000).await?;
        let photos = facebook_album_urls(&html);
        if photos.is_empty() {
            return Err(AppError::Validation("this Facebook post has no public photo".into()));
        }
        let mut files = Vec::new();
        let mut total = 0u64;
        for (index, raw) in photos.iter().enumerate() {
            let photo_url =
                Url::parse(raw).map_err(|_| AppError::Validation("photo url is invalid".into()))?;
            if !host_is_fbcdn(photo_url.host_str().unwrap_or("")) {
                return Err(AppError::Validation("photo host is not a public cdn".into()));
            }
            let remaining = max_bytes.saturating_sub(total);
            let bytes = fetch_jpeg(&self.http, &photo_url, remaining).await?;
            total = total.saturating_add(bytes.len() as u64);
            files.push((format!("{:02}.jpg", index + 1), bytes));
        }
        if files.len() == 1 {
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

/// Photo URLs from a public album. A page that already exposes a video stream
/// is left to yt-dlp. No cookies and no login form are read.
pub fn facebook_album_urls(html: &str) -> Vec<String> {
    if html.contains("\"playable_url\"")
        || html.contains("browser_native_hd_url")
        || html.contains("browser_native_sd_url")
    {
        return Vec::new();
    }
    let mut urls = Vec::new();
    let mut search = 0usize;
    while let Some(rel) = html[search..].find("\"all_subattachments\":") {
        let at = search + rel;
        let Some(nodes_rel) = html[at..].find("\"nodes\":[") else {
            break;
        };
        let open = at + nodes_rel + "\"nodes\":".len();
        let Some(end) = json_value_end(&html[open..]) else {
            break;
        };
        collect_photo_urls(&html[open..open + end], &mut urls);
        if urls.len() >= 10 {
            break;
        }
        search = open + end;
    }
    urls
}

fn collect_photo_urls(slice: &str, urls: &mut Vec<String>) {
    let mut rest = slice;
    while urls.len() < 10 {
        let Some(idx) = rest.find("\"__typename\":\"Photo\"") else {
            break;
        };
        let window = &rest[idx..rest.len().min(idx + 2500)];
        if let Some(url) = photo_uri(window) {
            let name = url.split('?').next().and_then(|path| path.rsplit('/').next()).unwrap_or("");
            let known = urls.iter().any(|existing| {
                existing.split('?').next().and_then(|path| path.rsplit('/').next()) == Some(name)
            });
            if !name.is_empty() && !known {
                urls.push(url);
            }
        }
        rest = &rest[idx + 20..];
    }
}

fn photo_uri(window: &str) -> Option<String> {
    for key in ["\"viewer_image\":{", "\"image\":{"] {
        let Some(start) = window.find(key) else {
            continue;
        };
        let uri_key = "\"uri\":\"";
        let Some(rel) = window[start..].find(uri_key) else {
            continue;
        };
        let uri_at = start + rel + uri_key.len();
        let Some(end_rel) = window[uri_at..].find('"') else {
            continue;
        };
        let raw = window[uri_at..uri_at + end_rel].replace("\\/", "/").replace("\\u0026", "&");
        if let Ok(url) = Url::parse(&raw)
            && url.scheme() == "https"
            && host_is_fbcdn(url.host_str().unwrap_or(""))
        {
            return Some(raw);
        }
    }
    None
}

fn host_is_fbcdn(host: &str) -> bool {
    let host = host.trim_end_matches('.').to_ascii_lowercase();
    host == "fbcdn.net" || host.ends_with(".fbcdn.net")
}

fn json_value_end(text: &str) -> Option<usize> {
    let bytes = text.as_bytes();
    if bytes.first().copied() != Some(b'[') {
        return None;
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
        assert!(args.iter().any(|arg| arg == "bv*[height<=720]+ba/b[height<=720]"));
        assert!(!args.iter().any(|arg| arg == "sh" || arg == "-c" || arg.contains("cookies")));
        assert_eq!(args.last().map(String::as_str), Some(url));
        assert_eq!(args.iter().filter(|arg| arg.as_str() == url).count(), 1);
    }

    #[test]
    fn facebook_album_keeps_the_full_photo_and_ignores_video_pages() {
        let html = r#"{"all_subattachments":{"count":1,"nodes":[{"media":{"__typename":"Photo","is_playable":false,"image":{"uri":"https:\/\/scontent.xx.fbcdn.net\/v\/thumb.jpg?x=1","height":1,"width":1},"viewer_image":{"height":2,"width":2,"uri":"https:\/\/scontent.xx.fbcdn.net\/v\/full.jpg?y=2"},"id":"1"}}]}}"#;
        let urls = facebook_album_urls(html);
        assert_eq!(urls, ["https://scontent.xx.fbcdn.net/v/full.jpg?y=2"]);
        let video = format!("{html}\"playable_url\":\"https://example.invalid/v.mp4\"");
        assert!(facebook_album_urls(&video).is_empty());
    }

    #[test]
    fn platform_hosts_are_allowlisted() {
        assert!(host_matches(PlatformKind::YouTube, "www.youtube.com"));
        assert!(!host_matches(PlatformKind::YouTube, "evil.example"));
        assert!(host_matches(PlatformKind::X, "twitter.com"));
    }
}
