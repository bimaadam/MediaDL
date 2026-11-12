use serde::{Deserialize, Serialize};

use super::error::DomainError;

/// Video or audio output. The kind is part of the media identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MediaKind {
    Video,
    Audio,
    Image,
}

/// A numeric height. The extractor selector is built from this value, never from free text.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Quality {
    Best,
    Height(u16),
}

/// Container the worker must produce. `Original` keeps the extractor output.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Container {
    Original,
    Mp4,
    Webm,
    Mkv,
    Mp3,
    M4a,
    Jpeg,
}

impl Container {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Original => "original",
            Self::Mp4 => "mp4",
            Self::Webm => "webm",
            Self::Mkv => "mkv",
            Self::Mp3 => "mp3",
            Self::M4a => "m4a",
            Self::Jpeg => "jpg",
        }
    }

    pub const fn content_type(self, kind: MediaKind) -> &'static str {
        match (kind, self) {
            (MediaKind::Audio, Self::Mp3) => "audio/mpeg",
            (MediaKind::Audio, Self::M4a) => "audio/mp4",
            (MediaKind::Audio, _) => "audio/mpeg",
            (MediaKind::Image, _) => "image/jpeg",
            (_, Self::Webm) => "video/webm",
            (_, Self::Mkv) => "video/x-matroska",
            _ => "video/mp4",
        }
    }
}

/// Audio room tried first when fitting a merged file under a byte ceiling.
/// Five mebibytes leaves a published 1080p H.264 stream selectable when that
/// stream plus its AAC track is still under Telegram's upload limit.
const FIT_AUDIO_ROOMS: [u64; 3] = [5 * 1024 * 1024, 8 * 1024 * 1024, 16 * 1024 * 1024];

/// Requested processing profile. Field order is part of the canonical hash input.
/// `max_bytes` is absent on historical profiles, so their hash does not change.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ProcessingProfile {
    pub kind: MediaKind,
    pub quality: Quality,
    pub container: Container,
    /// Largest merged file the caller can deliver. `None` keeps the uncapped selector.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_bytes: Option<u64>,
}

impl ProcessingProfile {
    pub fn video_mp4(height: u16) -> Result<Self, DomainError> {
        let profile = Self {
            kind: MediaKind::Video,
            quality: Quality::Height(height),
            container: Container::Mp4,
            max_bytes: None,
        };
        profile.validate()?;
        Ok(profile)
    }

    pub fn best_video() -> Self {
        Self {
            kind: MediaKind::Video,
            quality: Quality::Best,
            container: Container::Mp4,
            max_bytes: None,
        }
    }

    pub fn audio_mp3() -> Self {
        Self {
            kind: MediaKind::Audio,
            quality: Quality::Best,
            container: Container::Mp3,
            max_bytes: None,
        }
    }

    pub fn best_image() -> Self {
        Self {
            kind: MediaKind::Image,
            quality: Quality::Best,
            container: Container::Jpeg,
            max_bytes: None,
        }
    }

    /// Ask the extractor for a published stream that fits this merged-file ceiling.
    /// Audio and photo profiles ignore it. Uncapped video hashes stay unchanged.
    pub fn within(self, max_bytes: u64) -> Self {
        if self.kind != MediaKind::Video || max_bytes == 0 {
            return self;
        }
        Self { max_bytes: Some(max_bytes), ..self }
    }

    /// Fixed ladder kept so callback payloads still fit Telegram's limit.
    /// The chat menu is built from the platform's published formats.
    pub fn format_menu() -> Vec<Self> {
        let mut profiles = vec![Self::best_video()];
        for height in [360_u16, 480, 720, 1080] {
            if let Ok(profile) = Self::video_mp4(height) {
                profiles.push(profile);
            }
        }
        profiles.push(Self::audio_mp3());
        profiles
    }

    pub fn validate(self) -> Result<(), DomainError> {
        match self.quality {
            Quality::Best => {}
            Quality::Height(height) => {
                if !(1..=4320).contains(&height) {
                    return Err(DomainError::InvalidProfile(
                        "height must be between 1 and 4320".into(),
                    ));
                }
                if self.kind == MediaKind::Audio {
                    return Err(DomainError::InvalidProfile(
                        "audio profiles do not take a video height".into(),
                    ));
                }
                if self.kind == MediaKind::Image {
                    return Err(DomainError::InvalidProfile(
                        "photo profiles do not take a video height".into(),
                    ));
                }
            }
        }
        match (self.kind, self.container) {
            (MediaKind::Audio, Container::Mp3 | Container::M4a | Container::Original) => Ok(()),
            (MediaKind::Audio, _) => Err(DomainError::InvalidProfile(
                "audio output must be mp3, m4a, or original".into(),
            )),
            (MediaKind::Image, Container::Jpeg | Container::Original) => Ok(()),
            (MediaKind::Image, _) => {
                Err(DomainError::InvalidProfile("photo output must be jpeg or original".into()))
            }
            (MediaKind::Video, Container::Mp3 | Container::M4a | Container::Jpeg) => {
                Err(DomainError::InvalidProfile(
                    "video output cannot use an audio or photo container".into(),
                ))
            }
            (MediaKind::Video, _) => Ok(()),
        }
    }

    /// yt-dlp format selector built only from this profile. Never from free text.
    /// A height asks for that exact stream. It does not fall back to a larger one.
    /// A byte ceiling prefers a known-size H.264 MP4. Missing sizes are skipped,
    /// because those streams are what blow past the ceiling.
    pub fn ytdlp_format(self) -> String {
        match (self.kind, self.quality, self.max_bytes) {
            (MediaKind::Audio, _, _) => "ba/b".to_owned(),
            (MediaKind::Image, _, _) => "b".to_owned(),
            (MediaKind::Video, Quality::Best, None) => "bv*+ba/b".to_owned(),
            (MediaKind::Video, Quality::Height(height), None) => {
                format!("bv*[height={height}]+ba/b[height={height}]")
            }
            (MediaKind::Video, Quality::Best, Some(limit)) => fit_best(limit),
            (MediaKind::Video, Quality::Height(height), Some(limit)) => fit_height(height, limit),
        }
    }

    pub fn needs_ffmpeg(self) -> bool {
        matches!(
            (self.kind, self.container),
            (MediaKind::Audio, Container::Mp3 | Container::M4a)
                | (MediaKind::Video, Container::Mp4 | Container::Webm | Container::Mkv)
        )
    }

    pub fn menu_label(self) -> String {
        match (self.kind, self.quality) {
            (MediaKind::Audio, _) => "Audio".to_owned(),
            (MediaKind::Image, _) => "Album".to_owned(),
            (MediaKind::Video, Quality::Best) => "Best".to_owned(),
            (MediaKind::Video, Quality::Height(height)) => format!("{height}p"),
        }
    }

    pub fn quality_label(self) -> String {
        let base = match self.quality {
            Quality::Best => "best".to_owned(),
            Quality::Height(height) => format!("h{height}"),
        };
        match self.max_bytes {
            Some(bytes) => format!("{base}:{bytes}"),
            None => base,
        }
    }
}

fn fit_best(limit: u64) -> String {
    let mut alts = sized_pairs("", limit);
    alts.push("bv*+ba/b".to_owned());
    alts.join("/")
}

fn fit_height(height: u16, limit: u64) -> String {
    sized_pairs(&format!("height={height}"), limit).join("/")
}

/// Known-size pairs only. `filesize>0` drops a reported zero, which is not a real file.
fn sized_pairs(video_prefix: &str, limit: u64) -> Vec<String> {
    let mut alts = Vec::with_capacity(FIT_AUDIO_ROOMS.len() + 2);
    for room in FIT_AUDIO_ROOMS {
        let video = limit.saturating_sub(room).max(1);
        alts.push(merged(
            video_prefix,
            &["ext=mp4", "vcodec^=avc1", "filesize>0"],
            video,
            &["ext=m4a", "filesize>0"],
            room,
        ));
    }
    let room = FIT_AUDIO_ROOMS[1];
    let video = limit.saturating_sub(room).max(1);
    alts.push(merged(
        video_prefix,
        &["ext=mp4", "filesize>0"],
        video,
        &["ext=m4a", "filesize>0"],
        room,
    ));
    alts.push(merged(video_prefix, &["filesize>0"], video, &["filesize>0"], room));
    alts
}

fn merged(prefix: &str, video: &[&str], video_cap: u64, audio: &[&str], audio_cap: u64) -> String {
    format!("{}+{}", stream("bv*", prefix, video, video_cap), stream("ba", "", audio, audio_cap))
}

fn stream(kind: &str, prefix: &str, filters: &[&str], cap: u64) -> String {
    let mut parts = Vec::with_capacity(filters.len() + 2);
    if !prefix.is_empty() {
        parts.push(prefix.to_owned());
    }
    parts.extend(filters.iter().map(|filter| (*filter).to_owned()));
    parts.push(format!("filesize<={cap}"));
    format!("{kind}[{}]", parts.join("]["))
}

impl Default for ProcessingProfile {
    fn default() -> Self {
        Self::best_video()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uncapped_selectors_stay_exact() {
        assert_eq!(ProcessingProfile::best_video().ytdlp_format(), "bv*+ba/b");
        assert_eq!(
            ProcessingProfile::video_mp4(720).expect("profile").ytdlp_format(),
            "bv*[height=720]+ba/b[height=720]"
        );
        assert_eq!(ProcessingProfile::audio_mp3().ytdlp_format(), "ba/b");
        assert!(ProcessingProfile::audio_mp3().within(50 * 1024 * 1024).max_bytes.is_none());
    }

    #[test]
    fn a_byte_ceiling_prefers_a_known_h264_that_fits() {
        let limit = 50 * 1024 * 1024;
        let video = 46_493_464_u64;
        let audio = 4_108_947_u64;
        let room = 5 * 1024 * 1024;
        assert!(video + audio <= limit);
        assert!(video <= limit - room);
        assert!(audio <= room);
        let selector = ProcessingProfile::best_video().within(limit).ytdlp_format();
        let first = selector.split('/').next().expect("alternative");
        assert_eq!(
            first,
            format!(
                "bv*[ext=mp4][vcodec^=avc1][filesize>0][filesize<={}]+\
                 ba[ext=m4a][filesize>0][filesize<={room}]",
                limit - room
            )
        );
        assert!(selector.ends_with("/bv*+ba/b"));
        assert!(!selector.contains("cookies"));
    }

    #[test]
    fn a_capped_height_does_not_fall_back_to_another_size() {
        let selector = ProcessingProfile::video_mp4(1080)
            .expect("profile")
            .within(50 * 1024 * 1024)
            .ytdlp_format();
        assert!(selector.contains("height=1080"));
        assert!(selector.contains("filesize>0"));
        assert!(!selector.contains("bv*+ba/b"));
        assert!(!selector.contains("height<=1080"));
    }
}
