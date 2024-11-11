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

/// Height is constrained to a fixed ladder so callers cannot inject a format string.
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

/// Requested processing profile. Field order is part of the canonical hash input.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ProcessingProfile {
    pub kind: MediaKind,
    pub quality: Quality,
    pub container: Container,
}

impl ProcessingProfile {
    pub fn video_mp4(height: u16) -> Result<Self, DomainError> {
        let profile = Self {
            kind: MediaKind::Video,
            quality: Quality::Height(height),
            container: Container::Mp4,
        };
        profile.validate()?;
        Ok(profile)
    }

    pub fn best_video() -> Self {
        Self { kind: MediaKind::Video, quality: Quality::Best, container: Container::Mp4 }
    }

    pub fn audio_mp3() -> Self {
        Self { kind: MediaKind::Audio, quality: Quality::Best, container: Container::Mp3 }
    }

    pub fn best_image() -> Self {
        Self { kind: MediaKind::Image, quality: Quality::Best, container: Container::Jpeg }
    }

    /// Formats offered after a download. The first entry is the default fetch.
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
                const ALLOWED: [u16; 8] = [144, 240, 360, 480, 720, 1080, 1440, 2160];
                if !ALLOWED.contains(&height) {
                    return Err(DomainError::InvalidProfile(
                        "height must be one of 144, 240, 360, 480, 720, 1080, 1440, 2160".into(),
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

    /// yt-dlp format selector built only from this enum. Never from free text.
    pub fn ytdlp_format(self) -> &'static str {
        match (self.kind, self.quality) {
            (MediaKind::Audio, _) => "ba/b",
            (MediaKind::Image, _) => "b",
            (MediaKind::Video, Quality::Best) => "bv*+ba/b",
            (MediaKind::Video, Quality::Height(144)) => "bv*[height<=144]+ba/b[height<=144]",
            (MediaKind::Video, Quality::Height(240)) => "bv*[height<=240]+ba/b[height<=240]",
            (MediaKind::Video, Quality::Height(360)) => "bv*[height<=360]+ba/b[height<=360]",
            (MediaKind::Video, Quality::Height(480)) => "bv*[height<=480]+ba/b[height<=480]",
            (MediaKind::Video, Quality::Height(720)) => "bv*[height<=720]+ba/b[height<=720]",
            (MediaKind::Video, Quality::Height(1080)) => "bv*[height<=1080]+ba/b[height<=1080]",
            (MediaKind::Video, Quality::Height(1440)) => "bv*[height<=1440]+ba/b[height<=1440]",
            (MediaKind::Video, Quality::Height(2160)) => "bv*[height<=2160]+ba/b[height<=2160]",
            (MediaKind::Video, Quality::Height(_)) => "bv*+ba/b",
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
            (MediaKind::Image, _) => "Photo".to_owned(),
            (MediaKind::Video, Quality::Best) => "Best".to_owned(),
            (MediaKind::Video, Quality::Height(height)) => format!("{height}p"),
        }
    }

    pub fn quality_label(self) -> String {
        match self.quality {
            Quality::Best => "best".to_owned(),
            Quality::Height(height) => format!("h{height}"),
        }
    }
}

impl Default for ProcessingProfile {
    fn default() -> Self {
        Self::best_video()
    }
}
