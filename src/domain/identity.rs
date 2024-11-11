use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::error::DomainError;
use super::platform::PlatformKind;
use super::profile::{Container, ProcessingProfile};

/// Canonical content identifier. Raw URLs are never used as cache identity.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ContentId(String);

impl ContentId {
    pub fn new(value: impl Into<String>) -> Result<Self, DomainError> {
        let value = value.into();
        if value.is_empty() || value.len() > 200 {
            return Err(DomainError::InvalidContentId);
        }
        if !value.chars().all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '~' | '-')) {
            return Err(DomainError::InvalidContentId);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for ContentId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Lowercase hex SHA-256 of the canonical processing profile.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ProfileHash(String);

impl ProfileHash {
    pub fn from_profile(profile: &ProcessingProfile) -> Result<Self, DomainError> {
        profile.validate()?;
        let canonical = canonical_profile_json(profile);
        let digest = Sha256::digest(canonical.as_bytes());
        Ok(Self(hex::encode(digest)))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for ProfileHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

fn canonical_profile_json(profile: &ProcessingProfile) -> String {
    // Struct field order is the canonical order. No map, no whitespace variance.
    #[derive(Serialize)]
    struct Canonical<'a> {
        container: &'a str,
        kind: &'a str,
        quality: &'a str,
    }
    let quality = profile.quality_label();
    let kind = match profile.kind {
        super::profile::MediaKind::Video => "video",
        super::profile::MediaKind::Audio => "audio",
        super::profile::MediaKind::Image => "image",
    };
    serde_json::to_string(&Canonical {
        container: profile.container.as_str(),
        kind,
        quality: &quality,
    })
    .unwrap_or_else(|_| {
        format!(
            "{{\"container\":\"{}\",\"kind\":\"{}\",\"quality\":\"{}\"}}",
            profile.container.as_str(),
            kind,
            quality
        )
    })
}

/// Deterministic identity shared by every user and delivery channel.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MediaIdentity {
    platform: PlatformKind,
    content_id: ContentId,
    profile_hash: ProfileHash,
}

impl MediaIdentity {
    pub fn derive(
        platform: PlatformKind,
        content_id: ContentId,
        profile: &ProcessingProfile,
    ) -> Result<Self, DomainError> {
        Ok(Self { platform, content_id, profile_hash: ProfileHash::from_profile(profile)? })
    }

    pub fn platform(&self) -> PlatformKind {
        self.platform
    }

    pub fn content_id(&self) -> &ContentId {
        &self.content_id
    }

    pub fn profile_hash(&self) -> &ProfileHash {
        &self.profile_hash
    }

    /// Stable fingerprint used inside Redis keys and storage paths.
    pub fn fingerprint(&self) -> String {
        format!(
            "{}:{}:{}",
            self.platform.code(),
            self.content_id.as_str(),
            self.profile_hash.as_str()
        )
    }

    pub fn cache_key(&self) -> String {
        format!(
            "media:{}:{}:{}",
            self.platform.code(),
            self.content_id.as_str(),
            self.profile_hash.as_str()
        )
    }

    pub fn lock_key(&self) -> String {
        format!("lock:media:{}", self.fingerprint())
    }

    pub fn metadata_key(&self) -> String {
        format!("meta:{}:{}", self.platform.code(), self.content_id.as_str())
    }

    pub fn telegram_key(&self, bot_id: &str) -> String {
        // v2 drops file ids saved when the upload name was media.bin.
        format!("telegram:v2:{bot_id}:{}", self.fingerprint())
    }

    pub fn storage_key(&self, version: i32) -> String {
        format!(
            "artifacts/{}/{}/{}/v{version}",
            self.platform.code(),
            self.content_id.as_str(),
            self.profile_hash.as_str()
        )
    }
}

/// Used by tests and direct-url identity. Not a general encoding helper.
pub fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

pub fn container_of(profile: &ProcessingProfile) -> Container {
    profile.container
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::profile::{Container, MediaKind, Quality};

    #[test]
    fn tracking_parameters_do_not_change_identity() {
        let profile = ProcessingProfile::video_mp4(720).unwrap();
        let left = MediaIdentity::derive(
            PlatformKind::YouTube,
            ContentId::new("dQw4w9WgXcQ").unwrap(),
            &profile,
        )
        .unwrap();
        let right = MediaIdentity::derive(
            PlatformKind::YouTube,
            ContentId::new("dQw4w9WgXcQ").unwrap(),
            &profile,
        )
        .unwrap();
        assert_eq!(left, right);
        assert_eq!(left.cache_key(), right.cache_key());
    }

    #[test]
    fn different_quality_is_a_different_identity() {
        let a = MediaIdentity::derive(
            PlatformKind::YouTube,
            ContentId::new("dQw4w9WgXcQ").unwrap(),
            &ProcessingProfile::video_mp4(720).unwrap(),
        )
        .unwrap();
        let b = MediaIdentity::derive(
            PlatformKind::YouTube,
            ContentId::new("dQw4w9WgXcQ").unwrap(),
            &ProcessingProfile::video_mp4(1080).unwrap(),
        )
        .unwrap();
        assert_ne!(a.profile_hash(), b.profile_hash());
    }

    #[test]
    fn profile_hash_is_stable_across_calls() {
        let profile = ProcessingProfile {
            kind: MediaKind::Audio,
            quality: Quality::Best,
            container: Container::Mp3,
        };
        let first = ProfileHash::from_profile(&profile).unwrap();
        let second = ProfileHash::from_profile(&profile).unwrap();
        assert_eq!(first, second);
        assert_eq!(first.as_str().len(), 64);
    }

    #[test]
    fn content_id_rejects_url_shaped_values() {
        assert!(ContentId::new("https://example.com/a").is_err());
        assert!(ContentId::new("").is_err());
    }

    #[test]
    fn telegram_cache_key_skips_media_bin_generation() {
        let identity = MediaIdentity::derive(
            PlatformKind::YouTube,
            ContentId::new("dQw4w9WgXcQ").unwrap(),
            &ProcessingProfile::video_mp4(720).unwrap(),
        )
        .unwrap();
        let key = identity.telegram_key("8242944978");
        assert!(key.starts_with("telegram:v2:8242944978:"));
        assert!(!key.starts_with("telegram:8242944978:"));
    }
}
