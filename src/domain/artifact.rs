use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactStatus {
    Uploading,
    Ready,
    Deleted,
}

impl ArtifactStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Uploading => "uploading",
            Self::Ready => "ready",
            Self::Deleted => "deleted",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "uploading" => Self::Uploading,
            "ready" => Self::Ready,
            "deleted" => Self::Deleted,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Artifact {
    pub id: Uuid,
    pub media_id: Uuid,
    pub profile_hash: String,
    pub version: i32,
    pub object_key: String,
    pub checksum_sha256: String,
    pub byte_size: i64,
    pub content_type: String,
    pub status: ArtifactStatus,
    pub created_at: DateTime<Utc>,
}

/// Cache payload. Binary media is never stored in Redis.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CachedArtifact {
    pub v: u32,
    pub artifact_id: Uuid,
    pub media_id: Uuid,
    pub object_key: String,
    pub checksum_sha256: String,
    pub byte_size: i64,
    pub content_type: String,
}

impl CachedArtifact {
    pub const VERSION: u32 = 1;

    pub fn from_artifact(artifact: &Artifact) -> Self {
        Self {
            v: Self::VERSION,
            artifact_id: artifact.id,
            media_id: artifact.media_id,
            object_key: artifact.object_key.clone(),
            checksum_sha256: artifact.checksum_sha256.clone(),
            byte_size: artifact.byte_size,
            content_type: artifact.content_type.clone(),
        }
    }

    pub fn is_current(&self) -> bool {
        self.v == Self::VERSION && self.byte_size >= 0 && self.checksum_sha256.len() == 64
    }
}
