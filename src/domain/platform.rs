use serde::{Deserialize, Serialize};

/// Supported source platforms. Each value has its own provider adapter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlatformKind {
    YouTube,
    TikTok,
    Facebook,
    Instagram,
    X,
    Reddit,
    Direct,
}

impl PlatformKind {
    pub const fn code(self) -> &'static str {
        match self {
            Self::YouTube => "youtube",
            Self::TikTok => "tiktok",
            Self::Facebook => "facebook",
            Self::Instagram => "instagram",
            Self::X => "x",
            Self::Reddit => "reddit",
            Self::Direct => "direct",
        }
    }

    pub const fn display_name(self) -> &'static str {
        match self {
            Self::YouTube => "YouTube",
            Self::TikTok => "TikTok",
            Self::Facebook => "Facebook",
            Self::Instagram => "Instagram",
            Self::X => "X",
            Self::Reddit => "Reddit",
            Self::Direct => "Direct",
        }
    }

    pub const fn db_id(self) -> i16 {
        match self {
            Self::YouTube => 1,
            Self::TikTok => 2,
            Self::Facebook => 3,
            Self::Instagram => 4,
            Self::X => 5,
            Self::Reddit => 6,
            Self::Direct => 7,
        }
    }

    pub fn from_db_id(id: i16) -> Option<Self> {
        Some(match id {
            1 => Self::YouTube,
            2 => Self::TikTok,
            3 => Self::Facebook,
            4 => Self::Instagram,
            5 => Self::X,
            6 => Self::Reddit,
            7 => Self::Direct,
            _ => return None,
        })
    }

    pub fn from_code(code: &str) -> Option<Self> {
        Some(match code {
            "youtube" => Self::YouTube,
            "tiktok" => Self::TikTok,
            "facebook" => Self::Facebook,
            "instagram" => Self::Instagram,
            "x" => Self::X,
            "reddit" => Self::Reddit,
            "direct" => Self::Direct,
            _ => return None,
        })
    }

    pub fn all() -> &'static [Self] {
        &[
            Self::YouTube,
            Self::TikTok,
            Self::Facebook,
            Self::Instagram,
            Self::X,
            Self::Reddit,
            Self::Direct,
        ]
    }
}

impl std::fmt::Display for PlatformKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.code())
    }
}
