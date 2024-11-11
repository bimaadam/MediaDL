use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UserKind {
    Telegram,
    Api,
    System,
}

impl UserKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Telegram => "telegram",
            Self::Api => "api",
            Self::System => "system",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "telegram" => Self::Telegram,
            "api" => Self::Api,
            "system" => Self::System,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Actor {
    pub user_id: Uuid,
    pub kind: UserKind,
}

/// Where the finished artifact should be delivered. The core does not speak Telegram.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DeliveryChannel {
    Http,
    Telegram { bot_id: String, chat_id: i64, message_id: Option<i32> },
}

impl DeliveryChannel {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Http => "http",
            Self::Telegram { .. } => "telegram",
        }
    }

    pub fn chat_id(&self) -> i64 {
        match self {
            Self::Http => 0,
            Self::Telegram { chat_id, .. } => *chat_id,
        }
    }

    pub fn bot_id(&self) -> Option<&str> {
        match self {
            Self::Telegram { bot_id, .. } => Some(bot_id.as_str()),
            Self::Http => None,
        }
    }
}
