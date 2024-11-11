use thiserror::Error;

use crate::domain::DomainError;

#[derive(Debug, Error)]
pub enum AppError {
    #[error(transparent)]
    Domain(#[from] DomainError),
    #[error("not found")]
    NotFound,
    #[error("conflict")]
    Conflict,
    #[error("unauthorized")]
    Unauthorized,
    #[error("forbidden")]
    Forbidden,
    #[error("rate limited")]
    RateLimited,
    #[error("daily quota exceeded")]
    QuotaExceeded,
    #[error("the same media is being prepared, retry shortly")]
    Busy,
    #[error("{0}")]
    Validation(String),
    #[error("the content is private, restricted, or requires access this service will not bypass")]
    Restricted,
    #[error("media exceeds the configured size limit")]
    TooLarge,
    #[error("dependency failed: {0}")]
    Dependency(String),
    #[error("idempotency key was reused with a different request")]
    IdempotencyConflict,
    #[error("job was cancelled")]
    Cancelled,
}

impl AppError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::Domain(
                DomainError::InvalidUrl
                | DomainError::InvalidContentId
                | DomainError::InvalidProfile(_),
            ) => "validation",
            Self::Domain(DomainError::UnsupportedPlatform) => "unsupported_platform",
            Self::Domain(DomainError::BlockedDestination) => "blocked_destination",
            Self::Domain(DomainError::IllegalTransition { .. }) => "conflict",
            Self::NotFound => "not_found",
            Self::Conflict => "conflict",
            Self::Unauthorized => "unauthorized",
            Self::Forbidden => "forbidden",
            Self::RateLimited => "rate_limited",
            Self::QuotaExceeded => "quota_exceeded",
            Self::Busy => "busy",
            Self::Validation(_) => "validation",
            Self::Restricted => "restricted",
            Self::TooLarge => "too_large",
            Self::Dependency(_) => "dependency",
            Self::IdempotencyConflict => "idempotency_conflict",
            Self::Cancelled => "cancelled",
        }
    }

    pub fn http_status(&self) -> u16 {
        match self.code() {
            "validation" | "unsupported_platform" | "blocked_destination" => 400,
            "unauthorized" => 401,
            "forbidden" => 403,
            "not_found" => 404,
            "conflict" | "idempotency_conflict" => 409,
            "too_large" => 413,
            "rate_limited" | "quota_exceeded" => 429,
            "restricted" => 422,
            "busy" | "dependency" => 503,
            "cancelled" => 409,
            _ => 500,
        }
    }
}
