use thiserror::Error;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum DomainError {
    #[error("url is not a valid http(s) address")]
    InvalidUrl,
    #[error("platform is not supported")]
    UnsupportedPlatform,
    #[error("content identifier is empty or contains unsupported characters")]
    InvalidContentId,
    #[error("processing profile is not valid: {0}")]
    InvalidProfile(String),
    #[error("job cannot move from {from} to {to}")]
    IllegalTransition { from: String, to: String },
    #[error("destination address is not allowed")]
    BlockedDestination,
}
