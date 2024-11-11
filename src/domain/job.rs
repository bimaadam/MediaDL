use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::error::DomainError;
use super::identity::{MediaIdentity, ProfileHash};
use super::profile::ProcessingProfile;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Pending,
    Queued,
    Processing,
    Uploading,
    Completed,
    Failed,
    Cancelled,
    Expired,
}

impl JobStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Queued => "queued",
            Self::Processing => "processing",
            Self::Uploading => "uploading",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::Expired => "expired",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "pending" => Self::Pending,
            "queued" => Self::Queued,
            "processing" => Self::Processing,
            "uploading" => Self::Uploading,
            "completed" => Self::Completed,
            "failed" => Self::Failed,
            "cancelled" => Self::Cancelled,
            "expired" => Self::Expired,
            _ => return None,
        })
    }

    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled | Self::Expired)
    }

    pub const fn is_active(self) -> bool {
        matches!(self, Self::Pending | Self::Queued | Self::Processing | Self::Uploading)
    }

    pub fn transition(self, to: Self) -> Result<Self, DomainError> {
        let allowed = matches!(
            (self, to),
            (Self::Pending, Self::Queued | Self::Cancelled | Self::Failed)
                | (Self::Queued, Self::Processing | Self::Cancelled | Self::Expired | Self::Failed)
                | (
                    Self::Processing,
                    Self::Uploading | Self::Queued | Self::Failed | Self::Cancelled
                )
                | (Self::Uploading, Self::Completed | Self::Failed | Self::Queued)
                | (Self::Failed, Self::Queued | Self::Expired)
        );
        if allowed {
            Ok(to)
        } else {
            Err(DomainError::IllegalTransition {
                from: self.as_str().to_owned(),
                to: to.as_str().to_owned(),
            })
        }
    }
}

impl std::fmt::Display for JobStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureClass {
    Transient,
    Permanent,
    Restricted,
    Cancelled,
}

impl FailureClass {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Transient => "transient",
            Self::Permanent => "permanent",
            Self::Restricted => "restricted",
            Self::Cancelled => "cancelled",
        }
    }

    pub const fn is_retryable(self) -> bool {
        matches!(self, Self::Transient)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub base: std::time::Duration,
    pub cap: std::time::Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            base: std::time::Duration::from_secs(2),
            cap: std::time::Duration::from_secs(300),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryDecision {
    Retry { delay: std::time::Duration },
    Permanent,
}

impl RetryPolicy {
    /// Exponential backoff with deterministic jitter derived from the job id.
    /// Jitter does not use a process-global RNG, so retries stay reproducible.
    pub fn decide(self, attempt: u32, class: FailureClass, jitter_key: u64) -> RetryDecision {
        if !class.is_retryable() || attempt >= self.max_attempts {
            return RetryDecision::Permanent;
        }
        let shift = attempt.saturating_sub(1).min(16);
        let multiplier = 1u32 << shift;
        let exp = self.base.saturating_mul(multiplier);
        let capped = if exp > self.cap { self.cap } else { exp };
        let cap_ms = capped.as_millis() as u64;
        let jitter_ms = if cap_ms == 0 { 0 } else { jitter_key % (cap_ms / 5 + 1) };
        RetryDecision::Retry { delay: capped + std::time::Duration::from_millis(jitter_ms) }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Job {
    pub id: Uuid,
    pub media_id: Uuid,
    pub identity: MediaIdentity,
    pub profile: ProcessingProfile,
    pub status: JobStatus,
    pub attempt_count: u32,
    pub max_attempts: u32,
    pub failure_class: Option<FailureClass>,
    pub failure_reason: Option<String>,
    pub artifact_id: Option<Uuid>,
    pub requested_by: Option<Uuid>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl Job {
    pub fn profile_hash(&self) -> &ProfileHash {
        self.identity.profile_hash()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn active_states_reach_the_documented_terminals() {
        assert!(JobStatus::Pending.transition(JobStatus::Queued).is_ok());
        assert!(JobStatus::Queued.transition(JobStatus::Processing).is_ok());
        assert!(JobStatus::Processing.transition(JobStatus::Uploading).is_ok());
        assert!(JobStatus::Uploading.transition(JobStatus::Completed).is_ok());
        assert!(JobStatus::Completed.transition(JobStatus::Queued).is_err());
        assert!(JobStatus::Uploading.transition(JobStatus::Cancelled).is_err());
        assert!(JobStatus::Processing.transition(JobStatus::Cancelled).is_ok());
    }

    #[test]
    fn permanent_failures_are_not_retried() {
        let policy = RetryPolicy::default();
        assert_eq!(policy.decide(1, FailureClass::Restricted, 7), RetryDecision::Permanent);
        assert_eq!(policy.decide(1, FailureClass::Permanent, 7), RetryDecision::Permanent);
    }

    #[test]
    fn transient_failures_backoff_until_the_cap() {
        let policy = RetryPolicy {
            max_attempts: 3,
            base: std::time::Duration::from_secs(2),
            cap: std::time::Duration::from_secs(10),
        };
        let RetryDecision::Retry { delay } = policy.decide(1, FailureClass::Transient, 0) else {
            panic!("expected retry");
        };
        assert_eq!(delay, std::time::Duration::from_secs(2));
        assert_eq!(policy.decide(3, FailureClass::Transient, 1), RetryDecision::Permanent);
    }

    #[test]
    fn jitter_is_deterministic_for_the_same_key() {
        let policy = RetryPolicy::default();
        let a = policy.decide(2, FailureClass::Transient, 99);
        let b = policy.decide(2, FailureClass::Transient, 99);
        assert_eq!(a, b);
    }
}
