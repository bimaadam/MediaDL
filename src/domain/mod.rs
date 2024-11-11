//! Pure domain model. This module does not perform I/O and does not depend on
//! delivery channels, databases, or external tools.

pub mod artifact;
pub mod error;
pub mod identify;
pub mod identity;
pub mod job;
pub mod platform;
pub mod profile;
pub mod security;
pub mod user;

pub use artifact::{Artifact, ArtifactStatus, CachedArtifact};
pub use error::DomainError;
pub use identify::{
    CanonicalUrl, IdentifiedContent, identify_url, is_opaque_facebook_share, is_shortener,
};
pub use identity::{ContentId, MediaIdentity, ProfileHash};
pub use job::{FailureClass, Job, JobStatus, RetryDecision, RetryPolicy};
pub use platform::PlatformKind;
pub use profile::{Container, MediaKind, ProcessingProfile, Quality};
pub use security::{host_is_blocked_name, ip_is_public};
pub use user::{Actor, DeliveryChannel, UserKind};
