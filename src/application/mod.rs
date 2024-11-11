//! Use cases. Depends on the domain and on ports, never on adapters.

pub mod engine;
pub mod error;
pub mod model;
pub mod pipeline;
pub mod ports;

pub use engine::Engine;
pub use error::AppError;
pub use model::*;
pub use pipeline::Pipeline;
pub use ports::*;
