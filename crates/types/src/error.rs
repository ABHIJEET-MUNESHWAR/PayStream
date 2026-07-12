//! Crate-wide error type.

/// Errors surfaced across the PayStream pipeline. Libraries return this; the binary maps it to
/// `anyhow` at the top level.
#[derive(Debug, thiserror::Error)]
pub enum PayStreamError {
    #[error("failed to deserialize payment event: {0}")]
    Deserialize(#[source] serde_json::Error),

    #[error("failed to serialize enriched event: {0}")]
    Serialize(#[source] serde_json::Error),

    #[error("kafka error: {0}")]
    Kafka(String),

    #[error("configuration error: {0}")]
    Config(String),

    #[error("pipeline error: {0}")]
    Pipeline(String),
}

/// Convenient result alias.
pub type Result<T> = std::result::Result<T, PayStreamError>;
