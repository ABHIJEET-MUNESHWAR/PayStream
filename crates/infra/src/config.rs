//! Environment-driven configuration.

use paystream_types::{PayStreamError, Result};

/// Runtime configuration for the node, loaded from the environment.
#[derive(Debug, Clone)]
pub struct AppConfig {
    pub kafka_brokers: Vec<String>,
    pub input_topic: String,
    pub output_topic: String,
    pub partition: i32,
    pub http_port: u16,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            kafka_brokers: vec!["localhost:9092".to_string()],
            input_topic: "payments.events".to_string(),
            output_topic: "payments.enriched".to_string(),
            partition: 0,
            http_port: 8082,
        }
    }
}

impl AppConfig {
    /// Builds configuration from environment variables, falling back to sensible defaults.
    ///
    /// # Errors
    /// Returns [`PayStreamError::Config`] if a provided value cannot be parsed.
    pub fn from_env() -> Result<Self> {
        let defaults = Self::default();
        let kafka_brokers = std::env::var("PAYSTREAM_KAFKA")
            .map(|v| v.split(',').map(|s| s.trim().to_string()).collect())
            .unwrap_or(defaults.kafka_brokers);
        let input_topic = std::env::var("PAYSTREAM_INPUT_TOPIC").unwrap_or(defaults.input_topic);
        let output_topic = std::env::var("PAYSTREAM_OUTPUT_TOPIC").unwrap_or(defaults.output_topic);
        let partition = parse_env("PAYSTREAM_PARTITION", defaults.partition)?;
        let http_port = parse_env("PAYSTREAM_HTTP_PORT", defaults.http_port)?;
        Ok(Self {
            kafka_brokers,
            input_topic,
            output_topic,
            partition,
            http_port,
        })
    }
}

fn parse_env<T>(key: &str, default: T) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    match std::env::var(key) {
        Ok(raw) => raw
            .parse::<T>()
            .map_err(|e| PayStreamError::Config(format!("{key}: {e}"))),
        Err(_) => Ok(default),
    }
}
