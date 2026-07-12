//! PayStream infrastructure adapters: rskafka source/sink, config and Prometheus metrics.
#![forbid(unsafe_code)]

pub mod config;
pub mod kafka;
pub mod metrics;

pub use config::AppConfig;
pub use kafka::{KafkaEventSink, KafkaEventSource};
pub use metrics::install_prometheus;
