//! Ports (driven side) the pipeline depends on. The rskafka adapters live in `paystream-infra`;
//! tests use in-memory fakes. Abstracting Kafka behind these traits means the client library
//! (rskafka today, rdkafka in production) can be swapped without touching the pipeline.

use async_trait::async_trait;
use paystream_types::{EnrichedPaymentEvent, PaymentEvent, Result};

/// Source of raw payment events (a Kafka topic partition, or a fake in tests).
#[async_trait]
pub trait EventSource: Send + Sync {
    /// Fetches up to `max` events. An empty vec signals "nothing available right now".
    async fn next_batch(&self, max: usize) -> Result<Vec<PaymentEvent>>;
}

/// Sink for enriched events (the output Kafka topic, or a fake in tests).
#[async_trait]
pub trait EventSink: Send + Sync {
    async fn publish(&self, event: &EnrichedPaymentEvent) -> Result<()>;
}
