//! rskafka adapters implementing the core [`EventSource`] / [`EventSink`] ports.
//!
//! rskafka is a pure-Rust Kafka *protocol* client (no native librdkafka), which keeps the build
//! toolchain-free. The ports mean a production deployment can swap in `rdkafka` without changing
//! the pipeline. Offsets are tracked in-process (rskafka does not manage consumer groups).

use crate::metrics::names;
use async_trait::async_trait;
use chrono::Utc;
use paystream_core::ports::{EventSink, EventSource};
use paystream_types::{EnrichedPaymentEvent, PayStreamError, PaymentEvent, Result};
use rskafka::client::partition::{Compression, PartitionClient, UnknownTopicHandling};
use rskafka::client::{Client, ClientBuilder};
use rskafka::record::Record;
use std::collections::BTreeMap;
use std::ops::Range;
use std::sync::atomic::{AtomicI64, Ordering};

const FETCH_BYTES: Range<i32> = 1..10_000_000;

fn kafka_err<E: std::fmt::Display>(err: E) -> PayStreamError {
    PayStreamError::Kafka(err.to_string())
}

async fn build_client(brokers: Vec<String>) -> Result<Client> {
    ClientBuilder::new(brokers).build().await.map_err(kafka_err)
}

async fn ensure_topic(client: &Client, topic: &str, partitions: i32) {
    if let Ok(controller) = client.controller_client() {
        // Ignore "already exists" — topic creation is best-effort convenience for dev/demo.
        if let Err(err) = controller
            .create_topic(topic, partitions.max(1), 1, 5_000)
            .await
        {
            tracing::debug!(topic, error = %err, "create_topic (likely already exists)");
        }
    }
}

/// Consumes payment events from a Kafka topic partition, tracking its own offset.
pub struct KafkaEventSource {
    _client: Client,
    partition: PartitionClient,
    offset: AtomicI64,
    max_wait_ms: i32,
}

impl KafkaEventSource {
    /// Connects and binds to `(topic, partition)`, starting reads at `start_offset`.
    ///
    /// # Errors
    /// Returns [`PayStreamError::Kafka`] if the broker/partition cannot be reached.
    pub async fn connect(
        brokers: Vec<String>,
        topic: &str,
        partition: i32,
        start_offset: i64,
    ) -> Result<Self> {
        let client = build_client(brokers).await?;
        ensure_topic(&client, topic, partition + 1).await;
        let partition_client = client
            .partition_client(topic.to_owned(), partition, UnknownTopicHandling::Retry)
            .await
            .map_err(kafka_err)?;
        Ok(Self {
            _client: client,
            partition: partition_client,
            offset: AtomicI64::new(start_offset),
            max_wait_ms: 500,
        })
    }
}

#[async_trait]
impl EventSource for KafkaEventSource {
    async fn next_batch(&self, max: usize) -> Result<Vec<PaymentEvent>> {
        let offset = self.offset.load(Ordering::SeqCst);
        let (records, _high_watermark) = self
            .partition
            .fetch_records(offset, FETCH_BYTES, self.max_wait_ms)
            .await
            .map_err(kafka_err)?;

        let mut events = Vec::with_capacity(records.len().min(max));
        let mut next_offset = offset;
        for record_and_offset in records {
            next_offset = record_and_offset.offset + 1;
            if let Some(value) = record_and_offset.record.value.as_ref() {
                match serde_json::from_slice::<PaymentEvent>(value) {
                    Ok(event) => {
                        metrics::counter!(names::CONSUMED).increment(1);
                        events.push(event);
                    }
                    Err(err) => tracing::warn!(
                        error = %err, offset = record_and_offset.offset,
                        "skipping undecodable record"
                    ),
                }
            }
            if events.len() >= max {
                break;
            }
        }
        self.offset.store(next_offset, Ordering::SeqCst);
        Ok(events)
    }
}

/// Publishes enriched events to a Kafka topic partition, keyed by payment id for ordering.
pub struct KafkaEventSink {
    _client: Client,
    partition: PartitionClient,
}

impl KafkaEventSink {
    /// Connects and binds to `(topic, partition)`.
    ///
    /// # Errors
    /// Returns [`PayStreamError::Kafka`] if the broker/partition cannot be reached.
    pub async fn connect(brokers: Vec<String>, topic: &str, partition: i32) -> Result<Self> {
        let client = build_client(brokers).await?;
        ensure_topic(&client, topic, partition + 1).await;
        let partition_client = client
            .partition_client(topic.to_owned(), partition, UnknownTopicHandling::Retry)
            .await
            .map_err(kafka_err)?;
        Ok(Self {
            _client: client,
            partition: partition_client,
        })
    }
}

#[async_trait]
impl EventSink for KafkaEventSink {
    async fn publish(&self, event: &EnrichedPaymentEvent) -> Result<()> {
        let value = serde_json::to_vec(event).map_err(PayStreamError::Serialize)?;
        let record = Record {
            key: Some(event.event.payment_id.as_bytes().to_vec()),
            value: Some(value),
            headers: BTreeMap::new(),
            timestamp: Utc::now(),
        };
        match self
            .partition
            .produce(vec![record], Compression::NoCompression)
            .await
        {
            Ok(_offsets) => {
                metrics::counter!(names::PUBLISHED).increment(1);
                Ok(())
            }
            Err(err) => {
                metrics::counter!(names::PUBLISH_FAILED).increment(1);
                Err(kafka_err(err))
            }
        }
    }
}
