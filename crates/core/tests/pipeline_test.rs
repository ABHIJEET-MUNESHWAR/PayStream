//! End-to-end pipeline tests using in-memory fakes for the Kafka ports — proving enrichment,
//! aggregation, concurrency and the resilient (retrying) publish path without a broker.
// Money literals use a deliberate `<major>_<cents>` grouping (e.g. 1_000_00 == 1000.00).
#![allow(clippy::inconsistent_digit_grouping)]

use async_trait::async_trait;
use chrono::Utc;
use parking_lot::Mutex;
use paystream_core::ports::{EventSink, EventSource};
use paystream_core::{Aggregator, Pipeline, PipelineConfig};
use paystream_types::{
    Currency, Direction, EnrichedPaymentEvent, Money, PayStreamError, PaymentEvent, PaymentStatus,
    Result,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

/// Source that yields one preloaded batch, then empties.
struct VecSource {
    batches: Mutex<Vec<Vec<PaymentEvent>>>,
}

impl VecSource {
    fn new(events: Vec<PaymentEvent>) -> Self {
        Self {
            batches: Mutex::new(vec![events]),
        }
    }
}

#[async_trait]
impl EventSource for VecSource {
    async fn next_batch(&self, _max: usize) -> Result<Vec<PaymentEvent>> {
        Ok(self.batches.lock().pop().unwrap_or_default())
    }
}

/// Sink that records published events and can be told to fail its first `fail_first` attempts.
struct RecordingSink {
    published: Mutex<Vec<EnrichedPaymentEvent>>,
    attempts: AtomicUsize,
    fail_first: usize,
}

impl RecordingSink {
    fn new(fail_first: usize) -> Self {
        Self {
            published: Mutex::new(Vec::new()),
            attempts: AtomicUsize::new(0),
            fail_first,
        }
    }
}

#[async_trait]
impl EventSink for RecordingSink {
    async fn publish(&self, event: &EnrichedPaymentEvent) -> Result<()> {
        let attempt = self.attempts.fetch_add(1, Ordering::SeqCst) + 1;
        if attempt <= self.fail_first {
            return Err(PayStreamError::Kafka(format!(
                "transient publish failure #{attempt}"
            )));
        }
        self.published.lock().push(event.clone());
        Ok(())
    }
}

fn event(direction: Direction, status: PaymentStatus, minor: i64) -> PaymentEvent {
    PaymentEvent {
        payment_id: Uuid::new_v4(),
        direction,
        account: "acct-1".to_string(),
        amount: Money::new(Currency::MXN, minor),
        status,
        occurred_at: Utc::now(),
    }
}

fn fast_config() -> PipelineConfig {
    PipelineConfig {
        publish_timeout: Duration::from_secs(1),
        idle_backoff: Duration::from_millis(1),
        retry: paystream_resilience::RetryPolicy::new(4, Duration::from_millis(1)),
        ..PipelineConfig::default()
    }
}

#[tokio::test]
async fn processes_batch_enriches_aggregates_and_publishes() {
    let events = vec![
        event(Direction::PayIn, PaymentStatus::Completed, 1_000_00),
        event(Direction::PayOut, PaymentStatus::Completed, 500_00),
        event(Direction::PayIn, PaymentStatus::Failed, 250_00),
    ];
    let source = Arc::new(VecSource::new(events));
    let sink = Arc::new(RecordingSink::new(0));
    let aggregator = Arc::new(Aggregator::new());
    let pipeline = Pipeline::new(source, sink.clone(), aggregator.clone(), fast_config());

    let processed = pipeline.run_once().await.unwrap();

    assert_eq!(processed, 3);
    assert_eq!(sink.published.lock().len(), 3);
    let snap = aggregator.snapshot();
    assert_eq!(snap.total_processed, 3);
    assert_eq!(snap.by_status[&PaymentStatus::Completed], 2);
    assert_eq!(snap.settled_minor_by_currency[&Currency::MXN], 1_500_00);
}

#[tokio::test]
async fn transient_publish_failures_are_retried() {
    let source = Arc::new(VecSource::new(vec![event(
        Direction::PayIn,
        PaymentStatus::Completed,
        100_00,
    )]));
    let sink = Arc::new(RecordingSink::new(2)); // fail twice, succeed on the third attempt
    let aggregator = Arc::new(Aggregator::new());
    let pipeline = Pipeline::new(source, sink.clone(), aggregator, fast_config());

    let processed = pipeline.run_once().await.unwrap();

    assert_eq!(processed, 1);
    assert_eq!(sink.published.lock().len(), 1);
    assert_eq!(sink.attempts.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn empty_source_yields_zero() {
    let source = Arc::new(VecSource {
        batches: Mutex::new(vec![]),
    });
    let sink = Arc::new(RecordingSink::new(0));
    let pipeline = Pipeline::new(source, sink, Arc::new(Aggregator::new()), fast_config());
    assert_eq!(pipeline.run_once().await.unwrap(), 0);
}
