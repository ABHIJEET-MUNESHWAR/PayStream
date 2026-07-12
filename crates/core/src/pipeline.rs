//! The streaming pipeline: source → enrich → aggregate → sink, with per-event concurrency and a
//! resilient publish path (timeout + circuit breaker + retry with backoff).

use crate::aggregator::Aggregator;
use crate::enricher::enrich;
use crate::ports::{EventSink, EventSource};
use chrono::Utc;
use futures::stream::{self, StreamExt};
use paystream_resilience::{
    retry_with_backoff, with_timeout, CircuitBreaker, CircuitBreakerError, RetryPolicy,
};
use paystream_types::{PayStreamError, PaymentEvent, Result};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

/// Tunables for the pipeline's batching, concurrency and publish resilience.
#[derive(Debug, Clone)]
pub struct PipelineConfig {
    pub batch_size: usize,
    pub concurrency: usize,
    pub publish_timeout: Duration,
    pub idle_backoff: Duration,
    pub retry: RetryPolicy,
    pub breaker_failure_threshold: u32,
    pub breaker_open_duration: Duration,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            batch_size: 256,
            concurrency: 32,
            publish_timeout: Duration::from_secs(5),
            idle_backoff: Duration::from_millis(200),
            retry: RetryPolicy::default(),
            breaker_failure_threshold: 5,
            breaker_open_duration: Duration::from_secs(10),
        }
    }
}

/// Consumes payment events, enriches them, updates the shared [`Aggregator`], and republishes the
/// enriched events — processing each batch with bounded concurrency.
pub struct Pipeline<S, K> {
    source: Arc<S>,
    sink: Arc<K>,
    aggregator: Arc<Aggregator>,
    breaker: Arc<CircuitBreaker>,
    config: PipelineConfig,
}

impl<S, K> Pipeline<S, K>
where
    S: EventSource + 'static,
    K: EventSink + 'static,
{
    pub fn new(
        source: Arc<S>,
        sink: Arc<K>,
        aggregator: Arc<Aggregator>,
        config: PipelineConfig,
    ) -> Self {
        let breaker = Arc::new(CircuitBreaker::new(
            config.breaker_failure_threshold,
            config.breaker_open_duration,
        ));
        Self {
            source,
            sink,
            aggregator,
            breaker,
            config,
        }
    }

    /// Fetches one batch and processes it. Returns the number of events successfully republished
    /// (0 means the source was momentarily empty).
    pub async fn run_once(&self) -> Result<usize> {
        let events = self.source.next_batch(self.config.batch_size).await?;
        if events.is_empty() {
            return Ok(0);
        }
        Ok(self.process_batch(events).await)
    }

    /// Enriches, aggregates and publishes a batch with bounded concurrency; returns success count.
    pub async fn process_batch(&self, events: Vec<PaymentEvent>) -> usize {
        let now = Utc::now();
        let concurrency = self.config.concurrency.max(1);
        stream::iter(events)
            .map(|event| {
                let sink = self.sink.clone();
                let aggregator = self.aggregator.clone();
                let breaker = self.breaker.clone();
                let retry = self.config.retry.clone();
                let timeout = self.config.publish_timeout;
                async move {
                    let enriched = enrich(event, now);
                    aggregator.update(&enriched);
                    let published = retry_with_backoff(&retry, || {
                        publish_once(&sink, &breaker, timeout, &enriched)
                    })
                    .await;
                    if let Err(err) = &published {
                        tracing::error!(error = %err, "failed to publish enriched event after retries");
                    }
                    published.is_ok()
                }
            })
            .buffer_unordered(concurrency)
            .fold(0usize, |acc, ok| async move { acc + usize::from(ok) })
            .await
    }

    /// Runs the pipeline until `shutdown` is cancelled, backing off when the source is idle.
    pub async fn run(&self, shutdown: CancellationToken) {
        tracing::info!("pipeline started");
        loop {
            tokio::select! {
                biased;
                () = shutdown.cancelled() => {
                    tracing::info!("pipeline shutting down");
                    break;
                }
                result = self.run_once() => match result {
                    Ok(0) => tokio::time::sleep(self.config.idle_backoff).await,
                    Ok(n) => tracing::debug!(processed = n, "batch processed"),
                    Err(err) => {
                        tracing::error!(error = %err, "batch failed; backing off");
                        tokio::time::sleep(self.config.idle_backoff).await;
                    }
                }
            }
        }
    }

    #[must_use]
    pub fn breaker_state(&self) -> paystream_resilience::State {
        self.breaker.state()
    }
}

/// A single guarded publish attempt: timeout → circuit breaker → underlying sink.
async fn publish_once<K: EventSink>(
    sink: &Arc<K>,
    breaker: &Arc<CircuitBreaker>,
    timeout: Duration,
    enriched: &paystream_types::EnrichedPaymentEvent,
) -> Result<()> {
    let outcome = breaker
        .call(|| async {
            match with_timeout(timeout, sink.publish(enriched)).await {
                Ok(publish_result) => publish_result,
                Err(_timed_out) => Err(PayStreamError::Pipeline("publish timed out".to_string())),
            }
        })
        .await;

    match outcome {
        Ok(()) => Ok(()),
        Err(CircuitBreakerError::Open) => {
            Err(PayStreamError::Pipeline("circuit breaker open".to_string()))
        }
        Err(CircuitBreakerError::Inner(err)) => Err(err),
    }
}
