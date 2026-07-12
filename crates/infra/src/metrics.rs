//! Prometheus metrics wiring.

use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};

/// Metric names, centralized to avoid drift between emit and scrape.
pub mod names {
    pub const CONSUMED: &str = "paystream_events_consumed_total";
    pub const PUBLISHED: &str = "paystream_events_published_total";
    pub const PUBLISH_FAILED: &str = "paystream_events_publish_failed_total";
}

/// Installs the Prometheus recorder and returns a handle used to render the scrape endpoint.
///
/// # Panics
/// Panics if a global metrics recorder has already been installed.
#[must_use]
pub fn install_prometheus() -> PrometheusHandle {
    let handle = PrometheusBuilder::new()
        .install_recorder()
        .expect("failed to install Prometheus recorder");

    metrics::describe_counter!(names::CONSUMED, "Total payment events consumed from Kafka");
    metrics::describe_counter!(names::PUBLISHED, "Total enriched events published to Kafka");
    metrics::describe_counter!(
        names::PUBLISH_FAILED,
        "Total publish failures after retries"
    );
    handle
}
