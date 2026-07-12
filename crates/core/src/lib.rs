//! PayStream core: the pure streaming pipeline (ports, enrichment, aggregation) with no I/O.
#![forbid(unsafe_code)]
// Money literals use a deliberate `<major>_<cents>` grouping (e.g. 1_000_00 == 1000.00).
#![allow(clippy::inconsistent_digit_grouping)]

pub mod aggregator;
pub mod enricher;
pub mod pipeline;
pub mod ports;

pub use aggregator::Aggregator;
pub use enricher::{enrich, risk_score};
pub use pipeline::{Pipeline, PipelineConfig};
pub use ports::{EventSink, EventSource};
