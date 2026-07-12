//! A point-in-time snapshot of the running aggregates the pipeline maintains.

use crate::event::{Direction, PaymentStatus};
use crate::money::Currency;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Immutable snapshot of the real-time aggregates (the CQRS read model of the stream).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AggregatesSnapshot {
    /// Total number of events processed.
    pub total_processed: u64,
    /// Count of events by lifecycle status.
    pub by_status: BTreeMap<PaymentStatus, u64>,
    /// Count of events by direction.
    pub by_direction: BTreeMap<Direction, u64>,
    /// Net settled amount (minor units) by currency, counting only `COMPLETED` events.
    pub settled_minor_by_currency: BTreeMap<Currency, i64>,
    /// Highest risk score observed.
    pub max_risk_score: u8,
}
