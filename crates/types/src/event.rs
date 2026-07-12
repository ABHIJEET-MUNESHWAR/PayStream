//! Payment lifecycle events consumed from Kafka and their enriched form.

use crate::money::Money;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Direction of money movement relative to the platform ledger.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Direction {
    PayIn,
    PayOut,
}

/// Terminal or in-flight lifecycle state of a payment (mirrors the upstream orchestrator).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PaymentStatus {
    Initiated,
    ComplianceApproved,
    FundsReserved,
    RailSettled,
    LedgerPosted,
    Completed,
    Failed,
    Rejected,
}

impl PaymentStatus {
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            PaymentStatus::Completed | PaymentStatus::Failed | PaymentStatus::Rejected
        )
    }
}

/// A payment lifecycle event as published on the input topic.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PaymentEvent {
    pub payment_id: Uuid,
    pub direction: Direction,
    pub account: String,
    pub amount: Money,
    pub status: PaymentStatus,
    pub occurred_at: DateTime<Utc>,
}

/// A [`PaymentEvent`] enriched by the pipeline with a risk score and ingest latency, ready to be
/// published on the output topic.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnrichedPaymentEvent {
    #[serde(flatten)]
    pub event: PaymentEvent,
    pub risk_score: u8,
    pub ingest_latency_ms: i64,
    pub processed_at: DateTime<Utc>,
}
