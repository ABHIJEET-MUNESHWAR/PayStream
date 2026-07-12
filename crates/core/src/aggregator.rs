//! Thread-safe running aggregates — the stream's in-memory CQRS read model. Updated concurrently by
//! the pipeline and snapshotted by the API.

use parking_lot::Mutex;
use paystream_types::{AggregatesSnapshot, EnrichedPaymentEvent, PaymentStatus};

/// Maintains real-time counts and settled totals over the event stream.
#[derive(Default)]
pub struct Aggregator {
    state: Mutex<AggregatesSnapshot>,
}

impl Aggregator {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Folds one enriched event into the aggregates. O(1) amortized.
    pub fn update(&self, enriched: &EnrichedPaymentEvent) {
        let event = &enriched.event;
        let mut state = self.state.lock();
        state.total_processed += 1;
        *state.by_status.entry(event.status).or_insert(0) += 1;
        *state.by_direction.entry(event.direction).or_insert(0) += 1;
        if event.status == PaymentStatus::Completed {
            *state
                .settled_minor_by_currency
                .entry(event.amount.currency)
                .or_insert(0) += event.amount.minor_units;
        }
        state.max_risk_score = state.max_risk_score.max(enriched.risk_score);
    }

    /// Returns a consistent point-in-time snapshot.
    #[must_use]
    pub fn snapshot(&self) -> AggregatesSnapshot {
        self.state.lock().clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::enricher::enrich;
    use chrono::Utc;
    use paystream_types::{Currency, Direction, Money, PaymentEvent};
    use uuid::Uuid;

    fn enriched(direction: Direction, status: PaymentStatus, minor: i64) -> EnrichedPaymentEvent {
        enrich(
            PaymentEvent {
                payment_id: Uuid::new_v4(),
                direction,
                account: "a".to_string(),
                amount: Money::new(Currency::MXN, minor),
                status,
                occurred_at: Utc::now(),
            },
            Utc::now(),
        )
    }

    #[test]
    fn counts_and_totals_accumulate() {
        let agg = Aggregator::new();
        agg.update(&enriched(
            Direction::PayIn,
            PaymentStatus::Completed,
            1_000_00,
        ));
        agg.update(&enriched(
            Direction::PayOut,
            PaymentStatus::Completed,
            500_00,
        ));
        agg.update(&enriched(Direction::PayIn, PaymentStatus::Failed, 999_00));

        let snap = agg.snapshot();
        assert_eq!(snap.total_processed, 3);
        assert_eq!(snap.by_status[&PaymentStatus::Completed], 2);
        assert_eq!(snap.by_status[&PaymentStatus::Failed], 1);
        assert_eq!(snap.by_direction[&Direction::PayIn], 2);
        // Only COMPLETED contributes to settled totals.
        assert_eq!(snap.settled_minor_by_currency[&Currency::MXN], 1_500_00);
    }

    #[test]
    fn tracks_max_risk_score() {
        let agg = Aggregator::new();
        agg.update(&enriched(Direction::PayIn, PaymentStatus::Completed, 10_00));
        agg.update(&enriched(
            Direction::PayOut,
            PaymentStatus::Completed,
            2_000_000_00,
        ));
        assert_eq!(agg.snapshot().max_risk_score, 75);
    }
}
