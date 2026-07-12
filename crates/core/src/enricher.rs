//! Pure enrichment: derive a risk score and ingest latency for a payment event. Deterministic and
//! side-effect free, so it is trivially unit-tested and safe to run in parallel across a batch.

use chrono::{DateTime, Utc};
use paystream_types::{Direction, EnrichedPaymentEvent, PaymentEvent};

const LARGE_AMOUNT_MINOR: i64 = 1_000_000_00;
const MEDIUM_AMOUNT_MINOR: i64 = 100_000_00;

/// Enriches a raw event as of `now`, computing a 0–100 risk score and non-negative ingest latency.
#[must_use]
pub fn enrich(event: PaymentEvent, now: DateTime<Utc>) -> EnrichedPaymentEvent {
    let latency_ms = (now - event.occurred_at).num_milliseconds().max(0);
    let risk_score = risk_score(&event);
    EnrichedPaymentEvent {
        risk_score,
        ingest_latency_ms: latency_ms,
        processed_at: now,
        event,
    }
}

/// Explainable heuristic risk score, shaped like the output of a model so it is easy to swap for one.
#[must_use]
pub fn risk_score(event: &PaymentEvent) -> u8 {
    let mut score: u32 = 0;
    let magnitude = event.amount.minor_units.abs();
    if magnitude >= LARGE_AMOUNT_MINOR {
        score += 60;
    } else if magnitude >= MEDIUM_AMOUNT_MINOR {
        score += 30;
    }
    if event.direction == Direction::PayOut {
        score += 15;
    }
    score.min(100) as u8
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;
    use paystream_types::{Currency, Money, PaymentStatus};
    use uuid::Uuid;

    fn event(direction: Direction, minor: i64) -> PaymentEvent {
        PaymentEvent {
            payment_id: Uuid::nil(),
            direction,
            account: "acct-1".to_string(),
            amount: Money::new(Currency::MXN, minor),
            status: PaymentStatus::Completed,
            occurred_at: Utc::now(),
        }
    }

    #[test]
    fn small_pay_in_is_low_risk() {
        assert_eq!(risk_score(&event(Direction::PayIn, 10_00)), 0);
    }

    #[test]
    fn large_pay_out_is_high_risk() {
        assert_eq!(risk_score(&event(Direction::PayOut, 2_000_000_00)), 75);
    }

    #[test]
    fn latency_is_non_negative() {
        let mut e = event(Direction::PayIn, 100);
        e.occurred_at = Utc::now() + Duration::seconds(5); // future-dated (clock skew)
        let enriched = enrich(e, Utc::now());
        assert!(enriched.ingest_latency_ms >= 0);
    }

    #[test]
    fn enrich_computes_positive_latency() {
        let mut e = event(Direction::PayIn, 100);
        let now = Utc::now();
        e.occurred_at = now - Duration::milliseconds(250);
        let enriched = enrich(e, now);
        assert_eq!(enriched.ingest_latency_ms, 250);
    }
}
