//! Framework-free domain types for PayStream: money, payment events, aggregates and errors.
#![forbid(unsafe_code)]
// Money literals use a deliberate `<major>_<cents>` grouping (e.g. 1_000_00 == 1000.00).
#![allow(clippy::inconsistent_digit_grouping)]

pub mod aggregates;
pub mod error;
pub mod event;
pub mod money;

pub use aggregates::AggregatesSnapshot;
pub use error::{PayStreamError, Result};
pub use event::{Direction, EnrichedPaymentEvent, PaymentEvent, PaymentStatus};
pub use money::{Currency, CurrencyMismatch, Money};

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use uuid::Uuid;

    #[test]
    fn money_display_formats_minor_units() {
        assert_eq!(Money::new(Currency::USD, 1234).to_string(), "12.34 USD");
        assert_eq!(Money::new(Currency::BTC, 1).to_string(), "0.00000001 BTC");
    }

    #[test]
    fn money_add_same_currency_is_exact() {
        let a = Money::new(Currency::MXN, 100);
        let b = Money::new(Currency::MXN, 250);
        assert_eq!(a.checked_add(b).unwrap(), Money::new(Currency::MXN, 350));
    }

    #[test]
    fn money_add_mismatch_is_rejected() {
        let a = Money::new(Currency::USD, 100);
        let b = Money::new(Currency::EUR, 100);
        let err = a.checked_add(b).unwrap_err();
        assert_eq!(err.expected, Currency::USD);
        assert_eq!(err.actual, Currency::EUR);
    }

    #[test]
    fn currency_scales_are_correct() {
        assert_eq!(Currency::USD.minor_unit_scale(), 2);
        assert_eq!(Currency::BTC.minor_unit_scale(), 8);
        assert_eq!(Currency::USDC.minor_unit_scale(), 6);
    }

    #[test]
    fn payment_status_terminality() {
        assert!(PaymentStatus::Completed.is_terminal());
        assert!(PaymentStatus::Rejected.is_terminal());
        assert!(!PaymentStatus::Initiated.is_terminal());
    }

    #[test]
    fn payment_event_json_round_trips() {
        let event = PaymentEvent {
            payment_id: Uuid::nil(),
            direction: Direction::PayIn,
            account: "acct-1".to_string(),
            amount: Money::new(Currency::MXN, 100_00),
            status: PaymentStatus::Completed,
            occurred_at: Utc::now(),
        };
        let json = serde_json::to_string(&event).unwrap();
        let back: PaymentEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(event, back);
    }

    #[test]
    fn direction_and_status_serialize_screaming_snake() {
        assert_eq!(
            serde_json::to_string(&Direction::PayIn).unwrap(),
            "\"PAY_IN\""
        );
        assert_eq!(
            serde_json::to_string(&PaymentStatus::ComplianceApproved).unwrap(),
            "\"COMPLIANCE_APPROVED\""
        );
    }
}
