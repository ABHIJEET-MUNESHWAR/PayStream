//! Money as an exact, currency-tagged count of minor units. Floating point is never used on the
//! money path — the single most important invariant when aggregating financial flows.

use serde::{Deserialize, Serialize};
use std::fmt;

/// Supported settlement currencies with their minor-unit scale (decimal places).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Currency {
    USD,
    EUR,
    MXN,
    BRL,
    ARS,
    BTC,
    ETH,
    USDC,
}

impl Currency {
    /// Number of decimal places, e.g. `2` for USD (cents), `8` for BTC (sats).
    #[must_use]
    pub const fn minor_unit_scale(self) -> u32 {
        match self {
            Currency::USD | Currency::EUR | Currency::MXN | Currency::BRL | Currency::ARS => 2,
            Currency::BTC | Currency::ETH => 8,
            Currency::USDC => 6,
        }
    }
}

impl fmt::Display for Currency {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

/// Raised when two [`Money`] values of different currencies are combined.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("currency mismatch: expected {expected} but got {actual}")]
pub struct CurrencyMismatch {
    pub expected: Currency,
    pub actual: Currency,
}

/// An immutable, currency-safe monetary amount stored as a signed count of minor units.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Money {
    pub currency: Currency,
    pub minor_units: i64,
}

impl Money {
    #[must_use]
    pub const fn new(currency: Currency, minor_units: i64) -> Self {
        Self {
            currency,
            minor_units,
        }
    }

    #[must_use]
    pub const fn zero(currency: Currency) -> Self {
        Self {
            currency,
            minor_units: 0,
        }
    }

    /// Adds two amounts of the same currency using checked (overflow-safe) arithmetic.
    ///
    /// # Errors
    /// Returns [`CurrencyMismatch`] if the currencies differ.
    ///
    /// # Panics
    /// Panics only on `i64` overflow, which for real balances indicates corrupt input.
    pub fn checked_add(self, other: Money) -> Result<Money, CurrencyMismatch> {
        if self.currency != other.currency {
            return Err(CurrencyMismatch {
                expected: self.currency,
                actual: other.currency,
            });
        }
        Ok(Money {
            currency: self.currency,
            minor_units: self
                .minor_units
                .checked_add(other.minor_units)
                .expect("money addition overflowed i64"),
        })
    }

    #[must_use]
    pub const fn is_positive(self) -> bool {
        self.minor_units > 0
    }
}

impl fmt::Display for Money {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let scale = self.currency.minor_unit_scale();
        let divisor = 10_i64.pow(scale);
        let major = self.minor_units / divisor;
        let minor = (self.minor_units % divisor).abs();
        write!(
            f,
            "{}.{:0width$} {}",
            major,
            minor,
            self.currency,
            width = scale as usize
        )
    }
}
