//! A deterministic token-bucket rate limiter for backpressure/load-shedding.

use parking_lot::Mutex;
use std::time::Instant;

struct BucketState {
    tokens: f64,
    last: Instant,
}

/// Token bucket admitting up to `capacity` bursts, refilling at `refill_per_sec` tokens/second.
/// The `*_at` methods take an explicit `Instant`, making the limiter deterministically testable.
pub struct TokenBucket {
    inner: Mutex<BucketState>,
    capacity: f64,
    refill_per_sec: f64,
}

impl TokenBucket {
    #[must_use]
    pub fn new(capacity: u32, refill_per_sec: f64) -> Self {
        Self {
            inner: Mutex::new(BucketState {
                tokens: f64::from(capacity),
                last: Instant::now(),
            }),
            capacity: f64::from(capacity),
            refill_per_sec,
        }
    }

    /// Attempts to take one token using the current clock.
    pub fn try_acquire(&self) -> bool {
        self.try_acquire_at(Instant::now())
    }

    /// Attempts to take one token as of `now`, refilling for the elapsed time first.
    pub fn try_acquire_at(&self, now: Instant) -> bool {
        let mut state = self.inner.lock();
        let elapsed = now.saturating_duration_since(state.last).as_secs_f64();
        state.tokens = (state.tokens + elapsed * self.refill_per_sec).min(self.capacity);
        state.last = now;
        if state.tokens >= 1.0 {
            state.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn admits_up_to_capacity_then_rejects() {
        let bucket = TokenBucket::new(3, 1.0);
        let now = Instant::now();
        assert!(bucket.try_acquire_at(now));
        assert!(bucket.try_acquire_at(now));
        assert!(bucket.try_acquire_at(now));
        assert!(!bucket.try_acquire_at(now)); // exhausted
    }

    #[test]
    fn refills_over_time() {
        let bucket = TokenBucket::new(2, 1.0);
        let t0 = Instant::now();
        assert!(bucket.try_acquire_at(t0));
        assert!(bucket.try_acquire_at(t0));
        assert!(!bucket.try_acquire_at(t0));
        // After 1 second, one token has refilled.
        let t1 = t0 + Duration::from_secs(1);
        assert!(bucket.try_acquire_at(t1));
        assert!(!bucket.try_acquire_at(t1));
    }
}
