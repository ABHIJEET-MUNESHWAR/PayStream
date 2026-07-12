//! A minimal three-state circuit breaker (Closed → Open → Half-Open) for guarding an I/O boundary.

use parking_lot::Mutex;
use std::future::Future;
use std::time::{Duration, Instant};

/// Current breaker state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    /// Calls flow through; failures are counted.
    Closed,
    /// Calls are rejected fast until the cool-down elapses.
    Open,
    /// One trial call is allowed to probe recovery.
    HalfOpen,
}

/// Error returned by [`CircuitBreaker::call`].
#[derive(Debug, thiserror::Error)]
pub enum CircuitBreakerError<E> {
    /// The breaker is open; the call was not attempted.
    #[error("circuit breaker is open")]
    Open,
    /// The guarded operation failed.
    #[error(transparent)]
    Inner(E),
}

struct Inner {
    state: State,
    consecutive_failures: u32,
    opened_at: Option<Instant>,
}

/// A circuit breaker that opens after `failure_threshold` consecutive failures and stays open for
/// `open_duration` before allowing a half-open trial. The lock is never held across `.await`.
pub struct CircuitBreaker {
    inner: Mutex<Inner>,
    failure_threshold: u32,
    open_duration: Duration,
}

impl CircuitBreaker {
    #[must_use]
    pub fn new(failure_threshold: u32, open_duration: Duration) -> Self {
        Self {
            inner: Mutex::new(Inner {
                state: State::Closed,
                consecutive_failures: 0,
                opened_at: None,
            }),
            failure_threshold,
            open_duration,
        }
    }

    #[must_use]
    pub fn state(&self) -> State {
        self.inner.lock().state
    }

    /// Runs `op` if the breaker permits it, updating state from the outcome.
    ///
    /// # Errors
    /// Returns [`CircuitBreakerError::Open`] if the breaker is open, otherwise the operation's error.
    pub async fn call<F, Fut, T, E>(&self, op: F) -> Result<T, CircuitBreakerError<E>>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        if !self.allow_request(Instant::now()) {
            return Err(CircuitBreakerError::Open);
        }
        match op().await {
            Ok(value) => {
                self.on_success();
                Ok(value)
            }
            Err(err) => {
                self.on_failure(Instant::now());
                Err(CircuitBreakerError::Inner(err))
            }
        }
    }

    fn allow_request(&self, now: Instant) -> bool {
        let mut inner = self.inner.lock();
        match inner.state {
            State::Closed | State::HalfOpen => true,
            State::Open => {
                let elapsed = inner
                    .opened_at
                    .map_or(Duration::MAX, |t| now.duration_since(t));
                if elapsed >= self.open_duration {
                    inner.state = State::HalfOpen;
                    true
                } else {
                    false
                }
            }
        }
    }

    fn on_success(&self) {
        let mut inner = self.inner.lock();
        inner.state = State::Closed;
        inner.consecutive_failures = 0;
        inner.opened_at = None;
    }

    fn on_failure(&self, now: Instant) {
        let mut inner = self.inner.lock();
        inner.consecutive_failures += 1;
        if inner.state == State::HalfOpen || inner.consecutive_failures >= self.failure_threshold {
            inner.state = State::Open;
            inner.opened_at = Some(now);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn ok() -> Result<u32, &'static str> {
        Ok(1)
    }
    async fn fail() -> Result<u32, &'static str> {
        Err("boom")
    }

    #[tokio::test]
    async fn opens_after_threshold_failures() {
        let cb = CircuitBreaker::new(2, Duration::from_millis(50));
        assert!(matches!(
            cb.call(fail).await,
            Err(CircuitBreakerError::Inner("boom"))
        ));
        assert_eq!(cb.state(), State::Closed);
        assert!(matches!(
            cb.call(fail).await,
            Err(CircuitBreakerError::Inner("boom"))
        ));
        assert_eq!(cb.state(), State::Open);
        // Now fast-rejected without invoking the op.
        assert!(matches!(cb.call(ok).await, Err(CircuitBreakerError::Open)));
    }

    #[tokio::test]
    async fn half_opens_then_closes_on_success() {
        let cb = CircuitBreaker::new(1, Duration::from_millis(20));
        let _ = cb.call(fail).await;
        assert_eq!(cb.state(), State::Open);
        tokio::time::sleep(Duration::from_millis(30)).await;
        // Trial call succeeds -> closed.
        assert_eq!(cb.call(ok).await.unwrap(), 1);
        assert_eq!(cb.state(), State::Closed);
    }

    #[tokio::test]
    async fn half_open_failure_reopens() {
        let cb = CircuitBreaker::new(1, Duration::from_millis(20));
        let _ = cb.call(fail).await;
        tokio::time::sleep(Duration::from_millis(30)).await;
        let _ = cb.call(fail).await; // half-open trial fails
        assert_eq!(cb.state(), State::Open);
    }
}
