//! Retry with capped exponential backoff.

use std::future::Future;
use std::time::Duration;

/// Policy controlling retry attempts and backoff growth.
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub initial_backoff: Duration,
    pub multiplier: f64,
    pub max_backoff: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 4,
            initial_backoff: Duration::from_millis(50),
            multiplier: 2.0,
            max_backoff: Duration::from_secs(5),
        }
    }
}

impl RetryPolicy {
    #[must_use]
    pub fn new(max_attempts: u32, initial_backoff: Duration) -> Self {
        Self {
            max_attempts,
            initial_backoff,
            ..Self::default()
        }
    }
}

/// Runs `op`, retrying on `Err` up to `policy.max_attempts` with exponential backoff between tries.
/// Returns the last error if all attempts fail.
///
/// # Errors
/// Propagates the final error `E` from `op` once retries are exhausted.
pub async fn retry_with_backoff<F, Fut, T, E>(policy: &RetryPolicy, mut op: F) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: std::fmt::Display,
{
    let mut attempt: u32 = 1;
    let mut backoff = policy.initial_backoff;
    loop {
        match op().await {
            Ok(value) => return Ok(value),
            Err(err) => {
                if attempt >= policy.max_attempts {
                    tracing::warn!(attempt, error = %err, "operation failed; retries exhausted");
                    return Err(err);
                }
                tracing::warn!(attempt, error = %err, backoff_ms = backoff.as_millis() as u64,
                    "operation failed; retrying after backoff");
                tokio::time::sleep(backoff).await;
                backoff = backoff.mul_f64(policy.multiplier).min(policy.max_backoff);
                attempt += 1;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    #[tokio::test(start_paused = true)]
    async fn succeeds_after_transient_failures() {
        let calls = Arc::new(AtomicU32::new(0));
        let policy = RetryPolicy::new(5, Duration::from_millis(10));
        let calls_ref = calls.clone();
        let result: Result<u32, &str> = retry_with_backoff(&policy, || {
            let calls = calls_ref.clone();
            async move {
                let n = calls.fetch_add(1, Ordering::SeqCst) + 1;
                if n < 3 {
                    Err("transient")
                } else {
                    Ok(n)
                }
            }
        })
        .await;
        assert_eq!(result, Ok(3));
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn gives_up_after_max_attempts() {
        let calls = Arc::new(AtomicU32::new(0));
        let policy = RetryPolicy::new(3, Duration::from_millis(10));
        let calls_ref = calls.clone();
        let result: Result<u32, &str> = retry_with_backoff(&policy, || {
            let calls = calls_ref.clone();
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Err("always")
            }
        })
        .await;
        assert_eq!(result, Err("always"));
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }
}
