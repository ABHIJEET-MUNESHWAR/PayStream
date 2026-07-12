//! Timeout wrapper around a future.

use std::future::Future;
use std::time::Duration;

/// Returned when a guarded future does not complete within the budget.
#[derive(Debug, thiserror::Error)]
#[error("operation timed out after {0:?}")]
pub struct TimeoutError(pub Duration);

/// Awaits `fut`, failing with [`TimeoutError`] if it does not finish within `budget`.
///
/// # Errors
/// Returns [`TimeoutError`] on expiry.
pub async fn with_timeout<Fut, T>(budget: Duration, fut: Fut) -> Result<T, TimeoutError>
where
    Fut: Future<Output = T>,
{
    tokio::time::timeout(budget, fut)
        .await
        .map_err(|_| TimeoutError(budget))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn completes_within_budget() {
        let out = with_timeout(Duration::from_secs(1), async { 42 }).await;
        assert_eq!(out.unwrap(), 42);
    }

    #[tokio::test(start_paused = true)]
    async fn times_out_when_slow() {
        let result = with_timeout(Duration::from_millis(10), async {
            tokio::time::sleep(Duration::from_secs(10)).await;
            1
        })
        .await;
        assert!(result.is_err());
    }
}
