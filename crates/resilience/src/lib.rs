//! Composable resilience primitives: retry, circuit breaker, timeout and rate limiting.
#![forbid(unsafe_code)]

pub mod circuit_breaker;
pub mod rate_limit;
pub mod retry;
pub mod timeout;

pub use circuit_breaker::{CircuitBreaker, CircuitBreakerError, State};
pub use rate_limit::TokenBucket;
pub use retry::{retry_with_backoff, RetryPolicy};
pub use timeout::{with_timeout, TimeoutError};
