//! The bounded retry of a start-up open: a publication, a subscription,
//! or a catalog registration that a service opens once before it serves.
//!
//! A loaded host can delay the media driver or the catalog agent past one
//! request budget. Such a delay ends, so the open tries again after a
//! pause. The pauses follow the catalog backoff of `[discovery]`. The
//! tries stop at the limit: two Aeron stall budgets
//! ([`AeronRuntime::stall_budget`]), so the stall tolerance of the deploy
//! (`AERON_DRIVER_TIMEOUT`) sets it. After the limit, or at once on an
//! error that no later try can clear, the open returns its last error.
//! The service then exits as it does without a retry: a service that
//! cannot open its streams has no state to serve.
//!
//! [`AeronRuntime::stall_budget`]: crate::aeron_live::AeronRuntime::stall_budget

use std::fmt::Display;
use std::future::Future;
use std::ops::ControlFlow;
use std::time::Duration;

use tracing::warn;

use super::watch::WatchTiming;
use crate::error::LogError;

/// The retry state of one start-up open. See the module doc.
pub struct StartRetry {
    /// The stream, topic, or record that the open names, for the log.
    what: String,
    limit: Duration,
    backoff: WatchTiming,
    failures: u32,
}

impl StartRetry {
    /// The number of Aeron stall budgets in the limit. One budget is the
    /// wait through a stall that every Aeron party survives. The second
    /// budget covers the tries that the stall delays.
    const STALL_BUDGETS: u32 = 2;

    /// The retry of the open of `what`, limited to two `stall_budget`s,
    /// with the pauses of `backoff`.
    #[must_use]
    pub fn new(what: impl Display, stall_budget: Duration, backoff: WatchTiming) -> Self {
        Self {
            what: what.to_string(),
            // A limit past `Duration::MAX` has no end, which is the
            // meaning of so long a stall budget.
            limit: stall_budget.saturating_mul(Self::STALL_BUDGETS),
            backoff,
            failures: 0,
        }
    }

    /// The time after the first try at which the tries stop.
    #[must_use]
    pub fn limit(&self) -> Duration {
        self.limit
    }

    /// Run `open` until it succeeds, fails with an error that is not
    /// transient, or the limit passes.
    ///
    /// # Errors
    ///
    /// Returns the last error of `open`.
    pub async fn run<T, F, Fut>(mut self, mut open: F) -> Result<T, LogError>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, LogError>>,
    {
        let start = tokio::time::Instant::now();
        loop {
            match self.next(open().await, start.elapsed()) {
                ControlFlow::Break(done) => return done,
                ControlFlow::Continue(pause) => tokio::time::sleep(pause).await,
            }
        }
    }

    /// [`Self::run`] for an open that blocks its thread. The pauses block
    /// the thread too.
    ///
    /// # Errors
    ///
    /// Returns the last error of `open`.
    pub fn run_blocking<T>(
        mut self,
        mut open: impl FnMut() -> Result<T, LogError>,
    ) -> Result<T, LogError> {
        let start = std::time::Instant::now();
        loop {
            match self.next(open(), start.elapsed()) {
                ControlFlow::Break(done) => return done,
                ControlFlow::Continue(pause) => std::thread::sleep(pause),
            }
        }
    }

    /// The step after one try that ended `elapsed` after the first try
    /// started: the result, or the pause before the next try.
    fn next<T>(
        &mut self,
        outcome: Result<T, LogError>,
        elapsed: Duration,
    ) -> ControlFlow<Result<T, LogError>, Duration> {
        let error = match outcome {
            Ok(value) => return ControlFlow::Break(Ok(value)),
            Err(error) => error,
        };
        // A try that ends past the limit leaves no time, which is the
        // meaning of a negative rest.
        let left = self.limit.saturating_sub(elapsed);
        if !error.is_transient() || left.is_zero() {
            return ControlFlow::Break(Err(error));
        }
        // The count only grows the pause, and the backoff cap stops that
        // growth long before `u32::MAX`.
        self.failures = self.failures.saturating_add(1);
        let pause = self.backoff.backoff(self.failures).min(left);
        warn!(
            open = %self.what,
            failures = self.failures,
            error = %error,
            retry_in_ms = pause.as_millis(),
            left_ms = left.as_millis(),
            "discovery: start-up open failed; trying again"
        );
        ControlFlow::Continue(pause)
    }
}

#[cfg(test)]
mod tests;
