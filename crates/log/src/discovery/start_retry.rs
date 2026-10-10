//! The bounded retry of a start-up registration: a catalog record that a
//! service registers once before it serves.
//!
//! A loaded host or an agent restart can delay the Consul agent past one
//! request budget. Such a delay ends, and a registration is idempotent,
//! so the registration tries again after a pause. The pauses follow the
//! catalog backoff of `[discovery]`. The tries stop at the limit that
//! the caller gives: the start-up limit of the Aeron runtime
//! ([`AeronRuntime::start_open_limit`]), so the stall tolerance of the
//! deploy (`AERON_DRIVER_TIMEOUT`) sets it. After the limit, or at once
//! on an error that no later try can clear, the registration returns its
//! last error.
//!
//! An Aeron add does not use this retry. A second add during a stall
//! only queues behind the first, so a start-up add waits the whole limit
//! once ([`AddWait`]).
//!
//! A stop token ends the retries at once with a "stopped" error.
//!
//! [`AeronRuntime::start_open_limit`]: crate::aeron_live::AeronRuntime::start_open_limit
//! [`AddWait`]: crate::aeron_live::AddWait

use std::fmt::Display;
use std::future::Future;
use std::ops::ControlFlow;
use std::time::Duration;

use tokio_util::sync::CancellationToken;
use tracing::warn;

use super::watch::WatchTiming;
use crate::error::LogError;

/// The retry state of one start-up registration. See the module doc.
pub struct StartRetry {
    /// The record that the registration names, for the log.
    what: String,
    limit: Duration,
    backoff: WatchTiming,
    stop: CancellationToken,
    failures: u32,
}

impl StartRetry {
    /// The retry of the registration of `what` for up to `limit` after
    /// the first try, with the pauses of `backoff`. A cancel of `stop`
    /// ends the retries.
    #[must_use]
    pub fn new(
        what: impl Display,
        limit: Duration,
        backoff: WatchTiming,
        stop: CancellationToken,
    ) -> Self {
        Self {
            what: what.to_string(),
            limit,
            backoff,
            stop,
            failures: 0,
        }
    }

    /// Run `register` until it succeeds, fails with an error that is not
    /// transient, the limit passes, or the stop token is cancelled. A
    /// cancel during a pause ends the pause at once.
    ///
    /// # Errors
    ///
    /// Returns the last error of `register`, or the "stopped" error after
    /// a cancel.
    pub async fn run<T, F, Fut>(mut self, mut register: F) -> Result<T, LogError>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, LogError>>,
    {
        let start = tokio::time::Instant::now();
        loop {
            match self.next(register().await, start.elapsed()) {
                ControlFlow::Break(done) => return done,
                ControlFlow::Continue(pause) => self.pause(pause).await?,
            }
        }
    }

    /// Sleep `pause`, or end with the "stopped" error when the stop token
    /// is cancelled first.
    async fn pause(&self, pause: Duration) -> Result<(), LogError> {
        tokio::select! {
            biased;
            () = self.stop.cancelled() => Err(self.stopped()),
            () = tokio::time::sleep(pause) => Ok(()),
        }
    }

    /// The error of a registration that a stop ended.
    fn stopped(&self) -> LogError {
        LogError::Discovery(format!("stopped during start-up open of {}", self.what))
    }

    /// The step after one try that ended `elapsed` after the first try
    /// started: the result, or the pause before the next try. A cancelled
    /// stop token ends the registration before the pause.
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
        if self.stop.is_cancelled() {
            warn!(open = %self.what, error = %error, "discovery: start-up open stopped");
            return ControlFlow::Break(Err(self.stopped()));
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
