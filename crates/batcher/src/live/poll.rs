//! The periodic-reactor shape every poll binary in this crate shares
//! (`kardamom-batch-watcher`, `kardamom-batch-claimer`,
//! `kardamom-proof-submitter`): run one step, then retry immediately or
//! after an interval, or stop.
//!
//! Each binary defines a struct holding its own step driver (a
//! `BatchClaimer`, a `BatchWatcher`, a `ProofSubmitter`) plus a
//! [`PollLoop`], with a `tick(&self, provider: &P) -> ControlFlow<()>`
//! method whose body is `Self::report(step(provider).await)` followed by
//! `self.gate.gate(retry).await`. Its `run` drives it with
//! `while let ControlFlow::Continue(()) = self.tick(provider).await {}`
//! — the whole reactor, one line, with no nested loop or branch.

use std::ops::ControlFlow;
use std::time::Duration;

/// Parse a `--interval-secs` flag value: `0` means run once and stop
/// (`None`), and any other value is the poll interval. Parsing this once
/// at the CLI boundary means [`PollLoop`] never sees the `0` special case.
///
/// # Errors
/// Returns an error when `s` does not parse as a `u64`.
pub fn parse_interval_secs(s: &str) -> Result<Option<Duration>, std::num::ParseIntError> {
    let secs: u64 = s.parse()?;
    Ok(std::num::NonZeroU64::new(secs).map(|s| Duration::from_secs(s.get())))
}

/// One tick's outcome: retry immediately (more work may already be
/// ready), or wait for the poll interval.
pub enum Retry {
    Now,
    AfterInterval,
}

/// The retry/interval half of the reactor: given one tick's [`Retry`]
/// signal, decide whether the caller's `while let` loop keeps going.
/// `interval: None` means "run one tick and stop" (a poll binary's
/// `--interval-secs 0`, one-shot mode) once nothing is left to do.
pub struct PollLoop {
    interval: Option<Duration>,
}

impl PollLoop {
    #[must_use]
    pub fn new(interval: Option<Duration>) -> Self {
        Self { interval }
    }

    /// Turn `retry` into the loop's next move: [`ControlFlow::Continue`]
    /// to tick again (after sleeping the interval, when one is set), or
    /// [`ControlFlow::Break`] to stop.
    pub async fn gate(&self, retry: Retry) -> ControlFlow<()> {
        if matches!(retry, Retry::Now) {
            return ControlFlow::Continue(());
        }
        let Some(interval) = self.interval else {
            return ControlFlow::Break(());
        };
        tokio::time::sleep(interval).await;
        ControlFlow::Continue(())
    }
}
