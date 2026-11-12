//! The archive connect of a recorder start, with a bounded retry.
//!
//! A restarted node can start the archive after the process that records
//! on it. The connect then times out, and a later try succeeds. So the
//! start tries again until the connect limit passes. A stop ends the
//! tries at once.

use std::ops::ControlFlow;
use std::time::{Duration, Instant};

use tracing::warn;

use super::DiscoveredRecorder;
use crate::error::LogError;
use crate::recorder::ARCHIVE_CONNECT_TIMEOUT;

/// The pause between two connect tries. A refused connect fails at once,
/// and the pause keeps such tries apart.
const CONNECT_PAUSE: Duration = Duration::from_secs(1);

/// How long the own recordings take to go live once the archive session
/// connects. One catalog poll is about 500 ms. The budget only bounds a
/// stuck archive.
const LIVE_BUDGET: Duration = Duration::from_mins(1);

impl DiscoveredRecorder {
    /// How long a start waits for [`super::RecorderProgress::Ready`]: the
    /// connect tries, one last connect, and the start of the own
    /// recordings.
    #[must_use]
    pub fn ready_budget(&self) -> Duration {
        // A budget past `Duration::MAX` has no end, which is the meaning
        // of so long a limit.
        self.connect_limit
            .saturating_add(ARCHIVE_CONNECT_TIMEOUT)
            .saturating_add(LIVE_BUDGET)
    }

    /// Run `connect` until it succeeds, the connect limit passes, or the
    /// stop token is cancelled.
    pub(super) fn connect_with<S>(
        &self,
        mut connect: impl FnMut() -> Result<S, LogError>,
    ) -> Result<S, LogError> {
        let start = Instant::now();
        let mut failures: u32 = 0;
        loop {
            match self.connect_step(connect(), start.elapsed(), &mut failures) {
                ControlFlow::Break(done) => return done,
                ControlFlow::Continue(()) => {}
            }
        }
    }

    /// The step after one connect try that ended `elapsed` after the first
    /// try started: the result, or a pause before the next try.
    fn connect_step<S>(
        &self,
        outcome: Result<S, LogError>,
        elapsed: Duration,
        failures: &mut u32,
    ) -> ControlFlow<Result<S, LogError>> {
        let error = match outcome {
            Ok(session) => return ControlFlow::Break(Ok(session)),
            Err(error) => error,
        };
        if elapsed >= self.connect_limit {
            return ControlFlow::Break(Err(error));
        }
        // The count only names the try in the log. The connect limit ends
        // the tries long before `u32::MAX`.
        *failures = failures.saturating_add(1);
        warn!(
            failures = *failures,
            error = %error,
            left_ms = self.connect_limit.saturating_sub(elapsed).as_millis(),
            "discovered recorder: archive connect failed; trying again"
        );
        self.pause_connect();
        if self.stop.is_cancelled() {
            return ControlFlow::Break(Err(error));
        }
        ControlFlow::Continue(())
    }

    /// Sleep [`CONNECT_PAUSE`], or less when the stop token is cancelled.
    /// The thread runs outside every runtime, so it blocks on the handle.
    fn pause_connect(&self) {
        let stop = self.stop.clone();
        self.runtime.block_on(async {
            tokio::select! {
                () = stop.cancelled() => {}
                () = tokio::time::sleep(CONNECT_PAUSE) => {}
            }
        });
    }
}
