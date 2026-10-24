//! The output wait of a metered thread loop.

use kardamom_obs::pipeline::{LoopMeter, LoopState};

/// A [`LoopMeter`] that charges a wait on a full output to
/// [`LoopState::Blocked`].
pub(crate) trait OutputWait {
    /// Run `send`. When `full` is true, the output has no room, so `send`
    /// waits: the meter charges the work before it to busy, and the wait
    /// to blocked. When `full` is false, `send` returns at once, and the
    /// meter is not read.
    fn output_wait<R>(&mut self, full: bool, send: impl FnOnce() -> R) -> R;
}

impl OutputWait for LoopMeter {
    fn output_wait<R>(&mut self, full: bool, send: impl FnOnce() -> R) -> R {
        if !full {
            return send();
        }
        self.mark(LoopState::Busy);
        let outcome = send();
        self.mark(LoopState::Blocked);
        outcome
    }
}

#[cfg(test)]
#[path = "meter_tests.rs"]
pub(crate) mod tests;
