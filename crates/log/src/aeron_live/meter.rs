//! The pipeline metrics of the Aeron thread (see
//! `kardamom_obs::pipeline`). The thread is one per process and carries
//! the I/O of every service, so the `service` label of the exporter tells
//! the processes apart.

use kardamom_obs::pipeline::{LoopMeter, LoopState, Queue};
use metrics::Counter;

/// The offers that Aeron refused with a transient status
/// (`BACK_PRESSURED`, `NOT_CONNECTED`, `ADMIN_ACTION`) and that the retry
/// queue keeps for the next pass. One frame counts once for each refused
/// offer.
pub(super) const OFFER_RETRIES_TOTAL: &str = "kardamom_log_offer_retries_total";

/// The publishes in the retry queue of the Aeron thread after a pass.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Backlog(pub(super) usize);

impl Backlog {
    /// The state of a span with no work: blocked while a refused offer
    /// waits for its retry, idle otherwise.
    pub(super) fn quiet_state(self) -> LoopState {
        if self.0 == 0 {
            LoopState::Idle
        } else {
            LoopState::Blocked
        }
    }

    /// The state of one pass: busy when the pass handled a command or
    /// polled a fragment, else the quiet state.
    pub(super) fn pass_state(self, worked: bool) -> LoopState {
        if worked {
            LoopState::Busy
        } else {
            self.quiet_state()
        }
    }
}

/// The pipeline metrics of the Aeron thread. The thread owns them.
pub(super) struct ThreadMeter {
    pass: LoopMeter,
    commands: Queue,
    pending: Queue,
    retries: Counter,
}

impl ThreadMeter {
    pub(super) fn new() -> Self {
        Self {
            pass: LoopMeter::new("log.aeron"),
            commands: Queue::new("log.aeron_commands"),
            pending: Queue::new("log.aeron_pending"),
            retries: metrics::counter!(OFFER_RETRIES_TOTAL),
        }
    }

    /// Start a pass: set the depth of the command queue, the commands
    /// that came in during the last pass and wait.
    pub(super) fn start_pass(&self, commands: usize) {
        self.commands.set(commands);
    }

    /// End the work of a pass. `refused` is the count of offers that the
    /// pass retried and Aeron refused again.
    pub(super) fn end_pass(&mut self, worked: bool, backlog: Backlog, refused: usize) {
        self.pass.mark(backlog.pass_state(worked));
        self.pending.set(backlog.0);
        // A usize count fits in a u64 on every supported target.
        self.retries
            .increment(u64::try_from(refused).unwrap_or(u64::MAX));
    }

    /// End the wait for a command.
    pub(super) fn end_wait(&mut self, backlog: Backlog) {
        self.pass.mark(backlog.quiet_state());
    }

    /// End the handling of a command that ended a wait.
    pub(super) fn end_command(&mut self) {
        self.pass.mark(LoopState::Busy);
    }
}

#[cfg(test)]
#[path = "meter_tests.rs"]
mod tests;
