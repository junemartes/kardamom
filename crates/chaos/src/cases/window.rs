//! The time bound of a case: its deadline, and the load window under it.

use std::time::Duration;

use super::Case;
use crate::knobs::Knobs;

/// The bound of a case of the nightly shard. Some repeat a fault many
/// times or rebuild a whole fleet, and the nightly job has no per-case
/// budget.
const NIGHTLY_DEADLINE: Duration = Duration::from_mins(60);

impl Case {
    /// The longest the case may run, from the start of its load to its
    /// verdict. Every case of a regular shard finishes within the
    /// deadline of the knobs, so a slow case fails at once with a clear
    /// cause instead of pushing its shard past the job limit.
    #[must_use]
    pub fn deadline(self, k: &Knobs) -> Duration {
        if crate::Shard::Integrity.cases().contains(&self.name()) {
            NIGHTLY_DEADLINE
        } else {
            k.case_deadline
        }
    }

    /// The case load window: the deadline. The harness ends the
    /// submissions when the case body ends, so the window is only the
    /// upper bound, and the load runs through every fault and recovery.
    #[must_use]
    pub fn window(self, k: &Knobs) -> Duration {
        self.deadline(k)
    }
}

#[cfg(test)]
mod tests;
