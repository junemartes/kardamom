//! The chain's data-availability status, as the sealer fans it out to every
//! cluster session.

use core::time::Duration;
#[cfg(feature = "std")]
use std::time::Instant;

/// How long a cluster session may see no frame before its holder calls
/// the sealer halted on a lost quorum. The sealer sends a boundary and a
/// status on every tick (2 s in the deploy), so this is five ticks: longer
/// than a leader election.
pub const SEALER_SILENCE: Duration = Duration::from_secs(10);

/// How long a cluster session may see no frame after a status with the
/// snapshot notice. The consensus module sends nothing while the members
/// record the snapshot. Each recording is a chain of disk syncs, and on a
/// loaded disk the stall reached 11 s. The budget is about five times
/// that, and it equals the cap of the session's egress-silence window. A
/// lost quorum during a snapshot shows within this budget.
pub const SNAPSHOT_SILENCE: Duration = Duration::from_mins(1);

/// The sealer's view of data availability: the last L2 block the batcher
/// confirmed on L1 (its published cursor), the last sealed block, the
/// DA-lag budget, and whether the guard refuses new transactions. The
/// egress retention floors ride along, so an observer can show how far
/// the retention stretched above the posted head. The record-lag guard
/// follows.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ClusterStatus {
    /// The last L2 block posted to L1; 0 until the batcher publishes.
    pub posted_head: u64,
    /// The last sealed block.
    pub sealed_head: u64,
    /// The DA-lag budget in blocks; 0 means the guard is off.
    pub budget_blocks: u64,
    /// Whether the sealer refuses new transactions.
    pub halted: bool,
    /// The egress frames the member retains for replay.
    pub retained_frames: u64,
    /// The oldest record index still retained.
    pub floor_index: u64,
    /// The oldest boundary block still retained.
    pub floor_block: u64,
    /// The record-lag guard. A status frame from a sealer that does not
    /// send the guard reads as the default: no cursor, the guard off.
    pub record_lag: RecordLagStatus,
    /// A snapshot follows this status, and no frame comes until it is
    /// complete. A status frame without the snapshot tail reads as false.
    pub snapshot_follows: bool,
}

/// The sealer's record-lag guard: the best recorded cursor of the
/// executors, the budget, and whether the guard refuses new transactions.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RecordLagStatus {
    /// The highest canonical index that one executor or more recorded;
    /// `None` before the first recorded cursor.
    pub best_recorded: Option<u64>,
    /// The budget in canonical records; 0 means the guard is off.
    pub budget: u64,
    /// Whether the guard refuses new transactions.
    pub halted: bool,
}

impl ClusterStatus {
    /// How far the sealed head runs past the posted head.
    #[must_use]
    pub fn lag(&self) -> u64 {
        self.sealed_head.saturating_sub(self.posted_head)
    }
}

/// The silence check of one cluster session: how long the session may see
/// no frame, and since when it sees none.
///
/// A status or a boundary restarts the silence. A status with the
/// snapshot notice starts a snapshot: the budget is [`SNAPSHOT_SILENCE`]
/// until the next boundary or the next status without the notice. Other
/// frames do not change the check: the leader can send a relayed record
/// after the notice, before the snapshot stops it.
///
/// It reads the clock of the host, so it needs the `std` feature.
#[cfg(feature = "std")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SilenceDeadline {
    since: Instant,
    snapshot: bool,
}

#[cfg(feature = "std")]
impl SilenceDeadline {
    /// A check whose silence starts at `now`, with the normal budget.
    #[must_use]
    pub fn new(now: Instant) -> Self {
        Self {
            since: now,
            snapshot: false,
        }
    }

    /// A status frame arrived at `now`.
    pub fn on_status(&mut self, status: &ClusterStatus, now: Instant) {
        self.since = now;
        self.snapshot = status.snapshot_follows;
    }

    /// A boundary frame arrived at `now`. A snapshot is complete.
    pub fn on_boundary(&mut self, now: Instant) {
        self.since = now;
        self.snapshot = false;
    }

    /// How long the session may see no frame: [`SNAPSHOT_SILENCE`] in a
    /// snapshot, else [`SEALER_SILENCE`].
    #[must_use]
    pub fn budget(&self) -> Duration {
        if self.snapshot {
            SNAPSHOT_SILENCE
        } else {
            SEALER_SILENCE
        }
    }

    /// The silence at `now` when it reached the budget, else `None`.
    #[must_use]
    pub fn expired(&self, now: Instant) -> Option<Duration> {
        Some(now.saturating_duration_since(self.since)).filter(|silent| *silent >= self.budget())
    }

    /// Whether a snapshot that a notice announced covers `now`.
    #[must_use]
    pub fn in_snapshot(&self, now: Instant) -> bool {
        self.snapshot && self.expired(now).is_none()
    }
}

#[cfg(all(test, feature = "std"))]
#[path = "cluster_status_tests.rs"]
mod tests;
