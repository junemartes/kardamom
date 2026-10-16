//! The chain's data-availability status, as the sealer fans it out to every
//! cluster session.

use std::time::Duration;

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

    /// How long the session may see no frame after this status:
    /// [`SNAPSHOT_SILENCE`] when a snapshot follows, else
    /// [`SEALER_SILENCE`].
    #[must_use]
    pub fn silence_budget(&self) -> Duration {
        if self.snapshot_follows {
            SNAPSHOT_SILENCE
        } else {
            SEALER_SILENCE
        }
    }
}
