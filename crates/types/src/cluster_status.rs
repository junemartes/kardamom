//! The chain's data-availability status, as the sealer fans it out to every
//! cluster session.

/// The sealer's view of data availability: the last L2 block the batcher
/// confirmed on L1 (its published cursor), the last sealed block, the
/// DA-lag budget, and whether the guard refuses new transactions. The
/// egress retention floors ride along, so an observer can show how far
/// the retention stretched above the posted head.
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
}

impl ClusterStatus {
    /// How far the sealed head runs past the posted head.
    #[must_use]
    pub fn lag(&self) -> u64 {
        self.sealed_head.saturating_sub(self.posted_head)
    }
}
