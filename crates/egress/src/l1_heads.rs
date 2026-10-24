//! [`L1Heads`]: the heads that L1 holds for good, and the floor of the
//! store that follows them.
//!
//! The store keeps every block above `min(posted_head, final_head)`:
//! - the posted head is the newest L2 block that a finalized `BatchPosted`
//!   event covers. The batcher reads the blocks above it from the store
//!   to post them;
//! - the final head is the newest L2 block of an output whose challenge
//!   window ended (`WithdrawalOutputOracle.isFinalizable`). The attester
//!   and a challenger read the blocks above it. With no oracle, the floor
//!   is the posted head.
//!
//! A head that the egress did not read yet is 0, so the store keeps every
//! block until it knows each head. The store also keeps `--keep-blocks`
//! blocks below its head for the readers that resume, whatever the heads
//! say.

use std::num::NonZeroU64;

use crate::metrics;

/// The heads of L1 that bound the prune of the store.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct L1Heads {
    posted: u64,
    /// `None` with no oracle.
    final_head: Option<u64>,
}

impl L1Heads {
    /// The heads with no oracle: the floor is the posted head.
    #[must_use]
    pub const fn posted_only() -> Self {
        Self {
            posted: 0,
            final_head: None,
        }
    }

    /// The heads with an oracle: the floor is the lower of the two heads.
    #[must_use]
    pub const fn with_oracle() -> Self {
        Self {
            posted: 0,
            final_head: Some(0),
        }
    }

    /// A finalized post covers the blocks through `block`. An older post
    /// changes nothing.
    pub fn post(&mut self, block: u64) {
        self.posted = self.posted.max(block);
    }

    /// An output that covers the blocks through `block` is final. An
    /// older output changes nothing.
    pub fn finalize(&mut self, block: u64) {
        self.final_head = self.final_head.map(|head| head.max(block));
    }

    /// The newest block that L1 holds for good.
    #[must_use]
    pub fn floor(self) -> u64 {
        self.final_head
            .map_or(self.posted, |head| head.min(self.posted))
    }

    /// The first block that the store keeps when the next block of its
    /// head is `next_block`: the block after the floor, or the first block
    /// of the window of `keep_blocks` blocks below the head, whichever is
    /// lower.
    #[must_use]
    pub fn keep_from(self, next_block: u64, keep_blocks: NonZeroU64) -> u64 {
        // A window longer than the chain starts at block 0. A floor at
        // u64::MAX keeps only the window.
        let window = next_block.saturating_sub(keep_blocks.get());
        window.min(self.floor().saturating_add(1))
    }

    /// Export the distances of the stored head from the posted head and
    /// from the floor. A head above the stored head counts 0.
    pub(crate) fn show(self, next_block: u64) {
        // The store holds the blocks below `next_block`. Genesis holds none.
        let stored = next_block.saturating_sub(1);
        metrics::set_posted_distance(stored.saturating_sub(self.posted));
        metrics::set_floor_distance(stored.saturating_sub(self.floor()));
    }
}

#[cfg(test)]
#[path = "l1_heads_tests.rs"]
mod tests;
