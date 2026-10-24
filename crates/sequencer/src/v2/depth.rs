//! The parked depth of each vslot, for the `pending_depth` gauge of the v2
//! loop.
//!
//! The filter gives each transaction that it takes exactly one forward or
//! refusal, at once or after it parked. So the depth of a vslot is the
//! transactions that the filter took, less the verdicts that it gave, for
//! the senders of the vslot. The loop counts both in fixed arrays and sets
//! the gauges of the changed vslots at each sweep.

use crate::metrics;

/// The vslots of the shard map.
const VSLOTS: usize = 256;

pub(super) struct ParkDepth {
    depth: Box<[u32; VSLOTS]>,
    /// One bit per vslot whose depth changed since the last publish.
    changed: [u64; VSLOTS / 64],
}

impl Default for ParkDepth {
    fn default() -> Self {
        Self {
            depth: Box::new([0; VSLOTS]),
            changed: [0; VSLOTS / 64],
        }
    }
}

impl ParkDepth {
    /// The filter took a transaction of a sender in `vslot`.
    pub(super) fn took(&mut self, vslot: u8) {
        let i = usize::from(vslot);
        self.depth[i] = self.depth[i].saturating_add(1);
        self.mark(i);
    }

    /// The filter gave the verdict of a transaction of a sender in `vslot`.
    pub(super) fn gave(&mut self, vslot: u8) {
        let i = usize::from(vslot);
        self.depth[i] = self.depth[i].saturating_sub(1);
        self.mark(i);
    }

    /// The depth of `vslot`.
    #[cfg(test)]
    pub(super) fn of(&self, vslot: u8) -> u32 {
        self.depth[usize::from(vslot)]
    }

    /// Set the gauge of each vslot whose depth changed.
    pub(super) fn publish(&mut self, partition: u32) {
        for vslot in 0..=u8::MAX {
            self.publish_one(partition, vslot);
        }
        self.changed = [0; VSLOTS / 64];
    }

    fn publish_one(&self, partition: u32, vslot: u8) {
        let i = usize::from(vslot);
        if self.changed[i / 64] & (1 << (i % 64)) != 0 {
            metrics::record_pending_depth(partition, vslot, self.depth[i]);
        }
    }

    fn mark(&mut self, i: usize) {
        self.changed[i / 64] |= 1 << (i % 64);
    }
}
