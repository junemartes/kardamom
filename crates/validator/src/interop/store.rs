//! In-memory serving stores for the validator's feed surfaces (spec §5):
//! per-destination outbox lanes and the per-block attestation ring, both
//! retained for a configurable number of blocks. Deeper backfill is a v2
//! concern — the data is in DA.
//!
//! Writers are the engine-side seams (the extracting receipt sink, the
//! snapshot poller); readers are the WS subscription handlers, woken through
//! a `watch` version channel (the `MockInteropFeed` pattern — scan, then wait
//! for a bump).

use std::collections::{BTreeMap, VecDeque};
use std::num::NonZeroU64;
use std::sync::Mutex;

use alloy_primitives::B256;
use kardamom_types::xchain::OutboxMessage;
use tokio::sync::watch;

/// State plus its wake-up version, bumped on every mutation. Shared by
/// [`FeedStore`] and [`AttestationStore`]: lock, mutate, drop the lock,
/// bump the version is the one write path both use, and the version is
/// the one a subscriber's `subscribe()` waits on.
struct Versioned<T> {
    inner: Mutex<T>,
    version: watch::Sender<u64>,
}

impl<T> Versioned<T> {
    fn new(value: T) -> Self {
        Self {
            inner: Mutex::new(value),
            version: watch::channel(0).0,
        }
    }

    /// Read the state under the lock, and release it when this returns.
    fn read<R>(&self, f: impl FnOnce(&T) -> R) -> R {
        f(&crate::lock_recover(&self.inner))
    }

    /// Mutate the state under the lock, release it, then bump the
    /// wake-up version. A version, not a count: wrapping is the meaning
    /// here, and a subscriber only ever compares it for change.
    fn mutate(&self, f: impl FnOnce(&mut T)) {
        f(&mut crate::lock_recover(&self.inner));
        self.version.send_modify(|v| *v = v.wrapping_add(1));
    }

    fn subscribe(&self) -> watch::Receiver<u64> {
        self.version.subscribe()
    }
}

/// One destination's retained messages, in seq order (appends arrive in
/// block order and the Outbox's per-destination counter is dense, so pushes
/// are naturally ordered).
#[derive(Debug, Default)]
struct Lane {
    msgs: VecDeque<OutboxMessage>,
    /// Seq of the first retained message — everything below aged out of
    /// retention. A subscriber whose cursor is below this floor is LAGGED.
    floor: u64,
}

impl Lane {
    /// Drop every message below `cutoff` (an origin block number), raising
    /// `floor` past each one dropped.
    fn prune_below(&mut self, cutoff: u64) {
        while let Some(front) = self.msgs.front() {
            if front.origin_block_number >= cutoff {
                break;
            }
            // `seq` is wire-derived (from the extractor's own
            // re-execution, but ultimately from send counts a contract
            // can run arbitrarily high). A wrap would set the retention
            // floor to 0 and hide the loss.
            self.floor = front.seq.saturating_add(1);
            self.msgs.pop_front();
        }
    }
}

#[derive(Debug, Default)]
struct FeedInner {
    lanes: BTreeMap<u64, Lane>,
    /// Highest block observed (boundary-driven) — the retention anchor.
    head_block: u64,
}

/// Per-destination outbox feed store with block-based retention.
pub struct FeedStore {
    origin_chain_id: u64,
    retention_blocks: NonZeroU64,
    state: Versioned<FeedInner>,
}

impl FeedStore {
    #[must_use]
    pub fn new(origin_chain_id: u64, retention_blocks: NonZeroU64) -> Self {
        Self {
            origin_chain_id,
            retention_blocks,
            state: Versioned::new(FeedInner::default()),
        }
    }

    /// The chain this store serves for (stamped on every wire item).
    pub fn origin_chain_id(&self) -> u64 {
        self.origin_chain_id
    }

    /// Record one block's extracted messages (possibly none — every block
    /// advances the retention head) and prune lanes that aged out.
    pub fn append_block(&self, block: u64, msgs: Vec<OutboxMessage>) {
        let retention_blocks = self.retention_blocks.get();
        self.state.mutate(|g| {
            g.head_block = g.head_block.max(block);
            for m in msgs {
                let lane = g.lanes.entry(m.dest_chain_id).or_default();
                lane.msgs.push_back(m);
            }
            let cutoff = g.head_block.saturating_sub(retention_blocks);
            for lane in g.lanes.values_mut() {
                lane.prune_below(cutoff);
            }
        });
    }

    /// Everything retained for `dest` from `from_seq` onward, plus the
    /// lane's retention floor. `floor > from_seq` ⇒ the subscriber lagged
    /// out of retention and `floor - from_seq` items are gone.
    pub fn from_seq(&self, dest: u64, from_seq: u64) -> (Vec<OutboxMessage>, u64) {
        self.state.read(|g| match g.lanes.get(&dest) {
            Some(lane) => (
                lane.msgs
                    .iter()
                    .filter(|m| m.seq >= from_seq)
                    .cloned()
                    .collect(),
                lane.floor,
            ),
            None => (Vec::new(), 0),
        })
    }

    /// Wake-up channel for subscription handlers (tap BEFORE the first scan).
    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.state.subscribe()
    }
}

/// Ring of this validator's per-block `(block_number, state_root)`
/// attestations, block-retained like the feed store.
pub struct AttestationStore {
    retention_blocks: NonZeroU64,
    state: Versioned<VecDeque<(u64, B256)>>,
}

impl AttestationStore {
    #[must_use]
    pub fn new(retention_blocks: NonZeroU64) -> Self {
        Self {
            retention_blocks,
            state: Versioned::new(VecDeque::new()),
        }
    }

    /// Record `block`'s committed state root (call once per block, in order).
    pub fn push(&self, block: u64, state_root: B256) {
        let retention_blocks = self.retention_blocks.get();
        self.state.mutate(|g| {
            g.push_back((block, state_root));
            let cutoff = block.saturating_sub(retention_blocks);
            while g.front().is_some_and(|(b, _)| *b < cutoff) {
                g.pop_front();
            }
        });
    }

    /// Retained attestations from `from_block` onward, plus the first
    /// retained block (the retention floor; 0 when nothing is retained yet).
    pub fn from_block(&self, from_block: u64) -> (Vec<(u64, B256)>, u64) {
        self.state.read(|g| {
            let floor = g.front().map_or(0, |(b, _)| *b);
            (
                g.iter()
                    .filter(|(b, _)| *b >= from_block)
                    .copied()
                    .collect(),
                floor,
            )
        })
    }

    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.state.subscribe()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nz(n: u64) -> NonZeroU64 {
        NonZeroU64::new(n).expect("fixture retention")
    }

    use crate::interop::outbox_msg as msg;

    #[test]
    fn lanes_are_per_destination_and_cursor_scans_by_seq() {
        let store = FeedStore::new(1, nz(100));
        store.append_block(10, vec![msg(7, 0, 10), msg(9, 0, 10)]);
        store.append_block(11, vec![msg(7, 1, 11)]);

        let (m7, floor) = store.from_seq(7, 0);
        assert_eq!(m7.iter().map(|m| m.seq).collect::<Vec<_>>(), vec![0, 1]);
        assert_eq!(floor, 0);
        let (m7, _) = store.from_seq(7, 1);
        assert_eq!(m7.iter().map(|m| m.seq).collect::<Vec<_>>(), vec![1]);
        let (m9, _) = store.from_seq(9, 0);
        assert_eq!(m9.len(), 1);
        // Unknown destination: empty, floor 0 (nothing was ever pruned).
        assert_eq!(store.from_seq(999, 0), (Vec::new(), 0));
    }

    #[test]
    fn retention_prunes_by_block_and_raises_the_floor() {
        let store = FeedStore::new(1, nz(5));
        store.append_block(10, vec![msg(7, 0, 10), msg(7, 1, 10)]);
        store.append_block(12, vec![msg(7, 2, 12)]);
        // Head advances well past block 10: seqs 0-1 age out.
        store.append_block(16, vec![]);
        let (m, floor) = store.from_seq(7, 0);
        assert_eq!(m.iter().map(|m| m.seq).collect::<Vec<_>>(), vec![2]);
        assert_eq!(floor, 2, "floor names the first retained seq");
        // A cursor below the floor is the Lagged case; the caller computes
        // skipped = floor - cursor.
        assert!(floor > 0);
    }

    #[test]
    fn empty_blocks_advance_retention_without_messages() {
        let store = FeedStore::new(1, nz(2));
        store.append_block(1, vec![msg(7, 0, 1)]);
        for b in 2..=10 {
            store.append_block(b, vec![]);
        }
        let (m, floor) = store.from_seq(7, 0);
        assert!(m.is_empty());
        assert_eq!(floor, 1);
    }

    #[test]
    fn append_wakes_subscribers() {
        let store = FeedStore::new(1, nz(100));
        let mut rx = store.subscribe();
        let before = *rx.borrow_and_update();
        store.append_block(1, vec![]);
        assert!(rx.has_changed().unwrap());
        assert!(*rx.borrow_and_update() > before);
    }

    #[test]
    fn attestation_ring_retains_and_floors() {
        let ring = AttestationStore::new(nz(3));
        for b in 1..=10u64 {
            let byte = u8::try_from(b).expect("fixture block < 256");
            ring.push(b, B256::repeat_byte(byte));
        }
        let (all, floor) = ring.from_block(0);
        assert_eq!(floor, 7, "cutoff = 10 - 3");
        assert_eq!(all.first().unwrap().0, 7);
        assert_eq!(all.last().unwrap().0, 10);
        let (tail, _) = ring.from_block(9);
        assert_eq!(tail.len(), 2);
    }
}
