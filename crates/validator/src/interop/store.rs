//! In-memory serving stores for the validator's feed surfaces:
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

/// Blocks a [`FeedStore`] or [`AttestationStore`] retains before pruning,
/// parsed once at the CLI boundary (`--feed-retention-blocks`). A
/// `NonZeroU64` wrapper by name: distinguishes "how many blocks to keep"
/// from any other bare `NonZeroU64` a caller might have on hand, at every
/// constructor that takes one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetentionBlocks(NonZeroU64);

impl RetentionBlocks {
    #[must_use]
    pub const fn new(n: NonZeroU64) -> Self {
        Self(n)
    }

    #[must_use]
    pub fn get(self) -> NonZeroU64 {
        self.0
    }
}

impl std::str::FromStr for RetentionBlocks {
    type Err = std::num::ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse().map(Self)
    }
}

impl std::fmt::Display for RetentionBlocks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

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

/// The first seq a lane can serve. A resumed store never saw the blocks
/// before its resume point, so it cannot name a floor until the lane's
/// first post-resume message arrives; a genesis start saw every block, so
/// its floor is known from the start (seq 0, raised as retention prunes).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LaneFloor {
    Known(u64),
    UnknownAfterResume,
}

impl LaneFloor {
    /// The floor a lane starts at: known at 0 for a genesis start, unknown
    /// until the first message for a resumed store.
    fn at_start(resumed: bool) -> Self {
        if resumed {
            Self::UnknownAfterResume
        } else {
            Self::Known(0)
        }
    }
}

/// One destination's retained messages, in seq order (appends arrive in
/// block order and the Outbox's per-destination counter is dense, so pushes
/// are naturally ordered).
#[derive(Debug)]
struct Lane {
    msgs: VecDeque<OutboxMessage>,
    floor: LaneFloor,
}

impl Lane {
    fn starting(resumed: bool) -> Self {
        Self {
            msgs: VecDeque::new(),
            floor: LaneFloor::at_start(resumed),
        }
    }

    /// Name the floor from the first message seen, if not already known.
    fn learn_floor(&mut self, seq: u64) {
        if matches!(self.floor, LaneFloor::UnknownAfterResume) {
            self.floor = LaneFloor::Known(seq);
        }
    }

    /// Drop every message below `cutoff` (an origin block number), raising
    /// `floor` past each one dropped.
    fn prune_below(&mut self, cutoff: u64) {
        while let Some(front) = self
            .msgs
            .pop_front_if(|front| front.origin_block_number < cutoff)
        {
            // `seq` is wire-derived (from the extractor's own
            // re-execution, but ultimately from send counts a contract
            // can run arbitrarily high). A wrap would set the retention
            // floor to 0 and hide the loss.
            self.floor = LaneFloor::Known(front.seq.saturating_add(1));
        }
    }
}

#[derive(Debug, Default)]
struct FeedInner {
    lanes: BTreeMap<u64, Lane>,
    /// Highest block observed (boundary-driven) — the retention anchor.
    head_block: u64,
}

/// One cursor scan of a lane: what [`FeedStore::from_seq`] returns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LaneScan {
    /// Retained messages at or after the cursor, in seq order.
    pub msgs: Vec<OutboxMessage>,
    /// First seq the store can serve for this lane, when known. Above the
    /// cursor, this means the subscriber lagged out.
    pub floor_seq: LaneFloor,
    /// First origin block the store can serve: the retention cutoff, or
    /// the resume block after a restart, whichever is later.
    pub floor_block: u64,
    /// Highest block the store has seen. Every message from a lower block
    /// is already in the store.
    pub head_block: u64,
}

/// Per-destination outbox feed store with block-based retention.
pub struct FeedStore {
    origin_chain_id: u64,
    retention_blocks: NonZeroU64,
    /// First block the extractor sees after a restart. Messages from
    /// earlier blocks never reach this store, so no lane floor is known
    /// until the lane's first message arrives. `None` for a genesis start.
    resume_block: Option<u64>,
    state: Versioned<FeedInner>,
}

impl FeedStore {
    #[must_use]
    pub fn new(origin_chain_id: u64, retention_blocks: RetentionBlocks) -> Self {
        Self {
            origin_chain_id,
            retention_blocks: retention_blocks.get(),
            resume_block: None,
            state: Versioned::new(FeedInner::default()),
        }
    }

    /// Mark the store as resumed: `block` is the first block the extractor
    /// will see. A subscriber whose cursor is below a lane's first
    /// post-resume seq gets `Lagged` with this block as the floor.
    #[must_use]
    pub fn with_resume_block(mut self, block: Option<u64>) -> Self {
        self.resume_block = block;
        self
    }

    /// The chain this store serves for (stamped on every wire item).
    pub fn origin_chain_id(&self) -> u64 {
        self.origin_chain_id
    }

    /// Record one block's extracted messages (possibly none — every block
    /// advances the retention head) and prune lanes that aged out.
    pub fn append_block(&self, block: u64, msgs: Vec<OutboxMessage>) {
        let retention_blocks = self.retention_blocks.get();
        let resumed = self.resume_block.is_some();
        self.state.mutate(|g| {
            g.head_block = g.head_block.max(block);
            for m in msgs {
                let lane = g
                    .lanes
                    .entry(m.dest_chain_id)
                    .or_insert_with(|| Lane::starting(resumed));
                lane.learn_floor(m.seq);
                lane.msgs.push_back(m);
            }
            let cutoff = g.head_block.saturating_sub(retention_blocks);
            for lane in g.lanes.values_mut() {
                lane.prune_below(cutoff);
            }
        });
    }

    /// Everything retained for `dest` from `from_seq` onward, plus the
    /// lane's floors and the store head. The scan starts at the cursor's
    /// index (a binary search on the dense seq), not at the lane's front.
    pub fn from_seq(&self, dest: u64, from_seq: u64) -> LaneScan {
        let retention_blocks = self.retention_blocks.get();
        self.state.read(|g| {
            let cutoff = g.head_block.saturating_sub(retention_blocks);
            let floor_block = cutoff.max(self.resume_block.unwrap_or(0));
            match g.lanes.get(&dest) {
                Some(lane) => {
                    let idx = lane.msgs.partition_point(|m| m.seq < from_seq);
                    LaneScan {
                        msgs: lane.msgs.range(idx..).cloned().collect(),
                        floor_seq: lane.floor,
                        floor_block,
                        head_block: g.head_block,
                    }
                }
                None => LaneScan {
                    msgs: Vec::new(),
                    // No message for this lane since the start.
                    floor_seq: LaneFloor::at_start(self.resume_block.is_some()),
                    floor_block,
                    head_block: g.head_block,
                },
            }
        })
    }

    /// Wake-up channel for subscription handlers (tap BEFORE the first scan).
    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.state.subscribe()
    }
}

/// One attested block: this validator's committed state root at
/// `block_number`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Attestation {
    pub block_number: u64,
    pub state_root: B256,
}

/// One cursor scan of the attestation ring: what
/// [`AttestationStore::from_block`] returns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttestationScan {
    /// Retained attestations at or after the cursor, in block order.
    pub items: Vec<Attestation>,
    /// First retained block (the retention floor; 0 when nothing is
    /// retained yet).
    pub floor: u64,
}

/// Ring of this validator's per-block `(block_number, state_root)`
/// attestations, block-retained like the feed store.
pub struct AttestationStore {
    retention_blocks: NonZeroU64,
    state: Versioned<VecDeque<Attestation>>,
}

impl AttestationStore {
    #[must_use]
    pub fn new(retention_blocks: RetentionBlocks) -> Self {
        Self {
            retention_blocks: retention_blocks.get(),
            state: Versioned::new(VecDeque::new()),
        }
    }

    /// Record `block`'s committed state root (call once per block, in order).
    pub fn push(&self, block: u64, state_root: B256) {
        let retention_blocks = self.retention_blocks.get();
        self.state.mutate(|g| {
            g.push_back(Attestation {
                block_number: block,
                state_root,
            });
            let cutoff = block.saturating_sub(retention_blocks);
            while g.front().is_some_and(|a| a.block_number < cutoff) {
                g.pop_front();
            }
        });
    }

    /// Retained attestations from `from_block` onward, plus the retention
    /// floor.
    pub fn from_block(&self, from_block: u64) -> AttestationScan {
        self.state.read(|g| AttestationScan {
            items: g
                .iter()
                .filter(|a| a.block_number >= from_block)
                .copied()
                .collect(),
            floor: g.front().map_or(0, |a| a.block_number),
        })
    }

    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.state.subscribe()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nz(n: u64) -> RetentionBlocks {
        RetentionBlocks(NonZeroU64::new(n).expect("fixture retention"))
    }

    use crate::interop::outbox_msg as msg;

    #[test]
    fn lanes_are_per_destination_and_cursor_scans_by_seq() {
        let store = FeedStore::new(1, nz(100));
        store.append_block(10, vec![msg(7, 0, 10), msg(9, 0, 10)]);
        store.append_block(11, vec![msg(7, 1, 11)]);

        let scan = store.from_seq(7, 0);
        assert_eq!(
            scan.msgs.iter().map(|m| m.seq).collect::<Vec<_>>(),
            vec![0, 1]
        );
        assert_eq!(scan.floor_seq, LaneFloor::Known(0));
        assert_eq!(scan.head_block, 11);
        let scan = store.from_seq(7, 1);
        assert_eq!(scan.msgs.iter().map(|m| m.seq).collect::<Vec<_>>(), vec![1]);
        // A cursor past the tail scans nothing.
        assert!(store.from_seq(7, 5).msgs.is_empty());
        let scan = store.from_seq(9, 0);
        assert_eq!(scan.msgs.len(), 1);
        // Unknown destination on a genesis-start store: empty, floor 0.
        let scan = store.from_seq(999, 0);
        assert!(scan.msgs.is_empty());
        assert_eq!(scan.floor_seq, LaneFloor::Known(0));
    }

    #[test]
    fn retention_prunes_by_block_and_raises_the_floor() {
        let store = FeedStore::new(1, nz(5));
        store.append_block(10, vec![msg(7, 0, 10), msg(7, 1, 10)]);
        store.append_block(12, vec![msg(7, 2, 12)]);
        // Head advances well past block 10: seqs 0-1 age out.
        store.append_block(16, vec![]);
        let scan = store.from_seq(7, 0);
        assert_eq!(scan.msgs.iter().map(|m| m.seq).collect::<Vec<_>>(), vec![2]);
        assert_eq!(
            scan.floor_seq,
            LaneFloor::Known(2),
            "floor names the first retained seq"
        );
        assert_eq!(scan.floor_block, 11, "cutoff = 16 - 5");
        // A cursor below the floor (here, 0) is the Lagged case; the
        // caller computes skipped = floor - cursor.
    }

    #[test]
    fn empty_blocks_advance_retention_without_messages() {
        let store = FeedStore::new(1, nz(2));
        store.append_block(1, vec![msg(7, 0, 1)]);
        for b in 2..=10 {
            store.append_block(b, vec![]);
        }
        let scan = store.from_seq(7, 0);
        assert!(scan.msgs.is_empty());
        assert_eq!(scan.floor_seq, LaneFloor::Known(1));
        assert_eq!(scan.head_block, 10);
    }

    /// After a restart the store did not see the earlier blocks. The
    /// first message names the lane floor; a cursor below it is lagged,
    /// with the resume block as the block floor.
    #[test]
    fn a_resumed_store_learns_the_floor_from_the_first_message() {
        let store = FeedStore::new(1, nz(100)).with_resume_block(Some(50));
        // Before any message the floor is unknown.
        let scan = store.from_seq(7, 0);
        assert_eq!(scan.floor_seq, LaneFloor::UnknownAfterResume);
        assert_eq!(scan.floor_block, 50);
        store.append_block(50, vec![]);
        assert_eq!(
            store.from_seq(7, 0).floor_seq,
            LaneFloor::UnknownAfterResume
        );

        store.append_block(51, vec![msg(7, 5, 51)]);
        let scan = store.from_seq(7, 0);
        assert_eq!(
            scan.floor_seq,
            LaneFloor::Known(5),
            "the first seq served is the floor"
        );
        assert_eq!(scan.floor_block, 50);
        assert_eq!(scan.msgs.len(), 1);
        // A cursor at the floor is not lagged.
        assert_eq!(store.from_seq(7, 5).msgs.len(), 1);
    }

    /// The contrast to the resume case: a genesis start saw every block, so
    /// a first message at seq 5 is an origin fault. The floor stays 0 and
    /// the client's derivation rule reports the skip.
    #[test]
    fn a_genesis_store_keeps_floor_zero_for_a_skipping_lane() {
        let store = FeedStore::new(1, nz(100));
        store.append_block(1, vec![msg(7, 5, 1)]);
        let scan = store.from_seq(7, 0);
        assert_eq!(scan.floor_seq, LaneFloor::Known(0));
        assert_eq!(scan.floor_block, 0);
        assert_eq!(scan.msgs[0].seq, 5);
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
            ring.push(b, B256::repeat_byte(crate::interop::fixture_block_byte(b)));
        }
        let scan = ring.from_block(0);
        assert_eq!(scan.floor, 7, "cutoff = 10 - 3");
        assert_eq!(scan.items.first().unwrap().block_number, 7);
        assert_eq!(scan.items.last().unwrap().block_number, 10);
        assert_eq!(ring.from_block(9).items.len(), 2);
    }
}
