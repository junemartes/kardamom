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
use std::sync::Mutex;

use alloy_primitives::B256;
use kardamom_types::xchain::OutboxMessage;
use tokio::sync::watch;

/// One destination's retained messages, in seq order (appends arrive in
/// block order and the Outbox's per-destination counter is dense, so pushes
/// are naturally ordered).
#[derive(Debug, Default)]
struct Lane {
    msgs: VecDeque<OutboxMessage>,
    /// Seq of the first message this store can serve. Everything below it
    /// is gone: it aged out of retention, or the validator resumed after
    /// it. `None` after a resume until the lane's first message names it;
    /// a subscriber below a `Some` floor is LAGGED.
    floor: Option<u64>,
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
    /// First seq the store can serve for this lane, when known.
    /// `Some(floor)` with `floor > cursor` means the subscriber lagged out
    /// and `floor - cursor` items are gone. `None` means the lane has no
    /// message since the resume, so its floor is not yet known.
    pub floor_seq: Option<u64>,
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
    retention_blocks: u64,
    /// First block the extractor sees after a restart. Messages from
    /// earlier blocks never reach this store, so no lane floor is known
    /// until the lane's first message arrives. `None` for a genesis start.
    resume_block: Option<u64>,
    inner: Mutex<FeedInner>,
    /// Version bump per append; subscription handlers wait on it.
    items: watch::Sender<u64>,
}

impl FeedStore {
    pub fn new(origin_chain_id: u64, retention_blocks: u64) -> Self {
        Self {
            origin_chain_id,
            retention_blocks: retention_blocks.max(1),
            resume_block: None,
            inner: Mutex::new(FeedInner::default()),
            items: watch::channel(0).0,
        }
    }

    /// Mark the store as resumed: `block` is the first block the extractor
    /// will see. A subscriber whose cursor is below a lane's first
    /// post-resume seq gets `Lagged` with this block as the floor.
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
        let mut g = self.inner.lock().unwrap();
        g.head_block = g.head_block.max(block);
        let resumed = self.resume_block.is_some();
        for m in msgs {
            let lane = g.lanes.entry(m.dest_chain_id).or_insert_with(|| Lane {
                msgs: VecDeque::new(),
                // A genesis start saw every block: the floor is seq 0. A
                // resumed store learns the floor from the first message.
                floor: (!resumed).then_some(0),
            });
            if lane.floor.is_none() {
                lane.floor = Some(m.seq);
            }
            lane.msgs.push_back(m);
        }
        let cutoff = g.head_block.saturating_sub(self.retention_blocks);
        for lane in g.lanes.values_mut() {
            while let Some(front) = lane.msgs.front() {
                if front.origin_block_number >= cutoff {
                    break;
                }
                lane.floor = Some(front.seq + 1);
                lane.msgs.pop_front();
            }
        }
        drop(g);
        self.items.send_modify(|v| *v += 1);
    }

    /// Everything retained for `dest` from `from_seq` onward, plus the
    /// lane's floors and the store head. The scan starts at the cursor's
    /// index (a binary search on the dense seq), not at the lane's front.
    pub fn from_seq(&self, dest: u64, from_seq: u64) -> LaneScan {
        let g = self.inner.lock().unwrap();
        let cutoff = g.head_block.saturating_sub(self.retention_blocks);
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
                // No message for this lane since the start: a genesis start
                // knows the floor is 0, a resumed store does not know it.
                floor_seq: self.resume_block.is_none().then_some(0),
                floor_block,
                head_block: g.head_block,
            },
        }
    }

    /// Wake-up channel for subscription handlers (tap BEFORE the first scan).
    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.items.subscribe()
    }
}

/// Ring of this validator's per-block `(block_number, state_root)`
/// attestations, block-retained like the feed store.
pub struct AttestationStore {
    retention_blocks: u64,
    inner: Mutex<VecDeque<(u64, B256)>>,
    items: watch::Sender<u64>,
}

impl AttestationStore {
    pub fn new(retention_blocks: u64) -> Self {
        Self {
            retention_blocks: retention_blocks.max(1),
            inner: Mutex::new(VecDeque::new()),
            items: watch::channel(0).0,
        }
    }

    /// Record `block`'s committed state root (call once per block, in order).
    pub fn push(&self, block: u64, state_root: B256) {
        let mut g = self.inner.lock().unwrap();
        g.push_back((block, state_root));
        let cutoff = block.saturating_sub(self.retention_blocks);
        while g.front().is_some_and(|(b, _)| *b < cutoff) {
            g.pop_front();
        }
        drop(g);
        self.items.send_modify(|v| *v += 1);
    }

    /// Retained attestations from `from_block` onward, plus the first
    /// retained block (the retention floor; 0 when nothing is retained yet).
    pub fn from_block(&self, from_block: u64) -> (Vec<(u64, B256)>, u64) {
        let g = self.inner.lock().unwrap();
        let floor = g.front().map(|(b, _)| *b).unwrap_or(0);
        (
            g.iter()
                .filter(|(b, _)| *b >= from_block)
                .copied()
                .collect(),
            floor,
        )
    }

    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.items.subscribe()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::Address;

    fn msg(dest: u64, seq: u64, block: u64) -> OutboxMessage {
        OutboxMessage {
            origin_block_number: block,
            origin_block_hash: B256::repeat_byte(block as u8),
            dest_chain_id: dest,
            seq,
            sender: Address::repeat_byte(0xA1),
            target: Address::repeat_byte(0xB2),
            value: 0,
            gas_limit: 100_000,
            data: Default::default(),
            callback: None,
        }
    }

    #[test]
    fn lanes_are_per_destination_and_cursor_scans_by_seq() {
        let store = FeedStore::new(1, 100);
        store.append_block(10, vec![msg(7, 0, 10), msg(9, 0, 10)]);
        store.append_block(11, vec![msg(7, 1, 11)]);

        let scan = store.from_seq(7, 0);
        assert_eq!(
            scan.msgs.iter().map(|m| m.seq).collect::<Vec<_>>(),
            vec![0, 1]
        );
        assert_eq!(scan.floor_seq, Some(0));
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
        assert_eq!(scan.floor_seq, Some(0));
    }

    #[test]
    fn retention_prunes_by_block_and_raises_the_floor() {
        let store = FeedStore::new(1, 5);
        store.append_block(10, vec![msg(7, 0, 10), msg(7, 1, 10)]);
        store.append_block(12, vec![msg(7, 2, 12)]);
        // Head advances well past block 10: seqs 0-1 age out.
        store.append_block(16, vec![]);
        let scan = store.from_seq(7, 0);
        assert_eq!(scan.msgs.iter().map(|m| m.seq).collect::<Vec<_>>(), vec![2]);
        assert_eq!(
            scan.floor_seq,
            Some(2),
            "floor names the first retained seq"
        );
        assert_eq!(scan.floor_block, 11, "cutoff = 16 - 5");
        // A cursor below the floor is the Lagged case; the caller computes
        // skipped = floor - cursor.
        assert!(scan.floor_seq.unwrap() > 0);
    }

    #[test]
    fn empty_blocks_advance_retention_without_messages() {
        let store = FeedStore::new(1, 2);
        store.append_block(1, vec![msg(7, 0, 1)]);
        for b in 2..=10 {
            store.append_block(b, vec![]);
        }
        let scan = store.from_seq(7, 0);
        assert!(scan.msgs.is_empty());
        assert_eq!(scan.floor_seq, Some(1));
        assert_eq!(scan.head_block, 10);
    }

    /// H8: after a restart the store did not see the earlier blocks. The
    /// first message names the lane floor; a cursor below it is lagged,
    /// with the resume block as the block floor.
    #[test]
    fn a_resumed_store_learns_the_floor_from_the_first_message() {
        let store = FeedStore::new(1, 100).with_resume_block(Some(50));
        // Before any message the floor is unknown.
        let scan = store.from_seq(7, 0);
        assert_eq!(scan.floor_seq, None);
        assert_eq!(scan.floor_block, 50);
        store.append_block(50, vec![]);
        assert_eq!(store.from_seq(7, 0).floor_seq, None);

        store.append_block(51, vec![msg(7, 5, 51)]);
        let scan = store.from_seq(7, 0);
        assert_eq!(scan.floor_seq, Some(5), "the first seq served is the floor");
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
        let store = FeedStore::new(1, 100);
        store.append_block(1, vec![msg(7, 5, 1)]);
        let scan = store.from_seq(7, 0);
        assert_eq!(scan.floor_seq, Some(0));
        assert_eq!(scan.floor_block, 0);
        assert_eq!(scan.msgs[0].seq, 5);
    }

    #[test]
    fn append_wakes_subscribers() {
        let store = FeedStore::new(1, 100);
        let mut rx = store.subscribe();
        let before = *rx.borrow_and_update();
        store.append_block(1, vec![]);
        assert!(rx.has_changed().unwrap());
        assert!(*rx.borrow_and_update() > before);
    }

    #[test]
    fn attestation_ring_retains_and_floors() {
        let ring = AttestationStore::new(3);
        for b in 1..=10u64 {
            ring.push(b, B256::repeat_byte(b as u8));
        }
        let (all, floor) = ring.from_block(0);
        assert_eq!(floor, 7, "cutoff = 10 - 3");
        assert_eq!(all.first().unwrap().0, 7);
        assert_eq!(all.last().unwrap().0, 10);
        let (tail, _) = ring.from_block(9);
        assert_eq!(tail.len(), 2);
    }
}
