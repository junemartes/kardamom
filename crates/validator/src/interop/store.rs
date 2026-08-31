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
    /// Seq of the first retained message — everything below aged out of
    /// retention. A subscriber whose cursor is below this floor is LAGGED.
    floor: u64,
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
    retention_blocks: u64,
    inner: Mutex<FeedInner>,
    /// Version bump per append; subscription handlers wait on it.
    items: watch::Sender<u64>,
}

impl FeedStore {
    pub fn new(origin_chain_id: u64, retention_blocks: u64) -> Self {
        Self {
            origin_chain_id,
            retention_blocks: retention_blocks.max(1),
            inner: Mutex::new(FeedInner::default()),
            items: watch::channel(0).0,
        }
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
        for m in msgs {
            let lane = g.lanes.entry(m.dest_chain_id).or_default();
            lane.msgs.push_back(m);
        }
        let cutoff = g.head_block.saturating_sub(self.retention_blocks);
        for lane in g.lanes.values_mut() {
            while let Some(front) = lane.msgs.front() {
                if front.origin_block_number >= cutoff {
                    break;
                }
                lane.floor = front.seq + 1;
                lane.msgs.pop_front();
            }
        }
        drop(g);
        self.items.send_modify(|v| *v += 1);
    }

    /// Everything retained for `dest` from `from_seq` onward, plus the
    /// lane's retention floor. `floor > from_seq` ⇒ the subscriber lagged
    /// out of retention and `floor - from_seq` items are gone.
    pub fn from_seq(&self, dest: u64, from_seq: u64) -> (Vec<OutboxMessage>, u64) {
        let g = self.inner.lock().unwrap();
        match g.lanes.get(&dest) {
            Some(lane) => (
                lane.msgs
                    .iter()
                    .filter(|m| m.seq >= from_seq)
                    .cloned()
                    .collect(),
                lane.floor,
            ),
            None => (Vec::new(), 0),
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
        let store = FeedStore::new(1, 5);
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
        let store = FeedStore::new(1, 2);
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
