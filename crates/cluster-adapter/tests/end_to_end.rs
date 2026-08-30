//! End-to-end test through the Rust adapter chain:
//!
//! `ClusterRefPublisher`, then an in-Rust service mock that mirrors the Java
//! `CanonicalSealerState` (dedup, canonical_count, boundary), then
//! `ClusterTxOrderingSubscription`.
//!
//! This checks the whole Rust path against the documented wire contract and
//! the Java service's deterministic logic (unit-tested separately in
//! `cluster/sealer-service`). The test is fully in memory and deterministic:
//! no Aeron, no cluster, no threads.

use std::collections::{HashSet, VecDeque};

use alloy_primitives::{Address, B256};
use kardamom_cluster_adapter::gateway::fakes::{FakeEgress, FakeIngress};
use kardamom_cluster_adapter::wire::{encode_egress_boundary, encode_egress_record, split_ingress};
use kardamom_engine::reader::TxOrderingSubscription;
use kardamom_engine::reader::cluster::ClusterTxOrderingSubscription;
use kardamom_sequencer::outbound::TxOrderingRefPublisher;
use kardamom_sequencer::outbound::cluster::ClusterRefPublisher;
use kardamom_types::{BPosition, TxOrderingMessage, TxRef};

/// Mirrors the Java `CanonicalSealerState`. It does FIFO first-seen dedup,
/// keeps a 0-based canonical index, and floors the boundary timer to 250
/// ms.
struct MockService {
    seen: HashSet<[u8; 32]>,
    fifo: VecDeque<[u8; 32]>,
    capacity: usize,
    count: u64,
    block: u64,
}

impl MockService {
    fn new(capacity: usize) -> Self {
        Self {
            seen: HashSet::new(),
            fifo: VecDeque::new(),
            capacity,
            count: 0,
            block: 1,
        }
    }

    fn first_seen(&mut self, id: [u8; 32]) -> bool {
        if !self.seen.insert(id) {
            return false;
        }
        self.fifo.push_back(id);
        if self.fifo.len() > self.capacity
            && let Some(evicted) = self.fifo.pop_front()
        {
            self.seen.remove(&evicted);
        }
        true
    }

    /// Process one ingress frame. Emit a relayed egress frame only if the
    /// frame is first-seen.
    fn on_ingress(&mut self, ingress: &[u8]) -> Option<Vec<u8>> {
        let (cid, relayed) = split_ingress(ingress).unwrap();
        if !self.first_seen(cid) {
            return None;
        }
        let idx = self.count;
        self.count += 1;
        Some(encode_egress_record(idx, relayed))
    }

    /// Stamp a boundary at `clock_ms`, and advance the block number.
    fn tick(&mut self, clock_ms: u64) -> Vec<u8> {
        let frame = encode_egress_boundary(self.block, self.count, (clock_ms / 250) * 250, 0);
        self.block += 1;
        frame
    }
}

fn txref(tag: u8) -> TxRef {
    TxRef::new(
        B256::repeat_byte(tag),
        1,
        BPosition {
            term_id: 0,
            term_offset: tag as i32,
        },
        0,
    )
}

#[test]
fn dedup_order_and_boundary_alignment_end_to_end() {
    // Publish A, B, A (duplicate), C through the ref publisher.
    let ingress = FakeIngress::new();
    let mut publisher = ClusterRefPublisher::new(ingress.clone());
    let (a, b, c) = (txref(0xA1), txref(0xB2), txref(0xC3));
    let sender = Address::repeat_byte(0x5A);
    publisher.try_publish_ref(&a, sender, 0).unwrap();
    publisher.try_publish_ref(&b, sender, 1).unwrap();
    publisher.try_publish_ref(&a, sender, 0).unwrap(); // duplicate (republish)
    publisher.try_publish_ref(&c, sender, 2).unwrap();

    // Run the service mock over the ingress, then stamp a boundary.
    let mut service = MockService::new(1024);
    let egress = FakeEgress::new();
    for msg in ingress.accepted() {
        if let Some(frame) = service.on_ingress(&msg) {
            egress.push(frame);
        }
    }
    egress.push(service.tick(1_300)); // boundary
    egress.close();

    // Read the canonical stream through the executor's subscription seam.
    let mut sub = ClusterTxOrderingSubscription::new(egress);
    let mut records = Vec::new();
    let mut boundary = None;
    loop {
        match sub.next() {
            Ok((pos, TxOrderingMessage::TxRef(r))) => records.push((pos, r)),
            Ok((_pos, TxOrderingMessage::BoundaryStart(bnd))) => boundary = Some(bnd),
            Ok((_pos, other)) => panic!("unexpected message {other:?}"),
            Err(_) => break, // TxOrderingClosed means a clean EOF
        }
    }

    // The duplicate A collapsed. There are exactly 3 unique records, in
    // order, with monotonic 0-based canonical positions.
    assert_eq!(records.len(), 3, "duplicate must be deduped");
    assert_eq!(records[0].1, a);
    assert_eq!(records[1].1, b);
    assert_eq!(records[2].1, c);
    assert_eq!(records[0].0, BPosition::from_index(0));
    assert_eq!(records[1].0, BPosition::from_index(1));
    assert_eq!(records[2].0, BPosition::from_index(2));

    // The boundary's end_tx_idx equals the deduped record count (3). The l2
    // timestamp is floored to the 250 ms tick. The block number is genesis
    // (1).
    let bnd = boundary.expect("a boundary was emitted");
    assert_eq!(bnd.block_number, 1);
    assert_eq!(bnd.end_tx_idx.as_index(), 3);
    assert_eq!(bnd.l2_timestamp, 1_250);
}

#[test]
fn closed_stream_surfaces_as_clean_eof() {
    let egress = FakeEgress::new();
    egress.close();
    let mut sub = ClusterTxOrderingSubscription::new(egress);
    assert!(matches!(
        sub.next(),
        Err(kardamom_engine::ExecutorError::TxOrderingClosed)
    ));
}
