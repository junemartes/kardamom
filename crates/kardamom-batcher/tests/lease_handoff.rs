//! Three mock batchers: only the leader posts; failover bounded.

use alloy_primitives::{Address, B256};
use bytes::Bytes;
use kardamom_batcher::batch::{ClosedBlock, RecordedTx};
use kardamom_batcher::batcher::{Batcher, BatcherConfig, MockSender};
use kardamom_leases::{Lease, LeaseConfig};
use kardamom_types::{BPosition, FsyncWatermark, QuorumWatermark, TxEnvelope};

fn pos(o: i32) -> BPosition {
    BPosition {
        term_id: 0,
        term_offset: o,
    }
}

fn closed(n: u64) -> ClosedBlock {
    ClosedBlock {
        block_number: n,
        l2_timestamp: 1_700_000_000 + n,
        end_tx_idx: pos(0),
        txs: vec![RecordedTx {
            position: pos(0),
            envelope: TxEnvelope {
                correlation_id: 1,
                raw_tx: Bytes::from_static(b"raw"),
                sender: Address::repeat_byte(0xAB),
                tx_hash: B256::repeat_byte(0xCD),
            },
        }],
    }
}

fn lease_for(self_id: u8, all_ids: &[u8], caught_up: &[u8]) -> Lease {
    let cfg = LeaseConfig {
        self_id,
        all_ids: all_ids.to_vec(),
        caught_up_window: 1024 * 1024,
    };
    let mut l = Lease::new(cfg);
    let q = QuorumWatermark { position: pos(0) };
    l.observe_quorum(q);
    for id in caught_up {
        l.observe_fsync(FsyncWatermark {
            recorder_id: *id,
            position: pos(0),
        });
    }
    l
}

#[test]
fn only_leader_posts() {
    let all_ids = vec![0u8, 1u8, 2u8];

    let mut leader = Batcher::new(BatcherConfig::default(), MockSender::default());
    let mut standby1 = Batcher::new(BatcherConfig::default(), MockSender::default());
    let mut standby2 = Batcher::new(BatcherConfig::default(), MockSender::default());

    let lease0 = lease_for(0, &all_ids, &all_ids);
    let lease1 = lease_for(1, &all_ids, &all_ids);
    let lease2 = lease_for(2, &all_ids, &all_ids);

    assert!(lease0.held_by_us(), "lowest id holds the lease");
    assert!(!lease1.held_by_us());
    assert!(!lease2.held_by_us());

    leader.on_closed_block(closed(1), &lease0).unwrap();
    standby1.on_closed_block(closed(1), &lease1).unwrap();
    standby2.on_closed_block(closed(1), &lease2).unwrap();

    assert_eq!(leader.sender().sent.len(), 1);
    assert!(standby1.sender().sent.is_empty());
    assert!(standby2.sender().sent.is_empty());
}

#[test]
fn standby_takes_over_when_leader_falls_behind() {
    // Initially id=0 caught up. Then id=0 disappears (no longer caught up) and
    // id=1 should hold the lease — only id=1's batcher should post.
    let all_ids = vec![0u8, 1u8, 2u8];
    let lease1_initial = lease_for(1, &all_ids, &all_ids);
    assert!(!lease1_initial.held_by_us());

    let lease1_after_failover = lease_for(1, &all_ids, &[1, 2]);
    assert!(
        lease1_after_failover.held_by_us(),
        "id=1 takes over when id=0 falls out of caught-up set"
    );

    let mut batcher = Batcher::new(BatcherConfig::default(), MockSender::default());
    batcher
        .on_closed_block(closed(7), &lease1_after_failover)
        .unwrap();
    assert_eq!(batcher.sender().sent.len(), 1);
    assert_eq!(batcher.sender().sent[0].l2_block_start, 7);
}
