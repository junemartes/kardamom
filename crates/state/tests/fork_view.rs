//! `StateDatabase::fork_view` on the mdbx snapshot. A fork is an
//! independent read-only transaction at the same MVCC anchor. The
//! anchor check refuses a fork once the writer has advanced past the
//! source snapshot.

mod common;

use alloy_primitives::{U256, address};
use kardamom_types::StateDatabase;

#[test]
fn fork_reads_the_same_anchor_and_staleness_refuses() {
    let (_dir, writer) = common::open_tmp_writer();
    let addr = address!("0x00000000000000000000000000000000000000bb");

    // Drop the genesis snapshot.
    let _ = writer.snapshot_rx.recv();

    // Commit block 1 and take its snapshot.
    writer
        .delta_tx
        .send(common::simple_delta(1, addr, 100, 7, 999))
        .unwrap();
    let snap_at_1 = writer.snapshot_rx.recv().unwrap();

    // Fork while the writer is at block 1: anchors match, values match.
    let fork = snap_at_1
        .fork_view()
        .expect("fork at a quiescent anchor must succeed");
    let (_, bal, _) = fork.basic(addr).unwrap().unwrap();
    assert_eq!(bal, U256::from(100u64));
    assert_eq!(
        fork.storage(addr, common::slot_key(7)).unwrap(),
        U256::from(999u64)
    );

    // Advance the writer to block 2.
    writer
        .delta_tx
        .send(common::simple_delta(2, addr, 200, 7, 12345))
        .unwrap();
    let snap_at_2 = writer.snapshot_rx.recv().unwrap();

    // The fork from before the advance still reads block-1 state,
    // because it owns its own transaction.
    assert_eq!(
        fork.storage(addr, common::slot_key(7)).unwrap(),
        U256::from(999u64),
        "a fork minted at block 1 must keep the block-1 view"
    );

    // A fork from the stale snapshot must refuse. A fresh transaction
    // would anchor at block 2, which is not the state snap_at_1 represents.
    assert!(
        snap_at_1.fork_view().is_none(),
        "fork from a stale snapshot must return None"
    );

    // The current snapshot forks fine and sees block-2 state.
    let fork2 = snap_at_2.fork_view().expect("fork at the head anchor");
    assert_eq!(
        fork2.storage(addr, common::slot_key(7)).unwrap(),
        U256::from(12345u64)
    );
}
