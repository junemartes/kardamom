//! Snapshot-swap protocol: each commit publishes exactly one new snapshot
//! that exposes the post-commit view.

mod common;

use alloy_primitives::address;
use types::StateDatabase;

#[test]
fn each_commit_publishes_a_post_commit_snapshot() {
    let (_dir, writer) = common::open_tmp_writer();
    let addr = address!("0x00000000000000000000000000000000000000aa");

    // Initial snapshot at block 0 published on spawn.
    let snap0 = writer.snapshot_rx.recv().unwrap();
    assert_eq!(snap0.block_number(), 0);
    assert!(snap0.basic(addr).unwrap().is_none());

    // Apply block 1.
    writer
        .delta_tx
        .send(common::simple_delta(1, addr, 100, 0, 0))
        .unwrap();
    let snap1 = writer.snapshot_rx.recv().unwrap();
    assert_eq!(snap1.block_number(), 1);
    assert!(snap1.basic(addr).unwrap().is_some());

    // Apply block 2.
    writer
        .delta_tx
        .send(common::simple_delta(2, addr, 200, 0, 0))
        .unwrap();
    let snap2 = writer.snapshot_rx.recv().unwrap();
    assert_eq!(snap2.block_number(), 2);

    writer.shutdown().unwrap();
}

#[test]
fn tx_hash_lookup_round_trips_through_writer() {
    //: writer populates tx_hash_index on every commit; snapshots can
    // resolve tx_hash → BPosition → Receipt.
    let (_dir, writer) = common::open_tmp_writer();
    let addr = address!("0x00000000000000000000000000000000000000bb");
    let _ = writer.snapshot_rx.recv();

    writer
        .delta_tx
        .send(common::simple_delta(7, addr, 700, 1, 1))
        .unwrap();
    let snap = writer.snapshot_rx.recv().unwrap();

    // Find the receipt for block 7 by reading the BlockBoundary's end_tx_idx.
    let pos = common::bpos(7);
    let r = snap
        .get_receipt(pos)
        .unwrap()
        .expect("receipt at block 7's end_tx_idx");
    assert_eq!(r.gas_used, 21_000);

    // Cross-resolve by tx_hash.
    let tx_hash = r.tx_hash;
    let from_index = snap.get_tx_position(tx_hash).unwrap().unwrap();
    assert_eq!(from_index, pos);
    assert_eq!(snap.get_receipt(from_index).unwrap().unwrap(), r);

    writer.shutdown().unwrap();
}
