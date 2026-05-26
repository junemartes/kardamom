//! MVCC invariant: a snapshot opened before write N still reads pre-N values
//! after the writer has committed N.

mod common;

use alloy_primitives::{U256, address};
use kardamom_types::StateDatabase;

#[test]
fn pre_n_snapshot_keeps_pre_n_view() {
    let (_dir, writer) = common::open_tmp_writer();
    let addr = address!("0x00000000000000000000000000000000000000aa");

    // Drop the genesis snapshot.
    let _ = writer.snapshot_rx.recv();

    // Apply block 1. simple_delta stores `balance` literally (no offset).
    writer
        .delta_tx
        .send(common::simple_delta(1, addr, 100, 7, 999))
        .unwrap();
    let snap_at_1 = writer.snapshot_rx.recv().unwrap();
    let (_, bal1, _) = snap_at_1.basic(addr).unwrap().unwrap();
    assert_eq!(bal1, U256::from(100u64));
    assert_eq!(
        snap_at_1.storage(addr, common::slot_key(7)).unwrap(),
        U256::from(999u64)
    );

    // Apply block 2 — overwrites the slot.
    writer
        .delta_tx
        .send(common::simple_delta(2, addr, 200, 7, 12345))
        .unwrap();
    let snap_at_2 = writer.snapshot_rx.recv().unwrap();

    // The OLD snapshot must still see the OLD values.
    assert_eq!(
        snap_at_1.storage(addr, common::slot_key(7)).unwrap(),
        U256::from(999u64),
        "pre-N snapshot must still see pre-N storage value"
    );
    let (_, old_bal, _) = snap_at_1.basic(addr).unwrap().unwrap();
    assert_eq!(
        old_bal,
        U256::from(100u64),
        "pre-N snapshot must still see pre-N account balance"
    );

    // The NEW snapshot sees the NEW values.
    assert_eq!(
        snap_at_2.storage(addr, common::slot_key(7)).unwrap(),
        U256::from(12345u64)
    );
    let (_, new_bal, _) = snap_at_2.basic(addr).unwrap().unwrap();
    assert_eq!(new_bal, U256::from(200u64));

    writer.shutdown().unwrap();
}
