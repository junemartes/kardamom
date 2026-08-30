//! Apply a synthetic stream of block deltas and assert the post-replay state
//! matches the expected per-key values.

mod common;

use alloy_primitives::{U256, address};
use kardamom_state::StateSnapshot;
use kardamom_types::StateDatabase;

#[test]
fn writer_applies_deltas_and_state_reflects_them() {
    let (_dir, mut writer) = common::open_tmp_writer();
    let addr = address!("0x00000000000000000000000000000000000000aa");

    // Drain the spawn-time snapshot first so the per-block snapshots line up.
    let _genesis = writer.snapshot_rx.recv().unwrap();

    for block in 1..=5u64 {
        writer
            .delta_tx
            .send(common::simple_delta(
                block,
                addr,
                1000 + block,
                7,
                block * 100,
            ))
            .unwrap();
    }

    // Wait for each of the 5 post-commit snapshots.
    let mut latest: Option<StateSnapshot> = None;
    for _ in 0..5 {
        latest = writer.snapshot_rx.recv();
    }
    let snap = latest.expect("at least one snapshot");
    assert_eq!(snap.block_number(), 5);

    let (nonce, balance, _code_hash) = snap.basic(addr).unwrap().expect("account exists");
    assert_eq!(balance, U256::from(1005u64));
    assert_eq!(nonce, 5);

    let slot = snap.storage(addr, common::slot_key(7)).unwrap();
    assert_eq!(slot, U256::from(500u64));

    writer.shutdown().unwrap();
}
