//! Graceful recovery. Shutdown drains queued writes before the environment reopens.
//! The separate `writer_crash` target kills a process at real transaction boundaries.

mod common;

use alloy_primitives::{U256, address};
use kardamom_state::{StateSnapshot, StateWriter, read_recovery_point};
use kardamom_types::StateDatabase;

#[test]
fn recovery_point_matches_last_committed_block() {
    let dir = tempfile::tempdir().unwrap();
    let addr = address!("0x00000000000000000000000000000000000000aa");

    // Commit the first three blocks, then drain the fourth during shutdown.
    {
        let env = common::open_env(dir.path());
        let mut writer = StateWriter::spawn(env).unwrap();
        // Drain the genesis snapshot.
        let _ = writer.snapshot_rx.recv();
        for block in 1..=3u64 {
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
        // Wait for block 3, so the three commits landed. Three commits
        // can arrive as fewer than three wakes.
        common::wait_for_block(&writer, 3);
        writer
            .delta_tx
            .send(common::simple_delta(4, addr, 9999, 7, 9999))
            .unwrap();
        writer.shutdown().unwrap();
    }

    let env = common::open_env(dir.path());

    let rp = read_recovery_point(&env).unwrap();
    assert_eq!(rp.last_committed_block, 4);

    let snap = StateSnapshot::open(&env).unwrap();
    let (_, balance, _) = snap.basic(addr).unwrap().unwrap();
    assert_eq!(balance, U256::from(9999u64));
    assert_eq!(
        snap.storage(addr, common::slot_key(7)).unwrap(),
        U256::from(9999u64)
    );
}
