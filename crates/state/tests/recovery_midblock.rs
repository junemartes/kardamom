//! Recovery: drop the writer after some commits; re-open the env; assert the
//! recovery point matches the last committed block and that the post-recovery
//! snapshot still serves the right values.
//!
//! Note: a true "mid-block crash" is hard to provoke from Rust since
//! `mdbx_txn_commit` is atomic. The realistic invariant we test: any
//! uncommitted txn is discarded; whichever block is last committed, the
//! snapshot is internally consistent for that block.

mod common;

use alloy_primitives::{U256, address};
use kardamom_state::env::{Durability, StateEnvBuilder};
use kardamom_state::{StateSnapshot, StateWriter, read_recovery_point};
use kardamom_types::StateDatabase;

#[test]
fn recovery_point_matches_last_committed_block() {
    let dir = tempfile::tempdir().unwrap();
    let addr = address!("0x00000000000000000000000000000000000000aa");

    // --- run 1: commit blocks 1..=3, then drop ---
    {
        let env = StateEnvBuilder::new(dir.path())
            .durability(Durability::SafeNoSync)
            .open()
            .unwrap();
        let mut writer = StateWriter::spawn(env).unwrap();
        // drain genesis snapshot
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
        // Drain three snapshots so we know the commits landed.
        for _ in 0..3 {
            writer.snapshot_rx.recv().unwrap();
        }
        // Submit a 4th delta but immediately shut down — the writer may or
        // may not have committed it. We assert resilience either way.
        let _ = writer
            .delta_tx
            .send(common::simple_delta(4, addr, 9999, 7, 9999));
        writer.shutdown().unwrap();
    }

    // --- run 2: re-open, assert recovery point + snapshot consistency ---
    let env = StateEnvBuilder::new(dir.path())
        .durability(Durability::SafeNoSync)
        .open()
        .unwrap();

    let rp = read_recovery_point(&env).unwrap();
    // The writer may have committed block 4 before shutdown. Either 3 or 4
    // is acceptable; what matters is internal consistency for that block.
    assert!(
        rp.last_committed_block == 3 || rp.last_committed_block == 4,
        "expected 3 or 4, got {}",
        rp.last_committed_block
    );

    let snap = StateSnapshot::open(&env).unwrap();
    let (_, balance, _) = snap.basic(addr).unwrap().unwrap();
    // Block 3 → balance 1003, block 4 → balance 9999.
    let slot = snap.storage(addr, common::slot_key(7)).unwrap();
    if balance == U256::from(1003u64) {
        assert_eq!(slot, U256::from(300u64), "block 3 consistency");
        assert_eq!(rp.last_committed_block, 3);
    } else {
        assert_eq!(balance, U256::from(9999u64));
        assert_eq!(slot, U256::from(9999u64), "block 4 consistency");
        assert_eq!(rp.last_committed_block, 4);
    }
}
