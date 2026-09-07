//! BAL capture at the boundary handoff, footprint-shadow captures, and the
//! boundary-alignment fail-stop.

use alloy_primitives::{Address, address};
use alloy_signer_local::PrivateKeySigner;
use crossbeam_channel::bounded;

use crate::error::ExecutorError;
use crate::reader::ReaderToExec;
use crate::state::StaticSnapshotSource;

use crate::actor::BalHandoff;
use crate::actor::test_support::{ExecRig, ImmediateCommit, boundary_msg, feed, funded, tx_msg};

/// This test goes through the actor. With a BAL channel attached, the
/// handoff at each boundary must carry a populated Bal. Direct
/// `execute_tx` tests pass, but early measurement showed empty BALs
/// despite large deltas, so the gap is in this wiring, not the execution
/// logic.
#[test]
fn exec_handoff_carries_a_populated_bal() {
    let signer = PrivateKeySigner::random();
    let to = address!("00000000000000000000000000000000000ABCDE");
    let snap = funded(&signer, 0);
    let (bal_tx, bal_rx) = bounded::<BalHandoff>(8);

    let rx_r2e = feed(vec![
        tx_msg(&signer, to, 0, 0, 100),
        boundary_msg(1, 1, 1_700_000_000),
    ]);

    let (rig, _writer_log) = ExecRig::recording(StaticSnapshotSource(snap), ImmediateCommit);
    let (h, _rx_e2c) = rig.bal(bal_tx).spawn(rx_r2e);
    h.join().expect("no panic").expect("exec ok");

    let (_boundary, delta, bal) = bal_rx.try_recv().expect("a BAL handoff");
    assert!(
        !delta.accounts.is_empty(),
        "delta carries the block's writes"
    );
    let alloy = bal.into_alloy_bal();
    assert!(
        !alloy.is_empty(),
        "handoff Bal is EMPTY while the delta has {} accounts",
        delta.accounts.len()
    );
}

#[test]
fn exec_rejects_misaligned_boundary() {
    let signer = PrivateKeySigner::random();
    // The boundary claims 5 canonical records (pos(5) == from_index(5)),
    // but we applied only 1. This count mismatch causes BoundaryMisaligned.
    let rx_r2e = feed(vec![
        tx_msg(&signer, Address::from([0x22u8; 20]), 0, 0, 0),
        boundary_msg(1, 5, 0),
    ]);

    // Pre-fund the signer so the tx doesn't fail before we hit the boundary.
    let snap = funded(&signer, 0);

    let (rig, _writer_log) = ExecRig::recording(StaticSnapshotSource(snap), ImmediateCommit);
    let (h, _rx_e2c) = rig.spawn(rx_r2e);
    let res = h.join().expect("no panic");
    assert!(matches!(res, Err(ExecutorError::BoundaryMisaligned { .. })));
}

/// This test checks the footprint shadow path. With `shadow_tx` wired,
/// each non-empty block hands its per-transaction captures (envelope,
/// gas, and read/write cells) to the shadow channel at the boundary. The
/// handed-off block must survive a full `process_block` pass (grade and
/// train) without changing execution outputs.
#[test]
fn exec_hands_off_shadow_captures_at_boundary() {
    let signer = PrivateKeySigner::random();
    let from = signer.address();
    let to = address!("00000000000000000000000000000000000ABCDE");

    let snap = funded(&signer, 0);

    let (stx, srx) = bounded::<crate::shadow::ShadowBlock>(8);

    let mut records: Vec<ReaderToExec> = [(0u64, 100u64), (1, 50)]
        .into_iter()
        .map(|(i, value)| tx_msg(&signer, to, i, i, value))
        .collect();
    records.push(boundary_msg(1, 2, 1_700_000_000));
    let rx_r2e = feed(records);

    let (rig, _writer_log) = ExecRig::recording(StaticSnapshotSource(snap), ImmediateCommit);
    let (h, _rx_e2c) = rig.shadow(stx).spawn(rx_r2e);
    h.join().expect("no panic").expect("exec ok");

    let blk = srx.recv().expect("one shadow block handed off");
    assert_eq!(blk.block_number, 1);
    assert_eq!(blk.serial_records, 0);
    assert_eq!(blk.captures.len(), 2);
    for (i, c) in blk.captures.iter().enumerate() {
        assert_eq!(c.envelope.sender, from);
        // A value transfer writes both account tuples. Zero gas price
        // keeps the fee sink out (`legacy` sets gas_price = 0).
        assert!(
            c.write_cells
                .contains(&kardamom_footprint::Cell::Account(from))
        );
        assert!(
            c.write_cells
                .contains(&kardamom_footprint::Cell::Account(to))
        );
        assert!(c.touches.slot_reads.is_empty(), "transfers read no slots");
        assert!(c.gas_used > 0, "capture {i} carries gas");
    }
    assert!(srx.try_recv().is_err(), "exactly one block");

    // The handed-off shape feeds the grading path end-to-end. Native
    // transfers are tier-1 (never cold); the same sender gives one chain.
    crate::shadow::Shadow::new().process_block(blk);
}
