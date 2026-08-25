//! Exec-thread tests: streaming execution, boundary emission, BAL capture,
//! and the boundary-alignment fail-stop.

use std::sync::{Arc, Mutex};

use alloy_primitives::{Address, U256, address};
use alloy_signer_local::PrivateKeySigner;
use crossbeam_channel::bounded;
use kardamom_types::{BlockBoundary, BlockBoundaryStart};
use revm::primitives::KECCAK_EMPTY;

use crate::error::ExecutorError;
use crate::exec_types::TxIndex;
use crate::reader::{NoEpochCheck, ReaderToExec};
use crate::state::{MockStateDatabase, StaticSnapshotSource};

use super::test_support::{ImmediateCommit, RecordingQueue, legacy, pos};
use super::{BalHandoff, ExecToCommit, ExecutorConfig, ResumePoint, spawn_exec};

#[test]
fn exec_runs_two_txs_and_emits_slim_boundary() {
    let signer = PrivateKeySigner::random();
    let from = signer.address();
    let to = address!("00000000000000000000000000000000000ABCDE");

    let snap = MockStateDatabase::builder()
        .account(from, U256::from(10u128.pow(18)), 0, KECCAK_EMPTY)
        .build();
    let writer_log = Arc::new(Mutex::new(Vec::new()));

    let (tx_r2e, rx_r2e) = bounded::<ReaderToExec>(8);
    let (tx_e2c, rx_e2c) = bounded::<ExecToCommit>(8);

    tx_r2e
        .send(ReaderToExec::Tx {
            tx_idx: TxIndex(0),
            envelope: legacy(&signer, to, 0, 100),
            position: pos(0),
        })
        .unwrap();
    tx_r2e
        .send(ReaderToExec::Tx {
            tx_idx: TxIndex(1),
            envelope: legacy(&signer, to, 1, 50),
            position: pos(1),
        })
        .unwrap();
    tx_r2e
        // Two canonical records applied ⇒ cumulative count 2. end_tx_idx
        // encodes that count (pos(2) == BPosition::from_index(2)).
        .send(ReaderToExec::Boundary(BlockBoundaryStart {
            block_number: 1,
            end_tx_idx: pos(2),
            l2_timestamp: 1_700_000_000,
            l1_origin: 0,
        }))
        .unwrap();
    drop(tx_r2e);

    let cfg = ExecutorConfig::default();
    let h = spawn_exec(
        cfg,
        rx_r2e,
        tx_e2c,
        StaticSnapshotSource(snap),
        ImmediateCommit,
        RecordingQueue(writer_log.clone()),
        ResumePoint::GENESIS,
        None,
        None,
        None,
        None::<NoEpochCheck>,
    );
    h.join().expect("no panic").expect("exec ok");
    drop(rx_e2c);

    let log = writer_log.lock().unwrap();
    assert_eq!(log.len(), 1);
    let (boundary, delta) = &log[0];
    assert_eq!(boundary.block_number, 1);
    assert_eq!(boundary.end_tx_idx, pos(2));
    assert_eq!(boundary.l2_timestamp, 1_700_000_000);
    // The recipient received 150 total across both transfers — verify
    // by iterating the canonical Vec<AccountChange> the wire form holds.
    let to_acc = delta
        .accounts
        .iter()
        .find(|a| a.address == to)
        .expect("recipient");
    assert_eq!(to_acc.balance, U256::from(150u64));
    // #109: the block's receipts ride inside the BlockDelta, in arrival
    // order, so the writer persists them (receipts + tx_hash_index) and
    // eth_getTransactionReceipt answers from durable state post-restart.
    assert_eq!(delta.receipts.len(), 2, "both txs' receipts persisted");
    assert!(delta.receipts.iter().all(|r| r.block_number == 1));
    assert_eq!(delta.receipts[0].nonce, 0);
    assert_eq!(delta.receipts[1].nonce, 1);
    // S0 regression guard: destructure to enforce the shape of
    // BlockBoundary at compile time — specifically that NO state-root
    // commitment sneaks in. `l1_origin` is a deliberate addition (the L1
    // epoch this block belongs to); a new field appearing here without a
    // spec behind it is the thing this guard is watching for.
    let BlockBoundary {
        block_number: _,
        end_tx_idx: _,
        l1_origin: _,
        l2_timestamp: _,
    } = boundary;
}

/// End-to-end through the ACTOR: with a BAL channel attached, the
/// handoff at each boundary must carry a POPULATED Bal. Live phase-1
/// measurement produced 1-byte (empty) BALs while deltas were 76KB —
/// direct `execute_tx` tests passed, so the gap is in this wiring.
/// Scope-cache visibility across record kinds: a DEPOSIT credits an
/// account mid-block; a LATER TX in the same block spends that credit.
/// The deposit runs outside the ExecScope (own commit semantics), so
/// its writes are folded into the scope cache explicitly — this test
/// pins that fold. Without it the spend is an insufficient-funds skip.
#[test]
fn deposit_credit_is_visible_to_later_txs_in_the_block() {
    let signer = PrivateKeySigner::random();
    let from = signer.address();
    let to = address!("00000000000000000000000000000000000BEEF0");
    // The sender does NOT exist pre-block: only the deposit funds it.
    let snap = MockStateDatabase::builder().build();
    let writer_log = Arc::new(Mutex::new(Vec::new()));

    let (tx_r2e, rx_r2e) = bounded::<ReaderToExec>(8);
    let (tx_e2c, rx_e2c) = bounded::<ExecToCommit>(64);

    tx_r2e
        .send(ReaderToExec::Deposit {
            tx_idx: TxIndex(0),
            deposit: kardamom_types::Deposit {
                source_hash: alloy_primitives::B256::repeat_byte(0x11),
                from,
                to: Some(from),
                mint: 10u128.pow(18),
                value: U256::ZERO,
                gas_limit: 100_000,
                is_system_transaction: false,
                input: Default::default(),
            },
            position: pos(0),
        })
        .unwrap();
    tx_r2e
        .send(ReaderToExec::Tx {
            tx_idx: TxIndex(1),
            envelope: legacy(&signer, to, 1, 1_000),
            position: pos(64),
        })
        .unwrap();
    tx_r2e
        .send(ReaderToExec::Boundary(BlockBoundaryStart {
            block_number: 1,
            end_tx_idx: pos(2),
            l2_timestamp: 1_700_000_000,
            l1_origin: 0,
        }))
        .unwrap();
    drop(tx_r2e);

    let h = spawn_exec(
        ExecutorConfig::default(),
        rx_r2e,
        tx_e2c,
        StaticSnapshotSource(snap),
        ImmediateCommit,
        RecordingQueue(writer_log.clone()),
        ResumePoint::GENESIS,
        None,
        None,
        None,
        None::<NoEpochCheck>,
    );
    h.join().expect("no panic").expect("exec ok");

    // The transfer must have EXECUTED (status true), not skipped for
    // missing funds: the deposit's credit reached the scope cache.
    let mut saw_transfer_success = false;
    while let Ok(msg) = rx_e2c.try_recv() {
        if let ExecToCommit::Receipt(r) = msg
            && r.tx_idx == pos(64)
        {
            assert!(
                r.status,
                "transfer after same-block deposit must execute: {r:?}"
            );
            assert!(r.gas_used > 0);
            saw_transfer_success = true;
        }
    }
    assert!(saw_transfer_success, "transfer receipt not observed");
}

#[test]
fn exec_handoff_carries_a_populated_bal() {
    let signer = PrivateKeySigner::random();
    let from = signer.address();
    let to = address!("00000000000000000000000000000000000ABCDE");
    let snap = MockStateDatabase::builder()
        .account(from, U256::from(10u128.pow(18)), 0, KECCAK_EMPTY)
        .build();
    let writer_log = Arc::new(Mutex::new(Vec::new()));

    let (tx_r2e, rx_r2e) = bounded::<ReaderToExec>(8);
    let (tx_e2c, _rx_e2c) = bounded::<ExecToCommit>(64);
    let (bal_tx, bal_rx) = bounded::<BalHandoff>(8);

    tx_r2e
        .send(ReaderToExec::Tx {
            tx_idx: TxIndex(0),
            envelope: legacy(&signer, to, 0, 100),
            position: pos(0),
        })
        .unwrap();
    tx_r2e
        .send(ReaderToExec::Boundary(BlockBoundaryStart {
            block_number: 1,
            end_tx_idx: pos(1),
            l2_timestamp: 1_700_000_000,
            l1_origin: 0,
        }))
        .unwrap();
    drop(tx_r2e);

    let h = spawn_exec(
        ExecutorConfig::default(),
        rx_r2e,
        tx_e2c,
        StaticSnapshotSource(snap),
        ImmediateCommit,
        RecordingQueue(writer_log.clone()),
        ResumePoint::GENESIS,
        Some(bal_tx),
        None,
        None,
        None::<NoEpochCheck>,
    );
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
    let writer_log = Arc::new(Mutex::new(Vec::new()));

    let (tx_r2e, rx_r2e) = bounded::<ReaderToExec>(8);
    let (tx_e2c, _rx_e2c) = bounded::<ExecToCommit>(8);

    let signer = PrivateKeySigner::random();
    tx_r2e
        .send(ReaderToExec::Tx {
            tx_idx: TxIndex(0),
            envelope: legacy(&signer, Address::from([0x22u8; 20]), 0, 0),
            position: pos(0),
        })
        .unwrap();
    // Boundary claims 5 canonical records (pos(5) == from_index(5)) but we
    // only applied 1 ⇒ count mismatch ⇒ BoundaryMisaligned.
    tx_r2e
        .send(ReaderToExec::Boundary(BlockBoundaryStart {
            block_number: 1,
            end_tx_idx: pos(5),
            l2_timestamp: 0,
            l1_origin: 0,
        }))
        .unwrap();
    drop(tx_r2e);

    // Pre-fund the signer so the tx doesn't fail before we hit the boundary.
    let snap = MockStateDatabase::builder()
        .account(
            signer.address(),
            U256::from(10u128.pow(18)),
            0,
            KECCAK_EMPTY,
        )
        .build();

    let cfg = ExecutorConfig::default();
    let h = spawn_exec(
        cfg,
        rx_r2e,
        tx_e2c,
        StaticSnapshotSource(snap),
        ImmediateCommit,
        RecordingQueue(writer_log),
        ResumePoint::GENESIS,
        None,
        None,
        None,
        None::<NoEpochCheck>,
    );
    let res = h.join().expect("no panic");
    assert!(matches!(res, Err(ExecutorError::BoundaryMisaligned { .. })));
}

/// P1 footprint shadow: with `shadow_tx` wired, each non-empty block
/// hands its per-tx captures (envelope + gas + read/write cells) to the
/// shadow channel at the boundary — and the handed-off block survives a
/// full `process_block` pass (grade + train) without touching execution
/// outputs.
#[test]
fn exec_hands_off_shadow_captures_at_boundary() {
    let signer = PrivateKeySigner::random();
    let from = signer.address();
    let to = address!("00000000000000000000000000000000000ABCDE");

    let snap = MockStateDatabase::builder()
        .account(from, U256::from(10u128.pow(18)), 0, KECCAK_EMPTY)
        .build();
    let writer_log = Arc::new(Mutex::new(Vec::new()));

    let (tx_r2e, rx_r2e) = bounded::<ReaderToExec>(8);
    let (tx_e2c, rx_e2c) = bounded::<ExecToCommit>(8);
    let (stx, srx) = bounded::<crate::shadow::ShadowBlock>(8);

    for (i, value) in [(0u64, 100u64), (1, 50)] {
        tx_r2e
            .send(ReaderToExec::Tx {
                tx_idx: TxIndex(i),
                envelope: legacy(&signer, to, i, value),
                position: pos(i as i32),
            })
            .unwrap();
    }
    tx_r2e
        .send(ReaderToExec::Boundary(BlockBoundaryStart {
            block_number: 1,
            end_tx_idx: pos(2),
            l2_timestamp: 1_700_000_000,
            l1_origin: 0,
        }))
        .unwrap();
    drop(tx_r2e);

    let h = spawn_exec(
        ExecutorConfig::default(),
        rx_r2e,
        tx_e2c,
        StaticSnapshotSource(snap),
        ImmediateCommit,
        RecordingQueue(writer_log.clone()),
        ResumePoint::GENESIS,
        None,
        Some(stx),
        None,
        None::<NoEpochCheck>,
    );
    h.join().expect("no panic").expect("exec ok");
    drop(rx_e2c);

    let blk = srx.recv().expect("one shadow block handed off");
    assert_eq!(blk.block_number, 1);
    assert_eq!(blk.serial_records, 0);
    assert_eq!(blk.captures.len(), 2);
    for (i, c) in blk.captures.iter().enumerate() {
        assert_eq!(c.envelope.sender, from);
        // A value transfer writes both account tuples; zero gas price
        // keeps the fee sink out (gas_price = 0 in `legacy`).
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

    // The handed-off shape feeds the grading path end-to-end: native
    // transfers are tier-1 (never cold) and same-sender => one chain.
    let mut stats = kardamom_footprint::classifier::Stats::default();
    let mut exclude = std::collections::HashSet::new();
    exclude.insert(kardamom_footprint::Cell::Account(crate::shadow::FEE_SINK));
    crate::shadow::process_block(blk, &mut stats, &exclude);
}
