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
use crate::reader::ReaderToExec;
use crate::state::{MockStateDatabase, MutatingSnapshotSource, StaticSnapshotSource};

use super::test_support::{ApplyingRecordingQueue, ImmediateCommit, RecordingQueue, legacy, pos};
use super::{BalHandoff, ExecToCommit, ExecutorConfig, spawn_exec};

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
        0,
        None,
        None,
        None,
        None,
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
        0,
        None,
        None,
        None,
        None,
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
        0,
        None,
        Some(bal_tx),
        None,
        None,
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
        0,
        None,
        None,
        None,
        None,
    );
    let res = h.join().expect("no panic");
    assert!(matches!(res, Err(ExecutorError::BoundaryMisaligned { .. })));
}

// ---------------------------------------------------------------------------
// Block-close protocol actions (L1-governed feature flags)
// ---------------------------------------------------------------------------

use kardamom_exec_core::features::{
    FEATURE_HEALTH_CHECK, HEALTH_BEACON_SLOT, activation_slot, unpack_beacon,
};
use kardamom_types::upgrades::CHAIN_STATE;

/// Read the beacon out of a submitted block delta.
fn beacon_in(delta: &kardamom_types::BlockDelta) -> Option<(u64, u64, u64)> {
    delta
        .storage
        .iter()
        .find(|s| s.address == CHAIN_STATE && s.key == HEALTH_BEACON_SLOT)
        .map(|s| unpack_beacon(s.value))
}

/// Two empty blocks, flag never scheduled: the chain must be byte-identical to
/// one built without the feature existing. This is the property that lets the
/// code ship dormant on a live chain.
#[test]
fn a_dormant_feature_writes_nothing_at_block_close() {
    let writer_log = Arc::new(Mutex::new(Vec::new()));
    let (tx_r2e, rx_r2e) = bounded::<ReaderToExec>(8);
    let (tx_e2c, rx_e2c) = bounded::<ExecToCommit>(8);

    for block_number in 1..=2u64 {
        tx_r2e
            .send(ReaderToExec::Boundary(BlockBoundaryStart {
                block_number,
                end_tx_idx: pos(0),
                l2_timestamp: 1_700_000_000_000 + block_number * 250,
                l1_origin: 0,
            }))
            .unwrap();
    }
    drop(tx_r2e);

    spawn_exec(
        ExecutorConfig::default(),
        rx_r2e,
        tx_e2c,
        StaticSnapshotSource(MockStateDatabase::builder().build()),
        ImmediateCommit,
        RecordingQueue(writer_log.clone()),
        0,
        None,
        None,
        None,
        None,
    )
    .join()
    .expect("no panic")
    .expect("exec ok");
    drop(rx_e2c);

    let log = writer_log.lock().unwrap();
    assert_eq!(log.len(), 2);
    for (_, delta) in log.iter() {
        assert!(
            delta.storage.is_empty(),
            "a dormant flag must not touch state"
        );
    }
}

/// With the flag active, EVERY block records a beacon — the beat counter
/// increments across blocks and each beacon carries that block's own number
/// and header timestamp.
#[test]
fn an_active_feature_beats_once_per_block() {
    let writer_log = Arc::new(Mutex::new(Vec::new()));
    let (tx_r2e, rx_r2e) = bounded::<ReaderToExec>(8);
    let (tx_e2c, rx_e2c) = bounded::<ExecToCommit>(8);

    // Activated at a time already in the past.
    let db = MockStateDatabase::builder()
        .storage(
            CHAIN_STATE,
            activation_slot(FEATURE_HEALTH_CHECK),
            U256::from(1_000u64),
        )
        .build();

    for block_number in 1..=3u64 {
        tx_r2e
            .send(ReaderToExec::Boundary(BlockBoundaryStart {
                block_number,
                end_tx_idx: pos(0),
                l2_timestamp: 2_000 + block_number,
                l1_origin: 0,
            }))
            .unwrap();
    }
    drop(tx_r2e);

    spawn_exec(
        ExecutorConfig::default(),
        rx_r2e,
        tx_e2c,
        MutatingSnapshotSource(db.clone()),
        ImmediateCommit,
        ApplyingRecordingQueue {
            db,
            log: writer_log.clone(),
        },
        0,
        None,
        None,
        None,
        None,
    )
    .join()
    .expect("no panic")
    .expect("exec ok");
    drop(rx_e2c);

    let log = writer_log.lock().unwrap();
    assert_eq!(log.len(), 3);
    for (i, (boundary, delta)) in log.iter().enumerate() {
        let beat = i as u64 + 1;
        assert_eq!(
            beacon_in(delta),
            Some((beat, boundary.block_number, boundary.l2_timestamp)),
            "block {} must carry beat {beat} with its own header fields",
            boundary.block_number
        );
    }
}

/// The beacon must be timed off the block's OWN header stamp, not the
/// (previous boundary's) stamp its transactions executed with. A feature
/// scheduled between two boundaries must therefore fire in the first block
/// whose header reaches it.
#[test]
fn activation_is_judged_against_the_blocks_own_header_timestamp() {
    let writer_log = Arc::new(Mutex::new(Vec::new()));
    let (tx_r2e, rx_r2e) = bounded::<ReaderToExec>(8);
    let (tx_e2c, rx_e2c) = bounded::<ExecToCommit>(8);

    let activation = 5_000u64;
    let snap = MockStateDatabase::builder()
        .storage(
            CHAIN_STATE,
            activation_slot(FEATURE_HEALTH_CHECK),
            U256::from(activation),
        )
        .build();

    // Headers straddle the activation time: 4_999 (before), 5_000 (exactly at
    // it — inclusive, so it fires), 5_001 (after).
    for (block_number, ts) in [(1u64, 4_999u64), (2, 5_000), (3, 5_001)] {
        tx_r2e
            .send(ReaderToExec::Boundary(BlockBoundaryStart {
                block_number,
                end_tx_idx: pos(0),
                l2_timestamp: ts,
                l1_origin: 0,
            }))
            .unwrap();
    }
    drop(tx_r2e);

    spawn_exec(
        ExecutorConfig::default(),
        rx_r2e,
        tx_e2c,
        MutatingSnapshotSource(snap.clone()),
        ImmediateCommit,
        ApplyingRecordingQueue {
            db: snap,
            log: writer_log.clone(),
        },
        0,
        None,
        None,
        None,
        None,
    )
    .join()
    .expect("no panic")
    .expect("exec ok");
    drop(rx_e2c);

    let log = writer_log.lock().unwrap();
    assert_eq!(beacon_in(&log[0].1), None, "block 1 is before activation");
    assert_eq!(
        beacon_in(&log[1].1),
        Some((1, 2, 5_000)),
        "activation is inclusive: the block AT T beats"
    );
    assert_eq!(beacon_in(&log[2].1), Some((2, 3, 5_001)));
}
