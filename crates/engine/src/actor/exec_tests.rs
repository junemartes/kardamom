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
use crate::state::{MockStateDatabase, StaticSnapshotSource};

use super::test_support::{ImmediateCommit, RecordingQueue, legacy, pos};
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
        None,
    );
    let res = h.join().expect("no panic");
    assert!(matches!(res, Err(ExecutorError::BoundaryMisaligned { .. })));
}

// ── Interop P1: remote epochs execute as 0x7D deliveries ────────────────────

fn remote_epoch_fixture(origin: u64, n: u64) -> kardamom_types::xchain::RemoteEpochRecord {
    use kardamom_types::xchain::{RemoteEpochRecord, XChainMessage, remote_source_hash};
    RemoteEpochRecord {
        origin_chain_id: origin,
        anchor_number: 900,
        anchor_hash: alloy_primitives::B256::repeat_byte(0x0A),
        first_seq: 0,
        messages: (0..n)
            .map(|seq| XChainMessage {
                source_hash: remote_source_hash(origin, seq),
                seq,
                origin_sender: Address::repeat_byte(0xA5),
                target: Address::repeat_byte(0xB6),
                value: 0,
                gas_limit: 100_000,
                input: Default::default(),
                callback: None,
            })
            .collect(),
    }
}

/// Feed a marker + its messages + the closing boundary into the channel,
/// mirroring what the tx_ordering reader dispatches for one record.
fn send_remote_epoch(
    tx_r2e: &crossbeam_channel::Sender<ReaderToExec>,
    record: kardamom_types::xchain::RemoteEpochRecord,
) {
    let origin = record.origin_chain_id;
    let messages = record.messages.clone();
    tx_r2e
        .send(ReaderToExec::RemoteEpoch {
            tx_idx: TxIndex(0),
            record: Box::new(record),
            position: pos(0),
        })
        .unwrap();
    for (i, message) in messages.into_iter().enumerate() {
        tx_r2e
            .send(ReaderToExec::XChain {
                tx_idx: TxIndex(1 + i as u64),
                origin_chain_id: origin,
                message: Box::new(message),
                position: pos(1 + i as i32),
            })
            .unwrap();
    }
}

struct RecordingRemoteObserver(Arc<Mutex<Vec<u64>>>);
impl crate::reader::RemoteEpochObserver for RecordingRemoteObserver {
    fn observe(
        &mut self,
        rec: &kardamom_types::xchain::RemoteEpochRecord,
    ) -> Result<(), ExecutorError> {
        self.0.lock().unwrap().push(rec.origin_chain_id);
        Ok(())
    }
}

/// The marker consumes a slot but applies no tx; each message executes as a
/// 0x7D receipt keyed by its remote source hash, from the aliased origin
/// Outbox — and the observer seam fires on the marker, before the messages.
#[test]
fn remote_epoch_messages_execute_as_0x7d_receipts() {
    use kardamom_types::xchain;

    let origin: u64 = 424_242;
    let record = remote_epoch_fixture(origin, 2);

    let snap = MockStateDatabase::builder().build();
    let writer_log = Arc::new(Mutex::new(Vec::new()));
    let (tx_r2e, rx_r2e) = bounded::<ReaderToExec>(8);
    let (tx_e2c, rx_e2c) = bounded::<ExecToCommit>(64);

    send_remote_epoch(&tx_r2e, record);
    tx_r2e
        .send(ReaderToExec::Boundary(BlockBoundaryStart {
            block_number: 1,
            // Marker + 2 messages = 3 slots consumed.
            end_tx_idx: pos(3),
            l2_timestamp: 1_700_000_000,
            l1_origin: 0,
        }))
        .unwrap();
    drop(tx_r2e);

    let observed = Arc::new(Mutex::new(Vec::new()));
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
        Some(Box::new(RecordingRemoteObserver(observed.clone()))),
    );
    h.join().expect("no panic").expect("exec ok");

    let mut receipts = Vec::new();
    while let Ok(m) = rx_e2c.try_recv() {
        if let ExecToCommit::Receipt(r) = m {
            receipts.push(r);
        }
    }
    assert_eq!(
        receipts.len(),
        2,
        "marker applies no tx; each message applies one"
    );
    for (seq, r) in receipts.iter().enumerate() {
        assert_eq!(r.tx_type, kardamom_types::TX_TYPE_XCHAIN);
        assert_eq!(r.tx_hash, xchain::remote_source_hash(origin, seq as u64));
        assert!(r.status);
        assert_eq!(r.from, xchain::xchain_tx_sender(origin));
        assert_eq!(r.to, Some(xchain::INBOX));
    }
    assert_eq!(
        *observed.lock().unwrap(),
        vec![origin],
        "observer fired once, on the marker"
    );
    // The block's receipts also persist through the writer, like any tx's.
    let log = writer_log.lock().unwrap();
    assert_eq!(log[0].1.receipts.len(), 2);
}

/// Whole-block execution (the validator's parallel path) BUFFERS cross-chain
/// messages like deposits and hands them to the strategy at the boundary —
/// the slice-2 gap where this arm used to fail-stop the engine. The strategy
/// here dispatches through `execute_xchain_tx` exactly as the streaming path
/// does, and the receipts land on the commit channel unchanged.
#[test]
fn whole_block_strategy_receives_buffered_xchain_records() {
    use super::types::{BlockExec, BlockExecOutput, BufferedRecord};
    use crate::delta::PendingDelta;
    use crate::executor::execute_xchain_tx;
    use kardamom_types::xchain;

    let origin: u64 = 424_242;
    let record = remote_epoch_fixture(origin, 2);

    let snap = MockStateDatabase::builder().build();
    let writer_log = Arc::new(Mutex::new(Vec::new()));
    let (tx_r2e, rx_r2e) = bounded::<ReaderToExec>(8);
    let (tx_e2c, rx_e2c) = bounded::<ExecToCommit>(64);

    send_remote_epoch(&tx_r2e, record);
    tx_r2e
        .send(ReaderToExec::Boundary(BlockBoundaryStart {
            block_number: 1,
            end_tx_idx: pos(3), // marker + 2 messages
            l2_timestamp: 1_700_000_000,
            l1_origin: 0,
        }))
        .unwrap();
    drop(tx_r2e);

    // A minimal whole-block strategy: record what arrived, execute the
    // XChain arms sequentially through the shared executor entry point.
    let seen_kinds = Arc::new(Mutex::new(Vec::<&'static str>::new()));
    let seen = seen_kinds.clone();
    let strategy: BlockExec<MockStateDatabase> =
        Box::new(move |snapshot, _parent, records, env, _block| {
            let mut receipts = Vec::new();
            let mut delta = PendingDelta::new();
            let mut cumulative = 0u64;
            for (i, rec) in records.iter().enumerate() {
                match rec {
                    BufferedRecord::Tx { .. } => seen.lock().unwrap().push("tx"),
                    BufferedRecord::Deposit { .. } => seen.lock().unwrap().push("deposit"),
                    BufferedRecord::XChain {
                        tx_idx,
                        origin_chain_id,
                        message,
                        position,
                    } => {
                        seen.lock().unwrap().push("xchain");
                        let (r, ws) = execute_xchain_tx(
                            snapshot,
                            None,
                            &delta,
                            env,
                            *tx_idx,
                            *position,
                            *origin_chain_id,
                            message,
                            i as u64,
                            cumulative,
                            None,
                        )?;
                        cumulative = r.cumulative_gas_used;
                        delta.apply(ws);
                        receipts.push(r);
                    }
                }
            }
            Ok(BlockExecOutput { receipts, delta })
        });

    let h = spawn_exec(
        ExecutorConfig::default(),
        rx_r2e,
        tx_e2c,
        StaticSnapshotSource(snap),
        ImmediateCommit,
        RecordingQueue(writer_log),
        0,
        None,
        None,
        Some(strategy),
        None,
        None,
    );
    h.join().expect("no panic").expect("exec ok");

    assert_eq!(
        *seen_kinds.lock().unwrap(),
        vec!["xchain", "xchain"],
        "the strategy must receive the buffered 0x7D records (marker excluded)"
    );
    let receipts: Vec<_> = rx_e2c
        .try_iter()
        .filter_map(|m| match m {
            ExecToCommit::Receipt(r) => Some(r),
            _ => None,
        })
        .collect();
    assert_eq!(receipts.len(), 2);
    for (seq, r) in receipts.iter().enumerate() {
        assert_eq!(r.tx_type, kardamom_types::TX_TYPE_XCHAIN);
        assert_eq!(r.tx_hash, xchain::remote_source_hash(origin, seq as u64));
    }
}

struct RejectingRemoteObserver;
impl crate::reader::RemoteEpochObserver for RejectingRemoteObserver {
    fn observe(
        &mut self,
        rec: &kardamom_types::xchain::RemoteEpochRecord,
    ) -> Result<(), ExecutorError> {
        Err(ExecutorError::State(format!(
            "remote epoch rejected (origin {})",
            rec.origin_chain_id
        )))
    }
}

/// A rejected record fail-stops on the MARKER — before any of its messages
/// execute — the same posture as a rejected L1 epoch.
#[test]
fn rejected_remote_epoch_halts_before_messages_execute() {
    let record = remote_epoch_fixture(424_242, 2);
    let snap = MockStateDatabase::builder().build();
    let (tx_r2e, rx_r2e) = bounded::<ReaderToExec>(8);
    let (tx_e2c, rx_e2c) = bounded::<ExecToCommit>(64);
    send_remote_epoch(&tx_r2e, record);
    drop(tx_r2e);

    let h = spawn_exec(
        ExecutorConfig::default(),
        rx_r2e,
        tx_e2c,
        StaticSnapshotSource(snap),
        ImmediateCommit,
        RecordingQueue(Arc::new(Mutex::new(Vec::new()))),
        0,
        None,
        None,
        None,
        None,
        Some(Box::new(RejectingRemoteObserver)),
    );
    let res = h.join().expect("no panic");
    assert!(matches!(res, Err(ExecutorError::State(_))), "got {res:?}");
    assert!(
        !rx_e2c
            .try_iter()
            .any(|m| matches!(m, ExecToCommit::Receipt(_))),
        "no message may execute after its record is rejected"
    );
}
