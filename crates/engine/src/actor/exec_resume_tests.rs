//! Phase 2 recovery: skip-count replay.
//!
//! On restart, the exec thread receives the canonical stream replayed
//! from record 0. It must skip everything already committed (see
//! [`ResumePoint`]). These tests drive the skip path with synthetic
//! records, with no Aeron and no archive. They confirm the executor does
//! not re-commit a replayed block or re-emit a replayed receipt, and
//! still executes everything past the persisted cursor.

use std::sync::{Arc, Mutex};

use alloy_primitives::{U256, address};
use alloy_signer_local::PrivateKeySigner;
use crossbeam_channel::bounded;
use kardamom_types::BlockBoundaryStart;
use revm::primitives::KECCAK_EMPTY;

use crate::error::ExecutorError;
use crate::exec_types::TxIndex;
use crate::reader::{NoEpochCheck, ReaderToExec};
use crate::state::{MockStateDatabase, StaticSnapshotSource};

use super::test_support::{ImmediateCommit, RecordingQueue, drain_commits, legacy, pos};
use super::{ExecToCommit, ExecutorConfig, ResumePoint, spawn_exec};

#[test]
fn resume_executes_from_cursor_with_absolute_counts() {
    // Before restart, the executor committed block 1 (2 transactions,
    // record_count=2). The canonical source (cluster REPLAY_FROM) delivers
    // from the cursor: only block 2's new transaction and boundary arrive,
    // with absolute indices and counts (tx_idx 2, boundary end count 3).
    // The exec thread must seed its counters from the ResumePoint.
    // Starting them at zero made this exact stream fail with
    // BoundaryMisaligned on every mid-chain restart.
    let signer = PrivateKeySigner::random();
    let to = address!("00000000000000000000000000000000000ABCDE");
    // The snapshot represents the post-block-1 state: the signer nonce is
    // already at 2.
    let snap = MockStateDatabase::builder()
        .account(
            signer.address(),
            U256::from(10u128.pow(18)),
            2,
            KECCAK_EMPTY,
        )
        .build();
    let writer_log = Arc::new(Mutex::new(Vec::new()));

    let (tx_r2e, rx_r2e) = bounded::<ReaderToExec>(16);
    let (tx_e2c, rx_e2c) = bounded::<ExecToCommit>(16);

    // Only post-cursor work: block 2's transaction and boundary, with
    // absolute keys.
    tx_r2e
        .send(ReaderToExec::Tx {
            tx_idx: TxIndex(2),
            envelope: legacy(&signer, to, 2, 10),
            position: pos(2),
        })
        .unwrap();
    tx_r2e
        .send(ReaderToExec::Boundary(BlockBoundaryStart {
            block_number: 2,
            end_tx_idx: pos(3),
            l2_timestamp: 1_700_000_001,
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
        ResumePoint {
            block: 1,
            record_count: 2,
            l2_timestamp: 1_700_000_000,
        },
        None,
        None,
        None,
        None::<NoEpochCheck>,
        None,
    );
    h.join().expect("no panic").expect("exec ok");

    let (receipt_blocks, boundaries) = drain_commits(rx_e2c);
    // Block 2's single transaction produces a receipt attributed to block
    // 2, the seeded current_block. It is not attributed to block 1, which
    // would be a zero-seeded counter's value.
    assert_eq!(
        receipt_blocks,
        vec![2],
        "the post-cursor tx receipts once, in block 2"
    );
    assert_eq!(boundaries, vec![2], "block 2 commits");
    let log = writer_log.lock().unwrap();
    assert_eq!(log.len(), 1, "only block 2 is submitted to the writer");
    assert_eq!(log[0].0.block_number, 2);
}

#[test]
fn resume_after_empty_block_backlog() {
    // The sealer kept emitting empty blocks (1, 2, 3) while the executor
    // was down, so record_count stayed 0. With resume={block:3, count:0},
    // delivery resumes at block 4. Its first real transaction (absolute
    // index 0) executes and commits, attributed to block 4, not to a
    // counter restarted from 1.
    let signer = PrivateKeySigner::random();
    let to = address!("00000000000000000000000000000000000ABCDE");
    let snap = MockStateDatabase::builder()
        .account(
            signer.address(),
            U256::from(10u128.pow(18)),
            0,
            KECCAK_EMPTY,
        )
        .build();
    let writer_log = Arc::new(Mutex::new(Vec::new()));

    let (tx_r2e, rx_r2e) = bounded::<ReaderToExec>(16);
    let (tx_e2c, rx_e2c) = bounded::<ExecToCommit>(16);

    // Block 4: the first real transaction (count 0 to 1).
    tx_r2e
        .send(ReaderToExec::Tx {
            tx_idx: TxIndex(0),
            envelope: legacy(&signer, to, 0, 10),
            position: pos(0),
        })
        .unwrap();
    tx_r2e
        .send(ReaderToExec::Boundary(BlockBoundaryStart {
            block_number: 4,
            end_tx_idx: pos(1),
            l2_timestamp: 1_700_000_004,
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
        ResumePoint {
            block: 3,
            record_count: 0,
            l2_timestamp: 1_700_000_003,
        },
        None,
        None,
        None,
        None::<NoEpochCheck>,
        None,
    );
    h.join().expect("no panic").expect("exec ok");

    let (receipt_blocks, boundaries) = drain_commits(rx_e2c);
    assert_eq!(
        receipt_blocks,
        vec![4],
        "block 4's tx receipts once, in block 4"
    );
    assert_eq!(boundaries, vec![4]);
    let log = writer_log.lock().unwrap();
    assert_eq!(log.len(), 1);
    assert_eq!(log[0].0.block_number, 4);
}

#[test]
fn resume_boundary_alignment_still_checked() {
    // Resume must not bypass the boundary-alignment invariant. A boundary
    // whose absolute record count disagrees with the seeded-and-advanced
    // counter is still fatal. Here the cursor is 5, one transaction is
    // seen, so the count is 6, but the boundary claims 10.
    let signer = PrivateKeySigner::random();
    let to = address!("00000000000000000000000000000000000ABCDE");
    let snap = MockStateDatabase::builder()
        .account(
            signer.address(),
            U256::from(10u128.pow(18)),
            0,
            KECCAK_EMPTY,
        )
        .build();
    let (tx_r2e, rx_r2e) = bounded::<ReaderToExec>(8);
    let (tx_e2c, _rx_e2c) = bounded::<ExecToCommit>(8);

    tx_r2e
        .send(ReaderToExec::Tx {
            tx_idx: TxIndex(5),
            envelope: legacy(&signer, to, 0, 10),
            position: pos(5),
        })
        .unwrap();
    tx_r2e
        .send(ReaderToExec::Boundary(BlockBoundaryStart {
            block_number: 2,
            end_tx_idx: pos(10),
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
        RecordingQueue(Arc::new(Mutex::new(Vec::new()))),
        ResumePoint {
            block: 1,
            record_count: 5,
            l2_timestamp: 1_700_000_000,
        },
        None,
        None,
        None,
        None::<NoEpochCheck>,
        None,
    );
    let res = h.join().expect("no panic");
    assert!(matches!(res, Err(ExecutorError::BoundaryMisaligned { .. })));
}

#[test]
fn no_resume_executes_and_commits_block_one() {
    // resume=None is the fresh-start path: block 1 executes and commits.
    let signer = PrivateKeySigner::random();
    let to = address!("00000000000000000000000000000000000ABCDE");
    let snap = MockStateDatabase::builder()
        .account(
            signer.address(),
            U256::from(10u128.pow(18)),
            0,
            KECCAK_EMPTY,
        )
        .build();
    let writer_log = Arc::new(Mutex::new(Vec::new()));
    let (tx_r2e, rx_r2e) = bounded::<ReaderToExec>(8);
    let (tx_e2c, rx_e2c) = bounded::<ExecToCommit>(8);

    tx_r2e
        .send(ReaderToExec::Tx {
            tx_idx: TxIndex(0),
            envelope: legacy(&signer, to, 0, 10),
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
        None,
        None,
        None,
        None::<NoEpochCheck>,
        None,
    );
    h.join().expect("no panic").expect("exec ok");

    let (receipt_blocks, boundaries) = drain_commits(rx_e2c);
    assert_eq!(receipt_blocks, vec![1]);
    assert_eq!(boundaries, vec![1]);
    assert_eq!(writer_log.lock().unwrap().len(), 1);
}
