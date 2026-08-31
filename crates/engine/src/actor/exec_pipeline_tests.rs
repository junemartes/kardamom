//! Pipelined-commit tests. These check parent-layer visibility across
//! unsettled blocks, depth-K blocking behavior, and idle-tail settling
//! through the probe.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use alloy_primitives::{U256, address};
use alloy_signer_local::PrivateKeySigner;
use crossbeam_channel::bounded;
use kardamom_types::BlockBoundaryStart;
use revm::primitives::KECCAK_EMPTY;

use crate::exec_types::TxIndex;
use crate::reader::{NoEpochCheck, ReaderToExec};
use crate::state::{MockStateDatabase, StaticSnapshotSource};

use super::test_support::{ImmediateCommit, RecordingQueue, StagedCommit, legacy, pos};
use super::{ExecToCommit, ExecutorConfig, ResumePoint, spawn_exec};

/// This test checks pipelined commit. Block N+1 executes against
/// snapshot plus parent(N) while block N's commit is still unsettled.
/// Boundaries still forward in order once their block is durable. The
/// StaticSnapshotSource never advances, so cross-block visibility can
/// only come from the parent layer. A broken layer makes the block-2
/// spend fail (status 0) and turns this test red.
#[test]
fn exec_pipelines_commit_and_next_block_reads_parent_layer() {
    let signer_a = PrivateKeySigner::random();
    let signer_b = PrivateKeySigner::random();
    let a = signer_a.address();
    let b = signer_b.address();
    let c = address!("00000000000000000000000000000000000ABCDE");

    // Only A is funded at genesis. B's balance exists only in block 1's
    // delta until that block's commit lands. The static source never
    // exposes that commit.
    let snap = MockStateDatabase::builder()
        .account(a, U256::from(10u128.pow(18)), 0, KECCAK_EMPTY)
        .build();
    let writer_log = Arc::new(Mutex::new(Vec::new()));

    let (tx_r2e, rx_r2e) = bounded::<ReaderToExec>(8);
    let (tx_e2c, rx_e2c) = bounded::<ExecToCommit>(8);

    // Block 1: A sends to B, a large transfer so B can pay gas in block 2.
    tx_r2e
        .send(ReaderToExec::Tx {
            tx_idx: TxIndex(0),
            envelope: legacy(&signer_a, b, 0, 100_000_000_000_000_000),
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
    // Block 2: B sends to C, spending funds B has only from block 1's writes.
    tx_r2e
        .send(ReaderToExec::Tx {
            tx_idx: TxIndex(1),
            envelope: legacy(&signer_b, c, 0, 60),
            position: pos(1),
        })
        .unwrap();
    tx_r2e
        .send(ReaderToExec::Boundary(BlockBoundaryStart {
            block_number: 2,
            end_tx_idx: pos(2),
            l2_timestamp: 1_700_000_002,
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
        None,
    );
    h.join().expect("no panic").expect("exec ok");

    // The e2c ordering proves the pipeline shape. Block 2's receipt streams
    // before boundary 1 forwards. Boundary 1 settles when boundary 2
    // enters; boundary 2 settles at the end of the stream.
    let mut kinds = Vec::new();
    while let Ok(m) = rx_e2c.try_recv() {
        kinds.push(match m {
            ExecToCommit::Receipt(r) => {
                assert!(r.status, "every tx must succeed (parent layer visible)");
                format!("R{}", r.block_number)
            }
            ExecToCommit::Boundary(bd) => format!("B{}", bd.block_number),
        });
    }
    assert_eq!(
        kinds,
        vec!["R1", "R2", "B1", "B2"],
        "receipts stream ahead; boundaries settle in order at the next boundary / stream end"
    );

    // Block 2's delta must show the spend: C receives 60.
    let log = writer_log.lock().unwrap();
    assert_eq!(log.len(), 2);
    let (_, d2) = &log[1];
    let c_acc = d2
        .accounts
        .iter()
        .find(|x| x.address == c)
        .expect("C credited in block 2");
    assert_eq!(c_acc.balance, U256::from(60u64));
}

/// This test checks depth-K pipelining. With a writer that never advances
/// on its own, execution runs K=4 blocks ahead without blocking
/// (boundaries stay withheld). The 5th boundary blocks only for the
/// oldest commit, then settles forward in order. The end of the stream
/// drains the rest.
#[test]
fn exec_pipelines_k_deep_and_blocks_only_at_capacity() {
    let snap = MockStateDatabase::builder().build();
    let writer_log = Arc::new(Mutex::new(Vec::new()));
    let durable = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let blocking_waits = Arc::new(std::sync::atomic::AtomicU64::new(0));

    let (tx_r2e, rx_r2e) = bounded::<ReaderToExec>(16);
    let (tx_e2c, rx_e2c) = bounded::<ExecToCommit>(16);

    for n in 1..=6u64 {
        tx_r2e
            .send(ReaderToExec::Boundary(BlockBoundaryStart {
                block_number: n,
                end_tx_idx: pos(0),
                l2_timestamp: 1_700_000_000 + n,
                l1_origin: 0,
            }))
            .unwrap();
    }
    drop(tx_r2e);

    let h = spawn_exec(
        ExecutorConfig::default(),
        rx_r2e,
        tx_e2c,
        StaticSnapshotSource(snap),
        StagedCommit {
            durable: durable.clone(),
            blocking_waits: blocking_waits.clone(),
        },
        RecordingQueue(writer_log.clone()),
        ResumePoint::GENESIS,
        None,
        None,
        None,
        None::<NoEpochCheck>,
        None,
    );
    h.join().expect("no panic").expect("exec ok");

    // Boundaries 1-4 pipeline without any blocking wait. Boundaries 5 and
    // 6 each block once for the oldest commit. The end of stream drains
    // with one final wait: 2 capacity waits plus 1 drain wait.
    assert_eq!(
        blocking_waits.load(std::sync::atomic::Ordering::SeqCst),
        3,
        "exec must block only at depth K and at end-of-stream"
    );
    let boundaries: Vec<u64> = std::iter::from_fn(|| rx_e2c.try_recv().ok())
        .map(|m| match m {
            ExecToCommit::Boundary(b) => b.block_number,
            ExecToCommit::Receipt(_) => panic!("no receipts in this scenario"),
        })
        .collect();
    assert_eq!(
        boundaries,
        vec![1, 2, 3, 4, 5, 6],
        "settled in order, none lost"
    );
    assert_eq!(writer_log.lock().unwrap().len(), 6, "all deltas submitted");
}

/// This test checks idle-tail settling. In-flight commits below depth K
/// must settle and forward their boundaries with no further input on the
/// reader channel, using the idle probe, once the writer reports them
/// durable. Before the probe, settling happened only at the next
/// boundary. So an idle chain's last boundary closeouts (up to K of them)
/// never published. This stalled ingress watermarks and
/// executor/validator drains, and left attester blocks uncovered.
#[test]
fn exec_settles_inflight_commits_while_idle() {
    let snap = MockStateDatabase::builder().build();
    let writer_log = Arc::new(Mutex::new(Vec::new()));
    let durable = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let blocking_waits = Arc::new(std::sync::atomic::AtomicU64::new(0));

    let (tx_r2e, rx_r2e) = bounded::<ReaderToExec>(16);
    let (tx_e2c, rx_e2c) = bounded::<ExecToCommit>(16);

    // Three boundaries, below depth K, so nothing blocks. With the writer
    // stuck at 0, nothing settles either. The channel stays open, so no
    // end-of-stream drain can rescue them.
    for n in 1..=3u64 {
        tx_r2e
            .send(ReaderToExec::Boundary(BlockBoundaryStart {
                block_number: n,
                end_tx_idx: pos(0),
                l2_timestamp: 1_700_000_000 + n,
                l1_origin: 0,
            }))
            .unwrap();
    }

    let h = spawn_exec(
        ExecutorConfig::default(),
        rx_r2e,
        tx_e2c,
        StaticSnapshotSource(snap),
        StagedCommit {
            durable: durable.clone(),
            blocking_waits: blocking_waits.clone(),
        },
        RecordingQueue(writer_log.clone()),
        ResumePoint::GENESIS,
        None,
        None,
        None,
        None::<NoEpochCheck>,
        None,
    );

    // The writer catches up on its own. The exec thread must notice
    // through the idle probe and forward all three boundaries, with no
    // blocking wait and no further reader input.
    durable.store(3, std::sync::atomic::Ordering::SeqCst);
    let mut boundaries = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while boundaries.len() < 3 && std::time::Instant::now() < deadline {
        match rx_e2c.recv_timeout(Duration::from_millis(100)) {
            Ok(ExecToCommit::Boundary(b)) => boundaries.push(b.block_number),
            Ok(ExecToCommit::Receipt(_)) => panic!("no receipts in this scenario"),
            Err(_) => {}
        }
    }
    assert_eq!(
        boundaries,
        vec![1, 2, 3],
        "idle probe must settle + forward in-flight boundaries without further input"
    );
    assert_eq!(
        blocking_waits.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "an idle probe must never park"
    );

    drop(tx_r2e);
    h.join().expect("no panic").expect("exec ok");
}
