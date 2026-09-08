//! Resume from a mid-chain cursor.
//!
//! On restart, the reader delivers only the records and boundaries past
//! the last committed cursor, with absolute indices and counts. The exec
//! thread must seed its own counters from that cursor (see
//! [`ResumePoint`]) instead of starting at zero. These tests drive the
//! resume path with synthetic records, with no Aeron and no archive. They
//! confirm the executor aligns on a mid-chain boundary, does not
//! re-commit an already-committed block, and does not re-emit an
//! already-published receipt.

use alloy_primitives::address;
use alloy_signer_local::PrivateKeySigner;

use crate::error::ExecutorError;
use crate::state::StaticSnapshotSource;

use super::ResumePoint;
use super::test_support::{
    ExecRig, ImmediateCommit, boundary_msg, drain_commits, feed, funded, tx_msg,
};

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
    let snap = funded(&signer, 2);
    let (rig, writer_log) = ExecRig::recording(StaticSnapshotSource(snap), ImmediateCommit);
    let rig = rig.start(ResumePoint {
        block: 1,
        record_count: 2,
        l2_timestamp: 1_700_000_000,
    });

    // Only post-cursor work: block 2's transaction and boundary, with
    // absolute keys.
    let rx_r2e = feed(vec![
        tx_msg(&signer, to, 2, 2, 10),
        boundary_msg(2, 3, 1_700_000_001),
    ]);

    let (h, rx_e2c) = rig.spawn(rx_r2e);
    h.join().expect("no panic").expect("exec ok");

    let (receipt_blocks, boundaries) = drain_commits(&rx_e2c);
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
    let snap = funded(&signer, 0);
    let (rig, writer_log) = ExecRig::recording(StaticSnapshotSource(snap), ImmediateCommit);
    let rig = rig.start(ResumePoint {
        block: 3,
        record_count: 0,
        l2_timestamp: 1_700_000_003,
    });

    // Block 4: the first real transaction (count 0 to 1).
    let rx_r2e = feed(vec![
        tx_msg(&signer, to, 0, 0, 10),
        boundary_msg(4, 1, 1_700_000_004),
    ]);

    let (h, rx_e2c) = rig.spawn(rx_r2e);
    h.join().expect("no panic").expect("exec ok");

    let (receipt_blocks, boundaries) = drain_commits(&rx_e2c);
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
    let snap = funded(&signer, 0);

    let rx_r2e = feed(vec![
        tx_msg(&signer, to, 5, 0, 10),
        boundary_msg(2, 10, 1_700_000_000),
    ]);

    let (rig, _writer_log) = ExecRig::recording(StaticSnapshotSource(snap), ImmediateCommit);
    let (h, _rx_e2c) = rig
        .start(ResumePoint {
            block: 1,
            record_count: 5,
            l2_timestamp: 1_700_000_000,
        })
        .spawn(rx_r2e);
    let res = h.join().expect("no panic");
    assert!(matches!(res, Err(ExecutorError::BoundaryMisaligned { .. })));
}

#[test]
fn no_resume_executes_and_commits_block_one() {
    // resume=None is the fresh-start path: block 1 executes and commits.
    let signer = PrivateKeySigner::random();
    let to = address!("00000000000000000000000000000000000ABCDE");
    let snap = funded(&signer, 0);
    let (rig, writer_log) = ExecRig::recording(StaticSnapshotSource(snap), ImmediateCommit);

    let rx_r2e = feed(vec![
        tx_msg(&signer, to, 0, 0, 10),
        boundary_msg(1, 1, 1_700_000_000),
    ]);

    let (h, rx_e2c) = rig.spawn(rx_r2e);
    h.join().expect("no panic").expect("exec ok");

    let (receipt_blocks, boundaries) = drain_commits(&rx_e2c);
    assert_eq!(receipt_blocks, vec![1]);
    assert_eq!(boundaries, vec![1]);
    assert_eq!(writer_log.lock().unwrap().len(), 1);
}
