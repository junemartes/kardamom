//! The tx hook and the shared result path: `before` runs at arrival in
//! both execution modes, `after` runs when each tx's result exists, and
//! either one can stop the pipeline. Whole-block records finish through
//! the same path as streaming records.

use std::sync::atomic::Ordering;

use alloy_primitives::address;
use alloy_signer_local::PrivateKeySigner;
use crossbeam_channel::{Receiver, unbounded};

use crate::error::ExecutorError;
use crate::exec_types::TxIndex;
use crate::reader::ReaderToExec;
use crate::state::StaticSnapshotSource;

use crate::actor::test_hooks::{
    AppliedOkRecorder, EmptyBlockExec, HookCall, RecordingTxHook, SequentialBlockExec,
};
use crate::actor::test_support::{
    ExecRig, ImmediateCommit, RecordingQueue, boundary_msg, feed, funded, legacy, pos, tx_msg,
};
use crate::actor::{ExecToCommit, TxContext, TxHook};

/// Two transfers from one funded signer, then the block's boundary.
fn two_tx_block(signer: &PrivateKeySigner) -> Receiver<ReaderToExec> {
    let to = address!("00000000000000000000000000000000000ABCDE");
    feed(vec![
        tx_msg(signer, to, 0, 0, 100),
        tx_msg(signer, to, 1, 1, 50),
        boundary_msg(1, 2, 1_700_000_000),
    ])
}

/// Which record, if any, each hook method rejects.
#[derive(Clone, Copy, Default)]
struct Rejects {
    before: Option<u64>,
    after: Option<u64>,
}

/// A recording hook with `rejects`, and the receiver of its calls.
fn hook(rejects: Rejects) -> (RecordingTxHook, Receiver<HookCall>) {
    let (calls, rx) = unbounded();
    let hook = RecordingTxHook {
        calls,
        reject_before: rejects.before,
        reject_after: rejects.after,
    };
    (hook, rx)
}

fn rig(
    signer: &PrivateKeySigner,
) -> ExecRig<StaticSnapshotSource, ImmediateCommit, RecordingQueue> {
    ExecRig::recording(StaticSnapshotSource(funded(signer, 0)), ImmediateCommit).0
}

fn receipt_count(rx_e2c: &Receiver<ExecToCommit>) -> usize {
    rx_e2c
        .try_iter()
        .filter(|m| matches!(m, ExecToCommit::Receipt(_)))
        .count()
}

const fn applied(idx: u64, write_set: bool) -> HookCall {
    HookCall::Applied { idx, write_set }
}

#[test]
fn streaming_mode_reports_each_tx_with_its_write_set() {
    let signer = PrivateKeySigner::random();
    let (hook, calls) = hook(Rejects::default());
    let (h, _rx_e2c) = rig(&signer).tx_hook(hook).spawn(two_tx_block(&signer));
    h.join().expect("no panic").expect("exec ok");

    assert_eq!(
        calls.try_iter().collect::<Vec<_>>(),
        vec![
            HookCall::Before(0),
            applied(0, true),
            HookCall::Before(1),
            applied(1, true),
        ]
    );
}

/// `before` runs at arrival. `after` runs at the boundary, once the
/// strategy returns, with no per-tx write set.
#[test]
fn whole_block_mode_reports_each_tx_at_the_boundary() {
    let signer = PrivateKeySigner::random();
    let (hook, calls) = hook(Rejects::default());
    let (h, rx_e2c) = rig(&signer)
        .tx_hook(hook)
        .block_exec(SequentialBlockExec)
        .spawn(two_tx_block(&signer));
    h.join().expect("no panic").expect("exec ok");

    assert_eq!(
        calls.try_iter().collect::<Vec<_>>(),
        vec![
            HookCall::Before(0),
            HookCall::Before(1),
            applied(0, false),
            applied(1, false),
        ]
    );
    assert_eq!(receipt_count(&rx_e2c), 2);
}

/// A rejected record stops the pipeline at arrival. It never executes, so
/// no receipt reaches the commit thread and `after` does not run.
#[test]
fn before_rejection_stops_the_pipeline_before_execution() {
    let signer = PrivateKeySigner::random();
    let (hook, calls) = hook(Rejects {
        before: Some(1),
        ..Rejects::default()
    });
    let (h, rx_e2c) = rig(&signer).tx_hook(hook).spawn(two_tx_block(&signer));
    let res = h.join().expect("no panic");

    assert!(matches!(res, Err(ExecutorError::RecordIdentity(_))));
    assert_eq!(
        calls.try_iter().collect::<Vec<_>>(),
        vec![HookCall::Before(0), applied(0, true), HookCall::Before(1)]
    );
    assert_eq!(receipt_count(&rx_e2c), 1, "only the accepted tx streams");
}

/// A rejected result stops the pipeline before its receipt streams.
#[test]
fn after_rejection_stops_the_pipeline_before_the_receipt_streams() {
    let signer = PrivateKeySigner::random();
    let (hook, calls) = hook(Rejects {
        after: Some(0),
        ..Rejects::default()
    });
    let (h, rx_e2c) = rig(&signer).tx_hook(hook).spawn(two_tx_block(&signer));
    let res = h.join().expect("no panic");

    assert!(matches!(res, Err(ExecutorError::RecordIdentity(_))));
    assert_eq!(
        calls.try_iter().collect::<Vec<_>>(),
        vec![HookCall::Before(0), applied(0, true)]
    );
    assert_eq!(receipt_count(&rx_e2c), 0);
}

/// On the whole-block path, a rejected result stops the pipeline at the
/// boundary, after the earlier records' receipts and before the block
/// commits.
#[test]
fn after_rejection_in_whole_block_mode_stops_at_the_boundary() {
    let signer = PrivateKeySigner::random();
    let (hook, calls) = hook(Rejects {
        after: Some(1),
        ..Rejects::default()
    });
    let (h, rx_e2c) = rig(&signer)
        .tx_hook(hook)
        .block_exec(SequentialBlockExec)
        .spawn(two_tx_block(&signer));
    let res = h.join().expect("no panic");

    assert!(matches!(res, Err(ExecutorError::RecordIdentity(_))));
    assert_eq!(
        calls.try_iter().collect::<Vec<_>>(),
        vec![
            HookCall::Before(0),
            HookCall::Before(1),
            applied(0, false),
            applied(1, false),
        ]
    );
    let boundaries = rx_e2c
        .try_iter()
        .filter(|m| matches!(m, ExecToCommit::Boundary(_)))
        .count();
    assert_eq!(boundaries, 0, "the block never commits");
}

/// A strategy must return one receipt per record. Any other count stops
/// the pipeline.
#[test]
fn a_strategy_receipt_count_mismatch_stops_the_pipeline() {
    let signer = PrivateKeySigner::random();
    let (h, _rx_e2c) = rig(&signer)
        .block_exec(EmptyBlockExec)
        .spawn(two_tx_block(&signer));
    let res = h.join().expect("no panic");
    assert!(matches!(res, Err(ExecutorError::State(_))));
}

/// Whole-block records count toward `TX_APPLIED_TOTAL`, like streaming
/// records do.
#[test]
fn whole_block_mode_counts_applied_records() {
    let signer = PrivateKeySigner::random();
    let recorder = AppliedOkRecorder::default();
    let (res, _rx_e2c) = metrics::with_local_recorder(&recorder, || {
        rig(&signer)
            .block_exec(SequentialBlockExec)
            .run_here(two_tx_block(&signer))
    });
    res.expect("exec ok");
    assert_eq!(recorder.count.load(Ordering::Relaxed), 2);
}

/// A pair runs its first hook, then its second. When the first rejects,
/// the second does not run.
#[test]
fn a_pair_runs_in_order_and_stops_at_the_first_rejection() {
    let signer = PrivateKeySigner::random();
    let envelope = legacy(&signer, signer.address(), 0, 0);
    let tx = TxContext {
        block: 1,
        tx_idx: TxIndex(7),
        position: pos(7),
        envelope: &envelope,
    };
    let (first, first_calls) = hook(Rejects {
        before: Some(7),
        ..Rejects::default()
    });
    let (second, second_calls) = hook(Rejects::default());
    let mut pair = (first, second);

    assert!(pair.before(&tx).is_err());
    assert_eq!(
        first_calls.try_iter().collect::<Vec<_>>(),
        vec![HookCall::Before(7)]
    );
    assert_eq!(second_calls.try_iter().count(), 0);
}
