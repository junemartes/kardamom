//! Test doubles for the exec thread's role seams: a recording tx hook, two
//! whole-block strategies, and a metrics recorder that counts applied
//! records.

use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use crossbeam_channel::Sender;
use kardamom_types::StateDatabase;
use metrics::{Counter, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit};

use crate::block_env::ExecEnv;
use crate::delta::PendingDelta;
use crate::error::ExecutorError;

use super::{BlockExecOutput, BlockExecStrategy, BufferedRecord, TxContext, TxHook, TxOutcome};

/// One [`RecordingTxHook`] call, keyed by the record's absolute index.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum HookCall {
    Before(u64),
    /// `write_set` is true when the outcome carries the tx's own writes.
    Applied {
        idx: u64,
        write_set: bool,
    },
    Failed(u64),
}

/// The tx hook every `ExecRig` names. It sends each call to `calls`. Its
/// `before` rejects the record at index `reject_before`, and its `after`
/// rejects the one at `reject_after`.
pub(crate) struct RecordingTxHook {
    pub(crate) calls: Sender<HookCall>,
    pub(crate) reject_before: Option<u64>,
    pub(crate) reject_after: Option<u64>,
}

impl RecordingTxHook {
    fn verdict(idx: u64, reject: Option<u64>) -> Result<(), ExecutorError> {
        match reject {
            Some(r) if r == idx => Err(ExecutorError::RecordIdentity(format!("rejected {idx}"))),
            _ => Ok(()),
        }
    }
}

impl TxHook for RecordingTxHook {
    fn before(&mut self, tx: &TxContext<'_>) -> Result<(), ExecutorError> {
        let idx = tx.tx_idx.0;
        self.calls.send(HookCall::Before(idx)).unwrap();
        Self::verdict(idx, self.reject_before)
    }

    fn after(&mut self, tx: &TxContext<'_>, outcome: TxOutcome<'_>) -> Result<(), ExecutorError> {
        let idx = tx.tx_idx.0;
        let call = match outcome {
            TxOutcome::Applied { write_set, .. } => HookCall::Applied {
                idx,
                write_set: write_set.is_some(),
            },
            TxOutcome::Failed(_) => HookCall::Failed(idx),
        };
        self.calls.send(call).unwrap();
        Self::verdict(idx, self.reject_after)
    }
}

/// A whole-block strategy that executes nothing and returns an empty block.
pub(crate) struct EmptyBlockExec;

impl<D> BlockExecStrategy<D> for EmptyBlockExec {
    fn execute_block(
        &self,
        _snapshot: &D,
        _parent: Option<&PendingDelta>,
        _records: &[BufferedRecord],
        _env: ExecEnv,
        _block_number: u64,
    ) -> Result<BlockExecOutput, ExecutorError> {
        Ok(BlockExecOutput {
            receipts: Vec::new(),
            delta: PendingDelta::new(),
            bal: None,
        })
    }
}

/// A whole-block strategy that executes the block's records in order,
/// through the shared sequential driver.
pub(crate) struct SequentialBlockExec;

impl<D: StateDatabase> BlockExecStrategy<D> for SequentialBlockExec {
    fn execute_block(
        &self,
        snapshot: &D,
        parent: Option<&PendingDelta>,
        records: &[BufferedRecord],
        env: ExecEnv,
        _block_number: u64,
    ) -> Result<BlockExecOutput, ExecutorError> {
        crate::stateless::execute_block(snapshot, parent, records, env)
    }
}

/// A metrics recorder that counts `TX_APPLIED_TOTAL` with
/// `outcome = "ok"`, and drops every other metric.
#[derive(Default)]
pub(crate) struct AppliedOkRecorder {
    pub(crate) count: Arc<AtomicU64>,
}

impl Recorder for AppliedOkRecorder {
    fn describe_counter(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

    fn describe_gauge(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

    fn describe_histogram(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

    fn register_counter(&self, key: &Key, _metadata: &Metadata<'_>) -> Counter {
        let applied_ok = key.name() == crate::metrics::TX_APPLIED_TOTAL
            && key
                .labels()
                .any(|l| l.key() == "outcome" && l.value() == "ok");
        if applied_ok {
            Counter::from_arc(Arc::clone(&self.count))
        } else {
            Counter::noop()
        }
    }

    fn register_gauge(&self, _key: &Key, _metadata: &Metadata<'_>) -> Gauge {
        Gauge::noop()
    }

    fn register_histogram(&self, _key: &Key, _metadata: &Metadata<'_>) -> Histogram {
        Histogram::noop()
    }
}
