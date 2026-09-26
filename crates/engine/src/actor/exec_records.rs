//! The payload arms: `on_tx`, `on_deposit`, `on_xchain`, and the helpers
//! they share.

use std::time::{Duration, Instant};

use kardamom_types::xchain::XChainMessage;
use kardamom_types::{BPosition, Deposit, SnapshotSource, TxEnvelope};

use crate::block_env::ExecEnv;
use crate::delta::{PendingDelta, WriteSet};
use crate::error::ExecutorError;
use crate::exec_types::TxIndex;
use crate::executor::execute_xchain_tx;
use kardamom_exec_core::exec_types::TxSlot;
use kardamom_exec_core::executor::XChainDelivery;

use super::exec_block::{BlockRun, Streaming};
use super::exec_thread::{ExecState, Flow};
use super::tx_hook::{TxContext, TxHook, TxOutcome};
use super::types::{BufferedRecord, ExecToCommit};
use super::wiring::{ExecPorts, SnapshotDb};

impl<W: ExecPorts> ExecState<W> {
    /// Allot the next absolute record index. Every arm calls this once per
    /// record, in arrival order, so the counter is the canonical record
    /// count the boundary check compares against.
    ///
    /// # Errors
    ///
    /// Returns `Err` if the counter would overflow `u64`.
    pub(super) fn next_idx(&mut self) -> Result<TxIndex, ExecutorError> {
        let idx = self.cursor.next_tx_idx;
        self.cursor.next_tx_idx = idx.next()?;
        Ok(idx)
    }

    /// The block env. Every execution path derives its EVM environment
    /// from this: the streaming tx path, the deposit path, and the
    /// whole-block strategy.
    pub(super) fn exec_env(&self, block_number: u64) -> ExecEnv {
        ExecEnv {
            chain_id: self.cfg.chain_id.get(),
            block_number,
            l2_timestamp: self.cursor.l2_ts,
        }
    }

    /// Get the block's execution scope, building it lazily on first use.
    /// The scope is `None` at a block's start. The first Tx or Deposit
    /// builds it from the parent layer and the live delta, so later records
    /// in the block reuse one EVM and one commit-into cache.
    ///
    /// This takes `scope` and the read-side fields as separate borrows,
    /// not `&mut self`, so the caller keeps disjoint access to its other
    /// fields (for example `bal`, `tx_index`) while the returned scope
    /// stays borrowed.
    fn scope_or_init<'a>(
        scope: &'a mut Option<crate::executor::Executor<SnapshotDb<W>>>,
        snapshots: &W::Snapshots,
        parent: Option<&PendingDelta>,
        delta: &PendingDelta,
        current_block: u64,
        env: ExecEnv,
    ) -> Result<&'a mut crate::executor::Executor<SnapshotDb<W>>, ExecutorError> {
        if let Some(sc) = scope {
            Ok(sc)
        } else {
            let mut sc = crate::executor::Executor::new(
                snapshots.snapshot_after(current_block.saturating_sub(1)),
                parent,
                env,
            )?;
            sc.seed_layer(delta)?;
            Ok(scope.insert(sc))
        }
    }

    /// Streaming-path bookkeeping, shared by the Tx, Deposit, and `XChain`
    /// arms: account for the record's elapsed time, then finish it with its
    /// own write set.
    fn record_applied(
        &mut self,
        kind: RecordKind<'_>,
        position: BPosition,
        result: Result<(kardamom_types::Receipt, WriteSet), ExecutorError>,
        apply_start: Instant,
    ) -> Result<Flow, ExecutorError> {
        *self.block.apply_elapsed.get_or_insert(Duration::ZERO) += apply_start.elapsed();
        self.finish_record(Finished {
            kind,
            position,
            result: result.map(|(receipt, ws)| (receipt, Writes::Record(ws))),
        })
    }

    /// The one result path for every executed record, in both execution
    /// modes. It does all of the following:
    /// - updates the ok/error counters, and surfaces the error, if any
    /// - runs the tx hook's `after`, for a tx record
    /// - advances the cumulative gas and the per-block index
    /// - folds a streaming write set into the live delta
    /// - streams the receipt to the commit thread
    pub(super) fn finish_record(&mut self, finished: Finished<'_>) -> Result<Flow, ExecutorError> {
        let Finished {
            kind,
            position,
            result,
        } = finished;
        self.observe(&kind, position, &result)?;
        let (receipt, writes) = result?;
        self.block.cumulative_gas_used = receipt.cumulative_gas_used;
        self.block.tx_index += 1;
        let accounts = match writes {
            Writes::Record(ws) => {
                let rows = ws.account_rows();
                self.block.delta.apply(ws);
                rows
            }
            Writes::Rows(rows) => rows,
        };
        self.block.receipts.push(receipt.clone());
        let item = kardamom_types::ReceiptRows { receipt, accounts };
        match self.io.tx.send(ExecToCommit::Receipt(Box::new(item))) {
            Ok(()) => Ok(Flow::Continue),
            Err(_) => Ok(Flow::Stop),
        }
    }

    /// Count the record's outcome, log a failure, and run the tx hook's
    /// `after` for a tx record. The execution error wins over a hook
    /// error: for a failed record, this returns `Ok`, and the caller
    /// returns the execution error.
    fn observe(
        &mut self,
        kind: &RecordKind<'_>,
        position: BPosition,
        result: &Result<(kardamom_types::Receipt, Writes), ExecutorError>,
    ) -> Result<(), ExecutorError> {
        match result {
            Ok(_) => self.metrics.applied_ok.increment(1),
            Err(e) => {
                self.metrics.applied_error.increment(1);
                tracing::error!(block = self.cursor.block, ?position, error = ?e, "exec ERROR: {} failed", kind.label());
            }
        }
        let RecordKind::Tx(tx) = kind else {
            return Ok(());
        };
        match result {
            Ok((receipt, writes)) => self.observers.tx_hook.after(
                tx,
                TxOutcome::Applied {
                    receipt,
                    write_set: writes.write_set(),
                },
            ),
            Err(e) => {
                let _ = self.observers.tx_hook.after(tx, TxOutcome::Failed(e));
                Ok(())
            }
        }
    }

    pub(super) fn on_tx(
        &mut self,
        envelope: TxEnvelope,
        position: BPosition,
    ) -> Result<Flow, ExecutorError> {
        let tx_idx = self.next_idx()?;
        // The hook runs at arrival, before the streaming or whole-block
        // branch, so one check point covers both execution modes.
        let tx = TxContext {
            block: self.cursor.block,
            tx_idx,
            position,
            envelope: &envelope,
        };
        self.observers.tx_hook.before(&tx)?;
        let env = self.exec_env(self.cursor.block);
        let Streaming { scope, shadow } = match &mut self.block.run {
            // Whole-block strategy: defer to the boundary, so batches can
            // execute concurrently. The boundary finishes the record.
            BlockRun::Whole(whole) => {
                return Ok(whole.defer(BufferedRecord::Tx {
                    tx_idx,
                    envelope,
                    position,
                }));
            }
            BlockRun::Streaming(streaming) => &mut **streaming,
        };
        let apply_start = Instant::now();
        let sc = Self::scope_or_init(
            scope,
            &self.io.snapshots,
            self.commits.parent.as_ref(),
            &self.block.delta,
            self.cursor.block,
            env,
        )?;
        // Shadow read capture: build a default TouchSet only when the
        // shadow is on. The None path costs nothing.
        let mut touches = shadow
            .as_ref()
            .map(|_| crate::executor::TouchSet::default());
        let slot = TxSlot {
            tx_idx,
            tx_position: position,
            tx_index_in_block: self.block.tx_index,
            cumulative_gas_used_before: self.block.cumulative_gas_used,
        };
        let result = sc.execute_tx(
            slot,
            &envelope,
            self.block
                .bal
                .as_mut()
                .map(|capture| capture.slot(self.block.tx_index)),
            touches.as_mut(),
        );
        // These fire only for a successful tx. On error, the `if let Ok`
        // guards skip them, and `record_applied` below returns the error.
        // The shadow capture runs before `record_applied` consumes the
        // `WriteSet`.
        if let (Some(capture), Ok((_, ws))) = (&self.block.bal, &result) {
            capture.log_progress(self.cursor.block, self.block.tx_index, ws);
        }
        if let (Some(shadow), Some(touches), Ok((receipt, ws))) = (shadow, touches, &result) {
            shadow.capture(&envelope, touches, receipt, ws);
        }
        self.record_applied(RecordKind::Tx(tx), position, result, apply_start)
    }

    /// A deposit has no wire position of its own: its slot index is its
    /// position, matching the offline replay's `global_pos` assignment.
    pub(super) fn on_deposit(&mut self, deposit: Deposit) -> Result<Flow, ExecutorError> {
        let tx_idx = self.next_idx()?;
        let position = BPosition::from_index(tx_idx.0);
        let env = self.exec_env(self.cursor.block);
        let Streaming { scope, shadow } = match &mut self.block.run {
            BlockRun::Whole(whole) => {
                return Ok(whole.defer(BufferedRecord::Deposit {
                    tx_idx,
                    deposit,
                    position,
                }));
            }
            BlockRun::Streaming(streaming) => &mut **streaming,
        };
        let apply_start = Instant::now();
        // Deposits run ON the block scope (same lazy init as `on_tx`): the
        // mint and the inner call commit into the block cache, so later
        // txs observe them with no fold-back layer.
        let sc = Self::scope_or_init(
            scope,
            &self.io.snapshots,
            self.commits.parent.as_ref(),
            &self.block.delta,
            self.cursor.block,
            env,
        )?;
        let slot = TxSlot {
            tx_idx,
            tx_position: position,
            tx_index_in_block: self.block.tx_index,
            cumulative_gas_used_before: self.block.cumulative_gas_used,
        };
        let result = sc.execute_deposit(
            slot,
            &deposit,
            self.block
                .bal
                .as_mut()
                .map(|capture| capture.slot(self.block.tx_index)),
        );
        if let (Some(shadow), Ok(_)) = (shadow, &result) {
            shadow.count_serial();
        }
        self.record_applied(RecordKind::Deposit, position, result, apply_start)
    }

    /// A cross-chain message takes its slot index as its position, like a
    /// deposit.
    pub(super) fn on_xchain(
        &mut self,
        origin_chain_id: u64,
        message: Box<XChainMessage>,
    ) -> Result<Flow, ExecutorError> {
        let tx_idx = self.next_idx()?;
        let position = BPosition::from_index(tx_idx.0);
        let env = self.exec_env(self.cursor.block);
        let scope = match &mut self.block.run {
            // Whole-block execution (the validator's parallel path): buffer
            // like a deposit — the strategy replays the block's records in
            // canonical order at the boundary, dispatching this arm through
            // the SAME `execute_xchain_tx` the streaming path uses.
            BlockRun::Whole(whole) => {
                return Ok(whole.defer(BufferedRecord::XChain {
                    tx_idx,
                    origin_chain_id,
                    message,
                    position,
                }));
            }
            BlockRun::Streaming(streaming) => &mut streaming.scope,
        };
        let apply_start = Instant::now();
        let slot = TxSlot {
            tx_idx,
            tx_position: position,
            tx_index_in_block: self.block.tx_index,
            cumulative_gas_used_before: self.block.cumulative_gas_used,
        };
        let result = execute_xchain_tx(
            &self.commits.snapshot,
            self.commits.parent.as_ref(),
            &self.block.delta,
            env,
            slot,
            XChainDelivery {
                origin_chain_id,
                message: &message,
            },
            self.block
                .bal
                .as_mut()
                .map(|capture| capture.slot(self.block.tx_index)),
        );
        // Like deposits, the delivery runs outside the scope (own commit
        // semantics) — fold its writes into the block cache so later txs
        // in this block observe them.
        if let (Some(sc), Ok((_, ws))) = (scope.as_mut(), &result) {
            let mut layer = PendingDelta::new();
            layer.apply(ws.clone());
            sc.seed_layer(&layer)?;
        }
        self.record_applied(RecordKind::XChain, position, result, apply_start)
    }
}

/// Which kind of record finished: the log label, and the tx hook's
/// context for a tx record.
pub(super) enum RecordKind<'a> {
    Tx(TxContext<'a>),
    Deposit,
    XChain,
}

impl<'a> RecordKind<'a> {
    /// The kind of a record the whole-block strategy executed, and its
    /// position.
    pub(super) fn of_buffered(rec: &'a BufferedRecord, block: u64) -> (Self, BPosition) {
        match rec {
            BufferedRecord::Tx {
                tx_idx,
                envelope,
                position,
            } => (
                Self::Tx(TxContext {
                    block,
                    tx_idx: *tx_idx,
                    position: *position,
                    envelope,
                }),
                *position,
            ),
            BufferedRecord::Deposit { position, .. } => (Self::Deposit, *position),
            BufferedRecord::XChain { position, .. } => (Self::XChain, *position),
        }
    }

    fn label(&self) -> &'static str {
        match self {
            Self::Tx(_) => "execute_tx",
            Self::Deposit => "execute_deposit_tx",
            Self::XChain => "execute_xchain_tx",
        }
    }
}

/// Where a finished record's writes go.
#[allow(clippy::large_enum_variant)] // A short-lived stack value; a box costs one allocation per tx.
pub(super) enum Writes {
    /// Streaming: the record's own write set. It folds into the block's
    /// delta, and its rows ride its receipt.
    Record(WriteSet),
    /// Whole-block: the strategy already built the block's delta. These
    /// rows ride the receipt.
    Rows(Vec<kardamom_types::AccountRow>),
}

impl Writes {
    fn write_set(&self) -> Option<&WriteSet> {
        match self {
            Self::Record(ws) => Some(ws),
            Self::Rows(_) => None,
        }
    }
}

/// One executed record, ready for [`ExecState::finish_record`].
pub(super) struct Finished<'a> {
    pub(super) kind: RecordKind<'a>,
    pub(super) position: BPosition,
    pub(super) result: Result<(kardamom_types::Receipt, Writes), ExecutorError>,
}
