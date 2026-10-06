//! The payload arms: `on_tx`, `on_deposit`, `on_xchain`, and the helpers
//! they share.

use std::time::{Duration, Instant};

use kardamom_types::xchain::XChainMessage;
use kardamom_types::{BPosition, Deposit, SnapshotSource, TxEnvelope, TxRef};

use crate::block_env::ExecEnv;
use crate::delta::{PendingDelta, WriteSet};
use crate::error::ExecutorError;
use crate::exec_types::TxIndex;
use crate::executor::execute_xchain_tx;
use kardamom_exec_core::exec_types::TxSlot;
use kardamom_exec_core::executor::XChainDelivery;

use super::exec_block::{BlockRun, Streaming};
use super::exec_thread::{ExecState, Flow};
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
            fees: self.cursor.fees,
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

    /// Post-execution bookkeeping, shared by the Tx and Deposit arms. It
    /// does all of the following:
    /// - updates the ok/error counters
    /// - surfaces the error, if any
    /// - advances the cumulative gas and the per-block index
    /// - folds the write set into the live delta
    /// - accounts for elapsed time
    /// - streams the receipt to the commit thread
    fn record_applied(
        &mut self,
        what: &'static str,
        position: BPosition,
        result: Result<(kardamom_types::Receipt, WriteSet), ExecutorError>,
        apply_start: Instant,
    ) -> Result<Flow, ExecutorError> {
        if result.is_ok() {
            self.metrics.applied_ok.increment(1);
        } else {
            self.metrics.applied_error.increment(1);
        }
        if let Err(ref e) = result {
            tracing::error!(block = self.cursor.block, ?position, error = ?e, "exec ERROR: {what} failed");
        }
        let (receipt, ws) = result?;
        self.block.cumulative_gas_used = receipt.cumulative_gas_used;
        self.block.tx_index += 1;
        let accounts = ws.account_rows();
        self.block.delta.apply(ws);
        *self.block.apply_elapsed.get_or_insert(Duration::ZERO) += apply_start.elapsed();
        self.block.receipts.push(receipt.clone());
        let item = kardamom_types::ReceiptRows { receipt, accounts };
        if self
            .io
            .tx
            .send(ExecToCommit::Receipt(Box::new(item)))
            .is_err()
        {
            return Ok(Flow::Stop);
        }
        Ok(Flow::Continue)
    }

    pub(super) fn on_tx(
        &mut self,
        envelope: TxEnvelope,
        position: BPosition,
        tx_ref: TxRef,
    ) -> Result<Flow, ExecutorError> {
        let tx_idx = self.next_idx()?;
        // One check point for both execution modes. The code checks this at
        // arrival, before the streaming or whole-block branch. So a forged
        // envelope cannot execute now, and cannot hide in the block buffer.
        if self.cfg.verify_record_identity
            && let Err(e) = crate::stateless::verify_record_identity(&envelope)
        {
            tracing::error!(block = self.cursor.block, ?position, ?tx_idx, error = ?e, "exec ERROR: record identity forged");
            return Err(e);
        }
        self.block.refs.push(tx_ref);
        let env = self.exec_env(self.cursor.block);
        let Streaming { scope, shadow } = match &mut self.block.run {
            // Whole-block strategy: defer to the boundary, so batches can
            // execute concurrently.
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
        self.record_applied("execute_tx", position, result, apply_start)
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
        self.record_applied("execute_deposit_tx", position, result, apply_start)
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
        self.record_applied("execute_xchain_tx", position, result, apply_start)
    }
}
