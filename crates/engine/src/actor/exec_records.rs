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

use super::exec_thread::{ExecState, Flow};
use super::types::{BufferedRecord, ExecToCommit};
use super::wiring::{ExecPorts, SnapshotDb};

impl<W: ExecPorts> ExecState<W> {
    /// Buffer `rec` for the whole-block strategy to replay at the
    /// boundary, instead of executing it now. Returns the `Flow::Continue`
    /// the caller must return immediately, with no further work this call.
    fn defer(&mut self, rec: BufferedRecord) -> Flow {
        self.buffered.push(rec);
        Flow::Continue
    }

    /// Canonical-order check, shared by the Tx, Epoch, Deposit, `RemoteEpoch`,
    /// and `XChain` arms. The record's absolute index must be exactly the
    /// next expected index. On a match, the counter advances.
    pub(super) fn check_in_order(
        &mut self,
        kind: &'static str,
        tx_idx: TxIndex,
        position: BPosition,
    ) -> Result<(), ExecutorError> {
        if tx_idx != self.expected_tx_idx {
            tracing::error!(
                block = self.current_block,
                ?position,
                ?tx_idx,
                expected_tx_idx = ?self.expected_tx_idx,
                "exec ERROR: OutOfOrderTx ({kind})"
            );
            return Err(ExecutorError::OutOfOrderTx {
                got: tx_idx,
                expected: self.expected_tx_idx,
            });
        }
        self.expected_tx_idx = self.expected_tx_idx.next();
        Ok(())
    }

    /// The block env. Every execution path derives its EVM environment
    /// from this: the streaming tx path, the deposit path, and the
    /// whole-block strategy.
    pub(super) fn exec_env(&self, block_number: u64) -> ExecEnv {
        ExecEnv {
            chain_id: self.cfg.chain_id.get(),
            block_number,
            l2_timestamp: self.current_l2_ts,
        }
    }

    /// Get the block's execution scope, building it lazily on first use.
    /// The scope is `None` at a block's start. The first Tx or Deposit
    /// builds it from the parent layer and the live delta, so later records
    /// in the block reuse one EVM and one commit-into cache.
    ///
    /// This takes `scope` and the read-side fields as separate borrows,
    /// not `&mut self`, so the caller keeps disjoint access to its other
    /// fields (for example `bal_tx`, `tx_index_in_block`) while the
    /// returned scope stays borrowed.
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
            self.tx_applied_ok.increment(1);
        } else {
            self.tx_applied_error.increment(1);
        }
        if let Err(ref e) = result {
            tracing::error!(block = self.current_block, ?position, error = ?e, "exec ERROR: {what} failed");
        }
        let (receipt, ws) = result?;
        self.cumulative_gas_used = receipt.cumulative_gas_used;
        self.tx_index_in_block += 1;
        self.delta.apply(ws);
        *self.block_apply_elapsed.get_or_insert(Duration::ZERO) += apply_start.elapsed();
        self.block_receipts.push(receipt.clone());
        if self.tx.send(ExecToCommit::Receipt(receipt)).is_err() {
            return Ok(Flow::Stop);
        }
        Ok(Flow::Continue)
    }

    pub(super) fn on_tx(
        &mut self,
        tx_idx: TxIndex,
        envelope: TxEnvelope,
        position: BPosition,
    ) -> Result<Flow, ExecutorError> {
        self.check_in_order("Tx", tx_idx, position)?;
        // One check point for both execution modes. The code checks this at
        // arrival, before the streaming or whole-block branch. So a forged
        // envelope cannot execute now, and cannot hide in the block buffer.
        if self.cfg.verify_record_identity
            && let Err(e) = crate::stateless::verify_record_identity(&envelope)
        {
            tracing::error!(block = self.current_block, ?position, ?tx_idx, error = ?e, "exec ERROR: record identity forged");
            return Err(e);
        }
        if self.block_exec.is_some() {
            // Whole-block strategy: defer to the boundary, so batches can
            // execute concurrently.
            return Ok(self.defer(BufferedRecord::Tx {
                tx_idx,
                envelope,
                position,
            }));
        }
        let env = self.exec_env(self.current_block);
        let apply_start = Instant::now();
        let sc = Self::scope_or_init(
            &mut self.scope,
            &self.snapshots,
            self.parent.as_ref(),
            &self.delta,
            self.current_block,
            env,
        )?;
        // Shadow read capture: build a default TouchSet only when the
        // shadow is on. The None path costs nothing.
        let mut touches = self
            .shadow_tx
            .as_ref()
            .map(|_| crate::executor::TouchSet::default());
        let slot = TxSlot {
            tx_idx,
            tx_position: position,
            tx_index_in_block: self.tx_index_in_block,
            cumulative_gas_used_before: self.cumulative_gas_used,
        };
        let result = sc.execute_tx(
            slot,
            &envelope,
            self.bal_tx
                .as_ref()
                .map(|_| (&mut self.block_bal, self.tx_index_in_block + 1)),
            touches.as_mut(),
        );
        // This log fires only for a successful tx. On error, the `if let
        // Ok` guard skips it, and `record_applied` below returns the error.
        if let Ok((_, ws)) = &result {
            self.log_bal_progress(ws);
        }
        self.capture_shadow(&envelope, touches.take(), &result);
        self.record_applied("execute_tx", position, result, apply_start)
    }

    /// Log BAL capture progress every 512 txs, when a BAL publisher is
    /// attached.
    fn log_bal_progress(&self, ws: &WriteSet) {
        if self.bal_tx.is_some() && self.tx_index_in_block.is_multiple_of(512) {
            tracing::debug!(
                block = self.current_block,
                tx_index_in_block = self.tx_index_in_block,
                bal_accounts = self.block_bal.accounts.len(),
                ws_accounts = ws.accounts.len(),
                "BAL capture progress"
            );
        }
    }

    /// Capture the shadow data for one tx, when the shadow is on and the
    /// tx applied. Called before `record_applied` consumes the `WriteSet`.
    /// Cloning the envelope is just a refcount increment. Cell extraction
    /// is one pass over the small per-tx sets.
    fn capture_shadow(
        &mut self,
        envelope: &TxEnvelope,
        touches: Option<crate::executor::TouchSet>,
        result: &Result<(kardamom_types::Receipt, WriteSet), ExecutorError>,
    ) {
        if let (Some(t), Ok((receipt, ws))) = (touches, result) {
            self.shadow_captures.push(crate::shadow::ShadowTxCapture {
                envelope: envelope.clone(),
                gas_used: receipt.gas_used,
                touches: t,
                write_cells: crate::shadow::write_cells(ws),
            });
        }
    }

    pub(super) fn on_deposit(
        &mut self,
        tx_idx: TxIndex,
        deposit: Deposit,
        position: BPosition,
    ) -> Result<Flow, ExecutorError> {
        self.check_in_order("Deposit", tx_idx, position)?;
        if self.block_exec.is_some() {
            return Ok(self.defer(BufferedRecord::Deposit {
                tx_idx,
                deposit,
                position,
            }));
        }
        let env = self.exec_env(self.current_block);
        let apply_start = Instant::now();
        // Deposits run ON the block scope (same lazy init as `on_tx`): the
        // mint and the inner call commit into the block cache, so later
        // txs observe them with no fold-back layer.
        let sc = Self::scope_or_init(
            &mut self.scope,
            &self.snapshots,
            self.parent.as_ref(),
            &self.delta,
            self.current_block,
            env,
        )?;
        let slot = TxSlot {
            tx_idx,
            tx_position: position,
            tx_index_in_block: self.tx_index_in_block,
            cumulative_gas_used_before: self.cumulative_gas_used,
        };
        let result = sc.execute_deposit(
            slot,
            &deposit,
            self.bal_tx
                .as_ref()
                .map(|_| (&mut self.block_bal, self.tx_index_in_block + 1)),
        );
        // Shadow: deposits take the serial barrier lane (spec strategy 1).
        // The code counts them; it does not model them.
        if self.shadow_tx.is_some() && result.is_ok() {
            self.shadow_serial += 1;
        }
        self.record_applied("execute_deposit_tx", position, result, apply_start)
    }

    pub(super) fn on_xchain(
        &mut self,
        tx_idx: TxIndex,
        origin_chain_id: u64,
        message: Box<XChainMessage>,
        position: BPosition,
    ) -> Result<Flow, ExecutorError> {
        self.check_in_order("XChain", tx_idx, position)?;
        if self.block_exec.is_some() {
            // Whole-block execution (the validator's parallel path): buffer
            // like a deposit — the strategy replays the block's records in
            // canonical order at the boundary, dispatching this arm through
            // the SAME `execute_xchain_tx` the streaming path uses.
            return Ok(self.defer(BufferedRecord::XChain {
                tx_idx,
                origin_chain_id,
                message,
                position,
            }));
        }
        let env = self.exec_env(self.current_block);
        let apply_start = Instant::now();
        let slot = TxSlot {
            tx_idx,
            tx_position: position,
            tx_index_in_block: self.tx_index_in_block,
            cumulative_gas_used_before: self.cumulative_gas_used,
        };
        let result = execute_xchain_tx(
            &self.snapshot,
            self.parent.as_ref(),
            &self.delta,
            env,
            slot,
            XChainDelivery {
                origin_chain_id,
                message: &message,
            },
            self.bal_tx
                .as_ref()
                .map(|_| (&mut self.block_bal, self.tx_index_in_block + 1)),
        );
        // Like deposits, the delivery runs outside the scope (own commit
        // semantics) — fold its writes into the block cache so later txs
        // in this block observe them.
        if let (Some(sc), Ok((_, ws))) = (self.scope.as_mut(), &result) {
            let mut layer = PendingDelta::new();
            layer.apply(ws.clone());
            sc.seed_layer(&layer)?;
        }
        self.record_applied("execute_xchain_tx", position, result, apply_start)
    }
}
