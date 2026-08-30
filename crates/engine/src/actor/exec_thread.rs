//! This is the sequential execution thread. It reads canonical-ordered
//! records from the reader layer. It executes each record against the
//! snapshot, then the parent layer, then the delta. It sends receipts and
//! durably-settled boundaries to the commit thread.
//!
//! The loop state lives in [`ExecState`]. Each `ReaderToExec` arm is one
//! `on_*` method. The pipelined-commit settle sweep lives in
//! `exec_settle.rs`. The idle probe and the boundary arm both use it.

use std::collections::VecDeque;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver, Sender};

use kardamom_types::{
    BPosition, BlockBoundary, BlockBoundaryStart, BlockDelta, Deposit, EpochRecord, SnapshotSource,
    StateDatabase, TxEnvelope,
};

use crate::block_env::ExecEnv;
use crate::delta::{PendingDelta, WriteSet};
use crate::error::ExecutorError;
use crate::exec_types::TxIndex;
use crate::executor::execute_deposit_tx;
use crate::reader::{EpochObserver, ReaderToExec};

use super::ports::{StateWriterQueue, StateWriterSignal};
use super::types::{
    BalHandoff, BlockExec, BufferedRecord, ExecToCommit, ExecutorConfig, ResumePoint,
};

/// How long an IDLE exec thread waits before it probes the writer for
/// settled in-flight commits.
///
/// Settling used to happen only at the next boundary. This works under
/// steady load, when a boundary arrives every tick. But on an idle tail,
/// the last blocks never publish their boundary closeouts, and downstream
/// consumers stall.
///
/// Under load the timeout never fires, because records arrive faster.
/// When idle, the probe is a cheap, non-blocking read.
const IDLE_SETTLE_PROBE: Duration = Duration::from_millis(25);

/// Outcome of one receive attempt on the reader channel. See
/// [`ExecState::recv_next`].
enum Recv {
    Msg(ReaderToExec),
    IdleProbe,
    Closed,
}

/// Control-flow result of a message handler. Continue the loop, or stop
/// cleanly. Stop means the commit channel's receiver is gone: this is
/// shutdown, not an error.
pub(super) enum Flow {
    Continue,
    Stop,
}

/// The exec thread's mutable loop state. Each `spawn_exec` thread has one
/// instance. Each `ReaderToExec` arm is now a method, instead of inline code.
pub(super) struct ExecState<S: SnapshotSource, Q, P, E> {
    pub(super) cfg: ExecutorConfig,
    pub(super) rx: Receiver<ReaderToExec>,
    pub(super) tx: Sender<ExecToCommit>,
    pub(super) snapshots: S,
    pub(super) sw_signal: Q,
    pub(super) sw_queue: P,
    pub(super) bal_tx: Option<Sender<BalHandoff>>,
    /// Footprint shadow handoff (`crate::shadow`), one per block. Only the
    /// executor role uses this; it is `None` elsewhere. The whole-block
    /// (validator) path ignores it: captures use the streaming arm instead.
    pub(super) shadow_tx: Option<Sender<crate::shadow::ShadowBlock>>,
    /// Shadow tx captures and the serial-lane count for the current block.
    /// The code hands these off with `try_send` at each boundary; this never
    /// blocks. Both stay empty when the shadow is off.
    pub(super) shadow_captures: Vec<crate::shadow::ShadowTxCapture>,
    pub(super) shadow_serial: u32,
    pub(super) block_exec: Option<BlockExec<S::Db>>,
    /// A role-specific epoch check, statically dispatched. See
    /// [`crate::reader::EpochObserver`]. `None` means the code trusts the
    /// ordered stream.
    pub(super) epoch_observer: Option<E>,
    /// The snapshot source returns owned snapshots keyed by block number:
    /// the block just committed. This is the [`ResumePoint`]'s `block` field
    /// (0 on a fresh start).
    pub(super) snapshot: S::Db,
    pub(super) delta: PendingDelta,
    /// EIP-7928 capture: the per-block Bal. The code resets it at each
    /// boundary. It is maintained only when a publisher is attached
    /// (executor role).
    pub(super) block_bal: revm::state::bal::Bal,
    /// Whole-block buffer. Used only when a block-exec strategy is
    /// supplied (the validator parallel path).
    pub(super) buffered: Vec<BufferedRecord>,
    /// Per-block execution scope (streaming path): one EVM and one
    /// commit-into cache for the whole block. Building these per tx used
    /// about 90% of the allocation in the execution path.
    ///
    /// The code drops the scope at each boundary. It rebuilds the scope
    /// lazily, at the block's first tx. The rebuild seeds it with the
    /// parent and anything already in the live delta, for example deposits
    /// that landed before the first tx.
    pub(super) scope: Option<crate::executor::ExecScope<S::Db>>,
    /// Pipelined commit, at depth K. At each boundary, the code submits the
    /// finalized delta to the writer, but does not wait for it. The next
    /// block executes against the snapshot, then the merged unsettled layer,
    /// then the delta. Completed commits settle opportunistically, through a
    /// non-blocking probe at each boundary. The exec thread parks only when
    /// the writer is a full K blocks behind.
    ///
    /// A single slow fsync no longer touches execution at all. At depth 1 it
    /// still did: a blocking `wait_committed` call was the main source of
    /// receipt delay.
    ///
    /// Durability semantics do not change. A boundary reaches `tx_receipts`
    /// only after its block is durable. Receipts stream out before
    /// durability, at least once: a crash replay re-publishes them, and
    /// they are byte-identical.
    ///
    /// `parent` is the merged union of every unsettled block's writes. A
    /// later block's write wins over an earlier one. This gives one layer
    /// regardless of depth, so per-tx cache seeding stays O(one map). The
    /// code rebuilds `parent` from the survivors when commits settle.
    pub(super) parent: Option<PendingDelta>,
    pub(super) inflight: VecDeque<(BlockBoundary, PendingDelta)>,
    /// Per-block receipts, in arrival order. The code drains this into the
    /// BlockDelta at each boundary, so the writer can persist the receipts
    /// and the tx_hash_index tables. Each tx's receipt is cloned once, to
    /// feed both this list and the streaming tx_receipts publisher. This
    /// clone cost is flagged for saturation validation.
    pub(super) block_receipts: Vec<kardamom_types::Receipt>,
    /// Block-number bookkeeping. Blocks are 1-indexed; genesis is block 0.
    /// The exec thread assumes every block boundary it sees is for the
    /// current in-flight block. It does not re-derive block numbers on its
    /// own; it relies on the sealer.
    pub(super) current_block: u64,
    /// Block N's txs execute with boundary N-1's timestamp. On resume, the
    /// code already consumed that boundary before the restart. So its
    /// persisted value seeds the state. See [`ResumePoint::l2_timestamp`].
    pub(super) current_l2_ts: u64,
    /// Per-block RPC enrichment counters. The code resets these at each
    /// BoundaryStart.
    pub(super) tx_index_in_block: u64,
    pub(super) cumulative_gas_used: u64,
    /// The cumulative count of canonical records (TxRef and DepositRef)
    /// this exec thread has folded into a receipt.
    ///
    /// This is the boundary alignment key. `BlockBoundaryStart.end_tx_idx`
    /// carries the sealer's cumulative count of republished canonical
    /// records, encoded through `BPosition::from_index`. At each boundary,
    /// the two counts must match. `expected_tx_idx` already tracks this
    /// count: it advances once per applied Tx or Deposit, and never resets
    /// across blocks. So the code compares against it directly.
    ///
    /// The key is a count, not an Aeron byte position. A byte position is
    /// per-publication under the canonical-publisher MDC merge, and is
    /// ambiguous between an offer-return frame and a frame-start frame. An
    /// old position-based key broke under load for this reason.
    ///
    /// The counter is seeded at the resume cursor. Boundary counts on the
    /// wire are absolute, and delivery resumes at the cursor, so the
    /// counter must start there too. Starting at 0 made every mid-chain
    /// resume fail at its first boundary.
    pub(super) expected_tx_idx: TxIndex,
    /// Wall time spent executing the block's txs and deposits. This
    /// excludes channel idle time between txs. The BoundaryStart handler
    /// records this value when it closes the block. It is `None` for empty
    /// blocks.
    pub(super) block_apply_elapsed: Option<Duration>,
    /// Pre-resolved counter handles. This loop is the executor's hottest
    /// path, so the code skips the per-event registry lookup.
    pub(super) tx_applied_ok: metrics::Counter,
    pub(super) tx_applied_error: metrics::Counter,
}

impl<S, Q, P, E> ExecState<S, Q, P, E>
where
    S: SnapshotSource + 'static,
    Q: StateWriterSignal + 'static,
    P: StateWriterQueue + 'static,
    E: EpochObserver + 'static,
{
    /// Seed the loop state from the [`ResumePoint`] cursor.
    ///
    /// The canonical stream source delivers records from the persisted
    /// cursor onward (the cluster client's REPLAY_FROM; `reader::cluster`
    /// dedups any records below the cursor). So the exec thread seeds its
    /// absolute counters here, instead of replaying from record 0 and
    /// counting through them. A fresh start uses [`ResumePoint::GENESIS`],
    /// the same seeding with all-zero values.
    #[allow(clippy::too_many_arguments)] // Matches `spawn_exec`'s shape. See the note there.
    fn new(
        cfg: ExecutorConfig,
        rx: Receiver<ReaderToExec>,
        tx: Sender<ExecToCommit>,
        snapshots: S,
        sw_signal: Q,
        sw_queue: P,
        start: ResumePoint,
        bal_tx: Option<Sender<BalHandoff>>,
        shadow_tx: Option<Sender<crate::shadow::ShadowBlock>>,
        block_exec: Option<BlockExec<S::Db>>,
        epoch_observer: Option<E>,
    ) -> Self {
        let snapshot = snapshots.snapshot_after(start.block);
        Self {
            cfg,
            rx,
            tx,
            snapshots,
            sw_signal,
            sw_queue,
            bal_tx,
            shadow_tx,
            shadow_captures: Vec::new(),
            shadow_serial: 0,
            block_exec,
            epoch_observer,
            snapshot,
            delta: PendingDelta::new(),
            block_bal: revm::state::bal::Bal::new(),
            buffered: Vec::new(),
            scope: None,
            parent: None,
            inflight: VecDeque::new(),
            block_receipts: Vec::new(),
            current_block: start.block + 1,
            current_l2_ts: start.l2_timestamp,
            tx_index_in_block: 0,
            cumulative_gas_used: 0,
            expected_tx_idx: TxIndex(start.record_count),
            block_apply_elapsed: None,
            tx_applied_ok: metrics::counter!(crate::metrics::TX_APPLIED_TOTAL, "outcome" => "ok"),
            tx_applied_error: metrics::counter!(crate::metrics::TX_APPLIED_TOTAL, "outcome" => "error"),
        }
    }

    /// The exec thread's main loop. It receives a message, or runs the idle
    /// probe, then dispatches to the matching handler. It stops cleanly when
    /// the commit channel's receiver is gone, or the reader channel closes.
    fn run(&mut self) -> Result<(), ExecutorError> {
        loop {
            let msg = match self.recv_next() {
                Recv::Msg(m) => m,
                Recv::Closed => return self.on_closed(),
                Recv::IdleProbe => match self.on_idle_probe()? {
                    Flow::Continue => continue,
                    Flow::Stop => return Ok(()),
                },
            };
            let flow = match msg {
                ReaderToExec::Tx {
                    tx_idx,
                    envelope,
                    position,
                } => self.on_tx(tx_idx, envelope, position)?,
                ReaderToExec::Epoch {
                    tx_idx,
                    epoch,
                    position,
                } => self.on_epoch(tx_idx, epoch, position)?,
                ReaderToExec::Deposit {
                    tx_idx,
                    deposit,
                    position,
                } => self.on_deposit(tx_idx, deposit, position)?,
                ReaderToExec::Boundary(start) => self.on_boundary(start)?,
            };
            if let Flow::Stop = flow {
                return Ok(());
            }
        }
    }

    /// One receive attempt. With commits in flight, the wait is bounded by
    /// [`IDLE_SETTLE_PROBE`], so an idle tail still settles. With no commits
    /// in flight, the wait blocks indefinitely, because there is nothing to
    /// settle.
    fn recv_next(&self) -> Recv {
        if self.inflight.is_empty() {
            match self.rx.recv() {
                Ok(m) => Recv::Msg(m),
                Err(_) => Recv::Closed,
            }
        } else {
            match self.rx.recv_timeout(IDLE_SETTLE_PROBE) {
                Ok(m) => Recv::Msg(m),
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => Recv::IdleProbe,
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => Recv::Closed,
            }
        }
    }

    /// Canonical-order check, shared by the Tx, Epoch, and Deposit arms. The
    /// record's absolute index must be exactly the next expected index. On
    /// a match, the counter advances.
    fn check_in_order(
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
    fn exec_env(&self, block_number: u64) -> ExecEnv {
        ExecEnv {
            chain_id: self.cfg.chain_id,
            block_number,
            l2_timestamp: self.current_l2_ts,
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

    fn on_tx(
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
            self.buffered.push(BufferedRecord::Tx {
                tx_idx,
                envelope,
                position,
            });
            return Ok(Flow::Continue);
        }
        let env = self.exec_env(self.current_block);
        let apply_start = Instant::now();
        let sc = match self.scope.as_mut() {
            Some(sc) => sc,
            None => {
                let mut sc = crate::executor::ExecScope::new(
                    self.snapshots
                        .snapshot_after(self.current_block.saturating_sub(1)),
                    self.parent.as_ref(),
                    env,
                )?;
                sc.seed_layer(&self.delta)?;
                self.scope.insert(sc)
            }
        };
        // Shadow read capture: build a default TouchSet only when the
        // shadow is on. The None path costs nothing.
        let mut touches = self
            .shadow_tx
            .as_ref()
            .map(|_| crate::executor::TouchSet::default());
        let result = sc.execute_tx(
            tx_idx,
            position,
            &envelope,
            self.tx_index_in_block,
            self.cumulative_gas_used,
            self.bal_tx
                .as_ref()
                .map(|_| (&mut self.block_bal, self.tx_index_in_block + 1)),
            touches.as_mut(),
        );
        // This log fires only for a successful tx. On error, the `if let
        // Ok` guard skips it, and `record_applied` below returns the error.
        if let Ok((_, ws)) = &result
            && self.bal_tx.is_some()
            && self.tx_index_in_block.is_multiple_of(512)
        {
            tracing::debug!(
                block = self.current_block,
                tx_index_in_block = self.tx_index_in_block,
                bal_accounts = self.block_bal.accounts.len(),
                ws_accounts = ws.accounts.len(),
                "BAL capture progress"
            );
        }
        // Capture the shadow data before `record_applied` consumes the
        // WriteSet. Cloning the envelope is just a refcount increment. Cell
        // extraction is one pass over the small per-tx sets.
        if let (Some(t), Ok((receipt, ws))) = (touches.take(), &result) {
            self.shadow_captures.push(crate::shadow::ShadowTxCapture {
                envelope: envelope.clone(),
                gas_used: receipt.gas_used,
                touches: t,
                write_cells: crate::shadow::write_cells(ws),
            });
        }
        self.record_applied("execute_tx", position, result, apply_start)
    }

    fn on_epoch(
        &mut self,
        tx_idx: TxIndex,
        epoch: EpochRecord,
        position: BPosition,
    ) -> Result<Flow, ExecutorError> {
        // The epoch marker consumes one slot and applies no tx. It exists so
        // the L1 origin advances at a point every replica agrees on, and so
        // the deposits that follow start at a slot the sealer also reserved.
        self.check_in_order("Epoch", tx_idx, position)?;
        // Check this before the epoch's deposits apply, so a rejected epoch
        // fail-stops instead of committing.
        if let Some(obs) = self.epoch_observer.as_mut() {
            obs.observe(&epoch)?;
        }
        tracing::debug!(
            target: "kardamom_executor::exec",
            block = self.current_block,
            l1_number = epoch.l1_number,
            deposits = epoch.deposits.len(),
            "epoch marker: L1 origin advances"
        );
        Ok(Flow::Continue)
    }

    fn on_deposit(
        &mut self,
        tx_idx: TxIndex,
        deposit: Deposit,
        position: BPosition,
    ) -> Result<Flow, ExecutorError> {
        self.check_in_order("Deposit", tx_idx, position)?;
        if self.block_exec.is_some() {
            self.buffered.push(BufferedRecord::Deposit {
                tx_idx,
                deposit,
                position,
            });
            return Ok(Flow::Continue);
        }
        let env = self.exec_env(self.current_block);
        let apply_start = Instant::now();
        let result = execute_deposit_tx(
            &self.snapshot,
            self.parent.as_ref(),
            &self.delta,
            env,
            tx_idx,
            position,
            &deposit,
            self.tx_index_in_block,
            self.cumulative_gas_used,
            self.bal_tx
                .as_ref()
                .map(|_| (&mut self.block_bal, self.tx_index_in_block + 1)),
        );
        // Deposits run outside the scope; they are rare and have their own
        // commit semantics. Fold their writes into the block cache, so
        // later txs in this block can see them.
        if let (Some(sc), Ok((_, ws))) = (self.scope.as_mut(), &result) {
            let mut layer = PendingDelta::new();
            layer.apply(ws.clone());
            sc.seed_layer(&layer)?;
        }
        // Shadow: deposits take the serial barrier lane (spec strategy 1).
        // The code counts them; it does not model them.
        if self.shadow_tx.is_some() && result.is_ok() {
            self.shadow_serial += 1;
        }
        self.record_applied("execute_deposit_tx", position, result, apply_start)
    }

    /// Run the block-close protocol actions for the block being sealed.
    ///
    /// This supplies the two state layers that `exec-core` cannot see on its
    /// own: the merged unsettled-parent delta, then the mdbx snapshot. The
    /// composed read checks `delta`, then `parent`, then `snapshot`, in that
    /// order, matching what the EVM sees through `seed_cache_layer`. Reading
    /// the snapshot alone would miss the current block's writes and up to K
    /// unsettled blocks. For a
    /// flag read, that would activate a feature late, or never, on a busy
    /// chain, while a quieter replica activates it on time. This is a
    /// divergence.
    fn apply_block_close_actions(
        &mut self,
        block_number: u64,
        header_ts_ms: u64,
    ) -> Result<(), ExecutorError> {
        // Destructure by field: `delta` is borrowed mutably, while the
        // closure reads `parent` and `snapshot`.
        let Self {
            delta,
            parent,
            snapshot,
            ..
        } = self;

        let outcome = kardamom_exec_core::features::apply_block_close_actions(
            delta,
            block_number,
            header_ts_ms,
            |addr, slot| {
                if let Some(v) = parent.as_ref().and_then(|p| p.storage.get(&(addr, slot))) {
                    return Ok(*v);
                }
                snapshot.storage(addr, slot).map_err(|e| {
                    ExecutorError::State(format!("block-close read {addr}/{slot}: {e:?}"))
                })
            },
        )?;

        if let Some(beat) = outcome.health_beat {
            metrics::counter!(crate::metrics::HEALTH_BEACON_BEATS_TOTAL).increment(1);
            tracing::info!(
                block_number,
                beat,
                l2_timestamp_ms = header_ts_ms,
                "health beacon"
            );
        }
        Ok(())
    }

    fn on_boundary(&mut self, start: BlockBoundaryStart) -> Result<Flow, ExecutorError> {
        let BlockBoundaryStart {
            block_number,
            end_tx_idx,
            l2_timestamp,
            l1_origin,
        } = start;
        if let Flow::Stop = self.settle_at_boundary()? {
            return Ok(Flow::Stop);
        }
        // Alignment check. `BlockBoundaryStart.end_tx_idx` carries the
        // sealer's cumulative count of canonical records (TxRef and
        // DepositRef) republished through the end of this block, encoded
        // through `BPosition::from_index`. The executor must have applied
        // exactly that many records. `expected_tx_idx` tracks this count: it
        // advances once per applied Tx or Deposit, and never resets. The two
        // values must match.
        //
        // A mismatch means the executor's view of the canonical stream
        // diverged from the sealer's: a lost, extra, or reordered record.
        // This is fatal. Return the error so the process crash-loops,
        // instead of committing a wrong block.
        let want = end_tx_idx.as_index();
        let have = self.expected_tx_idx.0;
        if want != have {
            tracing::error!(
                block = block_number,
                want_count = want,
                have_count = have,
                "exec ERROR: BoundaryMisaligned (canonical record count)"
            );
            return Err(ExecutorError::BoundaryMisaligned {
                end: end_tx_idx,
                last_seen: BPosition::from_index(have),
            });
        }

        // Whole-block strategy: execute everything buffered for this block
        // now. The validator's batches run concurrently inside this call.
        // Then feed the receipts and delta into the same boundary path the
        // streaming executor uses. Commit ordering, durability gating, and
        // the write-set cross-check stay the same.
        if let Some(exec_block) = self.block_exec.as_ref() {
            let env = self.exec_env(block_number);
            let apply_start = Instant::now();
            let out = exec_block(
                &self.snapshot,
                self.parent.as_ref(),
                &self.buffered,
                env,
                block_number,
            )?;
            *self.block_apply_elapsed.get_or_insert(Duration::ZERO) += apply_start.elapsed();
            self.buffered.clear();
            self.delta = out.delta;
            // BAL parity across strategies: a capturing strategy hands its
            // folded per-block Bal here, so the boundary handoff below
            // publishes it the same way the streaming capture would. If a
            // publishing role's strategy captured nothing, it would
            // otherwise emit an empty BAL with no warning, and every
            // validator would degrade to the sequential fallback with no
            // signal. So this combination logs a warning.
            match out.bal {
                Some(b) => self.block_bal = b,
                None if self.bal_tx.is_some() => {
                    tracing::warn!(
                        block = block_number,
                        "block-exec strategy returned no BAL while BAL \
                         publication is on; publishing an empty capture"
                    );
                }
                None => {}
            }
            for r in out.receipts {
                self.block_receipts.push(r.clone());
                if self.tx.send(ExecToCommit::Receipt(r)).is_err() {
                    return Ok(Flow::Stop);
                }
            }
        }

        // Record the block's total execution time. Only record this when
        // the block had at least one tx; skip empty blocks.
        if let Some(elapsed) = self.block_apply_elapsed.take() {
            metrics::histogram!(crate::metrics::BLOCK_APPLY_DURATION_SECONDS)
                .record(elapsed.as_secs_f64());
        }

        // Block-close protocol actions (L1-governed feature flags).
        //
        // This call must happen here: after every record of the block has
        // landed in `self.delta` (through the streaming arms above, or the
        // whole-block strategy's fold above), and before the code takes the
        // delta for the writer. This order lets an upgrade deposit activate a
        // feature for the very block that carried it, and puts the actions'
        // writes in the same delta the validator cross-checks.
        //
        // This code runs for every role: the executor, the streaming
        // validator, and the parallel validator. It is engine code. A role
        // that skipped it would diverge on the first active block.
        //
        // `l2_timestamp` is this boundary's own stamp, the block's own header
        // time. It is not the previous boundary's time, which is what the
        // block's txs executed with.
        self.apply_block_close_actions(block_number, l2_timestamp)?;

        // No state-root computation yet. The sealed BlockBoundary on
        // tx_receipts is slim; it carries no commitment. `l1_origin` passes
        // through unchanged from the sealer's marker. It identifies the L1
        // epoch this block belongs to. This is what lets a reconstructor
        // place the epoch's deposits.
        let boundary = BlockBoundary {
            block_number,
            end_tx_idx,
            l2_timestamp,
            l1_origin,
        };

        // Drain the delta. Swap it out so the writer owns it, but keep a
        // clone as the next block's parent read layer. In pipelined commit,
        // cloning the block's write maps costs only a few ms, far less than
        // the fsync it takes off the critical path.
        //
        // The block's receipts ride inside the BlockDelta, in arrival order,
        // so the writer persists them durably. They also streamed out on
        // tx_receipts at execute time, above, ahead of durability. A crash in
        // that window re-executes the block on recovery and re-publishes
        // byte-identical receipts. So tx_receipts is at-least-once, and
        // every consumer must dedup on `tx_idx` (ingress already does).
        let pending = std::mem::take(&mut self.delta);
        // The block's execution scope dies with the block. The next block
        // gets a new parent layer and block env, and, once commits settle, a
        // fresh snapshot. This drop is unconditional: a scope reused across a
        // boundary would execute against the previous block's parent and env.
        self.scope = None;
        // EIP-7928 handoff: move the block's Bal, and a receipts-free copy of
        // the merged delta, to the publisher thread. Encoding and reliable
        // delivery happen entirely off this thread. A dropped send means the
        // publisher is gone mid-shutdown; this is not fatal.
        if let Some(btx) = self.bal_tx.as_ref() {
            let bal_delta = pending.clone().finalize(block_number, Vec::new());
            // Use try_send: the BAL handoff must never block execution. A
            // slow or stuck publisher pump once back-pressured this thread
            // through the bounded channel, and delayed every receipt behind
            // it. A dropped frame costs one block of BAL retention; that
            // block verifies as bal_missing, a tolerated path. A stalled
            // exec thread costs the whole chain.
            match btx.try_send((
                boundary.clone(),
                bal_delta,
                std::mem::take(&mut self.block_bal),
            )) {
                Ok(()) => {}
                Err(crossbeam_channel::TrySendError::Full(_)) => {
                    tracing::warn!(
                        block = block_number,
                        "BAL handoff full; dropping this block's frame \
                         (publisher pump stalled?)"
                    );
                }
                Err(crossbeam_channel::TrySendError::Disconnected(_)) => {
                    // The publisher is gone mid-shutdown; this is not fatal.
                }
            }
        }
        // Footprint-shadow handoff: the same never-block discipline as the
        // BAL handoff. A dropped block costs one block of measurement, which
        // is counted, not the chain. Skip empty blocks; there is nothing to
        // grade.
        if let Some(stx) = self.shadow_tx.as_ref()
            && (!self.shadow_captures.is_empty() || self.shadow_serial > 0)
        {
            let blk = crate::shadow::ShadowBlock {
                block_number,
                captures: std::mem::take(&mut self.shadow_captures),
                serial_records: std::mem::take(&mut self.shadow_serial),
            };
            match stx.try_send(blk) {
                Ok(()) => {}
                Err(crossbeam_channel::TrySendError::Full(_)) => {
                    metrics::counter!(
                        crate::metrics::FOOTPRINT_BLOCKS_TOTAL,
                        "outcome" => "dropped"
                    )
                    .increment(1);
                    tracing::warn!(
                        block = block_number,
                        "footprint-shadow handoff full; dropping this block's capture"
                    );
                }
                Err(crossbeam_channel::TrySendError::Disconnected(_)) => {}
            }
        }
        match self.parent.as_mut() {
            Some(m) => m.merge_from(&pending),
            None => self.parent = Some(pending.clone()),
        }
        self.inflight.push_back((boundary.clone(), pending.clone()));
        let bd: BlockDelta =
            pending.finalize(block_number, std::mem::take(&mut self.block_receipts));

        // Submit without waiting. The commit settles at a later boundary's
        // sweep, or at the end of the stream.
        self.sw_queue.submit(boundary, bd)?;
        self.current_block = block_number + 1;
        // New block opens with empty per-block counters.
        self.tx_index_in_block = 0;
        self.cumulative_gas_used = 0;
        // The next block's wall-clock timestamp arrives in its own
        // BlockBoundaryStart. Until then, keep the previous value as a
        // deterministic placeholder, for any tx that races ahead of the
        // sealer. In v0 the sealer is single-leader, so this branch is
        // purely defensive.
        self.current_l2_ts = l2_timestamp;
        Ok(Flow::Continue)
    }
}

// This is an internal seam between `Executor::run`, which takes the
// grouped `Inbound`/`Outbound`/`Start`/`RoleHooks` structs, and
// `ExecState::new`. The args here are already-destructured fields. A
// struct here would just re-wrap what the caller unwrapped.
#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn_exec<S, Q, P, E>(
    cfg: ExecutorConfig,
    rx: Receiver<ReaderToExec>,
    tx: Sender<ExecToCommit>,
    snapshots: S,
    sw_signal: Q,
    sw_queue: P,
    start: ResumePoint,
    bal_tx: Option<Sender<BalHandoff>>,
    shadow_tx: Option<Sender<crate::shadow::ShadowBlock>>,
    block_exec: Option<BlockExec<S::Db>>,
    epoch_observer: Option<E>,
) -> JoinHandle<Result<(), ExecutorError>>
where
    S: SnapshotSource + 'static,
    Q: StateWriterSignal + 'static,
    P: StateWriterQueue + 'static,
    E: EpochObserver + 'static,
{
    thread::Builder::new()
        .name("executor-exec".into())
        .spawn(move || -> Result<(), ExecutorError> {
            ExecState::new(
                cfg,
                rx,
                tx,
                snapshots,
                sw_signal,
                sw_queue,
                start,
                bal_tx,
                shadow_tx,
                block_exec,
                epoch_observer,
            )
            .run()
        })
        .expect("spawn exec")
}
