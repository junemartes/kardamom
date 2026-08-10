//! The sequential execution thread: consumes canonical-ordered records from
//! the reader layer, executes them against snapshot ∘ parent ∘ delta, and
//! feeds receipts + durably-settled boundaries to the commit thread.
//!
//! The loop's mutable state lives in [`ExecState`]; each `ReaderToExec` arm
//! is one `on_*` method. The pipelined-commit settle sweep (shared by the
//! idle probe and the boundary arm) lives in `exec_settle.rs`.

use std::collections::VecDeque;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver, Sender};

use kardamom_types::xchain::{RemoteEpochRecord, XChainMessage};
use kardamom_types::{
    BPosition, BlockBoundary, BlockBoundaryStart, BlockDelta, Deposit, EpochRecord, SnapshotSource,
    TxEnvelope,
};

use crate::block_env::ExecEnv;
use crate::delta::{PendingDelta, WriteSet};
use crate::error::ExecutorError;
use crate::exec_types::TxIndex;
use crate::executor::{execute_deposit_tx, execute_xchain_tx};
use crate::reader::{EpochObserver, ReaderToExec, RemoteEpochObserver};

use super::ports::{StateWriterQueue, StateWriterSignal};
use super::types::{
    BalHandoff, BlockExec, BufferedRecord, ExecToCommit, ExecutorConfig, ResumePoint,
};

/// How long an IDLE exec thread waits before probing the writer
/// for settled in-flight commits. Settling used to happen only at
/// the NEXT boundary — correct under sustained load (a boundary
/// per tick), but on an idle tail the last ≤K blocks' boundary
/// closeouts never published: ingress watermarks stalled (S4's
/// -32000), executor/validator never converged on a drain (S6/S9),
/// and the attester never covered the final blocks (S2) — the
/// chain-semantics suite went red from the day the pipeline
/// landed (#129) while the constantly-loaded cluster shards
/// stayed green. Under load the timeout never fires (records
/// arrive faster); when idle the probe is a cheap non-blocking
/// read.
const IDLE_SETTLE_PROBE: Duration = Duration::from_millis(25);

/// Outcome of one receive attempt on the reader channel (see
/// [`ExecState::recv_next`]).
enum Recv {
    Msg(ReaderToExec),
    IdleProbe,
    Closed,
}

/// Control-flow outcome of a message handler: keep looping, or stop cleanly
/// (the commit channel's receiver is gone — shutdown, not an error).
pub(super) enum Flow {
    Continue,
    Stop,
}

/// The exec thread's mutable loop state. One instance per `spawn_exec`
/// thread; every `ReaderToExec` arm of the old inline loop is now a method.
pub(super) struct ExecState<S: SnapshotSource, Q, P> {
    pub(super) cfg: ExecutorConfig,
    pub(super) rx: Receiver<ReaderToExec>,
    pub(super) tx: Sender<ExecToCommit>,
    pub(super) snapshots: S,
    pub(super) sw_signal: Q,
    pub(super) sw_queue: P,
    pub(super) bal_tx: Option<Sender<BalHandoff>>,
    pub(super) block_exec: Option<BlockExec<S::Db>>,
    pub(super) epoch_observer: Option<Box<dyn EpochObserver>>,
    pub(super) remote_epoch_observer: Option<Box<dyn RemoteEpochObserver>>,
    /// The snapshot source hands back owned snapshots keyed by block
    /// number: the block *just committed* — `initial_block` (== the
    /// persisted `resume.block` on a resume, 0 on a fresh start).
    pub(super) snapshot: S::Db,
    pub(super) delta: PendingDelta,
    /// EIP-7928 capture: per-block Bal, reset at each boundary; only
    /// maintained when a publisher is attached (executor role).
    pub(super) block_bal: revm::state::bal::Bal,
    /// Whole-block buffer, used only when a block-exec strategy is
    /// supplied (validator parallel path).
    pub(super) buffered: Vec<BufferedRecord>,
    /// Per-BLOCK execution scope (streaming path): one EVM + one
    /// commit-into cache for the whole block — the per-tx
    /// construction was ~90% of execution-path allocation. Dropped
    /// at each boundary; rebuilt lazily at the block's first tx
    /// (seeded with parent + whatever the live delta already holds,
    /// e.g. deposits that landed before the first tx).
    pub(super) scope: Option<crate::executor::ExecScope<S::Db>>,
    /// Pipelined commit (depth K): at each boundary the finalized
    /// delta is SUBMITTED to the writer but not awaited — the next
    /// block executes against snapshot ∘ merged-unsettled ∘ delta,
    /// completed commits settle opportunistically (non-blocking
    /// probe) at each boundary, and the exec thread only ever parks
    /// when the writer is a FULL K blocks behind. One fsync slower
    /// than a block interval therefore no longer touches execution
    /// at all (with depth 1 it still did: blocking wait_committed was
    /// the receipt tail's dominant source — commit p50 ~25ms even for
    /// empty blocks, p99 ~100ms, worst 1-2.5s). Durability semantics
    /// unchanged: a BOUNDARY still reaches tx_receipts only after its
    /// block is durable, and receipts already streamed pre-durability
    /// (AT-LEAST-ONCE, re-published byte-identical on crash replay).
    ///
    /// `parent` is the MERGED union of every unsettled block's writes
    /// (later blocks win) — one layer regardless of depth, so per-tx
    /// cache seeding stays O(one map); it is rebuilt from the
    /// survivors when commits settle.
    pub(super) parent: Option<PendingDelta>,
    pub(super) inflight: VecDeque<(BlockBoundary, PendingDelta)>,
    /// Per-block receipts in arrival order, drained into the
    /// BlockDelta at each boundary so the writer persists them
    /// (receipts + tx_hash_index tables; #109). The clone per tx is
    /// the price of feeding both this and the streaming tx_receipts
    /// publisher — flagged for saturation validation.
    pub(super) block_receipts: Vec<kardamom_types::Receipt>,
    /// Block-number bookkeeping. We treat blocks 1-indexed (genesis
    /// is block 0). The exec thread assumes every block boundary it
    /// sees is for the *current* in-flight block; it doesn't try to
    /// re-derive block numbers without sealer help.
    pub(super) current_block: u64,
    /// Block N's txs execute with boundary N-1's timestamp; on resume
    /// that boundary was consumed before the restart, so its persisted
    /// value seeds the state (see [`ResumePoint::l2_timestamp`]).
    pub(super) current_l2_ts: u64,
    /// Per-block RPC enrichment counters; reset at each BoundaryStart.
    pub(super) tx_index_in_block: u64,
    pub(super) cumulative_gas_used: u64,
    /// Cumulative count of canonical records (TxRef + DepositRef) this
    /// exec thread has folded into a receipt. This is the boundary
    /// alignment key: `BlockBoundaryStart.end_tx_idx` carries the
    /// sealer's cumulative count of republished canonical records
    /// (encoded via `BPosition::from_index`), and at each boundary the
    /// two counts MUST match. `expected_tx_idx` already tracks exactly
    /// this (it advances once per applied Tx/Deposit and never resets
    /// across blocks), so we compare against it directly.
    ///
    /// (Count, not Aeron byte position: positions are per-publication
    /// term spaces under the canonical-publisher MDC merge and ambiguous
    /// between offer-return and frame-start frames — the old position
    /// key broke under load for exactly that reason.)
    ///
    /// Seeded at the resume cursor: the boundary counts on the wire are
    /// ABSOLUTE, and delivery resumes at the cursor, so the counter must
    /// too (starting at 0 made every mid-chain resume die on its first
    /// boundary with a misalignment equal to the cursor).
    pub(super) expected_tx_idx: TxIndex,
    /// Wall time spent executing the block's txs/deposits (excludes
    /// channel idle time between txs), recorded when the BoundaryStart
    /// closes the block. `None` for empty blocks.
    pub(super) block_apply_elapsed: Option<Duration>,
    /// Pre-resolved counter handles: this loop is the executor's
    /// hottest path, so skip the per-event registry lookup.
    pub(super) tx_applied_ok: metrics::Counter,
    pub(super) tx_applied_error: metrics::Counter,
}

impl<S, Q, P> ExecState<S, Q, P>
where
    S: SnapshotSource + 'static,
    Q: StateWriterSignal + 'static,
    P: StateWriterQueue + 'static,
{
    /// Seed the loop state. Resume-from-cursor: the canonical stream source
    /// delivers from the persisted cursor onward (the cluster client's
    /// REPLAY_FROM; below-cursor records are deduped in reader::cluster), so
    /// the exec thread seeds its absolute counters from [`ResumePoint`]
    /// instead of replaying from record 0 and skip-counting. On a fresh
    /// start (`resume == None`) everything seeds at genesis values.
    #[allow(clippy::too_many_arguments)] // mirrors `spawn_exec`'s shape; see the note there.
    fn new(
        cfg: ExecutorConfig,
        rx: Receiver<ReaderToExec>,
        tx: Sender<ExecToCommit>,
        snapshots: S,
        sw_signal: Q,
        sw_queue: P,
        initial_block: u64,
        resume: Option<ResumePoint>,
        bal_tx: Option<Sender<BalHandoff>>,
        block_exec: Option<BlockExec<S::Db>>,
        epoch_observer: Option<Box<dyn EpochObserver>>,
        remote_epoch_observer: Option<Box<dyn RemoteEpochObserver>>,
    ) -> Self {
        let resume_count = resume.map(|r| r.record_count).unwrap_or(0);
        let snapshot = snapshots.snapshot_after(initial_block);
        Self {
            cfg,
            rx,
            tx,
            snapshots,
            sw_signal,
            sw_queue,
            bal_tx,
            block_exec,
            epoch_observer,
            remote_epoch_observer,
            snapshot,
            delta: PendingDelta::new(),
            block_bal: revm::state::bal::Bal::new(),
            buffered: Vec::new(),
            scope: None,
            parent: None,
            inflight: VecDeque::new(),
            block_receipts: Vec::new(),
            current_block: initial_block + 1,
            current_l2_ts: resume.map(|r| r.l2_timestamp).unwrap_or(0),
            tx_index_in_block: 0,
            cumulative_gas_used: 0,
            expected_tx_idx: TxIndex(resume_count),
            block_apply_elapsed: None,
            tx_applied_ok: metrics::counter!(crate::metrics::TX_APPLIED_TOTAL, "outcome" => "ok"),
            tx_applied_error: metrics::counter!(crate::metrics::TX_APPLIED_TOTAL, "outcome" => "error"),
        }
    }

    /// The exec thread's main loop: receive (or idle-probe), dispatch to the
    /// per-arm handler, stop cleanly when the commit channel's receiver is
    /// gone or the reader channel closes.
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
                ReaderToExec::RemoteEpoch {
                    tx_idx,
                    record,
                    position,
                } => self.on_remote_epoch(tx_idx, record, position)?,
                ReaderToExec::XChain {
                    tx_idx,
                    origin_chain_id,
                    message,
                    position,
                } => self.on_xchain(tx_idx, origin_chain_id, message, position)?,
                ReaderToExec::Boundary(start) => self.on_boundary(start)?,
            };
            if let Flow::Stop = flow {
                return Ok(());
            }
        }
    }

    /// One receive attempt. With commits in flight the wait is bounded by
    /// [`IDLE_SETTLE_PROBE`] so an idle tail still settles; with none, block
    /// indefinitely (nothing to settle).
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

    /// Canonical-order check shared by the Tx / Epoch / Deposit arms: the
    /// record's absolute index must be exactly the next expected one; on a
    /// match the counter advances.
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

    /// The block env every execution path (streaming tx, deposit,
    /// whole-block strategy) derives its EVM environment from.
    fn exec_env(&self, block_number: u64) -> ExecEnv {
        ExecEnv {
            chain_id: self.cfg.chain_id,
            block_number,
            l2_timestamp: self.current_l2_ts,
        }
    }

    /// Post-execution bookkeeping shared by the Tx and Deposit arms:
    /// ok/error counters, error surfacing, cumulative gas + per-block index
    /// advance, folding the write set into the live delta, elapsed-time
    /// accounting, and streaming the receipt to the commit thread.
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
        if self.block_exec.is_some() {
            // Whole-block strategy: defer to the boundary so
            // batches can execute concurrently.
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
        let result = sc.execute_tx(
            tx_idx,
            position,
            &envelope,
            self.tx_index_in_block,
            self.cumulative_gas_used,
            self.bal_tx
                .as_ref()
                .map(|_| (&mut self.block_bal, self.tx_index_in_block + 1)),
        );
        // Progress log fires only for an applied tx (an error propagates in
        // `record_applied` below before it would have logged).
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
        self.record_applied("execute_tx", position, result, apply_start)
    }

    fn on_epoch(
        &mut self,
        tx_idx: TxIndex,
        epoch: EpochRecord,
        position: BPosition,
    ) -> Result<Flow, ExecutorError> {
        // The marker consumes one slot and applies no tx: it
        // exists so the L1 origin advances at a point every
        // replica agrees on, and so the deposits that follow
        // start at a slot the sealer also reserved.
        self.check_in_order("Epoch", tx_idx, position)?;
        // Checked BEFORE the epoch's deposits are applied, so a
        // rejected epoch fail-stops instead of committing.
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
        // Deposits run outside the scope (rare, own commit
        // semantics) — fold their writes into the block
        // cache so later txs in this block observe them.
        if let (Some(sc), Ok((_, ws))) = (self.scope.as_mut(), &result) {
            let mut layer = PendingDelta::new();
            layer.apply(ws.clone());
            sc.seed_layer(&layer)?;
        }
        self.record_applied("execute_deposit_tx", position, result, apply_start)
    }

    fn on_remote_epoch(
        &mut self,
        tx_idx: TxIndex,
        record: Box<RemoteEpochRecord>,
        position: BPosition,
    ) -> Result<Flow, ExecutorError> {
        // Like the L1 epoch marker: consumes one slot, applies no tx — the
        // pair's origin advances at a point every replica agrees on, and the
        // messages that follow start at a slot the sealer also reserved.
        self.check_in_order("RemoteEpoch", tx_idx, position)?;
        // Checked BEFORE the record's messages execute, so a rejected
        // record fail-stops instead of committing.
        if let Some(obs) = self.remote_epoch_observer.as_mut() {
            obs.observe(&record)?;
        }
        tracing::debug!(
            target: "kardamom_executor::exec",
            block = self.current_block,
            origin_chain_id = record.origin_chain_id,
            anchor_number = record.anchor_number,
            messages = record.messages.len(),
            "remote epoch marker: pair origin advances"
        );
        Ok(Flow::Continue)
    }

    fn on_xchain(
        &mut self,
        tx_idx: TxIndex,
        origin_chain_id: u64,
        message: Box<XChainMessage>,
        position: BPosition,
    ) -> Result<Flow, ExecutorError> {
        self.check_in_order("XChain", tx_idx, position)?;
        if self.block_exec.is_some() {
            // Whole-block execution (the validator's parallel path) has no
            // BufferedRecord arm for cross-chain messages yet. Executing the
            // record inline here while the strategy replays the block without
            // it would fork the two paths — fail stop until the destination
            // validator wiring lands (interop P1, validator slice).
            return Err(ExecutorError::State(format!(
                "cross-chain message (origin {origin_chain_id}, seq {}) is not supported by \
                 whole-block execution on this build",
                message.seq
            )));
        }
        let env = self.exec_env(self.current_block);
        let apply_start = Instant::now();
        let result = execute_xchain_tx(
            &self.snapshot,
            self.parent.as_ref(),
            &self.delta,
            env,
            tx_idx,
            position,
            origin_chain_id,
            &message,
            self.tx_index_in_block,
            self.cumulative_gas_used,
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
        // Alignment: BlockBoundaryStart.end_tx_idx carries the
        // sealer's cumulative COUNT of canonical records
        // (TxRef + DepositRef) republished through the end of
        // this block, encoded via BPosition::from_index. The
        // executor must have applied exactly that many records
        // — i.e. `expected_tx_idx` (which advances once per
        // applied Tx/Deposit and never resets) must equal it.
        // A mismatch means the executor's view of the canonical
        // stream diverged from the sealer's (a lost / extra /
        // reordered record) — fatal: return so the process
        // crash-loops rather than committing a wrong block.
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

        // Whole-block strategy: execute everything buffered
        // for this block now (the validator's batches run
        // concurrently inside), then feed its receipts and
        // delta into the SAME boundary path the streaming
        // executor uses — commit ordering, durability gating
        // and the write-set cross-check are unchanged.
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
            for r in out.receipts {
                self.block_receipts.push(r.clone());
                if self.tx.send(ExecToCommit::Receipt(r)).is_err() {
                    return Ok(Flow::Stop);
                }
            }
        }

        // Record the block's accumulated execution time.
        // Only recorded when the block had at least one tx;
        // empty blocks are skipped.
        if let Some(elapsed) = self.block_apply_elapsed.take() {
            metrics::histogram!(crate::metrics::BLOCK_APPLY_DURATION_SECONDS)
                .record(elapsed.as_secs_f64());
        }

        // S0: NO state-root computation. The sealed
        // BlockBoundary on tx_receipts is slim — no
        // commitment. `l1_origin` rides through unchanged
        // from the sealer's marker: it identifies the L1
        // epoch this block belongs to, which is what lets a
        // reconstructor place the epoch's deposits.
        let boundary = BlockBoundary {
            block_number,
            end_tx_idx,
            l2_timestamp,
            l1_origin,
        };

        // Drain the delta. We swap it out so the writer owns
        // it — but RETAIN a clone as the next block's parent
        // read layer (pipelined commit: the clone of the
        // block's write maps costs a few ms; the fsync it
        // takes off the critical path costs 25ms-2.5s). The
        // block's receipts ride INSIDE the BlockDelta
        // (arrival order) so the writer persists them durably
        // (#109); they also streamed out on tx_receipts at
        // execute time, above, ahead of durability — a crash
        // in that window re-executes the block on recovery
        // and re-publishes byte-identical receipts, so
        // tx_receipts is AT-LEAST-ONCE and every consumer
        // must dedup on `tx_idx` (ingress does).
        let pending = std::mem::take(&mut self.delta);
        // The block's execution scope dies with the block:
        // the NEXT block gets a new parent layer and block
        // env, and (when commits settled) a fresh snapshot.
        // Dropping here is unconditional — a scope reused
        // across a boundary would execute against the
        // previous block's parent and env.
        self.scope = None;
        // EIP-7928 handoff: move the block's Bal + a
        // receipts-free copy of the merged delta to the
        // publisher thread (encode + reliable delivery live
        // entirely off this thread). A dropped send means
        // the publisher is gone mid-shutdown — not fatal.
        if let Some(btx) = self.bal_tx.as_ref() {
            let bal_delta = pending.clone().finalize(block_number, Vec::new());
            // try_send: the BAL handoff must NEVER block
            // execution — a slow/stuck publisher pump once
            // back-pressured this thread through the bounded
            // channel and delayed every receipt behind it
            // (S4). A dropped frame costs one block of BAL
            // retention (that block verifies as bal_missing,
            // the tolerated path); a stalled exec thread
            // costs the chain.
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
                    // Publisher gone mid-shutdown — not fatal.
                }
            }
        }
        match self.parent.as_mut() {
            Some(m) => m.merge_from(&pending),
            None => self.parent = Some(pending.clone()),
        }
        self.inflight.push_back((boundary.clone(), pending.clone()));
        let bd: BlockDelta =
            pending.finalize(block_number, std::mem::take(&mut self.block_receipts));

        // Submit WITHOUT waiting: the commit settles at a
        // later boundary's sweep (or at end of stream).
        self.sw_queue.submit(boundary, bd)?;
        self.current_block = block_number + 1;
        // New block opens with empty per-block counters.
        self.tx_index_in_block = 0;
        self.cumulative_gas_used = 0;
        // The next block's wall-clock timestamp arrives in
        // its own BlockBoundaryStart; until then we keep
        // the previous value as a deterministic
        // placeholder for any txs that race ahead of the
        // sealer (in v0 the sealer is single-leader so
        // this branch is purely defensive).
        self.current_l2_ts = l2_timestamp;
        Ok(Flow::Continue)
    }
}

// 8 args mirrors `Executor::run`'s shape (readers/exec/commit wiring); a config
// struct would just shuffle the same fields behind one name. See the note on
// `Executor::run`.
#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn_exec<S, Q, P>(
    cfg: ExecutorConfig,
    rx: Receiver<ReaderToExec>,
    tx: Sender<ExecToCommit>,
    snapshots: S,
    sw_signal: Q,
    sw_queue: P,
    initial_block: u64,
    resume: Option<ResumePoint>,
    bal_tx: Option<Sender<BalHandoff>>,
    block_exec: Option<BlockExec<S::Db>>,
    epoch_observer: Option<Box<dyn EpochObserver>>,
    remote_epoch_observer: Option<Box<dyn RemoteEpochObserver>>,
) -> JoinHandle<Result<(), ExecutorError>>
where
    S: SnapshotSource + 'static,
    Q: StateWriterSignal + 'static,
    P: StateWriterQueue + 'static,
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
                initial_block,
                resume,
                bal_tx,
                block_exec,
                epoch_observer,
                remote_epoch_observer,
            )
            .run()
        })
        .expect("spawn exec")
}
