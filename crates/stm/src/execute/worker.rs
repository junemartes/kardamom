use super::config::{PARK_POLL, SPIN_BEFORE_PARK, bal_index, nanos};
use super::graph::{BlockCtx, WorkerQueue};
use super::handle::PoolShared;
use super::metrics::{Metrics, TxResult};
use super::prepare::domain_hash;
use super::recycle::RecyclePools;
use super::session::BoundLayers;
use super::view::{BlockInput, MvView};
use crate::FEE_SINK;
use crate::mv::MvCache;
use crate::mv::ReadRecord;
use alloy_primitives::B256;
use alloy_primitives::U256;
use kardamom_exec_core::block_env::ExecEnv;
use kardamom_exec_core::delta::WriteSet;
use kardamom_exec_core::error::ExecutorError;
use kardamom_exec_core::exec_types::ReceiptStatus;
use kardamom_exec_core::exec_types::TxIndex;
use kardamom_exec_core::executor::DecodedTx;
use kardamom_exec_core::executor::Executor;
use kardamom_types::BPosition;
use kardamom_types::Receipt;
use kardamom_types::StateDatabase;
use kardamom_types::TxEnvelope;
use revm::Context;
use revm::ExecuteEvm;
use revm::MainBuilder;
use revm::MainContext;
use revm::context::result::ExecutionResult;
use std::sync::atomic::Ordering;

pub(super) fn worker_loop<S: StateDatabase + Sync>(
    shared: &PoolShared<S>,
    worker: usize,
    keep_hot: bool,
) {
    let mut seen = 0u64;
    loop {
        let Some(ctx) = shared.next_ctx(keep_hot, &mut seen) else {
            return;
        };
        run_worker_block(&ctx, worker);
        // The Arc drops here; seal()'s try_unwrap spin depends on it.
    }
}

/// One worker's participation in one block: one EVM for the whole block
/// (per-transaction construction was most of the execution-path
/// allocation), with the view re-aimed per transaction. Pops only its
/// own FIFO: same-domain transactions were hashed here in canonical
/// order, so a chain drains without any cross-thread handoff, and the
/// DAG carries only the cross-domain edges.
///
/// A worker almost never blocks on a dependency: a transaction reaches a
/// queue once its edge indegree is zero, and its FIFO-covered
/// predecessors sit ahead of it in the same queue, verified at take
/// time (`fifo_ready`), which is what keeps a stolen predecessor from
/// breaking the order. No wait-graph deadlock is possible: the
/// canonical total order bounds every edge and every FIFO obligation
/// (low index to high), and completion only ever removes them.
/// Late-bind gate: a deferred session's transactions are admitted and
/// queued before its read base exists. Nothing may execute until the
/// consumer binds the layers. The wait is bind latency (the
/// predecessor's fold), normally sub-millisecond; the block-at-a-time
/// path binds at session build, so this load is free. `None` when the
/// block aborts before a consumer ever binds; the tail's drain watchdog
/// is the loud backstop for a consumer that never binds at all.
fn wait_for_binding<S: StateDatabase>(ctx: &BlockCtx<S>) -> Option<&BoundLayers> {
    loop {
        if let Some(b) = ctx.binding.get() {
            return Some(b);
        }
        if ctx.aborted.load(Ordering::SeqCst) {
            return None;
        }
        std::thread::yield_now();
    }
}

/// Take the next runnable transaction for this worker, or `None` once
/// the block is aborted or fully drained (the caller must then leave).
/// Delegates each lock-holding step to its own [`Acquire`] method, so
/// no lock is ever held across a method call or a loop iteration: the
/// lock-order rule (a queue lock is never held while touching the
/// graph lock, since `prune`'s `push_ready` re-locks queues) is
/// enforced by those method boundaries instead of by manual `drop`s.
fn next_job<S: StateDatabase>(
    ctx: &BlockCtx<S>,
    worker: usize,
    qh: &WorkerQueue,
    local_next: &mut Option<u32>,
) -> Option<u32> {
    let mut acquire = Acquire {
        ctx,
        worker,
        qh,
        local_next,
    };
    loop {
        if ctx.aborted.load(Ordering::SeqCst) {
            return None;
        }
        match acquire.initial_poll() {
            Pop::Ready(i) => return Some(i),
            Pop::Stalled => {
                // A FIFO predecessor was stolen and is still running on
                // another thread. This is rare, and bounded by that
                // transaction's execution time. Yield and retry.
                std::thread::yield_now();
                continue;
            }
            Pop::Dry => {}
        }
        // Dry. Apply any parked completions myself before parking:
        // this is what makes batching safe, since the pool can never
        // sit idle on DAG updates nobody applied.
        if acquire.apply_pending() {
            continue;
        }
        if ctx.drained() {
            return None;
        }
        // Domain hashing collides when the number of domains is close
        // to the number of workers, so the busiest thread can take
        // well over its even share while some threads get nothing at
        // all. So an idle worker helps the busiest one rather than
        // parking.
        if let Some(stolen) = acquire.try_steal() {
            return Some(stolen);
        }
        if acquire.queue_has_work() {
            continue;
        }
        // Spin before parking: at high throughput the next transaction
        // is usually microseconds away, and a park/unpark pair costs
        // two syscalls, more than a small transfer's entire execution.
        // Only a worker that stays dry through the spin advertises
        // itself as parked and blocks.
        let t_idle = std::time::Instant::now();
        if !acquire.spin() {
            acquire.park();
        }
        ctx.metrics
            .idle_ns
            .fetch_add(nanos(t_idle.elapsed()), Ordering::Relaxed);
    }
}

/// What [`Acquire::initial_poll`] found.
enum Pop {
    Ready(u32),
    /// A FIFO pop came back, but its predecessor is still running
    /// elsewhere (`ctx.fifo_ready` said no); already put back.
    Stalled,
    Dry,
}

/// One worker's dry-queue handling for [`next_job`], split into small
/// steps so each queue-lock acquisition lives inside its own method
/// body and is released (by ordinary block-scope drop) before that
/// method calls into the graph lock (`ctx.prune`, `ctx.fifo_ready`,
/// `ctx.steal`) or returns. See `next_job`'s doc for the invariant this
/// enforces.
struct Acquire<'a, S: StateDatabase> {
    ctx: &'a BlockCtx<S>,
    worker: usize,
    qh: &'a WorkerQueue,
    local_next: &'a mut Option<u32>,
}

impl<S: StateDatabase> Acquire<'_, S> {
    /// Chain-local hand-off first (see `complete_inline`), then the
    /// shared bag, then this worker's own FIFO — whichever this
    /// block's scheduler mode uses. Bag entries dispatch at indegree 0
    /// with coverage off, so there is nothing to verify; a FIFO pop is
    /// verified with no lock held, since the check takes a node mutex
    /// and scans results.
    fn initial_poll(&mut self) -> Pop {
        if self.ctx.bag_mode {
            if let Some(i) = self.local_next.take() {
                return Pop::Ready(i);
            }
            return self.ctx.bag.pop().map_or(Pop::Dry, Pop::Ready);
        }
        let i = {
            let mut q = self.qh.q.lock().expect("queue poisoned");
            let Some(i) = q.pop_front() else {
                return Pop::Dry;
            };
            self.qh.len.fetch_sub(1, Ordering::Release);
            i
        };
        if self.ctx.fifo_ready(i) {
            return Pop::Ready(i);
        }
        self.ctx.metrics.fifo_stalls.fetch_add(1, Ordering::Relaxed);
        {
            let mut q = self.qh.q.lock().expect("queue poisoned");
            q.push_front(i);
            self.qh.len.fetch_add(1, Ordering::Release);
        }
        Pop::Stalled
    }

    /// Apply any completions parked on the graph while this worker is
    /// dry. Takes no queue lock: `ctx.prune` owns the graph lock.
    fn apply_pending(&self) -> bool {
        if self.ctx.pending.load(Ordering::SeqCst) > 0 {
            self.ctx.prune(true);
            true
        } else {
            false
        }
    }

    /// Whether an idle worker should steal a ready transaction from a
    /// busy one this block (see `PoolHandle::steal_enabled`'s doc).
    /// Takes no queue lock: `ctx.steal` owns the graph lock.
    fn try_steal(&self) -> Option<u32> {
        if !self.ctx.steal_enabled {
            return None;
        }
        let stolen = self.ctx.steal(self.worker)?;
        self.ctx.metrics.steals.fetch_add(1, Ordering::Relaxed);
        Some(stolen)
    }

    /// Whether the queue (or bag) already has something, checked under
    /// the queue lock and released before returning.
    fn queue_has_work(&self) -> bool {
        let q = self.qh.q.lock().expect("queue poisoned");
        !q.is_empty() || (self.ctx.bag_mode && !self.ctx.bag.is_empty())
    }

    /// Spin before parking. Returns `true` once the spin sees work (so
    /// the caller retries `initial_poll` instead of parking), `false`
    /// once the block ends or the spin budget runs out with nothing to
    /// show for it (the caller then calls `park`). Takes no queue lock:
    /// the probe is the lock-free `qh.len` atomic, matched by
    /// `queue_has_work`'s locked recheck once the caller decides to
    /// park.
    fn spin(&self) -> bool {
        let spin_start = std::time::Instant::now();
        loop {
            for _ in 0..SPIN_BEFORE_PARK {
                std::hint::spin_loop();
            }
            // Lock-free probe: a dry worker spinning here for tens of
            // microseconds must not contend with the feed's push into
            // this very queue.
            if self.qh.len.load(Ordering::Acquire) > 0
                || (self.ctx.bag_mode && !self.ctx.bag.is_empty())
            {
                return true;
            }
            // Completions may be parked while we spin. Apply them
            // ourselves rather than spin past the work they would
            // release.
            if self.apply_pending() {
                continue;
            }
            if self.ctx.drained() || self.ctx.aborted.load(Ordering::SeqCst) {
                return false;
            }
            if nanos(spin_start.elapsed()) >= self.ctx.spin_ns {
                return false;
            }
        }
    }

    /// A bounded wait, deliberately. A notification can be missed:
    /// `signal_done` wakes every queue without holding that queue's
    /// mutex, so a worker sitting between "decided to park" and
    /// "actually waiting" sleeps through it and never returns. The
    /// block drains, `seal` finishes, and the pool then hangs forever
    /// joining that thread. A timeout makes any missed wake
    /// self-healing, and costs nothing when wakes arrive normally.
    fn park(&self) {
        let q = self.qh.q.lock().expect("queue poisoned");
        if !q.is_empty() || (self.ctx.bag_mode && !self.ctx.bag.is_empty()) {
            return;
        }
        self.qh.parked.store(true, Ordering::Release);
        let _ = self
            .qh
            .cv
            .wait_timeout(q, PARK_POLL)
            .expect("queue poisoned");
        self.qh.parked.store(false, Ordering::Release);
    }
}

/// Which side of a completed write set is foreign: an account whose
/// domain hashes to another worker than the one that wrote it, the true
/// sharing that no lock granularity removes. Returns `(own, foreign)`.
fn record_write_domains(
    r: &Result<TxResult, ExecutorError>,
    worker: usize,
    n_workers: usize,
) -> (u64, u64) {
    let (mut own, mut foreign) = (0u64, 0u64);
    if let Ok(res) = r {
        for (addr, _) in &res.ws.accounts {
            if *addr == FEE_SINK {
                continue; // deferred, never published
            }
            if domain_hash(addr.as_slice(), n_workers) == worker {
                own += 1;
            } else {
                foreign += 1;
            }
        }
    }
    (own, foreign)
}

/// Completion: park the index in this worker's own buffer (uncontended)
/// and only touch the DAG once a batch has accumulated
/// (`prune_batch == 1` is the immediate policy). `pending` increments
/// before the buffer push. Prune runs concurrently (another worker's
/// batch, the tail's drain loop) and subtracts what it drains. A
/// completion visible in a buffer before its increment landed
/// underflowed the counter (a debug-build overflow panic); incremented-
/// but-unpushed is the safe direction, since a spurious prune drains
/// nothing.
fn complete_job<S: StateDatabase>(
    ctx: &BlockCtx<S>,
    worker: usize,
    job: u32,
    local_next: &mut Option<u32>,
) {
    if ctx.bag_mode {
        *local_next = ctx.complete_inline(job);
        return;
    }
    let owed = ctx.pending.fetch_add(1, Ordering::SeqCst) + 1;
    {
        let mut b = ctx.completed[worker].lock().expect("completed poisoned");
        b.push(job);
        ctx.completed_len[worker].0.fetch_add(1, Ordering::Release);
    }
    if owed >= ctx.prune_batch as u64 {
        ctx.prune(false);
    }
}

pub(super) fn run_worker_block<S: StateDatabase>(ctx: &BlockCtx<S>, worker: usize) {
    let Some(bound) = wait_for_binding(ctx) else {
        return;
    };
    let input = BlockInput {
        snapshot: ctx.snapshots.for_worker(worker),
        base: Some(&ctx.base),
        layers: &bound.layers,
        mv_layers: &bound.mv_layers,
    };
    let view = MvView::new(
        &ctx.mv,
        &input,
        bound.sink_start.clone(),
        &ctx.base_cache,
        &ctx.metrics,
    );
    let mut read_stash: Vec<Vec<ReadRecord>> = Vec::new();
    let mut local_next: Option<u32> = None;
    let mut evm = Context::mainnet()
        .with_db(view)
        .with_block(ctx.env.block_env())
        .with_cfg(ctx.env.cfg_env())
        .build_mainnet();
    let qh = &ctx.queues[worker];
    // Timing and read counts accumulate locally and flush once per
    // block. A `fetch_add` on shared metrics per read, or even per
    // transaction, had every worker writing the same cache lines. The
    // instrumentation was generating the very cross-core traffic it
    // aimed to measure.
    let mut local_busy_ns: u64 = 0;
    let mut local_first_ns: u64 = u64::MAX;
    let mut local_last_ns: u64 = 0;
    let (mut local_own, mut local_foreign) = (0u64, 0u64);
    let n_workers = ctx.queues.len();
    macro_rules! leave {
        ($evm:expr) => {{
            revm::context_interface::ContextTr::db_mut(&mut *$evm).flush_counters();
            ctx.metrics
                .busy_ns
                .fetch_add(local_busy_ns, Ordering::Relaxed);
            ctx.metrics.busy_per_worker[worker]
                .0
                .fetch_add(local_busy_ns, Ordering::Relaxed);
            ctx.metrics
                .writes_own
                .fetch_add(local_own, Ordering::Relaxed);
            ctx.metrics
                .writes_foreign
                .fetch_add(local_foreign, Ordering::Relaxed);
            if local_first_ns != u64::MAX {
                ctx.metrics
                    .first_dispatch_ns
                    .fetch_min(local_first_ns, Ordering::Relaxed);
                ctx.metrics
                    .last_done_ns
                    .fetch_max(local_last_ns, Ordering::Relaxed);
            }
            return;
        }};
    }
    loop {
        let Some(job) = next_job(ctx, worker, qh, &mut local_next) else {
            leave!(evm);
        };
        let slot = ctx.slots[job as usize]
            .get()
            .expect("slot set before its index is dispatched");
        let t_busy_at = nanos(ctx.started.elapsed());
        let r = execute_one(
            &mut evm,
            TxJob {
                local_idx: job,
                tx_idx: slot.tx_idx,
                position: slot.position,
                envelope: &slot.envelope,
                decoded: slot.decoded.as_ref(),
            },
            ExecCtx {
                mv: &ctx.mv,
                metrics: &ctx.metrics,
                env: ctx.env,
                sink_start_balance: bound.sink_start_balance,
                bal_base: ctx.bal_base,
            },
            &mut || take_read_buf(&mut read_stash, &ctx.recycle),
        );
        // Timestamps stay worker-local and fold once per block. Stamping
        // them globally cost two clock reads and two contended
        // read-modify-writes per transaction. On a small transfer, the
        // instrumentation was a measurable share of the work it claimed
        // to measure.
        let done_at = nanos(ctx.started.elapsed());
        let busy_ns = done_at.saturating_sub(t_busy_at);
        local_busy_ns = local_busy_ns.saturating_add(busy_ns);
        local_first_ns = local_first_ns.min(t_busy_at);
        local_last_ns = local_last_ns.max(done_at);
        let (own, foreign) = record_write_domains(&r, worker, n_workers);
        local_own += own;
        local_foreign += foreign;
        let errored = r.is_err();
        let _ = ctx.results[job as usize].set(r);
        if errored {
            ctx.aborted.store(true, Ordering::SeqCst);
            ctx.wake_all();
            return;
        }
        complete_job(ctx, worker, job, &mut local_next);
    }
}

/// One worker's EVM over its multi-version view: Executor's shape with
/// the concurrent DB swapped in.
pub(super) type WorkerEvm<'a, S> = revm::handler::MainnetEvm<
    revm::context::Context<
        revm::context::BlockEnv,
        revm::context::TxEnv,
        revm::context::CfgEnv,
        MvView<'a, S>,
    >,
>;

/// Pop a cleared read-record buffer (batch-refilled from the recycle
/// pool, one lock per 64 transactions); falls back to a fresh
/// allocation while the pool warms up.
pub(super) fn take_read_buf(
    stash: &mut Vec<Vec<ReadRecord>>,
    pools: &RecyclePools,
) -> Vec<ReadRecord> {
    if let Some(b) = stash.pop() {
        return b;
    }
    {
        let mut g = pools.read_bufs.lock().expect("pools poisoned");
        let n = g.len().min(64);
        let at = g.len() - n;
        stash.extend(g.drain(at..));
    }
    stash.pop().unwrap_or_else(|| Vec::with_capacity(128))
}

/// One transaction, addressed for `execute_one`: its position in the
/// block and the decoded form the reader threads already produced.
#[derive(Clone, Copy)]
pub(super) struct TxJob<'a> {
    pub(super) local_idx: u32,
    pub(super) tx_idx: TxIndex,
    pub(super) position: BPosition,
    pub(super) envelope: &'a TxEnvelope,
    pub(super) decoded: Option<&'a DecodedTx>,
}

/// What `execute_one` reads from and publishes into: the block's shared
/// multi-version view, the worker's metrics, and the block-start fee
/// sink state it must not re-derive per transaction.
#[derive(Clone, Copy)]
pub(super) struct ExecCtx<'a> {
    pub(super) mv: &'a MvCache,
    pub(super) metrics: &'a Metrics,
    pub(super) env: ExecEnv,
    pub(super) sink_start_balance: U256,
    pub(super) bal_base: Option<u64>,
}

impl TxJob<'_> {
    /// Build the skip-path result: a receipt and empty write set, no
    /// reads recorded, no fee credited. Used for every reason a
    /// transaction never really ran (undecodable, or an `EVMError` the
    /// executor treats as a skip rather than a hard failure). `self`'s
    /// own `position`, `envelope`, and `local_idx` fill in three of the
    /// skip receipt's fields, so the caller supplies only what is
    /// specific to the failure (the reason, the detail text, the
    /// resolved nonce and recipient, and the block number).
    fn skip<S: StateDatabase>(
        &self,
        reason: kardamom_types::SkipReason,
        detail: &str,
        nonce: u64,
        to: Option<alloy_primitives::Address>,
        block_number: u64,
    ) -> TxResult {
        let (receipt, ws) = Executor::<S>::skip_receipt(
            reason,
            detail,
            self.position,
            self.envelope,
            nonce,
            to,
            block_number,
            u64::from(self.local_idx),
            0,
        );
        TxResult {
            receipt,
            ws,
            reads: Vec::new(),
            fee_delta: U256::ZERO,
            sink_touched: false,
            bal_frag: None,
        }
    }
}

/// Execute one transaction against its multi-version view. Mirrors
/// `Executor::execute_tx` exactly (skip semantics, write-set emission,
/// receipt shape), with `MvCache` publish in place of the sequential
/// commit. The worker's EVM is reused across transactions; only the
/// view's index and read log are re-aimed.
pub(super) fn execute_one<S: StateDatabase, F: FnMut() -> Vec<ReadRecord>>(
    evm: &mut WorkerEvm<'_, S>,
    job: TxJob<'_>,
    ctx: ExecCtx<'_>,
    fresh_reads: &mut F,
) -> Result<TxResult, ExecutorError> {
    use alloy_consensus::Transaction;
    let TxJob {
        local_idx,
        tx_idx,
        position,
        envelope,
        decoded,
    } = job;
    let ExecCtx {
        mv,
        metrics,
        env,
        sink_start_balance,
        bal_base,
    } = ctx;
    let Some(alloy_env) = decoded else {
        return Ok(job.skip::<S>(
            kardamom_types::SkipReason::Undecodable,
            "undecodable raw_tx",
            0,
            None,
            env.block_number,
        ));
    };
    let (signer, nonce, to) = (envelope.sender, alloy_env.nonce(), alloy_env.to());
    let effective_gas_price = alloy_env
        .gas_price()
        .unwrap_or_else(|| alloy_env.max_fee_per_gas());
    let tx_env = reaim_and_build_tx_env(evm, local_idx, alloy_env, signer);
    let t_evm = std::time::Instant::now();
    let mut outcome = match evm.transact(tx_env) {
        Ok(o) => o,
        Err(revm::context::result::EVMError::Transaction(e)) => {
            return Ok(job.skip::<S>(
                kardamom_exec_core::executor::skip_reason_of_tx(&e),
                &format!("{e:?}"),
                nonce,
                to,
                env.block_number,
            ));
        }
        Err(revm::context::result::EVMError::Header(e)) => {
            return Ok(job.skip::<S>(
                kardamom_types::SkipReason::Header,
                &format!("{e:?}"),
                nonce,
                to,
                env.block_number,
            ));
        }
        Err(e) => {
            return Err(ExecutorError::Execution {
                idx: tx_idx,
                detail: format!("{e:?}"),
            });
        }
    };

    let evm_ns = nanos(t_evm.elapsed());
    metrics.evm_ns.fetch_add(evm_ns, Ordering::Relaxed);
    let gas_used = outcome.result.gas().tx_gas_used();
    // Build wire logs straight from the borrowed result: no
    // intermediate `logs.clone()` (topic Vecs and data Bytes per log).
    let (status, wire_logs) = status_and_wire_logs(&outcome.result);

    let ws = WriteSet::from_evm_state(&outcome.state);
    // EIP-7928 capture: this transaction's fragment at its block-global
    // index, through the same update_account the streaming path uses on
    // the same `outcome.state`. Everything the mv cache tracks reads
    // canonically here (a wound would replace this fragment). The fee
    // sink is the one untracked account: the commit pass rewrites its
    // balance write to the computed prefix, exactly as it rewrites the
    // WriteSet's.
    let bal_frag = capture_bal(bal_base, local_idx, env.block_number, &outcome.state)?;
    // Pooled journal: hand the spent state map back so the next
    // transaction does not regrow it (see `recycle_journal`).
    recycle_journal(evm, &mut outcome.state);
    // Publish in the ordered helper's sequence (code and storage, then
    // accounts; see `MvCache::publish_write_set`), skipping the fee
    // sink (Accumulator: all workers see block-start; the commit pass
    // computes the prefixes).
    let t_pub = std::time::Instant::now();
    mv.publish_write_set(local_idx, &ws, FEE_SINK);
    let pub_ns = nanos(t_pub.elapsed());
    metrics.publish_ns.fetch_add(pub_ns, Ordering::Relaxed);
    let sink_fee_delta = sink_fee_delta(&ws, sink_start_balance, env.block_number, tx_idx)?;
    let sink_touched = sink_fee_delta.is_some();
    let fee_delta = sink_fee_delta.unwrap_or(U256::ZERO);
    let reads = {
        // Take this transaction's read log back out of the worker's
        // view. The replacement comes from the recycle pool (cleared,
        // with capacity intact from a previous block's transaction).
        // The fresh per-transaction Vec and its growth reallocations
        // were the largest STM-specific allocation.
        let db = revm::context_interface::ContextTr::db_mut(&mut **evm);
        std::mem::replace(&mut db.reads, fresh_reads())
    };
    // Hashing now is pure waste when the commit pass must re-hash after
    // patching the accumulator's absolute balance.
    let write_set_hash = if sink_touched { B256::ZERO } else { ws.hash() };
    let receipt = build_receipt(ReceiptArgs {
        position,
        envelope,
        status,
        gas_used,
        wire_logs,
        write_set_hash,
        nonce,
        signer,
        to,
        effective_gas_price,
        block_number: env.block_number,
        local_idx,
    });
    Ok(TxResult {
        receipt,
        ws,
        reads,
        bal_frag,
        fee_delta,
        sink_touched,
    })
}

/// One scan answers both sink questions: whether this transaction
/// touched the fee sink at all, and if so, its credit this transaction
/// (see [`fee_delta_from_sink`]). `None` means the sink was not
/// touched; the caller derives `sink_touched` from that instead of
/// carrying a second, separately-fallible bool.
fn sink_fee_delta(
    ws: &WriteSet,
    sink_start_balance: U256,
    block_number: u64,
    tx_idx: TxIndex,
) -> Result<Option<U256>, ExecutorError> {
    let Some((_, (_, b, _))) = ws.accounts.iter().find(|(a, _)| *a == FEE_SINK) else {
        return Ok(None);
    };
    Ok(Some(fee_delta_from_sink(
        *b,
        sink_start_balance,
        block_number,
        tx_idx,
    )?))
}

/// A worker's fee-sink credit this transaction: the observed post-tx
/// sink balance minus the block-start value every worker was told (see
/// `ExecCtx::sink_start_balance`). `U256::sub` is `wrapping_sub` in
/// every profile, so a plain subtraction here would turn an observed
/// balance below the block-start value into a huge wrapped delta
/// written straight into a receipt, with no panic in any build. A
/// worker can only see a balance below the block-start value if its
/// view of the sink is corrupted (the fee sink only ever gains value
/// within a block), so this is a hard error, not a clamp.
///
/// # Errors
/// Returns an error naming the block, the transaction, the block-start
/// balance, and the observed balance, if `observed < sink_start_balance`.
fn fee_delta_from_sink(
    observed: U256,
    sink_start_balance: U256,
    block_number: u64,
    tx_idx: TxIndex,
) -> Result<U256, ExecutorError> {
    observed.checked_sub(sink_start_balance).ok_or_else(|| {
        ExecutorError::State(format!(
            "stm: worker read the fee sink below its block-start balance in block \
             {block_number}, tx {tx_idx:?}: block-start={sink_start_balance}, \
             observed={observed} — corrupted view, block must not commit"
        ))
    })
}

/// Re-aim the worker's view at this transaction (the view's index and
/// its read log belong to whichever transaction the worker is running
/// now), then build the transaction's `TxEnv`.
fn reaim_and_build_tx_env<S: StateDatabase>(
    evm: &mut WorkerEvm<'_, S>,
    local_idx: u32,
    alloy_env: &DecodedTx,
    signer: alloy_primitives::Address,
) -> revm::context::TxEnv {
    let db = revm::context_interface::ContextTr::db_mut(&mut **evm);
    db.idx = local_idx;
    db.reads.clear();
    alloy_env.tx_env(signer)
}

/// Classify one EVM outcome into a wire status and wire logs, built
/// straight from the borrowed result: no intermediate `logs.clone()`
/// (topic Vecs and data Bytes per log).
fn status_and_wire_logs(result: &ExecutionResult) -> (ReceiptStatus, Vec<kardamom_types::WireLog>) {
    match result {
        ExecutionResult::Success { logs, .. } => (
            ReceiptStatus::Success,
            logs.iter().map(kardamom_types::WireLog::from).collect(),
        ),
        ExecutionResult::Revert { .. } => (ReceiptStatus::Revert, Vec::new()),
        ExecutionResult::Halt { reason, .. } => (ReceiptStatus::Halt(reason.clone()), Vec::new()),
    }
}

/// EIP-7928 capture: this transaction's fragment at its block-global
/// index, through the same `update_account` the streaming path uses on
/// the same post-transaction state. Everything the mv cache tracks
/// reads canonically here (a wound would replace this fragment). The
/// fee sink is the one untracked account: the commit pass rewrites its
/// balance write to the computed prefix, exactly as it rewrites the
/// `WriteSet`'s.
/// # Errors
/// Returns an error if `bal_base + local_idx + 1` overflows `u64`
/// (`bal_base` is a caller-supplied count, not bounded by this crate).
fn capture_bal(
    bal_base: Option<u64>,
    local_idx: u32,
    block_number: u64,
    state: &revm::state::EvmState,
) -> Result<Option<revm::state::bal::Bal>, ExecutorError> {
    let Some(b) = bal_base else {
        return Ok(None);
    };
    let idx = bal_index(b, u64::from(local_idx), block_number)?;
    let mut frag = revm::state::bal::Bal::new();
    for (addr, account) in state {
        frag.update_account(idx, *addr, account);
    }
    Ok(Some(frag))
}

/// Pooled journal: revm's finalize `mem::take`s the state map out of the
/// journal into the outcome, leaving a zero-capacity map behind. Every
/// transaction then regrew a fresh table, which measurement showed as a
/// large share of the per-transaction allocation floor on light
/// workloads. Hand the spent table back: its entries drop here (they
/// are transaction-local by contract, so a stale entry would be read as
/// a cached truth by the next transaction's `load_account`), its
/// capacity survives, and the journal's own entry vec and transient
/// storage already clear in place.
fn recycle_journal<S: StateDatabase>(
    evm: &mut WorkerEvm<'_, S>,
    state: &mut revm::state::EvmState,
) {
    let mut spent = std::mem::take(state);
    spent.clear();
    revm::context_interface::ContextTr::journal_mut(&mut **evm)
        .inner
        .state = spent;
}

/// Everything [`build_receipt`] needs, grouped so the call site reads as
/// one record instead of a 12-argument list.
struct ReceiptArgs<'a> {
    position: BPosition,
    envelope: &'a TxEnvelope,
    status: ReceiptStatus,
    gas_used: u64,
    wire_logs: Vec<kardamom_types::WireLog>,
    write_set_hash: B256,
    nonce: u64,
    signer: alloy_primitives::Address,
    to: Option<alloy_primitives::Address>,
    effective_gas_price: u128,
    block_number: u64,
    local_idx: u32,
}

/// Build one transaction's receipt. `contract_address` is derived here
/// (CREATE at this sender/nonce, only on a successful contract
/// creation); every other field is a direct copy of its argument.
fn build_receipt(args: ReceiptArgs<'_>) -> Receipt {
    let ReceiptArgs {
        position,
        envelope,
        status,
        gas_used,
        wire_logs,
        write_set_hash,
        nonce,
        signer,
        to,
        effective_gas_price,
        block_number,
        local_idx,
    } = args;
    let contract_address = if to.is_none() && status.is_success() {
        Some(signer.create(nonce))
    } else {
        None
    };
    Receipt {
        tx_idx: position,
        tx_hash: envelope.tx_hash,
        tx_type: kardamom_types::tx_type_of(&envelope.raw_tx),
        status: status.is_success(),
        gas_used,
        logs: wire_logs,
        write_set_hash,
        nonce,
        from: signer,
        to,
        contract_address,
        effective_gas_price,
        block_number,
        transaction_index: u64::from(local_idx),
        // Canonical prefix sums land in the commit pass.
        cumulative_gas_used: 0,
        skip_reason: None,
    }
}
#[cfg(test)]
mod fee_delta_tests {
    use super::{ExecutorError, TxIndex, U256, fee_delta_from_sink};

    #[test]
    fn observed_at_or_above_start_credits_the_difference() {
        let start = U256::from(1_000u64);
        let observed = U256::from(1_021u64);
        assert_eq!(
            fee_delta_from_sink(observed, start, 1, TxIndex(0)).unwrap(),
            U256::from(21u64)
        );
        // Untouched this block: observed equals the block-start value.
        assert_eq!(
            fee_delta_from_sink(start, start, 1, TxIndex(0)).unwrap(),
            U256::ZERO
        );
    }

    #[test]
    fn observed_below_start_errors_not_wraps() {
        let start = U256::from(1_000u64);
        let observed = U256::from(999u64);
        let Err(ExecutorError::State(msg)) = fee_delta_from_sink(observed, start, 7, TxIndex(3))
        else {
            panic!("a sink balance below the block-start value must error, not wrap");
        };
        assert!(msg.contains("block-start=1000"), "{msg}");
        assert!(msg.contains("observed=999"), "{msg}");
    }
}
