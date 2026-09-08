//! One worker's job loop: pull the next DAG-ready transaction, dispatch
//! it to [`worker_execute::execute_one`], and publish or abort. See
//! `worker_execute.rs` for the execute path itself.

use super::config::{PARK_POLL, SPIN_BEFORE_PARK, nanos};
use super::graph::{BlockCtx, WorkerQueue};
use super::handle::PoolShared;
use super::metrics::TxResult;
use super::prepare::domain_hash;
use super::session::BoundLayers;
use super::view::{BlockInput, MvView};
use super::worker_execute::{ExecCtx, TxJob, WorkerEvm, execute_one, take_read_buf};
use crate::FEE_SINK;
use crate::mv::ReadRecord;
use kardamom_exec_core::error::ExecutorError;
use kardamom_types::StateDatabase;
use revm::Context;
use revm::MainBuilder;
use revm::MainContext;
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
    while ctx.binding.get().is_none() && !ctx.aborted.load(Ordering::SeqCst) {
        std::thread::yield_now();
    }
    ctx.binding.get()
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
        match acquire.poll_once() {
            JobPoll::Ready(i) => return Some(i),
            JobPoll::Done => return None,
            JobPoll::Retry => {}
        }
    }
}

/// One [`Acquire::poll_once`] outcome.
#[derive(Clone, Copy)]
enum JobPoll {
    Ready(u32),
    /// The block aborted or fully drained; the caller must leave.
    Done,
    /// No decision yet; poll again.
    Retry,
}

/// What [`Acquire::initial_poll`] found.
enum Pop {
    Ready(u32),
    /// A FIFO pop came back, but its predecessor is still running
    /// elsewhere (`ctx.fifo_ready` said no); already put back.
    Stalled,
    Dry,
}

/// One [`Acquire::spin_poll`] outcome.
#[derive(Clone, Copy)]
enum SpinPoll {
    /// The queue (or bag) has work.
    Found,
    /// No decision yet; spin again.
    Retry,
    /// The block ended, or the spin budget ran out.
    GiveUp,
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
    /// One [`next_job`] iteration: a ready index, done (aborted or
    /// drained; the caller must leave), or retry (spun, parked, or
    /// found more dry-queue work to apply first).
    fn poll_once(&mut self) -> JobPoll {
        if self.ctx.aborted.load(Ordering::SeqCst) {
            return JobPoll::Done;
        }
        match self.initial_poll() {
            Pop::Ready(i) => return JobPoll::Ready(i),
            Pop::Stalled => {
                // A FIFO predecessor was stolen and is still running on
                // another thread. This is rare, and bounded by that
                // transaction's execution time. Yield and retry.
                std::thread::yield_now();
                return JobPoll::Retry;
            }
            Pop::Dry => {}
        }
        // Dry. Apply any parked completions myself before parking:
        // this is what makes batching safe, since the pool can never
        // sit idle on DAG updates nobody applied.
        if self.apply_pending() {
            return JobPoll::Retry;
        }
        if self.ctx.drained() {
            return JobPoll::Done;
        }
        // Domain hashing collides when the number of domains is close
        // to the number of workers, so the busiest thread can take
        // well over its even share while some threads get nothing at
        // all. So an idle worker helps the busiest one rather than
        // parking.
        if let Some(stolen) = self.try_steal() {
            return JobPoll::Ready(stolen);
        }
        if self.queue_has_work() {
            return JobPoll::Retry;
        }
        self.spin_or_park();
        JobPoll::Retry
    }

    /// Spin before parking: at high throughput the next transaction is
    /// usually microseconds away, and a park/unpark pair costs two
    /// syscalls, more than a small transfer's entire execution. Only a
    /// worker that stays dry through the spin advertises itself as
    /// parked and blocks.
    fn spin_or_park(&self) {
        let t_idle = std::time::Instant::now();
        if !self.spin() {
            self.park();
        }
        self.ctx
            .metrics
            .idle_ns
            .fetch_add(nanos(t_idle.elapsed()), Ordering::Relaxed);
    }

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
            Self::spin_burst();
            match self.spin_poll(spin_start) {
                SpinPoll::Found => return true,
                SpinPoll::GiveUp => return false,
                SpinPoll::Retry => {}
            }
        }
    }

    /// One fixed-size burst of spin hints between [`Self::spin`]'s
    /// polls.
    fn spin_burst() {
        for _ in 0..SPIN_BEFORE_PARK {
            std::hint::spin_loop();
        }
    }

    /// One [`Self::spin`] poll, after a burst: work found, keep
    /// spinning, or give up (the block ended or the spin budget ran
    /// out). The `loop` in [`Self::spin`] stays free of a branch.
    fn spin_poll(&self, spin_start: std::time::Instant) -> SpinPoll {
        // Lock-free probe: a dry worker spinning here for tens of
        // microseconds must not contend with the feed's push into this
        // very queue.
        if self.qh.len.load(Ordering::Acquire) > 0
            || (self.ctx.bag_mode && !self.ctx.bag.is_empty())
        {
            return SpinPoll::Found;
        }
        // Completions may be parked while we spin. Apply them ourselves
        // rather than spin past the work they would release.
        if self.apply_pending() {
            return SpinPoll::Retry;
        }
        if self.ctx.drained() || self.ctx.aborted.load(Ordering::SeqCst) {
            return SpinPoll::GiveUp;
        }
        if nanos(spin_start.elapsed()) >= self.ctx.spin_ns {
            return SpinPoll::GiveUp;
        }
        SpinPoll::Retry
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
    let Ok(res) = r else {
        return (0, 0);
    };
    res.ws.accounts.iter().fold(
        (0u64, 0u64),
        |(own, foreign), (addr, _)| match tally_write_domain(*addr, worker, n_workers) {
            WriteDomain::Own => (own + 1, foreign),
            WriteDomain::Foreign => (own, foreign + 1),
            WriteDomain::Deferred => (own, foreign),
        },
    )
}

/// One written account's domain: the deferred fee sink, this worker's
/// own write, or a foreign one.
enum WriteDomain {
    Own,
    Foreign,
    /// The fee sink: never published, so it counts as neither.
    Deferred,
}

/// Classify one written account's domain against `worker`.
fn tally_write_domain(
    addr: alloy_primitives::Address,
    worker: usize,
    n_workers: usize,
) -> WriteDomain {
    if addr == FEE_SINK {
        return WriteDomain::Deferred;
    }
    if domain_hash(addr.as_slice(), n_workers) == worker {
        WriteDomain::Own
    } else {
        WriteDomain::Foreign
    }
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

/// Build this worker's EVM for the block: its multi-version view, aimed
/// at this worker's snapshot and the session's bound read base, wrapped
/// in a mainnet EVM built once and reused across every transaction the
/// worker runs. `input` lives in the caller's frame (its `snapshot`
/// field is owned there), since the returned view borrows it for the
/// worker's whole block.
fn build_worker_evm<'a, S: StateDatabase>(
    ctx: &'a BlockCtx<S>,
    input: &'a BlockInput<'a, S>,
    bound: &'a BoundLayers,
) -> WorkerEvm<'a, S> {
    let view = MvView::new(
        &ctx.mv,
        input,
        bound.sink_start.clone(),
        &ctx.base_cache,
        &ctx.metrics,
    );
    Context::mainnet()
        .with_db(view)
        .with_block(ctx.env.block_env())
        .with_cfg(ctx.env.cfg_env())
        .build_mainnet()
}

/// Timing and write-domain counts a worker accumulates locally across
/// the whole block, flushed once through [`flush_local_metrics`]. A
/// `fetch_add` on shared metrics per read, or even per transaction, had
/// every worker writing the same cache lines; the instrumentation was
/// generating the very cross-core traffic it aimed to measure.
#[derive(Default)]
struct LocalTotals {
    busy_ns: u64,
    first_ns: u64,
    last_ns: u64,
    own: u64,
    foreign: u64,
}

impl LocalTotals {
    fn new() -> Self {
        Self {
            first_ns: u64::MAX,
            ..Self::default()
        }
    }

    /// Fold one dispatched transaction's timing and write-domain split
    /// into the running totals.
    fn record(&mut self, t_busy_at: u64, done_at: u64, own: u64, foreign: u64) {
        self.busy_ns = self
            .busy_ns
            .saturating_add(done_at.saturating_sub(t_busy_at));
        self.first_ns = self.first_ns.min(t_busy_at);
        self.last_ns = self.last_ns.max(done_at);
        self.own += own;
        self.foreign += foreign;
    }
}

/// Flush one worker's whole-block local totals into the shared metrics,
/// and its view's read counters. Called once, at the end of the
/// worker's dispatch loop.
fn flush_local_metrics<S: StateDatabase>(
    ctx: &BlockCtx<S>,
    worker: usize,
    evm: &mut WorkerEvm<'_, S>,
    locals: &LocalTotals,
) {
    revm::context_interface::ContextTr::db_mut(&mut **evm).flush_counters();
    ctx.metrics
        .busy_ns
        .fetch_add(locals.busy_ns, Ordering::Relaxed);
    ctx.metrics.busy_per_worker[worker]
        .0
        .fetch_add(locals.busy_ns, Ordering::Relaxed);
    ctx.metrics
        .writes_own
        .fetch_add(locals.own, Ordering::Relaxed);
    ctx.metrics
        .writes_foreign
        .fetch_add(locals.foreign, Ordering::Relaxed);
    if locals.first_ns != u64::MAX {
        ctx.metrics
            .first_dispatch_ns
            .fetch_min(locals.first_ns, Ordering::Relaxed);
        ctx.metrics
            .last_done_ns
            .fetch_max(locals.last_ns, Ordering::Relaxed);
    }
}

/// Record one transaction's execution result: continue on success, or
/// abort the block (no metrics flush; the aborted path leaves state for
/// forensics, not for the caller to keep measuring) on failure. The
/// loop in [`run_worker_block`] dispatches on the returned value instead
/// of matching a bool.
fn record_result<S: StateDatabase>(
    ctx: &BlockCtx<S>,
    job: u32,
    r: Result<TxResult, ExecutorError>,
) -> std::ops::ControlFlow<()> {
    let errored = r.is_err();
    let _ = ctx.results[job as usize].set(r);
    if errored {
        ctx.aborted.store(true, Ordering::SeqCst);
        ctx.wake_all();
        return std::ops::ControlFlow::Break(());
    }
    std::ops::ControlFlow::Continue(())
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
    let mut evm = build_worker_evm(ctx, &input, bound);
    let mut read_stash: Vec<Vec<ReadRecord>> = Vec::new();
    let mut local_next: Option<u32> = None;
    let qh = &ctx.queues[worker];
    let mut locals = LocalTotals::new();
    let n_workers = ctx.queues.len();
    loop {
        let Some(job) = next_job(ctx, worker, qh, &mut local_next) else {
            flush_local_metrics(ctx, worker, &mut evm, &locals);
            return;
        };
        let slot = ctx.slot(job as usize);
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
        let (own, foreign) = record_write_domains(&r, worker, n_workers);
        locals.record(t_busy_at, done_at, own, foreign);
        match record_result(ctx, job, r) {
            std::ops::ControlFlow::Break(()) => return,
            std::ops::ControlFlow::Continue(()) => complete_job(ctx, worker, job, &mut local_next),
        }
    }
}
