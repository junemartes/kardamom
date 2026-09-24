use super::config::{MAX_BLOCK_TXS, PoolConfig, STALL_TIMEOUT};
use super::graph::BlockCtx;
use super::metrics::StmOutcome;
use super::prepare::Prepared;
use super::recycle::{RecyclePools, SpentBlock};
use super::session::TailJob;
use super::tail::{TailDeps, TailInput, TailStats, TailTiming, block_tail};
use super::touch::{Pow2, ShardTables, TouchTable};
use super::view::BaseCache;
use super::worker::worker_loop;
use crate::FastMap;
use kardamom_exec_core::error::ExecutorError;
use kardamom_exec_core::exec_types::TxIndex;
use kardamom_footprint::classifier::DomainKey;
use kardamom_types::BPosition;
use kardamom_types::StateDatabase;
use kardamom_types::TxEnvelope;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::atomic::Ordering;

/// One record for [`PoolHandle::run_block_prepared`]: the canonical
/// fields, owned, zipped with the [`Prepared`] result the reader
/// threads computed for it. Owned so `run_block_prepared` moves each
/// record into admission with no per-transaction clone.
pub struct PreparedTx {
    pub tx_idx: TxIndex,
    pub position: BPosition,
    pub envelope: TxEnvelope,
    pub prepared: Prepared,
}

pub(super) struct PoolState<S: StateDatabase> {
    pub(super) generation: u64,
    pub(super) ctx: Option<Arc<BlockCtx<S>>>,
    /// The next block, fed while `ctx` still executes (pipeline depth 2).
    /// The tail installs it the moment `ctx` drains; the generation bump
    /// walks the workers over. Feeding needs no workers, so admission of
    /// block N+1 overlaps execution of block N entirely.
    pub(super) next: Option<Arc<BlockCtx<S>>>,
    pub(super) shutdown: bool,
    pub(super) cfg: PoolConfig,
}

/// The pool's live block state, plus the condvar every wait on it uses.
/// One instance, shared by every worker, the feed thread, the reaper,
/// and the tail thread for the pool's whole lifetime.
pub(super) struct PoolShared<S: StateDatabase> {
    pub(super) state: Mutex<PoolState<S>>,
    pub(super) wake: Condvar,
}

impl<S: StateDatabase> PoolShared<S> {
    /// Block until a new block is staged for `worker`, or the pool
    /// shuts down. `seen` is the generation this worker last executed;
    /// this call advances it in place. `None` means shutdown: the
    /// caller must leave.
    pub(super) fn next_ctx(&self, keep_hot: bool, seen: &mut u64) -> Option<Arc<BlockCtx<S>>> {
        if keep_hot {
            return self.next_ctx_hot(seen);
        }
        let mut st = self.state.lock().expect("pool poisoned");
        while !(st.shutdown || (st.generation != *seen && st.ctx.is_some())) {
            st = self.wake.wait(st).expect("pool poisoned");
        }
        if st.shutdown {
            return None;
        }
        *seen = st.generation;
        st.ctx.clone()
    }

    /// `next_ctx`'s `keep_hot` path: never park on the condvar, so the
    /// core's frequency stays up. Poll the lock, then spin a fixed
    /// count of yields before polling again. The `loop` in
    /// [`Self::next_ctx_hot`] stays free of a branch.
    fn next_ctx_hot(&self, seen: &mut u64) -> Option<Arc<BlockCtx<S>>> {
        loop {
            match self.poll_hot(seen) {
                HotCtx::Ready(c) => return Some(c),
                HotCtx::Shutdown => return None,
                // keep_hot: hold the core's frequency, hand it over
                // instantly to whoever needs it (the commit tail pins
                // its threads here). `poll_hot` already ran the spin
                // before returning this, so there is nothing left to
                // do but poll again.
                HotCtx::Spin => (),
            }
        }
    }

    /// One `next_ctx_hot` poll: shutdown, a fresh context, or a spin
    /// (already run, lock released first) before the next poll.
    fn poll_hot(&self, seen: &mut u64) -> HotCtx<S> {
        {
            let st = self.state.lock().expect("pool poisoned");
            if st.shutdown {
                return HotCtx::Shutdown;
            }
            if st.generation != *seen
                && let Some(c) = &st.ctx
            {
                *seen = st.generation;
                return HotCtx::Ready(c.clone());
            }
        }
        Self::spin_hot();
        HotCtx::Spin
    }

    /// A fixed count of yields between `next_ctx_hot` polls, so the
    /// core's frequency stays up instead of parking.
    fn spin_hot() {
        for _ in 0..64 {
            std::thread::yield_now();
        }
    }
}

/// One [`PoolShared::poll_hot`] outcome.
enum HotCtx<S: StateDatabase> {
    /// A fresh context is ready.
    Ready(Arc<BlockCtx<S>>),
    /// The pool shut down.
    Shutdown,
    /// No change yet; keep spinning.
    Spin,
}

/// A persistent worker pool bound to one snapshot view for its lifetime:
/// the pipeline shape the live executor needs. Workers are spawned once
/// (no per-block thread cost), and each block is a session whose
/// transactions are admitted as they arrive from the sealer stream.
/// Canonical arrival makes the DAG incremental: a transaction's
/// predecessors are always already admitted, so execution overlaps the
/// feed, and sealing at the boundary only waits out the tail, validates,
/// and commits. `run_block` is the batch convenience; `begin_block`,
/// `push_tx`, and `seal` form the actor-shaped API (the
/// `ReaderToExec::Tx` arm pushes, the `Boundary` arm seals).
pub struct PoolHandle<'a, S: StateDatabase + Sync> {
    pub(super) shared: &'a PoolShared<S>,
    /// Mean per-transaction execution time of the last block, feeding
    /// the stealing policy. Atomic, not `Cell`: the persistent tail
    /// thread updates it after each block's fold while the feed thread
    /// reads it.
    pub(super) avg_tx_ns: std::sync::Arc<std::sync::atomic::AtomicU64>,
    pub(super) parallel_worth_ns: u64,
    pub(super) dispatch_by_sender: bool,
    pub(super) eager_chain: bool,
    pub(super) sticky_assign: bool,
    /// Domain to worker, pool-lifetime (feed-thread-owned). Capped: past
    /// `STICKY_CAP` entries, new domains fall back to hashing, so a
    /// long-lived pool cannot grow this without bound.
    pub(super) assign: std::cell::RefCell<FastMap<DomainKey, usize>>,
    /// Cumulative transactions dispatched per worker: the load the
    /// least-loaded choice reads.
    pub(super) assign_load: std::cell::RefCell<Vec<u64>>,
    /// The persistent tail thread's inbox: sealed blocks go
    /// here. The thread drains, releases the pool slot, and runs
    /// `block_tail` while the caller feeds the next block.
    pub(super) tail: std::sync::mpsc::Sender<TailJob<S>>,
    /// Pool-lifetime cache of the backend layer, below any pending-delta
    /// layer, which is probed before it (see `MvView`). Measurement
    /// showed almost all reads reach mdbx directly, because hot cells
    /// change every block, so a per-block cache can never hit. But the
    /// block's own delta carries every new value: `advance_base` upserts
    /// it, turning next block's backend reads into warm map hits. This
    /// is valid only if every backend commit is mirrored here, which the
    /// A/B harness's byte-identical-results check confirms.
    pub(super) base_cache: std::sync::Arc<BaseCache>,
    /// Recycled block structures (see [`RecyclePools`]).
    pub(super) recycle: std::sync::Arc<RecyclePools>,
    /// The feed's last-toucher index: pool-lifetime (allocated once,
    /// O(1) stamp reset per block) and feed-owned, exactly like
    /// `assign`. Only the single admission thread ever touches it.
    pub(super) touch: std::cell::RefCell<TouchTable>,
    /// Sharded admission: one last-toucher table per cell-space shard,
    /// plus the lanes that drive them. Shard k is touched only by the
    /// lane running chunk k, and lanes run one chunk each per batch.
    pub(super) shards: std::sync::Arc<ShardTables>,
    pub(super) admit_lanes: Option<std::sync::Arc<crate::pool::WorkerPool>>,
    pub(super) admit_shards: Option<std::num::NonZeroUsize>,
}

/// The pool's two persistent background threads: the reaper and the
/// tail. One instance per pool lifetime. `spawn` starts both threads;
/// `shutdown` drops the pool's own last clone of the tail thread's
/// inbox and wakes any parked worker. The reaper's own inbox has no
/// separate sender here: its only sender lives inside the tail
/// thread's `TailDeps` (see `spawn_tail`), so the reaper's `recv()`
/// loop ends once the tail thread exits, which happens after the pool
/// drops `tail_tx`.
struct PoolThreads<S: StateDatabase> {
    tail_tx: std::sync::mpsc::Sender<TailJob<S>>,
}

/// The tail thread's non-channel resources: the stealing policy's
/// learned average, the recycle pools, and the persistent hash/
/// validate lanes. Built once in `with_pool`.
struct TailResources {
    avg_tx_ns: std::sync::Arc<std::sync::atomic::AtomicU64>,
    recycle: std::sync::Arc<RecyclePools>,
    lanes: std::sync::Arc<crate::pool::WorkerPool>,
}

impl<S: super::StmBackend> PoolThreads<S> {
    /// Spawn the reaper and the persistent tail thread on `scope`, and
    /// return a handle owning each thread's inbox.
    fn spawn<'scope, 'env>(
        scope: &'scope std::thread::Scope<'scope, 'env>,
        shared_ref: &'env PoolShared<S>,
        resources: &TailResources,
    ) -> Self {
        let (reap_tx, reap_rx) = std::sync::mpsc::channel::<SpentBlock>();
        let (tail_tx, tail_rx) = std::sync::mpsc::channel::<TailJob<S>>();
        Self::spawn_reaper(scope, reap_rx);
        Self::spawn_tail(scope, tail_rx, shared_ref, reap_tx, resources);
        Self { tail_tx }
    }

    /// This pool's tail-thread sender, cloned for [`PoolHandle::tail`].
    fn tail_sender(&self) -> std::sync::mpsc::Sender<TailJob<S>> {
        self.tail_tx.clone()
    }

    /// Spawn worker `w`'s thread on `scope`: pin it to its assigned core
    /// (a warning, not a failure, if the pin does not take), then run
    /// its loop until the pool shuts down.
    fn spawn_worker<'scope>(
        scope: &'scope std::thread::Scope<'scope, '_>,
        shared_ref: &'scope PoolShared<S>,
        w: usize,
        pin_cores: &[usize],
        hot: bool,
    ) {
        let pin = pin_cores.to_vec();
        scope.spawn(move || {
            if crate::pin_current(w, &pin) == Some(false) {
                tracing::warn!(
                    worker = w,
                    core = pin[w % pin.len()],
                    "stm: worker pin failed"
                );
            }
            worker_loop(shared_ref, w, hot);
        });
    }

    /// Spawn the reaper: it drops junk freight and scrubs recyclable
    /// arenas in place (drops entries, keeps buffers), so `seal` pays
    /// for neither, and the next session build maps nothing. Exits
    /// when the pool drops the last sender.
    fn spawn_reaper<'scope>(
        scope: &'scope std::thread::Scope<'scope, '_>,
        reap_rx: std::sync::mpsc::Receiver<SpentBlock>,
    ) {
        scope.spawn(move || {
            while let Ok(r) = reap_rx.recv() {
                r.reap();
            }
        });
    }

    /// Wait out the in-flight tail of execution, the same watchdog as
    /// the inline path: a stranded edge fail-stops with forensics
    /// instead of freezing.
    fn drain_block(ctx: &BlockCtx<S>) -> Option<ExecutorError> {
        let deadline = std::time::Instant::now() + STALL_TIMEOUT;
        while !(ctx.aborted.load(Ordering::SeqCst) || ctx.drained()) {
            match Self::drain_step(ctx, deadline) {
                DrainStep::Continue => (),
                DrainStep::Stalled(err) => return Some(err),
            }
        }
        None
    }

    /// One [`Self::drain_block`] poll: past the deadline, still pending
    /// (prune and keep waiting), or idle (yield and keep waiting).
    fn drain_step(ctx: &BlockCtx<S>, deadline: std::time::Instant) -> DrainStep {
        if std::time::Instant::now() > deadline {
            return DrainStep::Stalled(Self::stall_error(ctx));
        }
        if ctx.pending.load(Ordering::SeqCst) > 0 {
            ctx.prune(true);
            return DrainStep::Continue;
        }
        std::thread::yield_now();
        DrainStep::Continue
    }

    /// Build the drain-watchdog error: which transactions are still
    /// unresolved, and why the block is stuck.
    fn stall_error(ctx: &BlockCtx<S>) -> ExecutorError {
        let admitted = ctx.admitted.load(Ordering::SeqCst);
        let finished = ctx.finished.load(Ordering::SeqCst);
        let stuck: Vec<(u32, u32)> = (0..admitted)
            .filter(|i| ctx.results[*i as usize].get().is_none())
            .map(|i| (i, ctx.nodes[i as usize].indegree.load(Ordering::SeqCst)))
            .take(16)
            .collect();
        tracing::error!(
            block = ctx.env.block_number,
            admitted,
            finished,
            double_exit = ctx.double_exit.load(Ordering::SeqCst),
            ?stuck,
            "stm: block failed to drain — scheduler invariant violated"
        );
        ExecutorError::State(format!(
            "stm: block {} failed to drain after {:?}: admitted={admitted} \
finished={finished} stuck(idx,indegree)={stuck:?}",
            ctx.env.block_number, STALL_TIMEOUT
        ))
    }

    /// Wait out any worker still holding `ctx_arc` (wedged in a stall
    /// path), then return it unwrapped. `None` means the watchdog
    /// fired: a wedge error was already sent on `out`, and the ctx was
    /// leaked rather than spun on forever (the wedged worker still
    /// references it).
    fn unwrap_ctx(
        mut ctx_arc: Arc<BlockCtx<S>>,
        out: &std::sync::mpsc::Sender<Result<StmOutcome, ExecutorError>>,
    ) -> Option<BlockCtx<S>> {
        let unwrap_deadline = std::time::Instant::now() + STALL_TIMEOUT;
        loop {
            match Self::unwrap_step(ctx_arc, unwrap_deadline, out) {
                UnwrapStep::Done(ctx) => return Some(*ctx),
                UnwrapStep::Retry(next) => ctx_arc = next,
                UnwrapStep::Wedged => return None,
            }
        }
    }

    /// One [`Self::unwrap_ctx`] step: try the unwrap, and on failure run
    /// one [`Self::wedge_step`]. The `loop` in [`Self::unwrap_ctx`] stays
    /// free of a branch.
    fn unwrap_step(
        ctx_arc: Arc<BlockCtx<S>>,
        unwrap_deadline: std::time::Instant,
        out: &std::sync::mpsc::Sender<Result<StmOutcome, ExecutorError>>,
    ) -> UnwrapStep<S> {
        match Arc::try_unwrap(ctx_arc) {
            Ok(ctx) => UnwrapStep::Done(Box::new(ctx)),
            Err(back) => match Self::wedge_step(back, unwrap_deadline, out) {
                WedgeStep::Retry(next) => UnwrapStep::Retry(next),
                WedgeStep::Wedged => UnwrapStep::Wedged,
            },
        }
    }

    /// One [`Self::unwrap_ctx`] retry step, for an `Arc` some other
    /// holder still shares: past the watchdog deadline (wedged; send
    /// the error on `out` and leak the ctx rather than spin, since the
    /// wedged worker still references it), or not yet (yield and hand
    /// `back` on for another try).
    fn wedge_step(
        back: Arc<BlockCtx<S>>,
        unwrap_deadline: std::time::Instant,
        out: &std::sync::mpsc::Sender<Result<StmOutcome, ExecutorError>>,
    ) -> WedgeStep<S> {
        // Watchdog: a worker that never drops its Arc (wedged in a
        // stall path) would spin this loop forever and silently hang
        // every later ticket. Fail loudly instead.
        if std::time::Instant::now() <= unwrap_deadline {
            std::thread::yield_now();
            return WedgeStep::Retry(back);
        }
        let holders = Arc::strong_count(&back);
        eprintln!(
            "stm WEDGE: ctx unwrap stalled {}s, {} Arc holders, \
block {}, fifo_stalls {}",
            STALL_TIMEOUT.as_secs(),
            holders,
            back.env.block_number,
            back.metrics.fifo_stalls.load(Ordering::Relaxed),
        );
        let _ = out.send(Err(ExecutorError::State(format!(
            "stm: ctx unwrap stalled, {holders} holders"
        ))));
        std::mem::forget(back);
        WedgeStep::Wedged
    }

    /// Spawn the persistent tail thread. One thread owns every block's
    /// post-drain work, in submission order. Per-block scoped threads
    /// for sub-millisecond phases measured as a net loss. This thread
    /// is also what lets the caller feed block N+1 while block N
    /// validates and commits.
    fn spawn_tail<'scope, 'env>(
        scope: &'scope std::thread::Scope<'scope, 'env>,
        tail_rx: std::sync::mpsc::Receiver<TailJob<S>>,
        shared_ref: &'env PoolShared<S>,
        reap_tx: std::sync::mpsc::Sender<SpentBlock>,
        resources: &TailResources,
    ) {
        // The tail thread owns the only sender into the reaper's
        // inbox; the reaper's recv() loop ends once this thread exits.
        let deps = TailDeps {
            reaper: reap_tx,
            avg_tx_ns: resources.avg_tx_ns.clone(),
            recycle: resources.recycle.clone(),
            lanes: resources.lanes.clone(),
        };
        scope.spawn(move || {
            while let Ok(job) = tail_rx.recv() {
                Self::process_tail_job(job, shared_ref, &deps);
            }
        });
    }

    /// Drain, release the slot, unwrap, and run one block's tail work.
    /// The `while` loop in [`Self::spawn_tail`] stays free of a branch.
    fn process_tail_job(job: TailJob<S>, shared_ref: &PoolShared<S>, deps: &TailDeps) {
        let TailJob {
            ctx,
            n_txs,
            started,
            cold,
            edges,
            dispatch,
            out,
            delta_out,
            feed,
        } = job;
        let drain_err = Self::drain_block(&ctx);
        Self::release_slot(shared_ref);
        if let Some(e) = drain_err {
            let _ = out.send(Err(e));
            return;
        }
        let t_exec_wall = started.elapsed();
        let t_drain0 = std::time::Instant::now();
        let Some(ctx) = Self::unwrap_ctx(ctx, &out) else {
            return;
        };
        let t_drain = t_drain0.elapsed();
        let _ = out.send(block_tail(
            TailInput {
                ctx,
                n_txs,
                delta_out,
            },
            TailTiming {
                exec_wall: t_exec_wall,
                drain: t_drain,
            },
            TailStats {
                cold,
                edges,
                dispatch,
                feed,
            },
            deps.clone(),
        ));
    }

    /// Release the current block's slot, and install the staged block,
    /// if any: its admission ran while this block executed, so the
    /// workers walk straight onto full queues.
    fn release_slot(shared_ref: &PoolShared<S>) {
        {
            let mut st = shared_ref.state.lock().expect("pool poisoned");
            st.ctx = st.next.take();
            if st.ctx.is_some() {
                st.generation += 1;
            }
        }
        shared_ref.wake.notify_all();
    }

    /// Close the pool down: flag `shutdown` for any worker still
    /// parked and wake every queue. `self` (the pool's own last clone
    /// of the tail thread's inbox) drops at return, closing that
    /// channel; the tail thread exits once its `recv()` loop sees that,
    /// which in turn drops its own sender into the reaper's inbox, so
    /// the reaper exits in its turn.
    #[allow(
        clippy::unused_self,
        reason = "self is consumed for its Drop effect (closing the tail thread's inbox), not read; an associated function would have nothing to consume"
    )]
    fn shutdown(self, shared: &PoolShared<S>) {
        {
            let mut st = shared.state.lock().expect("pool poisoned");
            st.shutdown = true;
        }
        shared.wake.notify_all();
    }
}

/// One [`PoolThreads::drain_step`] outcome.
enum DrainStep {
    /// The watchdog deadline passed; the block is stuck.
    Stalled(ExecutorError),
    /// Not drained yet; keep waiting.
    Continue,
}

/// One [`PoolThreads::wedge_step`] outcome.
enum WedgeStep<S: super::StmBackend> {
    /// Still shared; keep waiting with this `Arc`.
    Retry(Arc<BlockCtx<S>>),
    /// Past the watchdog deadline; the ctx is leaked.
    Wedged,
}

/// One [`PoolThreads::unwrap_step`] outcome.
enum UnwrapStep<S: super::StmBackend> {
    /// The `Arc` unwrapped; here is the owned context, boxed so this
    /// variant does not force every other variant to reserve its size.
    Done(Box<BlockCtx<S>>),
    /// Still shared; keep waiting with this `Arc`.
    Retry(Arc<BlockCtx<S>>),
    /// Past the watchdog deadline; the ctx is leaked.
    Wedged,
}

/// Spawn `workers` pool threads for the duration of `f`.
///
/// # Panics
/// Panics if a worker thread fails to spawn.
pub fn with_pool<S: super::StmBackend, R>(
    cfg: PoolConfig,
    f: impl FnOnce(&PoolHandle<'_, S>) -> R,
) -> R {
    let workers_nz = cfg.workers;
    let workers = workers_nz.get();
    let pin_cores = cfg.pin_cores.clone();
    let cfg_keep_hot = cfg.keep_hot;
    let cfg_tail_on_workers = cfg.tail_on_workers;
    let cfg_admit_shards = cfg.admit_shards;
    let fifo_opts = cfg.scheduler.fifo_options();
    let (sticky_assign, parallel_worth_ns, dispatch_by_sender, eager_chain) = (
        fifo_opts.sticky_assign,
        cfg.parallel_worth_ns,
        fifo_opts.dispatch_by_sender,
        fifo_opts.eager_chain,
    );
    let shared: PoolShared<S> = PoolShared {
        state: Mutex::new(PoolState {
            generation: 0,
            ctx: None,
            next: None,
            shutdown: false,
            cfg,
        }),
        wake: Condvar::new(),
    };
    let shared_ref = &shared;
    let avg_tx_ns = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    // Persistent tail lanes (see crate::pool): the hash-and-validate
    // chunks run on threads created once, not spawned per block.
    let lane_pool = std::sync::Arc::new(crate::pool::WorkerPool::new(
        workers_nz,
        if cfg_keep_hot && cfg_tail_on_workers {
            &pin_cores
        } else {
            &[]
        },
    ));
    let recycle_pools = std::sync::Arc::new(RecyclePools {
        arenas: Mutex::new(Vec::new()),
        mv_clean: Mutex::new(Vec::new()),
        mv_parked: Mutex::new(Vec::new()),
        read_bufs: Mutex::new(Vec::new()),
        deltas: Mutex::new(Vec::new()),
    });
    std::thread::scope(|scope| {
        let tail_resources = TailResources {
            avg_tx_ns: avg_tx_ns.clone(),
            recycle: recycle_pools.clone(),
            lanes: lane_pool.clone(),
        };
        let threads = PoolThreads::spawn(scope, shared_ref, &tail_resources);
        for w in 0..workers {
            PoolThreads::spawn_worker(scope, shared_ref, w, &pin_cores, cfg_keep_hot);
        }
        let handle = PoolHandle {
            shared: shared_ref,
            avg_tx_ns: avg_tx_ns.clone(),
            sticky_assign,
            assign: std::cell::RefCell::new(FastMap::default()),
            assign_load: std::cell::RefCell::new(vec![0; workers]),
            base_cache: std::sync::Arc::new(BaseCache::new()),
            recycle: recycle_pools.clone(),
            // 4x MAX_BLOCK_TXS slots: a block's live cell count is at
            // most ~2 per tx, so load factor stays <= 0.5 (about 1.5
            // probes) at 256KB total.
            touch: std::cell::RefCell::new(TouchTable::new(Pow2::new(MAX_BLOCK_TXS * 4))),
            shards: std::sync::Arc::new(ShardTables::new(
                cfg_admit_shards.map_or(1, std::num::NonZeroUsize::get),
                // Per shard; `ShardTables::new` rounds it up to a power
                // of two, and rounding down would also crowd the table
                // (cells do not divide evenly across shards). Floored at
                // 64: past `admit_shards > 16_384` the plain quotient
                // hits 0, which rounds up to a 1-slot table whose probe
                // loop never ends.
                ((MAX_BLOCK_TXS * 4) / cfg_admit_shards.map_or(1, std::num::NonZeroUsize::get))
                    .max(64),
            )),
            admit_lanes: cfg_admit_shards.map(|k| {
                // These lanes run on caller cores; the worker cores stay
                // dedicated to execution, which runs while the feed
                // admits.
                std::sync::Arc::new(crate::pool::WorkerPool::new(k, &[]))
            }),
            admit_shards: cfg_admit_shards,
            tail: threads.tail_sender(),
            parallel_worth_ns,
            dispatch_by_sender,
            eager_chain,
        };
        // `handle` (and the tail-sender clone it owns) drops at the end
        // of this block, before `threads.shutdown` closes the pool's
        // own senders: the tail thread's `recv()` loop exits only once
        // every clone is gone.
        let r = f(&handle);
        threads.shutdown(shared_ref);
        r
    })
}
