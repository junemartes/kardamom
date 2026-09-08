use super::config::{
    ADMIT_BATCH, MAX_BLOCK_TXS, PARK_POLL, PoolConfig, STALL_TIMEOUT, STEAL_WORTH_NS, account_info,
    nanos,
};
use super::graph::{BlockCtx, Node, PerWorker, WorkerQueue};
use super::metrics::{FeedTimings, Metrics, PaddedLen, PaddedLen64, StmOutcome};
use super::prepare::Prepared;
use super::recycle::{RecyclePools, SpentArena, SpentBlock};
use super::sequential::execute_block_sequential;
use super::session::{BlockSession, LayerBinder, ReadBase, TailJob};
use super::tail::{TailDeps, TailInput, TailStats, TailTiming, block_tail};
use super::touch::{Pow2, ShardTables, TouchTable};
use super::view::BaseCache;
use super::worker::worker_loop;
use crate::FastMap;
use crate::mv::MvCache;
use kardamom_exec_core::block_env::ExecEnv;
use kardamom_exec_core::delta::PendingDelta;
use kardamom_exec_core::error::ExecutorError;
use kardamom_exec_core::exec_types::TxIndex;
use kardamom_footprint::classifier::DomainKey;
use kardamom_footprint::classifier::Stats;
use kardamom_types::BPosition;
use kardamom_types::StateDatabase;
use kardamom_types::TxEnvelope;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU32;
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
        loop {
            if st.shutdown {
                return None;
            }
            if st.generation != *seen
                && let Some(c) = &st.ctx
            {
                *seen = st.generation;
                return Some(c.clone());
            }
            st = self.wake.wait(st).expect("pool poisoned");
        }
    }

    /// `next_ctx`'s `keep_hot` path: never park on the condvar, so the
    /// core's frequency stays up. Poll the lock, then spin a fixed
    /// count of yields before polling again.
    fn next_ctx_hot(&self, seen: &mut u64) -> Option<Arc<BlockCtx<S>>> {
        loop {
            {
                let st = self.state.lock().expect("pool poisoned");
                if st.shutdown {
                    return None;
                }
                if st.generation != *seen
                    && let Some(c) = &st.ctx
                {
                    *seen = st.generation;
                    return Some(c.clone());
                }
            }
            // keep_hot: hold the core's frequency, hand it over
            // instantly to whoever needs it (the commit tail pins its
            // threads here).
            for _ in 0..64 {
                std::thread::yield_now();
            }
        }
    }
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
            if std::time::Instant::now() > deadline {
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
                return Some(ExecutorError::State(format!(
                    "stm: block {} failed to drain after {:?}: admitted={admitted} \
finished={finished} stuck(idx,indegree)={stuck:?}",
                    ctx.env.block_number, STALL_TIMEOUT
                )));
            }
            if ctx.pending.load(Ordering::SeqCst) > 0 {
                ctx.prune(true);
                continue;
            }
            std::thread::yield_now();
        }
        None
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
            match Arc::try_unwrap(ctx_arc) {
                Ok(c) => return Some(c),
                Err(back) => {
                    // Watchdog: a worker that never drops its Arc
                    // (wedged in a stall path) would spin this loop
                    // forever and silently hang every later ticket.
                    // Fail loudly instead.
                    if std::time::Instant::now() > unwrap_deadline {
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
                        // Leak the ctx rather than spin: the wedged
                        // worker still references it.
                        std::mem::forget(back);
                        return None;
                    }
                    ctx_arc = back;
                    std::thread::yield_now();
                }
            }
        }
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
            'jobs: while let Ok(job) = tail_rx.recv() {
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
                // Release the slot in every path, and install the
                // staged block, if any: its admission ran while this
                // block executed, so the workers walk straight onto
                // full queues.
                {
                    let mut st = shared_ref.state.lock().expect("pool poisoned");
                    st.ctx = st.next.take();
                    if st.ctx.is_some() {
                        st.generation += 1;
                    }
                }
                shared_ref.wake.notify_all();
                if let Some(e) = drain_err {
                    let _ = out.send(Err(e));
                    continue;
                }
                let t_exec_wall = started.elapsed();
                let t_drain0 = std::time::Instant::now();
                let Some(ctx) = Self::unwrap_ctx(ctx, &out) else {
                    continue 'jobs;
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
        });
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
            pin_cores.clone()
        } else {
            Vec::new()
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
            let pin = pin_cores.clone();
            let hot = cfg_keep_hot;
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
                std::sync::Arc::new(crate::pool::WorkerPool::new(k, Vec::new()))
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

/// One state view per worker, checked once at construction so no later
/// caller can index past the given `Vec`.
impl<'a, S: StateDatabase + Sync> PoolHandle<'a, S> {
    /// Open a block session. `base` is the pre-block delta (the actor's
    /// live delta at the block's first tx), owned by the session.
    ///
    /// # Errors
    /// Returns an error if the pool is shut down, or if a previous block
    /// never released its slot before the stall timeout.
    ///
    /// # Panics
    /// Panics if the pool's internal lock is poisoned (a worker thread
    /// panicked while holding it).
    pub fn begin_block<'p>(
        &'p self,
        snapshot: S,
        base: PendingDelta,
        env: ExecEnv,
        stats: &'p Stats,
    ) -> Result<BlockSession<'p, 'a, S>, ExecutorError>
    where
        S: super::StmSnapshot,
    {
        let workers = {
            let st = self.shared.state.lock().expect("pool poisoned");
            st.cfg.workers.get()
        };
        // Cloning shares one transaction; backends that serialize reads
        // need `begin_block_per_worker` with independent views.
        self.begin_block_per_worker(vec![snapshot; workers], base, env, stats)
    }

    /// [`Self::begin_block`] with an independent state view per worker.
    /// See [`BlockCtx::snapshots`] for why the backend can require it.
    ///
    /// # Errors
    /// Returns an error if `snapshots` has fewer entries than the pool
    /// has workers, if the pool is shut down, or if a previous block
    /// never released its slot before the stall timeout.
    pub fn begin_block_per_worker<'p>(
        &'p self,
        snapshots: Vec<S>,
        base: PendingDelta,
        env: ExecEnv,
        stats: &'p Stats,
    ) -> Result<BlockSession<'p, 'a, S>, ExecutorError> {
        self.begin_block_layered(snapshots, base, Vec::new(), env, stats)
    }

    /// Like [`Self::begin_block_per_worker`], with unsettled predecessor
    /// deltas layered (newest first) without cloning or merging: the
    /// pipelined caller's zero-copy read stack.
    ///
    /// # Errors
    /// Returns an error under the same conditions as
    /// [`Self::begin_block_per_worker`].
    pub fn begin_block_layered<'p>(
        &'p self,
        snapshots: Vec<S>,
        base: PendingDelta,
        layers: Vec<std::sync::Arc<PendingDelta>>,
        env: ExecEnv,
        stats: &'p Stats,
    ) -> Result<BlockSession<'p, 'a, S>, ExecutorError> {
        let (sess, binder) = self.begin_block_deferred(snapshots, base, env, stats)?;
        binder.bind(ReadBase::Deltas(layers))?;
        Ok(sess)
    }

    /// [`Self::begin_block_layered`] with the layer bind deferred.
    /// Admission is layer-independent, so the pipelined consumer
    /// builds, feeds, and even submits this session while the
    /// predecessor still executes, then calls [`LayerBinder::bind`]
    /// when the predecessor's delta releases. Workers wait on the bind
    /// before touching state. A consumer that will never bind must call
    /// `abort_active` instead; the drain watchdog is the backstop.
    ///
    /// # Errors
    /// Returns an error under the same conditions as
    /// [`Self::begin_block_per_worker`].
    pub fn begin_block_deferred<'p>(
        &'p self,
        snapshots: Vec<S>,
        base: PendingDelta,
        env: ExecEnv,
        stats: &'p Stats,
    ) -> Result<(BlockSession<'p, 'a, S>, LayerBinder<S>), ExecutorError> {
        self.begin_block_deferred_inner(snapshots, base, env, stats, None)
    }

    /// [`Self::begin_block_layered`] with EIP-7928 capture on: every
    /// transaction records its per-transaction BAL fragment at
    /// block-global index `bal_base + local_idx + 1` (`bal_base` is the
    /// count of canonical records before this run, non-zero when the
    /// caller segments a block around deposits), and the sealed
    /// [`StmOutcome::bal`] carries the folded, sink-computed block BAL.
    /// The executor's `--parallel-execution` strategy is the intended
    /// caller; roles that never publish a BAL (validator, benches) use
    /// the capture-free variants and pay nothing.
    ///
    /// # Errors
    /// Returns an error under the same conditions as
    /// [`Self::begin_block_per_worker`].
    pub fn begin_block_layered_bal<'p>(
        &'p self,
        snapshots: Vec<S>,
        base: PendingDelta,
        layers: Vec<std::sync::Arc<PendingDelta>>,
        env: ExecEnv,
        stats: &'p Stats,
        bal_base: u64,
    ) -> Result<BlockSession<'p, 'a, S>, ExecutorError> {
        let (sess, binder) =
            self.begin_block_deferred_inner(snapshots, base, env, stats, Some(bal_base))?;
        binder.bind(ReadBase::Deltas(layers))?;
        Ok(sess)
    }

    /// Sweep parked mv caches whose last layer reference has dropped
    /// back onto the clean pool, so a later block can pop a scrubbed
    /// structure instead of mapping a fresh arena.
    fn sweep_parked_mv(&self) {
        let mut parked = self.recycle.mv_parked.lock().expect("pools poisoned");
        if parked.is_empty() {
            return;
        }
        let mut clean = self.recycle.mv_clean.lock().expect("pools poisoned");
        let drained: Vec<_> = std::mem::take(&mut *parked);
        for arc in drained {
            match Arc::try_unwrap(arc) {
                Ok(cache) => {
                    cache.scrub();
                    clean.push(cache);
                }
                Err(still) => parked.push(still),
            }
        }
    }

    /// Pop a recycled block arena and a recycled, scrubbed mv cache, if
    /// either pool has one ready.
    fn take_recycled_arena(&self) -> (Option<SpentArena>, Option<MvCache>) {
        let recycled = self.recycle.arenas.lock().expect("pools poisoned").pop();
        let recycled_mv = self.recycle.mv_clean.lock().expect("pools poisoned").pop();
        (recycled, recycled_mv)
    }

    /// Install `ctx` as the pool's current block, or stage it as `next`
    /// if a block is already executing. Waits, bounded by the drain
    /// watchdog, for the pipeline depth cap of 2 (one executing, one
    /// staged) to free a slot.
    fn install_ctx(&self, ctx: &Arc<BlockCtx<S>>) -> Result<(), ExecutorError> {
        let mut st = self.shared.state.lock().expect("pool poisoned");
        let deadline = std::time::Instant::now() + STALL_TIMEOUT;
        while st.next.is_some() {
            if std::time::Instant::now() > deadline {
                return Err(ExecutorError::State(
                    "stm pool: previous blocks never released the slots".into(),
                ));
            }
            let (back, _) = self
                .shared
                .wake
                .wait_timeout(st, PARK_POLL)
                .expect("pool poisoned");
            st = back;
        }
        if st.ctx.is_none() {
            st.ctx = Some(ctx.clone());
            st.generation += 1;
        } else {
            st.next = Some(ctx.clone());
        }
        Ok(())
    }

    pub(super) fn begin_block_deferred_inner<'p>(
        &'p self,
        snapshots: Vec<S>,
        base: PendingDelta,
        env: ExecEnv,
        stats: &'p Stats,
        bal_base: Option<u64>,
    ) -> Result<(BlockSession<'p, 'a, S>, LayerBinder<S>), ExecutorError> {
        let (workers_nz, prune_batch, bag_mode) = {
            let st = self.shared.state.lock().expect("pool poisoned");
            (
                st.cfg.workers,
                st.cfg.prune_batch,
                st.cfg.scheduler.is_bag(),
            )
        };
        let workers = workers_nz.get();
        let snapshots = PerWorker::new(snapshots, workers_nz)?;
        // Recycle (steady-state zero-allocation blocks): sweep parked
        // mv caches whose last layer reference has dropped, then pop
        // scrubbed structures instead of mapping fresh arenas.
        self.sweep_parked_mv();
        // O(1) reset of the feed's last-toucher index for this block.
        self.touch.borrow_mut().clear();
        if self.admit_shards.is_some() {
            self.shards_clear();
        }
        let (recycled, recycled_mv) = self.take_recycled_arena();
        let (r_slots, r_results, r_nodes) = match recycled {
            Some(a) => (Some(a.slots), Some(a.results), Some(a.nodes)),
            None => (None, None, None),
        };
        let ctx = Arc::new(BlockCtx {
            env,
            snapshots,
            base,
            bal_base,
            binding: std::sync::OnceLock::new(),
            mv: recycled_mv.map_or_else(|| Arc::new(MvCache::new()), Arc::new),
            base_cache: self.base_cache.clone(),
            recycle: self.recycle.clone(),
            slots: r_slots.unwrap_or_else(|| {
                (0..MAX_BLOCK_TXS)
                    .map(|_| std::sync::OnceLock::new())
                    .collect()
            }),
            results: r_results.unwrap_or_else(|| {
                (0..MAX_BLOCK_TXS)
                    .map(|_| std::sync::OnceLock::new())
                    .collect()
            }),
            bag: super::graph::Bag::new(),
            bag_mode,
            queues: (0..workers)
                .map(|_| WorkerQueue {
                    q: Mutex::new(std::collections::VecDeque::new()),
                    len: std::sync::atomic::AtomicUsize::new(0),
                    cv: Condvar::new(),
                    parked: AtomicBool::new(false),
                })
                .collect(),
            // A per-block arena: with pipelined admission, two blocks
            // are alive at once, so one pool-shared arena would alias.
            // It is recycled through the reaper's scrub, so steady
            // state allocates none.
            nodes: Arc::new(
                r_nodes.unwrap_or_else(|| (0..MAX_BLOCK_TXS).map(|_| Node::default()).collect()),
            ),
            admitted: AtomicU32::new(0),
            finished: AtomicU32::new(0),
            sealed: AtomicBool::new(false),
            completed: (0..workers).map(|_| Mutex::new(Vec::new())).collect(),
            completed_len: (0..workers).map(|_| PaddedLen::default()).collect(),
            pending: std::sync::atomic::AtomicU64::new(0),
            prune_batch: prune_batch.get(),
            started: std::time::Instant::now(),
            steal_enabled: self.steal_enabled(),
            spin_ns: self.spin_ns(),
            aborted: AtomicBool::new(false),
            double_exit: AtomicU32::new(0),
            metrics: Metrics {
                // fetch_min seeds from the top.
                first_dispatch_ns: std::sync::atomic::AtomicU64::new(u64::MAX),
                busy_per_worker: (0..workers).map(|_| PaddedLen64::default()).collect(),
                ..Default::default()
            },
        });
        // Pipeline depth cap of 2: one block executing (`ctx`), one
        // staged (`next`). Wait only when both are occupied, bounded by
        // the drain watchdog.
        self.install_ctx(&ctx)?;
        self.shared.wake.notify_all();
        let sess = BlockSession {
            pool: self,
            ctx,
            stats,
            workers,
            cold: 0,
            edges: 0,
            preds_buf: Vec::with_capacity(8),
            stage_timing: std::env::var_os("KARDAMOM_STM_FEED_STAGES").is_some(),
            admit_batch: Vec::with_capacity(ADMIT_BATCH),
            last_barrier: None,
            dispatch: vec![0; workers],
            n_txs: 0,
            started: std::time::Instant::now(),
            feed: FeedTimings::default(),
        };
        let binder = LayerBinder {
            ctx: Arc::downgrade(&sess.ctx),
        };
        Ok((sess, binder))
    }

    /// Feed a block whose transactions were prepared upstream (decode
    /// and predict already done, off this thread): the pipelined shape
    /// the `tx_data` reader threads will use.
    ///
    /// # Errors
    /// Returns an error if the block is declined and the sequential
    /// fallback fails, if opening the session fails (see
    /// [`Self::begin_block_per_worker`]), or if admitting or sealing a
    /// transaction fails.
    pub fn run_block_prepared(
        &self,
        snapshots: Vec<S>,
        base: PendingDelta,
        env: ExecEnv,
        txs: Vec<PreparedTx>,
        stats: &Stats,
    ) -> Result<StmOutcome, ExecutorError> {
        if !self.parallel_worth_it() {
            return self.decline_prepared(&snapshots[0], &base, env, &txs);
        }
        let mut sess = self.begin_block_per_worker(snapshots, base, env, stats)?;
        for record in txs {
            sess.push_prepared(
                record.tx_idx,
                record.position,
                record.envelope,
                record.prepared,
            )?;
        }
        sess.seal()
    }

    /// Return a settled release's delta shell for reuse. The fold's
    /// `PendingDelta` hashmap tables are one of the largest per-block
    /// allocations. The consumer calls this once a release's Arc
    /// unwraps, after `advance_base`. Entries drop here, but the tables
    /// keep their capacity.
    ///
    /// # Panics
    /// Panics if the recycle pool's internal lock is poisoned (a worker
    /// thread panicked while holding it).
    pub fn recycle_delta(&self, mut d: PendingDelta) {
        d.accounts.clear();
        d.storage.clear();
        d.code.clear();
        let mut g = self.recycle.deltas.lock().expect("pools poisoned");
        if g.len() < 4 {
            g.push(d);
        }
    }

    /// Clear every shard's last-toucher table (⊤ barrier, and per
    /// block). Safe because admission is quiesced at both call sites.
    pub(super) fn shards_clear(&self) {
        for k in 0..self.shards.len() {
            // SAFETY: no lane is running (the batch was flushed first).
            unsafe { self.shards.table(k) }.clear();
        }
    }

    /// The worker with the least sticky-assign load so far this block,
    /// or `hashed` if the load table is empty. The borrow ends with this
    /// function's return.
    pub(super) fn least_loaded(&self, hashed: usize) -> usize {
        let load = self.assign_load.borrow();
        (0..load.len()).min_by_key(|w| load[*w]).unwrap_or(hashed)
    }

    /// Whether an idle worker should steal a ready transaction from a
    /// busy one this block. Unknown on the first block of a pool: allow
    /// it, and let the measurement correct course from the next block
    /// on.
    fn steal_enabled(&self) -> bool {
        let avg = self.avg_tx_ns.load(Ordering::Relaxed);
        avg == 0 || avg >= STEAL_WORTH_NS
    }

    /// Nanoseconds a dry worker spins before it parks. Bridges roughly
    /// one link-release gap (about one transaction), bounded: spinning
    /// a full core for more than about 60us of silence is waste, and
    /// below about 5us the spin cannot outlast even a fast release.
    fn spin_ns(&self) -> u64 {
        let avg = self.avg_tx_ns.load(Ordering::Relaxed);
        if avg == 0 {
            20_000
        } else {
            avg.clamp(5_000, 60_000)
        }
    }

    /// Abort the executing block and any staged successor: workers
    /// stop at their next dispatch check, the drain completes on the
    /// abort flag, and the affected tickets resolve (to an error when
    /// transactions were left unexecuted). This is the speculative-
    /// release wound-abort path: whoever layered a block on a delta
    /// that a `corrected`
    /// release later invalidated calls this to hurry the stale block
    /// out, then rebuilds and resubmits from retained inputs. The stale
    /// ticket's outcome must be discarded either way, since a small
    /// block may finish on stale layers before the flag lands, making
    /// its bytes garbage.
    ///
    /// # Panics
    /// Panics if the pool's internal lock is poisoned (a worker thread
    /// panicked while holding it).
    pub fn abort_active(&self) {
        {
            let st = self.shared.state.lock().expect("pool poisoned");
            for c in st.ctx.iter().chain(st.next.iter()) {
                c.aborted.store(true, Ordering::SeqCst);
                c.wake_all();
            }
        }
        self.shared.wake.notify_all();
    }

    /// Mirror a committed delta into the pool-lifetime backend cache.
    /// Call this after the state writer applies the same delta. Skipping
    /// the call leaves stale entries and produces wrong reads; the
    /// harness's byte-identical assertion is the guard against that.
    ///
    /// # Panics
    /// Panics if the base cache's internal lock is poisoned (a worker
    /// thread panicked while holding it).
    pub fn advance_base(&self, delta: &PendingDelta) {
        BaseCache::insert_by_shard(
            &self.base_cache.accounts,
            delta
                .accounts
                .iter()
                .map(|(addr, (nonce, balance, code_hash))| {
                    (*addr, Some(account_info(*nonce, *balance, *code_hash)))
                }),
            BaseCache::shard,
        );
        BaseCache::insert_by_shard(
            &self.base_cache.storage,
            delta
                .storage
                .iter()
                .map(|((addr, key), value)| ((*addr, *key), *value)),
            |(addr, _)| BaseCache::shard(addr),
        );
        for (hash, code) in &delta.code {
            self.base_cache
                .code
                .write()
                .expect("base cache poisoned")
                .insert(
                    *hash,
                    revm::state::Bytecode::new_raw(alloy_primitives::Bytes::copy_from_slice(code)),
                );
        }
    }

    /// Would parallel execution pay for itself on this workload?
    ///
    /// Uses the mean per-transaction execution time learned from
    /// previous blocks: the same statistic the stealing policy runs on.
    /// A fresh pool has no measurement yet and is given the benefit of
    /// the doubt; one block is enough to correct course.
    pub fn parallel_worth_it(&self) -> bool {
        let avg = self.avg_tx_ns.load(Ordering::Relaxed);
        avg == 0 || avg >= self.parallel_worth_ns
    }

    /// Feed the decline gate after a block executed outside the pool (a
    /// caller-side sequential path, such as the executor strategy's own
    /// decline branch). Without this the gate is a trap door: `avg_tx_ns`
    /// would hold the value that caused the decline forever. Mirrors what
    /// [`decline`](Self::decline) does for pool-internal declines.
    pub fn learn_sequential(&self, elapsed: std::time::Duration, txs: usize) {
        if txs > 0 {
            self.avg_tx_ns
                .store(nanos(elapsed) / txs as u64, Ordering::Relaxed);
        }
    }

    /// Run the block on this thread, through the same code path the
    /// sequential executor uses, not a reimplementation of it.
    pub(super) fn decline(
        &self,
        snapshot: &S,
        base: &PendingDelta,
        env: ExecEnv,
        txs: &[(TxIndex, BPosition, TxEnvelope)],
    ) -> Result<StmOutcome, ExecutorError> {
        let started = std::time::Instant::now();
        let (receipts, delta) = execute_block_sequential(snapshot, Some(base), env, txs)?;
        Ok(self.decline_outcome(started, txs.len(), receipts, delta))
    }

    /// [`Self::decline`] for a block whose transactions are already
    /// decoded (see [`Self::run_block_prepared`]): runs the sequential
    /// fallback through [`execute_block_sequential_decoded`] instead,
    /// with `SeqTx` records built lazily from `txs`, since `Prepared`
    /// already holds the decode and re-running it would pay the RLP
    /// decode twice for a fallback path that already exists to be slow.
    fn decline_prepared(
        &self,
        snapshot: &S,
        base: &PendingDelta,
        env: ExecEnv,
        txs: &[PreparedTx],
    ) -> Result<StmOutcome, ExecutorError> {
        let started = std::time::Instant::now();
        let records = txs.iter().map(|r| super::sequential::SeqTx {
            tx_idx: r.tx_idx,
            position: r.position,
            envelope: &r.envelope,
            decoded: r.prepared.decoded.as_ref(),
        });
        let (receipts, delta) = super::sequential::execute_block_sequential_decoded_iter(
            snapshot,
            Some(base),
            env,
            records,
        )?;
        Ok(self.decline_outcome(started, txs.len(), receipts, delta))
    }

    /// Keep measuring while declining (see `learn_sequential`'s doc for
    /// why), then build the declined outcome. Sequential per-transaction
    /// cost slightly overstates the pool's own, since it hashes each
    /// write set inline, which the pool defers to its parallel commit
    /// phase. So the bias favors re-entering parallel execution rather
    /// than staying out.
    fn decline_outcome(
        &self,
        started: std::time::Instant,
        n: usize,
        receipts: Vec<kardamom_types::Receipt>,
        delta: PendingDelta,
    ) -> StmOutcome {
        if n > 0 {
            self.avg_tx_ns
                .store(nanos(started.elapsed()) / n as u64, Ordering::Relaxed);
        }
        StmOutcome {
            receipts,
            delta,
            declined: true,
            learned_tx_ns: self.avg_tx_ns.load(Ordering::Relaxed),
            writes_own: 0,
            writes_foreign: 0,
            fifo_covered: 0,
            fifo_stalls: 0,
            read_us: 0,
            busy_per_worker_us: Vec::new(),
            ..Default::default()
        }
    }

    /// Batch convenience: feed the whole block, seal, return the outcome.
    ///
    /// # Errors
    /// Returns an error if the block is declined and the sequential
    /// fallback fails, if opening the session fails (see
    /// [`Self::begin_block_per_worker`]), or if admitting or sealing a
    /// transaction fails.
    pub fn run_block(
        &self,
        snapshots: Vec<S>,
        base: PendingDelta,
        env: ExecEnv,
        txs: &[(TxIndex, BPosition, TxEnvelope)],
        stats: &Stats,
    ) -> Result<StmOutcome, ExecutorError> {
        if !self.parallel_worth_it() {
            return self.decline(&snapshots[0], &base, env, txs);
        }
        let mut sess = self.begin_block_per_worker(snapshots, base, env, stats)?;
        for (t, p, e) in txs {
            sess.push_tx(*t, *p, e.clone())?;
        }
        sess.seal()
    }
}
