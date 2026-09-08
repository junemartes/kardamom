//! [`PoolHandle`]'s own operations: opening a block session (the
//! `begin_block*` family and their admission-slot machinery), and
//! running or declining a block once a session is open.

use super::config::{
    ADMIT_BATCH, MAX_BLOCK_TXS, PARK_POLL, STALL_TIMEOUT, STEAL_WORTH_NS, account_info, nanos,
};
use super::graph::{BlockCtx, Node, PerWorker, WorkerQueue};
use super::handle::{PoolHandle, PoolState, PreparedTx};
use super::metrics::{FeedTimings, Metrics, PaddedLen, PaddedLen64, StmOutcome};
use super::recycle::SpentArena;
use super::sequential::execute_block_sequential;
use super::session::{BlockSession, LayerBinder, ReadBase};
use super::view::BaseCache;
use crate::mv::MvCache;
use kardamom_exec_core::block_env::ExecEnv;
use kardamom_exec_core::delta::PendingDelta;
use kardamom_exec_core::error::ExecutorError;
use kardamom_exec_core::exec_types::TxIndex;
use kardamom_footprint::classifier::Stats;
use kardamom_types::BPosition;
use kardamom_types::StateDatabase;
use kardamom_types::TxEnvelope;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;

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
            Self::sweep_one_parked(arc, &mut parked, &mut clean);
        }
    }

    /// Reclaim `arc` onto `clean` when no other layer reference remains,
    /// or park it back for the next sweep. The `for` loop in
    /// [`Self::sweep_parked_mv`] stays free of a branch.
    fn sweep_one_parked(
        arc: Arc<MvCache>,
        parked: &mut Vec<Arc<MvCache>>,
        clean: &mut Vec<MvCache>,
    ) {
        match Arc::try_unwrap(arc) {
            Ok(cache) => {
                cache.scrub();
                clean.push(cache);
            }
            Err(still) => parked.push(still),
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
            st = self.wait_for_slot(st, deadline)?;
        }
        if st.ctx.is_none() {
            st.ctx = Some(ctx.clone());
            st.generation += 1;
        } else {
            st.next = Some(ctx.clone());
        }
        Ok(())
    }

    /// One [`Self::install_ctx`] wait step: past the drain watchdog
    /// deadline is an error; otherwise park on the condvar until the
    /// next poll and return the refreshed guard.
    fn wait_for_slot<'g>(
        &'g self,
        st: MutexGuard<'g, PoolState<S>>,
        deadline: std::time::Instant,
    ) -> Result<MutexGuard<'g, PoolState<S>>, ExecutorError> {
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
        Ok(back)
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
                .map(|(addr, fields)| (*addr, Some(account_info(*fields)))),
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
