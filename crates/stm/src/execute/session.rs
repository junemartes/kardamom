use super::config::{ADMIT_BATCH, MAX_BLOCK_TXS, STICKY_CAP, nanos};
use super::graph::{BlockCtx, TxSlot};
use super::handle::PoolHandle;
use super::metrics::{FeedTimings, StmOutcome};
use super::prepare::{Prepared, domain_hash};
use super::view::BlockInput;
use crate::FEE_SINK;
use crate::mv::MvCache;
use alloy_primitives::U256;
use kardamom_exec_core::delta::PendingDelta;
use kardamom_exec_core::error::ExecutorError;
use kardamom_exec_core::exec_types::TxIndex;
use kardamom_exec_core::executor::DecodedTx;
use kardamom_footprint::classifier::DomainKey;
use kardamom_footprint::classifier::Stats;
use kardamom_types::BPosition;
use kardamom_types::StateDatabase;
use kardamom_types::TxEnvelope;
use revm::database::DatabaseRef;
use revm::state::AccountInfo;
use std::sync::Arc;
use std::sync::atomic::Ordering;

/// The late-bound part of a block's read base (see `BlockCtx::binding`).
pub(super) struct BoundLayers {
    /// Predecessor mv caches, newest first.
    pub(super) mv_layers: Vec<std::sync::Arc<MvCache>>,
    pub(super) layers: Vec<std::sync::Arc<PendingDelta>>,
    pub(super) sink_start: Option<AccountInfo>,
    pub(super) sink_start_balance: U256,
}

/// One sealed block handed to the persistent tail thread: drain,
/// release the pool slot, then `block_tail`.
pub(super) struct TailJob<S: StateDatabase> {
    pub(super) ctx: Arc<BlockCtx<S>>,
    pub(super) n_txs: usize,
    pub(super) started: std::time::Instant,
    pub(super) cold: usize,
    pub(super) edges: usize,
    pub(super) dispatch: Vec<u32>,
    pub(super) out: std::sync::mpsc::Sender<Result<StmOutcome, ExecutorError>>,
    pub(super) delta_out: Option<DeltaOut>,
    /// The session's folded feed-side timing, carried across the thread
    /// hand-off instead of a `Metrics` atomic (see `BlockSession`).
    pub(super) feed: FeedTimings,
}

/// Streaming delta hand-off: block N's folded delta, released to
/// whoever layers block N+1, before N's receipts, and in speculative
/// mode, before N's validation verdict.
pub struct DeltaRelease {
    pub block: u64,
    pub delta: std::sync::Arc<PendingDelta>,
    /// True when this re-issues a block whose earlier speculative
    /// release was invalidated by a wound. Everything layered on the
    /// stale release must be aborted and rebuilt on this delta.
    pub corrected: bool,
}

/// Binds a deferred session's read base (see
/// [`PoolHandle::begin_block_deferred`]). Consumed by `bind`. A binder
/// dropped without binding leaves the block gated; call `abort_active`
/// on it. Holds only a weak reference, since the tail's ctx unwrap must
/// not wait on a consumer that decided to abort instead of bind.
pub struct LayerBinder<S: StateDatabase> {
    pub(super) ctx: std::sync::Weak<BlockCtx<S>>,
}

/// A deferred session's read base. The fee sink is never published to
/// an mv cache, so `Mv` carries its block-start value directly; `Deltas`
/// probes it through the delta layers instead. The two ways of
/// supplying the sink are exactly the two variants, so a caller cannot
/// construct the invalid pair (mv layers present, no sink value).
pub enum ReadBase {
    /// Unsettled predecessor deltas, newest first.
    Deltas(Vec<std::sync::Arc<PendingDelta>>),
    /// Predecessor mv layers, newest first, probed before `deltas`.
    Mv {
        layers: Vec<std::sync::Arc<MvCache>>,
        /// The fee sink's block-start value (from the predecessor's
        /// [`MvRelease::sink_final`]); `None` means the account does
        /// not exist yet.
        sink_final: Option<AccountInfo>,
        deltas: Vec<std::sync::Arc<PendingDelta>>,
    },
}

impl<S: StateDatabase> LayerBinder<S> {
    /// Install the unsettled predecessor read base and wake the gated
    /// workers.
    ///
    /// # Errors
    /// Returns an error if the session was aborted before this binds,
    /// if reading the fee sink's block-start value fails (the `Deltas`
    /// case probes it), or if the layers are already bound.
    pub fn bind(self, base: ReadBase) -> Result<(), ExecutorError> {
        let Some(ctx) = self.ctx.upgrade() else {
            return Err(ExecutorError::State(
                "stm: deferred block gone before bind (aborted)".into(),
            ));
        };
        let (mv_layers, layers, sink_start) = match base {
            ReadBase::Deltas(layers) => {
                let probe = BlockInput {
                    snapshot: ctx.snapshots.primary(),
                    base: Some(&ctx.base),
                    layers: &layers,
                    mv_layers: &[],
                };
                let sink_start = probe
                    .basic_ref(FEE_SINK)
                    .map_err(|e| ExecutorError::State(format!("fee-sink read: {e}")))?;
                (Vec::new(), layers, sink_start)
            }
            ReadBase::Mv {
                layers: mv_layers,
                sink_final,
                deltas,
            } => (mv_layers, deltas, sink_final),
        };
        let sink_start_balance = sink_start.as_ref().map_or(U256::ZERO, |a| a.balance);
        if ctx
            .binding
            .set(BoundLayers {
                mv_layers,
                layers,
                sink_start,
                sink_start_balance,
            })
            .is_err()
        {
            return Err(ExecutorError::State("layers already bound".into()));
        }
        ctx.wake_all();
        Ok(())
    }
}

/// The early streaming release: block N's multi-version cache,
/// shipped right after drain and extract, before phase-1, fold, or
/// validation. Its top version per cell equals what
/// the fold will compute; the sink (never published to mv) rides along,
/// already computed. Pre-verdict by construction: a wound invalidates
/// it through the corrected `DeltaRelease` that follows.
pub struct MvRelease {
    pub block: u64,
    pub mv: std::sync::Arc<MvCache>,
    /// The fee sink's final account for this block (start plus fee sum).
    pub sink_final: Option<AccountInfo>,
}

pub(super) struct DeltaOut {
    pub(super) tx: std::sync::mpsc::Sender<DeltaRelease>,
    /// mv-as-layer early release channel (implies speculative).
    pub(super) mv_tx: Option<std::sync::mpsc::Sender<MvRelease>>,
    /// Speculative: release at fold, concurrent with validation. A
    /// wound invalidates the release, and a `corrected` re-issue
    /// follows. Conservative: release only after the verdict, when
    /// the delta can no longer change.
    pub(super) speculative: bool,
}

impl DeltaOut {
    /// Send one delta release on this channel, best-effort: a dropped
    /// receiver (the consumer already moved on, or never subscribed)
    /// is not the tail's problem.
    pub(super) fn release(&self, block: u64, delta: std::sync::Arc<PendingDelta>, corrected: bool) {
        let _ = self.tx.send(DeltaRelease {
            block,
            delta,
            corrected,
        });
    }
}

/// A submitted block's pending outcome. Outcomes complete in submission
/// order; `wait` blocks until this block is validated, repaired if
/// wounded, and committed.
pub struct BlockTicket {
    pub(super) rx: std::sync::mpsc::Receiver<Result<StmOutcome, ExecutorError>>,
}

impl BlockTicket {
    /// Block until the tail thread resolves this ticket's outcome.
    ///
    /// # Errors
    /// Returns an error if the tail thread is gone, or any error the
    /// block itself produced (see [`BlockSession::submit`]).
    pub fn wait(self) -> Result<StmOutcome, ExecutorError> {
        self.rx
            .recv()
            .unwrap_or_else(|_| Err(ExecutorError::State("stm pool: tail thread gone".into())))
    }
}

/// One in-flight block being fed to the pool.
pub struct BlockSession<'p, 'a, S: StateDatabase + Sync> {
    pub(super) pool: &'p PoolHandle<'a, S>,
    pub(super) ctx: Arc<BlockCtx<S>>,
    pub(super) stats: &'p Stats,
    pub(super) workers: usize,
    pub(super) cold: usize,
    pub(super) edges: usize,
    /// Reusable predecessor scratch: a fresh `Vec` per transaction was a
    /// heap allocation on the serial feed.
    pub(super) preds_buf: Vec<u32>,
    /// `KARDAMOM_STM_FEED_STAGES`: per-stage feed timers, off by default,
    /// since they cost what they measure.
    pub(super) stage_timing: bool,
    /// Sharded admission: indices awaiting dependency discovery.
    pub(super) admit_batch: Vec<u32>,
    /// The most recent ⊤ (cold) transaction: conflicts with everything,
    /// so every later admission takes an edge from it while it is
    /// outstanding.
    pub(super) last_barrier: Option<u32>,
    pub(super) dispatch: Vec<u32>,
    /// Admitted count. The envelopes live in the ctx slots; the repair
    /// path reads them there, with no parallel copy.
    pub(super) n_txs: usize,
    pub(super) started: std::time::Instant,
    /// Feed-side per-phase nanoseconds, folded once at `seal` instead of
    /// through a cross-thread atomic: this session is owned by one
    /// thread for its whole life, and the tail thread only ever sees the
    /// totals, carried on `TailJob`.
    pub(super) feed: FeedTimings,
}

/// A new transaction's slot contents: what `BlockSession::store_slot`
/// owns and moves into `BlockCtx::slots`.
struct NewSlot {
    tx_idx: TxIndex,
    position: BPosition,
    envelope: TxEnvelope,
    decoded: Option<DecodedTx>,
}

/// The transaction one admission call is registering: its local index,
/// its slot's `u32` twin, the worker it was assigned to, and whether it
/// is ⊤ (cold, conflicts with everything).
#[derive(Clone, Copy)]
struct AdmitTarget {
    i: usize,
    idx: u32,
    worker: usize,
    is_cold: bool,
}

/// One admission lane's shard-registration state: its own table, its
/// shard index among `k`, and the atomic edge counter it feeds. Built
/// once per lane in [`BlockSession::flush_admit_batch`]'s `body`
/// closure, then [`Self::register`]ed against once per batch index.
struct ShardLane<'a, S: StateDatabase> {
    ctx: &'a BlockCtx<S>,
    table: &'a mut super::touch::TouchTable,
    k: usize,
    sh: usize,
    edges: &'a std::sync::atomic::AtomicUsize,
}

impl<S: StateDatabase> ShardLane<'_, S> {
    /// One admitted transaction's registrations against this lane's
    /// shard: install `idx` as each of its shard-`sh` cells' last
    /// toucher, and take an edge (via
    /// [`BlockCtx::try_edge`](super::graph::BlockCtx::try_edge)) from
    /// whichever transaction it displaces, if that one is still open.
    #[allow(
        clippy::cast_possible_truncation,
        reason = "h % k is < k, which came from usize (admit_shards), so it always fits back in usize"
    )]
    fn register(&mut self, idx: u32) {
        let slot = self.ctx.slots[idx as usize]
            .get()
            .expect("slot set before admission batch");
        for h in &slot.hashes {
            if (*h % self.k as u64) as usize != self.sh {
                continue;
            }
            if let Some(p) = self.table.upsert(*h, idx)
                && p != idx
                && self.ctx.try_edge(p, idx)
            {
                self.edges.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

impl<S: StateDatabase + Sync> BlockSession<'_, '_, S> {
    /// Run dependency discovery for the queued batch: lane `k` owns
    /// cell-space shard `k`, walks the batch in index order, upserts its
    /// own cells, and registers the edges it finds. Returns with every
    /// edge in place, so the guards can be dropped and the ready
    /// transactions dispatched.
    pub(super) fn flush_admit_batch(&mut self) {
        if self.admit_batch.is_empty() {
            return;
        }
        let k = self
            .pool
            .admit_shards
            .map_or(1, std::num::NonZeroUsize::get);
        let batch: &[u32] = &self.admit_batch;
        let ctx = &self.ctx;
        let shards = &self.pool.shards;
        let edges = std::sync::atomic::AtomicUsize::new(0);
        let body = |sh: usize| {
            // SAFETY: chunk `sh` is executed by exactly one lane, and
            // no other chunk touches table `sh`.
            let table = unsafe { shards.table(sh) };
            let mut lane = ShardLane {
                ctx,
                table,
                k,
                sh,
                edges: &edges,
            };
            for &idx in batch {
                lane.register(idx);
            }
        };
        match &self.pool.admit_lanes {
            Some(l) => l
                .run(k.min(l.workers().get()), &|_lane, sh| body(sh))
                .expect("admission lane panicked"),
            None => {
                for sh in 0..k {
                    body(sh);
                }
            }
        }
        self.edges += edges.load(Ordering::Relaxed);
        // Every edge for this batch is registered: drop the router
        // guards, dispatching whatever is ready.
        for &idx in &self.admit_batch {
            if self.ctx.nodes[idx as usize]
                .indegree
                .fetch_sub(1, Ordering::AcqRel)
                == 1
            {
                let w = self.ctx.nodes[idx as usize].worker.load(Ordering::Acquire);
                self.ctx.push_ready(w, idx);
            }
        }
        self.admit_batch.clear();
    }

    /// Admit the next canonical transaction: one function computation,
    /// then an assignment to a thread.
    ///
    /// 1. Predict the footprint (the footprint classifier), pure and
    ///    off-lock.
    /// 2. Update the live DAG: each predicted cell's last toucher becomes
    ///    a predecessor if it has not finished yet. A ⊤ (cold)
    ///    transaction takes edges from everything outstanding and
    ///    becomes the barrier every later transaction depends on.
    /// 3. Assign a thread by hashing the primary contention domain, and
    ///    dispatch right away when the indegree is already zero.
    ///
    /// Domain-hashed assignment is what keeps the DAG's chains cheap:
    /// same-domain transactions land on the same thread in canonical
    /// order, so a chain drains as a FIFO with no cross-thread handoff
    /// at all. The graph only has to carry the cross-domain and
    /// multi-domain edges.
    ///
    /// Conflicts the prediction missed are not the graph's business.
    /// They are caught at validation and repaired by wounding the later
    /// transaction (see [`BlockSession::seal`]): the wound leg of
    /// wound-wait, with the DAG edge as the wait leg.
    ///
    /// # Errors
    /// Returns an error if the block already holds `MAX_BLOCK_TXS`
    /// transactions.
    pub fn push_tx(
        &mut self,
        tx_idx: TxIndex,
        position: BPosition,
        envelope: TxEnvelope,
    ) -> Result<(), ExecutorError> {
        // Convenience path: prepare inline. The pipelined caller (the
        // tx_data readers) calls `prepare` upstream and `push_prepared`
        // here, keeping decode and predict off this serial thread
        // entirely. Timed separately (not through `prepare`) so
        // `decode_us` and `predict_us` report their own phase instead of
        // the combined total.
        let t_decode = std::time::Instant::now();
        let decoded = Prepared::decode(&envelope, tx_idx);
        self.feed.decode_ns = self
            .feed
            .decode_ns
            .saturating_add(nanos(t_decode.elapsed()));
        let t_predict = std::time::Instant::now();
        let (domains, domain_hashes, primary, cold) =
            Prepared::predict(&envelope, decoded.as_ref(), self.stats);
        self.feed.predict_ns = self
            .feed
            .predict_ns
            .saturating_add(nanos(t_predict.elapsed()));
        let prep = Prepared {
            decoded,
            domains,
            domain_hashes,
            primary,
            cold,
        };
        self.push_prepared(tx_idx, position, envelope, prep)
    }

    /// Pick the worker for a predicted domain: hash-stable, with an
    /// optional sticky override that pins a domain to the least-loaded
    /// worker the first time it is seen (see `STICKY_CAP`). Bag mode has
    /// no owner, so no assignment at all: the hash and sticky-assign
    /// logic was the largest single feed stage, measured per
    /// transaction, computing a value the bag never reads.
    fn assign_worker(&self, domain: Option<DomainKey>, i: usize) -> usize {
        if self.ctx.bag_mode {
            return 0;
        }
        let hashed = match domain {
            Some(DomainKey::Account(a)) => domain_hash(a.as_slice(), self.workers),
            Some(DomainKey::Fixed(a, k)) => {
                let mut b = [0u8; 8];
                b[..4].copy_from_slice(&a.as_slice()[16..20]);
                b[4..].copy_from_slice(&k.as_slice()[28..32]);
                domain_hash(&b, self.workers)
            }
            // ⊤ and empty predictions: canonical round-robin.
            None => i % self.workers,
        };
        let worker = match (self.pool.sticky_assign, domain) {
            (true, Some(key)) => {
                let mut map = self.pool.assign.borrow_mut();
                if let Some(w) = map.get(&key) {
                    *w
                } else if map.len() >= STICKY_CAP {
                    hashed
                } else {
                    let w = self.pool.least_loaded(hashed);
                    map.insert(key, w);
                    w
                }
            }
            _ => hashed,
        };
        if self.pool.sticky_assign {
            self.pool.assign_load.borrow_mut()[worker] += 1;
        }
        worker
    }

    /// Store the transaction's slot (the envelope owns it: no clone, no
    /// parallel vec, since the repair path reads slots too) and bump the
    /// feed-owned counters.
    fn store_slot(
        &mut self,
        i: usize,
        worker: usize,
        slot: NewSlot,
        hashes: &[u64],
        sharded: bool,
    ) {
        let NewSlot {
            tx_idx,
            position,
            envelope,
            decoded,
        } = slot;
        self.ctx.slots[i]
            .set(TxSlot {
                tx_idx,
                position,
                envelope,
                decoded,
                hashes: if sharded {
                    hashes.iter().copied().collect()
                } else {
                    smallvec::SmallVec::new()
                },
            })
            .unwrap_or_else(|_| unreachable!("slot set once per index"));
        self.n_txs += 1;
        self.dispatch[worker] += 1;
    }

    /// Take an edge from `p` to `idx` (the transaction being admitted),
    /// if `p` is still open, and tally it on `self.edges`. Delegates
    /// the shared check-and-register step to
    /// [`BlockCtx::try_edge`](super::graph::BlockCtx::try_edge), which
    /// `ShardLane::register` also calls, tallying into an atomic
    /// instead.
    fn try_edge(&mut self, p: u32, idx: u32) {
        if self.ctx.try_edge(p, idx) {
            self.edges += 1;
        }
    }

    /// Sharded admission: node init and queueing only. The router does
    /// node init and queues the index; dependency discovery
    /// (last-toucher upserts and edge registration) happens later, in
    /// the batch flush, one lane per cell-space shard. No transaction
    /// can dispatch before its batch completes, because the router's
    /// guard is dropped only there. That is why per-shard guards are
    /// unnecessary.
    fn admit_sharded(
        &mut self,
        target: AdmitTarget,
        t_feed: std::time::Instant,
    ) -> Result<(), ExecutorError> {
        let AdmitTarget {
            i,
            idx,
            worker,
            is_cold,
        } = target;
        {
            let node = &self.ctx.nodes[i];
            node.worker.store(worker, Ordering::Release);
            node.indegree.store(1, Ordering::Release);
            let mut c = node.children.lock().expect("children poisoned");
            if node.open.load(Ordering::Acquire) {
                return Err(ExecutorError::State(format!(
                    "stm: tx index {i} admitted twice"
                )));
            }
            c.clear();
            node.open.store(true, Ordering::Release);
        }
        self.ctx.admitted.fetch_add(1, Ordering::SeqCst);
        // The barrier edge is global, so the router registers it.
        if let Some(b) = self.last_barrier {
            self.try_edge(b, idx);
        }
        if is_cold {
            // ⊤ conflicts with everything: settle the batch, then do
            // the barrier serially (edges from everything outstanding,
            // and clear all shard tables).
            self.flush_admit_batch();
            for p in 0..idx {
                self.try_edge(p, idx);
            }
            self.last_barrier = Some(idx);
            self.pool.shards_clear();
        }
        self.admit_batch.push(idx);
        if self.admit_batch.len() >= ADMIT_BATCH {
            self.flush_admit_batch();
        }
        self.feed.feed_ns = self.feed.feed_ns.saturating_add(nanos(t_feed.elapsed()));
        Ok(())
    }

    /// Serial (unsharded) admission: dependency discovery against the
    /// feed-owned last-toucher index, node registration, edge
    /// publication against every still-open predecessor, and dispatch
    /// if this transaction's guard was the last one outstanding. No
    /// global lock: opening this node's registration point, seeding the
    /// admission guard, then registering on each predecessor that is
    /// still open. The guard (+1) means a predecessor finishing
    /// mid-admission can never drive the count to zero and dispatch a
    /// half-linked transaction. Dropping it at the end is what actually
    /// releases this transaction.
    fn admit_serial(
        &mut self,
        target: AdmitTarget,
        hashes: &[u64],
        t_feed: std::time::Instant,
        t_pre_end: Option<std::time::Instant>,
    ) -> Result<(), ExecutorError> {
        let AdmitTarget {
            i,
            idx,
            worker,
            is_cold,
        } = target;
        let mut preds = std::mem::take(&mut self.preds_buf);
        preds.clear();
        if let Some(b) = self.last_barrier {
            preds.push(b);
        }
        {
            let mut touch = self.pool.touch.borrow_mut();
            if is_cold {
                // ⊤: conflicts with everything. Every outstanding
                // transaction is a candidate predecessor, and this
                // transaction becomes the barrier.
                preds.clear();
                preds.extend(0..idx);
                self.last_barrier = Some(idx);
                touch.clear();
            } else {
                // Hashes came from `prepare`, off this thread.
                for h in hashes {
                    if let Some(p) = touch.upsert(*h, idx) {
                        preds.push(p);
                    }
                }
                preds.sort_unstable();
                preds.dedup();
            }
        }

        let t_admit = std::time::Instant::now();
        if let Some(t_pre_end) = t_pre_end {
            self.feed.feed_dag_ns = self
                .feed
                .feed_dag_ns
                .saturating_add(nanos(t_admit - t_pre_end));
        }
        {
            // Register once. Unreachable through the public API, since
            // the local index comes from this session's own counter, so
            // no caller can name an occupied slot. Asserted anyway,
            // because a refactor that reused indices would otherwise
            // resurrect a closed registration point and hang whichever
            // child registered on it, with no diagnostic.
            let node = &self.ctx.nodes[i];
            node.worker.store(worker, Ordering::Release);
            node.indegree.store(1, Ordering::Release);
            let mut c = node.children.lock().expect("children poisoned");
            if node.open.load(Ordering::Acquire) {
                return Err(ExecutorError::State(format!(
                    "stm: tx index {i} admitted twice (registration must happen exactly once)"
                )));
            }
            c.clear();
            if !self.ctx.bag_mode {
                // FIFO-scheduler state: the bag has no queue position to
                // record and no take-time verification to feed, so this
                // second mutex, per transaction, on the serial feed, is
                // pure ceremony there.
                node.queued.store(false, Ordering::Release);
                node.fifo_preds.lock().expect("fifo_preds poisoned").clear();
            }
            node.open.store(true, Ordering::Release);
        }
        self.ctx.admitted.fetch_add(1, Ordering::SeqCst);
        let mut deg = 0u32;
        let mut covered = 0u64;
        let eager = self.pool.eager_chain;
        for &p in &preds {
            if p == idx {
                continue;
            }
            let pn = &self.ctx.nodes[p as usize];
            let mut list = pn.children.lock().expect("children poisoned");
            if pn.open.load(Ordering::Acquire) {
                // p is unfinished. If it was already released to this
                // transaction's own queue (indegree 0 is definitive:
                // admission is serial, so p's guard was dropped long ago
                // and a released node is never re-blocked), FIFO
                // position orders it. Record it for take-time
                // verification instead of an edge, and the whole prune
                // hand-off for this link disappears. A stale read of a
                // nonzero indegree only costs an edge, never
                // correctness.
                if eager
                    && !self.ctx.bag_mode
                    && pn.worker.load(Ordering::Acquire) == worker
                    && pn.queued.load(Ordering::Acquire)
                {
                    self.ctx.nodes[i]
                        .fifo_preds
                        .lock()
                        .expect("fifo_preds poisoned")
                        .push(p);
                    covered += 1;
                } else {
                    // Increment before publishing the edge: the matching
                    // decrement can only happen once this child is
                    // visible in p's list, so the add always precedes
                    // its own subtract.
                    self.ctx.nodes[i].indegree.fetch_add(1, Ordering::AcqRel);
                    list.push(idx);
                    deg += 1;
                    if pn.worker.load(Ordering::Acquire) == worker
                        && pn.queued.load(Ordering::Acquire)
                    {
                        self.feed.redundant_edges += 1;
                    }
                }
            }
            // else: p already finished and published, no edge needed.
        }
        self.edges += deg as usize;
        if covered > 0 {
            self.feed.fifo_covered += covered;
        }
        self.preds_buf = preds; // scratch back for the next tx

        // Drop the admission guard; if every registered predecessor has
        // already retired, this tx is ours to dispatch.
        let dispatch_now = self.ctx.nodes[i].indegree.fetch_sub(1, Ordering::AcqRel) == 1;
        self.feed.admit_ns = self.feed.admit_ns.saturating_add(nanos(t_admit.elapsed()));
        if dispatch_now {
            self.ctx.push_ready(worker, idx);
        }
        self.feed.feed_ns = self.feed.feed_ns.saturating_add(nanos(t_feed.elapsed()));
        Ok(())
    }

    /// Admit a transaction whose decode and prediction were computed
    /// upstream (see [`prepare`]). This is the executor's real hot path:
    /// everything left here is graph work, which must stay serial and
    /// in canonical order, because an edge means "the previous
    /// transaction that touched this domain".
    ///
    /// # Errors
    /// Returns an error if the block already holds `MAX_BLOCK_TXS`
    /// transactions.
    ///
    /// # Panics
    /// Panics only if the block's transaction count somehow exceeds
    /// `u32::MAX`, which the `MAX_BLOCK_TXS` check above already rules
    /// out.
    pub fn push_prepared(
        &mut self,
        tx_idx: TxIndex,
        position: BPosition,
        envelope: TxEnvelope,
        prep: Prepared,
    ) -> Result<(), ExecutorError> {
        let t_feed = std::time::Instant::now();
        let i = self.n_txs;
        if i >= MAX_BLOCK_TXS {
            return Err(ExecutorError::State(format!(
                "stm pool: block exceeds MAX_BLOCK_TXS={MAX_BLOCK_TXS} (gas-limit math says impossible)"
            )));
        }
        let idx = u32::try_from(i).expect("just checked i < MAX_BLOCK_TXS above");
        let Prepared {
            decoded,
            domains: cells,
            domain_hashes: hashes,
            primary: domain,
            cold: is_cold,
        } = prep;
        if is_cold {
            self.cold += 1;
        }

        // Hash the domain to a worker. Hashing is stable across blocks,
        // which keeps state warm.
        // Policy: which side of a two-account transaction do we own?
        let domain = if self.pool.dispatch_by_sender {
            let sender_cell = DomainKey::Account(envelope.sender);
            if cells.contains(&sender_cell) {
                Some(sender_cell)
            } else {
                domain
            }
        } else {
            domain
        };
        let worker = self.assign_worker(domain, i);

        let sharded = self.pool.admit_shards.is_some();
        let slot = NewSlot {
            tx_idx,
            position,
            envelope,
            decoded,
        };
        self.store_slot(i, worker, slot, &hashes, sharded);
        // Stage timers are opt-in: two extra clock reads per transaction
        // measured about 5% of the serial feed, and the feed is the
        // thing they measure.
        let t_pre_end = if self.stage_timing {
            let t = std::time::Instant::now();
            self.feed.feed_pre_ns = self.feed.feed_pre_ns.saturating_add(nanos(t - t_feed));
            Some(t)
        } else {
            None
        };

        // (2) Update the live DAG, and (3) dispatch if ready.
        // Candidate predecessors come from the feed-owned last-toucher
        // index. No lock is needed, because admission is single-threaded
        // and prune never reads it.
        let target = AdmitTarget {
            i,
            idx,
            worker,
            is_cold,
        };
        if sharded {
            return self.admit_sharded(target, t_feed);
        }
        self.admit_serial(target, &hashes, t_feed, t_pre_end)
    }

    /// The boundary: no more transactions. Wait out the in-flight tail,
    /// validate every recorded read, and wound (re-execute at its
    /// canonical position, sequentially, against the computed prefix)
    /// any transaction a missed conflict convicted, per transaction, not
    /// whole-block. Then commit in canonical order.
    ///
    /// # Errors
    /// Returns an error under the same conditions as [`Self::submit`],
    /// plus any error the tail thread's validation, wound repair, or
    /// commit produced.
    pub fn seal(self) -> Result<StmOutcome, ExecutorError> {
        self.submit()?.wait()
    }

    /// Hand this block to the persistent tail thread and return right
    /// away. The pool slot frees once execution drains, so
    /// the caller may begin feeding the next block while this one
    /// validates and commits on the tail thread.
    ///
    /// # Errors
    /// Returns an error if the persistent tail thread is gone.
    pub fn submit(self) -> Result<BlockTicket, ExecutorError> {
        self.submit_with(None)
    }

    /// Like [`Self::submit_streaming`], plus the early mv release:
    /// `mv_tx` receives this block's multi-version
    /// cache right after drain and extract, the earliest point a
    /// successor can bind on. `delta_tx` still receives the folded
    /// delta (for base-cache advancement and writer settlement), plus
    /// the `corrected` re-issue on a wound.
    ///
    /// # Errors
    /// Returns an error under the same conditions as [`Self::submit`].
    pub fn submit_streaming_mv(
        self,
        mv_tx: std::sync::mpsc::Sender<MvRelease>,
        delta_tx: std::sync::mpsc::Sender<DeltaRelease>,
    ) -> Result<BlockTicket, ExecutorError> {
        self.submit_with(Some(DeltaOut {
            tx: delta_tx,
            mv_tx: Some(mv_tx),
            speculative: true,
        }))
    }

    /// `submit`, plus a streaming delta release: the tail sends this
    /// block's folded delta on `delta_tx` as soon as it exists. That
    /// is at the fold, before validation, when `speculative`; after
    /// the verdict when not. On a wound, the tail sends a second,
    /// `corrected` release; the consumer must abort anything layered
    /// on the first.
    ///
    /// # Errors
    /// Returns an error under the same conditions as [`Self::submit`].
    pub fn submit_streaming(
        self,
        delta_tx: std::sync::mpsc::Sender<DeltaRelease>,
        speculative: bool,
    ) -> Result<BlockTicket, ExecutorError> {
        self.submit_with(Some(DeltaOut {
            tx: delta_tx,
            mv_tx: None,
            speculative,
        }))
    }

    /// Shared body for [`Self::submit`], [`Self::submit_streaming`], and
    /// [`Self::submit_streaming_mv`]: settle the admit batch (a partial
    /// batch would strand its router guards, and the block would never
    /// drain), seal the block, and hand it to the persistent tail
    /// thread. The three public methods differ only in `delta_out`.
    ///
    /// # Errors
    /// Returns an error if the persistent tail thread is gone.
    fn submit_with(self, delta_out: Option<DeltaOut>) -> Result<BlockTicket, ExecutorError> {
        let mut this = self;
        this.flush_admit_batch();
        let BlockSession {
            pool,
            ctx,
            n_txs,
            started,
            cold,
            edges,
            dispatch,
            feed,
            ..
        } = this;
        ctx.sealed.store(true, Ordering::SeqCst);
        ctx.wake_all();
        let (out, rx) = std::sync::mpsc::channel();
        pool.tail
            .send(TailJob {
                ctx,
                n_txs,
                started,
                cold,
                edges,
                dispatch,
                out,
                delta_out,
                feed,
            })
            .map_err(|_| ExecutorError::State("stm pool: tail thread gone".into()))?;
        Ok(BlockTicket { rx })
    }
}
