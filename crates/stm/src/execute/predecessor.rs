//! Admission's predecessor graph: sharded dependency discovery (last-
//! toucher registration and edge publication against the DAG), and the
//! serial equivalent. `session.rs` owns the admission entry points that
//! call into this half.

use super::config::{ADMIT_BATCH, nanos};
use super::graph::BlockCtx;
use super::session::{AdmitTarget, BlockSession};
use super::touch::TouchTable;
use kardamom_exec_core::error::ExecutorError;
use kardamom_types::StateDatabase;
use std::sync::atomic::Ordering;

/// One [`BlockSession::link_predecessor`] call's fixed context: the
/// same for every predecessor of one admitted transaction.
#[derive(Clone, Copy)]
struct PredLinkCtx {
    idx: u32,
    i: usize,
    worker: usize,
    eager: bool,
}

/// One [`BlockSession::link_predecessor`] outcome.
enum Link {
    /// An edge was taken from the predecessor.
    Edge,
    /// The predecessor is FIFO-covered instead; no edge.
    Covered,
    /// The predecessor already finished, or is the transaction itself.
    None,
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
    fn register(&mut self, idx: u32) {
        let slot = self.ctx.slot(idx as usize);
        for h in &slot.hashes {
            self.register_one(idx, *h);
        }
    }

    /// One cell hash's registration: skip it when it does not belong to
    /// this lane's shard, otherwise upsert it and take an edge from
    /// whichever transaction it displaces, if still open. The `for`
    /// loop in [`Self::register`] stays free of a branch.
    #[allow(
        clippy::cast_possible_truncation,
        reason = "h % k is < k, which came from usize (admit_shards), so it always fits back in usize"
    )]
    fn register_one(&mut self, idx: u32, h: u64) {
        if (h % self.k as u64) as usize != self.sh {
            return;
        }
        let Some(p) = self.table.upsert(h, idx) else {
            return;
        };
        if p != idx && self.ctx.try_edge(p, idx) {
            self.edges.fetch_add(1, Ordering::Relaxed);
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
            None => Self::run_shards_sequential(k, &body),
        }
        self.edges += edges.load(Ordering::Relaxed);
        // Every edge for this batch is registered: drop the router
        // guards, dispatching whatever is ready.
        for &idx in &self.admit_batch {
            self.dispatch_if_ready(idx);
        }
        self.admit_batch.clear();
    }

    /// Run every shard's discovery pass on this thread, in order,
    /// instead of one lane per shard. The `None` arm in
    /// [`Self::flush_admit_batch`] stays free of a loop.
    fn run_shards_sequential(k: usize, body: &impl Fn(usize)) {
        for sh in 0..k {
            body(sh);
        }
    }

    /// Drop `idx`'s admission guard, and dispatch it when this was the
    /// last one outstanding. The `for` loop in
    /// [`Self::flush_admit_batch`] stays free of a branch.
    fn dispatch_if_ready(&self, idx: u32) {
        if self.ctx.nodes[idx as usize]
            .indegree
            .fetch_sub(1, Ordering::AcqRel)
            != 1
        {
            return;
        }
        let w = self.ctx.nodes[idx as usize].worker.load(Ordering::Acquire);
        self.ctx.push_ready(w, idx);
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
    pub(super) fn admit_sharded(
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
            self.admit_cold_sharded(idx);
        }
        self.admit_batch.push(idx);
        if self.admit_batch.len() >= ADMIT_BATCH {
            self.flush_admit_batch();
        }
        self.feed.feed_ns = self.feed.feed_ns.saturating_add(nanos(t_feed.elapsed()));
        Ok(())
    }

    /// Handle a ⊤ transaction under sharded admission: settle the
    /// pending batch, take an edge from every outstanding predecessor,
    /// then become the new barrier and clear every shard table. The
    /// `if` in [`Self::admit_sharded`] stays free of a loop.
    fn admit_cold_sharded(&mut self, idx: u32) {
        self.flush_admit_batch();
        for p in 0..idx {
            self.try_edge(p, idx);
        }
        self.last_barrier = Some(idx);
        self.pool.shards_clear();
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
    pub(super) fn admit_serial(
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
                Self::collect_hash_preds(&mut touch, hashes, idx, &mut preds);
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
        let link_ctx = PredLinkCtx {
            idx,
            i,
            worker,
            eager: self.pool.eager_chain,
        };
        let (deg, covered) = preds.iter().fold((0u32, 0u64), |(deg, covered), &p| {
            match self.link_predecessor(p, &link_ctx) {
                Link::Edge => (deg + 1, covered),
                Link::Covered => (deg, covered + 1),
                Link::None => (deg, covered),
            }
        });
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

    /// Upsert every hash in `hashes` against the last-toucher index,
    /// collecting the predecessors it displaces, then sort and dedup
    /// them. The `else` arm in [`Self::admit_serial`] stays free of a
    /// loop.
    fn collect_hash_preds(touch: &mut TouchTable, hashes: &[u64], idx: u32, preds: &mut Vec<u32>) {
        // Hashes came from `prepare`, off this thread.
        for h in hashes {
            Self::push_if_touched(touch, *h, idx, preds);
        }
        preds.sort_unstable();
        preds.dedup();
    }

    /// Record `idx` as `h`'s predecessor when the hash was already
    /// touched this block. The `for` loop in
    /// [`Self::collect_hash_preds`] stays free of a branch.
    fn push_if_touched(touch: &mut TouchTable, h: u64, idx: u32, preds: &mut Vec<u32>) {
        let Some(p) = touch.upsert(h, idx) else {
            return;
        };
        preds.push(p);
    }

    /// Take an edge from `p` to the transaction being admitted, or cover
    /// it by FIFO position instead, unless `p` already finished and
    /// published (no edge needed) or `p` is the transaction itself.
    /// [`Self::admit_serial`]'s loop folds the count with the returned
    /// [`Link`].
    fn link_predecessor(&mut self, p: u32, link: &PredLinkCtx) -> Link {
        if p == link.idx {
            return Link::None;
        }
        let pn = &self.ctx.nodes[p as usize];
        let mut list = pn.children.lock().expect("children poisoned");
        if !pn.open.load(Ordering::Acquire) {
            return Link::None;
        }
        // p is unfinished. If it was already released to this
        // transaction's own queue (indegree 0 is definitive: admission
        // is serial, so p's guard was dropped long ago and a released
        // node is never re-blocked), FIFO position orders it. Record it
        // for take-time verification instead of an edge, and the whole
        // prune hand-off for this link disappears. A stale read of a
        // nonzero indegree only costs an edge, never correctness.
        if link.eager
            && !self.ctx.bag_mode
            && pn.worker.load(Ordering::Acquire) == link.worker
            && pn.queued.load(Ordering::Acquire)
        {
            self.ctx.nodes[link.i]
                .fifo_preds
                .lock()
                .expect("fifo_preds poisoned")
                .push(p);
            return Link::Covered;
        }
        // Increment before publishing the edge: the matching decrement
        // can only happen once this child is visible in p's list, so
        // the add always precedes its own subtract.
        self.ctx.nodes[link.i]
            .indegree
            .fetch_add(1, Ordering::AcqRel);
        list.push(link.idx);
        if pn.worker.load(Ordering::Acquire) == link.worker && pn.queued.load(Ordering::Acquire) {
            self.feed.redundant_edges += 1;
        }
        Link::Edge
    }
}
