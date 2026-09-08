use super::config::MAX_BLOCK_TXS;
use super::config::{BlockTxCount, nanos};
use super::metrics::{Metrics, PaddedLen, TxResult};
use super::recycle::RecyclePools;
use super::session::BoundLayers;
use super::view::BaseCache;
use crate::mv::MvCache;
use kardamom_exec_core::block_env::ExecEnv;
use kardamom_exec_core::delta::PendingDelta;
use kardamom_exec_core::error::ExecutorError;
use kardamom_exec_core::exec_types::TxIndex;
use kardamom_exec_core::executor::DecodedTx;
use kardamom_types::BPosition;
use kardamom_types::StateDatabase;
use kardamom_types::TxEnvelope;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;

/// One state view per worker, checked once at the block boundary: the
/// invariant every later read of `BlockCtx::snapshots` depends on.
pub(super) struct PerWorker<S>(Vec<S>);

impl<S> PerWorker<S> {
    pub(super) fn new(
        snapshots: Vec<S>,
        workers: std::num::NonZeroUsize,
    ) -> Result<Self, ExecutorError> {
        if snapshots.len() < workers.get() {
            return Err(ExecutorError::State(format!(
                "one state view per worker: {} given, {workers} needed",
                snapshots.len()
            )));
        }
        Ok(Self(snapshots))
    }

    /// The first worker's view: what sequential execution and the
    /// commit tail's single-threaded reads use.
    pub(super) fn primary(&self) -> &S {
        &self.0[0]
    }

    /// Worker `w`'s own view.
    pub(super) fn for_worker(&self, w: usize) -> &S {
        &self.0[w % self.0.len()]
    }
}

/// The bag scheduler's shared runnable set. Its capacity is
/// `MAX_BLOCK_TXS`, the same bound admission enforces
/// (`push_prepared` returns an error above it), so a push can never
/// overflow: the bound holds by construction, not by a guard at the
/// push site.
pub(super) struct Bag(crossbeam_queue::ArrayQueue<u32>);

impl Bag {
    pub(super) fn new() -> Self {
        Self(crossbeam_queue::ArrayQueue::new(MAX_BLOCK_TXS))
    }

    /// Push a ready transaction. Never overflows: see the type doc.
    pub(super) fn push(&self, idx: u32) {
        self.0
            .push(idx)
            .expect("bag capacity is MAX_BLOCK_TXS; push_prepared already caps admission there");
    }

    pub(super) fn pop(&self) -> Option<u32> {
        self.0.pop()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// One admitted transaction. Its slot is set before its index becomes
/// visible to workers (through the ready heap), so workers read it
/// lock-free.
pub(super) struct TxSlot {
    pub(super) tx_idx: TxIndex,
    pub(super) position: BPosition,
    pub(super) envelope: TxEnvelope,
    pub(super) decoded: Option<DecodedTx>,
    /// Predicted cell hashes (from `prepare`, off-thread). Sharded
    /// admission reads them from here; the serial feed uses the
    /// `Prepared` copy directly and leaves this empty.
    pub(super) hashes: smallvec::SmallVec<[u64; 4]>,
}

/// One worker's FIFO. The feed pushes (single producer) and the worker
/// pops (single consumer): a two-party lock with near-zero contention.
/// Canonical arrival order per thread means same-domain chains execute
/// in order with no cross-thread coordination at all.
pub(super) struct WorkerQueue {
    pub(super) q: Mutex<std::collections::VecDeque<u32>>,
    /// Length hint, maintained alongside every queue mutation. Spinning
    /// workers and the steal scan read this instead of taking the
    /// mutex. A dry worker probing its queue for tens of microseconds,
    /// and every steal attempt locking every queue just to read a
    /// length, were contending with the feed's submissions. This was the
    /// measured reason admission cost grew with worker count on
    /// fully-independent work. A stale read costs one wasted lock
    /// attempt or one missed-then-caught item; the authoritative
    /// empty-check before parking still happens under the mutex.
    pub(super) len: std::sync::atomic::AtomicUsize,
    pub(super) cv: Condvar,
    /// Whether this worker is parked on `cv`. Waking a thread that is
    /// already running costs a futex syscall for nothing, and dispatch
    /// happens once per transaction. On a 21k-gas transfer that is about
    /// 2.7us of real work, so a wasted wake is a large fraction of the
    /// budget.
    pub(super) parked: AtomicBool,
}

/// One transaction's node in the live dependency DAG, and its
/// registration point.
///
/// The lock-free trick that removes the global admission lock: a node is
/// "still in flight" exactly while its `children` list is open. Admission
/// registers an edge by pushing into a predecessor's open list; the
/// predecessor's completion closes the list (`None`) and drains it. Both
/// happen under that one node's tiny mutex, so "is p outstanding?" and
/// "register my edge on p" are a single atomic step. This is precisely
/// the guarantee a `Weak::upgrade` cannot give on its own: dropping the
/// last strong reference does not order against a concurrent
/// registration, so an edge could be registered onto a list already
/// drained, and its child would then wait forever for a decrement that
/// nobody sends.
///
/// Contention is nil: only the single feed thread pushes, and only the
/// one worker that executed p closes. No structure is contended by two
/// threads for the whole block.
#[derive(Default)]
pub(super) struct Node {
    /// True while this transaction is outstanding and accepting edges.
    /// Flipped under `children`'s lock, which is what makes "is p
    /// outstanding?" and "register my edge on p" one atomic step.
    pub(super) open: AtomicBool,
    /// Children registered while open. Drained in place at close so the
    /// buffer keeps its capacity for the next block that reuses this
    /// arena slot: steady-state allocation is zero.
    pub(super) children: Mutex<Vec<u32>>,
    /// Outstanding predecessors. Carries a +1 admission guard while the
    /// feed is still registering this transaction's edges, so a
    /// predecessor that finishes mid-admission cannot drive the count to
    /// zero early and dispatch a half-linked transaction.
    pub(super) indegree: AtomicU32,
    /// The thread this transaction was assigned. Written before any edge
    /// naming it exists, so whoever dispatches it reads a settled value.
    pub(super) worker: std::sync::atomic::AtomicUsize,
    /// True once this node has been handed to a worker queue. The eager
    /// coverage test reads this, not `indegree == 0`, because prune
    /// decrements indegrees first and pushes later. In that window the
    /// feed would otherwise enqueue a successor ahead of its
    /// predecessor, and the owner would then spin forever on a head
    /// whose FIFO predecessor sits behind it (an intermittent silent
    /// wedge, observed on the transfers shape). This flag is set under
    /// the queue lock, so a `true` read orders the predecessor's push
    /// before any subsequent eager push to the same queue.
    pub(super) queued: AtomicBool,
    /// Predecessors covered by FIFO order instead of an edge (eager
    /// chain mode): they were already released to this transaction's own
    /// queue when it was admitted, so queue position orders them, with
    /// no edge and no prune hand-off. Written only by the serial feed
    /// before the transaction can be released. Read by whoever takes
    /// the transaction from a queue, which must verify each one has a
    /// result before executing: work stealing can move a FIFO
    /// predecessor to another thread mid-flight, and this verification
    /// is what makes that race benign instead of a data race on state.
    pub(super) fifo_preds: Mutex<Vec<u32>>,
}

/// Per-block shared context; workers hold an `Arc` for the block's
/// duration.
pub(super) struct BlockCtx<S: StateDatabase> {
    pub(super) env: ExecEnv,
    /// One state view per worker.
    ///
    /// This is not an optimization; it is a requirement of the backend.
    /// mdbx's synchronized read transaction guards its pointer with a
    /// mutex, so workers sharing one snapshot funnel every state read
    /// through one lock. This was measured as parallel execution getting
    /// slower with more workers, while the in-memory backend scaled
    /// normally. Each worker therefore reads through its own
    /// transaction, all opened at the same committed block, so the view
    /// is identical.
    pub(super) snapshots: PerWorker<S>,
    pub(super) base: PendingDelta,
    /// EIP-7928 capture: `Some(base_index)` turns on per-transaction
    /// fragment capture, with fragment indices `base_index + local_idx +
    /// 1` (block-global; the caller passes the count of canonical
    /// records before this run, non-zero when a block is segmented
    /// around deposits). `None` means no capture (the default;
    /// validators and benches never pay for it).
    pub(super) bal_base: Option<u64>,
    /// Unsettled predecessor deltas, newest first (see `BlockInput`),
    /// plus the fee-sink block-start view: everything about the block's
    /// read base that depends on its predecessor's outcome. This is
    /// late-bound: admission is layer-independent, so a pipelined
    /// consumer builds, feeds, and submits this block during its
    /// predecessor's execution, and binds the layers when the
    /// predecessor's delta releases. Workers wait for the bind before
    /// executing (see `run_worker_block`); the block-at-a-time path
    /// binds at session build, making the wait free.
    pub(super) binding: std::sync::OnceLock<BoundLayers>,
    /// An Arc, so this outlives the block as a predecessor mv layer
    /// (mv-as-layer releases clone it; the last holder drops it, usually
    /// the reaper).
    pub(super) mv: Arc<MvCache>,
    /// Shared read-through cache over the immutable block-input layer.
    pub(super) base_cache: std::sync::Arc<BaseCache>,
    /// Recycle pools (read-record buffers for workers; the tail ships
    /// spent arenas back through the reaper).
    pub(super) recycle: std::sync::Arc<RecyclePools>,
    pub(super) slots: Vec<std::sync::OnceLock<TxSlot>>,
    pub(super) results: Vec<std::sync::OnceLock<Result<TxResult, ExecutorError>>>,
    pub(super) queues: Vec<WorkerQueue>,
    /// Bag-scheduler mode (see `Scheduler::Bag`): the shared runnable
    /// set. Always allocated, used only when `bag_mode`.
    pub(super) bag: Bag,
    pub(super) bag_mode: bool,
    /// The pool's arena (see [`PoolHandle::arena`]): shared, never
    /// reallocated, indexed concurrently while the feed is still
    /// admitting.
    pub(super) nodes: Arc<Vec<Node>>,
    /// Admitted (feed-only writer) and finished (workers) counts. The
    /// block is drained when it is sealed and the two agree.
    pub(super) admitted: AtomicU32,
    pub(super) finished: AtomicU32,
    pub(super) sealed: AtomicBool,
    /// Per-worker completion buffers: a finishing worker parks its index
    /// here (uncontended, since it owns the slot) instead of retiring
    /// edges on every transaction. A prune drains them.
    pub(super) completed: Vec<Mutex<Vec<u32>>>,
    /// Length of each buffer, readable without taking its mutex. A prune
    /// would otherwise lock every worker's buffer just to find it empty,
    /// and spinning workers force-prune often, which was the largest
    /// single overhead on micro-gas workloads. Padded to a cache line
    /// each: these counters are read-modify-written by every worker on
    /// every completion and read by every prune, so packing them into a
    /// plain array let each completion invalidate the cache line for
    /// every other worker.
    pub(super) completed_len: Vec<PaddedLen>,
    /// Completions parked across all buffers: DAG updates owed.
    pub(super) pending: std::sync::atomic::AtomicU64,
    pub(super) prune_batch: usize,
    /// Block start, so workers can stamp first-dispatch and
    /// last-completion offsets without reaching into the session.
    pub(super) started: std::time::Instant,
    /// Whether an idle worker may take a ready transaction from another
    /// thread.
    ///
    /// Stealing migrates a transaction to a core whose caches know
    /// nothing about the accounts it touches. That pays off well when
    /// the transaction is expensive (a uniswap swap, about 15us) and
    /// loses badly when it is not (a 21k-gas transfer, about 2.75us),
    /// where the migration costs more than the work it moves. So the
    /// policy is measured, not fixed: the pool tracks mean
    /// per-transaction execution time and enables stealing only above a
    /// threshold.
    pub(super) steal_enabled: bool,
    /// How long a dry worker spins before parking, in nanoseconds, sized
    /// from the measured mean per-transaction time. A short fixed spin
    /// was much shorter than the typical gap between chain-link
    /// releases (about one transaction's execution time), so workers
    /// parked into the exact window their next transaction arrived in
    /// and paid the full poll interval to notice it. Measurement showed
    /// most of a worker's idle time was this park latency, not
    /// scheduler cost.
    pub(super) spin_ns: u64,
    pub(super) aborted: AtomicBool,
    /// Nodes observed leaving the graph more than once. Always zero. A
    /// non-zero value is a scheduler bug surfaced at seal, rather than a
    /// silently stranded edge.
    pub(super) double_exit: AtomicU32,
    pub(super) metrics: Metrics,
}

/// A completing node's ready-children buffer: the first 8 stay on the
/// stack; the rest spill to a heap `Vec`. No per-completion allocation
/// on the common case, where a node has few children.
#[derive(Default)]
struct ReadyBuf {
    buf: [u32; 8],
    n: usize,
    spill: Vec<u32>,
}

impl ReadyBuf {
    fn push(&mut self, c: u32) {
        if self.n < self.buf.len() {
            self.buf[self.n] = c;
            self.n += 1;
        } else {
            self.spill.push(c);
        }
    }

    /// Every buffered index, stack entries first, then the spill.
    fn drain(&mut self) -> impl Iterator<Item = u32> + '_ {
        self.buf[..self.n]
            .iter()
            .copied()
            .chain(self.spill.drain(..))
    }
}

/// The prune pass's cross-worker ready set: each entry names the
/// worker a newly-ready child was assigned to, so `prune` can hand it
/// off once every worker's completed buffer has drained.
#[derive(Default)]
struct ReadySet(Vec<(usize, u32)>);

impl ReadySet {
    fn push(&mut self, worker: usize, idx: u32) {
        self.0.push((worker, idx));
    }
}

impl IntoIterator for ReadySet {
    type Item = (usize, u32);
    type IntoIter = std::vec::IntoIter<(usize, u32)>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

/// Lock order (the engine's one hard rule): the graph lock may be taken
/// while holding nothing, and a queue lock may be taken while holding
/// nothing or the graph lock's results, but never while the graph lock
/// is held. Every dispatch therefore collects its ready set under the
/// graph lock, releases it, and only then pushes. The idle path takes
/// the graph lock only after dropping its queue lock.
impl<S: StateDatabase> BlockCtx<S> {
    /// Wake every worker queue's condvar: a parked worker re-checks its
    /// queue, the bag, and the drain condition on every wake, so this
    /// is the one signal every "something changed" path sends.
    pub(super) fn wake_all(&self) {
        for q in &self.queues {
            q.cv.notify_all();
        }
    }

    /// Index `idx`'s envelope slot, pairing the index with the
    /// invariant every reader relies on: admission sets a slot before
    /// the index that names it becomes visible (through the ready
    /// queue, the bag, or a batch flush), so every later read here
    /// finds it populated.
    ///
    /// # Panics
    /// Panics if the slot is unset, which means an index reached a
    /// reader before admission finished writing it: a scheduler
    /// invariant violation, not a caller contract.
    pub(super) fn slot(&self, idx: usize) -> &TxSlot {
        self.slots[idx]
            .get()
            .expect("admission sets a slot before its index becomes visible")
    }

    /// This block's read base, once [`LayerBinder::bind`](super::session::LayerBinder::bind)
    /// has installed it.
    ///
    /// # Panics
    /// Panics if called before binding: every caller runs after
    /// `wait_for_binding` (or the block-at-a-time path, which binds at
    /// session build), so this is a scheduler-invariant check, not a
    /// caller contract.
    pub(super) fn bound(&self) -> &BoundLayers {
        self.binding.get().expect("layers bound before execution")
    }

    /// Close `job`'s node: its one legitimate exit from the graph. A
    /// second close would strand every edge registered on it in
    /// between, so the invariant is checked, not assumed.
    ///
    /// Must be called with `node.children`'s lock already held (`node`
    /// must be `job`'s own node): the same lock a concurrent edge
    /// registration checks `open` under, which is what makes "is this
    /// node still open?" and "close it" one atomic step relative to
    /// admission.
    fn close_node(&self, job: u32, node: &Node) {
        if !node.open.swap(false, Ordering::AcqRel) {
            tracing::error!(
                tx = job,
                "stm: tx left the graph twice — scheduler invariant violated"
            );
            self.double_exit.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// Take an edge from `p` to `idx`, if `p` is still open: increments
    /// `idx`'s indegree and appends it to `p`'s child list. Returns
    /// whether an edge was actually taken; the caller tallies its own
    /// edge counter (a plain field or an atomic, depending on caller)
    /// on `true`.
    pub(super) fn try_edge(&self, p: u32, idx: u32) -> bool {
        let pn = &self.nodes[p as usize];
        let mut list = pn.children.lock().expect("children poisoned");
        if pn.open.load(Ordering::Acquire) {
            self.nodes[idx as usize]
                .indegree
                .fetch_add(1, Ordering::AcqRel);
            list.push(idx);
            true
        } else {
            false
        }
    }

    /// May `idx` execute right now? True when every FIFO-covered
    /// predecessor has a result. Ordinary FIFO drain makes this true by
    /// construction, since the predecessor sat ahead in the same queue.
    /// It is false only when a steal moved a predecessor to another
    /// thread and that thread is still running it.
    pub(super) fn fifo_ready(&self, idx: u32) -> bool {
        let preds = self.nodes[idx as usize]
            .fifo_preds
            .lock()
            .expect("fifo_preds poisoned");
        preds
            .iter()
            .all(|p| self.results[*p as usize].get().is_some())
    }

    /// Steal one ready transaction from the longest other queue.
    ///
    /// Safe by verification: under eager chain mode, queue position does
    /// carry an ordering obligation (FIFO-covered predecessors have no
    /// edge), so anything taken from a queue, here or by its owner, is
    /// checked runnable first through `fifo_ready`. A mid-chain link
    /// fails the check and stays put.
    ///
    /// Taken from the back, leaving the owner its front: the owner's
    /// front holds the oldest entries, most likely to have warm state,
    /// and the two ends rarely contend.
    pub(super) fn steal(&self, thief: usize) -> Option<u32> {
        // Any queued transaction is stealable; queues hold 0 or 1 items
        // almost always, since a domain releases one ready transaction
        // at a time under DAG chains. Hint lengths only; the victim's
        // own lock confirms below. Ties keep the earliest worker index,
        // matching a left-to-right scan.
        let best = self
            .queues
            .iter()
            .enumerate()
            .filter(|&(w, _)| w != thief)
            .map(|(w, qh)| (w, qh.len.load(Ordering::Acquire)))
            .filter(|&(_, len)| len >= 1)
            .max_by_key(|&(w, len)| (len, std::cmp::Reverse(w)));
        let (victim, _) = best?;
        let vq = &self.queues[victim];
        // Verification stays under the victim's lock, deliberately. This
        // is the one place the pop, verify, and putback steps must be
        // atomic. A back-putback after an unlocked window can reorder
        // things: the feed can eagerly enqueue the candidate's own
        // FIFO-successor into the gap, the putback lands behind it, and
        // the owner livelocks on a head whose predecessor now sits
        // behind it (this caused several test runs to hang). The owner's
        // pop-path verify can run unlocked, because its front-putback
        // preserves relative order; a back-putback cannot.
        let mut q = vq.q.lock().expect("queue poisoned");
        let cand = q.pop_back()?;
        if self.fifo_ready(cand) {
            vq.len.fetch_sub(1, Ordering::Release);
            Some(cand)
        } else {
            q.push_back(cand);
            None
        }
    }

    /// Hand a ready transaction to its assigned thread. Called only with
    /// no node mutex held (lock order: node registration points are
    /// leaves).
    pub(super) fn push_ready(&self, worker: usize, idx: u32) {
        if self.bag_mode {
            // One shared lock-free runnable set: no assignment, no
            // per-worker locks, balanced by whoever pops first. `queued`
            // is irrelevant, since coverage is off in bag mode (every
            // dependency is an edge).
            self.bag.push(idx);
            if let Some(qh) = self
                .queues
                .iter()
                .find(|qh| qh.parked.load(Ordering::Acquire))
            {
                qh.cv.notify_one();
            }
            return;
        }
        let qh = &self.queues[worker];
        {
            let mut q = qh.q.lock().expect("queue poisoned");
            self.nodes[idx as usize]
                .queued
                .store(true, Ordering::Release);
            q.push_back(idx);
            qh.len.fetch_add(1, Ordering::Release);
        }
        // Only wake a worker that actually parked. Under load the queue
        // is rarely empty, so this elides nearly every syscall.
        if qh.parked.load(Ordering::Acquire) {
            qh.cv.notify_one();
        }
    }

    /// Bag-mode completion, inline: the finishing worker closes its own
    /// node right here, with no per-worker completion buffer, no
    /// cross-worker buffer scan, and no `pending` counter round-trip.
    /// One uncontended child-list lock, one indegree `fetch_sub` per
    /// child, and ready children go straight to the bag.
    pub(super) fn complete_inline(&self, job: u32) -> Option<u32> {
        let node = &self.nodes[job as usize];
        // Collect under the lock, dispatch after: the bag push is
        // lock-free, but keeping the child-list critical section minimal
        // matters while the feed races to register on this node.
        let mut ready = {
            let mut list = node.children.lock().expect("children poisoned");
            self.close_node(job, node);
            let mut ready = ReadyBuf::default();
            list.iter()
                .filter_map(|&c| self.collect_if_ready(c))
                .for_each(|c| ready.push(c));
            list.clear();
            ready
        };
        self.finished.fetch_add(1, Ordering::SeqCst);
        self.metrics.completions.fetch_add(1, Ordering::Relaxed);
        // Chain-local hand-off: the first ready child stays with the
        // completing worker as its next job. It is returned directly,
        // with no bag operation, so the cache stays warm. A chain
        // streams on one core exactly as the FIFO scheduler streamed
        // it, without a queue. The rest go to the bag for whoever is
        // free.
        let mut it = ready.drain();
        let keep = it.next();
        it.for_each(|c| self.push_ready(0, c));
        if self.drained() {
            self.wake_all();
        }
        keep
    }

    /// Decrement child `c`'s indegree, and return it when this was its
    /// last outstanding predecessor. `None` means another predecessor is
    /// still outstanding.
    fn collect_if_ready(&self, c: u32) -> Option<u32> {
        let child = &self.nodes[c as usize];
        (child.indegree.fetch_sub(1, Ordering::AcqRel) == 1).then_some(c)
    }

    /// Apply parked completions to the live DAG: close each finished
    /// node's registration point, retire the edges that were registered
    /// while it was open, and hand whatever became ready to its thread.
    /// Takes no global lock, only the finished nodes' own mutexes.
    pub(super) fn prune(&self, forced: bool) -> usize {
        let t0 = std::time::Instant::now();
        let mut ready = ReadySet::default();
        let applied: usize = self
            .completed
            .iter()
            .enumerate()
            .map(|(w, buf)| self.drain_worker(w, buf, &mut ready))
            .sum();
        if applied > 0 {
            self.pending.fetch_sub(applied as u64, Ordering::SeqCst);
            self.finished
                .fetch_add(BlockTxCount::new(applied).get(), Ordering::SeqCst);
        }
        self.metrics
            .prune_ns
            .fetch_add(nanos(t0.elapsed()), Ordering::Relaxed);
        self.metrics.prune_calls.fetch_add(1, Ordering::Relaxed);
        if forced {
            self.metrics.prune_forced.fetch_add(1, Ordering::Relaxed);
        }
        self.metrics
            .completions
            .fetch_add(applied as u64, Ordering::Relaxed);
        for (w, c) in ready {
            self.push_ready(w, c);
        }
        if self.drained() {
            self.wake_all();
        }
        applied
    }

    /// Drain worker `w`'s completed buffer, close each finished node,
    /// fold its newly ready children into `ready`, and return the
    /// drained count.
    fn drain_worker(&self, w: usize, buf: &Mutex<Vec<u32>>, ready: &mut ReadySet) -> usize {
        // Skip untouched buffers without paying for their mutex.
        if self.completed_len[w].0.load(Ordering::Acquire) == 0 {
            return 0;
        }
        let Some(drained) = Self::take_drained(&self.completed_len[w], buf) else {
            return 0;
        };
        let n = drained.len();
        for job in drained {
            self.close_and_collect(job, ready);
        }
        n
    }

    /// Take worker `w`'s completed buffer under its lock, and lower its
    /// atomic length counter to match, unless the buffer is empty.
    fn take_drained(len: &PaddedLen, buf: &Mutex<Vec<u32>>) -> Option<Vec<u32>> {
        let mut b = buf.lock().expect("completed poisoned");
        if b.is_empty() {
            return None;
        }
        len.0
            .fetch_sub(BlockTxCount::new(b.len()).get(), Ordering::AcqRel);
        Some(std::mem::take(&mut *b))
    }

    /// Leave `job`'s node once, and fold each of its children into
    /// `ready` when this closed edge was their last outstanding
    /// predecessor. Closing is the node's exit from the graph; a second
    /// close would strand every edge registered in between, so this is
    /// asserted rather than assumed. The child list is drained in place
    /// so its capacity survives for the next block that reuses this
    /// arena slot. The `for` loop in [`Self::drain_worker`] stays free
    /// of a branch.
    fn close_and_collect(&self, job: u32, ready: &mut ReadySet) {
        let node = &self.nodes[job as usize];
        let mut list = node.children.lock().expect("children poisoned");
        self.close_node(job, node);
        for c in list.iter() {
            self.queue_ready_child(*c, ready);
        }
        list.clear();
    }

    /// Decrement child `c`'s indegree, and record it as ready, with its
    /// assigned worker, when this was its last outstanding predecessor.
    /// The `for` loop in [`Self::close_and_collect`] stays free of a
    /// branch.
    fn queue_ready_child(&self, c: u32, ready: &mut ReadySet) {
        let child = &self.nodes[c as usize];
        if child.indegree.fetch_sub(1, Ordering::AcqRel) != 1 {
            return;
        }
        ready.push(child.worker.load(Ordering::Acquire), c);
    }

    pub(super) fn drained(&self) -> bool {
        self.sealed.load(Ordering::SeqCst)
            && self.finished.load(Ordering::SeqCst) == self.admitted.load(Ordering::SeqCst)
    }
}
