//! The parallel block executor (spec §P2, offline milestone): workers pull
//! DAG-ready txs, execute each against a per-tx multi-version view, publish
//! versions, and a canonical-order commit pass materializes the exact
//! sequential artifacts — receipts (cumulative gas, accumulator-fixed
//! write-set hashes) and the block `PendingDelta`, byte-identical to
//! `ExecScope` output by construction and re-checked by validation.
//!
//! Wound-wait runtime detection (ESTIMATE marks, child self-abort) is
//! deliberately NOT here yet: under pessimistic scheduling a conflict is an
//! ordered edge, so the miss classes it accelerates are expected ~never
//! (P1 shadow: 0 across 18,400 graded txs). Validation + whole-block
//! sequential fallback (invariant #3) carries correctness alone in P2a;
//! the optimization lands with P2b once the A/B shows where it pays.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Condvar, Mutex};

use alloy_primitives::{B256, U256};
use kardamom_exec_core::block_env::ExecEnv;
use kardamom_exec_core::delta::{PendingDelta, WriteSet};
use kardamom_exec_core::error::ExecutorError;
use kardamom_exec_core::exec_types::{ReceiptStatus, TxIndex};
use kardamom_exec_core::executor::{
    ExecScope, SnapshotRef, decode_alloy_envelope, invalid_skip, tx_env_from_alloy, wire_log,
    write_set_from_evm_state,
};
use kardamom_footprint::Cell;
use kardamom_footprint::classifier::{DomainKey, Stats};
use kardamom_types::{BPosition, Receipt, StateDatabase, TxEnvelope};
use revm::context::result::ExecutionResult;
use revm::database::DatabaseRef;
use revm::state::AccountInfo;
use revm::{Context, ExecuteEvm, MainBuilder, MainContext};

use crate::FEE_SINK;
use crate::mv::{MvCache, ReadRecord};
use crate::schedule;

/// Layered block-input view: the pre-block delta over the snapshot — what
/// sequential execution sees at the block's first tx. Read-only and shared
/// by every worker.
pub struct BlockInput<'a, S: StateDatabase> {
    pub snapshot: &'a S,
    pub base: Option<&'a PendingDelta>,
}

impl<'a, S: StateDatabase> DatabaseRef for BlockInput<'a, S> {
    type Error = kardamom_exec_core::executor::StateRefError;

    fn basic_ref(
        &self,
        address: alloy_primitives::Address,
    ) -> Result<Option<AccountInfo>, Self::Error> {
        if let Some(base) = self.base
            && let Some((nonce, balance, code_hash)) = base.accounts.get(&address)
        {
            let code_hash = if *code_hash == B256::ZERO {
                revm::primitives::KECCAK_EMPTY
            } else {
                *code_hash
            };
            return Ok(Some(AccountInfo {
                nonce: *nonce,
                balance: *balance,
                code_hash,
                account_id: None,
                code: None,
            }));
        }
        SnapshotRef {
            inner: self.snapshot,
        }
        .basic_ref(address)
    }

    fn code_by_hash_ref(&self, code_hash: B256) -> Result<revm::state::Bytecode, Self::Error> {
        if let Some(base) = self.base
            && let Some(code) = base.code.get(&code_hash)
            && !code.is_empty()
        {
            return Ok(revm::state::Bytecode::new_raw(
                alloy_primitives::Bytes::copy_from_slice(code),
            ));
        }
        SnapshotRef {
            inner: self.snapshot,
        }
        .code_by_hash_ref(code_hash)
    }

    fn storage_ref(
        &self,
        address: alloy_primitives::Address,
        index: U256,
    ) -> Result<U256, Self::Error> {
        let key = B256::from(index.to_be_bytes::<32>());
        if let Some(base) = self.base
            && let Some(v) = base.storage.get(&(address, key))
        {
            return Ok(*v);
        }
        SnapshotRef {
            inner: self.snapshot,
        }
        .storage_ref(address, index)
    }

    fn block_hash_ref(&self, number: u64) -> Result<B256, Self::Error> {
        SnapshotRef {
            inner: self.snapshot,
        }
        .block_hash_ref(number)
    }
}

/// Per-tx database view: multi-version cache at index `idx` over the block
/// input, recording every first read for validation. The fee sink is served
/// from the cached block-start info and never recorded — the `Accumulator`
/// boundary (its correctness is the commit pass's prefix algebra, not
/// version validation).
struct MvView<'a, S: StateDatabase> {
    mv: &'a MvCache,
    base: &'a BlockInput<'a, S>,
    idx: u32,
    reads: Vec<ReadRecord>,
    sink_start: Option<AccountInfo>,
    // Worker-local memos over the IMMUTABLE-for-the-block layers (base
    // input; content-addressed code). Without them every MvCache miss
    // re-walks the delta BTreeMaps and the snapshot — and worse, re-copies
    // full contract bytecode per call. The multi-version lists themselves
    // are never memoized (their answers depend on the reader's index).
    base_accounts: std::collections::HashMap<alloy_primitives::Address, Option<AccountInfo>>,
    base_storage: std::collections::HashMap<(alloy_primitives::Address, B256), U256>,
    code_cache: std::collections::HashMap<B256, revm::state::Bytecode>,
}

impl<'a, S: StateDatabase> MvView<'a, S> {
    fn new(mv: &'a MvCache, base: &'a BlockInput<'a, S>, sink_start: Option<AccountInfo>) -> Self {
        Self {
            mv,
            base,
            idx: 0,
            reads: Vec::new(),
            sink_start,
            base_accounts: std::collections::HashMap::new(),
            base_storage: std::collections::HashMap::new(),
            code_cache: std::collections::HashMap::new(),
        }
    }
}

impl<'a, S: StateDatabase> revm::Database for MvView<'a, S> {
    type Error = kardamom_exec_core::executor::StateRefError;

    fn basic(
        &mut self,
        address: alloy_primitives::Address,
    ) -> Result<Option<AccountInfo>, Self::Error> {
        if address == FEE_SINK {
            return Ok(self.sink_start.clone());
        }
        if let Some((ver, a)) = self.mv.read_account(self.idx, &address) {
            self.reads.push(ReadRecord::Account(address, Some(ver)));
            return Ok(Some(AccountInfo {
                nonce: a.nonce,
                balance: a.balance,
                code_hash: if a.code_hash == B256::ZERO {
                    revm::primitives::KECCAK_EMPTY
                } else {
                    a.code_hash
                },
                account_id: None,
                code: None,
            }));
        }
        self.reads.push(ReadRecord::Account(address, None));
        if let Some(a) = self.base_accounts.get(&address) {
            return Ok(a.clone());
        }
        let a = self.base.basic_ref(address)?;
        self.base_accounts.insert(address, a.clone());
        Ok(a)
    }

    fn code_by_hash(&mut self, code_hash: B256) -> Result<revm::state::Bytecode, Self::Error> {
        // Content-addressed: no version, no record — memo both sources
        // (Bytecode clones are refcounted; the copy happens once).
        if let Some(c) = self.code_cache.get(&code_hash) {
            return Ok(c.clone());
        }
        let c = if let Some(code) = self.mv.read_code(&code_hash) {
            self.reads.push(ReadRecord::Code(code_hash, true));
            revm::state::Bytecode::new_raw(alloy_primitives::Bytes::copy_from_slice(&code))
        } else {
            // A MISS is recorded too: if a concurrent CREATE publishes this
            // hash later, this tx ran against absent code and is wounded.
            self.reads.push(ReadRecord::Code(code_hash, false));
            self.base.code_by_hash_ref(code_hash)?
        };
        self.code_cache.insert(code_hash, c.clone());
        Ok(c)
    }

    fn storage(
        &mut self,
        address: alloy_primitives::Address,
        index: U256,
    ) -> Result<U256, Self::Error> {
        let key = B256::from(index.to_be_bytes::<32>());
        if let Some((ver, v)) = self.mv.read_slot(self.idx, &address, &key) {
            self.reads.push(ReadRecord::Slot(address, key, Some(ver)));
            return Ok(v);
        }
        self.reads.push(ReadRecord::Slot(address, key, None));
        if let Some(v) = self.base_storage.get(&(address, key)) {
            return Ok(*v);
        }
        let v = self.base.storage_ref(address, index)?;
        self.base_storage.insert((address, key), v);
        Ok(v)
    }

    fn block_hash(&mut self, number: u64) -> Result<B256, Self::Error> {
        self.base.block_hash_ref(number)
    }
}

/// One executed tx's artifacts, pre-commit (cumulative gas and the
/// accumulator wsh fixup land in the canonical-order commit pass).
struct TxResult {
    receipt: Receipt,
    ws: WriteSet,
    reads: Vec<ReadRecord>,
    /// This tx's exact credit to the fee sink (post − block-start seen).
    fee_delta: U256,
}

/// Outcome of one block through the STM engine.
pub struct StmOutcome {
    pub receipts: Vec<Receipt>,
    pub delta: PendingDelta,
    /// Txs WOUNDED at validation — a conflict the marks missed, repaired
    /// by re-executing at the canonical position (per-tx; the whole block
    /// never re-runs). Zero on every measured workload so far.
    pub wounds: usize,
    /// Any wound fired (spec invariant #3's counter, per block).
    pub fallback: bool,
    /// ⊤ (cold, untrained-selector) txs — they wait out the prefix.
    pub cold: usize,
    /// Live-DAG edges created across the block (only against predecessors
    /// that were still outstanding at admission).
    pub edges: usize,
    /// Txs dispatched per worker queue — the domain-affinity histogram.
    pub dispatch: Vec<u32>,
    /// Scheduler cost, measured (the numbers the prune-batch knob is
    /// tuned on): time held in the graph lock split by cause, prune
    /// invocations and how many were starvation-forced, the realized
    /// batch size, and worker idle time.
    pub feed_us: u64,
    pub decode_us: u64,
    pub predict_us: u64,
    pub admit_us: u64,
    pub prune_us: u64,
    pub prune_calls: u64,
    pub prune_forced: u64,
    pub avg_batch: f64,
    pub idle_us: u64,
}

/// FNV-1a `BuildHasher` for the scheduler's internal maps.
///
/// `DomainKey`s are already high-entropy (addresses, hashes, key words),
/// so the standard library's SipHash — chosen to resist adversarial key
/// collisions in maps whose keys come from untrusted input — buys nothing
/// here and costs real time on a path measured in hundreds of nanoseconds
/// per tx. A collision-heavy map would only cost scheduling throughput,
/// never correctness: edges are still exact (the key COMPARISON is
/// unchanged), and a mispredicted schedule is wound-repairable.
#[derive(Default, Clone, Copy)]
struct FnvBuild;

struct Fnv(u64);

impl std::hash::BuildHasher for FnvBuild {
    type Hasher = Fnv;
    fn build_hasher(&self) -> Fnv {
        Fnv(0xcbf2_9ce4_8422_2325)
    }
}

impl std::hash::Hasher for Fnv {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, bytes: &[u8]) {
        for b in bytes {
            self.0 ^= *b as u64;
            self.0 = self.0.wrapping_mul(0x100_0000_01b3);
        }
    }
    fn write_u64(&mut self, n: u64) {
        self.write(&n.to_le_bytes());
    }
    fn write_u32(&mut self, n: u32) {
        self.write(&n.to_le_bytes());
    }
    fn write_u8(&mut self, n: u8) {
        self.write(&[n]);
    }
}

type FastMap<K, V> = HashMap<K, V, FnvBuild>;

/// The ⊤ key: a cold tx marks it, and every tx probes it before executing,
/// so "conflicts with everything" needs no graph. Not a real account — it
/// is never read or written by the EVM, only used as a pending-mark key.
const WILDCARD: alloy_primitives::Address = alloy_primitives::Address::repeat_byte(0xFF);

/// Stable domain → worker mapping. Quality only affects BALANCE across
/// threads, never correctness (ordering comes from marks + FIFO).
fn domain_hash(bytes: &[u8], workers: usize) -> usize {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in bytes {
        h ^= *b as u64;
        h = h.wrapping_mul(0x100_0000_01b3);
    }
    (h % workers as u64) as usize
}

/// Gas-limit-derived hard cap on txs per block: `BLOCK_GAS_LIMIT` / 21k
/// intrinsic gas = 1,428, with ~2.8x headroom. Slots are pre-allocated per
/// block so workers address them lock-free while the feed is still
/// admitting (slab reuse across blocks is a noted follow-up).
const MAX_BLOCK_TXS: usize = 4_096;

/// One admitted tx. Its slot is set BEFORE its index becomes visible to
/// workers (via the ready heap), so workers read it lock-free.
struct TxSlot {
    tx_idx: TxIndex,
    position: BPosition,
    envelope: TxEnvelope,
    decoded: Option<alloy_consensus::TxEnvelope>,
}

/// One worker's FIFO. The feed pushes (single producer), the worker pops
/// (single consumer) — a two-party lock with near-zero contention, and
/// canonical arrival order per thread means same-domain chains execute in
/// order with no cross-thread coordination at all.
struct WorkerQueue {
    q: Mutex<std::collections::VecDeque<u32>>,
    cv: Condvar,
}

/// Engine instrumentation — the numbers the prune-batch decision is made
/// on (spec: "health is judged on the PAIR of error rates plus realized
/// utilization"; the same discipline applies to the scheduler's own cost).
#[derive(Default)]
pub struct Metrics {
    /// Nanoseconds spent HOLDING the graph lock, split by cause.
    pub admit_ns: std::sync::atomic::AtomicU64,
    pub prune_ns: std::sync::atomic::AtomicU64,
    /// Prune invocations and how many were STARVATION-forced (a worker had
    /// nothing to run and had to apply pending completions itself).
    pub prune_calls: std::sync::atomic::AtomicU64,
    pub prune_forced: std::sync::atomic::AtomicU64,
    /// Completions applied by prunes (÷ prune_calls = realized batch).
    pub completions: std::sync::atomic::AtomicU64,
    /// Nanoseconds workers spent parked with nothing to run.
    pub idle_ns: std::sync::atomic::AtomicU64,
    /// Envelope decode and footprint prediction, the two pure-computation
    /// parts of admission (the rest is the graph lock + dispatch).
    pub decode_ns: std::sync::atomic::AtomicU64,
    pub predict_ns: std::sync::atomic::AtomicU64,
    /// WHOLE-admission nanoseconds (decode + predict + graph + dispatch).
    /// The feed is a single thread, so this is a hard serial floor on
    /// block latency — the number that says whether the scheduler or the
    /// workers are the constraint.
    pub feed_ns: std::sync::atomic::AtomicU64,
}

/// Pool configuration.
#[derive(Debug, Clone, Copy)]
pub struct PoolConfig {
    pub workers: usize,
    /// Apply completions to the DAG in batches of this many. `1` updates
    /// the graph on every completion (the immediate policy); larger values
    /// trade graph-lock traffic for dispatch latency — a worker that runs
    /// dry always force-prunes first, so batching can never starve the
    /// pool, only delay a handoff.
    pub prune_batch: usize,
}

/// MEASURED default (uniswap 8-pair, 16x500, 8 workers): batching 8
/// completions per graph-lock acquisition cuts prune time ~30% with no
/// wall-clock cost — worth taking, but NOT the lever. The binding
/// constraint is the serial feed (~33% of block wall, 57% of it
/// footprint prediction), which is why `prune_batch` is a knob and not a
/// fix. See the P2b notes in the PR.
pub const DEFAULT_PRUNE_BATCH: usize = 8;

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            workers: 1,
            prune_batch: DEFAULT_PRUNE_BATCH,
        }
    }
}

/// One tx's node in the LIVE dependency DAG — and its REGISTRATION POINT.
///
/// The lock-free trick that removes the global admission lock: a node is
/// "still in flight" exactly while its `children` list is OPEN. Admission
/// registers an edge by pushing into a predecessor's open list; the
/// predecessor's completion CLOSES the list (`None`) and drains it. Both
/// happen under that ONE node's tiny mutex, so "is p outstanding?" and
/// "register my edge on p" are a single atomic step — which is precisely
/// the guarantee a `Weak::upgrade` cannot give on its own (dropping the
/// last strong reference does not order against a concurrent
/// registration, so an edge could be registered onto a list already
/// drained, and its child would then wait forever for a decrement nobody
/// will send).
///
/// Contention is nil: only the single feed thread pushes, and only the
/// one worker that executed p closes. There is no structure any two
/// threads contend on for the whole block.
#[derive(Default)]
struct Node {
    /// `Some` = outstanding, accepting edges. `None` = finished, published.
    children: Mutex<Option<Vec<u32>>>,
    /// Outstanding predecessors. Carries a +1 ADMISSION GUARD while the
    /// feed is still registering this tx's edges, so a predecessor that
    /// finishes mid-admission cannot drive the count to zero early and
    /// dispatch a half-linked tx.
    indegree: AtomicU32,
    /// The thread this tx was assigned. Written before any edge naming it
    /// exists, so whoever dispatches it reads a settled value.
    worker: std::sync::atomic::AtomicUsize,
}

/// Per-block shared context; workers hold an `Arc` for the block's
/// duration.
struct BlockCtx<'env, S: StateDatabase> {
    env: ExecEnv,
    snapshot: &'env S,
    base: PendingDelta,
    sink_start: Option<AccountInfo>,
    sink_start_balance: U256,
    mv: MvCache,
    slots: Vec<std::sync::OnceLock<TxSlot>>,
    results: Vec<std::sync::OnceLock<Result<TxResult, ExecutorError>>>,
    queues: Vec<WorkerQueue>,
    /// Pre-allocated so the array NEVER reallocates: workers index it
    /// concurrently while the feed is still admitting. `MAX_BLOCK_TXS` is
    /// the gas-limit bound, so this is a fixed, provably sufficient size.
    nodes: Vec<Node>,
    /// Admitted (feed-only writer) and finished (workers) counts; the
    /// block is drained when sealed and the two agree.
    admitted: AtomicU32,
    finished: AtomicU32,
    sealed: AtomicBool,
    /// Per-worker completion buffers: a finishing worker parks its index
    /// here (uncontended — it owns the slot) instead of touching the graph
    /// on every tx. A prune drains them all under the graph lock.
    completed: Vec<Mutex<Vec<u32>>>,
    /// Completions parked across all buffers, i.e. DAG updates owed.
    pending: std::sync::atomic::AtomicU64,
    prune_batch: usize,
    done_cv: Condvar,
    aborted: AtomicBool,
    metrics: Metrics,
}

/// LOCK ORDER (the engine's one hard rule): the graph lock may be taken
/// while holding nothing, and a queue lock may be taken while holding
/// nothing or the graph lock's RESULTS — never while the graph lock is
/// HELD. Every dispatch therefore collects its ready set under the graph
/// lock, releases it, and only then pushes. The idle path takes the graph
/// lock only after dropping its queue lock.
impl<S: StateDatabase> BlockCtx<'_, S> {
    /// Hand a READY tx to its assigned thread. Called only with no node
    /// mutex held (lock order: node registration points are leaves).
    fn push_ready(&self, worker: usize, idx: u32) {
        let qh = &self.queues[worker];
        let mut q = qh.q.lock().expect("queue poisoned");
        q.push_back(idx);
        drop(q);
        qh.cv.notify_one();
    }

    /// Apply parked completions to the live DAG: CLOSE each finished
    /// node's registration point, retire the edges that were registered
    /// while it was open, and hand whatever became ready to its thread.
    /// Takes no global lock — only the finished nodes' own mutexes.
    fn prune(&self, forced: bool) -> usize {
        let t0 = std::time::Instant::now();
        let mut applied = 0usize;
        let mut ready: Vec<(usize, u32)> = Vec::new();
        for buf in &self.completed {
            let drained: Vec<u32> = {
                let mut b = buf.lock().expect("completed poisoned");
                if b.is_empty() {
                    continue;
                }
                std::mem::take(&mut *b)
            };
            for job in drained {
                applied += 1;
                // CLOSE: after this, admission can no longer register an
                // edge on `job` — it observes the closed list and skips,
                // which is correct because `job` has already published.
                let kids = {
                    let mut c = self.nodes[job as usize]
                        .children
                        .lock()
                        .expect("children poisoned");
                    c.take().unwrap_or_default()
                };
                for c in kids {
                    if self.nodes[c as usize]
                        .indegree
                        .fetch_sub(1, Ordering::AcqRel)
                        == 1
                    {
                        ready.push((self.nodes[c as usize].worker.load(Ordering::Acquire), c));
                    }
                }
            }
        }
        if applied > 0 {
            self.pending.fetch_sub(applied as u64, Ordering::SeqCst);
            self.finished.fetch_add(applied as u32, Ordering::SeqCst);
        }
        self.metrics
            .prune_ns
            .fetch_add(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
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
            for q in &self.queues {
                q.cv.notify_all();
            }
            self.done_cv.notify_all();
        }
        applied
    }

    fn drained(&self) -> bool {
        self.sealed.load(Ordering::SeqCst)
            && self.finished.load(Ordering::SeqCst) == self.admitted.load(Ordering::SeqCst)
    }
}

struct PoolState<'env, S: StateDatabase> {
    generation: u64,
    ctx: Option<Arc<BlockCtx<'env, S>>>,
    shutdown: bool,
    cfg: PoolConfig,
}

type PoolShared<'env, S> = (Mutex<PoolState<'env, S>>, Condvar);

/// A persistent worker pool bound to one snapshot view for its lifetime —
/// the PIPELINE shape the live executor needs: workers are spawned ONCE
/// (no per-block thread cost); each block is a session whose txs are
/// admitted AS THEY ARRIVE from the sealer stream. Canonical arrival makes
/// the DAG incremental — a tx's predecessors are always already admitted,
/// so execution overlaps the feed, and sealing at the boundary only waits
/// out the tail, validates, and commits. `run_block` is the batch
/// convenience; `begin_block`/`push_tx`/`seal` is the actor-shaped API
/// (P3: the `ReaderToExec::Tx` arm pushes, the `Boundary` arm seals).
pub struct PoolHandle<'a, S: StateDatabase + Sync> {
    shared: &'a PoolShared<'a, S>,
    snapshot: &'a S,
}

/// Spawn `workers` pool threads for the duration of `f`.
pub fn with_pool<S: StateDatabase + Sync, R>(
    snapshot: &S,
    cfg: PoolConfig,
    f: impl FnOnce(&PoolHandle<'_, S>) -> R,
) -> R {
    let workers = cfg.workers.max(1);
    let cfg = PoolConfig {
        workers,
        prune_batch: cfg.prune_batch.max(1),
    };
    let shared: PoolShared<'_, S> = (
        Mutex::new(PoolState {
            generation: 0,
            ctx: None,
            shutdown: false,
            cfg,
        }),
        Condvar::new(),
    );
    let shared_ref = &shared;
    std::thread::scope(|scope| {
        for w in 0..workers {
            scope.spawn(move || worker_loop(shared_ref, w));
        }
        let handle = PoolHandle {
            shared: &shared,
            snapshot,
        };
        let r = f(&handle);
        let mut st = shared_ref.0.lock().expect("pool poisoned");
        st.shutdown = true;
        drop(st);
        shared_ref.1.notify_all();
        r
    })
}

/// One in-flight block being fed to the pool.
pub struct BlockSession<'p, 'a, S: StateDatabase + Sync> {
    pool: &'p PoolHandle<'a, S>,
    ctx: Arc<BlockCtx<'a, S>>,
    exclude: HashSet<Cell>,
    stats: &'p Stats,
    workers: usize,
    cold: usize,
    edges: usize,
    /// Per-DOMAIN last toucher — the index the edges come from. FEED-OWNED
    /// (admission is single-threaded and prune never reads it), so it
    /// lives here rather than in the shared graph: keeping it out of the
    /// critical section removes ~8 hashmap operations per tx from the
    /// lock. Keyed symbolically — no keccak on the hot path.
    last_toucher: FastMap<DomainKey, u32>,
    /// The most recent ⊤ (cold) tx: conflicts with everything, so every
    /// later admission takes an edge from it while it is outstanding.
    last_barrier: Option<u32>,
    dispatch: Vec<u32>,
    /// Cold (⊤) txs still need ORDER, and with no graph there is no
    /// barrier to express it with — so a cold tx marks the wildcard: it
    /// waits for every earlier tx and every later tx waits for it, via
    /// the pending-mark machinery on one shared key.
    /// Kept for the sequential-fallback path (envelope payloads are
    /// refcounted; this is index metadata, not a byte copy).
    txs: Vec<(TxIndex, BPosition, TxEnvelope)>,
    started: std::time::Instant,
}

impl<'a, S: StateDatabase + Sync> PoolHandle<'a, S> {
    /// Open a block session. `base` is the pre-block delta (the actor's
    /// live delta at the block's first tx), owned by the session.
    pub fn begin_block<'p>(
        &'p self,
        base: PendingDelta,
        env: ExecEnv,
        stats: &'p Stats,
    ) -> Result<BlockSession<'p, 'a, S>, ExecutorError> {
        let (workers, prune_batch) = {
            let st = self.shared.0.lock().expect("pool poisoned");
            (st.cfg.workers, st.cfg.prune_batch)
        };
        let probe = BlockInput {
            snapshot: self.snapshot,
            base: Some(&base),
        };
        let sink_start = probe
            .basic_ref(FEE_SINK)
            .map_err(|e| ExecutorError::State(format!("fee-sink read: {e}")))?;
        let sink_start_balance = sink_start.as_ref().map(|a| a.balance).unwrap_or(U256::ZERO);
        let ctx = Arc::new(BlockCtx {
            env,
            snapshot: self.snapshot,
            base,
            sink_start,
            sink_start_balance,
            mv: MvCache::new(),
            slots: (0..MAX_BLOCK_TXS)
                .map(|_| std::sync::OnceLock::new())
                .collect(),
            results: (0..MAX_BLOCK_TXS)
                .map(|_| std::sync::OnceLock::new())
                .collect(),
            queues: (0..workers)
                .map(|_| WorkerQueue {
                    q: Mutex::new(std::collections::VecDeque::new()),
                    cv: Condvar::new(),
                })
                .collect(),
            nodes: (0..MAX_BLOCK_TXS).map(|_| Node::default()).collect(),
            admitted: AtomicU32::new(0),
            finished: AtomicU32::new(0),
            sealed: AtomicBool::new(false),
            completed: (0..workers).map(|_| Mutex::new(Vec::new())).collect(),
            pending: std::sync::atomic::AtomicU64::new(0),
            prune_batch: prune_batch.max(1),
            done_cv: Condvar::new(),
            aborted: AtomicBool::new(false),
            metrics: Metrics::default(),
        });
        {
            let mut st = self.shared.0.lock().expect("pool poisoned");
            if st.ctx.is_some() {
                return Err(ExecutorError::State(
                    "stm pool: a block session is already active".into(),
                ));
            }
            st.ctx = Some(ctx.clone());
            st.generation += 1;
        }
        self.shared.1.notify_all();
        let mut exclude = HashSet::new();
        exclude.insert(Cell::Account(FEE_SINK));
        Ok(BlockSession {
            pool: self,
            ctx,
            exclude,
            stats,
            workers,
            cold: 0,
            edges: 0,
            last_toucher: FastMap::default(),
            last_barrier: None,
            dispatch: vec![0; workers],
            txs: Vec::new(),
            started: std::time::Instant::now(),
        })
    }

    /// Batch convenience: feed the whole block, seal, return the outcome.
    pub fn run_block(
        &self,
        base: PendingDelta,
        env: ExecEnv,
        txs: &[(TxIndex, BPosition, TxEnvelope)],
        stats: &Stats,
    ) -> Result<StmOutcome, ExecutorError> {
        let mut sess = self.begin_block(base, env, stats)?;
        for (t, p, e) in txs {
            sess.push_tx(*t, *p, e.clone())?;
        }
        sess.seal()
    }
}

impl<'p, 'a, S: StateDatabase + Sync> BlockSession<'p, 'a, S> {
    /// Admit the next canonical tx: ONE function computation, then an
    /// assignment to a thread.
    ///
    /// 1. PREDICT the footprint (the P0/P1 classifier) — pure, off-lock.
    /// 2. UPDATE THE LIVE DAG: each predicted cell's last toucher becomes
    ///    a predecessor **if it has not finished yet**; a ⊤ (cold) tx
    ///    takes edges from everything outstanding and becomes the barrier
    ///    every later tx depends on.
    /// 3. ASSIGN a thread by hashing the primary contention domain, and
    ///    dispatch immediately when the indegree is already zero.
    ///
    /// Domain-hashed assignment is what keeps the DAG's chains cheap:
    /// same-domain txs land on the SAME thread in canonical order, so a
    /// chain drains as a FIFO with no cross-thread handoff at all — the
    /// graph only has to carry the cross-domain and multi-domain edges.
    ///
    /// Conflicts the prediction MISSED are not the graph's business: they
    /// are caught at validation and repaired by wounding the later tx (see
    /// [`BlockSession::seal`]) — the wound leg of wound-wait, with the DAG
    /// edge as the wait leg.
    pub fn push_tx(
        &mut self,
        tx_idx: TxIndex,
        position: BPosition,
        envelope: TxEnvelope,
    ) -> Result<(), ExecutorError> {
        let t_feed = std::time::Instant::now();
        let i = self.txs.len();
        if i >= MAX_BLOCK_TXS {
            return Err(ExecutorError::State(format!(
                "stm pool: block exceeds MAX_BLOCK_TXS={MAX_BLOCK_TXS} (gas-limit math says impossible)"
            )));
        }
        let idx = i as u32;
        let t_dec = std::time::Instant::now();
        let decoded = decode_alloy_envelope(&envelope.raw_tx, tx_idx).ok();
        let view = schedule::scheduling_view_decoded(idx, &envelope, decoded.as_ref());
        self.ctx
            .metrics
            .decode_ns
            .fetch_add(t_dec.elapsed().as_nanos() as u64, Ordering::Relaxed);
        let t_pred = std::time::Instant::now();

        // (1) Predict — off-lock. Collect the cells and pick the primary
        // contention domain: the first non-sender cell in the prediction's
        // canonical order (stable across txs of one flow, which is what
        // puts a pool's traffic on one thread), else the sender cell —
        // exactly the SenderChain lane for tier-1-only txs.
        let mut cells: Vec<DomainKey> = Vec::new();
        let mut domain: Option<DomainKey> = None;
        let is_cold = match self.stats.predict_domains(&view) {
            Some(predicted) => {
                for c in predicted {
                    if c == DomainKey::Account(FEE_SINK) {
                        continue;
                    }
                    let is_sender = matches!(c, DomainKey::Account(a) if a == envelope.sender);
                    let domain_is_sender =
                        matches!(domain, Some(DomainKey::Account(a)) if a == envelope.sender);
                    if domain.is_none() || (!is_sender && domain_is_sender) {
                        domain = Some(c);
                    }
                    cells.push(c);
                }
                false
            }
            None => {
                self.cold += 1;
                true
            }
        };
        self.ctx
            .metrics
            .predict_ns
            .fetch_add(t_pred.elapsed().as_nanos() as u64, Ordering::Relaxed);
        let worker = match domain {
            Some(DomainKey::Account(a)) => domain_hash(a.as_slice(), self.workers),
            Some(DomainKey::Fixed(a, k)) => {
                let mut b = [0u8; 8];
                b[..4].copy_from_slice(&a.as_slice()[16..20]);
                b[4..].copy_from_slice(&k.as_slice()[28..32]);
                domain_hash(&b, self.workers)
            }
            Some(DomainKey::Derived {
                contract,
                base,
                outer,
                ..
            }) => {
                // The instance IS the domain: a pool pair, a CLOB market,
                // a user's balance entry. Hash contract+base+key word.
                let mut b = [0u8; 9];
                b[..4].copy_from_slice(&contract.as_slice()[16..20]);
                b[4] = base;
                b[5..].copy_from_slice(&outer.to_be_bytes::<32>()[28..32]);
                domain_hash(&b, self.workers)
            }
            // ⊤ and empty predictions: canonical round-robin.
            None => i % self.workers,
        };

        self.ctx.slots[i]
            .set(TxSlot {
                tx_idx,
                position,
                envelope: envelope.clone(),
                decoded,
            })
            .unwrap_or_else(|_| unreachable!("slot set once per index"));
        self.txs.push((tx_idx, position, envelope));
        self.dispatch[worker] += 1;

        // (2) Update the live DAG + (3) dispatch if ready.
        // Candidate predecessors come from the FEED-OWNED last-toucher
        // index — no lock needed, because admission is single-threaded and
        // prune never reads it.
        let mut preds: Vec<u32> = Vec::with_capacity(cells.len() + 1);
        if let Some(b) = self.last_barrier {
            preds.push(b);
        }
        if is_cold {
            // ⊤: conflicts with everything — every outstanding tx is a
            // candidate predecessor, and this tx becomes the barrier.
            preds.clear();
            preds.extend(0..idx);
            self.last_barrier = Some(idx);
            self.last_toucher.clear();
        } else {
            for c in &cells {
                if let Some(p) = self.last_toucher.insert(*c, idx) {
                    preds.push(p);
                }
            }
            preds.sort_unstable();
            preds.dedup();
        }

        let t_admit = std::time::Instant::now();
        // NO GLOBAL LOCK. Open this node's registration point, seed the
        // admission guard, then register on each predecessor that is still
        // open. The guard (+1) means a predecessor finishing mid-admission
        // can never drive the count to zero and dispatch a half-linked tx;
        // dropping it at the end is what actually releases this tx.
        {
            let node = &self.ctx.nodes[i];
            node.worker.store(worker, Ordering::Release);
            node.indegree.store(1, Ordering::Release);
            *node.children.lock().expect("children poisoned") = Some(Vec::new());
        }
        self.ctx.admitted.fetch_add(1, Ordering::SeqCst);
        let mut deg = 0u32;
        for p in preds {
            if p == idx {
                continue;
            }
            let mut c = self.ctx.nodes[p as usize]
                .children
                .lock()
                .expect("children poisoned");
            if let Some(list) = c.as_mut() {
                // Increment BEFORE publishing the edge: the matching
                // decrement can only happen once this child is visible in
                // p's list, so the add always precedes its own subtract.
                self.ctx.nodes[i].indegree.fetch_add(1, Ordering::AcqRel);
                list.push(idx);
                deg += 1;
            }
            // else: p already finished and published — no edge needed.
        }
        self.edges += deg as usize;
        // Drop the admission guard; if every registered predecessor has
        // already retired, this tx is ours to dispatch.
        let dispatch_now = self.ctx.nodes[i].indegree.fetch_sub(1, Ordering::AcqRel) == 1;
        self.ctx
            .metrics
            .admit_ns
            .fetch_add(t_admit.elapsed().as_nanos() as u64, Ordering::Relaxed);
        if dispatch_now {
            self.ctx.push_ready(worker, idx);
        }
        self.ctx
            .metrics
            .feed_ns
            .fetch_add(t_feed.elapsed().as_nanos() as u64, Ordering::Relaxed);
        Ok(())
    }

    /// The boundary: no more txs. Wait out the in-flight tail, validate
    /// every recorded read, and WOUND (re-execute at its canonical
    /// position, sequentially, against the materialized prefix) any tx a
    /// missed conflict convicted — per-tx, not whole-block. Then commit in
    /// canonical order.
    pub fn seal(self) -> Result<StmOutcome, ExecutorError> {
        let BlockSession {
            pool,
            ctx,
            txs,
            started,
            cold,
            edges,
            dispatch,
            ..
        } = self;
        ctx.sealed.store(true, Ordering::SeqCst);
        for q in &ctx.queues {
            q.cv.notify_all();
        }
        // Apply anything parked, then wait out the tail. Workers force a
        // prune before they park, so this loop only ever waits on
        // execution, never on unapplied updates.
        while !(ctx.aborted.load(Ordering::SeqCst) || ctx.drained()) {
            if ctx.pending.load(Ordering::SeqCst) > 0 {
                ctx.prune(true);
                continue;
            }
            std::thread::yield_now();
        }
        // Release the pool for the next block before the (serial)
        // validate+commit tail.
        {
            let mut st = pool.shared.0.lock().expect("pool poisoned");
            st.ctx = None;
        }
        let t_exec_wall = started.elapsed();

        // Take sole ownership: each worker drops its Arc as it observes
        // the sealed-and-drained condition (microseconds of yield at most).
        let mut ctx_arc = ctx;
        let ctx = loop {
            match Arc::try_unwrap(ctx_arc) {
                Ok(c) => break c,
                Err(back) => {
                    ctx_arc = back;
                    std::thread::yield_now();
                }
            }
        };
        let aborted = ctx.aborted.load(Ordering::SeqCst);
        let n = txs.len();
        let mut tx_results = Vec::with_capacity(n);
        for cell in ctx.results.into_iter().take(n) {
            match cell.into_inner() {
                Some(Ok(r)) => tx_results.push(r),
                Some(Err(e)) => return Err(e),
                None => {
                    return Err(ExecutorError::State(if aborted {
                        "stm pool: block aborted".into()
                    } else {
                        "stm pool: sealed block has unexecuted txs (scheduler bug)".into()
                    }));
                }
            }
        }

        // Validation: every recorded read must still be the highest
        // version below the reader. A conviction is a WOUND — the marks
        // missed a real conflict (the tx read a cell an earlier tx wrote
        // without a mark to park on).
        let t_val = std::time::Instant::now();
        let wounded: Vec<usize> = tx_results
            .iter()
            .enumerate()
            .filter(|(i, r)| r.reads.iter().any(|rec| !ctx.mv.validate(*i as u32, rec)))
            .map(|(i, _)| i)
            .collect();
        let t_validate = t_val.elapsed();
        let wounds = wounded.len();

        // Canonical-order commit. A wounded tx is RE-EXECUTED here against
        // the exact materialized prefix (the delta as of its position), so
        // its result is the sequential one by construction — the whole
        // block never re-runs. Everything after a wound sees the corrected
        // state through the same prefix, so a wound cascade re-executes
        // only the txs it actually reaches.
        let mut receipts = Vec::with_capacity(n);
        let mut delta = PendingDelta::new();
        let mut cumulative = 0u64;
        let mut sink_running = ctx.sink_start_balance;
        let mut wounded_set: HashSet<usize> = wounded.into_iter().collect();
        // A tx after a re-executed one may also be stale: once ANY wound
        // fires, later txs are re-checked against the live prefix by
        // comparing their write set to a replay. Cheapest correct policy:
        // re-execute every tx at or after the first wound.
        if let Some(first) = wounded_set.iter().copied().min() {
            for i in first..n {
                wounded_set.insert(i);
            }
        }
        let mut layered = ctx.base.clone();
        for (i, mut r) in tx_results.into_iter().enumerate() {
            if wounded_set.contains(&i) {
                let (tx_idx, position, envelope) = &txs[i];
                let mut scope = ExecScope::new(ctx.snapshot, Some(&layered), ctx.env)?;
                let (mut receipt, ws) = scope.execute_tx(
                    *tx_idx, *position, envelope, i as u64, cumulative, None, None,
                )?;
                cumulative = receipt.cumulative_gas_used;
                receipt.transaction_index = i as u64;
                layered.apply(ws.clone());
                delta.apply(ws);
                receipts.push(receipt);
                continue;
            }
            cumulative += r.receipt.gas_used;
            r.receipt.cumulative_gas_used = cumulative;
            sink_running += r.fee_delta;
            if let Some(entry) = r.ws.accounts.iter_mut().find(|(a, _)| *a == FEE_SINK) {
                entry.1.1 = sink_running;
                r.receipt.write_set_hash = r.ws.hash();
            }
            layered.apply(r.ws.clone());
            delta.apply(r.ws);
            receipts.push(r.receipt);
        }

        if wounds > 0 {
            tracing::warn!(
                block = ctx.env.block_number,
                wounds,
                rerun = wounded_set.len(),
                "stm: wound — per-tx re-execution at canonical position"
            );
        }
        if std::env::var("KARDAMOM_STM_PHASE_TIMING").is_ok() {
            eprintln!(
                "phase block={} n={} feed+exec={:?} validate={:?} wounds={}",
                ctx.env.block_number, n, t_exec_wall, t_validate, wounds
            );
        }
        let m = &ctx.metrics;
        let prune_calls = m.prune_calls.load(Ordering::Relaxed);
        let completions = m.completions.load(Ordering::Relaxed);
        Ok(StmOutcome {
            receipts,
            delta,
            wounds,
            fallback: wounds > 0,
            cold,
            edges,
            dispatch,
            feed_us: m.feed_ns.load(Ordering::Relaxed) / 1_000,
            decode_us: m.decode_ns.load(Ordering::Relaxed) / 1_000,
            predict_us: m.predict_ns.load(Ordering::Relaxed) / 1_000,
            admit_us: m.admit_ns.load(Ordering::Relaxed) / 1_000,
            prune_us: m.prune_ns.load(Ordering::Relaxed) / 1_000,
            prune_calls,
            prune_forced: m.prune_forced.load(Ordering::Relaxed),
            avg_batch: if prune_calls == 0 {
                0.0
            } else {
                completions as f64 / prune_calls as f64
            },
            idle_us: m.idle_ns.load(Ordering::Relaxed) / 1_000,
        })
    }
}

fn worker_loop<S: StateDatabase + Sync>(shared: &PoolShared<'_, S>, worker: usize) {
    let mut seen = 0u64;
    loop {
        let ctx = {
            let mut st = shared.0.lock().expect("pool poisoned");
            loop {
                if st.shutdown {
                    return;
                }
                if st.generation != seen
                    && let Some(c) = &st.ctx
                {
                    seen = st.generation;
                    break c.clone();
                }
                st = shared.1.wait(st).expect("pool poisoned");
            }
        };
        run_worker_block(&ctx, worker);
        // Arc drops here — seal()'s try_unwrap spin depends on it.
    }
}

/// One worker's participation in one block: ONE EVM for the whole block
/// (per-tx construction was ~90% of execution-path allocation), the view
/// re-aimed per tx. Pops ONLY its own FIFO — same-domain txs were hashed
/// here in canonical order, so a chain drains without any cross-thread
/// handoff; the DAG carries only the cross-domain edges.
///
/// A worker never blocks on a dependency: a tx reaches a queue only once
/// its indegree is zero, i.e. every predicted predecessor has FINISHED and
/// published. Ordering is structural, so there is no wait-graph to
/// deadlock — the canonical total order bounds every edge (low index →
/// high), and completion only ever removes edges.
fn run_worker_block<S: StateDatabase>(ctx: &BlockCtx<'_, S>, worker: usize) {
    let input = BlockInput {
        snapshot: ctx.snapshot,
        base: Some(&ctx.base),
    };
    let view = MvView::new(&ctx.mv, &input, ctx.sink_start.clone());
    let mut evm = Context::mainnet()
        .with_db(view)
        .with_block(ctx.env.block_env())
        .with_cfg(ctx.env.cfg_env())
        .build_mainnet();
    let qh = &ctx.queues[worker];
    loop {
        let job = {
            let mut q = qh.q.lock().expect("queue poisoned");
            loop {
                if ctx.aborted.load(Ordering::SeqCst) {
                    return;
                }
                if let Some(i) = q.pop_front() {
                    break i;
                }
                // Dry. Apply any parked completions MYSELF before parking:
                // this is what makes batching safe — the pool can never sit
                // idle on DAG updates nobody applied. The queue lock is
                // dropped first (lock order: never hold a queue lock while
                // taking the graph lock).
                drop(q);
                if ctx.pending.load(Ordering::SeqCst) > 0 {
                    ctx.prune(true);
                    q = qh.q.lock().expect("queue poisoned");
                    continue;
                }
                if ctx.drained() {
                    return;
                }
                q = qh.q.lock().expect("queue poisoned");
                if !q.is_empty() {
                    continue;
                }
                let t_idle = std::time::Instant::now();
                q = qh.cv.wait(q).expect("queue poisoned");
                ctx.metrics
                    .idle_ns
                    .fetch_add(t_idle.elapsed().as_nanos() as u64, Ordering::Relaxed);
            }
        };
        let slot = ctx.slots[job as usize]
            .get()
            .expect("slot set before its index is dispatched");
        let r = execute_one(
            &mut evm,
            &ctx.mv,
            ctx.env,
            job,
            slot.tx_idx,
            slot.position,
            &slot.envelope,
            slot.decoded.as_ref(),
            ctx.sink_start_balance,
        );
        let errored = r.is_err();
        let _ = ctx.results[job as usize].set(r);
        if errored {
            ctx.aborted.store(true, Ordering::SeqCst);
            for q in &ctx.queues {
                q.cv.notify_all();
            }
            ctx.done_cv.notify_all();
            return;
        }

        // COMPLETION: park the index in this worker's own buffer
        // (uncontended) and only touch the DAG once a batch has
        // accumulated. `prune_batch == 1` is the immediate policy.
        {
            let mut b = ctx.completed[worker].lock().expect("completed poisoned");
            b.push(job);
        }
        let owed = ctx.pending.fetch_add(1, Ordering::SeqCst) + 1;
        if owed as usize >= ctx.prune_batch {
            ctx.prune(false);
        }
    }
}

/// Batch entry point over a transient pool — kept for tests and simple
/// callers; long-lived callers (the A/B harness, the P3 actor) hold a
/// [`with_pool`] scope and amortize the spawn away entirely.
pub fn execute_block_stm<S: StateDatabase + Sync>(
    snapshot: &S,
    base: Option<&PendingDelta>,
    env: ExecEnv,
    txs: &[(TxIndex, BPosition, TxEnvelope)],
    stats: &Stats,
    workers: usize,
) -> Result<StmOutcome, ExecutorError> {
    with_pool(
        snapshot,
        PoolConfig {
            workers,
            ..Default::default()
        },
        |pool| pool.run_block(base.cloned().unwrap_or_default(), env, txs, stats),
    )
}

/// The sequential reference path (also the fallback): `ExecScope` per
/// block, canonical order — the executor's streaming semantics.
pub fn execute_block_sequential<S: StateDatabase + Sync>(
    snapshot: &S,
    base: Option<&PendingDelta>,
    env: ExecEnv,
    txs: &[(TxIndex, BPosition, TxEnvelope)],
) -> Result<(Vec<Receipt>, PendingDelta), ExecutorError> {
    let mut scope = ExecScope::new(snapshot, base, env)?;
    let mut receipts = Vec::with_capacity(txs.len());
    let mut delta = PendingDelta::new();
    let mut cumulative = 0u64;
    for (i, (tx_idx, position, envelope)) in txs.iter().enumerate() {
        let (receipt, ws) = scope.execute_tx(
            *tx_idx, *position, envelope, i as u64, cumulative, None, None,
        )?;
        cumulative = receipt.cumulative_gas_used;
        delta.apply(ws);
        receipts.push(receipt);
    }
    Ok((receipts, delta))
}

/// One worker's EVM over its multi-version view — ExecScope's shape with
/// the concurrent DB swapped in.
type WorkerEvm<'a, S> = revm::handler::MainnetEvm<
    revm::context::Context<
        revm::context::BlockEnv,
        revm::context::TxEnv,
        revm::context::CfgEnv,
        MvView<'a, S>,
    >,
>;

/// Execute one tx against its multi-version view. Mirrors
/// `ExecScope::execute_tx` exactly — #92 skip semantics, write-set
/// emission, receipt shape — with MvCache publish in place of the
/// sequential commit. The worker's EVM is reused across txs; only the
/// view's index and read log are re-aimed.
#[allow(clippy::too_many_arguments)]
fn execute_one<S: StateDatabase>(
    evm: &mut WorkerEvm<'_, S>,
    mv: &MvCache,
    env: ExecEnv,
    local_idx: u32,
    tx_idx: TxIndex,
    position: BPosition,
    envelope: &TxEnvelope,
    decoded: Option<&alloy_consensus::TxEnvelope>,
    sink_start_balance: U256,
) -> Result<TxResult, ExecutorError> {
    let skip = |reason: &str, nonce: u64, to: Option<alloy_primitives::Address>| {
        let (receipt, ws) = invalid_skip(
            reason,
            position,
            envelope,
            nonce,
            to,
            env.block_number,
            local_idx as u64,
            0,
        );
        TxResult {
            receipt,
            ws,
            reads: Vec::new(),
            fee_delta: U256::ZERO,
        }
    };

    let _ = tx_idx;
    let Some(alloy_env) = decoded else {
        return Ok(skip("undecodable raw_tx", 0, None));
    };
    use alloy_consensus::Transaction;
    let signer = envelope.sender;
    let nonce = alloy_env.nonce();
    let to = alloy_env.to();
    let effective_gas_price = alloy_env
        .gas_price()
        .unwrap_or_else(|| alloy_env.max_fee_per_gas());

    {
        // Re-aim the worker's view at this tx.
        let db = revm::context_interface::ContextTr::db_mut(&mut **evm);
        db.idx = local_idx;
        db.reads.clear();
    }
    let tx_env = tx_env_from_alloy(alloy_env, signer);
    let outcome = match evm.transact(tx_env) {
        Ok(o) => o,
        Err(revm::context::result::EVMError::Transaction(reason)) => {
            return Ok(skip(&format!("{reason:?}"), nonce, to));
        }
        Err(revm::context::result::EVMError::Header(reason)) => {
            return Ok(skip(&format!("{reason:?}"), nonce, to));
        }
        Err(e) => {
            return Err(ExecutorError::Execution {
                idx: tx_idx,
                detail: format!("{e:?}"),
            });
        }
    };

    let gas_used = outcome.result.gas().tx_gas_used();
    let (status, logs) = match &outcome.result {
        ExecutionResult::Success { logs, .. } => (ReceiptStatus::Success, logs.clone()),
        ExecutionResult::Revert { .. } => (ReceiptStatus::Revert, Vec::new()),
        ExecutionResult::Halt { reason, .. } => (ReceiptStatus::Halt(reason.clone()), Vec::new()),
    };

    let ws = write_set_from_evm_state(&outcome.state);
    // Publish in the ordered helper's sequence (code+storage, THEN
    // accounts — see `MvCache::publish_write_set`), skipping the fee sink
    // (Accumulator: all workers see block-start; the commit pass
    // materializes the prefixes).
    mv.publish_write_set(local_idx, &ws, FEE_SINK);
    let fee_delta = ws
        .accounts
        .iter()
        .find(|(a, _)| *a == FEE_SINK)
        .map(|(_, (_, b, _))| *b - sink_start_balance)
        .unwrap_or(U256::ZERO);

    let reads = {
        // Take this tx's read log back out of the worker's view.
        let db = revm::context_interface::ContextTr::db_mut(&mut **evm);
        std::mem::take(&mut db.reads)
    };

    let write_set_hash = ws.hash();
    let contract_address = if to.is_none() && status.is_success() {
        Some(signer.create(nonce))
    } else {
        None
    };
    let receipt = Receipt {
        tx_idx: position,
        tx_hash: envelope.tx_hash,
        tx_type: kardamom_types::tx_type_of(&envelope.raw_tx),
        status: status.is_success(),
        gas_used,
        logs: logs.iter().map(wire_log).collect(),
        write_set_hash,
        nonce,
        from: signer,
        to,
        contract_address,
        effective_gas_price,
        block_number: env.block_number,
        transaction_index: local_idx as u64,
        // Canonical prefix sums land in the commit pass.
        cumulative_gas_used: 0,
    };
    Ok(TxResult {
        receipt,
        ws,
        reads,
        fee_delta,
    })
}
