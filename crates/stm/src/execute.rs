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

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};

use alloy_primitives::{B256, U256};
use kardamom_exec_core::block_env::ExecEnv;
use kardamom_exec_core::delta::{PendingDelta, WriteSet};
use kardamom_exec_core::error::ExecutorError;
use kardamom_exec_core::exec_types::{ReceiptStatus, TxIndex};
use kardamom_exec_core::executor::{
    ExecScope, SnapshotRef, decode_alloy_envelope, invalid_skip, tx_env_from_alloy, wire_log,
    write_set_from_evm_state,
};
use kardamom_footprint::classifier::{DomainKey, Stats};
use kardamom_types::{BPosition, Receipt, StateDatabase, TxEnvelope};
use revm::context::result::ExecutionResult;
use revm::database::DatabaseRef;
use revm::state::AccountInfo;
use revm::{Context, ExecuteEvm, MainBuilder, MainContext};

use crate::mv::{MvCache, ReadRecord};
use crate::schedule;
use crate::{FEE_SINK, FastMap};

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

/// SHARED read-through cache over the block-input layer.
///
/// The block input (pre-block delta ∘ snapshot) is immutable for the whole
/// block, so every worker that misses the multi-version cache asks the
/// same questions and gets the same answers. Per-worker memos made each
/// thread re-answer them independently, which is why total CPU time GREW
/// with worker count — 159ms at one worker, 325ms at eight, for the same
/// 8000 transactions. Sharing the answers is what turns extra threads into
/// extra throughput instead of extra work.
///
/// Sharded like [`MvCache`], read-mostly, and correctness-neutral: it
/// caches an immutable layer, so a stale entry is impossible.
#[derive(Default)]
struct BaseCache {
    accounts: Vec<RwLock<FastMap<alloy_primitives::Address, Option<AccountInfo>>>>,
    storage: Vec<RwLock<FastMap<(alloy_primitives::Address, B256), U256>>>,
    code: RwLock<FastMap<B256, revm::state::Bytecode>>,
}

const BASE_SHARDS: usize = 64;

impl BaseCache {
    fn new() -> Self {
        Self {
            accounts: (0..BASE_SHARDS)
                .map(|_| RwLock::new(FastMap::with_hasher(crate::FnvBuild)))
                .collect(),
            storage: (0..BASE_SHARDS)
                .map(|_| RwLock::new(FastMap::with_hasher(crate::FnvBuild)))
                .collect(),
            code: RwLock::new(FastMap::with_hasher(crate::FnvBuild)),
        }
    }

    fn shard(addr: &alloy_primitives::Address) -> usize {
        let b = addr.as_slice();
        (b[19] as usize) % BASE_SHARDS
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
    /// SHARED across workers — see [`BaseCache`].
    base_cache: &'a BaseCache,
    metrics: &'a Metrics,
    /// Read counters accumulated WITHOUT atomics and flushed once per
    /// block. Incrementing shared atomics per read had every worker
    /// hammering the same cache lines — instrumentation distorting the
    /// very contention it was measuring.
    n_reads: u64,
    n_mv_hit: u64,
    n_base_hit: u64,
    n_backend: u64,
}

impl<'a, S: StateDatabase> MvView<'a, S> {
    fn new(
        mv: &'a MvCache,
        base: &'a BlockInput<'a, S>,
        sink_start: Option<AccountInfo>,
        base_cache: &'a BaseCache,
        metrics: &'a Metrics,
    ) -> Self {
        Self {
            mv,
            base,
            idx: 0,
            reads: Vec::new(),
            sink_start,
            base_cache,
            metrics,
            n_reads: 0,
            n_mv_hit: 0,
            n_base_hit: 0,
            n_backend: 0,
        }
    }

    /// Fold this worker's counters into the shared metrics — once per
    /// block, not once per read.
    fn flush_counters(&mut self) {
        self.metrics
            .reads_total
            .fetch_add(self.n_reads, Ordering::Relaxed);
        self.metrics
            .reads_mv_hit
            .fetch_add(self.n_mv_hit, Ordering::Relaxed);
        self.metrics
            .reads_base_hit
            .fetch_add(self.n_base_hit, Ordering::Relaxed);
        self.metrics
            .reads_backend
            .fetch_add(self.n_backend, Ordering::Relaxed);
        self.n_reads = 0;
        self.n_mv_hit = 0;
        self.n_base_hit = 0;
        self.n_backend = 0;
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
        self.n_reads += 1;
        if let Some((ver, a)) = self.mv.read_account(self.idx, &address) {
            self.n_mv_hit += 1;
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
        let sh = BaseCache::shard(&address);
        if let Some(a) = self.base_cache.accounts[sh]
            .read()
            .expect("base cache poisoned")
            .get(&address)
        {
            self.n_base_hit += 1;
            return Ok(a.clone());
        }
        self.n_backend += 1;
        let a = self.base.basic_ref(address)?;
        self.base_cache.accounts[sh]
            .write()
            .expect("base cache poisoned")
            .insert(address, a.clone());
        Ok(a)
    }

    fn code_by_hash(&mut self, code_hash: B256) -> Result<revm::state::Bytecode, Self::Error> {
        // Content-addressed: no version, no record — memo both sources
        // (Bytecode clones are refcounted; the copy happens once).
        if let Some(c) = self
            .base_cache
            .code
            .read()
            .expect("base cache poisoned")
            .get(&code_hash)
        {
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
        self.base_cache
            .code
            .write()
            .expect("base cache poisoned")
            .insert(code_hash, c.clone());
        Ok(c)
    }

    fn storage(
        &mut self,
        address: alloy_primitives::Address,
        index: U256,
    ) -> Result<U256, Self::Error> {
        let key = B256::from(index.to_be_bytes::<32>());
        self.n_reads += 1;
        if let Some((ver, v)) = self.mv.read_slot(self.idx, &address, &key) {
            self.n_mv_hit += 1;
            self.reads.push(ReadRecord::Slot(address, key, Some(ver)));
            return Ok(v);
        }
        self.reads.push(ReadRecord::Slot(address, key, None));
        let sh = BaseCache::shard(&address);
        if let Some(v) = self.base_cache.storage[sh]
            .read()
            .expect("base cache poisoned")
            .get(&(address, key))
        {
            self.n_base_hit += 1;
            return Ok(*v);
        }
        self.n_backend += 1;
        let v = self.base.storage_ref(address, index)?;
        self.base_cache.storage[sh]
            .write()
            .expect("base cache poisoned")
            .insert((address, key), v);
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
    /// The write set contains the fee sink, so its hash is finalized at
    /// COMMIT (after the prefix balance is materialized) and computing it
    /// during execution would be thrown away. P0 measured this at 100% of
    /// txs, so the saved keccak is not an edge case.
    sink_touched: bool,
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
    /// Nodes observed leaving the graph more than once. ALWAYS ZERO —
    /// asserted by the test suite and worth an alert in production: a
    /// non-zero value means a tx completed twice and the edges registered
    /// in between were stranded.
    pub double_exit: u32,
    /// Scheduler cost, measured (the numbers the prune-batch knob is
    /// tuned on): time held in the graph lock split by cause, prune
    /// invocations and how many were starvation-forced, the realized
    /// batch size, and worker idle time.
    pub feed_us: u64,
    pub redundant_edges: u64,
    pub steals: u64,
    pub reads_total: u64,
    pub reads_mv_hit: u64,
    pub reads_base_hit: u64,
    pub reads_backend: u64,
    pub evm_us: u64,
    pub publish_us: u64,
    /// Where the block's wall time went. `busy_us / (workers *
    /// parallel_span_us)` is the honest core utilization; `ramp_us` and
    /// `commit_us` are the serial head and tail no worker count reduces.
    pub busy_us: u64,
    pub parallel_span_us: u64,
    pub ramp_us: u64,
    pub commit_us: u64,
    pub commit_hash_us: u64,
    pub commit_delta_us: u64,
    pub decode_us: u64,
    pub predict_us: u64,
    pub admit_us: u64,
    pub prune_us: u64,
    pub prune_calls: u64,
    pub prune_forced: u64,
    pub avg_batch: f64,
    pub idle_us: u64,
}

/// Stable domain → worker mapping. Quality only affects BALANCE across
/// threads, never correctness: ordering comes from the DAG's edges, and
/// an idle worker may steal from any queue.
fn domain_hash(bytes: &[u8], workers: usize) -> usize {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in bytes {
        h ^= *b as u64;
        h = h.wrapping_mul(0x100_0000_01b3);
    }
    (h % workers as u64) as usize
}

/// How long `seal` waits for a block to drain before declaring a
/// scheduler bug. Generous by orders of magnitude: a 30M-gas block is
/// milliseconds of execution, so anything past this is a stranded edge,
/// not slow work.
const STALL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Everything about a tx that can be derived WITHOUT touching the graph:
/// the RLP decode and the footprint prediction. Both are pure functions of
/// the envelope bytes and the stats snapshot, so they do not belong on the
/// executor's single feed thread — the pipeline computes them upstream,
/// where the work is already sharded.
///
/// In the live executor (P3) this is produced by the M tx_data reader
/// threads, which touch every envelope anyway and run BEFORE the canonical
/// order arrives (the join buffer exists precisely because tx_data leads
/// tx_ordering), so the work lands in slack that already exists.
pub struct Prepared {
    /// `None` when the envelope does not decode — the #92 skip path.
    pub decoded: Option<alloy_consensus::TxEnvelope>,
    /// Predicted contention domains, fee sink already excluded.
    pub domains: Vec<DomainKey>,
    /// The domain that decides which thread runs this tx.
    pub primary: Option<DomainKey>,
    /// ⊤: untrained selector — orders behind everything outstanding.
    pub cold: bool,
}

/// Decode + predict, off the feed thread. `stats` must be a snapshot
/// trained on PRIOR blocks only (in the live executor: an `Arc<Stats>`
/// swapped at each boundary, so readers never take a lock).
///
/// Getting this wrong is not a correctness event: a bad prediction costs a
/// mis-schedule, which surfaces as a wound and re-executes that one tx at
/// its canonical position. That is what makes it safe to compute here,
/// concurrently, ahead of canonical order.
pub fn prepare(envelope: &TxEnvelope, tx_idx: TxIndex, stats: &Stats) -> Prepared {
    let decoded = decode_alloy_envelope(&envelope.raw_tx, tx_idx).ok();
    // The local index is irrelevant to prediction (it only labels the
    // observation), so preparation needs no position in the block.
    let view = schedule::scheduling_view_decoded(0, envelope, decoded.as_ref());
    let mut domains: Vec<DomainKey> = Vec::new();
    let mut primary: Option<DomainKey> = None;
    let cold = match stats.predict_domains(&view) {
        Some(predicted) => {
            for c in predicted {
                if c == DomainKey::Account(FEE_SINK) {
                    continue;
                }
                // The primary contention domain is the first non-sender
                // cell in canonical order (stable across txs of one flow,
                // which is what puts a pool's traffic on one thread),
                // falling back to the sender cell — the SenderChain lane
                // for tier-1-only txs.
                let is_sender = matches!(c, DomainKey::Account(a) if a == envelope.sender);
                let primary_is_sender =
                    matches!(primary, Some(DomainKey::Account(a)) if a == envelope.sender);
                if primary.is_none() || (!is_sender && primary_is_sender) {
                    primary = Some(c);
                }
                domains.push(c);
            }
            false
        }
        None => true,
    };
    Prepared {
        decoded,
        domains,
        primary,
        cold,
    }
}

/// Spins before a dry worker parks. Sized so the spin costs far less than
/// the park/unpark syscall pair it avoids, while still yielding promptly
/// when a block really is drained.
/// MEASURED: yielding partway through the spin was tried and REVERTED —
/// it helped only the oversubscribed case (12 workers on 12 cores) and
/// cost 20-35% everywhere else. Do not run more workers than cores minus
/// the feed thread; that is the real fix for oversubscription.
const SPIN_BEFORE_PARK: u32 = 256;

/// Longest a parked worker sleeps before re-checking its queue and the
/// drain condition itself. Bounds the damage of a missed wake to one
/// poll interval instead of a permanent hang.
const PARK_POLL: std::time::Duration = std::time::Duration::from_micros(200);

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
    /// Whether this worker is PARKED on `cv`. Waking a thread that is
    /// already running costs a futex syscall for nothing, and dispatch
    /// happens once per transaction — on a 21k-gas transfer that is ~2.7us
    /// of real work, so a wasted wake is a large fraction of the budget.
    parked: AtomicBool,
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
    /// Ready txs taken from another thread's queue to fix imbalance.
    pub steals: std::sync::atomic::AtomicU64,
    /// Read-path breakdown. The multi-version path costs 1.7x sequential
    /// single-threaded, so which lookup dominates decides what to fix.
    pub reads_total: std::sync::atomic::AtomicU64,
    /// Reads served by a version written earlier in THIS block.
    pub reads_mv_hit: std::sync::atomic::AtomicU64,
    /// Reads that fell through to the shared base cache, and of those, the
    /// ones that had to touch the backing store.
    pub reads_base_hit: std::sync::atomic::AtomicU64,
    pub reads_backend: std::sync::atomic::AtomicU64,
    /// Split of a worker's per-tx time: inside revm (`transact`, which
    /// includes the read path) vs publishing the write set into the
    /// multi-version cache. Guessing which dominates has been wrong twice.
    pub evm_ns: std::sync::atomic::AtomicU64,
    pub publish_ns: std::sync::atomic::AtomicU64,
    /// Nanoseconds workers spent INSIDE revm (the only work that is
    /// actually the point). `busy / (workers x parallel_span)` is the true
    /// core utilization — idle time alone cannot distinguish "the DAG had
    /// no work to give" from "work existed and nobody picked it up".
    pub busy_ns: std::sync::atomic::AtomicU64,
    /// Wall from the block's FIRST dispatch to its LAST completion — the
    /// span during which parallelism was even possible.
    pub parallel_span_ns: std::sync::atomic::AtomicU64,
    /// Wall before the first dispatch (feed ramp) and after the last
    /// completion (serial validate + commit tail). Both are per-block
    /// costs that no worker count can reduce.
    pub ramp_ns: std::sync::atomic::AtomicU64,
    pub commit_ns: std::sync::atomic::AtomicU64,
    /// Nanos of the first dispatch / last completion, as offsets from the
    /// session start (interior mutability so workers can stamp them).
    first_dispatch_ns: std::sync::atomic::AtomicU64,
    last_done_ns: std::sync::atomic::AtomicU64,
    /// Envelope decode and footprint prediction, the two pure-computation
    /// parts of admission (the rest is the graph lock + dispatch).
    pub decode_ns: std::sync::atomic::AtomicU64,
    pub predict_ns: std::sync::atomic::AtomicU64,
    /// Edges whose predecessor was assigned to the SAME thread AND was
    /// already dispatched — the FIFO queue already orders those, so the
    /// edge enforces nothing. Measured to decide whether eliding them is
    /// worth the subtlety (the elision is only sound for predecessors
    /// ALREADY dispatched: one still waiting could be queued after its
    /// own child).
    pub redundant_edges: std::sync::atomic::AtomicU64,
    /// Commit-tail breakdown. The tail is SERIAL and flat in worker count
    /// (~9.6ms of a 22ms transfers block), so whatever dominates it is a
    /// fixed parallelization tax — the thing that caps speedup no matter
    /// how many cores are available.
    pub commit_hash_ns: std::sync::atomic::AtomicU64,
    pub commit_delta_ns: std::sync::atomic::AtomicU64,
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
    /// True while this tx is outstanding and accepting edges. Flipped
    /// under `children`'s lock, which is what makes "is p outstanding?"
    /// and "register my edge on p" one atomic step.
    open: AtomicBool,
    /// Children registered while open. Drained IN PLACE at close so the
    /// buffer keeps its capacity for the next block that reuses this
    /// arena slot — steady-state allocation is zero.
    children: Mutex<Vec<u32>>,
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
struct BlockCtx<S: StateDatabase> {
    env: ExecEnv,
    /// One state view PER WORKER.
    ///
    /// This is not an optimization, it is a requirement of the backend:
    /// mdbx's synchronized read transaction guards its pointer with a
    /// mutex ("serialises access to the transaction pointer"), so workers
    /// sharing ONE snapshot funnel every state read through one lock —
    /// measured as parallel execution getting SLOWER with more workers
    /// (0.86x at 4, 0.78x at 8) while the in-memory backend scaled. Each
    /// worker therefore reads through its own transaction, all opened at
    /// the same committed block, so the view is identical.
    snapshots: Vec<S>,
    base: PendingDelta,
    sink_start: Option<AccountInfo>,
    sink_start_balance: U256,
    mv: MvCache,
    /// Shared read-through cache over the immutable block-input layer.
    base_cache: BaseCache,
    slots: Vec<std::sync::OnceLock<TxSlot>>,
    results: Vec<std::sync::OnceLock<Result<TxResult, ExecutorError>>>,
    queues: Vec<WorkerQueue>,
    /// The pool's arena (see [`PoolHandle::arena`]) — shared, never
    /// reallocated, indexed concurrently while the feed is still
    /// admitting.
    nodes: Arc<Vec<Node>>,
    /// Admitted (feed-only writer) and finished (workers) counts; the
    /// block is drained when sealed and the two agree.
    admitted: AtomicU32,
    finished: AtomicU32,
    sealed: AtomicBool,
    /// Per-worker completion buffers: a finishing worker parks its index
    /// here (uncontended — it owns the slot) instead of retiring edges on
    /// every tx; a prune drains them.
    completed: Vec<Mutex<Vec<u32>>>,
    /// Length of each buffer, readable WITHOUT taking its mutex. A prune
    /// otherwise locks every worker's buffer just to find it empty, and
    /// spinning workers force-prune often — measured at ~2us/tx on
    /// micro-gas workloads, the largest single overhead there.
    completed_len: Vec<AtomicU32>,
    /// Completions parked across all buffers, i.e. DAG updates owed.
    pending: std::sync::atomic::AtomicU64,
    prune_batch: usize,
    /// Block start, so workers can stamp first-dispatch / last-completion
    /// offsets without reaching into the session.
    started: std::time::Instant,
    done_cv: Condvar,
    aborted: AtomicBool,
    /// Nodes observed leaving the graph more than once — always zero; a
    /// non-zero value is a scheduler bug surfaced at seal rather than a
    /// silently stranded edge.
    double_exit: AtomicU32,
    metrics: Metrics,
}

/// LOCK ORDER (the engine's one hard rule): the graph lock may be taken
/// while holding nothing, and a queue lock may be taken while holding
/// nothing or the graph lock's RESULTS — never while the graph lock is
/// HELD. Every dispatch therefore collects its ready set under the graph
/// lock, releases it, and only then pushes. The idle path takes the graph
/// lock only after dropping its queue lock.
impl<S: StateDatabase> BlockCtx<S> {
    /// Steal one ready tx from the longest other queue.
    ///
    /// SAFE BY CONSTRUCTION, and worth spelling out: a tx only ever
    /// reaches a queue once its indegree hit zero, i.e. every predicted
    /// predecessor has finished and published — including its
    /// same-domain ones, which get real edges like any other. So queue
    /// position carries NO ordering obligation; the domain-hashed
    /// assignment is a locality hint, not a correctness mechanism, and
    /// any thread may run any ready tx.
    ///
    /// Taken from the BACK, leaving the owner its front: the owner's
    /// front is the oldest (most likely to have warm state), and the two
    /// ends rarely contend.
    fn steal(&self, thief: usize) -> Option<u32> {
        let mut best: Option<(usize, usize)> = None;
        for (w, qh) in self.queues.iter().enumerate() {
            if w == thief {
                continue;
            }
            let len = qh.q.lock().expect("queue poisoned").len();
            if len > 1 && best.is_none_or(|(_, b)| len > b) {
                best = Some((w, len));
            }
        }
        let (victim, _) = best?;
        let mut q = self.queues[victim].q.lock().expect("queue poisoned");
        q.pop_back()
    }

    /// Hand a READY tx to its assigned thread. Called only with no node
    /// mutex held (lock order: node registration points are leaves).
    fn push_ready(&self, worker: usize, idx: u32) {
        let qh = &self.queues[worker];
        let mut q = qh.q.lock().expect("queue poisoned");
        q.push_back(idx);
        drop(q);
        // Only wake a worker that actually parked. Under load the queue is
        // rarely empty, so this elides nearly every syscall.
        if qh.parked.load(Ordering::Acquire) {
            qh.cv.notify_one();
        }
    }

    /// Apply parked completions to the live DAG: CLOSE each finished
    /// node's registration point, retire the edges that were registered
    /// while it was open, and hand whatever became ready to its thread.
    /// Takes no global lock — only the finished nodes' own mutexes.
    fn prune(&self, forced: bool) -> usize {
        let t0 = std::time::Instant::now();
        let mut applied = 0usize;
        let mut ready: Vec<(usize, u32)> = Vec::new();
        for (w, buf) in self.completed.iter().enumerate() {
            // Skip untouched buffers without paying for their mutex.
            if self.completed_len[w].load(Ordering::Acquire) == 0 {
                continue;
            }
            let drained: Vec<u32> = {
                let mut b = buf.lock().expect("completed poisoned");
                if b.is_empty() {
                    continue;
                }
                self.completed_len[w].fetch_sub(b.len() as u32, Ordering::AcqRel);
                std::mem::take(&mut *b)
            };
            for job in drained {
                applied += 1;
                // LEAVE ONCE. Closing IS the node's exit from the graph;
                // a second close would strand every edge registered in
                // between, so it is asserted rather than assumed. The
                // list is drained IN PLACE so its capacity survives for
                // the next block that reuses this arena slot.
                let node = &self.nodes[job as usize];
                let mut list = node.children.lock().expect("children poisoned");
                if !node.open.swap(false, Ordering::AcqRel) {
                    debug_assert!(
                        false,
                        "stm: tx left the graph twice (completion must happen exactly once)"
                    );
                    tracing::error!(
                        tx = job,
                        "stm: tx left the graph twice — scheduler invariant violated"
                    );
                    self.double_exit.fetch_add(1, Ordering::SeqCst);
                }
                for c in list.iter() {
                    let child = &self.nodes[*c as usize];
                    if child.indegree.fetch_sub(1, Ordering::AcqRel) == 1 {
                        ready.push((child.worker.load(Ordering::Acquire), *c));
                    }
                }
                list.clear();
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

struct PoolState<S: StateDatabase> {
    generation: u64,
    ctx: Option<Arc<BlockCtx<S>>>,
    shutdown: bool,
    cfg: PoolConfig,
}

type PoolShared<S> = (Mutex<PoolState<S>>, Condvar);

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
    shared: &'a PoolShared<S>,
    /// THE ARENA. Allocated once for the pool's lifetime and reused by
    /// every block: a node is addressed by its index, so nothing here is
    /// ever allocated, freed, or reference-counted per transaction. This
    /// is the whole reason the graph is not an `Arc` graph — `Arc` gives
    /// the same semantics but demands one heap allocation per node plus
    /// refcount traffic on every clone, which measured 4.3ms -> 38.9ms of
    /// release cost per 8000 txs. Here a "weak reference" is the index,
    /// and liveness is the node's own state (`children == None` means it
    /// already left the graph).
    ///
    /// Sized by the gas limit (`MAX_BLOCK_TXS`), so it cannot overflow;
    /// `reset` clears only the prefix a block actually used, and the
    /// children vectors KEEP THEIR CAPACITY, so steady-state allocation
    /// across blocks is zero.
    arena: Arc<Vec<Node>>,
}

/// Spawn `workers` pool threads for the duration of `f`.
pub fn with_pool<S: StateDatabase + Sync + 'static, R>(
    cfg: PoolConfig,
    f: impl FnOnce(&PoolHandle<'_, S>) -> R,
) -> R {
    let workers = cfg.workers.max(1);
    let cfg = PoolConfig {
        workers,
        prune_batch: cfg.prune_batch.max(1),
    };
    let shared: PoolShared<S> = (
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
            shared: shared_ref,
            arena: Arc::new((0..MAX_BLOCK_TXS).map(|_| Node::default()).collect()),
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
    ctx: Arc<BlockCtx<S>>,
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
        snapshot: S,
        base: PendingDelta,
        env: ExecEnv,
        stats: &'p Stats,
    ) -> Result<BlockSession<'p, 'a, S>, ExecutorError>
    where
        S: Clone,
    {
        let workers = {
            let st = self.shared.0.lock().expect("pool poisoned");
            st.cfg.workers.max(1)
        };
        // Cloning shares one transaction; backends that serialise reads
        // need `begin_block_per_worker` with independent views.
        self.begin_block_per_worker(vec![snapshot; workers], base, env, stats)
    }

    /// [`Self::begin_block`] with an INDEPENDENT state view per worker —
    /// see [`BlockCtx::snapshots`] for why the backend can require it.
    pub fn begin_block_per_worker<'p>(
        &'p self,
        snapshots: Vec<S>,
        base: PendingDelta,
        env: ExecEnv,
        stats: &'p Stats,
    ) -> Result<BlockSession<'p, 'a, S>, ExecutorError> {
        let (workers, prune_batch) = {
            let st = self.shared.0.lock().expect("pool poisoned");
            (st.cfg.workers, st.cfg.prune_batch)
        };
        assert!(
            snapshots.len() >= workers.max(1),
            "one state view per worker: {} given, {workers} needed",
            snapshots.len()
        );
        let probe = BlockInput {
            snapshot: snapshots.first().expect("at least one snapshot"),
            base: Some(&base),
        };
        let sink_start = probe
            .basic_ref(FEE_SINK)
            .map_err(|e| ExecutorError::State(format!("fee-sink read: {e}")))?;
        let sink_start_balance = sink_start.as_ref().map(|a| a.balance).unwrap_or(U256::ZERO);
        let ctx = Arc::new(BlockCtx {
            env,
            snapshots,
            base,
            sink_start,
            sink_start_balance,
            mv: MvCache::new(),
            base_cache: BaseCache::new(),
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
                    parked: AtomicBool::new(false),
                })
                .collect(),
            nodes: self.arena.clone(),
            admitted: AtomicU32::new(0),
            finished: AtomicU32::new(0),
            sealed: AtomicBool::new(false),
            completed: (0..workers).map(|_| Mutex::new(Vec::new())).collect(),
            completed_len: (0..workers).map(|_| AtomicU32::new(0)).collect(),
            pending: std::sync::atomic::AtomicU64::new(0),
            prune_batch: prune_batch.max(1),
            started: std::time::Instant::now(),
            done_cv: Condvar::new(),
            aborted: AtomicBool::new(false),
            double_exit: AtomicU32::new(0),
            metrics: Metrics {
                // fetch_min seeds from the top.
                first_dispatch_ns: std::sync::atomic::AtomicU64::new(u64::MAX),
                ..Default::default()
            },
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
        Ok(BlockSession {
            pool: self,
            ctx,
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

    /// Feed a block whose txs were PREPARED upstream (decode + predict
    /// already done, off this thread) — the pipelined shape P3's tx_data
    /// readers will use.
    pub fn run_block_prepared(
        &self,
        snapshots: Vec<S>,
        base: PendingDelta,
        env: ExecEnv,
        txs: &[(TxIndex, BPosition, TxEnvelope)],
        prepared: Vec<Prepared>,
        stats: &Stats,
    ) -> Result<StmOutcome, ExecutorError> {
        debug_assert_eq!(txs.len(), prepared.len(), "one Prepared per tx");
        let mut sess = self.begin_block_per_worker(snapshots, base, env, stats)?;
        for ((t, p, e), prep) in txs.iter().zip(prepared) {
            sess.push_prepared(*t, *p, e.clone(), prep)?;
        }
        sess.seal()
    }

    /// Batch convenience: feed the whole block, seal, return the outcome.
    pub fn run_block(
        &self,
        snapshots: Vec<S>,
        base: PendingDelta,
        env: ExecEnv,
        txs: &[(TxIndex, BPosition, TxEnvelope)],
        stats: &Stats,
    ) -> Result<StmOutcome, ExecutorError> {
        let mut sess = self.begin_block_per_worker(snapshots, base, env, stats)?;
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
        // Convenience path: prepare inline. The pipelined caller (P3's
        // tx_data readers) calls `prepare` upstream and `push_prepared`
        // here, keeping decode+predict off this serial thread entirely.
        let t_prep = std::time::Instant::now();
        let prep = prepare(&envelope, tx_idx, self.stats);
        let dt = t_prep.elapsed().as_nanos() as u64;
        self.ctx.metrics.decode_ns.fetch_add(dt, Ordering::Relaxed);
        self.push_prepared(tx_idx, position, envelope, prep)
    }

    /// Admit a tx whose decode and prediction were computed UPSTREAM (see
    /// [`prepare`]). This is the executor's real hot path: everything left
    /// here is graph work, which must stay serial and in canonical order
    /// because an edge is "the previous tx that touched this domain".
    pub fn push_prepared(
        &mut self,
        tx_idx: TxIndex,
        position: BPosition,
        envelope: TxEnvelope,
        prep: Prepared,
    ) -> Result<(), ExecutorError> {
        let _ = tx_idx;
        let t_feed = std::time::Instant::now();
        let i = self.txs.len();
        if i >= MAX_BLOCK_TXS {
            return Err(ExecutorError::State(format!(
                "stm pool: block exceeds MAX_BLOCK_TXS={MAX_BLOCK_TXS} (gas-limit math says impossible)"
            )));
        }
        let idx = i as u32;
        let Prepared {
            decoded,
            domains: cells,
            primary: domain,
            cold: is_cold,
        } = prep;
        if is_cold {
            self.cold += 1;
        }

        // Domain -> worker by HASH, deliberately. Round-robin on first
        // sight was tried and REVERTED: it spread domains exactly (the
        // busiest thread's share fell 1.96x -> 1.50x) yet cost 9% wall,
        // because hashing is STABLE ACROSS BLOCKS — a pool returns to the
        // same worker every block, keeping its state warm in that core's
        // caches — while first-seen ordering reshuffles the assignment
        // each block. Locality beat balance.
        let worker = match domain {
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
            // REGISTER ONCE. Unreachable through the public API — the
            // local index comes from this session's own counter, so no
            // caller can name an occupied slot — but asserted anyway,
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
            node.open.store(true, Ordering::Release);
        }
        self.ctx.admitted.fetch_add(1, Ordering::SeqCst);
        let mut deg = 0u32;
        for p in preds {
            if p == idx {
                continue;
            }
            let pn = &self.ctx.nodes[p as usize];
            let mut list = pn.children.lock().expect("children poisoned");
            if pn.open.load(Ordering::Acquire) {
                // Increment BEFORE publishing the edge: the matching
                // decrement can only happen once this child is visible in
                // p's list, so the add always precedes its own subtract.
                self.ctx.nodes[i].indegree.fetch_add(1, Ordering::AcqRel);
                list.push(idx);
                deg += 1;
                if pn.worker.load(Ordering::Acquire) == worker
                    && pn.indegree.load(Ordering::Acquire) == 0
                {
                    self.ctx
                        .metrics
                        .redundant_edges
                        .fetch_add(1, Ordering::Relaxed);
                }
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
        // WATCHDOG. Every admitted tx must leave the graph exactly once,
        // so this loop terminates — unless a scheduler bug strands an
        // edge (a decrement that never arrives), in which case the naive
        // version would spin forever and the executor would be frozen but
        // alive: metrics up, chain stopped, no diagnosis. The engine
        // instead fail-stops with the graph's state, which crash-recovery
        // replays cleanly.
        let deadline = std::time::Instant::now() + STALL_TIMEOUT;
        while !(ctx.aborted.load(Ordering::SeqCst) || ctx.drained()) {
            if ctx.pending.load(Ordering::SeqCst) > 0 {
                ctx.prune(true);
                continue;
            }
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
                return Err(ExecutorError::State(format!(
                    "stm: block {} failed to drain after {:?}: admitted={admitted} \
                     finished={finished} stuck(idx,indegree)={stuck:?}",
                    ctx.env.block_number, STALL_TIMEOUT
                )));
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
        //
        // SERIAL by definition: this is the block's tail, and no worker
        // count shortens it.
        let t_com = std::time::Instant::now();
        let (mut hash_ns, mut delta_ns) = (0u64, 0u64);
        let mut receipts = Vec::with_capacity(n);
        let mut delta = PendingDelta::new();
        let mut cumulative = 0u64;
        let mut sink_running = ctx.sink_start_balance;
        let mut wounded_set: HashSet<usize> = wounded.into_iter().collect();
        // A tx after a re-executed one may also be stale: once ANY wound
        // fires, later txs are re-checked against the live prefix.
        if let Some(first) = wounded_set.iter().copied().min() {
            for i in first..n {
                wounded_set.insert(i);
            }
        }
        let repairing = !wounded_set.is_empty();

        if !repairing {
            // FAST PATH — the one that runs essentially always.
            //
            // The write-set hash is ~1.25us of keccak per tx and CANNOT be
            // made cheaper (it is one permutation per 136 bytes of a
            // contract the receipts depend on). On the serial commit tail
            // it was 72% of that tail — the largest fixed parallelization
            // tax in the engine, untouched by worker count.
            //
            // It does not have to be serial. The only thing forcing it
            // there is the accumulator's absolute balance, and THAT is a
            // prefix sum: computable in one cheap pass with no hashing, so
            // afterwards every tx's hash is independent.
            //
            // Phase 1 (serial, ~ns/tx): cumulative gas + accumulator
            // materialization.
            for r in tx_results.iter_mut() {
                cumulative += r.receipt.gas_used;
                r.receipt.cumulative_gas_used = cumulative;
                sink_running += r.fee_delta;
                if r.sink_touched
                    && let Some(entry) = r.ws.accounts.iter_mut().find(|(a, _)| *a == FEE_SINK)
                {
                    entry.1.1 = sink_running;
                }
            }
            // Phase 2 (parallel): hash. Scoped threads rather than the
            // worker pool — this is a self-contained parallel map over an
            // owned slice, and keeping it out of the pool's state machine
            // is worth one spawn (~tens of us) against the milliseconds it
            // removes from the tail.
            let t_h = std::time::Instant::now();
            let threads = ctx.queues.len().min(n.max(1));
            if threads > 1 && n > 64 {
                let chunk = n.div_ceil(threads);
                std::thread::scope(|sc| {
                    for part in tx_results.chunks_mut(chunk) {
                        sc.spawn(move || {
                            for r in part {
                                if r.sink_touched {
                                    r.receipt.write_set_hash = r.ws.hash();
                                }
                            }
                        });
                    }
                });
            } else {
                for r in tx_results.iter_mut() {
                    if r.sink_touched {
                        r.receipt.write_set_hash = r.ws.hash();
                    }
                }
            }
            hash_ns += t_h.elapsed().as_nanos() as u64;
            // Phase 3 (serial): fold the block delta in canonical order.
            let t_d = std::time::Instant::now();
            for r in tx_results {
                delta.apply(r.ws);
                receipts.push(r.receipt);
            }
            delta_ns += t_d.elapsed().as_nanos() as u64;
        } else {
            // REPAIR PATH: a wound fired, so txs from the first wound on
            // re-execute against the exact materialized prefix. Strictly
            // sequential by nature, and rare enough that its cost is not
            // worth optimizing.
            let mut layered = ctx.base.clone();
            for (i, mut r) in tx_results.into_iter().enumerate() {
                if wounded_set.contains(&i) {
                    let (tx_idx, position, envelope) = &txs[i];
                    let mut scope = ExecScope::new(&ctx.snapshots[0], Some(&layered), ctx.env)?;
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
                if r.sink_touched {
                    if let Some(entry) = r.ws.accounts.iter_mut().find(|(a, _)| *a == FEE_SINK) {
                        entry.1.1 = sink_running;
                    }
                    r.receipt.write_set_hash = r.ws.hash();
                }
                layered.apply(r.ws.clone());
                delta.apply(r.ws);
                receipts.push(r.receipt);
            }
        }

        let t_commit = t_com.elapsed();
        ctx.metrics
            .commit_hash_ns
            .fetch_add(hash_ns, Ordering::Relaxed);
        ctx.metrics
            .commit_delta_ns
            .fetch_add(delta_ns, Ordering::Relaxed);
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
            double_exit: ctx.double_exit.load(Ordering::SeqCst),
            feed_us: m.feed_ns.load(Ordering::Relaxed) / 1_000,
            redundant_edges: m.redundant_edges.load(Ordering::Relaxed),
            steals: m.steals.load(Ordering::Relaxed),
            reads_total: m.reads_total.load(Ordering::Relaxed),
            reads_mv_hit: m.reads_mv_hit.load(Ordering::Relaxed),
            reads_base_hit: m.reads_base_hit.load(Ordering::Relaxed),
            reads_backend: m.reads_backend.load(Ordering::Relaxed),
            evm_us: m.evm_ns.load(Ordering::Relaxed) / 1_000,
            publish_us: m.publish_ns.load(Ordering::Relaxed) / 1_000,
            busy_us: m.busy_ns.load(Ordering::Relaxed) / 1_000,
            parallel_span_us: {
                let f = m.first_dispatch_ns.load(Ordering::Relaxed);
                let l = m.last_done_ns.load(Ordering::Relaxed);
                if f == u64::MAX || l < f {
                    0
                } else {
                    (l - f) / 1_000
                }
            },
            ramp_us: {
                let f = m.first_dispatch_ns.load(Ordering::Relaxed);
                if f == u64::MAX { 0 } else { f / 1_000 }
            },
            commit_us: t_commit.as_micros() as u64,
            commit_hash_us: m.commit_hash_ns.load(Ordering::Relaxed) / 1_000,
            commit_delta_us: m.commit_delta_ns.load(Ordering::Relaxed) / 1_000,
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

fn worker_loop<S: StateDatabase + Sync>(shared: &PoolShared<S>, worker: usize) {
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
fn run_worker_block<S: StateDatabase>(ctx: &BlockCtx<S>, worker: usize) {
    let input = BlockInput {
        snapshot: &ctx.snapshots[worker % ctx.snapshots.len()],
        base: Some(&ctx.base),
    };
    let view = MvView::new(
        &ctx.mv,
        &input,
        ctx.sink_start.clone(),
        &ctx.base_cache,
        &ctx.metrics,
    );
    let mut evm = Context::mainnet()
        .with_db(view)
        .with_block(ctx.env.block_env())
        .with_cfg(ctx.env.cfg_env())
        .build_mainnet();
    let qh = &ctx.queues[worker];
    // Timing and read counts accumulate LOCALLY and flush once per block.
    // Per-read (and even per-tx) `fetch_add` on shared metrics had every
    // worker writing the same cache lines — instrumentation generating the
    // very cross-core traffic it was measuring.
    let mut local_busy_ns: u64 = 0;
    macro_rules! leave {
        ($evm:expr) => {{
            revm::context_interface::ContextTr::db_mut(&mut *$evm).flush_counters();
            ctx.metrics
                .busy_ns
                .fetch_add(local_busy_ns, Ordering::Relaxed);
            return;
        }};
    }
    loop {
        let job = {
            let mut q = qh.q.lock().expect("queue poisoned");
            loop {
                if ctx.aborted.load(Ordering::SeqCst) {
                    leave!(evm);
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
                    leave!(evm);
                }
                // Domain hashing collides when domains ~ workers (measured:
                // the busiest thread took 1.7-2.3x its even share, and
                // ~1.25 threads per block got nothing at all), so an idle
                // worker helps the busiest one rather than parking.
                if let Some(stolen) = ctx.steal(worker) {
                    ctx.metrics.steals.fetch_add(1, Ordering::Relaxed);
                    break stolen;
                }
                q = qh.q.lock().expect("queue poisoned");
                if !q.is_empty() {
                    continue;
                }
                // SPIN before parking: at high throughput the next tx is
                // usually microseconds away, and a park/unpark pair costs
                // two syscalls — more than a small transfer's entire
                // execution. Only a worker that stays dry through the spin
                // advertises itself as parked and blocks.
                let t_idle = std::time::Instant::now();
                drop(q);
                let mut spun = false;
                for _ in 0..SPIN_BEFORE_PARK {
                    std::hint::spin_loop();
                    if !qh.q.lock().expect("queue poisoned").is_empty() {
                        spun = true;
                        break;
                    }
                }
                q = qh.q.lock().expect("queue poisoned");
                if !spun && q.is_empty() {
                    // BOUNDED wait, deliberately. A notification can be
                    // missed: `signal_done` wakes every queue WITHOUT
                    // holding that queue's mutex, so a worker sitting
                    // between "decided to park" and "actually waiting"
                    // sleeps through it and never returns — the block
                    // drains, `seal` finishes, and the pool then hangs
                    // forever joining that thread. (Introducing the spin
                    // above widened that window enough to hit it every
                    // run; the race predates it.) A timeout makes any
                    // missed wake self-healing, and costs nothing when
                    // wakes arrive normally.
                    qh.parked.store(true, Ordering::Release);
                    let (nq, _) = qh.cv.wait_timeout(q, PARK_POLL).expect("queue poisoned");
                    q = nq;
                    qh.parked.store(false, Ordering::Release);
                }
                ctx.metrics
                    .idle_ns
                    .fetch_add(t_idle.elapsed().as_nanos() as u64, Ordering::Relaxed);
            }
        };
        let slot = ctx.slots[job as usize]
            .get()
            .expect("slot set before its index is dispatched");
        let t_busy = std::time::Instant::now();
        let r = execute_one(
            &mut evm,
            &ctx.mv,
            &ctx.metrics,
            ctx.env,
            job,
            slot.tx_idx,
            slot.position,
            &slot.envelope,
            slot.decoded.as_ref(),
            ctx.sink_start_balance,
        );
        let busy = t_busy.elapsed();
        local_busy_ns += busy.as_nanos() as u64;
        ctx.metrics.first_dispatch_ns.fetch_min(
            ctx.started.elapsed().as_nanos() as u64 - busy.as_nanos() as u64,
            Ordering::Relaxed,
        );
        ctx.metrics
            .last_done_ns
            .fetch_max(ctx.started.elapsed().as_nanos() as u64, Ordering::Relaxed);
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
            ctx.completed_len[worker].fetch_add(1, Ordering::Release);
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
pub fn execute_block_stm<S: StateDatabase + Sync + Clone + 'static>(
    snapshot: &S,
    base: Option<&PendingDelta>,
    env: ExecEnv,
    txs: &[(TxIndex, BPosition, TxEnvelope)],
    stats: &Stats,
    workers: usize,
) -> Result<StmOutcome, ExecutorError> {
    with_pool(
        PoolConfig {
            workers,
            ..Default::default()
        },
        |pool: &PoolHandle<'_, S>| {
            pool.run_block(
                vec![snapshot.clone(); workers.max(1)],
                base.cloned().unwrap_or_default(),
                env,
                txs,
                stats,
            )
        },
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
    metrics: &Metrics,
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
            sink_touched: false,
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
    let t_evm = std::time::Instant::now();
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

    metrics
        .evm_ns
        .fetch_add(t_evm.elapsed().as_nanos() as u64, Ordering::Relaxed);
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
    let t_pub = std::time::Instant::now();
    mv.publish_write_set(local_idx, &ws, FEE_SINK);
    metrics
        .publish_ns
        .fetch_add(t_pub.elapsed().as_nanos() as u64, Ordering::Relaxed);
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

    let sink_touched = ws.accounts.iter().any(|(a, _)| *a == FEE_SINK);
    // Hashing now is pure waste when the commit pass must re-hash after
    // patching the accumulator's absolute balance.
    let write_set_hash = if sink_touched { B256::ZERO } else { ws.hash() };
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
        sink_touched,
    })
}
