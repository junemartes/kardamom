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
    /// Unsettled predecessor deltas, NEWEST FIRST, probed before `base`.
    /// Arc-shared so pipelined submission builds a block's read layers
    /// without cloning or merging a single entry (spec P3a: the merge
    /// clone + advance were a measured, growing multi-ms drag on the
    /// pipeline loop).
    pub layers: &'a [std::sync::Arc<PendingDelta>],
    /// Predecessor MULTI-VERSION CACHES, NEWEST FIRST, probed before
    /// everything else (spec P3b mv-as-layer): a drained block's mv top
    /// version per cell IS its final delta — before any fold ran. Reads
    /// probe at `u32::MAX`. Immutable once the block drained; a wound
    /// invalidates the whole layer through the corrected-release
    /// protocol, never by mutation.
    pub mv_layers: &'a [std::sync::Arc<crate::mv::MvCache>],
}

impl<'a, S: StateDatabase> DatabaseRef for BlockInput<'a, S> {
    type Error = kardamom_exec_core::executor::StateRefError;

    fn basic_ref(
        &self,
        address: alloy_primitives::Address,
    ) -> Result<Option<AccountInfo>, Self::Error> {
        for mv in self.mv_layers.iter() {
            if let Some((_, a)) = mv.read_account(u32::MAX, &address) {
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
        }
        for layer in self.layers.iter().map(|l| l.as_ref()).chain(self.base) {
            if let Some((nonce, balance, code_hash)) = layer.accounts.get(&address) {
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
        }
        SnapshotRef {
            inner: self.snapshot,
        }
        .basic_ref(address)
    }

    fn code_by_hash_ref(&self, code_hash: B256) -> Result<revm::state::Bytecode, Self::Error> {
        for mv in self.mv_layers.iter() {
            if let Some(code) = mv.read_code(&code_hash) {
                return Ok(revm::state::Bytecode::new_raw(
                    alloy_primitives::Bytes::copy_from_slice(&code),
                ));
            }
        }
        for layer in self.layers.iter().map(|l| l.as_ref()).chain(self.base) {
            if let Some(code) = layer.code.get(&code_hash)
                && !code.is_empty()
            {
                return Ok(revm::state::Bytecode::new_raw(
                    alloy_primitives::Bytes::copy_from_slice(code),
                ));
            }
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
        for mv in self.mv_layers.iter() {
            if let Some((_, v)) = mv.read_slot(u32::MAX, &address, &key) {
                return Ok(v);
            }
        }
        for layer in self.layers.iter().map(|l| l.as_ref()).chain(self.base) {
            if let Some(v) = layer.storage.get(&(address, key)) {
                return Ok(*v);
            }
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
    /// Wall nanoseconds inside the read path (basic/storage/code), split
    /// out of `evm_ns` — the flamegraph inlines these frames into the
    /// interpreter, so timing is the only way to see them.
    n_read_ns: u64,
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
            n_read_ns: 0,
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
        self.metrics
            .read_ns
            .fetch_add(self.n_read_ns, Ordering::Relaxed);
        self.n_read_ns = 0;
    }
}

impl<'a, S: StateDatabase> MvView<'a, S> {
    fn basic_inner(
        &mut self,
        address: alloy_primitives::Address,
    ) -> Result<Option<AccountInfo>, kardamom_exec_core::executor::StateRefError> {
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
        // Predecessor MV LAYERS first (newest first, at u32::MAX — the
        // top version is the final value; spec P3b mv-as-layer), then
        // pending-delta LAYERS, then the base layer, all BEFORE the
        // cache: the pool-lifetime cache mirrors the BACKEND only, and
        // these layers change per block.
        for mv in self.base.mv_layers.iter() {
            if let Some((_, a)) = mv.read_account(u32::MAX, &address) {
                self.n_base_hit += 1;
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
        }
        for layer in self
            .base
            .layers
            .iter()
            .map(|l| l.as_ref())
            .chain(self.base.base)
        {
            if let Some((nonce, balance, code_hash)) = layer.accounts.get(&address) {
                self.n_base_hit += 1;
                return Ok(Some(AccountInfo {
                    nonce: *nonce,
                    balance: *balance,
                    code_hash: if *code_hash == B256::ZERO {
                        revm::primitives::KECCAK_EMPTY
                    } else {
                        *code_hash
                    },
                    account_id: None,
                    code: None,
                }));
            }
        }
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
        let a = SnapshotRef {
            inner: self.base.snapshot,
        }
        .basic_ref(address)?;
        self.base_cache.accounts[sh]
            .write()
            .expect("base cache poisoned")
            .insert(address, a.clone());
        Ok(a)
    }

    fn code_by_hash_inner(
        &mut self,
        code_hash: B256,
    ) -> Result<revm::state::Bytecode, kardamom_exec_core::executor::StateRefError> {
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

    fn storage_inner(
        &mut self,
        address: alloy_primitives::Address,
        index: U256,
    ) -> Result<U256, kardamom_exec_core::executor::StateRefError> {
        let key = B256::from(index.to_be_bytes::<32>());
        self.n_reads += 1;
        if let Some((ver, v)) = self.mv.read_slot(self.idx, &address, &key) {
            self.n_mv_hit += 1;
            self.reads.push(ReadRecord::Slot(address, key, Some(ver)));
            return Ok(v);
        }
        self.reads.push(ReadRecord::Slot(address, key, None));
        for mv in self.base.mv_layers.iter() {
            if let Some((_, v)) = mv.read_slot(u32::MAX, &address, &key) {
                self.n_base_hit += 1;
                return Ok(v);
            }
        }
        for layer in self
            .base
            .layers
            .iter()
            .map(|l| l.as_ref())
            .chain(self.base.base)
        {
            if let Some(v) = layer.storage.get(&(address, key)) {
                self.n_base_hit += 1;
                return Ok(*v);
            }
        }
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
        let v = SnapshotRef {
            inner: self.base.snapshot,
        }
        .storage_ref(address, index)?;
        self.base_cache.storage[sh]
            .write()
            .expect("base cache poisoned")
            .insert((address, key), v);
        Ok(v)
    }
}

impl<'a, S: StateDatabase> revm::Database for MvView<'a, S> {
    type Error = kardamom_exec_core::executor::StateRefError;

    // Thin TIMED wrappers: the read path inlines into the interpreter and
    // is invisible to a sampling profiler, so `n_read_ns` carves it out of
    // `evm_ns` by measurement. Two clock reads per state access (~50ns)
    // against multi-microsecond questions.
    fn basic(
        &mut self,
        address: alloy_primitives::Address,
    ) -> Result<Option<AccountInfo>, Self::Error> {
        let t0 = std::time::Instant::now();
        let r = self.basic_inner(address);
        self.n_read_ns += t0.elapsed().as_nanos() as u64;
        r
    }

    fn code_by_hash(&mut self, code_hash: B256) -> Result<revm::state::Bytecode, Self::Error> {
        let t0 = std::time::Instant::now();
        let r = self.code_by_hash_inner(code_hash);
        self.n_read_ns += t0.elapsed().as_nanos() as u64;
        r
    }

    fn storage(
        &mut self,
        address: alloy_primitives::Address,
        index: U256,
    ) -> Result<U256, Self::Error> {
        let t0 = std::time::Instant::now();
        let r = self.storage_inner(address, index);
        self.n_read_ns += t0.elapsed().as_nanos() as u64;
        r
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
#[derive(Default)]
pub struct StmOutcome {
    pub receipts: Vec<Receipt>,
    pub delta: PendingDelta,
    /// Txs WOUNDED at validation — a conflict the marks missed, repaired
    /// by re-executing at the canonical position (per-tx; the whole block
    /// never re-runs). Zero on every measured workload so far.
    pub wounds: usize,
    /// Any wound fired (spec invariant #3's counter, per block).
    pub fallback: bool,
    /// The pool DECLINED this block and ran it sequentially, because the
    /// work per transaction was too small for parallel execution to pay
    /// for its own coordination. See `PARALLEL_WORTH_NS`.
    pub declined: bool,
    /// Mean per-tx execution time this block taught the pool — the input
    /// to the next block's decline decision. Non-zero after a DECLINED
    /// block too: that is what keeps the gate from being a trap door.
    pub learned_tx_ns: u64,
    /// Account writes whose domain belongs to another worker.
    pub writes_own: u64,
    pub writes_foreign: u64,
    /// Chain links ordered by FIFO position instead of a DAG edge, and
    /// how often a taken tx had to wait on a stolen FIFO predecessor.
    pub fifo_covered: u64,
    pub fifo_stalls: u64,
    pub read_us: u64,
    /// Per-worker busy microseconds — see `Metrics::busy_per_worker`.
    pub busy_per_worker_us: Vec<u64>,
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
    pub commit_fold_us: u64,
    pub commit_lane_us: u64,
    pub feed_pre_us: u64,
    pub feed_dag_us: u64,
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
#[derive(Clone)]
pub struct Prepared {
    /// `None` when the envelope does not decode — the #92 skip path.
    pub decoded: Option<alloy_consensus::TxEnvelope>,
    /// Predicted contention domains, fee sink already excluded.
    pub domains: Vec<DomainKey>,
    /// 64-bit hash per domain, computed HERE (off the feed thread, in
    /// parallel with every other tx's preparation) so the serial feed
    /// only probes. See [`TouchTable`] for why hashing by value is
    /// sound.
    pub domain_hashes: Vec<u64>,
    /// The domain that decides which thread runs this tx.
    pub primary: Option<DomainKey>,
    /// ⊤: untrained selector — orders behind everything outstanding.
    pub cold: bool,
}

/// Hash a contention cell to 64 bits (see [`TouchTable`]: equal cells
/// hash equal, and a collision can only fabricate a conservative edge).
#[inline]
pub fn domain_hash64(d: &DomainKey) -> u64 {
    use std::hash::{BuildHasher, Hasher};
    let mut h = crate::FnvBuild.build_hasher();
    match d {
        DomainKey::Account(a) => {
            h.write_u8(1);
            h.write(a.as_slice());
        }
        DomainKey::Fixed(a, k) => {
            h.write_u8(2);
            h.write(a.as_slice());
            h.write(k.as_slice());
        }
    }
    h.finish()
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
    let mut domain_hashes: Vec<u64> = Vec::new();
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
                domain_hashes.push(domain_hash64(&c));
                domains.push(c);
            }
            false
        }
        None => true,
    };
    Prepared {
        decoded,
        domains,
        domain_hashes,
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
/// Mean per-tx execution time below which the pool DECLINES a block and
/// runs it sequentially.
///
/// Parallel execution buys down only the execution span; it cannot buy
/// down the serial feed (~0.4us/tx of admission) or the commit tail, and
/// it adds cross-core traffic to every read and publish. Below some
/// amount of work per transaction those fixed costs exceed anything more
/// cores can return, and the honest thing is not to compete.
///
/// RECALIBRATED after the frequency root-cause fix: the original 8us
/// threshold came from measurements where worker cores ran at half
/// clock and transfers lost (0.87x). With cores held at frequency,
/// fully-independent 21k transfers (~4.6us/tx on the mdbx stack)
/// measure 1.54x at 4 workers — they belong on the parallel path. The
/// threshold now sits below transfer cost; only degenerate sub-2.5us
/// work declines.
///
/// This is a FLOOR, not a verdict on transfers: the costs it defends
/// against — the single-threaded feed and the serial delta fold — are
/// implementation limits, and if they come down this constant should come
/// down with them.
pub const PARALLEL_WORTH_NS: u64 = 2_500;

/// Sticky-assignment map bound; beyond it new domains hash as before.
const STICKY_CAP: usize = 65_536;

const SPIN_BEFORE_PARK: u32 = 256;

/// Mean per-tx execution time above which moving a ready tx to an idle
/// core beats keeping its state warm on the owning one. Between a 21k-gas
/// transfer (~2.75us, migration loses) and a uniswap swap (~15us,
/// migration wins).
const STEAL_WORTH_NS: u64 = 6_000;

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
    /// Length HINT, maintained alongside every queue mutation. Spinning
    /// workers and the steal scan read this instead of taking the mutex:
    /// a dry worker probing its queue every ~half-microsecond for up to
    /// 60us, and every steal attempt locking EVERY queue just to read a
    /// length, were contending with the feed's submissions — the measured
    /// reason admission cost GREW with worker count (1.3us/tx at w=1 to
    /// 2.15us at w=4 on fully-independent work). A stale read costs one
    /// wasted lock attempt or one missed-then-caught item; the
    /// authoritative empty-check before parking still happens under the
    /// mutex.
    len: std::sync::atomic::AtomicUsize,
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
/// One completion counter per worker, each on its OWN cache line.
#[derive(Default)]
#[repr(align(64))]
pub(crate) struct PaddedLen(pub(crate) AtomicU32);

/// One u64 counter per cache line (see [`PaddedLen`]).
#[derive(Default)]
#[repr(align(64))]
pub struct PaddedLen64(pub std::sync::atomic::AtomicU64);

#[derive(Default)]
pub struct Metrics {
    /// Nanoseconds spent HOLDING the graph lock, split by cause.
    pub admit_ns: std::sync::atomic::AtomicU64,
    /// Feed pre-DAG bookkeeping: assignment + slot store + envelope
    /// clone (everything before the last-toucher upsert).
    pub feed_pre_ns: std::sync::atomic::AtomicU64,
    /// Feed last-toucher upserts + preds build.
    pub feed_dag_ns: std::sync::atomic::AtomicU64,
    /// Tail: the fold thread's own body, and the hash+validate lanes'
    /// own bodies (aggregate). The gap to the scope's wall is thread
    /// spawn/join.
    pub commit_fold_ns: std::sync::atomic::AtomicU64,
    pub commit_lane_ns: std::sync::atomic::AtomicU64,
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
    /// Account writes published, split by whether the account's domain
    /// belongs to the publishing worker. A FOREIGN write is one two or
    /// more workers can perform on the same account — the true sharing
    /// that no lock granularity removes. Reasoning about which side of a
    /// transfer is foreign has been wrong twice; this counts it.
    pub writes_own: std::sync::atomic::AtomicU64,
    pub writes_foreign: std::sync::atomic::AtomicU64,
    /// Predecessors covered by FIFO order instead of an edge, and takes
    /// of a queued tx that found a FIFO predecessor still running (a
    /// steal moved it) and had to wait.
    pub fifo_covered: std::sync::atomic::AtomicU64,
    pub fifo_stalls: std::sync::atomic::AtomicU64,
    /// Nanoseconds inside MvView's read path — carved OUT of `evm_ns`.
    pub read_ns: std::sync::atomic::AtomicU64,
    /// Per-worker busy nanoseconds — the straggler detector. Dispatch can
    /// be perfectly balanced and idle still high if CORES run at
    /// different speeds (this box's bimodal memory state is per-thread):
    /// the histogram shows it directly.
    pub busy_per_worker: Vec<PaddedLen64>,
}

/// Pool configuration.
#[derive(Debug, Clone)]
pub struct PoolConfig {
    pub workers: usize,
    /// Apply completions to the DAG in batches of this many. `1` updates
    /// the graph on every completion (the immediate policy); larger values
    /// trade graph-lock traffic for dispatch latency — a worker that runs
    /// dry always force-prunes first, so batching can never starve the
    /// pool, only delay a handoff.
    pub prune_batch: usize,
    /// Mean per-tx execution time below which the pool DECLINES a block
    /// and runs it sequentially. See `PARALLEL_WORTH_NS` for the measured
    /// default. Injectable so the policy is testable without depending on
    /// how loaded the machine is, and tunable per deployment.
    pub parallel_worth_ns: u64,
    /// Dispatch on the SENDER rather than the first non-sender cell.
    ///
    /// A transfer writes two accounts and dispatch can only own one of
    /// them, so this chooses WHICH side is foreign. Measured at 4 workers
    /// on transfers, the default (recipient) yields 62.2% own-domain
    /// writes, matching `50% + 1/workers x 50%` exactly. Pure scheduling
    /// policy — the DAG still takes edges on every cell either way — so
    /// it cannot change results, only locality.
    pub dispatch_by_sender: bool,
    /// Enqueue a tx at ADMISSION when every unfinished predecessor is
    /// already released to the same worker's FIFO — queue order then
    /// enforces the chain and the edge/prune hand-off is skipped
    /// entirely. Per-link hand-off through batched pruning is the prime
    /// suspect for the span floor (chains release one tx per prune).
    pub eager_chain: bool,
    /// Assign each NEW domain to the least-loaded worker and remember the
    /// choice for the pool's lifetime, instead of hashing.
    ///
    /// Hashing is stable but collision-blind: 4 hot pairs over 4 workers
    /// land on 4 distinct threads only ~28% of the time, and a collision
    /// puts TWO serial chains on one core — measured as dispatch
    /// [2591, 4017, 694, 698] on the 4-pair scenario, the busiest worker
    /// carrying half the block. Round-robin-on-first-sight was tried and
    /// REVERTED because its assignment reshuffled every block; this keeps
    /// the cross-block stickiness that made hashing win, and fixes only
    /// the collisions.
    pub sticky_assign: bool,
    /// BAG SCHEDULER (the DEFAULT; spec: "admission-queue redesign"):
    /// every runnable tx goes into ONE shared lock-free bag popped by
    /// whichever worker is free; completion is INLINE (the finishing
    /// worker closes its node and dispatches children — no prune
    /// batching) with CHAIN-LOCAL HAND-OFF (the first ready child stays
    /// on the completing worker: chains stream on one core with zero
    /// queue ops). No per-worker queues, no stealing, no eager coverage
    /// (every dependency is an edge). Measured >= the FIFO scheduler on
    /// every ladder rung (parcounter 2.78 -> 2.95x, uniswap 2.31 -> 2.50,
    /// partransfer 1.57 -> 2.0, defi 1.31 -> 1.34, transfers 1.00 ->
    /// 1.07). `false` selects the legacy per-worker FIFO scheduler.
    pub bag_scheduler: bool,
    /// Between blocks, workers SPIN-YIELD instead of sleeping on the
    /// condvar. schedutil drops a core to base clock (2.1 vs 4.2GHz
    /// measured) the moment it idles, and burst-park execution never
    /// ramps it back — the root cause of the long "bimodal machine"
    /// hunt. A yielding spinner holds the governor's utilization signal
    /// up while surrendering the core within microseconds to any real
    /// work — including the commit tail's scoped threads, which pin to
    /// these same cores. Costs idle watts; production executor pools
    /// run continuously busy, so this mainly serves dedicated-core
    /// deployments and honest benchmarking.
    pub keep_hot: bool,
    /// Run the commit tail's parallel phases ON the worker cores.
    /// Right for block-at-a-time (workers park during the tail, their
    /// cores are hot and instantly yielded); WRONG for the pipeline,
    /// where the next block executes on those cores while this block's
    /// tail runs — the phases then stay on the caller's mask.
    pub tail_on_workers: bool,
    /// Pin worker i to `pin_cores[i % len]`.
    ///
    /// MEASURED REASON (Ryzen 3600, two 3-core CCXes with SPLIT 16MB L3):
    /// a worker sharing its CCX with the mdbx writer runs the SAME block
    /// at 20.5us/tx that it runs at 10.6us/tx isolated — the writer's
    /// page churn evicts the interpreter's working set from the shared
    /// L3, a uniform memory-level tax no code-level timer can see (it
    /// slowed evm and the read path by the same 28%). Empty = let the
    /// scheduler place workers (it settles them ON the writer's CCX often
    /// enough to produce a floating per-block performance step).
    pub pin_cores: Vec<usize>,
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
            parallel_worth_ns: PARALLEL_WORTH_NS,
            dispatch_by_sender: false,
            eager_chain: true,
            sticky_assign: false,
            bag_scheduler: true,
            keep_hot: false,
            tail_on_workers: true,
            pin_cores: Vec::new(),
        }
    }
}

/// The feed's LAST-TOUCHER index: cell-hash -> most recent toucher.
///
/// Flat and hash-keyed, not a `HashMap<DomainKey, u32>`: the map held
/// ~8k live entries of 53-byte keys (~512KB, L2-busting) and compared
/// those keys on every probe, and the upsert pair was the serial feed's
/// largest stage (0.29µs/tx measured). Here a slot is 16 bytes, the
/// whole table is 256KB, a probe is ONE cache line, and the key
/// comparison is a u64.
///
/// COLLISIONS ARE SAFE BY CONSTRUCTION: a 64-bit collision fabricates a
/// dependency EDGE between two txs that do not actually share a cell.
/// The DAG is conservative — a false edge costs a sliver of parallelism
/// and nothing else, while a MISSED edge (impossible here: equal cells
/// hash equal) is what validation exists to catch.
///
/// Reset is O(1): a slot belongs to the current block only if its
/// `stamp` matches, so a new block bumps the stamp instead of clearing
/// 256KB.
struct TouchSlot {
    hash: u64,
    idx: u32,
    stamp: u32,
}

pub(crate) struct TouchTable {
    slots: Vec<TouchSlot>,
    mask: usize,
    stamp: u32,
}

impl TouchTable {
    fn new(capacity_pow2: usize) -> Self {
        Self {
            slots: (0..capacity_pow2)
                .map(|_| TouchSlot {
                    hash: 0,
                    idx: 0,
                    stamp: 0,
                })
                .collect(),
            mask: capacity_pow2 - 1,
            stamp: 0,
        }
    }

    /// O(1) between-block reset (and the ⊤-barrier clear).
    fn clear(&mut self) {
        self.stamp = self.stamp.wrapping_add(1);
        if self.stamp == 0 {
            // Wrapped after 4.3B blocks: hard-clear so no stale slot can
            // resurrect, then restart at 1.
            for s in self.slots.iter_mut() {
                s.stamp = 0;
            }
            self.stamp = 1;
        }
    }

    /// Record `idx` as the latest toucher of `hash`; return the previous
    /// one, if this block has seen the cell.
    #[inline]
    fn upsert(&mut self, hash: u64, idx: u32) -> Option<u32> {
        let mut i = hash as usize & self.mask;
        loop {
            let slot = &mut self.slots[i];
            if slot.stamp != self.stamp {
                slot.hash = hash;
                slot.idx = idx;
                slot.stamp = self.stamp;
                return None;
            }
            if slot.hash == hash {
                let prev = slot.idx;
                slot.idx = idx;
                return Some(prev);
            }
            i = (i + 1) & self.mask;
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
    /// TRUE once this node has been handed to a worker queue. The eager
    /// coverage test reads THIS, not `indegree == 0`: prune decrements
    /// indegrees first and pushes later, and in that window the feed
    /// would otherwise enqueue a successor AHEAD of its predecessor —
    /// the owner then spins forever on a head whose FIFO predecessor
    /// sits behind it (measured: intermittent silent wedge on the
    /// transfers shape, ~20% of runs). Set under the queue lock, so a
    /// `true` read orders the predecessor's push before any subsequent
    /// eager push to the same queue.
    queued: AtomicBool,
    /// Predecessors covered by FIFO ORDER instead of an edge (eager chain
    /// mode): they were already released to THIS tx's own queue when this
    /// tx was admitted, so queue position orders them — no edge, no prune
    /// hand-off. Written only by the serial feed before the tx can be
    /// released; read by whoever takes the tx from a queue, which must
    /// verify each one has a result before executing (work stealing can
    /// move a FIFO predecessor to another thread mid-flight, and the
    /// verification is what makes that race benign rather than a data
    /// race on state).
    fifo_preds: Mutex<Vec<u32>>,
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
    /// Unsettled predecessor deltas, newest first (see `BlockInput`).
    /// Unsettled predecessor deltas + the fee-sink block-start view —
    /// everything about the block's READ BASE that depends on its
    /// predecessor's outcome. LATE-BOUND (spec P3b): admission is
    /// layer-independent, so a pipelined consumer builds, feeds, and
    /// submits this block during its predecessor's execution and binds
    /// the layers when the predecessor's delta releases. Workers gate on
    /// the bind before executing (see `run_worker_block`); the
    /// block-at-a-time path binds at session build, making the gate
    /// free.
    binding: std::sync::OnceLock<BoundLayers>,
    /// Arc: outlives the block as a predecessor mv layer (mv-as-layer
    /// releases clone it; the last holder drops it, usually the reaper).
    mv: Arc<MvCache>,
    /// Shared read-through cache over the immutable block-input layer.
    base_cache: std::sync::Arc<BaseCache>,
    /// Recycle pools (read-record buffers for workers; the tail ships
    /// spent arenas back through the reaper).
    recycle: std::sync::Arc<RecyclePools>,
    slots: Vec<std::sync::OnceLock<TxSlot>>,
    results: Vec<std::sync::OnceLock<Result<TxResult, ExecutorError>>>,
    queues: Vec<WorkerQueue>,
    /// Bag-scheduler mode (see PoolConfig::bag_scheduler): the shared
    /// runnable set. Allocated always (16KB), used when `bag_mode`.
    bag: crossbeam_queue::ArrayQueue<u32>,
    bag_mode: bool,
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
    /// PADDED to a cache line each. These are RMW'd by every worker on
    /// every completion and read by every prune; packed as a plain
    /// `Vec<AtomicU32>` all eight counters shared ONE line, so each
    /// completion invalidated it for every other worker.
    completed_len: Vec<PaddedLen>,
    /// Completions parked across all buffers, i.e. DAG updates owed.
    pending: std::sync::atomic::AtomicU64,
    prune_batch: usize,
    /// Block start, so workers can stamp first-dispatch / last-completion
    /// offsets without reaching into the session.
    started: std::time::Instant,
    /// Whether an idle worker may take a ready tx from another thread.
    ///
    /// Stealing migrates a transaction to a core whose caches know nothing
    /// about the accounts it touches. That pays handsomely when the tx is
    /// expensive (uniswap swaps, ~15us: 1.55x -> 1.67x) and LOSES badly
    /// when it is not (21k-gas transfers, ~2.75us: 0.93x -> 0.60x), where
    /// the migration costs more than the work it moves. So the policy is
    /// measured, not fixed: the pool tracks mean per-tx execution time and
    /// enables stealing only above a threshold.
    steal_enabled: bool,
    /// How long a dry worker spins before parking, in ns — sized from the
    /// measured mean per-tx time. The old fixed 256 `spin_loop` hints
    /// (<1us) were 25x SHORTER than the typical gap between chain-link
    /// releases (~one tx execution), so workers parked into the exact
    /// window their next transaction arrived in and paid up to the 200us
    /// poll to notice it. MEASURED at w=4/4-pair: 25k dry cycles per 8k
    /// txs, ~30% of worker capacity idle, while all prune work cost 8ms —
    /// the idle was park latency, not scheduler cost.
    spin_ns: u64,
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
    /// Safe by VERIFICATION: under eager chain mode queue position DOES
    /// carry an ordering obligation (FIFO-covered predecessors have no
    /// edge), so anything taken from a queue — here or by its owner — is
    /// checked runnable first via `fifo_ready`. A mid-chain link fails
    /// the check and stays put.
    ///
    /// Taken from the BACK, leaving the owner its front: the owner's
    /// front is the oldest (most likely to have warm state), and the two
    /// ends rarely contend.
    /// May `idx` execute right now? True when every FIFO-covered
    /// predecessor has a result. Ordinary FIFO drain makes this true by
    /// construction (the predecessor sat AHEAD in the same queue); it is
    /// false only when a steal moved a predecessor to another thread and
    /// that thread is still running it.
    fn fifo_ready(&self, idx: u32) -> bool {
        let preds = self.nodes[idx as usize]
            .fifo_preds
            .lock()
            .expect("fifo_preds poisoned");
        preds
            .iter()
            .all(|p| self.results[*p as usize].get().is_some())
    }

    fn steal(&self, thief: usize) -> Option<u32> {
        let mut best: Option<(usize, usize)> = None;
        for (w, qh) in self.queues.iter().enumerate() {
            if w == thief {
                continue;
            }
            // Hint only — the victim's own lock confirms below.
            let len = qh.len.load(Ordering::Acquire);
            // ANY queued tx is stealable. The previous `len > 1` guard —
            // "leave the owner its work" — silently disabled stealing
            // altogether: under DAG chains a domain releases ONE ready tx
            // at a time, so queues hold 0 or 1 items essentially always.
            // That capped effective parallelism at roughly the number of
            // domains that happened to hash to distinct workers.
            if len >= 1 && best.is_none_or(|(_, b)| len > b) {
                best = Some((w, len));
            }
        }
        let (victim, _) = best?;
        let vq = &self.queues[victim];
        // Verification stays UNDER the victim's lock, deliberately — this
        // is the one place the pop/verify/putback triple must be atomic.
        // A back-putback after an unlocked window reorders: the feed can
        // eagerly enqueue the candidate's own FIFO-successor into the gap,
        // the putback lands BEHIND it, and the owner livelocks on a head
        // whose predecessor now sits behind it (measured: 3/6 test runs
        // hung). The owner's pop-path verify can run unlocked because its
        // FRONT-putback preserves relative order; back-putback cannot.
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

    /// Hand a READY tx to its assigned thread. Called only with no node
    /// mutex held (lock order: node registration points are leaves).
    fn push_ready(&self, worker: usize, idx: u32) {
        if self.bag_mode {
            // ONE shared lock-free runnable set: no assignment, no
            // per-worker locks, balanced by whoever pops first. `queued`
            // is irrelevant (coverage is off in bag mode — every
            // dependency is an edge).
            let pushed = self.bag.push(idx).is_ok();
            debug_assert!(pushed, "bag sized at MAX_BLOCK_TXS");
            for qh in &self.queues {
                if qh.parked.load(Ordering::Acquire) {
                    qh.cv.notify_one();
                    break;
                }
            }
            return;
        }
        let qh = &self.queues[worker];
        let mut q = qh.q.lock().expect("queue poisoned");
        self.nodes[idx as usize]
            .queued
            .store(true, Ordering::Release);
        q.push_back(idx);
        qh.len.fetch_add(1, Ordering::Release);
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
    /// BAG MODE completion, INLINE: the finishing worker closes its own
    /// node right here — no per-worker completion buffer, no
    /// cross-worker buffer scan, no `pending` counter round-trip. One
    /// uncontended child-list lock, one indegree fetch_sub per child,
    /// ready children go straight to the bag. Prune batching only ever
    /// existed to amortize the OLD global graph lock; per-node locks
    /// made it ceremony (measured ~0.8µs per completion on independent
    /// transfers — the next wall after the feed).
    fn complete_inline(&self, job: u32) -> Option<u32> {
        let node = &self.nodes[job as usize];
        let mut list = node.children.lock().expect("children poisoned");
        if !node.open.swap(false, Ordering::AcqRel) {
            debug_assert!(false, "stm: tx left the graph twice");
            tracing::error!(
                tx = job,
                "stm: tx left the graph twice — scheduler invariant violated"
            );
            self.double_exit.fetch_add(1, Ordering::SeqCst);
        }
        // Collect under the lock, dispatch after (bag push is lock-free,
        // but keeping the child-list critical section minimal matters
        // for the feed racing to register on this node).
        // Fixed-size stack buffer + spill: no per-completion allocation.
        let mut ready_buf = [0u32; 8];
        let mut n_ready = 0usize;
        let mut spill: Vec<u32> = Vec::new();
        for c in list.iter() {
            let child = &self.nodes[*c as usize];
            if child.indegree.fetch_sub(1, Ordering::AcqRel) == 1 {
                if n_ready < ready_buf.len() {
                    ready_buf[n_ready] = *c;
                    n_ready += 1;
                } else {
                    spill.push(*c);
                }
            }
        }
        list.clear();
        drop(list);
        self.finished.fetch_add(1, Ordering::SeqCst);
        self.metrics.completions.fetch_add(1, Ordering::Relaxed);
        // CHAIN-LOCAL HAND-OFF: the FIRST ready child stays with the
        // completing worker as its next job (returned, no bag op, warm
        // cache — a chain streams on one core exactly as the FIFO
        // scheduler streamed it, without a queue); the rest go to the
        // bag for whoever is free.
        let mut keep: Option<u32> = None;
        for c in ready_buf.iter().take(n_ready).chain(spill.iter()) {
            if keep.is_none() {
                keep = Some(*c);
            } else {
                self.push_ready(0, *c);
            }
        }
        if self.drained() {
            for q in &self.queues {
                q.cv.notify_all();
            }
            self.done_cv.notify_all();
        }
        keep
    }

    fn prune(&self, forced: bool) -> usize {
        let t0 = std::time::Instant::now();
        let mut applied = 0usize;
        let mut ready: Vec<(usize, u32)> = Vec::new();
        for (w, buf) in self.completed.iter().enumerate() {
            // Skip untouched buffers without paying for their mutex.
            if self.completed_len[w].0.load(Ordering::Acquire) == 0 {
                continue;
            }
            let drained: Vec<u32> = {
                let mut b = buf.lock().expect("completed poisoned");
                if b.is_empty() {
                    continue;
                }
                self.completed_len[w]
                    .0
                    .fetch_sub(b.len() as u32, Ordering::AcqRel);
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
    /// The NEXT block, fed while `ctx` still executes (pipeline depth 2).
    /// The tail installs it the moment `ctx` drains; the generation bump
    /// walks the workers over. Feeding needs no workers, so admission of
    /// block N+1 overlaps execution of block N entirely.
    next: Option<Arc<BlockCtx<S>>>,
    shutdown: bool,
    cfg: PoolConfig,
}

type PoolShared<S> = (Mutex<PoolState<S>>, Condvar);

/// A spent block's droppables, shipped to the reaper thread.
/// Recycle pools (steady-state zero-allocation blocks): the reaper
/// scrubs spent block structures IN PLACE (drop entries, keep every
/// buffer) and parks them here; session build pops instead of mapping
/// ~8MB of fresh arenas per block. mv caches outlive their block as
/// mv-as-layer references, so a still-shared cache parks until its
/// last Arc drops and is swept at the next build.
struct RecyclePools {
    arenas: Mutex<Vec<SpentArena>>,
    mv_clean: Mutex<Vec<MvCache>>,
    mv_parked: Mutex<Vec<Arc<MvCache>>>,
    /// Cleared per-tx read-record buffers, returned by the reaper in
    /// one batch per block, taken by workers in batches of 64 — the
    /// per-tx `Vec::with_capacity` + growth reallocs were the largest
    /// STM-specific allocation (~4KB/tx at contract weight).
    read_bufs: Mutex<Vec<Vec<ReadRecord>>>,
    /// Cleared result-carcass vecs (4096 x ~450B TxResult — one of the
    /// ~16 huge per-block allocations the size histogram exposed).
    carcasses: Mutex<Vec<Vec<TxResult>>>,
    /// Cleared PendingDelta shells for the fold (the maps' tables are
    /// the other huge per-block allocation); returned by the consumer
    /// via [`PoolHandle::recycle_delta`] once a release settles.
    deltas: Mutex<Vec<PendingDelta>>,
}

struct SpentArena {
    slots: Vec<std::sync::OnceLock<TxSlot>>,
    results: Vec<std::sync::OnceLock<Result<TxResult, ExecutorError>>>,
    nodes: Vec<Node>,
}

enum Reap {
    /// A finished block's result carcass: read-record buffers recycle,
    /// the rest drops.
    Results {
        results: Vec<TxResult>,
        pools: std::sync::Arc<RecyclePools>,
    },
    /// A finished block's recyclable structures.
    Arena {
        slots: Vec<std::sync::OnceLock<TxSlot>>,
        results: Vec<std::sync::OnceLock<Result<TxResult, ExecutorError>>>,
        nodes: Arc<Vec<Node>>,
        mv: Arc<MvCache>,
        pools: std::sync::Arc<RecyclePools>,
    },
}

/// One sealed block handed to the persistent TAIL thread (spec P3a):
/// drain, release the pool slot, then `block_tail`.
/// The late-bound part of a block's read base (see `BlockCtx::binding`).
struct BoundLayers {
    /// Predecessor mv caches, newest first (spec P3b mv-as-layer).
    mv_layers: Vec<std::sync::Arc<MvCache>>,
    layers: Vec<std::sync::Arc<PendingDelta>>,
    sink_start: Option<AccountInfo>,
    sink_start_balance: U256,
}

struct TailJob<S: StateDatabase> {
    ctx: Arc<BlockCtx<S>>,
    n_txs: usize,
    started: std::time::Instant,
    cold: usize,
    edges: usize,
    dispatch: Vec<u32>,
    out: std::sync::mpsc::Sender<Result<StmOutcome, ExecutorError>>,
    delta_out: Option<DeltaOut>,
}

/// Streaming delta hand-off (spec P3): block N's folded delta, released
/// to whoever layers block N+1 — before N's receipts, and (in
/// speculative mode) before N's validation verdict.
pub struct DeltaRelease {
    pub block: u64,
    pub delta: std::sync::Arc<PendingDelta>,
    /// True when this re-issues a block whose earlier speculative
    /// release was invalidated by a wound: everything layered on the
    /// stale release must be aborted and rebuilt on THIS delta.
    pub corrected: bool,
}

/// Binds a deferred session's read base (see
/// [`PoolHandle::begin_block_deferred`]). Consumed by `bind`; a binder
/// dropped without binding leaves the block gated — `abort_active` it.
/// Holds only a WEAK reference: the tail's ctx unwrap must not wait on
/// a consumer that decided to abort instead of bind.
pub struct LayerBinder<S: StateDatabase> {
    ctx: std::sync::Weak<BlockCtx<S>>,
}

impl<S: StateDatabase> LayerBinder<S> {
    /// Install the unsettled predecessor layers (newest first) and
    /// probe the fee-sink block-start view through them; wakes the
    /// gated workers.
    pub fn bind(self, layers: Vec<std::sync::Arc<PendingDelta>>) -> Result<(), ExecutorError> {
        self.bind_with(Vec::new(), layers, None)
    }

    /// [`Self::bind`] with predecessor MV LAYERS (spec P3b
    /// mv-as-layer), newest first, probed before the delta layers. The
    /// fee sink is never published to an mv cache, so `sink_final`
    /// (from the predecessor's [`MvRelease`]) is REQUIRED whenever mv
    /// layers are present; without mv layers it may be None and the
    /// sink is probed through the delta layers as usual.
    pub fn bind_with(
        self,
        mv_layers: Vec<std::sync::Arc<MvCache>>,
        layers: Vec<std::sync::Arc<PendingDelta>>,
        sink_final: Option<Option<AccountInfo>>,
    ) -> Result<(), ExecutorError> {
        let Some(ctx) = self.ctx.upgrade() else {
            return Err(ExecutorError::State(
                "stm: deferred block gone before bind (aborted)".into(),
            ));
        };
        let sink_start = match sink_final {
            Some(sink) => sink,
            None => {
                assert!(
                    mv_layers.is_empty(),
                    "mv layers cannot serve the fee sink — pass the release's sink_final"
                );
                let probe = BlockInput {
                    snapshot: ctx.snapshots.first().expect("at least one snapshot"),
                    base: Some(&ctx.base),
                    layers: &layers,
                    mv_layers: &[],
                };
                probe
                    .basic_ref(FEE_SINK)
                    .map_err(|e| ExecutorError::State(format!("fee-sink read: {e}")))?
            }
        };
        let sink_start_balance = sink_start.as_ref().map(|a| a.balance).unwrap_or(U256::ZERO);
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
        for q in &ctx.queues {
            q.cv.notify_all();
        }
        ctx.done_cv.notify_all();
        Ok(())
    }
}

/// The EARLY streaming release (spec P3b mv-as-layer): block N's
/// multi-version cache, shipped right after drain + extract — before
/// phase-1/fold/validation. Its top version per cell equals what the
/// fold will compute; the sink (never published to mv) rides along,
/// already materialized. Pre-verdict by construction: a wound
/// invalidates it through the corrected `DeltaRelease` that follows.
pub struct MvRelease {
    pub block: u64,
    pub mv: std::sync::Arc<MvCache>,
    /// The fee sink's FINAL account for this block (start + fee sum).
    pub sink_final: Option<AccountInfo>,
}

struct DeltaOut {
    tx: std::sync::mpsc::Sender<DeltaRelease>,
    /// mv-as-layer early release channel (implies speculative).
    mv_tx: Option<std::sync::mpsc::Sender<MvRelease>>,
    /// Speculative (spec P3b): release at fold, CONCURRENT with
    /// validation — a wound invalidates the release (a `corrected`
    /// re-issue follows). Conservative (P3a): release only after the
    /// verdict, when the delta can no longer change.
    speculative: bool,
}

/// A submitted block's pending outcome. Outcomes complete in submission
/// order; `wait` blocks until this block is validated, repaired if
/// wounded, and committed.
pub struct BlockTicket {
    rx: std::sync::mpsc::Receiver<Result<StmOutcome, ExecutorError>>,
}

impl BlockTicket {
    pub fn wait(self) -> Result<StmOutcome, ExecutorError> {
        self.rx
            .recv()
            .unwrap_or_else(|_| Err(ExecutorError::State("stm pool: tail thread gone".into())))
    }
}

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
    /// Mean per-tx execution time of the last block, feeding the stealing
    /// policy. Atomic (not `Cell`): P3's persistent tail thread updates
    /// it after each block's fold while the feed thread reads it.
    avg_tx_ns: std::sync::Arc<std::sync::atomic::AtomicU64>,
    parallel_worth_ns: u64,
    dispatch_by_sender: bool,
    eager_chain: bool,
    sticky_assign: bool,
    /// Domain -> worker, pool-lifetime (feed-thread-owned). Capped: past
    /// `STICKY_CAP` entries new domains fall back to hashing, so a
    /// long-lived pool cannot grow this without bound.
    assign: std::cell::RefCell<FastMap<DomainKey, usize>>,
    /// Cumulative txs dispatched per worker — the load the least-loaded
    /// choice reads.
    assign_load: std::cell::RefCell<Vec<u64>>,
    /// The persistent TAIL thread's inbox (spec P3a): sealed blocks go
    /// here; the thread drains, releases the pool slot, and runs
    /// `block_tail` while the caller feeds the next block.
    tail: std::sync::mpsc::Sender<TailJob<S>>,
    /// POOL-LIFETIME cache of the BACKEND layer (below any pending-delta
    /// layer, which is probed before it — see `MvView`). parcounter
    /// measured 100% of reads reaching mdbx: hot cells change every
    /// block, so a per-block cache can never hit. But the block's own
    /// delta CARRIES every new value — `advance_base` upserts it, turning
    /// next block's backend reads into warm map hits. Valid iff every
    /// backend commit is mirrored here; the A/B harness asserts
    /// byte-identical results per block, which checks exactly that.
    base_cache: std::sync::Arc<BaseCache>,
    /// Recycled block structures (see [`RecyclePools`]).
    recycle: std::sync::Arc<RecyclePools>,
    /// The feed's last-toucher index — POOL-LIFETIME (allocated once,
    /// O(1) stamp reset per block) and feed-owned, exactly like
    /// `assign`: only the single admission thread ever touches it.
    touch: std::cell::RefCell<TouchTable>,
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
        ..cfg
    };
    let pin_cores = cfg.pin_cores.clone();
    let cfg_keep_hot = cfg.keep_hot;
    let cfg_tail_on_workers = cfg.tail_on_workers;
    let (sticky_assign, parallel_worth_ns, dispatch_by_sender, eager_chain) = (
        cfg.sticky_assign,
        cfg.parallel_worth_ns,
        cfg.dispatch_by_sender,
        cfg.eager_chain,
    );
    let shared: PoolShared<S> = (
        Mutex::new(PoolState {
            generation: 0,
            ctx: None,
            next: None,
            shutdown: false,
            cfg,
        }),
        Condvar::new(),
    );
    let shared_ref = &shared;
    let avg_tx_ns = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let (reap_tx, reap_rx) = std::sync::mpsc::channel::<Reap>();
    let (tail_tx, tail_rx) = std::sync::mpsc::channel::<TailJob<S>>();
    let recycle_pools = std::sync::Arc::new(RecyclePools {
        arenas: Mutex::new(Vec::new()),
        mv_clean: Mutex::new(Vec::new()),
        mv_parked: Mutex::new(Vec::new()),
        read_bufs: Mutex::new(Vec::new()),
        carcasses: Mutex::new(Vec::new()),
        deltas: Mutex::new(Vec::new()),
    });
    std::thread::scope(|scope| {
        // Reaper: drops junk freight and SCRUBS recyclable arenas (in
        // place — entries dropped, buffers kept) so seal() pays for
        // neither, and the next session build maps nothing. Exits when
        // the pool drops the last sender.
        scope.spawn(move || {
            while let Ok(r) = reap_rx.recv() {
                match r {
                    Reap::Results { mut results, pools } => {
                        let mut bufs: Vec<Vec<ReadRecord>> = Vec::with_capacity(results.len());
                        for r in results.iter_mut() {
                            let mut b = std::mem::take(&mut r.reads);
                            b.clear();
                            bufs.push(b);
                        }
                        {
                            let mut g = pools.read_bufs.lock().expect("pools poisoned");
                            let room = MAX_BLOCK_TXS.saturating_sub(g.len());
                            g.extend(bufs.into_iter().take(room));
                        }
                        // The carcass vec itself recycles: drop the spent
                        // TxResults in place, keep the 4096-slot buffer.
                        results.clear();
                        let mut g = pools.carcasses.lock().expect("pools poisoned");
                        if g.len() < 4 {
                            g.push(results);
                        }
                    }
                    Reap::Arena {
                        mut slots,
                        mut results,
                        nodes,
                        mv,
                        pools,
                    } => {
                        for c in slots.iter_mut() {
                            let _ = c.take();
                        }
                        for c in results.iter_mut() {
                            let _ = c.take();
                        }
                        if let Ok(nodes) = Arc::try_unwrap(nodes) {
                            for nd in nodes.iter() {
                                nd.open.store(false, Ordering::Relaxed);
                                nd.children.lock().expect("node poisoned").clear();
                                nd.indegree.store(0, Ordering::Relaxed);
                                nd.worker.store(0, Ordering::Relaxed);
                                nd.queued.store(false, Ordering::Relaxed);
                                nd.fifo_preds.lock().expect("node poisoned").clear();
                            }
                            pools
                                .arenas
                                .lock()
                                .expect("pools poisoned")
                                .push(SpentArena {
                                    slots,
                                    results,
                                    nodes,
                                });
                        }
                        match Arc::try_unwrap(mv) {
                            Ok(cache) => {
                                cache.scrub();
                                pools.mv_clean.lock().expect("pools poisoned").push(cache);
                            }
                            Err(shared) => {
                                pools.mv_parked.lock().expect("pools poisoned").push(shared)
                            }
                        }
                    }
                }
            }
        });
        // The persistent TAIL thread (spec P3a). One thread owns every
        // block's post-drain work in submission order; per-block scoped
        // threads for sub-millisecond phases measured as a net loss, and
        // this thread is also what lets the caller feed block N+1 while
        // block N validates and commits.
        {
            let reaper = reap_tx.clone();
            let recycle = recycle_pools.clone();
            let pins = pin_cores.clone();
            let hot = cfg_keep_hot;
            let tow = cfg_tail_on_workers;
            let avg = avg_tx_ns.clone();
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
                    } = job;
                    // Drain: wait out the in-flight tail of execution.
                    // WATCHDOG as in the inline path — a stranded edge
                    // fail-stops with forensics instead of freezing.
                    let deadline = std::time::Instant::now() + STALL_TIMEOUT;
                    let mut drain_err: Option<ExecutorError> = None;
                    while !(ctx.aborted.load(Ordering::SeqCst) || ctx.drained()) {
                        if std::time::Instant::now() > deadline {
                            let admitted = ctx.admitted.load(Ordering::SeqCst);
                            let finished = ctx.finished.load(Ordering::SeqCst);
                            let stuck: Vec<(u32, u32)> = (0..admitted)
                                .filter(|i| ctx.results[*i as usize].get().is_none())
                                .map(|i| {
                                    (i, ctx.nodes[i as usize].indegree.load(Ordering::SeqCst))
                                })
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
                            drain_err = Some(ExecutorError::State(format!(
                                "stm: block {} failed to drain after {:?}: admitted={admitted}                                  finished={finished} stuck(idx,indegree)={stuck:?}",
                                ctx.env.block_number, STALL_TIMEOUT
                            )));
                            break;
                        }
                        if ctx.pending.load(Ordering::SeqCst) > 0 {
                            ctx.prune(true);
                            continue;
                        }
                        std::thread::yield_now();
                    }
                    // Release the slot in ALL paths — and INSTALL the
                    // staged block, if any: its admission ran while this
                    // block executed, so the workers walk straight onto
                    // full queues.
                    {
                        let mut st = shared_ref.0.lock().expect("pool poisoned");
                        st.ctx = st.next.take();
                        if st.ctx.is_some() {
                            st.generation += 1;
                        }
                    }
                    shared_ref.1.notify_all();
                    if let Some(e) = drain_err {
                        let _ = out.send(Err(e));
                        continue;
                    }
                    let t_exec_wall = started.elapsed();
                    let mut ctx_arc = ctx;
                    let t_drain0 = std::time::Instant::now();
                    let unwrap_deadline = std::time::Instant::now() + STALL_TIMEOUT;
                    let ctx = loop {
                        match Arc::try_unwrap(ctx_arc) {
                            Ok(c) => break c,
                            Err(back) => {
                                // WATCHDOG: a worker that never drops its
                                // Arc (wedged in a stall path) would spin
                                // this loop forever and hang every later
                                // ticket SILENTLY. Fail loudly instead.
                                if std::time::Instant::now() > unwrap_deadline {
                                    let holders = Arc::strong_count(&back);
                                    eprintln!(
                                        "stm WEDGE: ctx unwrap stalled {}s, {} Arc holders,                                          block {}, fifo_stalls {}",
                                        STALL_TIMEOUT.as_secs(),
                                        holders,
                                        back.env.block_number,
                                        back.metrics.fifo_stalls.load(Ordering::Relaxed),
                                    );
                                    let _ = out.send(Err(ExecutorError::State(format!(
                                        "stm: ctx unwrap stalled, {holders} holders"
                                    ))));
                                    // Leak the ctx rather than spin: the
                                    // wedged worker still references it.
                                    std::mem::forget(back);
                                    continue 'jobs;
                                }
                                ctx_arc = back;
                                std::thread::yield_now();
                            }
                        }
                    };
                    let t_drain = t_drain0.elapsed();
                    let _ = out.send(block_tail(
                        ctx, n_txs, t_exec_wall, t_drain, cold, edges, dispatch, hot, tow,
                        &pins, &reaper, &avg, delta_out, &recycle,
                    ));
                }
            });
        }
        for w in 0..workers {
            let pin = pin_cores.clone();
            let hot = cfg_keep_hot;
            scope.spawn(move || {
                if !pin.is_empty() {
                    let id = core_affinity::CoreId {
                        id: pin[w % pin.len()],
                    };
                    if !core_affinity::set_for_current(id) {
                        tracing::warn!(worker = w, core = id.id, "stm: worker pin failed");
                    }
                }
                worker_loop(shared_ref, w, hot)
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
            touch: std::cell::RefCell::new(TouchTable::new(MAX_BLOCK_TXS * 4)),
            tail: tail_tx.clone(),
            parallel_worth_ns,
            dispatch_by_sender,
            eager_chain,
        };
        let r = f(&handle);
        drop(handle);
        drop(reap_tx);
        drop(tail_tx);
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
    /// Reusable predecessor scratch — a fresh `Vec` per tx was a heap
    /// allocation on the serial feed (~50ns/tx).
    preds_buf: Vec<u32>,
    /// `KARDAMOM_STM_FEED_STAGES`: per-stage feed timers (off by default
    /// — they cost what they measure).
    stage_timing: bool,
    /// The most recent ⊤ (cold) tx: conflicts with everything, so every
    /// later admission takes an edge from it while it is outstanding.
    last_barrier: Option<u32>,
    dispatch: Vec<u32>,
    /// Admitted count; the envelopes live in the ctx slots (the repair
    /// path reads them there — no parallel copy).
    n_txs: usize,
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
        self.begin_block_layered(snapshots, base, Vec::new(), env, stats)
    }

    /// Like [`Self::begin_block_per_worker`], with unsettled predecessor
    /// deltas layered (newest first) WITHOUT cloning or merging — the
    /// pipelined caller's zero-copy read stack (spec P3a).
    pub fn begin_block_layered<'p>(
        &'p self,
        snapshots: Vec<S>,
        base: PendingDelta,
        layers: Vec<std::sync::Arc<PendingDelta>>,
        env: ExecEnv,
        stats: &'p Stats,
    ) -> Result<BlockSession<'p, 'a, S>, ExecutorError> {
        let (sess, binder) = self.begin_block_deferred(snapshots, base, env, stats)?;
        binder.bind(layers)?;
        Ok(sess)
    }

    /// [`Self::begin_block_layered`] with the layer bind DEFERRED (spec
    /// P3b): admission is layer-independent, so the pipelined consumer
    /// builds, feeds, and even submits this session while the
    /// predecessor still executes, then calls [`LayerBinder::bind`]
    /// when the predecessor's delta releases. Workers wait on the bind
    /// before touching state; a consumer that will never bind must
    /// `abort_active` instead (the drain watchdog is the backstop).
    pub fn begin_block_deferred<'p>(
        &'p self,
        snapshots: Vec<S>,
        base: PendingDelta,
        env: ExecEnv,
        stats: &'p Stats,
    ) -> Result<(BlockSession<'p, 'a, S>, LayerBinder<S>), ExecutorError> {
        let (workers, prune_batch, bag_mode) = {
            let st = self.shared.0.lock().expect("pool poisoned");
            (st.cfg.workers, st.cfg.prune_batch, st.cfg.bag_scheduler)
        };
        assert!(
            snapshots.len() >= workers.max(1),
            "one state view per worker: {} given, {workers} needed",
            snapshots.len()
        );
        // RECYCLE (steady-state zero-allocation blocks): sweep parked
        // mv caches whose last layer reference has dropped, then pop
        // scrubbed structures instead of mapping fresh arenas.
        {
            let mut parked = self.recycle.mv_parked.lock().expect("pools poisoned");
            if !parked.is_empty() {
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
        }
        // O(1) reset of the feed's last-toucher index for this block.
        self.touch.borrow_mut().clear();
        let recycled = self.recycle.arenas.lock().expect("pools poisoned").pop();
        let recycled_mv = self.recycle.mv_clean.lock().expect("pools poisoned").pop();
        let (r_slots, r_results, r_nodes) = match recycled {
            Some(a) => (Some(a.slots), Some(a.results), Some(a.nodes)),
            None => (None, None, None),
        };
        let ctx = Arc::new(BlockCtx {
            env,
            snapshots,
            base,
            binding: std::sync::OnceLock::new(),
            mv: recycled_mv
                .map(Arc::new)
                .unwrap_or_else(|| Arc::new(MvCache::new())),
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
            bag: crossbeam_queue::ArrayQueue::new(MAX_BLOCK_TXS),
            bag_mode,
            queues: (0..workers)
                .map(|_| WorkerQueue {
                    q: Mutex::new(std::collections::VecDeque::new()),
                    len: std::sync::atomic::AtomicUsize::new(0),
                    cv: Condvar::new(),
                    parked: AtomicBool::new(false),
                })
                .collect(),
            // Per-BLOCK arena (with pipelined admission two blocks are
            // alive at once, so ONE pool-shared arena would alias) —
            // but RECYCLED through the reaper's scrub, so steady state
            // allocates none.
            nodes: Arc::new(
                r_nodes.unwrap_or_else(|| (0..MAX_BLOCK_TXS).map(|_| Node::default()).collect()),
            ),
            admitted: AtomicU32::new(0),
            finished: AtomicU32::new(0),
            sealed: AtomicBool::new(false),
            completed: (0..workers).map(|_| Mutex::new(Vec::new())).collect(),
            completed_len: (0..workers).map(|_| PaddedLen::default()).collect(),
            pending: std::sync::atomic::AtomicU64::new(0),
            prune_batch: prune_batch.max(1),
            started: std::time::Instant::now(),
            steal_enabled: {
                let avg = self.avg_tx_ns.load(Ordering::Relaxed);
                // Unknown (first block of a pool): allow it, and let the
                // measurement correct course from the next block on.
                avg == 0 || avg >= STEAL_WORTH_NS
            },
            spin_ns: {
                // Bridge roughly one link-release gap (~one tx), bounded:
                // spinning a full core for more than ~60us of silence is
                // waste, and below ~5us the spin cannot outlast even a
                // fast release.
                let avg = self.avg_tx_ns.load(Ordering::Relaxed);
                if avg == 0 {
                    20_000
                } else {
                    avg.clamp(5_000, 60_000)
                }
            },
            done_cv: Condvar::new(),
            aborted: AtomicBool::new(false),
            double_exit: AtomicU32::new(0),
            metrics: Metrics {
                // fetch_min seeds from the top.
                first_dispatch_ns: std::sync::atomic::AtomicU64::new(u64::MAX),
                busy_per_worker: (0..workers).map(|_| PaddedLen64::default()).collect(),
                ..Default::default()
            },
        });
        {
            let mut st = self.shared.0.lock().expect("pool poisoned");
            // Pipeline depth cap = 2: one block executing (`ctx`), one
            // staged (`next`). Wait only when BOTH are occupied, bounded
            // by the drain watchdog.
            let deadline = std::time::Instant::now() + STALL_TIMEOUT;
            while st.next.is_some() {
                if std::time::Instant::now() > deadline {
                    return Err(ExecutorError::State(
                        "stm pool: previous blocks never released the slots".into(),
                    ));
                }
                let (back, _) = self
                    .shared
                    .1
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
        }
        self.shared.1.notify_all();
        let sess = BlockSession {
            pool: self,
            ctx,
            stats,
            workers,
            cold: 0,
            edges: 0,
            preds_buf: Vec::with_capacity(8),
            stage_timing: std::env::var_os("KARDAMOM_STM_FEED_STAGES").is_some(),
            last_barrier: None,
            dispatch: vec![0; workers],
            n_txs: 0,
            started: std::time::Instant::now(),
        };
        let binder = LayerBinder {
            ctx: Arc::downgrade(&sess.ctx),
        };
        Ok((sess, binder))
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
        if !self.parallel_worth_it() {
            return self.decline(&snapshots[0], base, env, txs);
        }
        let mut sess = self.begin_block_per_worker(snapshots, base, env, stats)?;
        for ((t, p, e), prep) in txs.iter().zip(prepared) {
            sess.push_prepared(*t, *p, e.clone(), prep)?;
        }
        sess.seal()
    }

    /// Return a settled release's delta shell for reuse: the fold's
    /// PendingDelta hashmap tables are ~1MB per block, one of the huge
    /// per-block allocations the size histogram exposed. The consumer
    /// calls this once a release's Arc unwraps (after advance_base);
    /// entries drop here, tables keep their capacity.
    pub fn recycle_delta(&self, mut d: PendingDelta) {
        d.accounts.clear();
        d.storage.clear();
        d.code.clear();
        let mut g = self.recycle.deltas.lock().expect("pools poisoned");
        if g.len() < 4 {
            g.push(d);
        }
    }

    /// Abort the executing block and any staged successor: workers
    /// stop at their next dispatch check, the drain completes on the
    /// abort flag, and the affected tickets resolve (to an error when
    /// txs were left unexecuted). The P3b wound-abort path: whoever
    /// layered a block on a delta that a `corrected` release later
    /// invalidated calls this to hurry the stale block out, then
    /// rebuilds and resubmits from retained inputs — and must DISCARD
    /// the stale ticket's outcome either way (a small block may finish
    /// on stale layers before the flag lands; its bytes are garbage).
    pub fn abort_active(&self) {
        let st = self.shared.0.lock().expect("pool poisoned");
        for c in st.ctx.iter().chain(st.next.iter()) {
            c.aborted.store(true, Ordering::SeqCst);
            for q in &c.queues {
                q.cv.notify_all();
            }
            c.done_cv.notify_all();
        }
        drop(st);
        self.shared.1.notify_all();
    }

    /// Mirror a committed delta into the pool-lifetime backend cache.
    /// Call AFTER the state writer applies the same delta; skipping the
    /// call leaves stale entries and produces wrong reads — the harness's
    /// byte-identical assertion is the guard.
    pub fn advance_base(&self, delta: &PendingDelta) {
        // ONE write-lock per touched shard, not one per entry: the
        // per-entry version acquired ~12k write locks against executing
        // workers' read locks and measured as a GROWING multi-ms drag on
        // the pipeline loop (and stretched the executing block's span by
        // slowing its reads). Group first, lock once.
        let mut acc_by_shard: Vec<Vec<(alloy_primitives::Address, Option<AccountInfo>)>> =
            (0..BASE_SHARDS).map(|_| Vec::new()).collect();
        for (addr, (nonce, balance, code_hash)) in delta.accounts.iter() {
            acc_by_shard[BaseCache::shard(addr)].push((
                *addr,
                Some(AccountInfo {
                    nonce: *nonce,
                    balance: *balance,
                    code_hash: if *code_hash == B256::ZERO {
                        revm::primitives::KECCAK_EMPTY
                    } else {
                        *code_hash
                    },
                    account_id: None,
                    code: None,
                }),
            ));
        }
        for (sh, entries) in acc_by_shard.into_iter().enumerate() {
            if entries.is_empty() {
                continue;
            }
            let mut m = self.base_cache.accounts[sh]
                .write()
                .expect("base cache poisoned");
            for (k, v) in entries {
                m.insert(k, v);
            }
        }
        let mut sto_by_shard: Vec<Vec<((alloy_primitives::Address, B256), U256)>> =
            (0..BASE_SHARDS).map(|_| Vec::new()).collect();
        for ((addr, key), value) in delta.storage.iter() {
            sto_by_shard[BaseCache::shard(addr)].push(((*addr, *key), *value));
        }
        for (sh, entries) in sto_by_shard.into_iter().enumerate() {
            if entries.is_empty() {
                continue;
            }
            let mut m = self.base_cache.storage[sh]
                .write()
                .expect("base cache poisoned");
            for (k, v) in entries {
                m.insert(k, v);
            }
        }
        for (hash, code) in delta.code.iter() {
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
    /// Uses the mean per-tx execution time learned from previous blocks —
    /// the same statistic the stealing policy runs on. A fresh pool has no
    /// measurement yet and is given the benefit of the doubt; one block is
    /// enough to correct course.
    fn parallel_worth_it(&self) -> bool {
        let avg = self.avg_tx_ns.load(Ordering::Relaxed);
        avg == 0 || avg >= self.parallel_worth_ns
    }

    /// Run the block on this thread, through the SAME code path the
    /// sequential executor uses — not a reimplementation of it.
    fn decline(
        &self,
        snapshot: &S,
        base: PendingDelta,
        env: ExecEnv,
        txs: &[(TxIndex, BPosition, TxEnvelope)],
    ) -> Result<StmOutcome, ExecutorError> {
        let started = std::time::Instant::now();
        let (receipts, delta) = execute_block_sequential(snapshot, Some(&base), env, txs)?;
        // KEEP MEASURING while declining. Without this the gate is a trap
        // door: `avg_tx_ns` would hold the value that caused the decline
        // forever, and a pool that once saw cheap transfers would refuse
        // to parallelize a heavy contract block later in the same run.
        // Sequential per-tx cost slightly OVERSTATES the pool's own (it
        // hashes each write set inline, which the pool defers to its
        // parallel commit phase), so the bias is toward re-entering
        // parallel execution rather than staying out.
        if !txs.is_empty() {
            self.avg_tx_ns.store(
                started.elapsed().as_nanos() as u64 / txs.len() as u64,
                Ordering::Relaxed,
            );
        }
        Ok(StmOutcome {
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
        })
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
        if !self.parallel_worth_it() {
            return self.decline(&snapshots[0], base, env, txs);
        }
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
        let i = self.n_txs;
        if i >= MAX_BLOCK_TXS {
            return Err(ExecutorError::State(format!(
                "stm pool: block exceeds MAX_BLOCK_TXS={MAX_BLOCK_TXS} (gas-limit math says impossible)"
            )));
        }
        let idx = i as u32;
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

        // Domain -> worker by HASH, deliberately. Round-robin on first
        // sight was tried and REVERTED: it spread domains exactly (the
        // busiest thread's share fell 1.96x -> 1.50x) yet cost 9% wall,
        // because hashing is STABLE ACROSS BLOCKS — a pool returns to the
        // same worker every block, keeping its state warm in that core's
        // caches — while first-seen ordering reshuffles the assignment
        // each block. Locality beat balance.
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
        // BAG MODE: no owner, so no assignment at all — the whole
        // hash/sticky block below was 0.68µs of serial feed per tx
        // (measured: the largest single feed stage) computing a value
        // the bag never reads.
        let worker = if self.ctx.bag_mode {
            0
        } else {
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
                        let load = self.pool.assign_load.borrow();
                        let w = (0..self.workers).min_by_key(|w| load[*w]).unwrap_or(hashed);
                        drop(load);
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
        };

        // The slot OWNS the envelope: no clone, no parallel vec (the
        // repair path reads slots too).
        self.ctx.slots[i]
            .set(TxSlot {
                tx_idx,
                position,
                envelope,
                decoded,
            })
            .unwrap_or_else(|_| unreachable!("slot set once per index"));
        self.n_txs += 1;
        self.dispatch[worker] += 1;
        // Stage timers are OPT-IN: two extra clock reads per tx measured
        // ~5% of the serial feed, and the feed is the thing they measure.
        let t_pre_end = if self.stage_timing {
            let t = std::time::Instant::now();
            self.ctx
                .metrics
                .feed_pre_ns
                .fetch_add((t - t_feed).as_nanos() as u64, Ordering::Relaxed);
            Some(t)
        } else {
            None
        };

        // (2) Update the live DAG + (3) dispatch if ready.
        // Candidate predecessors come from the FEED-OWNED last-toucher
        // index — no lock needed, because admission is single-threaded and
        // prune never reads it.
        let mut preds = std::mem::take(&mut self.preds_buf);
        preds.clear();
        if let Some(b) = self.last_barrier {
            preds.push(b);
        }
        {
            let mut touch = self.pool.touch.borrow_mut();
            if is_cold {
                // ⊤: conflicts with everything — every outstanding tx is
                // a candidate predecessor, and this tx becomes the
                // barrier.
                preds.clear();
                preds.extend(0..idx);
                self.last_barrier = Some(idx);
                touch.clear();
            } else {
                // Hashes came from `prepare`, off this thread.
                for h in &hashes {
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
            self.ctx
                .metrics
                .feed_dag_ns
                .fetch_add((t_admit - t_pre_end).as_nanos() as u64, Ordering::Relaxed);
        }
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
            if !self.ctx.bag_mode {
                // FIFO-scheduler state: the bag has no queue position to
                // record and no take-time verification to feed, so this
                // second mutex (per tx, on the serial feed) is pure
                // ceremony there.
                node.queued.store(false, Ordering::Release);
                node.fifo_preds.lock().expect("fifo_preds poisoned").clear();
            }
            node.open.store(true, Ordering::Release);
        }
        self.ctx.admitted.fetch_add(1, Ordering::SeqCst);
        let mut deg = 0u32;
        let mut covered = 0u64;
        let eager = self.pool.eager_chain;
        for &p in preds.iter() {
            if p == idx {
                continue;
            }
            let pn = &self.ctx.nodes[p as usize];
            let mut list = pn.children.lock().expect("children poisoned");
            if pn.open.load(Ordering::Acquire) {
                // p is unfinished. If it was already RELEASED to this
                // tx's own queue (indegree 0 is definitive: admission is
                // serial, so p's guard was dropped long ago and a
                // released node is never re-blocked), FIFO position
                // orders it — record for take-time verification instead
                // of an edge, and the whole prune hand-off for this link
                // disappears. A stale read of a nonzero indegree only
                // costs an edge, never correctness.
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
                    // Increment BEFORE publishing the edge: the matching
                    // decrement can only happen once this child is
                    // visible in p's list, so the add always precedes
                    // its own subtract.
                    self.ctx.nodes[i].indegree.fetch_add(1, Ordering::AcqRel);
                    list.push(idx);
                    deg += 1;
                    if pn.worker.load(Ordering::Acquire) == worker
                        && pn.queued.load(Ordering::Acquire)
                    {
                        self.ctx
                            .metrics
                            .redundant_edges
                            .fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            // else: p already finished and published — no edge needed.
        }
        self.edges += deg as usize;
        if covered > 0 {
            self.ctx
                .metrics
                .fifo_covered
                .fetch_add(covered, Ordering::Relaxed);
        }
        self.preds_buf = preds; // scratch back for the next tx

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
        self.submit()?.wait()
    }

    /// Hand this block to the persistent tail thread and return
    /// immediately (spec P3a). The pool slot frees once execution
    /// drains, so the caller may begin feeding the NEXT block while this
    /// one validates and commits on the tail thread.
    pub fn submit(self) -> Result<BlockTicket, ExecutorError> {
        let BlockSession {
            pool,
            ctx,
            n_txs,
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
                delta_out: None,
            })
            .map_err(|_| ExecutorError::State("stm pool: tail thread gone".into()))?;
        Ok(BlockTicket { rx })
    }

    /// `submit`, plus a streaming delta release (spec P3): the tail
    /// sends this block's folded delta on `delta_tx` as soon as it
    /// exists — at the fold, before validation, when `speculative`
    /// (P3b); after the verdict when not (P3a). On a wound the tail
    /// sends a second, `corrected` release; the consumer must abort
    /// anything layered on the first.
    /// `submit_streaming` speculative, plus the EARLY mv release (spec
    /// P3b mv-as-layer): `mv_tx` receives this block's multi-version
    /// cache right after drain + extract — the earliest point a
    /// successor can bind on — and `delta_tx` still receives the folded
    /// delta (for base-cache advancement and writer settlement), plus
    /// the `corrected` re-issue on a wound.
    pub fn submit_streaming_mv(
        self,
        mv_tx: std::sync::mpsc::Sender<MvRelease>,
        delta_tx: std::sync::mpsc::Sender<DeltaRelease>,
    ) -> Result<BlockTicket, ExecutorError> {
        let BlockSession {
            pool,
            ctx,
            n_txs,
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
                delta_out: Some(DeltaOut {
                    tx: delta_tx,
                    mv_tx: Some(mv_tx),
                    speculative: true,
                }),
            })
            .map_err(|_| ExecutorError::State("stm pool: tail thread gone".into()))?;
        Ok(BlockTicket { rx })
    }

    pub fn submit_streaming(
        self,
        delta_tx: std::sync::mpsc::Sender<DeltaRelease>,
        speculative: bool,
    ) -> Result<BlockTicket, ExecutorError> {
        let BlockSession {
            pool,
            ctx,
            n_txs,
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
                delta_out: Some(DeltaOut {
                    tx: delta_tx,
                    mv_tx: None,
                    speculative,
                }),
            })
            .map_err(|_| ExecutorError::State("stm pool: tail thread gone".into()))?;
        Ok(BlockTicket { rx })
    }
}

/// The BLOCK TAIL: everything after the pool is released — extraction,
/// validation, wound repair, the canonical commit, learning, teardown
/// hand-off. Standalone so P3's persistent tail thread can own it; the
/// block-at-a-time path calls it inline (seal = drain + tail).
#[allow(clippy::too_many_arguments)]
fn block_tail<S: StateDatabase + Sync>(
    mut ctx: BlockCtx<S>,
    n_txs: usize,
    t_exec_wall: std::time::Duration,
    t_drain: std::time::Duration,
    cold: usize,
    edges: usize,
    dispatch: Vec<u32>,
    keep_hot: bool,
    tail_on_workers: bool,
    pin_cores: &[usize],
    reaper: &std::sync::mpsc::Sender<Reap>,
    avg_tx_ns: &std::sync::atomic::AtomicU64,
    delta_out: Option<DeltaOut>,
    recycle: &std::sync::Arc<RecyclePools>,
) -> Result<StmOutcome, ExecutorError> {
    let t_extract0 = std::time::Instant::now();
    let aborted = ctx.aborted.load(Ordering::SeqCst);
    let n = n_txs;
    let mut tx_results = recycle
        .carcasses
        .lock()
        .expect("pools poisoned")
        .pop()
        .unwrap_or_else(|| Vec::with_capacity(n));
    // take() IN PLACE: the spent OnceLock array keeps its buffer and
    // recycles through the reaper (steady-state zero-allocation blocks).
    for cell in ctx.results.iter_mut().take(n) {
        match cell.take() {
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

    let t_extract = t_extract0.elapsed();
    // EARLY RELEASE (spec P3b mv-as-layer): the mv cache's top version
    // per cell IS the final delta — before any fold ran. Ship it now,
    // with the fee sink materialized alongside (never published to
    // mv). Pre-verdict by construction: a wound invalidates it through
    // the corrected DeltaRelease that follows the repair.
    if let Some(DeltaOut {
        mv_tx: Some(mv_tx), ..
    }) = &delta_out
    {
        let b0 = ctx.binding.get().expect("layers bound before execution");
        let mut fee_sum = U256::ZERO;
        for r in tx_results.iter() {
            fee_sum += r.fee_delta;
        }
        let sink_final = match &b0.sink_start {
            Some(a) => {
                let mut a = a.clone();
                a.balance = b0.sink_start_balance + fee_sum;
                Some(a)
            }
            None if fee_sum > U256::ZERO => Some(AccountInfo {
                nonce: 0,
                balance: fee_sum,
                code_hash: revm::primitives::KECCAK_EMPTY,
                account_id: None,
                code: None,
            }),
            None => None,
        };
        let _ = mv_tx.send(MvRelease {
            block: ctx.env.block_number,
            mv: ctx.mv.clone(),
            sink_final,
        });
    }

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
    let mut sink_running = ctx
        .binding
        .get()
        .expect("layers bound before execution")
        .sink_start_balance;
    // FAST-PATH PREFIX — runs before the validation verdict exists.
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
    // materialization. Safe before the verdict: the repair path's
    // kept-prefix arm performs these exact mutations itself
    // (idempotent), and re-executed txs are rebuilt from scratch.
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
    // Fold + hash + VALIDATION, one overlap scope (fusing hash and
    // fold measured worse twice; OVERLAP won — validation now joins
    // the same scope): the fold builds the delta; each hash lane
    // hashes its chunk into the side array, then VALIDATES the same
    // chunk (read-only replay against the multi-version cache — every
    // recorded read must still be the highest version below the
    // reader; a conviction is a WOUND). Validation was its own phase
    // before the commit; hiding it under the fold, the longest pole,
    // removes it from the wall.
    //
    // The fold joins FIRST: the delta exists at that point — the
    // streaming release point (spec P3) — while hash+validate lanes
    // are still running.
    let t_h = std::time::Instant::now();
    let n_res = tx_results.len();
    let tail_pins: &[usize] = if keep_hot && tail_on_workers {
        pin_cores
    } else {
        &[]
    };
    let hash_threads = ctx.queues.len().saturating_sub(1).max(1).min(n_res.max(1));
    let chunk = n_res.div_ceil(hash_threads);
    let mut hashes: Vec<B256> = vec![B256::ZERO; n_res];
    let val_ns = std::sync::atomic::AtomicU64::new(0);
    let fold_inline = |results: &[TxResult]| -> PendingDelta {
        let mut sink_final: Option<(u64, U256, B256)> = None;
        let mut d = recycle
            .deltas
            .lock()
            .expect("pools poisoned")
            .pop()
            .unwrap_or_default();
        d.accounts.reserve(results.len() * 2);
        d.storage.reserve(results.len());
        for r in results.iter() {
            for (a, v) in r.ws.accounts.iter() {
                if *a == FEE_SINK {
                    sink_final = Some(*v);
                } else {
                    d.accounts.insert(*a, *v);
                }
            }
            for (k, v) in r.ws.storage.iter() {
                d.storage.insert(*k, *v);
            }
            for (h, b) in r.ws.code.iter() {
                d.code.insert(*h, b.clone());
            }
        }
        if let Some(v) = sink_final {
            d.accounts.insert(FEE_SINK, v);
        }
        d
    };
    // The mv pipeline runs the tail SEQUENTIALLY on this one thread:
    // its latency hides behind the NEXT block's execution (the early
    // release already shipped), so the goal is not speed but QUIET —
    // four parallel lanes of keccak + hashmap builds co-running with
    // the executing span measured +19% worker busy (memory bandwidth
    // + the two shared caller cores; see the topology note in the
    // bench). Validation runs FIRST so a wound skips the wasted fold
    // and hash entirely.
    // Pipeline tails run their lanes on the CALLER cores (tail_on_workers
    // is false there), so parallelism costs the executing block nothing
    // but memory bandwidth. Fully serial was the first cut — fine when
    // the tail is much shorter than the span it hides behind, wrong when
    // it is not (micro-tx blocks: serial tail ~7ms vs span ~5ms, so the
    // TAIL becomes the pacer). Opt back in with KARDAMOM_STM_SERIAL_TAIL.
    let serial_tail = delta_out.as_ref().is_some_and(|d| d.mv_tx.is_some())
        && std::env::var_os("KARDAMOM_STM_SERIAL_TAIL").is_some();
    let (delta_arc, wounded): (std::sync::Arc<PendingDelta>, Vec<usize>) = if serial_tail {
        let t0 = std::time::Instant::now();
        let wounded: Vec<usize> = tx_results
            .iter()
            .enumerate()
            .filter(|(i, r)| r.reads.iter().any(|rec| !ctx.mv.validate(*i as u32, rec)))
            .map(|(i, _)| i)
            .collect();
        val_ns.fetch_add(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
        if wounded.is_empty() {
            let delta_arc = std::sync::Arc::new(fold_inline(&tx_results));
            if let Some(DeltaOut {
                tx,
                speculative: true,
                ..
            }) = &delta_out
            {
                let _ = tx.send(DeltaRelease {
                    block: ctx.env.block_number,
                    delta: delta_arc.clone(),
                    corrected: false,
                });
            }
            for (i, r) in tx_results.iter().enumerate() {
                if r.sink_touched {
                    hashes[i] = r.ws.hash();
                }
            }
            (delta_arc, wounded)
        } else {
            // Fold and hash skipped: the repair path rebuilds both, and
            // the corrected release follows it.
            (std::sync::Arc::new(PendingDelta::new()), wounded)
        }
    } else {
        std::thread::scope(|sc| {
            // Fold thread: read-only copy, plain upserts in canonical
            // order ("later tx wins" is the map's own semantics), on
            // the last worker core (hot, parked, longest pole). Code
            // entries are COPIED (Bytes is refcounted), not drained:
            // the released delta must carry CREATEd code for the next
            // block's readers.
            let results_ref: &[TxResult] = &tx_results;
            let warm_delta = recycle
                .deltas
                .lock()
                .expect("pools poisoned")
                .pop()
                .unwrap_or_default();
            // The fold runs on THIS thread (the tail thread, on a caller
            // core): it frees a worker core for a hash lane, drops a
            // spawn, and reaches the release point without a join.
            let fold = move || {
                let mut sink_final: Option<(u64, U256, B256)> = None;
                let mut d = warm_delta;
                d.accounts.reserve(results_ref.len() * 2);
                d.storage.reserve(results_ref.len());
                for r in results_ref.iter() {
                    for (a, v) in r.ws.accounts.iter() {
                        if *a == FEE_SINK {
                            sink_final = Some(*v);
                        } else {
                            d.accounts.insert(*a, *v);
                        }
                    }
                    for (k, v) in r.ws.storage.iter() {
                        d.storage.insert(*k, *v);
                    }
                    for (h, b) in r.ws.code.iter() {
                        d.code.insert(*h, b.clone());
                    }
                }
                if let Some(v) = sink_final {
                    d.accounts.insert(FEE_SINK, v);
                }
                d
            };
            // Hash+validate lanes: read ws, write ONLY the side array
            // and a local wounded list.
            let hash_parts: Vec<(usize, &mut [B256])> = {
                let mut out = Vec::new();
                let mut rest: &mut [B256] = &mut hashes;
                let mut base = 0usize;
                while rest.len() > chunk {
                    let (head, tail_s) = rest.split_at_mut(chunk);
                    out.push((base, head));
                    base += chunk;
                    rest = tail_s;
                }
                out.push((base, rest));
                out
            };
            let mut lanes = Vec::with_capacity(hash_parts.len());
            for (ti, (base, out_slice)) in hash_parts.into_iter().enumerate() {
                let results_ref: &[TxResult] = &tx_results;
                let mv = &ctx.mv;
                let val_ns = &val_ns;
                let lane_metrics = &ctx.metrics;
                lanes.push(sc.spawn(move || {
                    if !tail_pins.is_empty() {
                        let _ = core_affinity::set_for_current(core_affinity::CoreId {
                            id: tail_pins[ti % tail_pins.len().max(1)],
                        });
                    }
                    let t_lane0 = std::time::Instant::now();
                    let len = out_slice.len();
                    for (j, slot) in out_slice.iter_mut().enumerate() {
                        let r = &results_ref[base + j];
                        if r.sink_touched {
                            *slot = r.ws.hash();
                        }
                    }
                    let t0 = std::time::Instant::now();
                    let mut wounded_local = Vec::new();
                    for j in 0..len {
                        let i = base + j;
                        if results_ref[i]
                            .reads
                            .iter()
                            .any(|rec| !mv.validate(i as u32, rec))
                        {
                            wounded_local.push(i);
                        }
                    }
                    val_ns.fetch_add(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
                    lane_metrics
                        .commit_lane_ns
                        .fetch_add(t_lane0.elapsed().as_nanos() as u64, Ordering::Relaxed);
                    wounded_local
                }));
            }
            let delta_arc = std::sync::Arc::new(fold());
            // SPECULATIVE RELEASE (spec P3b): the delta ships here,
            // while validation is still running. Measured wound rate
            // across the campaign: zero — the gamble prices only when
            // it fires, and a `corrected` re-issue follows if it does.
            if let Some(DeltaOut {
                tx,
                speculative: true,
                ..
            }) = &delta_out
            {
                let _ = tx.send(DeltaRelease {
                    block: ctx.env.block_number,
                    delta: delta_arc.clone(),
                    corrected: false,
                });
            }
            let wounded: Vec<usize> = lanes
                .into_iter()
                .flat_map(|h| h.join().expect("hash+validate lane"))
                .collect();
            (delta_arc, wounded)
        })
    };
    hash_ns += t_h.elapsed().as_nanos() as u64;
    let t_validate = std::time::Duration::from_nanos(val_ns.load(Ordering::Relaxed));
    let wounds = wounded.len();
    let mut wounded_set: HashSet<usize> = wounded.into_iter().collect();
    // A tx after a re-executed one may also be stale: once ANY wound
    // fires, later txs are re-checked against the live prefix.
    if let Some(first) = wounded_set.iter().copied().min() {
        for i in first..n {
            wounded_set.insert(i);
        }
    }
    if wounds == 0 {
        let t_d = std::time::Instant::now();
        // The consumer may still hold the released Arc — clone then
        // (pipeline mode); a sole owner unwraps for free
        // (block-at-a-time, no release).
        delta = std::sync::Arc::try_unwrap(delta_arc).unwrap_or_else(|a| (*a).clone());
        // Conservative release (spec P3a): only now, when the delta
        // can no longer change.
        if let Some(DeltaOut {
            tx,
            speculative: false,
            ..
        }) = &delta_out
        {
            let _ = tx.send(DeltaRelease {
                block: ctx.env.block_number,
                delta: std::sync::Arc::new(delta.clone()),
                corrected: false,
            });
        }
        // Serial epilogue: patch receipt hashes in place, move receipts
        // out, then ship the WHOLE results vec (write sets + read logs)
        // to the reaper as one move — a per-element carcass copy here
        // measured 10-13ms of pure memmove and ate the overlap's win.
        for (i, r) in tx_results.iter_mut().enumerate() {
            if r.sink_touched {
                r.receipt.write_set_hash = hashes[i];
            }
            receipts.push(std::mem::take(&mut r.receipt));
        }
        reaper
            .send(Reap::Results {
                results: tx_results,
                pools: recycle.clone(),
            })
            .ok();
        delta_ns += t_d.elapsed().as_nanos() as u64;
    } else {
        // A speculative release (if any) was WRONG: drop our Arc and
        // rebuild the delta on the repair path; a `corrected` release
        // follows the repair.
        drop(delta_arc);
        // Phase 1 already ran over these results; the kept-prefix arm
        // below recomputes the same values, so restart the running
        // sums from zero.
        cumulative = 0;
        sink_running = ctx
            .binding
            .get()
            .expect("layers bound before execution")
            .sink_start_balance;
        // REPAIR PATH: a wound fired, so txs from the first wound on
        // re-execute against the exact materialized prefix. Strictly
        // sequential by nature, and rare enough that its cost is not
        // worth optimizing.
        //
        // The prefix starts from the FULL pre-block view: unsettled
        // predecessor LAYERS (oldest first — `MvView` probes them
        // newest-first over `base`, so base is the bottom) merged over
        // the owned base delta. Dropping the layers here read
        // pre-predecessor state into re-executed txs — found by the
        // P3b adversarial pipeline test (rejected receipts from stale
        // nonces), latent since the layers landed: the bench scenarios
        // never wound.
        let mut layered = ctx.base.clone();
        {
            let b = ctx.binding.get().expect("layers bound before execution");
            for l in b.layers.iter().rev() {
                layered.merge_from(l);
            }
            // mv layers are NEWER than the delta layers (probed first on
            // the read path), so they merge last; `final_delta` is the
            // fold-shaped materialization — rare path, fold cost.
            for mv in b.mv_layers.iter().rev() {
                layered.merge_from(&mv.final_delta());
            }
        }
        for (i, mut r) in tx_results.into_iter().enumerate() {
            if wounded_set.contains(&i) {
                // The slot holds the envelope for the whole block — no
                // second copy in a parallel vec (that clone was 0.1µs of
                // serial feed per tx).
                let slot = ctx.slots[i].get().expect("slot set for every admitted tx");
                let (tx_idx, position, envelope) = (slot.tx_idx, slot.position, &slot.envelope);
                let mut scope = ExecScope::new(&ctx.snapshots[0], Some(&layered), ctx.env)?;
                let (mut receipt, ws) = scope
                    .execute_tx(tx_idx, position, envelope, i as u64, cumulative, None, None)?;
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
        // CORRECTED release: whoever consumed the speculative delta
        // must unwind onto this one (spec P3b wound-abort). Sent in
        // conservative mode too — it is simply the first release then.
        if let Some(d) = &delta_out {
            let _ = d.tx.send(DeltaRelease {
                block: ctx.env.block_number,
                delta: std::sync::Arc::new(delta.clone()),
                corrected: d.speculative,
            });
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
            "phase block={} n={} feed+exec={:?} drain={:?} extract={:?} validate={:?} commit={:?} wounds={}",
            ctx.env.block_number, n, t_exec_wall, t_drain, t_extract, t_validate, t_commit, wounds
        );
    }
    // Destructure: the HEAVY parts (multi-version cache with ~n
    // versions, a thousand tx slots) go to the reaper; the light rest
    // drops here. `S` (the snapshots) stays inline, so no 'static
    // bound is needed on the payload.
    let BlockCtx {
        mv,
        slots,
        results,
        nodes,
        metrics,
        ..
    } = ctx;
    reaper
        .send(Reap::Arena {
            slots,
            results,
            nodes,
            mv,
            pools: recycle.clone(),
        })
        .ok();
    let m = &metrics;
    // Feed the stealing policy: mean per-tx execution time this block.
    if n > 0 {
        avg_tx_ns.store(
            m.busy_ns.load(Ordering::Relaxed) / n as u64,
            Ordering::Relaxed,
        );
    }
    let prune_calls = m.prune_calls.load(Ordering::Relaxed);
    let completions = m.completions.load(Ordering::Relaxed);
    Ok(StmOutcome {
        receipts,
        delta,
        wounds,
        fallback: wounds > 0,
        declined: false,
        learned_tx_ns: avg_tx_ns.load(Ordering::Relaxed),
        writes_own: m.writes_own.load(Ordering::Relaxed),
        writes_foreign: m.writes_foreign.load(Ordering::Relaxed),
        fifo_covered: m.fifo_covered.load(Ordering::Relaxed),
        fifo_stalls: m.fifo_stalls.load(Ordering::Relaxed),
        read_us: m.read_ns.load(Ordering::Relaxed) / 1_000,
        busy_per_worker_us: m
            .busy_per_worker
            .iter()
            .map(|c| c.0.load(Ordering::Relaxed) / 1_000)
            .collect(),
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
        commit_fold_us: m.commit_fold_ns.load(Ordering::Relaxed) / 1_000,
        commit_lane_us: m.commit_lane_ns.load(Ordering::Relaxed) / 1_000,
        feed_pre_us: m.feed_pre_ns.load(Ordering::Relaxed) / 1_000,
        feed_dag_us: m.feed_dag_ns.load(Ordering::Relaxed) / 1_000,
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

fn worker_loop<S: StateDatabase + Sync>(shared: &PoolShared<S>, worker: usize, keep_hot: bool) {
    let mut seen = 0u64;
    loop {
        let ctx = 'next: {
            loop {
                {
                    let mut st = shared.0.lock().expect("pool poisoned");
                    if !keep_hot {
                        loop {
                            if st.shutdown {
                                return;
                            }
                            if st.generation != seen
                                && let Some(c) = &st.ctx
                            {
                                seen = st.generation;
                                break 'next c.clone();
                            }
                            st = shared.1.wait(st).expect("pool poisoned");
                        }
                    }
                    if st.shutdown {
                        return;
                    }
                    if st.generation != seen
                        && let Some(c) = &st.ctx
                    {
                        seen = st.generation;
                        break 'next c.clone();
                    }
                }
                // keep_hot: hold the core's frequency, hand it over
                // instantly to whoever needs it (the commit tail pins
                // its threads here).
                for _ in 0..64 {
                    std::thread::yield_now();
                }
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
/// A worker (almost) never blocks on a dependency: a tx reaches a queue
/// once its EDGE indegree is zero, and its FIFO-covered predecessors sit
/// ahead of it in the same queue — verified at take time (`fifo_ready`),
/// which is what keeps a stolen predecessor from breaking the order. No
/// wait-graph deadlock is possible: the canonical total order bounds every
/// edge and every FIFO obligation (low index → high), and completion only
/// ever removes them.
fn run_worker_block<S: StateDatabase>(ctx: &BlockCtx<S>, worker: usize) {
    // LATE-BIND GATE (spec P3b): a deferred session's txs are admitted
    // and queued before its read base exists; nothing may execute until
    // the consumer binds the layers. The wait is bind latency (the
    // predecessor's fold), normally sub-millisecond; the block-at-a-time
    // path binds at session build, so this is one free load. A consumer
    // that never binds must abort; the tail's drain watchdog is the
    // loud backstop.
    let bound = loop {
        if let Some(b) = ctx.binding.get() {
            break b;
        }
        if ctx.aborted.load(Ordering::SeqCst) {
            return;
        }
        std::thread::yield_now();
    };
    let input = BlockInput {
        snapshot: &ctx.snapshots[worker % ctx.snapshots.len()],
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
    // Timing and read counts accumulate LOCALLY and flush once per block.
    // Per-read (and even per-tx) `fetch_add` on shared metrics had every
    // worker writing the same cache lines — instrumentation generating the
    // very cross-core traffic it was measuring.
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
        let job = {
            let mut q = qh.q.lock().expect("queue poisoned");
            loop {
                if ctx.aborted.load(Ordering::SeqCst) {
                    leave!(evm);
                }
                if ctx.bag_mode {
                    // Chain-local hand-off first (see complete_inline),
                    // then the shared bag. Bag entries are indegree-0-
                    // dispatched with coverage off ⇒ nothing to verify.
                    if let Some(i) = local_next.take() {
                        drop(q);
                        break i;
                    }
                    if let Some(i) = ctx.bag.pop() {
                        drop(q);
                        break i;
                    }
                } else if let Some(i) = q.pop_front() {
                    qh.len.fetch_sub(1, Ordering::Release);
                    // Verify with NO lock held — the check takes a node
                    // mutex and scans results, and holding the queue lock
                    // across it would block the feed's submissions.
                    drop(q);
                    if ctx.fifo_ready(i) {
                        break i;
                    }
                    // A FIFO predecessor was stolen and is still running
                    // on another thread — rare, and bounded by that tx's
                    // execution time. Put the head back and yield.
                    ctx.metrics.fifo_stalls.fetch_add(1, Ordering::Relaxed);
                    q = qh.q.lock().expect("queue poisoned");
                    q.push_front(i);
                    qh.len.fetch_add(1, Ordering::Release);
                    drop(q);
                    std::thread::yield_now();
                    q = qh.q.lock().expect("queue poisoned");
                    continue;
                }
                // Dry. Apply any parked completions MYSELF before parking:
                // this is what makes batching safe — the pool can never sit
                // idle on DAG updates nobody applied. The queue lock is
                // dropped first (lock order: never hold a queue lock while
                // taking the graph lock — and prune's push_ready re-locks
                // queues, so holding one here self-deadlocks).
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
                if ctx.steal_enabled
                    && let Some(stolen) = ctx.steal(worker)
                {
                    ctx.metrics.steals.fetch_add(1, Ordering::Relaxed);
                    break stolen;
                }
                q = qh.q.lock().expect("queue poisoned");
                if !q.is_empty() || (ctx.bag_mode && !ctx.bag.is_empty()) {
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
                let spin_start = std::time::Instant::now();
                loop {
                    for _ in 0..SPIN_BEFORE_PARK {
                        std::hint::spin_loop();
                    }
                    // Lock-free probe: a dry worker spinning here for tens
                    // of microseconds must not contend with the feed's
                    // push into this very queue.
                    if qh.len.load(Ordering::Acquire) > 0 || (ctx.bag_mode && !ctx.bag.is_empty()) {
                        spun = true;
                        break;
                    }
                    // Completions may be parked while we spin — apply them
                    // ourselves rather than spin past the work they would
                    // release.
                    if ctx.pending.load(Ordering::SeqCst) > 0 {
                        ctx.prune(true);
                        continue;
                    }
                    if ctx.drained() || ctx.aborted.load(Ordering::SeqCst) {
                        break;
                    }
                    if spin_start.elapsed().as_nanos() as u64 >= ctx.spin_ns {
                        break;
                    }
                }
                q = qh.q.lock().expect("queue poisoned");
                if !spun && q.is_empty() && (!ctx.bag_mode || ctx.bag.is_empty()) {
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
        let t_busy_at = ctx.started.elapsed().as_nanos() as u64;
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
            bound.sink_start_balance,
            &mut || take_read_buf(&mut read_stash, &ctx.recycle),
        );
        // Timestamps stay WORKER-LOCAL and fold once per block. Stamping
        // them globally cost two clock reads and two contended RMWs per
        // transaction — on a 2.7us transfer, the instrumentation was a
        // measurable share of the work it claimed to measure.
        let done_at = ctx.started.elapsed().as_nanos() as u64;
        let busy_ns = done_at.saturating_sub(t_busy_at);
        local_busy_ns += busy_ns;
        local_first_ns = local_first_ns.min(t_busy_at);
        local_last_ns = local_last_ns.max(done_at);
        // Census the write set against dispatch: an account whose domain
        // hashes to another worker can be written by more than one thread.
        if let Ok(res) = &r {
            for (addr, _) in res.ws.accounts.iter() {
                if *addr == FEE_SINK {
                    continue; // deferred, never published
                }
                if domain_hash(addr.as_slice(), n_workers) == worker {
                    local_own += 1;
                } else {
                    local_foreign += 1;
                }
            }
        }
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
        //
        // `pending` increments BEFORE the buffer push: prune runs
        // concurrently (another worker's batch, the tail's drain
        // loop) and subtracts what it drains — a completion visible
        // in a buffer before its increment landed underflowed the
        // counter (debug-build overflow panic; found by the P3b
        // adversarial test's abort storms). Incremented-but-unpushed
        // is the safe direction: a spurious prune drains nothing.
        if ctx.bag_mode {
            local_next = ctx.complete_inline(job);
        } else {
            let owed = ctx.pending.fetch_add(1, Ordering::SeqCst) + 1;
            {
                let mut b = ctx.completed[worker].lock().expect("completed poisoned");
                b.push(job);
                ctx.completed_len[worker].0.fetch_add(1, Ordering::Release);
            }
            if owed as usize >= ctx.prune_batch {
                ctx.prune(false);
            }
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
    // Diagnostic split, env-gated: how much of the sequential wall is
    // `execute_tx` itself vs the delta fold around it. Comparing engines
    // by their outer walls alone has misattributed overhead twice.
    let timing = std::env::var_os("KARDAMOM_SEQ_TIMING").is_some();
    let mut exec_ns = 0u64;
    for (i, (tx_idx, position, envelope)) in txs.iter().enumerate() {
        let t0 = timing.then(std::time::Instant::now);
        let (receipt, ws) = scope.execute_tx(
            *tx_idx, *position, envelope, i as u64, cumulative, None, None,
        )?;
        if let Some(t0) = t0 {
            exec_ns += t0.elapsed().as_nanos() as u64;
        }
        cumulative = receipt.cumulative_gas_used;
        delta.apply(ws);
        receipts.push(receipt);
    }
    if timing && !txs.is_empty() {
        eprintln!(
            "seq block {}: execute_tx sum {:.1}ms ({} txs)",
            env.block_number,
            exec_ns as f64 / 1e6,
            txs.len()
        );
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
/// Pop a cleared read-record buffer (batch-refilled from the recycle
/// pool, one lock per 64 txs); falls back to a fresh allocation while
/// the pool warms up.
fn take_read_buf(stash: &mut Vec<Vec<ReadRecord>>, pools: &RecyclePools) -> Vec<ReadRecord> {
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
    fresh_reads: &mut dyn FnMut() -> Vec<ReadRecord>,
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
    let mut outcome = match evm.transact(tx_env) {
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
    // Wire logs straight from the borrowed result — the intermediate
    // `logs.clone()` (topics Vecs + data Bytes per log, every success)
    // was allocation for its own sake.
    let (status, wire_logs) = match &outcome.result {
        ExecutionResult::Success { logs, .. } => {
            (ReceiptStatus::Success, logs.iter().map(wire_log).collect())
        }
        ExecutionResult::Revert { .. } => (ReceiptStatus::Revert, Vec::new()),
        ExecutionResult::Halt { reason, .. } => (ReceiptStatus::Halt(reason.clone()), Vec::new()),
    };

    let ws = write_set_from_evm_state(&outcome.state);
    // POOLED JOURNAL: revm's finalize mem::takes the state map out of
    // the journal into `outcome.state`, leaving a capacity-0 map behind
    // — every tx then regrew a fresh table (measured as the shared
    // per-tx allocation floor, ~2.5 allocs/tx in the <4K bucket). Hand
    // the SPENT table back: its entries drop here (they are tx-local by
    // contract — a stale entry would be read as a cached truth by the
    // next tx's load_account), its capacity survives, and the journal's
    // own entry vec / transient storage already clear in place.
    {
        let mut spent = std::mem::take(&mut outcome.state);
        spent.clear();
        revm::context_interface::ContextTr::journal_mut(&mut **evm)
            .inner
            .state = spent;
    }
    // Publish in the ordered helper's sequence (code+storage, THEN
    // accounts — see `MvCache::publish_write_set`), skipping the fee sink
    // (Accumulator: all workers see block-start; the commit pass
    // materializes the prefixes).
    let t_pub = std::time::Instant::now();
    mv.publish_write_set(local_idx, &ws, FEE_SINK);
    metrics
        .publish_ns
        .fetch_add(t_pub.elapsed().as_nanos() as u64, Ordering::Relaxed);
    // One scan answers both sink questions.
    let sink_entry = ws.accounts.iter().find(|(a, _)| *a == FEE_SINK);
    let sink_touched = sink_entry.is_some();
    let fee_delta = sink_entry
        .map(|(_, (_, b, _))| *b - sink_start_balance)
        .unwrap_or(U256::ZERO);

    let reads = {
        // Take this tx's read log back out of the worker's view. The
        // replacement comes from the RECYCLE pool (cleared, capacity
        // intact from a previous block's tx): the fresh
        // per-tx Vec + its growth reallocs were the largest
        // STM-specific allocation (~4KB/tx at contract weight).
        let db = revm::context_interface::ContextTr::db_mut(&mut **evm);
        std::mem::replace(&mut db.reads, fresh_reads())
    };
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
        logs: wire_logs,
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
