//! The parallel block executor (an offline milestone): workers pull
//! DAG-ready transactions, execute each against a per-transaction
//! multi-version view, and publish versions. A canonical-order commit
//! pass then computes the exact sequential artifacts: receipts
//! (cumulative gas, accumulator-fixed write-set hashes) and the block
//! `PendingDelta`. These are byte-identical to `Executor` output by
//! construction, and validation re-checks this.
//!
//! Wound-wait runtime detection (ESTIMATE marks, child self-abort) is not
//! here yet, by design. Under pessimistic scheduling, a conflict is an
//! ordered edge, so the miss classes this detection would accelerate are
//! expected almost never (the footprint shadow scheduler saw zero across
//! 18,400 graded transactions). Validation plus whole-block sequential
//! fallback (invariant #3) carries correctness alone at this stage. The
//! optimization lands later, once the A/B test shows where it pays off.

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};

use alloy_primitives::{B256, U256};
use kardamom_exec_core::block_env::ExecEnv;
use kardamom_exec_core::delta::{PendingDelta, WriteSet};
use kardamom_exec_core::error::ExecutorError;
use kardamom_exec_core::exec_types::{ReceiptStatus, TxIndex};
use kardamom_exec_core::executor::{DecodedTx, Executor, SnapshotRef};
use kardamom_footprint::classifier::{DomainKey, Stats};
use kardamom_types::{BPosition, Receipt, StateDatabase, TxEnvelope};
use revm::context::result::ExecutionResult;
use revm::database::DatabaseRef;
use revm::state::AccountInfo;
use revm::{Context, ExecuteEvm, MainBuilder, MainContext};

use crate::mv::{MvCache, ReadRecord};
use crate::schedule;
use crate::{FEE_SINK, FastMap};

/// Layered block-input view: the pre-block delta over the snapshot, what
/// sequential execution sees at the block's first transaction. Read-only
/// and shared by every worker.
pub struct BlockInput<'a, S: StateDatabase> {
    pub snapshot: &'a S,
    pub base: Option<&'a PendingDelta>,
    /// Unsettled predecessor deltas, newest first, probed before `base`.
    /// Arc-shared so pipelined submission builds a block's read layers
    /// without cloning or merging a single entry. The older merge-clone
    /// and advance step was a measured, growing multi-ms drag on the
    /// pipeline loop.
    pub layers: &'a [std::sync::Arc<PendingDelta>],
    /// Predecessor multi-version caches, newest first, probed before
    /// everything else: a drained block's mv top version per cell is
    /// its final delta, before any fold ran. Reads probe at
    /// `u32::MAX`. Immutable once the block drains; a wound
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

/// Shared read-through cache over the block-input layer.
///
/// The block input (the pre-block delta over the snapshot) is immutable
/// for the whole block, so every worker that misses the multi-version
/// cache asks the same questions and gets the same answers. Per-worker
/// memos made each thread answer them independently, which is why total
/// CPU time grew with worker count for the same set of transactions.
/// Sharing the answers is what turns extra threads into extra throughput
/// instead of extra work.
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

/// Per-transaction database view: the multi-version cache at index `idx`
/// over the block input, recording every first read for validation. The
/// fee sink is served from the cached block-start info and never recorded
/// (the `Accumulator` boundary). Its correctness comes from the commit
/// pass's prefix algebra, not from version validation.
struct MvView<'a, S: StateDatabase> {
    mv: &'a MvCache,
    base: &'a BlockInput<'a, S>,
    idx: u32,
    reads: Vec<ReadRecord>,
    sink_start: Option<AccountInfo>,
    /// Shared across workers. See [`BaseCache`].
    base_cache: &'a BaseCache,
    metrics: &'a Metrics,
    /// Read counters accumulated without atomics and flushed once per
    /// block. Incrementing shared atomics on every read had every worker
    /// hammering the same cache lines. The instrumentation was distorting
    /// the very contention it aimed to measure.
    n_reads: u64,
    n_mv_hit: u64,
    n_base_hit: u64,
    n_backend: u64,
    /// Wall nanoseconds inside the read path (basic, storage, code),
    /// split out of `evm_ns`. The flamegraph inlines these frames into
    /// the interpreter, so timing is the only way to see them.
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

    /// Fold this worker's counters into the shared metrics, once per
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
        // Probe predecessor mv layers first (newest first, at
        // `u32::MAX`, where the top version is the final value), then
        // pending-delta layers, then the base layer, all before the
        // cache. The pool-lifetime cache mirrors
        // the backend only, and these layers change per block.
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
        // Content-addressed: no version, no record. Memoize both
        // sources; Bytecode clones are refcounted, so the copy happens
        // once.
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
            // A miss is recorded too: if a concurrent CREATE publishes
            // this hash later, this transaction ran against absent code
            // and is wounded.
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

    // Thin timed wrappers: the read path inlines into the interpreter and
    // is invisible to a sampling profiler, so `n_read_ns` carves it out
    // of `evm_ns` by direct measurement. Two clock reads per state
    // access cost little against multi-microsecond questions.
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

/// One executed transaction's artifacts, before commit. The canonical-order
/// commit pass adds cumulative gas and the accumulator write-set-hash fixup.
struct TxResult {
    receipt: Receipt,
    ws: WriteSet,
    reads: Vec<ReadRecord>,
    /// EIP-7928 capture fragment: this transaction's BAL updates at its
    /// block-global index (`bal_base + local_idx + 1`), recorded through
    /// the same `Bal::update_account` the streaming path uses. `None`
    /// when capture is off, or the transaction was an invalid skip (a
    /// skip carries no fragment in either mode). The commit pass rewrites
    /// the fee-sink balance write to the computed prefix value (workers
    /// see the block-start sink, not the canonical running sum) and
    /// folds the fragments in canonical order.
    bal_frag: Option<revm::state::bal::Bal>,
    /// This transaction's exact credit to the fee sink (the value after,
    /// minus the block-start value seen).
    fee_delta: U256,
    /// The write set contains the fee sink, so its hash is finalized at
    /// commit, after the prefix balance is computed. Hashing it during
    /// execution would be wasted work. Offline measurement found this
    /// case at nearly every transaction, so the saved keccak is not an
    /// edge case.
    sink_touched: bool,
}

/// Outcome of one block through the STM engine.
#[derive(Default)]
pub struct StmOutcome {
    pub receipts: Vec<Receipt>,
    pub delta: PendingDelta,
    /// EIP-7928 capture: the block's per-transaction fragments folded in
    /// canonical order (see `merge_bal_fragments`), with the fee-sink
    /// writes computed to the canonical prefix and wounded transactions'
    /// fragments replaced by their repair capture. `Some` only when the
    /// session was opened with capture (`begin_block_layered_bal`).
    /// Wire-identical to the streaming path's sequential capture by
    /// construction.
    pub bal: Option<revm::state::bal::Bal>,
    /// Transactions wounded at validation: a conflict the marks missed,
    /// repaired by re-executing at the canonical position (per
    /// transaction; the whole block never re-runs). Zero on every
    /// measured workload so far.
    pub wounds: usize,
    /// Whether any wound fired (spec invariant #3's counter, per block).
    pub fallback: bool,
    /// The pool declined this block and ran it sequentially, because the
    /// work per transaction was too small for parallel execution to pay
    /// for its own coordination. See `PARALLEL_WORTH_NS`.
    pub declined: bool,
    /// Mean per-transaction execution time this block taught the pool:
    /// the input to the next block's decline decision. Non-zero after a
    /// declined block too. That is what keeps the gate from being a trap
    /// door.
    pub learned_tx_ns: u64,
    /// Account writes whose domain belongs to another worker.
    pub writes_own: u64,
    pub writes_foreign: u64,
    /// Chain links ordered by FIFO position instead of a DAG edge, and
    /// how often a taken transaction had to wait on a stolen FIFO
    /// predecessor.
    pub fifo_covered: u64,
    pub fifo_stalls: u64,
    pub read_us: u64,
    /// Per-worker busy microseconds. See `Metrics::busy_per_worker`.
    pub busy_per_worker_us: Vec<u64>,
    /// ⊤ (cold, untrained-selector) transactions. They wait out the
    /// prefix.
    pub cold: usize,
    /// Live-DAG edges created across the block (only against predecessors
    /// that were still outstanding at admission).
    pub edges: usize,
    /// Transactions dispatched per worker queue: the domain-affinity
    /// histogram.
    pub dispatch: Vec<u32>,
    /// Nodes observed leaving the graph more than once. Always zero.
    /// Asserted by the test suite and worth an alert in production: a
    /// non-zero value means a transaction completed twice and the edges
    /// registered in between were stranded.
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
    /// parallel_span_us)` is the honest core utilization. `ramp_us` and
    /// `commit_us` are the serial head and tail that no worker count
    /// reduces.
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

/// Stable domain-to-worker mapping. Its quality only affects balance
/// across threads, never correctness: ordering comes from the DAG's
/// edges, and an idle worker may steal from any queue.
fn domain_hash(bytes: &[u8], workers: usize) -> usize {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in bytes {
        h ^= *b as u64;
        h = h.wrapping_mul(0x100_0000_01b3);
    }
    (h % workers as u64) as usize
}

/// How long `seal` waits for a block to drain before it declares a
/// scheduler bug. Generous by orders of magnitude: a 30M-gas block takes
/// milliseconds to execute, so anything past this is a stranded edge,
/// not slow work.
const STALL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Everything about a transaction that can be derived without touching the
/// graph: the RLP decode and the footprint prediction. Both are pure
/// functions of the envelope bytes and the stats snapshot, so they do not
/// belong on the executor's single feed thread. The pipeline computes them
/// upstream, where the work is already sharded.
///
/// In the live executor, the M tx_data reader threads produce this.
/// They touch every envelope anyway and run before the canonical order
/// arrives (the join buffer exists precisely because tx_data leads
/// tx_ordering), so the work lands in slack that already exists.
#[derive(Clone)]
pub struct Prepared {
    /// `None` when the envelope does not decode: the #92 skip path.
    pub decoded: Option<DecodedTx>,
    /// Predicted contention domains, with the fee sink already excluded.
    pub domains: Vec<DomainKey>,
    /// A 64-bit hash per domain, computed here, off the feed thread, in
    /// parallel with every other transaction's preparation, so the
    /// serial feed only probes. See [`TouchTable`] for why hashing by
    /// value is sound.
    pub domain_hashes: Vec<u64>,
    /// The domain that decides which thread runs this transaction.
    pub primary: Option<DomainKey>,
    /// ⊤: an untrained selector. Orders behind everything outstanding.
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

/// Decode and predict, off the feed thread. `stats` must be a snapshot
/// trained on prior blocks only (in the live executor, this is an
/// `Arc<Stats>` swapped at each boundary, so readers never take a lock).
///
/// Getting this wrong is not a correctness problem: a bad prediction
/// costs a mis-schedule, which surfaces as a wound and re-executes that
/// one transaction at its canonical position. That is what makes it safe
/// to compute here, concurrently, ahead of canonical order.
pub fn prepare(envelope: &TxEnvelope, tx_idx: TxIndex, stats: &Stats) -> Prepared {
    let decoded = DecodedTx::decode(&envelope.raw_tx, tx_idx).ok();
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
                // cell in canonical order. This is stable across
                // transactions of one flow, which is what puts a pool's
                // traffic on one thread. It falls back to the sender
                // cell, the SenderChain lane, for tier-1-only
                // transactions.
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
/// when a block really has drained.
/// Yielding partway through the spin was tested. It helped only the
/// oversubscribed case (12 workers on 12 cores) and cost 20-35%
/// everywhere else, so it was reverted. Do not run more workers than
/// cores minus the feed thread; that is the real fix for
/// oversubscription.
/// Mean per-transaction execution time below which the pool declines a
/// block and runs it sequentially.
///
/// Parallel execution buys down only the execution span. It cannot buy
/// down the serial feed (about 0.4us/tx of admission) or the commit
/// tail, and it adds cross-core traffic to every read and publish. Below
/// some amount of work per transaction, those fixed costs exceed
/// anything more cores can return, and the honest choice is not to
/// compete.
///
/// Recalibrated after a frequency root-cause fix. The original 8us
/// threshold came from measurements where worker cores ran at half
/// clock, and transfers lost speed (0.87x) there. With cores held at
/// full frequency, fully-independent 21k-gas transfers (about 4.6us/tx
/// on the mdbx stack) measure 1.54x at 4 workers, so they belong on the
/// parallel path. The threshold now sits below transfer cost; only
/// degenerate sub-2.5us work declines.
///
/// This is a floor, not a verdict on transfers: the costs it defends
/// against (the single-threaded feed and the serial delta fold) are
/// implementation limits. If they come down, this constant should come
/// down with them.
pub const PARALLEL_WORTH_NS: u64 = 2_500;

/// Sticky-assignment map bound; beyond it new domains hash as before.
const STICKY_CAP: usize = 65_536;

const SPIN_BEFORE_PARK: u32 = 256;

/// Mean per-transaction execution time above which moving a ready
/// transaction to an idle core beats keeping its state warm on the
/// owning one. Set between a 21k-gas transfer (about 2.75us, where
/// migration loses) and a uniswap swap (about 15us, where migration
/// wins).
const STEAL_WORTH_NS: u64 = 6_000;

/// Longest a parked worker sleeps before re-checking its queue and the
/// drain condition itself. Bounds the damage of a missed wake to one
/// poll interval instead of a permanent hang.
const PARK_POLL: std::time::Duration = std::time::Duration::from_micros(200);

/// Gas-limit-derived hard cap on transactions per block:
/// `BLOCK_GAS_LIMIT` / 21k intrinsic gas = 1,428, with about 2.8x
/// headroom. Slots are pre-allocated per block so workers address them
/// lock-free while the feed is still admitting (slab reuse across blocks
/// is a noted follow-up).
/// Transactions per sharded-admission batch (see `flush_admit_batch`).
const ADMIT_BATCH: usize = 512;

const MAX_BLOCK_TXS: usize = 4_096;

/// One admitted transaction. Its slot is set before its index becomes
/// visible to workers (through the ready heap), so workers read it
/// lock-free.
struct TxSlot {
    tx_idx: TxIndex,
    position: BPosition,
    envelope: TxEnvelope,
    decoded: Option<DecodedTx>,
    /// Predicted cell hashes (from `prepare`, off-thread). Sharded
    /// admission reads them from here; the serial feed uses the
    /// `Prepared` copy directly and leaves this empty.
    hashes: smallvec::SmallVec<[u64; 4]>,
}

/// One worker's FIFO. The feed pushes (single producer) and the worker
/// pops (single consumer): a two-party lock with near-zero contention.
/// Canonical arrival order per thread means same-domain chains execute
/// in order with no cross-thread coordination at all.
struct WorkerQueue {
    q: Mutex<std::collections::VecDeque<u32>>,
    /// Length hint, maintained alongside every queue mutation. Spinning
    /// workers and the steal scan read this instead of taking the
    /// mutex. A dry worker probing its queue for tens of microseconds,
    /// and every steal attempt locking every queue just to read a
    /// length, were contending with the feed's submissions. This was the
    /// measured reason admission cost grew with worker count on
    /// fully-independent work. A stale read costs one wasted lock
    /// attempt or one missed-then-caught item; the authoritative
    /// empty-check before parking still happens under the mutex.
    len: std::sync::atomic::AtomicUsize,
    cv: Condvar,
    /// Whether this worker is parked on `cv`. Waking a thread that is
    /// already running costs a futex syscall for nothing, and dispatch
    /// happens once per transaction. On a 21k-gas transfer that is about
    /// 2.7us of real work, so a wasted wake is a large fraction of the
    /// budget.
    parked: AtomicBool,
}

/// Engine instrumentation: the numbers the prune-batch decision is made
/// on. Health is judged on the pair of error rates plus realized
/// utilization; the same discipline applies to the scheduler's own
/// cost. One completion counter per worker, each on its own cache line.
#[derive(Default)]
#[repr(align(64))]
pub(crate) struct PaddedLen(pub(crate) AtomicU32);

/// One u64 counter per cache line (see [`PaddedLen`]).
#[derive(Default)]
#[repr(align(64))]
pub struct PaddedLen64(pub std::sync::atomic::AtomicU64);

#[derive(Default)]
pub struct Metrics {
    /// Nanoseconds spent holding the graph lock, split by cause.
    pub admit_ns: std::sync::atomic::AtomicU64,
    /// Feed pre-DAG bookkeeping: assignment, slot store, and envelope
    /// clone (everything before the last-toucher upsert).
    pub feed_pre_ns: std::sync::atomic::AtomicU64,
    /// Feed last-toucher upserts plus predecessor list build.
    pub feed_dag_ns: std::sync::atomic::AtomicU64,
    /// Tail: the fold thread's own body, and the hash-and-validate
    /// lanes' own bodies (aggregate). The gap to the scope's wall is
    /// thread spawn and join.
    pub commit_fold_ns: std::sync::atomic::AtomicU64,
    pub commit_lane_ns: std::sync::atomic::AtomicU64,
    pub prune_ns: std::sync::atomic::AtomicU64,
    /// Prune invocations and how many were starvation-forced (a worker
    /// had nothing to run and had to apply pending completions itself).
    pub prune_calls: std::sync::atomic::AtomicU64,
    pub prune_forced: std::sync::atomic::AtomicU64,
    /// Completions applied by prunes (divided by prune_calls gives the
    /// realized batch size).
    pub completions: std::sync::atomic::AtomicU64,
    /// Nanoseconds workers spent parked with nothing to run.
    pub idle_ns: std::sync::atomic::AtomicU64,
    /// Ready transactions taken from another thread's queue to fix
    /// imbalance.
    pub steals: std::sync::atomic::AtomicU64,
    /// Read-path breakdown. The multi-version path costs 1.7x sequential
    /// single-threaded, so which lookup dominates decides what to fix.
    pub reads_total: std::sync::atomic::AtomicU64,
    /// Reads served by a version written earlier in this block.
    pub reads_mv_hit: std::sync::atomic::AtomicU64,
    /// Reads that fell through to the shared base cache, and of those,
    /// the ones that had to touch the backing store.
    pub reads_base_hit: std::sync::atomic::AtomicU64,
    pub reads_backend: std::sync::atomic::AtomicU64,
    /// Split of a worker's per-transaction time: inside revm (`transact`,
    /// which includes the read path) versus publishing the write set
    /// into the multi-version cache. Guessing which dominates has been
    /// wrong twice.
    pub evm_ns: std::sync::atomic::AtomicU64,
    pub publish_ns: std::sync::atomic::AtomicU64,
    /// Nanoseconds workers spent inside revm, the only work that is
    /// actually the point. `busy / (workers x parallel_span)` is the true
    /// core utilization. Idle time alone cannot tell "the DAG had no work
    /// to give" apart from "work existed and nobody picked it up".
    pub busy_ns: std::sync::atomic::AtomicU64,
    /// Wall time from the block's first dispatch to its last completion:
    /// the span during which parallelism was even possible.
    pub parallel_span_ns: std::sync::atomic::AtomicU64,
    /// Wall time before the first dispatch (feed ramp) and after the
    /// last completion (serial validate and commit tail). Both are
    /// per-block costs that no worker count can reduce.
    pub ramp_ns: std::sync::atomic::AtomicU64,
    pub commit_ns: std::sync::atomic::AtomicU64,
    /// Nanoseconds of the first dispatch and last completion, as offsets
    /// from the session start (interior mutability so workers can stamp
    /// them).
    first_dispatch_ns: std::sync::atomic::AtomicU64,
    last_done_ns: std::sync::atomic::AtomicU64,
    /// Envelope decode and footprint prediction, the two pure-computation
    /// parts of admission (the rest is the graph lock and dispatch).
    pub decode_ns: std::sync::atomic::AtomicU64,
    pub predict_ns: std::sync::atomic::AtomicU64,
    /// Edges whose predecessor was assigned to the same thread and was
    /// already dispatched. The FIFO queue already orders those, so the
    /// edge enforces nothing. Measured to decide whether eliding them is
    /// worth the subtlety: eliding is only sound for predecessors
    /// already dispatched, since one still waiting could be queued after
    /// its own child.
    pub redundant_edges: std::sync::atomic::AtomicU64,
    /// Commit-tail breakdown. The tail is serial and flat in worker
    /// count (about 9.6ms of a 22ms transfers block), so whatever
    /// dominates it is a fixed parallelization tax: the thing that caps
    /// speedup no matter how many cores are available.
    pub commit_hash_ns: std::sync::atomic::AtomicU64,
    pub commit_delta_ns: std::sync::atomic::AtomicU64,
    /// Whole-admission nanoseconds (decode, predict, graph, and
    /// dispatch). The feed is a single thread, so this is a hard serial
    /// floor on block latency: the number that says whether the
    /// scheduler or the workers are the constraint.
    pub feed_ns: std::sync::atomic::AtomicU64,
    /// Account writes published, split by whether the account's domain
    /// belongs to the publishing worker. A foreign write is one that two
    /// or more workers can perform on the same account: the true sharing
    /// that no lock granularity removes. Reasoning about which side of a
    /// transfer is foreign has been wrong twice; this counts it.
    pub writes_own: std::sync::atomic::AtomicU64,
    pub writes_foreign: std::sync::atomic::AtomicU64,
    /// Predecessors covered by FIFO order instead of an edge, and how
    /// often a queued transaction found a FIFO predecessor still
    /// running (a steal moved it) and had to wait.
    pub fifo_covered: std::sync::atomic::AtomicU64,
    pub fifo_stalls: std::sync::atomic::AtomicU64,
    /// Nanoseconds inside MvView's read path, carved out of `evm_ns`.
    pub read_ns: std::sync::atomic::AtomicU64,
    /// Per-worker busy nanoseconds: the straggler detector. Dispatch can
    /// be perfectly balanced and idle time still high if cores run at
    /// different speeds (this box's bimodal memory state is per-thread).
    /// The histogram shows it directly.
    pub busy_per_worker: Vec<PaddedLen64>,
}

/// Pool configuration.
#[derive(Debug, Clone)]
pub struct PoolConfig {
    pub workers: usize,
    /// Apply completions to the DAG in batches of this many. `1` updates
    /// the graph on every completion (the immediate policy). Larger
    /// values trade graph-lock traffic for dispatch latency. A worker
    /// that runs dry always force-prunes first, so batching can never
    /// starve the pool, only delay a handoff.
    pub prune_batch: usize,
    /// Mean per-transaction execution time below which the pool declines
    /// a block and runs it sequentially. See `PARALLEL_WORTH_NS` for the
    /// measured default. Injectable so the policy is testable without
    /// depending on how loaded the machine is, and tunable per
    /// deployment.
    pub parallel_worth_ns: u64,
    /// Dispatch on the sender rather than the first non-sender cell.
    ///
    /// A transfer writes two accounts, and dispatch can only own one of
    /// them, so this chooses which side is foreign. Measured at 4
    /// workers on transfers, the default (recipient) yields 62.2%
    /// own-domain writes, matching `50% + 1/workers x 50%` exactly. This
    /// is pure scheduling policy: the DAG still takes edges on every
    /// cell either way, so it cannot change results, only locality.
    pub dispatch_by_sender: bool,
    /// Enqueue a transaction at admission when every unfinished
    /// predecessor is already released to the same worker's FIFO. Queue
    /// order then enforces the chain, and the edge and prune hand-off is
    /// skipped entirely. Per-link hand-off through batched pruning is
    /// the prime suspect for the span floor (chains release one
    /// transaction per prune).
    pub eager_chain: bool,
    /// Assign each new domain to the least-loaded worker and remember
    /// the choice for the pool's lifetime, instead of hashing.
    ///
    /// Hashing is stable but collision-blind: 4 hot pairs over 4 workers
    /// land on 4 distinct threads only about 28% of the time, and a
    /// collision puts two serial chains on one core, measured as one
    /// worker carrying half the block in a 4-pair scenario.
    /// Round-robin-on-first-sight was tested and reverted, because its
    /// assignment reshuffled every block. This keeps the cross-block
    /// stickiness that made hashing win, and fixes only the collisions.
    pub sticky_assign: bool,
    /// Bag scheduler (the default): every runnable transaction goes
    /// into one shared lock-free bag,
    /// popped by whichever worker is free. Completion is inline (the
    /// finishing worker closes its node and dispatches children, with no
    /// prune batching) with chain-local hand-off (the first ready child
    /// stays on the completing worker, so chains stream on one core with
    /// zero queue operations). No per-worker queues, no stealing, no
    /// eager coverage (every dependency is an edge). Measured at or
    /// above the FIFO scheduler on every workload tested. `false`
    /// selects the legacy per-worker FIFO scheduler.
    pub bag_scheduler: bool,
    /// Sharded admission: the number of
    /// cell-space shards the feed's dependency discovery is split
    /// across; 0 means the serial feed. Cell `c` belongs to shard
    /// `h(c) % K`, so every real conflict is owned by exactly one shard
    /// and no shard writes another's table. Discovery is batched: the
    /// batch boundary is the synchronization point, so a transaction
    /// cannot dispatch until every shard has registered its edges, and
    /// no per-shard guards are needed. Shard lanes live on the caller
    /// cores; the worker cores stay dedicated to execution.
    pub admit_shards: usize,
    /// Between blocks, workers spin-yield instead of sleeping on the
    /// condvar. schedutil drops a core to base clock the moment it
    /// idles, and burst-park execution never ramps it back up. A
    /// yielding spinner holds the governor's utilization signal up
    /// while surrendering the core within microseconds to any real
    /// work, including the commit tail's scoped threads, which pin to
    /// these same cores. This costs idle watts; production executor
    /// pools run continuously busy, so this setting mainly serves
    /// dedicated-core deployments and honest benchmarking.
    pub keep_hot: bool,
    /// Run the commit tail's parallel phases on the worker cores. Right
    /// for block-at-a-time, where workers park during the tail and
    /// their cores are hot and instantly yielded. Wrong for the
    /// pipeline, where the next block executes on those cores while
    /// this block's tail runs; the phases then stay on the caller's
    /// mask.
    pub tail_on_workers: bool,
    /// Pin worker i to `pin_cores[i % len]`.
    ///
    /// Measured reason: on a machine with two CPU core clusters sharing
    /// a split L3 cache, a worker sharing its cluster with the mdbx
    /// writer ran the same block roughly twice as slow as it ran
    /// isolated. The writer's page churn evicts the interpreter's
    /// working set from the shared cache, a memory-level tax that no
    /// code-level timer can see. An empty list lets the scheduler place
    /// workers, which settles them on the writer's cluster often enough
    /// to produce a floating per-block performance step.
    pub pin_cores: Vec<usize>,
}

/// Measured default: batching 8 completions per graph-lock acquisition
/// cuts prune time by about 30% with no wall-clock cost. Worth taking,
/// but not the main lever. The binding constraint is the serial feed
/// (about 33% of block wall time, 57% of that spent on footprint
/// prediction), which is why `prune_batch` is a tuning knob, not a fix.
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
            admit_shards: 0,
            keep_hot: false,
            tail_on_workers: true,
            pin_cores: Vec::new(),
        }
    }
}

/// The feed's last-toucher index: cell-hash to most recent toucher.
///
/// Flat and hash-keyed, not a `HashMap<DomainKey, u32>`. The map held
/// thousands of live entries with large keys, ran past the L2 cache
/// size, and compared those keys on every probe. The upsert pair was
/// the serial feed's largest stage. Here a slot is 16 bytes, the whole
/// table fits in 256KB, a probe is one cache line, and the key
/// comparison is a u64.
///
/// Collisions are safe by construction: a 64-bit collision fabricates a
/// dependency edge between two transactions that do not actually share
/// a cell. The DAG is conservative: a false edge costs a sliver of
/// parallelism and nothing else, while a missed edge (impossible here,
/// since equal cells hash equal) is what validation exists to catch.
///
/// Reset is O(1): a slot belongs to the current block only if its
/// `stamp` matches, so a new block bumps the stamp instead of clearing
/// the whole table.
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
        // This is a real assert, not a debug_assert: with a
        // non-power-of-two capacity, the `& mask` probe walk reaches
        // only a subset of the slots, and `upsert` spins forever once
        // that subset fills. It is cheap to check once per table, and
        // impossible to diagnose later.
        assert!(
            capacity_pow2.is_power_of_two(),
            "TouchTable capacity must be a power of two, got {capacity_pow2}"
        );
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
            // The stamp wrapped after billions of blocks. Hard-clear so
            // no stale slot can resurrect, then restart at 1.
            for s in self.slots.iter_mut() {
                s.stamp = 0;
            }
            self.stamp = 1;
        }
    }

    /// Record `idx` as the latest toucher of `hash`, and return the
    /// previous one, if this block has seen the cell.
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
struct Node {
    /// True while this transaction is outstanding and accepting edges.
    /// Flipped under `children`'s lock, which is what makes "is p
    /// outstanding?" and "register my edge on p" one atomic step.
    open: AtomicBool,
    /// Children registered while open. Drained in place at close so the
    /// buffer keeps its capacity for the next block that reuses this
    /// arena slot: steady-state allocation is zero.
    children: Mutex<Vec<u32>>,
    /// Outstanding predecessors. Carries a +1 admission guard while the
    /// feed is still registering this transaction's edges, so a
    /// predecessor that finishes mid-admission cannot drive the count to
    /// zero early and dispatch a half-linked transaction.
    indegree: AtomicU32,
    /// The thread this transaction was assigned. Written before any edge
    /// naming it exists, so whoever dispatches it reads a settled value.
    worker: std::sync::atomic::AtomicUsize,
    /// True once this node has been handed to a worker queue. The eager
    /// coverage test reads this, not `indegree == 0`, because prune
    /// decrements indegrees first and pushes later. In that window the
    /// feed would otherwise enqueue a successor ahead of its
    /// predecessor, and the owner would then spin forever on a head
    /// whose FIFO predecessor sits behind it (an intermittent silent
    /// wedge, observed on the transfers shape). This flag is set under
    /// the queue lock, so a `true` read orders the predecessor's push
    /// before any subsequent eager push to the same queue.
    queued: AtomicBool,
    /// Predecessors covered by FIFO order instead of an edge (eager
    /// chain mode): they were already released to this transaction's own
    /// queue when it was admitted, so queue position orders them, with
    /// no edge and no prune hand-off. Written only by the serial feed
    /// before the transaction can be released. Read by whoever takes
    /// the transaction from a queue, which must verify each one has a
    /// result before executing: work stealing can move a FIFO
    /// predecessor to another thread mid-flight, and this verification
    /// is what makes that race benign instead of a data race on state.
    fifo_preds: Mutex<Vec<u32>>,
}

/// Per-block shared context; workers hold an `Arc` for the block's
/// duration.
struct BlockCtx<S: StateDatabase> {
    env: ExecEnv,
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
    snapshots: Vec<S>,
    base: PendingDelta,
    /// EIP-7928 capture: `Some(base_index)` turns on per-transaction
    /// fragment capture, with fragment indices `base_index + local_idx +
    /// 1` (block-global; the caller passes the count of canonical
    /// records before this run, non-zero when a block is segmented
    /// around deposits). `None` means no capture (the default;
    /// validators and benches never pay for it).
    bal_base: Option<u64>,
    /// Unsettled predecessor deltas, newest first (see `BlockInput`),
    /// plus the fee-sink block-start view: everything about the block's
    /// read base that depends on its predecessor's outcome. This is
    /// late-bound: admission is layer-independent, so a pipelined
    /// consumer builds, feeds, and submits this block during its
    /// predecessor's execution, and binds the layers when the
    /// predecessor's delta releases. Workers wait for the bind before
    /// executing (see `run_worker_block`); the block-at-a-time path
    /// binds at session build, making the wait free.
    binding: std::sync::OnceLock<BoundLayers>,
    /// An Arc, so this outlives the block as a predecessor mv layer
    /// (mv-as-layer releases clone it; the last holder drops it, usually
    /// the reaper).
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
    /// runnable set. Always allocated, used only when `bag_mode`.
    bag: crossbeam_queue::ArrayQueue<u32>,
    bag_mode: bool,
    /// The pool's arena (see [`PoolHandle::arena`]): shared, never
    /// reallocated, indexed concurrently while the feed is still
    /// admitting.
    nodes: Arc<Vec<Node>>,
    /// Admitted (feed-only writer) and finished (workers) counts. The
    /// block is drained when it is sealed and the two agree.
    admitted: AtomicU32,
    finished: AtomicU32,
    sealed: AtomicBool,
    /// Per-worker completion buffers: a finishing worker parks its index
    /// here (uncontended, since it owns the slot) instead of retiring
    /// edges on every transaction. A prune drains them.
    completed: Vec<Mutex<Vec<u32>>>,
    /// Length of each buffer, readable without taking its mutex. A prune
    /// would otherwise lock every worker's buffer just to find it empty,
    /// and spinning workers force-prune often, which was the largest
    /// single overhead on micro-gas workloads. Padded to a cache line
    /// each: these counters are read-modify-written by every worker on
    /// every completion and read by every prune, so packing them into a
    /// plain array let each completion invalidate the cache line for
    /// every other worker.
    completed_len: Vec<PaddedLen>,
    /// Completions parked across all buffers: DAG updates owed.
    pending: std::sync::atomic::AtomicU64,
    prune_batch: usize,
    /// Block start, so workers can stamp first-dispatch and
    /// last-completion offsets without reaching into the session.
    started: std::time::Instant,
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
    steal_enabled: bool,
    /// How long a dry worker spins before parking, in nanoseconds, sized
    /// from the measured mean per-transaction time. A short fixed spin
    /// was much shorter than the typical gap between chain-link
    /// releases (about one transaction's execution time), so workers
    /// parked into the exact window their next transaction arrived in
    /// and paid the full poll interval to notice it. Measurement showed
    /// most of a worker's idle time was this park latency, not
    /// scheduler cost.
    spin_ns: u64,
    done_cv: Condvar,
    aborted: AtomicBool,
    /// Nodes observed leaving the graph more than once. Always zero. A
    /// non-zero value is a scheduler bug surfaced at seal, rather than a
    /// silently stranded edge.
    double_exit: AtomicU32,
    metrics: Metrics,
}

/// Lock order (the engine's one hard rule): the graph lock may be taken
/// while holding nothing, and a queue lock may be taken while holding
/// nothing or the graph lock's results, but never while the graph lock
/// is held. Every dispatch therefore collects its ready set under the
/// graph lock, releases it, and only then pushes. The idle path takes
/// the graph lock only after dropping its queue lock.
impl<S: StateDatabase> BlockCtx<S> {
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
    /// May `idx` execute right now? True when every FIFO-covered
    /// predecessor has a result. Ordinary FIFO drain makes this true by
    /// construction, since the predecessor sat ahead in the same queue.
    /// It is false only when a steal moved a predecessor to another
    /// thread and that thread is still running it.
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
            // Hint only; the victim's own lock confirms below.
            let len = qh.len.load(Ordering::Acquire);
            // Any queued transaction is stealable. A previous `len > 1`
            // guard, meant to leave the owner its work, silently disabled
            // stealing altogether: under DAG chains, a domain releases
            // one ready transaction at a time, so queues hold 0 or 1
            // items almost always. That capped effective parallelism at
            // roughly the number of domains that happened to hash to
            // distinct workers.
            if len >= 1 && best.is_none_or(|(_, b)| len > b) {
                best = Some((w, len));
            }
        }
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
    fn push_ready(&self, worker: usize, idx: u32) {
        if self.bag_mode {
            // One shared lock-free runnable set: no assignment, no
            // per-worker locks, balanced by whoever pops first. `queued`
            // is irrelevant, since coverage is off in bag mode (every
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
        // Only wake a worker that actually parked. Under load the queue
        // is rarely empty, so this elides nearly every syscall.
        if qh.parked.load(Ordering::Acquire) {
            qh.cv.notify_one();
        }
    }

    /// Apply parked completions to the live DAG: close each finished
    /// node's registration point, retire the edges that were registered
    /// while it was open, and hand whatever became ready to its thread.
    /// Takes no global lock, only the finished nodes' own mutexes.
    /// Bag-mode completion, inline: the finishing worker closes its own
    /// node right here, with no per-worker completion buffer, no
    /// cross-worker buffer scan, and no `pending` counter round-trip.
    /// One uncontended child-list lock, one indegree fetch_sub per
    /// child, and ready children go straight to the bag. Prune batching
    /// only ever existed to amortize the old global graph lock;
    /// per-node locks made it unnecessary ceremony, and this path is
    /// faster on independent transfers.
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
        // Collect under the lock, dispatch after: the bag push is
        // lock-free, but keeping the child-list critical section minimal
        // matters while the feed races to register on this node.
        // Fixed-size stack buffer, plus a spill vec: no per-completion
        // allocation.
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
        // Chain-local hand-off: the first ready child stays with the
        // completing worker as its next job. It is returned directly,
        // with no bag operation, so the cache stays warm. A chain
        // streams on one core exactly as the FIFO scheduler streamed
        // it, without a queue. The rest go to the bag for whoever is
        // free.
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
                // Leave once. Closing is the node's exit from the graph.
                // A second close would strand every edge registered in
                // between, so this is asserted rather than assumed. The
                // list is drained in place so its capacity survives for
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
    /// The next block, fed while `ctx` still executes (pipeline depth 2).
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
/// scrubs spent block structures in place (drops entries, keeps every
/// buffer) and parks them here. Session build pops from here instead of
/// mapping fresh arenas per block. mv caches outlive their block as
/// mv-as-layer references, so a still-shared cache parks until its
/// last Arc drops, and is swept at the next build.
struct RecyclePools {
    arenas: Mutex<Vec<SpentArena>>,
    mv_clean: Mutex<Vec<MvCache>>,
    mv_parked: Mutex<Vec<Arc<MvCache>>>,
    /// Cleared per-transaction read-record buffers, returned by the
    /// reaper in one batch per block, taken by workers in batches of
    /// 64. The per-transaction `Vec::with_capacity` and its growth
    /// reallocations were the largest STM-specific allocation.
    read_bufs: Mutex<Vec<Vec<ReadRecord>>>,
    /// Cleared PendingDelta shells for the fold (the maps' tables are
    /// the other huge per-block allocation), returned by the consumer
    /// through [`PoolHandle::recycle_delta`] once a release settles.
    deltas: Mutex<Vec<PendingDelta>>,
}

struct SpentArena {
    slots: Vec<std::sync::OnceLock<TxSlot>>,
    results: Vec<std::sync::OnceLock<Result<TxResult, ExecutorError>>>,
    nodes: Vec<Node>,
}

enum Reap {
    /// A finished block's recyclable structures.
    Arena {
        slots: Vec<std::sync::OnceLock<TxSlot>>,
        results: Vec<std::sync::OnceLock<Result<TxResult, ExecutorError>>>,
        nodes: Arc<Vec<Node>>,
        mv: Arc<MvCache>,
        pools: std::sync::Arc<RecyclePools>,
    },
}

/// One sealed block handed to the persistent tail thread: drain,
/// release the pool slot, then `block_tail`.
/// Per-shard last-toucher tables for sharded admission.
///
/// SAFETY: table `k` is touched only from the lane executing chunk `k`,
/// and `WorkerPool::run` hands each chunk index to exactly one lane and
/// returns only after every lane has finished. So accesses to a given
/// table are serialized, and the batch boundary orders them against the
/// router's reads.
struct ShardTables(Vec<std::cell::UnsafeCell<TouchTable>>);

unsafe impl Sync for ShardTables {}
unsafe impl Send for ShardTables {}

impl ShardTables {
    fn new(k: usize, capacity_per_shard: usize) -> Self {
        Self(
            (0..k)
                .map(|_| std::cell::UnsafeCell::new(TouchTable::new(capacity_per_shard)))
                .collect(),
        )
    }
    /// # Safety
    /// Caller must be the sole accessor of shard `k` for the duration
    /// (guaranteed by the one-chunk-per-lane contract).
    #[allow(clippy::mut_from_ref)]
    unsafe fn table(&self, k: usize) -> &mut TouchTable {
        unsafe { &mut *self.0[k].get() }
    }
    fn len(&self) -> usize {
        self.0.len()
    }
}

/// Shared, read-only view of a drained block's results, read in place
/// out of the block arena (see the presence prepass in `block_tail`).
/// Every slot is known to hold `Some(Ok(_))` before this is built.
#[derive(Clone, Copy)]
struct Results<'a>(&'a [std::sync::OnceLock<Result<TxResult, ExecutorError>>]);

impl<'a> Results<'a> {
    #[inline]
    fn get(&self, i: usize) -> &'a TxResult {
        match self.0[i].get() {
            Some(Ok(r)) => r,
            _ => unreachable!("presence prepass proved every result present"),
        }
    }
    #[inline]
    fn len(&self) -> usize {
        self.0.len()
    }
    fn iter(&self) -> impl Iterator<Item = &'a TxResult> + '_ {
        (0..self.0.len()).map(|i| self.get(i))
    }
}

/// The late-bound part of a block's read base (see `BlockCtx::binding`).
struct BoundLayers {
    /// Predecessor mv caches, newest first.
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
    ctx: std::sync::Weak<BlockCtx<S>>,
}

impl<S: StateDatabase> LayerBinder<S> {
    /// Install the unsettled predecessor layers (newest first) and
    /// probe the fee-sink block-start view through them; wakes the
    /// gated workers.
    pub fn bind(self, layers: Vec<std::sync::Arc<PendingDelta>>) -> Result<(), ExecutorError> {
        self.bind_with(Vec::new(), layers, None)
    }

    /// [`Self::bind`] with predecessor mv layers, newest first, probed
    /// before the delta layers. The
    /// fee sink is never published to an mv cache, so `sink_final`
    /// (from the predecessor's [`MvRelease`]) is required whenever mv
    /// layers are present. Without mv layers it may be `None`, and the
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

struct DeltaOut {
    tx: std::sync::mpsc::Sender<DeltaRelease>,
    /// mv-as-layer early release channel (implies speculative).
    mv_tx: Option<std::sync::mpsc::Sender<MvRelease>>,
    /// Speculative: release at fold, concurrent with validation. A
    /// wound invalidates the release, and a `corrected` re-issue
    /// follows. Conservative: release only after the verdict, when
    /// the delta can no longer change.
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
    shared: &'a PoolShared<S>,
    /// Mean per-transaction execution time of the last block, feeding
    /// the stealing policy. Atomic, not `Cell`: the persistent tail
    /// thread updates it after each block's fold while the feed thread
    /// reads it.
    avg_tx_ns: std::sync::Arc<std::sync::atomic::AtomicU64>,
    parallel_worth_ns: u64,
    dispatch_by_sender: bool,
    eager_chain: bool,
    sticky_assign: bool,
    /// Domain to worker, pool-lifetime (feed-thread-owned). Capped: past
    /// `STICKY_CAP` entries, new domains fall back to hashing, so a
    /// long-lived pool cannot grow this without bound.
    assign: std::cell::RefCell<FastMap<DomainKey, usize>>,
    /// Cumulative transactions dispatched per worker: the load the
    /// least-loaded choice reads.
    assign_load: std::cell::RefCell<Vec<u64>>,
    /// The persistent tail thread's inbox: sealed blocks go
    /// here. The thread drains, releases the pool slot, and runs
    /// `block_tail` while the caller feeds the next block.
    tail: std::sync::mpsc::Sender<TailJob<S>>,
    /// Pool-lifetime cache of the backend layer, below any pending-delta
    /// layer, which is probed before it (see `MvView`). Measurement
    /// showed almost all reads reach mdbx directly, because hot cells
    /// change every block, so a per-block cache can never hit. But the
    /// block's own delta carries every new value: `advance_base` upserts
    /// it, turning next block's backend reads into warm map hits. This
    /// is valid only if every backend commit is mirrored here, which the
    /// A/B harness's byte-identical-results check confirms.
    base_cache: std::sync::Arc<BaseCache>,
    /// Recycled block structures (see [`RecyclePools`]).
    recycle: std::sync::Arc<RecyclePools>,
    /// The feed's last-toucher index: pool-lifetime (allocated once,
    /// O(1) stamp reset per block) and feed-owned, exactly like
    /// `assign`. Only the single admission thread ever touches it.
    touch: std::cell::RefCell<TouchTable>,
    /// Sharded admission: one last-toucher table per cell-space shard,
    /// plus the lanes that drive them. Shard k is touched only by the
    /// lane running chunk k, and lanes run one chunk each per batch.
    shards: std::sync::Arc<ShardTables>,
    admit_lanes: Option<std::sync::Arc<crate::pool::WorkerPool>>,
    admit_shards: usize,
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
    let cfg_admit_shards = cfg.admit_shards;
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
    // Persistent tail lanes (see crate::pool): the hash-and-validate
    // chunks run on threads created once, not spawned per block.
    let lane_pool = std::sync::Arc::new(crate::pool::WorkerPool::new(
        workers.max(1),
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
        // Reaper: drops junk freight and scrubs recyclable arenas in
        // place (drops entries, keeps buffers), so seal() pays for
        // neither, and the next session build maps nothing. Exits when
        // the pool drops the last sender.
        scope.spawn(move || {
            while let Ok(r) = reap_rx.recv() {
                match r {
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
                        // Harvest read-record buffers on the way out. The
                        // tail no longer builds a leftover Vec, so this
                        // is where spent results are dropped.
                        let mut bufs: Vec<Vec<ReadRecord>> = Vec::new();
                        for c in results.iter_mut() {
                            if let Some(Ok(mut r)) = c.take() {
                                let mut b = std::mem::take(&mut r.reads);
                                b.clear();
                                bufs.push(b);
                            }
                        }
                        if !bufs.is_empty() {
                            let mut g = pools.read_bufs.lock().expect("pools poisoned");
                            let room = MAX_BLOCK_TXS.saturating_sub(g.len());
                            g.extend(bufs.into_iter().take(room));
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
        // The persistent tail thread. One thread owns every
        // block's post-drain work, in submission order. Per-block scoped
        // threads for sub-millisecond phases measured as a net loss.
        // This thread is also what lets the caller feed block N+1 while
        // block N validates and commits.
        {
            let reaper = reap_tx.clone();
            let recycle = recycle_pools.clone();
            let lanes_h = lane_pool.clone();
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
                    // This is the same watchdog as the inline path: a
                    // stranded edge fail-stops with forensics instead of
                    // freezing.
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
                    // Release the slot in every path, and install the
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
                                // Watchdog: a worker that never drops its
                                // Arc (wedged in a stall path) would spin
                                // this loop forever and silently hang
                                // every later ticket. Fail loudly instead.
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
                        &pins, &reaper, &avg, delta_out, &recycle, &lanes_h,
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
            shards: std::sync::Arc::new(ShardTables::new(
                cfg_admit_shards.max(1),
                // Per shard, rounded UP to a power of two: the probe walk
                // requires it, and rounding down would also crowd the
                // table (cells do not divide evenly across shards).
                ((MAX_BLOCK_TXS * 4) / cfg_admit_shards.max(1)).next_power_of_two(),
            )),
            admit_lanes: (cfg_admit_shards > 0).then(|| {
                // These lanes run on caller cores; the worker cores stay
                // dedicated to execution, which runs while the feed
                // admits.
                std::sync::Arc::new(crate::pool::WorkerPool::new(cfg_admit_shards, Vec::new()))
            }),
            admit_shards: cfg_admit_shards,
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
    /// Per-domain last toucher: the index the edges come from.
    /// Feed-owned, since admission is single-threaded and prune never
    /// reads it, so it lives here rather than in the shared graph.
    /// Keeping it out of the critical section removes several hashmap
    /// operations per transaction from the lock. Keyed symbolically, so
    /// there is no keccak on the hot path.
    /// Reusable predecessor scratch: a fresh `Vec` per transaction was a
    /// heap allocation on the serial feed.
    preds_buf: Vec<u32>,
    /// `KARDAMOM_STM_FEED_STAGES`: per-stage feed timers, off by default,
    /// since they cost what they measure.
    stage_timing: bool,
    /// Sharded admission: indices awaiting dependency discovery.
    admit_batch: Vec<u32>,
    /// The most recent ⊤ (cold) transaction: conflicts with everything,
    /// so every later admission takes an edge from it while it is
    /// outstanding.
    last_barrier: Option<u32>,
    dispatch: Vec<u32>,
    /// Admitted count. The envelopes live in the ctx slots; the repair
    /// path reads them there, with no parallel copy.
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
        // Cloning shares one transaction; backends that serialize reads
        // need `begin_block_per_worker` with independent views.
        self.begin_block_per_worker(vec![snapshot; workers], base, env, stats)
    }

    /// [`Self::begin_block`] with an independent state view per worker.
    /// See [`BlockCtx::snapshots`] for why the backend can require it.
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

    /// [`Self::begin_block_layered`] with the layer bind deferred.
    /// Admission is layer-independent, so the pipelined consumer
    /// builds, feeds, and even submits this session while the
    /// predecessor still executes, then calls [`LayerBinder::bind`]
    /// when the predecessor's delta releases. Workers wait on the bind
    /// before touching state. A consumer that will never bind must call
    /// `abort_active` instead; the drain watchdog is the backstop.
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
        binder.bind(layers)?;
        Ok(sess)
    }

    fn begin_block_deferred_inner<'p>(
        &'p self,
        snapshots: Vec<S>,
        base: PendingDelta,
        env: ExecEnv,
        stats: &'p Stats,
        bal_base: Option<u64>,
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
        // Recycle (steady-state zero-allocation blocks): sweep parked
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
        if self.admit_shards > 0 {
            self.shards_clear();
        }
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
            bal_base,
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
            prune_batch: prune_batch.max(1),
            started: std::time::Instant::now(),
            steal_enabled: {
                let avg = self.avg_tx_ns.load(Ordering::Relaxed);
                // Unknown on the first block of a pool: allow it, and
                // let the measurement correct course from the next
                // block on.
                avg == 0 || avg >= STEAL_WORTH_NS
            },
            spin_ns: {
                // Bridge roughly one link-release gap (about one
                // transaction), bounded: spinning a full core for more
                // than about 60us of silence is waste, and below about
                // 5us the spin cannot outlast even a fast release.
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
            // Pipeline depth cap of 2: one block executing (`ctx`), one
            // staged (`next`). Wait only when both are occupied, bounded
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
            admit_batch: Vec::with_capacity(ADMIT_BATCH),
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

    /// Feed a block whose transactions were prepared upstream (decode
    /// and predict already done, off this thread): the pipelined shape
    /// the tx_data reader threads will use.
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

    /// Return a settled release's delta shell for reuse. The fold's
    /// PendingDelta hashmap tables are one of the largest per-block
    /// allocations. The consumer calls this once a release's Arc
    /// unwraps, after `advance_base`. Entries drop here, but the tables
    /// keep their capacity.
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
    fn shards_clear(&self) {
        for k in 0..self.shards.len() {
            // SAFETY: no lane is running (the batch was flushed first).
            unsafe { self.shards.table(k) }.clear();
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
    /// Call this after the state writer applies the same delta. Skipping
    /// the call leaves stale entries and produces wrong reads; the
    /// harness's byte-identical assertion is the guard against that.
    pub fn advance_base(&self, delta: &PendingDelta) {
        // One write-lock per touched shard, not one per entry. The
        // per-entry version acquired thousands of write locks against
        // executing workers' read locks, and measurement showed this as
        // a growing multi-millisecond drag on the pipeline loop, since
        // it also stretched the executing block's span by slowing its
        // reads. Group entries first, then lock once.
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
                .store(elapsed.as_nanos() as u64 / txs as u64, Ordering::Relaxed);
        }
    }

    /// Run the block on this thread, through the same code path the
    /// sequential executor uses, not a reimplementation of it.
    fn decline(
        &self,
        snapshot: &S,
        base: PendingDelta,
        env: ExecEnv,
        txs: &[(TxIndex, BPosition, TxEnvelope)],
    ) -> Result<StmOutcome, ExecutorError> {
        let started = std::time::Instant::now();
        let (receipts, delta) = execute_block_sequential(snapshot, Some(&base), env, txs)?;
        // Keep measuring while declining. Without this the gate is a
        // trap door: `avg_tx_ns` would hold the value that caused the
        // decline forever, and a pool that once saw cheap transfers
        // would refuse to parallelize a heavy contract block later in
        // the same run. Sequential per-transaction cost slightly
        // overstates the pool's own, since it hashes each write set
        // inline, which the pool defers to its parallel commit phase.
        // So the bias favors re-entering parallel execution rather than
        // staying out.
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
    /// Run dependency discovery for the queued batch: lane `k` owns
    /// cell-space shard `k`, walks the batch in index order, upserts its
    /// own cells, and registers the edges it finds. Returns with every
    /// edge in place, so the guards can be dropped and the ready
    /// transactions dispatched.
    fn flush_admit_batch(&mut self) {
        if self.admit_batch.is_empty() {
            return;
        }
        let k = self.pool.admit_shards.max(1);
        let batch: &[u32] = &self.admit_batch;
        let ctx = &self.ctx;
        let shards = &self.pool.shards;
        let edges = std::sync::atomic::AtomicUsize::new(0);
        let body = |sh: usize| {
            // SAFETY: chunk `sh` is executed by exactly one lane, and
            // no other chunk touches table `sh`.
            let table = unsafe { shards.table(sh) };
            for &idx in batch.iter() {
                let slot = ctx.slots[idx as usize]
                    .get()
                    .expect("slot set before admission batch");
                for h in slot.hashes.iter() {
                    if (*h % k as u64) as usize != sh {
                        continue;
                    }
                    if let Some(p) = table.upsert(*h, idx) {
                        if p == idx {
                            continue;
                        }
                        let pn = &ctx.nodes[p as usize];
                        let mut list = pn.children.lock().expect("children poisoned");
                        if pn.open.load(Ordering::Acquire) {
                            ctx.nodes[idx as usize]
                                .indegree
                                .fetch_add(1, Ordering::AcqRel);
                            list.push(idx);
                            edges.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            }
        };
        match &self.pool.admit_lanes {
            Some(l) => l
                .run(k.min(l.workers()), &|_lane, sh| body(sh))
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
        for &idx in self.admit_batch.iter() {
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
    pub fn push_tx(
        &mut self,
        tx_idx: TxIndex,
        position: BPosition,
        envelope: TxEnvelope,
    ) -> Result<(), ExecutorError> {
        // Convenience path: prepare inline. The pipelined caller (the
        // tx_data readers) calls `prepare` upstream and `push_prepared`
        // here, keeping decode and predict off this serial thread
        // entirely.
        let t_prep = std::time::Instant::now();
        let prep = prepare(&envelope, tx_idx, self.stats);
        let dt = t_prep.elapsed().as_nanos() as u64;
        self.ctx.metrics.decode_ns.fetch_add(dt, Ordering::Relaxed);
        self.push_prepared(tx_idx, position, envelope, prep)
    }

    /// Admit a transaction whose decode and prediction were computed
    /// upstream (see [`prepare`]). This is the executor's real hot path:
    /// everything left here is graph work, which must stay serial and
    /// in canonical order, because an edge means "the previous
    /// transaction that touched this domain".
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

        // Domain to worker by hash, deliberately. Round-robin on first
        // sight was tested: it spread domains more evenly, but cost
        // wall-clock time, because hashing is stable across blocks (a
        // pool returns to the same worker every block, keeping its
        // state warm in that core's caches), while first-seen ordering
        // reshuffles the assignment each block. Locality beat balance,
        // so this reverted to hashing.
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
        // Bag mode: no owner, so no assignment at all. The hash and
        // sticky-assign logic below was the largest single feed stage,
        // measured per transaction, computing a value the bag never
        // reads.
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

        // The slot owns the envelope: no clone, no parallel vec, since
        // the repair path reads slots too.
        let sharded = self.pool.admit_shards > 0;
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
        // Stage timers are opt-in: two extra clock reads per transaction
        // measured about 5% of the serial feed, and the feed is the
        // thing they measure.
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

        // (2) Update the live DAG, and (3) dispatch if ready.
        // Candidate predecessors come from the feed-owned last-toucher
        // index. No lock is needed, because admission is single-threaded
        // and prune never reads it.
        if sharded {
            // Sharded admission. The router does node init and queues
            // the index; dependency discovery (last-toucher upserts and
            // edge registration) happens in the batch flush, one lane
            // per cell-space shard. No transaction can dispatch before
            // its batch completes, because the router's guard is
            // dropped only there. That is why per-shard guards are
            // unnecessary.
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
                let pn = &self.ctx.nodes[b as usize];
                let mut list = pn.children.lock().expect("children poisoned");
                if pn.open.load(Ordering::Acquire) {
                    self.ctx.nodes[i].indegree.fetch_add(1, Ordering::AcqRel);
                    list.push(idx);
                    self.edges += 1;
                }
            }
            if is_cold {
                // ⊤ conflicts with everything: settle the batch, then do
                // the barrier serially (edges from everything
                // outstanding, and clear all shard tables).
                self.flush_admit_batch();
                for p in 0..idx {
                    let pn = &self.ctx.nodes[p as usize];
                    let mut list = pn.children.lock().expect("children poisoned");
                    if pn.open.load(Ordering::Acquire) {
                        self.ctx.nodes[i].indegree.fetch_add(1, Ordering::AcqRel);
                        list.push(idx);
                        self.edges += 1;
                    }
                }
                self.last_barrier = Some(idx);
                self.pool.shards_clear();
            }
            self.admit_batch.push(idx);
            if self.admit_batch.len() >= ADMIT_BATCH {
                self.flush_admit_batch();
            }
            self.ctx
                .metrics
                .feed_ns
                .fetch_add(t_feed.elapsed().as_nanos() as u64, Ordering::Relaxed);
            return Ok(());
        }

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
        // No global lock. Open this node's registration point, seed the
        // admission guard, then register on each predecessor that is
        // still open. The guard (+1) means a predecessor finishing
        // mid-admission can never drive the count to zero and dispatch a
        // half-linked transaction. Dropping it at the end is what
        // actually releases this transaction.
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
        for &p in preds.iter() {
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
                        self.ctx
                            .metrics
                            .redundant_edges
                            .fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            // else: p already finished and published, no edge needed.
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

    /// The boundary: no more transactions. Wait out the in-flight tail,
    /// validate every recorded read, and wound (re-execute at its
    /// canonical position, sequentially, against the computed prefix)
    /// any transaction a missed conflict convicted, per transaction, not
    /// whole-block. Then commit in canonical order.
    pub fn seal(self) -> Result<StmOutcome, ExecutorError> {
        self.submit()?.wait()
    }

    /// Hand this block to the persistent tail thread and return right
    /// away. The pool slot frees once execution drains, so
    /// the caller may begin feeding the next block while this one
    /// validates and commits on the tail thread.
    pub fn submit(self) -> Result<BlockTicket, ExecutorError> {
        // Settle first: sealing with a partial batch would strand its
        // router guards, and the block would never drain.
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
            ..
        } = this;
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

    /// `submit`, plus a streaming delta release: the tail sends this
    /// block's folded delta on `delta_tx` as soon as it exists. That
    /// is at the fold, before validation, when `speculative`; after
    /// the verdict when not. On a wound, the tail sends a second,
    /// `corrected` release; the consumer must abort anything layered
    /// on the first.
    /// `submit_streaming` speculative, plus the early mv release:
    /// `mv_tx` receives this block's multi-version
    /// cache right after drain and extract, the earliest point a
    /// successor can bind on. `delta_tx` still receives the folded
    /// delta (for base-cache advancement and writer settlement), plus
    /// the `corrected` re-issue on a wound.
    pub fn submit_streaming_mv(
        self,
        mv_tx: std::sync::mpsc::Sender<MvRelease>,
        delta_tx: std::sync::mpsc::Sender<DeltaRelease>,
    ) -> Result<BlockTicket, ExecutorError> {
        // Settle first: sealing with a partial batch would strand its
        // router guards, and the block would never drain.
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
            ..
        } = this;
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
        // Settle first: sealing with a partial batch would strand its
        // router guards, and the block would never drain.
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
            ..
        } = this;
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

/// The block tail: everything after the pool is released. This includes
/// extraction, validation, wound repair, the canonical commit, learning,
/// and the teardown hand-off. Standalone so the persistent tail thread
/// can own it; the block-at-a-time path calls it inline (seal = drain
/// plus tail).
/// Rewrite a per-transaction BAL fragment's fee-sink balance write(s) to
/// the computed canonical prefix value: the fragment-side mirror of the
/// commit pass's WriteSet sink rewrite. Workers execute against the
/// block-start sink, so their captured value is `start + own_fee`, not
/// the running sum the sequential capture records.
fn rewrite_frag_sink(frag: &mut revm::state::bal::Bal, value: U256) {
    if let Some(acct) = frag.accounts.get_mut(&FEE_SINK) {
        for w in acct.account_info.balance.writes.iter_mut() {
            w.1 = value;
        }
    }
}

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
    lanes: &crate::pool::WorkerPool,
) -> Result<StmOutcome, ExecutorError> {
    let t_extract0 = std::time::Instant::now();
    let aborted = ctx.aborted.load(Ordering::SeqCst);
    let n = n_txs;
    // A presence prepass, not an extraction. Building a `Vec<TxResult>`
    // moved hundreds of bytes per transaction for nothing: the tail
    // only ever indexes results, and the arena that holds them already
    // recycles through the reaper. So check that every slot holds a
    // success and then read them in place.
    for (i, cell) in ctx.results.iter_mut().take(n).enumerate() {
        match cell.get_mut() {
            Some(Ok(_)) => {}
            Some(Err(_)) => {
                // Take it out to return by value (the arena is ours).
                match cell.take() {
                    Some(Err(e)) => return Err(e),
                    _ => unreachable!("just observed an error here"),
                }
            }
            None => {
                let _ = i;
                return Err(ExecutorError::State(if aborted {
                    "stm pool: block aborted".into()
                } else {
                    "stm pool: sealed block has unexecuted txs (scheduler bug)".into()
                }));
            }
        }
    }

    let t_extract = t_extract0.elapsed();
    // Early release: the mv cache's top version per cell is the final
    // delta, before any fold ran. Ship it now,
    // with the fee sink computed alongside (never published to mv).
    // Pre-verdict by construction: a wound invalidates it through the
    // corrected DeltaRelease that follows the repair.
    if let Some(DeltaOut {
        mv_tx: Some(mv_tx), ..
    }) = &delta_out
    {
        let b0 = ctx.binding.get().expect("layers bound before execution");
        let mut fee_sum = U256::ZERO;
        for cell in ctx.results.iter().take(n) {
            if let Some(Ok(r)) = cell.get() {
                fee_sum += r.fee_delta;
            }
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

    // Canonical-order commit. A wounded transaction is re-executed here
    // against the exact computed prefix (the delta as of its position),
    // so its result is the sequential one by construction. The whole
    // block never re-runs. Everything after a wound sees the corrected
    // state through the same prefix, so a wound cascade re-executes
    // only the transactions it actually reaches.
    //
    // Serial by definition: this is the block's tail, and no worker
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
    // Fast-path prefix, runs before the validation verdict exists.
    //
    // The write-set hash costs about 1.25us of keccak per transaction
    // and cannot be made cheaper, since it is one permutation per 136
    // bytes of a contract the receipts depend on. On the serial commit
    // tail it was 72% of that tail: the largest fixed parallelization
    // tax in the engine, untouched by worker count.
    //
    // It does not have to be serial. The only thing forcing it there is
    // the accumulator's absolute balance, and that is a prefix sum:
    // computable in one cheap pass with no hashing, so afterwards every
    // transaction's hash is independent.
    //
    // Phase 1 (serial, fast per transaction): cumulative gas and
    // accumulator computation. Safe before the verdict: the repair
    // path's kept-prefix arm performs these exact mutations itself
    // (idempotent), and re-executed transactions are rebuilt from
    // scratch.
    for cell in ctx.results.iter_mut().take(n) {
        let r = match cell.get_mut() {
            Some(Ok(r)) => r,
            _ => unreachable!("presence prepass proved every result present"),
        };
        cumulative += r.receipt.gas_used;
        r.receipt.cumulative_gas_used = cumulative;
        sink_running += r.fee_delta;
        if r.sink_touched
            && let Some(entry) = r.ws.accounts.iter_mut().find(|(a, _)| *a == FEE_SINK)
        {
            entry.1.1 = sink_running;
            // Same computation for the capture fragment (see
            // `rewrite_frag_sink`); a wound rebuilds both from scratch.
            if let Some(frag) = r.bal_frag.as_mut() {
                rewrite_frag_sink(frag, sink_running);
            }
        }
    }
    // The shared view is taken after the serial prefix's mutations.
    let tx_results = Results(&ctx.results[..n]);
    // Fold, hash, and validation, in one overlap scope. Fusing hash and
    // fold measured worse twice; overlap won, and validation now joins
    // the same scope. The fold builds the delta; each hash lane hashes
    // its chunk into the side array, then validates the same chunk
    // (a read-only replay against the multi-version cache: every
    // recorded read must still be the highest version below the
    // reader, and a conviction is a wound). Validation used to be its
    // own phase before the commit; hiding it under the fold, the
    // longest pole, removes it from the wall.
    //
    // The fold joins first: the delta exists at that point, the
    // streaming release point, while the hash-and-validate
    // lanes are still running.
    let t_h = std::time::Instant::now();
    let n_res = tx_results.len();
    // Lane pinning is decided once, when the pool builds its lanes.
    let _ = (keep_hot, tail_on_workers, pin_cores);
    let mut hashes: Vec<B256> = vec![B256::ZERO; n_res];
    let val_ns = std::sync::atomic::AtomicU64::new(0);
    let fold_inline = |results: Results<'_>| -> PendingDelta {
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
    // The mv pipeline runs the tail sequentially on this one thread. Its
    // latency hides behind the next block's execution, since the early
    // release already shipped, so the goal is not speed but quiet: four
    // parallel lanes of keccak and hashmap builds co-running with the
    // executing span measured a real increase in worker busy time (from
    // memory bandwidth pressure and the two shared caller cores; see
    // the topology note in the bench). Validation runs first, so a
    // wound skips the wasted fold and hash entirely.
    // Pipeline tails run their lanes on the caller cores
    // (tail_on_workers is false there), so parallelism costs the
    // executing block nothing but memory bandwidth. Fully serial was
    // the first cut. It works fine when the tail is much shorter than
    // the span it hides behind, but is wrong when it is not (on
    // micro-transaction blocks, the serial tail can exceed the span, so
    // the tail becomes the pacer). Opt back in with
    // KARDAMOM_STM_SERIAL_TAIL.
    let serial_tail = delta_out.as_ref().is_some_and(|d| d.mv_tx.is_some())
        && std::env::var_os("KARDAMOM_STM_SERIAL_TAIL").is_some();
    let (delta_arc, wounded): (std::sync::Arc<PendingDelta>, Vec<usize>) = if serial_tail {
        let t0 = std::time::Instant::now();
        let wounded: Vec<usize> = (0..tx_results.len())
            .filter(|i| {
                tx_results
                    .get(*i)
                    .reads
                    .iter()
                    .any(|rec| !ctx.mv.validate(*i as u32, rec))
            })
            .collect();
        val_ns.fetch_add(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
        if wounded.is_empty() {
            let delta_arc = std::sync::Arc::new(fold_inline(tx_results));
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
            (std::sync::Arc::new(PendingDelta::new()), wounded)
        }
    } else {
        // Persistent lanes (crate::pool): hash and validate, chunked by
        // index, on threads the pool created once. Per-block scoped
        // spawns cost a large share of the tail once the witness hash
        // got cheap. The fold runs here, on the tail thread, concurrently
        // with the lanes and without a join before the release point.
        let n_lanes = lanes.workers().max(1);
        let n_ch = n_lanes.min(n_res.max(1));
        let chunk = n_res.div_ceil(n_ch);
        let wounded_parts: Vec<Mutex<Vec<usize>>> =
            (0..n_ch).map(|_| Mutex::new(Vec::new())).collect();
        // Each chunk owns a disjoint slice of `hashes`. Lanes never
        // share an index (crate::pool hands each out exactly once).
        struct HashOut(*mut B256);
        // SAFETY: chunk i writes only hashes[i*chunk .. (i+1)*chunk],
        // disjoint from every other chunk's range, and `lanes.run` hands
        // each chunk index out exactly once.
        unsafe impl Sync for HashOut {}
        impl HashOut {
            /// # Safety
            /// `i` must lie in the calling chunk's exclusive range.
            unsafe fn set(&self, i: usize, v: B256) {
                unsafe { *self.0.add(i) = v };
            }
        }
        let out = HashOut(hashes.as_mut_ptr());
        let mv = &ctx.mv;
        let val_ns_ref = &val_ns;
        let lane_metrics = &ctx.metrics;
        let results_ref = tx_results;
        let lane_body = |ci: usize| {
            let t_lane0 = std::time::Instant::now();
            let base = ci * chunk;
            let end = (base + chunk).min(n_res);
            for i in base..end {
                let r = results_ref.get(i);
                if r.sink_touched {
                    // SAFETY: `i` lies in this chunk's exclusive range.
                    unsafe { out.set(i, r.ws.hash()) };
                }
            }
            let t0 = std::time::Instant::now();
            let mut local = Vec::new();
            for i in base..end {
                if results_ref
                    .get(i)
                    .reads
                    .iter()
                    .any(|rec| !mv.validate(i as u32, rec))
                {
                    local.push(i);
                }
            }
            val_ns_ref.fetch_add(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
            lane_metrics
                .commit_lane_ns
                .fetch_add(t_lane0.elapsed().as_nanos() as u64, Ordering::Relaxed);
            if !local.is_empty() {
                wounded_parts[ci]
                    .lock()
                    .expect("wounded part poisoned")
                    .extend(local);
            }
        };
        std::thread::scope(|sc| {
            // One scoped thread only to drive the lanes, so the fold can
            // run on this thread concurrently and reach the release
            // point without waiting for the hash work.
            let driver = sc.spawn(|| {
                lanes
                    .run(n_ch, &|_lane, i| lane_body(i))
                    .expect("tail lane panicked")
            });
            let delta_arc = std::sync::Arc::new(fold_inline(tx_results));
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
            driver.join().expect("lane driver");
            // Lanes are done, since run() returned inside the driver, so
            // the borrow is over. Drain the per-chunk lists in order.
            let wounded: Vec<usize> = wounded_parts
                .iter()
                .flat_map(|m| std::mem::take(&mut *m.lock().expect("wounded part poisoned")))
                .collect();
            (delta_arc, wounded)
        })
    };
    hash_ns += t_h.elapsed().as_nanos() as u64;
    let t_validate = std::time::Duration::from_nanos(val_ns.load(Ordering::Relaxed));
    let wounds = wounded.len();
    let mut wounded_set: HashSet<usize> = wounded.into_iter().collect();
    // Capture fragments, collected in canonical order by whichever arm
    // runs (empty when capture is off). Folded once, at the outcome.
    let mut out_frags: Vec<revm::state::bal::Bal> = Vec::new();
    // A transaction after a re-executed one may also be stale: once any
    // wound fires, later transactions are re-checked against the live
    // prefix.
    if let Some(first) = wounded_set.iter().copied().min() {
        for i in first..n {
            wounded_set.insert(i);
        }
    }
    if wounds == 0 {
        let t_d = std::time::Instant::now();
        // The consumer may still hold the released Arc, so clone then
        // (pipeline mode); a sole owner unwraps for free
        // (block-at-a-time, no release).
        delta = std::sync::Arc::try_unwrap(delta_arc).unwrap_or_else(|a| (*a).clone());
        // Conservative release: only now, when the delta can
        // no longer change.
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
        // out, then ship the whole results vec (write sets and read
        // logs) to the reaper as one move. A per-element copy here
        // measured many milliseconds of pure memmove and ate the
        // overlap's win. The shared view is Copy and simply goes out of
        // use here; the arena becomes mutable again for the receipt
        // epilogue.
        for (i, cell) in ctx.results.iter_mut().take(n).enumerate() {
            let r = match cell.get_mut() {
                Some(Ok(r)) => r,
                _ => unreachable!("presence prepass proved every result present"),
            };
            if r.sink_touched {
                r.receipt.write_set_hash = hashes[i];
            }
            out_frags.extend(r.bal_frag.take());
            receipts.push(std::mem::take(&mut r.receipt));
        }
        // The spent results (write sets and read logs) ride the arena to
        // the reaper, which harvests their read buffers and recycles
        // the whole array, with no leftover Vec and no per-block moves.
        delta_ns += t_d.elapsed().as_nanos() as u64;
    } else {
        // A speculative release, if any, was wrong: drop our Arc and
        // rebuild the delta on the repair path. A `corrected` release
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
        // Repair path: a wound fired, so transactions from the first
        // wound on re-execute against the exact computed prefix.
        // Strictly sequential by nature, and rare enough that its cost
        // is not worth optimizing.
        //
        // The prefix starts from the full pre-block view: unsettled
        // predecessor layers (oldest first, since `MvView` probes them
        // newest-first over `base`, so base is the bottom) merged over
        // the owned base delta. Dropping the layers here read
        // pre-predecessor state into re-executed transactions. This bug
        // was found by the speculative-release adversarial pipeline test (rejected
        // receipts from stale nonces) and had been latent since the
        // layers landed, since the bench scenarios never wound.
        let mut layered = ctx.base.clone();
        {
            let b = ctx.binding.get().expect("layers bound before execution");
            for l in b.layers.iter().rev() {
                layered.merge_from(l);
            }
            // mv layers are newer than the delta layers (probed first on
            // the read path), so they merge last. `final_delta` is the
            // fold-shaped computation: a rare path, paying the fold cost.
            for mv in b.mv_layers.iter().rev() {
                layered.merge_from(&mv.final_delta());
            }
        }
        let spent: Vec<TxResult> = ctx
            .results
            .iter_mut()
            .take(n)
            .map(|c| match c.take() {
                Some(Ok(r)) => r,
                _ => unreachable!("presence prepass proved every result present"),
            })
            .collect();
        for (i, mut r) in spent.into_iter().enumerate() {
            if wounded_set.contains(&i) {
                // The slot holds the envelope for the whole block, so
                // there is no second copy in a parallel vec.
                let slot = ctx.slots[i].get().expect("slot set for every admitted tx");
                let (tx_idx, position, envelope) = (slot.tx_idx, slot.position, &slot.envelope);
                let mut scope = Executor::new(&ctx.snapshots[0], Some(&layered), ctx.env)?;
                // Repair capture replaces the wounded fragment: this
                // execution runs against the computed prefix, so its
                // capture (fee sink included) is canonical directly.
                let mut repair_frag = ctx.bal_base.map(|_| revm::state::bal::Bal::new());
                let bal_arg = match (repair_frag.as_mut(), ctx.bal_base) {
                    (Some(f), Some(b)) => Some((f, b + i as u64 + 1)),
                    _ => None,
                };
                let (mut receipt, ws) = scope.execute_tx(
                    tx_idx, position, envelope, i as u64, cumulative, bal_arg, None,
                )?;
                cumulative = receipt.cumulative_gas_used;
                receipt.transaction_index = i as u64;
                layered.apply(ws.clone());
                delta.apply(ws);
                receipts.push(receipt);
                // A repaired skip captures nothing, the same hole a
                // skipped transaction leaves on the streaming path.
                out_frags.extend(repair_frag.filter(|f| !f.accounts.is_empty()));
                continue;
            }
            cumulative += r.receipt.gas_used;
            r.receipt.cumulative_gas_used = cumulative;
            sink_running += r.fee_delta;
            if r.sink_touched {
                if let Some(entry) = r.ws.accounts.iter_mut().find(|(a, _)| *a == FEE_SINK) {
                    entry.1.1 = sink_running;
                }
                if let Some(frag) = r.bal_frag.as_mut() {
                    rewrite_frag_sink(frag, sink_running);
                }
                r.receipt.write_set_hash = r.ws.hash();
            }
            out_frags.extend(r.bal_frag.take());
            layered.apply(r.ws.clone());
            delta.apply(r.ws);
            receipts.push(r.receipt);
        }
        // Corrected release: whoever consumed the speculative delta
        // must unwind onto this one. Sent in
        // conservative mode too; it is simply the first release then.
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
    // Destructure: the heavy parts (the multi-version cache and its
    // versions, and the block's transaction slots) go to the reaper;
    // the light rest drops here. `S` (the snapshots) stays inline, so
    // no 'static bound is needed on the payload.
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
    // Feed the stealing policy: mean per-transaction execution time
    // this block.
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
        bal: ctx
            .bal_base
            .is_some()
            .then(|| kardamom_exec_core::bal_ladder::merge_bal_fragments(out_frags)),
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
fn run_worker_block<S: StateDatabase>(ctx: &BlockCtx<S>, worker: usize) {
    // Late-bind gate: a deferred session's transactions are
    // admitted and queued before its read base exists. Nothing may
    // execute until the consumer binds the layers. The wait is bind
    // latency (the predecessor's fold), normally sub-millisecond; the
    // block-at-a-time path binds at session build, so this load is
    // free. A consumer that never binds must abort; the tail's drain
    // watchdog is the loud backstop.
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
    // Timing and read counts accumulate locally and flush once per
    // block. A `fetch_add` on shared metrics per read, or even per
    // transaction, had every worker writing the same cache lines. The
    // instrumentation was generating the very cross-core traffic it
    // aimed to measure.
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
                    // then the shared bag. Bag entries dispatch at
                    // indegree 0 with coverage off, so there is nothing
                    // to verify.
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
                    // Verify with no lock held. The check takes a node
                    // mutex and scans results, and holding the queue
                    // lock across it would block the feed's submissions.
                    drop(q);
                    if ctx.fifo_ready(i) {
                        break i;
                    }
                    // A FIFO predecessor was stolen and is still running
                    // on another thread. This is rare, and bounded by
                    // that transaction's execution time. Put the head
                    // back and yield.
                    ctx.metrics.fifo_stalls.fetch_add(1, Ordering::Relaxed);
                    q = qh.q.lock().expect("queue poisoned");
                    q.push_front(i);
                    qh.len.fetch_add(1, Ordering::Release);
                    drop(q);
                    std::thread::yield_now();
                    q = qh.q.lock().expect("queue poisoned");
                    continue;
                }
                // Dry. Apply any parked completions myself before
                // parking: this is what makes batching safe, since the
                // pool can never sit idle on DAG updates nobody applied.
                // The queue lock is dropped first (lock order: never
                // hold a queue lock while taking the graph lock, since
                // prune's push_ready re-locks queues, so holding one
                // here self-deadlocks).
                drop(q);
                if ctx.pending.load(Ordering::SeqCst) > 0 {
                    ctx.prune(true);
                    q = qh.q.lock().expect("queue poisoned");
                    continue;
                }
                if ctx.drained() {
                    leave!(evm);
                }
                // Domain hashing collides when the number of domains is
                // close to the number of workers, so the busiest thread
                // can take well over its even share while some threads
                // get nothing at all. So an idle worker helps the
                // busiest one rather than parking.
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
                // Spin before parking: at high throughput the next
                // transaction is usually microseconds away, and a
                // park/unpark pair costs two syscalls, more than a small
                // transfer's entire execution. Only a worker that stays
                // dry through the spin advertises itself as parked and
                // blocks.
                let t_idle = std::time::Instant::now();
                drop(q);
                let mut spun = false;
                let spin_start = std::time::Instant::now();
                loop {
                    for _ in 0..SPIN_BEFORE_PARK {
                        std::hint::spin_loop();
                    }
                    // Lock-free probe: a dry worker spinning here for
                    // tens of microseconds must not contend with the
                    // feed's push into this very queue.
                    if qh.len.load(Ordering::Acquire) > 0 || (ctx.bag_mode && !ctx.bag.is_empty()) {
                        spun = true;
                        break;
                    }
                    // Completions may be parked while we spin. Apply
                    // them ourselves rather than spin past the work
                    // they would release.
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
                    // A bounded wait, deliberately. A notification can
                    // be missed: `signal_done` wakes every queue without
                    // holding that queue's mutex, so a worker sitting
                    // between "decided to park" and "actually waiting"
                    // sleeps through it and never returns. The block
                    // drains, `seal` finishes, and the pool then hangs
                    // forever joining that thread. (Introducing the
                    // spin above widened that window enough to hit it
                    // on every run; the race predates it.) A timeout
                    // makes any missed wake self-healing, and costs
                    // nothing when wakes arrive normally.
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
            ctx.bal_base,
            &mut || take_read_buf(&mut read_stash, &ctx.recycle),
        );
        // Timestamps stay worker-local and fold once per block. Stamping
        // them globally cost two clock reads and two contended
        // read-modify-writes per transaction. On a small transfer, the
        // instrumentation was a measurable share of the work it claimed
        // to measure.
        let done_at = ctx.started.elapsed().as_nanos() as u64;
        let busy_ns = done_at.saturating_sub(t_busy_at);
        local_busy_ns += busy_ns;
        local_first_ns = local_first_ns.min(t_busy_at);
        local_last_ns = local_last_ns.max(done_at);
        // Check the write set against dispatch: an account whose domain
        // hashes to another worker can be written by more than one
        // thread.
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

        // Completion: park the index in this worker's own buffer
        // (uncontended) and only touch the DAG once a batch has
        // accumulated. `prune_batch == 1` is the immediate policy.
        //
        // `pending` increments before the buffer push. Prune runs
        // concurrently (another worker's batch, the tail's drain loop)
        // and subtracts what it drains. A completion visible in a
        // buffer before its increment landed underflowed the counter
        // (a debug-build overflow panic, found by the speculative-release
        // adversarial test's abort storms). Incremented-but-unpushed is the safe
        // direction: a spurious prune drains nothing.
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

/// Batch entry point over a transient pool. Kept for tests and simple
/// callers; long-lived callers (the A/B harness, the live actor) hold a
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

/// The sequential reference path (also the fallback): `Executor` per
/// block, in canonical order, the executor's streaming semantics.
/// [`execute_block_sequential`] with the RLP already decoded: the fair
/// baseline for A/B against the parallel engine.
///
/// The parallel path receives pre-decoded envelopes from `prepare`
/// (readers do it, off the execution thread). The sequential path used
/// to decode inline, so a naive A/B charged the decode cost to
/// sequential only, which inflated every ratio. Production gets the
/// same benefit: whoever reads the stream can hand the decode to either
/// engine.
pub fn execute_block_sequential_decoded<S: StateDatabase + Sync>(
    snapshot: &S,
    base: Option<&PendingDelta>,
    env: ExecEnv,
    txs: &[(TxIndex, BPosition, TxEnvelope)],
    decoded: &[Option<DecodedTx>],
) -> Result<(Vec<Receipt>, PendingDelta), ExecutorError> {
    debug_assert_eq!(txs.len(), decoded.len(), "one decode slot per tx");
    let mut scope = Executor::new(snapshot, base, env)?;
    let mut receipts = Vec::with_capacity(txs.len());
    let mut delta = PendingDelta::new();
    let mut cumulative = 0u64;
    let timing = std::env::var_os("KARDAMOM_SEQ_TIMING").is_some();
    let mut exec_ns = 0u64;
    for (i, (tx_idx, position, envelope)) in txs.iter().enumerate() {
        let t0 = timing.then(std::time::Instant::now);
        let (receipt, ws) = match decoded.get(i).and_then(|d| d.as_ref()) {
            Some(d) => scope.execute_tx_decoded(
                *tx_idx, *position, envelope, d, i as u64, cumulative, None, None,
            )?,
            // Undecodable: the inline path produces the skip receipt.
            None => scope.execute_tx(
                *tx_idx, *position, envelope, i as u64, cumulative, None, None,
            )?,
        };
        if let Some(t0) = t0 {
            exec_ns += t0.elapsed().as_nanos() as u64;
        }
        cumulative = receipt.cumulative_gas_used;
        delta.apply(ws);
        receipts.push(receipt);
    }
    if timing && !txs.is_empty() {
        eprintln!(
            "seq block {}: execute_tx sum {:.1}ms ({} txs, pre-decoded)",
            env.block_number,
            exec_ns as f64 / 1e6,
            txs.len()
        );
    }
    Ok((receipts, delta))
}

pub fn execute_block_sequential<S: StateDatabase + Sync>(
    snapshot: &S,
    base: Option<&PendingDelta>,
    env: ExecEnv,
    txs: &[(TxIndex, BPosition, TxEnvelope)],
) -> Result<(Vec<Receipt>, PendingDelta), ExecutorError> {
    let mut scope = Executor::new(snapshot, base, env)?;
    let mut receipts = Vec::with_capacity(txs.len());
    let mut delta = PendingDelta::new();
    let mut cumulative = 0u64;
    // Diagnostic split, env-gated: how much of the sequential wall time
    // is `execute_tx` itself versus the delta fold around it. Comparing
    // engines by their outer walls alone has misattributed overhead
    // twice.
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

/// One worker's EVM over its multi-version view: Executor's shape with
/// the concurrent DB swapped in.
type WorkerEvm<'a, S> = revm::handler::MainnetEvm<
    revm::context::Context<
        revm::context::BlockEnv,
        revm::context::TxEnv,
        revm::context::CfgEnv,
        MvView<'a, S>,
    >,
>;

/// Execute one transaction against its multi-version view. Mirrors
/// `Executor::execute_tx` exactly (#92 skip semantics, write-set
/// emission, receipt shape), with MvCache publish in place of the
/// sequential commit. The worker's EVM is reused across transactions;
/// only the view's index and read log are re-aimed.
#[allow(clippy::too_many_arguments)]
/// Pop a cleared read-record buffer (batch-refilled from the recycle
/// pool, one lock per 64 transactions); falls back to a fresh
/// allocation while the pool warms up.
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
    decoded: Option<&DecodedTx>,
    sink_start_balance: U256,
    bal_base: Option<u64>,
    fresh_reads: &mut dyn FnMut() -> Vec<ReadRecord>,
) -> Result<TxResult, ExecutorError> {
    let skip = |reason: kardamom_types::SkipReason,
                detail: &str,
                nonce: u64,
                to: Option<alloy_primitives::Address>| {
        let (receipt, ws) = Executor::<S>::skip_receipt(
            reason,
            detail,
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
            bal_frag: None,
        }
    };

    let _ = tx_idx;
    let Some(alloy_env) = decoded else {
        return Ok(skip(
            kardamom_types::SkipReason::Undecodable,
            "undecodable raw_tx",
            0,
            None,
        ));
    };
    use alloy_consensus::Transaction;
    let signer = envelope.sender;
    let nonce = alloy_env.nonce();
    let to = alloy_env.to();
    let effective_gas_price = alloy_env
        .gas_price()
        .unwrap_or_else(|| alloy_env.max_fee_per_gas());

    {
        // Re-aim the worker's view at this transaction.
        let db = revm::context_interface::ContextTr::db_mut(&mut **evm);
        db.idx = local_idx;
        db.reads.clear();
    }
    let tx_env = alloy_env.tx_env(signer);
    let t_evm = std::time::Instant::now();
    let mut outcome = match evm.transact(tx_env) {
        Ok(o) => o,
        Err(revm::context::result::EVMError::Transaction(e)) => {
            return Ok(skip(
                kardamom_exec_core::executor::skip_reason_of_tx(&e),
                &format!("{e:?}"),
                nonce,
                to,
            ));
        }
        Err(revm::context::result::EVMError::Header(e)) => {
            return Ok(skip(
                kardamom_types::SkipReason::Header,
                &format!("{e:?}"),
                nonce,
                to,
            ));
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
    // Wire logs straight from the borrowed result. The intermediate
    // `logs.clone()` (topic Vecs and data Bytes per log, on every
    // success) was allocation for its own sake.
    let (status, wire_logs) = match &outcome.result {
        ExecutionResult::Success { logs, .. } => (
            ReceiptStatus::Success,
            logs.iter().map(kardamom_types::WireLog::from).collect(),
        ),
        ExecutionResult::Revert { .. } => (ReceiptStatus::Revert, Vec::new()),
        ExecutionResult::Halt { reason, .. } => (ReceiptStatus::Halt(reason.clone()), Vec::new()),
    };

    let ws = WriteSet::from_evm_state(&outcome.state);
    // EIP-7928 capture: this transaction's fragment at its block-global
    // index, through the same update_account the streaming path uses on
    // the same `outcome.state`. Everything the mv cache tracks reads
    // canonically here (a wound would replace this fragment). The fee
    // sink is the one untracked account: the commit pass rewrites its
    // balance write to the computed prefix, exactly as it rewrites the
    // WriteSet's.
    let bal_frag = bal_base.map(|b| {
        let mut frag = revm::state::bal::Bal::new();
        let idx = b + local_idx as u64 + 1;
        for (addr, account) in outcome.state.iter() {
            frag.update_account(idx, *addr, account);
        }
        frag
    });
    // Pooled journal: revm's finalize mem::takes the state map out of
    // the journal into `outcome.state`, leaving a zero-capacity map
    // behind. Every transaction then regrew a fresh table, which
    // measurement showed as a large share of the per-transaction
    // allocation floor on light workloads. Hand the spent table back:
    // its entries drop here (they are transaction-local by contract, so
    // a stale entry would be read as a cached truth by the next
    // transaction's load_account), its capacity survives, and the
    // journal's own entry vec and transient storage already clear in
    // place.
    {
        let mut spent = std::mem::take(&mut outcome.state);
        spent.clear();
        revm::context_interface::ContextTr::journal_mut(&mut **evm)
            .inner
            .state = spent;
    }
    // Publish in the ordered helper's sequence (code and storage, then
    // accounts; see `MvCache::publish_write_set`), skipping the fee
    // sink (Accumulator: all workers see block-start; the commit pass
    // computes the prefixes).
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
        // Take this transaction's read log back out of the worker's
        // view. The replacement comes from the recycle pool (cleared,
        // with capacity intact from a previous block's transaction).
        // The fresh per-transaction Vec and its growth reallocations
        // were the largest STM-specific allocation.
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
        skip_reason: None,
    };
    Ok(TxResult {
        receipt,
        ws,
        reads,
        fee_delta,
        sink_touched,
        bal_frag,
    })
}
