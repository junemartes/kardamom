use super::config::{account_info, nanos};
use super::metrics::Metrics;
use crate::FEE_SINK;
use crate::FastMap;
use crate::mv::MvCache;
use crate::mv::ReadRecord;
use alloy_primitives::B256;
use alloy_primitives::U256;
use kardamom_exec_core::delta::PendingDelta;
use kardamom_exec_core::executor::SnapshotRef;
use kardamom_types::StateDatabase;
use revm::database::DatabaseRef;
use revm::state::AccountInfo;
use std::sync::RwLock;
use std::sync::atomic::Ordering;

/// Layered block-input view: the pre-block delta over the snapshot, what
/// sequential execution sees at the block's first transaction. Read-only
/// and shared by every worker.
pub(crate) struct BlockInput<'a, S: StateDatabase> {
    pub(crate) snapshot: &'a S,
    pub(crate) base: Option<&'a PendingDelta>,
    /// Unsettled predecessor deltas, newest first, probed before `base`.
    /// Arc-shared so pipelined submission builds a block's read layers
    /// without cloning or merging a single entry. The older merge-clone
    /// and advance step was a measured, growing multi-ms drag on the
    /// pipeline loop.
    pub(crate) layers: &'a [std::sync::Arc<PendingDelta>],
    /// Predecessor multi-version caches, newest first, probed before
    /// everything else: a drained block's mv top version per cell is
    /// its final delta, before any fold ran. Reads probe at
    /// `u32::MAX`. Immutable once the block drains; a wound
    /// invalidates the whole layer through the corrected-release
    /// protocol, never by mutation.
    pub(crate) mv_layers: &'a [std::sync::Arc<crate::mv::MvCache>],
}

impl<S: StateDatabase> DatabaseRef for BlockInput<'_, S> {
    type Error = kardamom_exec_core::executor::StateRefError;

    fn basic_ref(
        &self,
        address: alloy_primitives::Address,
    ) -> Result<Option<AccountInfo>, Self::Error> {
        for mv in self.mv_layers {
            if let Some((_, a)) = mv.read_account(u32::MAX, &address) {
                return Ok(Some(account_info(a.nonce, a.balance, a.code_hash)));
            }
        }
        for layer in self
            .layers
            .iter()
            .map(std::convert::AsRef::as_ref)
            .chain(self.base)
        {
            if let Some((nonce, balance, code_hash)) = layer.accounts.get(&address) {
                return Ok(Some(account_info(*nonce, *balance, *code_hash)));
            }
        }
        SnapshotRef {
            inner: self.snapshot,
        }
        .basic_ref(address)
    }

    fn code_by_hash_ref(&self, code_hash: B256) -> Result<revm::state::Bytecode, Self::Error> {
        for mv in self.mv_layers {
            if let Some(code) = mv.read_code(&code_hash) {
                return Ok(revm::state::Bytecode::new_raw(
                    alloy_primitives::Bytes::copy_from_slice(&code),
                ));
            }
        }
        for layer in self
            .layers
            .iter()
            .map(std::convert::AsRef::as_ref)
            .chain(self.base)
        {
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
        for mv in self.mv_layers {
            if let Some((_, v)) = mv.read_slot(u32::MAX, &address, &key) {
                return Ok(v);
            }
        }
        for layer in self
            .layers
            .iter()
            .map(std::convert::AsRef::as_ref)
            .chain(self.base)
        {
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
pub(super) struct BaseCache {
    pub(super) accounts: Vec<RwLock<FastMap<alloy_primitives::Address, Option<AccountInfo>>>>,
    pub(super) storage: Vec<RwLock<FastMap<(alloy_primitives::Address, B256), U256>>>,
    pub(super) code: RwLock<FastMap<B256, revm::state::Bytecode>>,
}

pub(super) const BASE_SHARDS: usize = 64;

impl BaseCache {
    pub(super) fn new() -> Self {
        Self {
            accounts: crate::shard_vec(BASE_SHARDS),
            storage: crate::shard_vec(BASE_SHARDS),
            code: RwLock::new(FastMap::with_hasher(crate::FnvBuild)),
        }
    }

    pub(super) fn shard(addr: &alloy_primitives::Address) -> usize {
        let b = addr.as_slice();
        (b[19] as usize) % BASE_SHARDS
    }

    /// Group `items` by `shard_of`, then insert each shard's group
    /// under one write lock: one write-lock acquisition per touched
    /// shard, not one per entry. The per-entry version acquired
    /// thousands of write locks against executing workers' read locks,
    /// and measurement showed this as a growing multi-millisecond drag
    /// on the pipeline loop, since it also stretched the executing
    /// block's span by slowing its reads.
    pub(super) fn insert_by_shard<K, V>(
        table: &[RwLock<FastMap<K, V>>],
        items: impl IntoIterator<Item = (K, V)>,
        shard_of: impl Fn(&K) -> usize,
    ) where
        K: std::hash::Hash + Eq,
    {
        let mut by_shard: Vec<Vec<(K, V)>> = (0..table.len()).map(|_| Vec::new()).collect();
        for (k, v) in items {
            by_shard[shard_of(&k)].push((k, v));
        }
        for (sh, entries) in by_shard
            .into_iter()
            .enumerate()
            .filter(|(_, e)| !e.is_empty())
        {
            table[sh]
                .write()
                .expect("base cache poisoned")
                .extend(entries);
        }
    }
}

/// Per-transaction database view: the multi-version cache at index `idx`
/// over the block input, recording every first read for validation. The
/// fee sink is served from the cached block-start info and never recorded
/// (the `Accumulator` boundary). Its correctness comes from the commit
/// pass's prefix algebra, not from version validation.
pub(super) struct MvView<'a, S: StateDatabase> {
    pub(super) mv: &'a MvCache,
    pub(super) base: &'a BlockInput<'a, S>,
    pub(super) idx: u32,
    pub(super) reads: Vec<ReadRecord>,
    pub(super) sink_start: Option<AccountInfo>,
    /// Shared across workers. See [`BaseCache`].
    pub(super) base_cache: &'a BaseCache,
    pub(super) metrics: &'a Metrics,
    /// Read counters accumulated without atomics and flushed once per
    /// block. Incrementing shared atomics on every read had every worker
    /// hammering the same cache lines. The instrumentation was distorting
    /// the very contention it aimed to measure.
    pub(super) n_reads: u64,
    pub(super) n_mv_hit: u64,
    pub(super) n_base_hit: u64,
    pub(super) n_backend: u64,
    /// Wall nanoseconds inside the read path (basic, storage, code),
    /// split out of `evm_ns`. The flamegraph inlines these frames into
    /// the interpreter, so timing is the only way to see them.
    pub(super) n_read_ns: u64,
}

impl<'a, S: StateDatabase> MvView<'a, S> {
    pub(super) fn new(
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
    pub(super) fn flush_counters(&mut self) {
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

impl<S: StateDatabase> MvView<'_, S> {
    pub(super) fn basic_inner(
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
            return Ok(Some(account_info(a.nonce, a.balance, a.code_hash)));
        }
        self.reads.push(ReadRecord::Account(address, None));
        // Probe predecessor mv layers first (newest first, at
        // `u32::MAX`, where the top version is the final value), then
        // pending-delta layers, then the base layer, all before the
        // cache. The pool-lifetime cache mirrors
        // the backend only, and these layers change per block.
        for mv in self.base.mv_layers {
            if let Some((_, a)) = mv.read_account(u32::MAX, &address) {
                self.n_base_hit += 1;
                return Ok(Some(account_info(a.nonce, a.balance, a.code_hash)));
            }
        }
        for layer in self
            .base
            .layers
            .iter()
            .map(std::convert::AsRef::as_ref)
            .chain(self.base.base)
        {
            if let Some((nonce, balance, code_hash)) = layer.accounts.get(&address) {
                self.n_base_hit += 1;
                return Ok(Some(account_info(*nonce, *balance, *code_hash)));
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

    pub(super) fn code_by_hash_inner(
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

    pub(super) fn storage_inner(
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
        for mv in self.base.mv_layers {
            if let Some((_, v)) = mv.read_slot(u32::MAX, &address, &key) {
                self.n_base_hit += 1;
                return Ok(v);
            }
        }
        for layer in self
            .base
            .layers
            .iter()
            .map(std::convert::AsRef::as_ref)
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

impl<S: StateDatabase> revm::Database for MvView<'_, S> {
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
        self.n_read_ns = self.n_read_ns.saturating_add(nanos(t0.elapsed()));
        r
    }

    fn code_by_hash(&mut self, code_hash: B256) -> Result<revm::state::Bytecode, Self::Error> {
        let t0 = std::time::Instant::now();
        let r = self.code_by_hash_inner(code_hash);
        self.n_read_ns = self.n_read_ns.saturating_add(nanos(t0.elapsed()));
        r
    }

    fn storage(
        &mut self,
        address: alloy_primitives::Address,
        index: U256,
    ) -> Result<U256, Self::Error> {
        let t0 = std::time::Instant::now();
        let r = self.storage_inner(address, index);
        self.n_read_ns = self.n_read_ns.saturating_add(nanos(t0.elapsed()));
        r
    }

    fn block_hash(&mut self, number: u64) -> Result<B256, Self::Error> {
        self.base.block_hash_ref(number)
    }
}
