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

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};

use alloy_primitives::{B256, U256};
use bytes::Bytes;
use kardamom_exec_core::block_env::ExecEnv;
use kardamom_exec_core::delta::{PendingDelta, WriteSet};
use kardamom_exec_core::error::ExecutorError;
use kardamom_exec_core::exec_types::{ReceiptStatus, TxIndex};
use kardamom_exec_core::executor::{
    ExecScope, SnapshotRef, decode_alloy_envelope, invalid_skip, tx_env_from_alloy, wire_log,
    write_set_from_evm_state,
};
use kardamom_footprint::Cell;
use kardamom_footprint::classifier::Stats;
use kardamom_types::{BPosition, Receipt, StateDatabase, TxEnvelope};
use revm::context::result::ExecutionResult;
use revm::database::DatabaseRef;
use revm::state::AccountInfo;
use revm::{Context, ExecuteEvm, MainBuilder, MainContext};

use crate::FEE_SINK;
use crate::mv::{AccountVersion, MvCache, ReadRecord};
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
            revm::state::Bytecode::new_raw(alloy_primitives::Bytes::copy_from_slice(&code))
        } else {
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
    /// Validation failed and the block re-executed sequentially
    /// (invariant #3). Outputs are the sequential ones.
    pub fallback: bool,
    /// Read records convicted by validation (0 unless `fallback`).
    pub validation_failures: usize,
    /// Schedule diagnostics.
    pub cold: usize,
    pub edges: usize,
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

/// Graph + progress state under the per-block lock. Admission (the feed
/// side) and completion (workers) both mutate it; every operation is a few
/// vector ops — the lock is held for microseconds.
#[derive(Default)]
struct Graph {
    admitted: u32,
    finished: u32,
    sealed: bool,
    indegree: Vec<u32>,
    children: Vec<Vec<u32>>,
    complete: Vec<bool>,
    // Canonical-order-first ready policy: the lowest ready index runs
    // first — chains drain in order and read-then-published windows (the
    // only source of validation convictions) stay minimal.
    ready: BinaryHeap<Reverse<u32>>,
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
    graph: Mutex<Graph>,
    work_cv: Condvar,
    done_cv: Condvar,
    aborted: AtomicBool,
}

struct PoolState<'env, S: StateDatabase> {
    generation: u64,
    ctx: Option<Arc<BlockCtx<'env, S>>>,
    shutdown: bool,
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
    workers: usize,
    f: impl FnOnce(&PoolHandle<'_, S>) -> R,
) -> R {
    let workers = workers.max(1);
    let shared: PoolShared<'_, S> = (
        Mutex::new(PoolState {
            generation: 0,
            ctx: None,
            shutdown: false,
        }),
        Condvar::new(),
    );
    std::thread::scope(|scope| {
        for _ in 0..workers {
            scope.spawn(|| worker_loop(&shared));
        }
        let handle = PoolHandle {
            shared: &shared,
            snapshot,
        };
        let r = f(&handle);
        let mut st = shared.0.lock().expect("pool poisoned");
        st.shutdown = true;
        drop(st);
        shared.1.notify_all();
        r
    })
}

/// One in-flight block being fed to the pool.
pub struct BlockSession<'p, 'a, S: StateDatabase + Sync> {
    pool: &'p PoolHandle<'a, S>,
    ctx: Arc<BlockCtx<'a, S>>,
    dag: schedule::DagBuilder,
    exclude: HashSet<Cell>,
    stats: &'p Stats,
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
            graph: Mutex::new(Graph::default()),
            work_cv: Condvar::new(),
            done_cv: Condvar::new(),
            aborted: AtomicBool::new(false),
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
            dag: schedule::DagBuilder::default(),
            exclude,
            stats,
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
    /// Admit the next canonical tx: predict, extend the DAG, and dispatch
    /// immediately if it has no unfinished predecessors — workers execute
    /// it while the sealer is still streaming the rest of the block.
    pub fn push_tx(
        &mut self,
        tx_idx: TxIndex,
        position: BPosition,
        envelope: TxEnvelope,
    ) -> Result<(), ExecutorError> {
        let i = self.txs.len();
        if i >= MAX_BLOCK_TXS {
            return Err(ExecutorError::State(format!(
                "stm pool: block exceeds MAX_BLOCK_TXS={MAX_BLOCK_TXS} (gas-limit math says impossible)"
            )));
        }
        let decoded = decode_alloy_envelope(&envelope.raw_tx, tx_idx).ok();
        let view = schedule::scheduling_view_decoded(i as u32, &envelope, decoded.as_ref());
        let cells = self.stats.predict(&view);
        self.ctx.slots[i]
            .set(TxSlot {
                tx_idx,
                position,
                envelope: envelope.clone(),
                decoded,
            })
            .unwrap_or_else(|_| unreachable!("slot set once per index"));
        self.txs.push((tx_idx, position, envelope));
        let preds = self.dag.admit(i as u32, cells, &self.exclude);
        let mut g = self.ctx.graph.lock().expect("graph poisoned");
        g.indegree.push(0);
        g.children.push(Vec::new());
        g.complete.push(false);
        let mut deg = 0u32;
        for p in preds {
            // A predecessor that already FINISHED contributes no live edge
            // (its writes are published); admit-vs-complete is atomic under
            // the graph lock.
            if !g.complete[p as usize] {
                g.children[p as usize].push(i as u32);
                deg += 1;
            }
        }
        g.indegree[i] = deg;
        g.admitted += 1;
        let dispatch = deg == 0;
        if dispatch {
            g.ready.push(Reverse(i as u32));
        }
        drop(g);
        if dispatch {
            self.ctx.work_cv.notify_one();
        }
        Ok(())
    }

    /// The boundary: no more txs. Wait out the in-flight tail, validate
    /// every recorded read, commit in canonical order (Accumulator prefix
    /// materialization + wsh fixup); a validation conviction falls back to
    /// sequential re-execution (invariant #3).
    pub fn seal(self) -> Result<StmOutcome, ExecutorError> {
        let BlockSession {
            pool,
            ctx,
            dag,
            txs,
            started,
            ..
        } = self;
        {
            let mut g = ctx.graph.lock().expect("graph poisoned");
            g.sealed = true;
        }
        ctx.work_cv.notify_all();
        {
            let mut g = ctx.graph.lock().expect("graph poisoned");
            while !(ctx.aborted.load(Ordering::SeqCst) || g.finished == g.admitted) {
                g = ctx.done_cv.wait(g).expect("graph poisoned");
            }
        }
        // Release the pool for the next block before the (serial)
        // validate+commit tail.
        {
            let mut st = pool.shared.0.lock().expect("pool poisoned");
            st.ctx = None;
        }
        let t_exec_wall = started.elapsed();

        // Take sole ownership of the context: each worker drops its Arc as
        // it observes the sealed-and-done condition — a few microseconds of
        // yield-spin at most.
        let mut ctx = ctx;
        let ctx = loop {
            match Arc::try_unwrap(ctx) {
                Ok(c) => break c,
                Err(back) => {
                    ctx = back;
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

        // Validation (spec: "Validation remains as the final invariant
        // check"): every recorded read must still be the highest version
        // below the reader. A conviction = the prediction missed a real
        // conflict.
        let t_val = std::time::Instant::now();
        let validation_failures: usize = tx_results
            .iter()
            .enumerate()
            .map(|(i, r)| {
                r.reads
                    .iter()
                    .filter(|rec| !ctx.mv.validate(i as u32, rec))
                    .count()
            })
            .sum();
        let t_validate = t_val.elapsed();
        if validation_failures > 0 {
            // Invariant #3: discard, re-execute sequentially, count it.
            tracing::warn!(
                block = ctx.env.block_number,
                validation_failures,
                "stm: validation conviction — sequential fallback"
            );
            let (receipts, delta) =
                execute_block_sequential(ctx.snapshot, Some(&ctx.base), ctx.env, &txs)?;
            return Ok(StmOutcome {
                receipts,
                delta,
                fallback: true,
                validation_failures,
                cold: dag.cold,
                edges: dag.edges,
            });
        }

        // Canonical-order commit: cumulative gas, accumulator
        // materialization + wsh fixup, delta fold. Receipts/delta
        // byte-identical to sequential.
        let mut receipts = Vec::with_capacity(n);
        let mut delta = PendingDelta::new();
        let mut cumulative = 0u64;
        let mut sink_running = ctx.sink_start_balance;
        for mut r in tx_results {
            cumulative += r.receipt.gas_used;
            r.receipt.cumulative_gas_used = cumulative;
            sink_running += r.fee_delta;
            if let Some(entry) = r.ws.accounts.iter_mut().find(|(a, _)| *a == FEE_SINK) {
                entry.1.1 = sink_running;
                r.receipt.write_set_hash = r.ws.hash();
            }
            delta.apply(r.ws);
            receipts.push(r.receipt);
        }

        if std::env::var("KARDAMOM_STM_PHASE_TIMING").is_ok() {
            eprintln!(
                "phase block={} n={} feed+exec={:?} validate={:?}",
                ctx.env.block_number, n, t_exec_wall, t_validate
            );
        }
        Ok(StmOutcome {
            receipts,
            delta,
            fallback: false,
            validation_failures: 0,
            cold: dag.cold,
            edges: dag.edges,
        })
    }
}

fn worker_loop<S: StateDatabase + Sync>(shared: &PoolShared<'_, S>) {
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
        run_worker_block(&ctx);
        // Arc drops here — seal()'s try_unwrap spin depends on it.
    }
}

/// One worker's participation in one block: ONE EVM for the whole block
/// (per-tx construction was ~90% of execution-path allocation), the view
/// re-aimed per tx. Returns when the block is sealed and drained.
fn run_worker_block<S: StateDatabase>(ctx: &BlockCtx<'_, S>) {
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
    loop {
        let job = {
            let mut g = ctx.graph.lock().expect("graph poisoned");
            loop {
                if ctx.aborted.load(Ordering::SeqCst)
                    || (g.sealed && g.finished == g.admitted && g.ready.is_empty())
                {
                    return;
                }
                if let Some(Reverse(i)) = g.ready.pop() {
                    break i;
                }
                g = ctx.work_cv.wait(g).expect("graph poisoned");
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
            ctx.work_cv.notify_all();
            ctx.done_cv.notify_all();
            return;
        }
        let mut g = ctx.graph.lock().expect("graph poisoned");
        g.complete[job as usize] = true;
        g.finished += 1;
        // Later admissions may still append to children[job] — but they
        // check complete[job] under this same lock first, so a drained
        // list is final.
        let kids = std::mem::take(&mut g.children[job as usize]);
        let mut newly = 0usize;
        for c in kids {
            g.indegree[c as usize] -= 1;
            if g.indegree[c as usize] == 0 {
                g.ready.push(Reverse(c));
                newly += 1;
            }
        }
        let done = g.sealed && g.finished == g.admitted;
        drop(g);
        if newly > 0 || done {
            ctx.work_cv.notify_all();
        }
        if done {
            ctx.done_cv.notify_all();
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
    with_pool(snapshot, workers, |pool| {
        pool.run_block(base.cloned().unwrap_or_default(), env, txs, stats)
    })
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
    // Publish: every written cell EXCEPT the fee sink (Accumulator — all
    // workers see block-start; the commit pass materializes prefixes).
    for (addr, (n_, b_, ch_)) in ws.accounts.iter() {
        if *addr == FEE_SINK {
            continue;
        }
        mv.publish_account(
            local_idx,
            *addr,
            AccountVersion {
                nonce: *n_,
                balance: *b_,
                code_hash: *ch_,
            },
        );
    }
    for ((addr, key), value) in ws.storage.iter() {
        mv.publish_slot(local_idx, *addr, *key, *value);
    }
    for (hash, code) in ws.code.iter() {
        mv.publish_code(*hash, Bytes::clone(code));
    }
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
