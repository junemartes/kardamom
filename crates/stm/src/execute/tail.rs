use super::config::{bal_index, nanos};
use super::graph::BlockCtx;
use super::metrics::{FeedTimings, Metrics, StmOutcome, TxResult};
use super::recycle::{RecyclePools, SpentBlock};
use super::session::{DeltaOut, MvRelease};
use crate::FEE_SINK;
use alloy_primitives::B256;
use alloy_primitives::U256;
use kardamom_exec_core::delta::PendingDelta;
use kardamom_exec_core::delta::WriteSet;
use kardamom_exec_core::error::ExecutorError;
use kardamom_exec_core::executor::Executor;
use kardamom_types::StateDatabase;
use revm::state::AccountInfo;
use std::collections::HashSet;
use std::sync::atomic::Ordering;

/// Shared, read-only view of a drained block's results, read in place
/// out of the block arena (see the presence prepass in `block_tail`).
/// Every slot is known to hold `Some(Ok(_))` before this is built.
#[derive(Clone, Copy)]
pub(super) struct Results<'a>(&'a [std::sync::OnceLock<Result<TxResult, ExecutorError>>]);

impl<'a> Results<'a> {
    /// The presence prepass: every slot in `cells` must hold a success,
    /// or the block never really drained (a scheduler bug, unless
    /// `aborted`) or a transaction failed outright. Takes an error slot
    /// out to return it by value (the arena is the caller's). This is
    /// the proof every other `Results` method leans on: once it
    /// returns `Ok`, `get` cannot fail.
    ///
    /// # Errors
    /// Returns the execution error found in a failed slot, or a
    /// scheduler-bug/abort error for an unset slot.
    pub(super) fn verify(
        cells: &mut [std::sync::OnceLock<Result<TxResult, ExecutorError>>],
        aborted: bool,
    ) -> Result<(), ExecutorError> {
        for cell in cells.iter_mut() {
            match cell.get_mut() {
                Some(Ok(_)) => {}
                Some(Err(_)) => match cell.take() {
                    Some(Err(e)) => return Err(e),
                    _ => unreachable!("just observed an error here"),
                },
                None => {
                    return Err(ExecutorError::State(if aborted {
                        "stm pool: block aborted".into()
                    } else {
                        "stm pool: sealed block has unexecuted txs (scheduler bug)".into()
                    }));
                }
            }
        }
        Ok(())
    }

    #[inline]
    pub(super) fn get(&self, i: usize) -> &'a TxResult {
        match self.0[i].get() {
            Some(Ok(r)) => r,
            _ => unreachable!("presence prepass proved every result present"),
        }
    }
    #[inline]
    pub(super) fn len(&self) -> usize {
        self.0.len()
    }
    pub(super) fn iter(&self) -> impl Iterator<Item = &'a TxResult> + '_ {
        (0..self.0.len()).map(|i| self.get(i))
    }
}

/// One drained result cell, mutably: the presence prepass ([`Results::verify`])
/// already proved every cell in the block's live range holds a success,
/// so the `unreachable!` is a scheduler-invariant check, not a caller
/// contract.
fn result_mut(cell: &mut std::sync::OnceLock<Result<TxResult, ExecutorError>>) -> &mut TxResult {
    let Some(Ok(r)) = cell.get_mut() else {
        unreachable!("presence prepass proved every result present")
    };
    r
}

// Each chunk owns a disjoint slice of `hashes` inside
// `Tail::fold_hash_validate`'s lane path. Lanes never share an index
// (crate::pool hands each out exactly once).
struct HashOut(*mut B256);
// SAFETY: chunk i writes only hashes[i*chunk .. (i+1)*chunk], disjoint
// from every other chunk's range, and `lanes.run` hands each chunk
// index out exactly once.
unsafe impl Sync for HashOut {}
impl HashOut {
    /// # Safety
    /// `i` must lie in the calling chunk's exclusive range.
    unsafe fn set(&self, i: usize, v: B256) {
        unsafe { *self.0.add(i) = v };
    }
}

// Chunk ci owns wounded_parts[ci] exclusively, the same disjoint-slot
// ownership as `HashOut`, so the mutex the old shape used here
// protected nothing.
struct WoundedOut(*mut Vec<usize>);
// SAFETY: chunk ci writes only wounded_parts[ci], disjoint from every
// other chunk, and `lanes.run` hands each chunk index out exactly once.
unsafe impl Sync for WoundedOut {}
impl WoundedOut {
    /// # Safety
    /// `ci` must be the calling chunk's own index.
    unsafe fn set(&self, ci: usize, items: Vec<usize>) {
        unsafe { *self.0.add(ci) = items };
    }
}

/// One lane's exclusive slice of a block's results, `[base, end)`, for
/// `Tail::fold_hash_validate`'s lane body: `hash_into` and `validate`
/// are its two passes, timed separately (`lane_body` calls each in
/// turn, with its own `Instant`).
struct Chunk<'a> {
    results: Results<'a>,
    base: usize,
    end: usize,
}

impl Chunk<'_> {
    /// This chunk's hash pass: every touched-sink result's write-set
    /// hash, written through `out` at its own index.
    ///
    /// # Safety
    /// `[base, end)` must lie inside the calling chunk's exclusive range.
    unsafe fn hash_into(&self, out: &HashOut) {
        for i in self.base..self.end {
            let r = self.results.get(i);
            if r.sink_touched {
                // SAFETY: `i` lies in `[base, end)`, the caller's own range.
                unsafe { out.set(i, r.ws.hash()) };
            }
        }
    }

    /// This chunk's validate pass: every wounded index — a recorded
    /// read the final mv lists no longer support.
    fn validate(&self, mv: &crate::mv::MvCache) -> Vec<usize> {
        (self.base..self.end)
            .filter(|&i| {
                let idx = u32::try_from(i).expect("tx index bounded by MAX_BLOCK_TXS");
                self.results
                    .get(i)
                    .reads
                    .iter()
                    .any(|rec| !mv.validate(idx, rec))
            })
            .collect()
    }
}

/// Rewrite a per-transaction BAL fragment's fee-sink balance write(s) to
/// the computed canonical prefix value: the fragment-side mirror of the
/// commit pass's `WriteSet` sink rewrite. Workers execute against the
/// block-start sink, so their captured value is `start + own_fee`, not
/// the running sum the sequential capture records.
pub(super) fn rewrite_frag_sink(frag: &mut revm::state::bal::Bal, value: U256) {
    if let Some(acct) = frag.accounts.get_mut(&FEE_SINK) {
        for w in &mut acct.account_info.balance.writes {
            w.1 = value;
        }
    }
}

/// The commit-order accumulator: cumulative gas and the running
/// fee-sink balance, both prefix sums over canonical order, plus the
/// block number the overflow error names. Held by `Tail` as one field
/// (`self.prefix`), borrowed disjointly from `self.ctx.results` (a
/// different field) by both `serial_prefix` and `repair_wounded`.
struct Prefix {
    cumulative: u64,
    sink_running: U256,
    block_number: u64,
}

impl Prefix {
    /// One transaction's accumulator step: fold its gas into the
    /// running cumulative total, and its fee-sink credit into the
    /// running sink balance, then patch both the write set's fee-sink
    /// entry and its BAL fragment (if any) to the new absolute value.
    /// Shared by `Tail::serial_prefix` (the fast path, over live
    /// results) and `Tail::repair_wounded` (recomputed from a wound
    /// onward, over results already taken out of the arena); the two
    /// differ only in the overflow message's `context` suffix and in
    /// whether the write-set hash is patched here or later (the fast
    /// path's hash comes from `fold_hash_validate`'s lanes; a repair
    /// computes it directly, since it never reaches that pass).
    ///
    /// # Errors
    /// Returns an error, with `context` appended, if the fee-sink
    /// accumulator overflows `U256`.
    fn apply(&mut self, r: &mut TxResult, context: &str) -> Result<(), ExecutorError> {
        // Bounded by the block gas limit (never checked by this crate,
        // but the wire-level producer enforces it), so a saturating
        // add is the honest ceiling, not a silent wrap.
        self.cumulative = self.cumulative.saturating_add(r.receipt.gas_used);
        r.receipt.cumulative_gas_used = self.cumulative;
        self.sink_running = self.sink_running.checked_add(r.fee_delta).ok_or_else(|| {
            ExecutorError::State(format!(
                "stm: block {} fee-sink accumulator overflowed U256{context}",
                self.block_number
            ))
        })?;
        if r.sink_touched {
            if let Some(entry) = r.ws.accounts.iter_mut().find(|(a, _)| *a == FEE_SINK) {
                entry.1.1 = self.sink_running;
            }
            // Same computation for the capture fragment (see
            // `rewrite_frag_sink`); a wound rebuilds both from scratch.
            if let Some(frag) = r.bal_frag.as_mut() {
                rewrite_frag_sink(frag, self.sink_running);
            }
        }
        Ok(())
    }
}

/// The sealed block and what the tail consumes it into.
pub(super) struct TailInput<S: StateDatabase> {
    pub(super) ctx: BlockCtx<S>,
    pub(super) n_txs: usize,
    pub(super) delta_out: Option<DeltaOut>,
}

/// Wall time the feed and drain phases already spent, measured by the
/// caller before the tail starts.
#[derive(Clone, Copy)]
pub(super) struct TailTiming {
    pub(super) exec_wall: std::time::Duration,
    pub(super) drain: std::time::Duration,
}

/// Feed-side counts and per-phase nanoseconds, folded once here instead
/// of through a cross-thread atomic (see `BlockSession`'s plain fields).
pub(super) struct TailStats {
    pub(super) cold: usize,
    pub(super) edges: usize,
    pub(super) dispatch: Vec<u32>,
    pub(super) feed: FeedTimings,
}

/// Shared pool resources the tail hands work back to or draws lanes
/// from. The persistent tail thread builds one instance at spawn and
/// clones it per block.
#[derive(Clone)]
pub(super) struct TailDeps {
    pub(super) reaper: std::sync::mpsc::Sender<SpentBlock>,
    pub(super) avg_tx_ns: std::sync::Arc<std::sync::atomic::AtomicU64>,
    pub(super) recycle: std::sync::Arc<RecyclePools>,
    pub(super) lanes: std::sync::Arc<crate::pool::WorkerPool>,
}

/// Absorb one transaction's write set into the delta accumulator `d`:
/// every write merged, last-writer-wins, with the fee sink pulled out
/// into `sink_final` instead (the commit pass computes its prefix
/// separately; the delta never carries it directly). Kept off
/// `PendingDelta` itself: `exec-core`'s copy of that type is shared
/// with call sites outside the STM engine's fold, and this shape
/// (three flat inserts plus the sink carve-out) is specific to how the
/// STM tail folds a result set, not a general delta operation.
/// The fold's running state: the delta accumulator, and the fee
/// sink's pulled-out final value (never folded into the delta
/// directly; the commit pass computes its prefix separately).
struct Fold {
    delta: PendingDelta,
    sink_final: Option<(u64, U256, B256)>,
}

impl Fold {
    /// Absorb one transaction's write set: every write merged,
    /// last-writer-wins, with the fee sink pulled out into
    /// `self.sink_final` instead of the delta.
    fn absorb(&mut self, ws: &WriteSet) {
        for (a, v) in &ws.accounts {
            if *a == FEE_SINK {
                self.sink_final = Some(*v);
            } else {
                self.delta.accounts.insert(*a, *v);
            }
        }
        for (k, v) in &ws.storage {
            self.delta.storage.insert(*k, *v);
        }
        for (h, b) in &ws.code {
            self.delta.code.insert(*h, b.clone());
        }
    }

    /// Patch the fee sink's final value back into the delta (if it was
    /// touched), and hand the delta over.
    fn finish(mut self) -> PendingDelta {
        if let Some(v) = self.sink_final {
            self.delta.accounts.insert(FEE_SINK, v);
        }
        self.delta
    }
}

/// Fold a chunk's results into one delta: every write set merged,
/// last-writer-wins, with the fee sink pulled out (it is never folded
/// into the delta directly; the commit pass computes its prefix
/// separately). The delta's tables come from the recycle pool when one
/// is free, so a steady-state block allocates none.
fn fold_inline(results: Results<'_>, recycle: &RecyclePools) -> PendingDelta {
    let delta = recycle
        .deltas
        .lock()
        .expect("pools poisoned")
        .pop()
        .unwrap_or_default();
    let mut fold = Fold {
        delta,
        sink_final: None,
    };
    fold.delta.accounts.reserve(results.len() * 2);
    fold.delta.storage.reserve(results.len());
    for r in results.iter() {
        fold.absorb(&r.ws);
    }
    fold.finish()
}

/// The opt-in fully-serial tail (`KARDAMOM_STM_SERIAL_TAIL`): validate
/// every recorded read on this one thread, then fold and hash only if
/// nothing wounded. See `block_tail`'s serial-vs-lanes doc for why this
/// exists; production runs the persistent-lanes path instead.
fn serial_hash_and_validate<S: StateDatabase>(
    tx_results: Results<'_>,
    ctx: &BlockCtx<S>,
    hashes: &mut [B256],
    delta_out: Option<&DeltaOut>,
    recycle: &RecyclePools,
    val_ns: &std::sync::atomic::AtomicU64,
    fold_ns: &std::sync::atomic::AtomicU64,
) -> (std::sync::Arc<PendingDelta>, Vec<usize>) {
    let t0 = std::time::Instant::now();
    let wounded: Vec<usize> = (0..tx_results.len())
        .filter(|i| {
            let idx = u32::try_from(*i).expect("tx index bounded by MAX_BLOCK_TXS");
            tx_results
                .get(*i)
                .reads
                .iter()
                .any(|rec| !ctx.mv.validate(idx, rec))
        })
        .collect();
    val_ns.fetch_add(nanos(t0.elapsed()), Ordering::Relaxed);
    if !wounded.is_empty() {
        return (std::sync::Arc::new(PendingDelta::new()), wounded);
    }
    let t_fold = std::time::Instant::now();
    let delta_arc = std::sync::Arc::new(fold_inline(tx_results, recycle));
    fold_ns.fetch_add(nanos(t_fold.elapsed()), Ordering::Relaxed);
    if let Some(d) = delta_out
        && d.speculative
    {
        d.release(ctx.env.block_number, delta_arc.clone(), false);
    }
    for (i, r) in tx_results.iter().enumerate() {
        if r.sink_touched {
            hashes[i] = r.ws.hash();
        }
    }
    (delta_arc, wounded)
}

/// The block tail: everything after the pool is released. This includes
/// extraction, validation, wound repair, the canonical commit, learning,
/// and the teardown hand-off. Standalone so the persistent tail thread
/// can own it; the block-at-a-time path calls it inline (seal = drain
/// plus tail).
pub(super) fn block_tail<S: StateDatabase + Sync>(
    input: TailInput<S>,
    timing: TailTiming,
    stats: TailStats,
    deps: TailDeps,
) -> Result<StmOutcome, ExecutorError> {
    Tail::new(input, timing, stats, deps)?.run()
}

/// Destructure a drained block: the heavy parts (the multi-version
/// cache and its versions, and the block's transaction slots) go to the
/// reaper; the light rest (the metrics) is returned. `S` (the
/// snapshots) stays inline in `ctx`, so no `'static` bound is needed on
/// the reap payload. Also feeds the stealing policy: mean
/// per-transaction execution time this block.
fn reap_and_learn<S: StateDatabase>(
    ctx: BlockCtx<S>,
    n: usize,
    avg_tx_ns: &std::sync::atomic::AtomicU64,
    reaper: &std::sync::mpsc::Sender<SpentBlock>,
    recycle: &std::sync::Arc<RecyclePools>,
) -> Metrics {
    let BlockCtx {
        mv,
        slots,
        results,
        nodes,
        metrics,
        ..
    } = ctx;
    reaper
        .send(SpentBlock {
            slots,
            results,
            nodes,
            mv,
            pools: recycle.clone(),
        })
        .ok();
    if n > 0 {
        avg_tx_ns.store(
            metrics.busy_ns.load(Ordering::Relaxed) / n as u64,
            Ordering::Relaxed,
        );
    }
    metrics
}

/// The parallel span (first dispatch to last completion) and the ramp
/// (time to first dispatch), both in microseconds. `first_dispatch_ns`
/// seeds from `u64::MAX` (`fetch_min`), so it stays there on a block
/// that dispatched nothing.
fn span_and_ramp_us(m: &Metrics) -> (u64, u64) {
    let f = m.first_dispatch_ns.load(Ordering::Relaxed);
    let l = m.last_done_ns.load(Ordering::Relaxed);
    let span = if f == u64::MAX || l < f {
        0
    } else {
        (l - f) / 1_000
    };
    let ramp = if f == u64::MAX { 0 } else { f / 1_000 };
    (span, ramp)
}

/// One block's commit tail, held across its phases (extraction,
/// validation, wound repair, the canonical commit, and the teardown
/// hand-off): `run` calls the phase methods in order, each reading and
/// updating this same running state instead of passing it through
/// method arguments.
struct Tail<S: StateDatabase + Sync> {
    ctx: BlockCtx<S>,
    n: usize,
    delta_out: Option<DeltaOut>,
    timing: TailTiming,
    stats: TailStats,
    deps: TailDeps,
    t_extract: std::time::Duration,
    prefix: Prefix,
    receipts: Vec<kardamom_types::Receipt>,
    delta: PendingDelta,
    hashes: Vec<B256>,
    hash_ns: u64,
    delta_ns: u64,
    val_ns: u64,
    fold_ns: u64,
    out_frags: Vec<revm::state::bal::Bal>,
}

impl<S: StateDatabase + Sync> Tail<S> {
    /// Build the tail state and run the presence prepass: every slot in
    /// `0..n` must hold a success, or the block never really drained
    /// (a scheduler bug) or was aborted. A prepass error is an early
    /// return, since a bad slot is unusable for any later phase.
    ///
    /// # Errors
    /// Returns an error if a slot holds an execution error, or if a
    /// slot is unset (aborted, or a scheduler bug).
    fn new(
        input: TailInput<S>,
        timing: TailTiming,
        stats: TailStats,
        deps: TailDeps,
    ) -> Result<Self, ExecutorError> {
        let TailInput {
            mut ctx,
            n_txs: n,
            delta_out,
        } = input;
        let t_extract0 = std::time::Instant::now();
        let aborted = ctx.aborted.load(Ordering::SeqCst);
        Results::verify(&mut ctx.results[..n], aborted)?;
        let t_extract = t_extract0.elapsed();
        let sink_running = ctx.bound().sink_start_balance;
        let block_number = ctx.env.block_number;
        Ok(Self {
            ctx,
            n,
            delta_out,
            timing,
            stats,
            deps,
            t_extract,
            prefix: Prefix {
                cumulative: 0,
                sink_running,
                block_number,
            },
            receipts: Vec::with_capacity(n),
            delta: PendingDelta::new(),
            hashes: Vec::new(),
            hash_ns: 0,
            delta_ns: 0,
            val_ns: 0,
            fold_ns: 0,
            out_frags: Vec::new(),
        })
    }

    /// The early streaming release: the mv cache's top version per cell
    /// is the final delta, before any fold ran. Ship it now, with the
    /// fee sink computed alongside (never published to mv). Pre-verdict
    /// by construction: a wound invalidates it through the corrected
    /// `DeltaRelease` that follows the repair.
    ///
    /// # Errors
    /// Returns an error if the summed fee credits overflow `U256` (the
    /// block gas limit bounds any one transaction's fee, but summing
    /// them is still checked, not assumed).
    fn send_early_mv(&self) -> Result<(), ExecutorError> {
        let Some(DeltaOut {
            mv_tx: Some(mv_tx), ..
        }) = &self.delta_out
        else {
            return Ok(());
        };
        let b0 = self.ctx.bound();
        let mut fee_sum = U256::ZERO;
        for cell in self.ctx.results.iter().take(self.n) {
            if let Some(Ok(r)) = cell.get() {
                fee_sum = fee_sum.checked_add(r.fee_delta).ok_or_else(|| {
                    ExecutorError::State(format!(
                        "stm: block {} fee-sum overflowed U256 summing per-tx fee credits",
                        self.ctx.env.block_number
                    ))
                })?;
            }
        }
        let sink_final = match &b0.sink_start {
            Some(a) => {
                let mut a = a.clone();
                a.balance = b0.sink_start_balance.checked_add(fee_sum).ok_or_else(|| {
                    ExecutorError::State(format!(
                        "stm: block {} fee sink balance overflowed U256 (start={}, fee_sum={fee_sum})",
                        self.ctx.env.block_number, b0.sink_start_balance
                    ))
                })?;
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
            block: self.ctx.env.block_number,
            mv: self.ctx.mv.clone(),
            sink_final,
        });
        Ok(())
    }

    /// Canonical-order commit, phase 1 (serial, fast per transaction):
    /// cumulative gas and the fee-sink accumulator. Safe before the
    /// verdict: the repair path's kept-prefix arm performs these exact
    /// mutations itself (idempotent), and re-executed transactions are
    /// rebuilt from scratch.
    ///
    /// A wounded transaction is re-executed later against the exact
    /// computed prefix (the delta as of its position), so its result is
    /// the sequential one by construction. The whole block never
    /// re-runs. Everything after a wound sees the corrected state
    /// through the same prefix, so a wound cascade re-executes only the
    /// transactions it actually reaches. Serial by definition: this is
    /// the block's tail, and no worker count shortens it.
    ///
    /// The write-set hash costs about 1.25us of keccak per transaction
    /// and cannot be made cheaper, since it is one permutation per 136
    /// bytes of a contract the receipts depend on. It does not have to
    /// be serial: the only thing forcing it here is the accumulator's
    /// absolute balance, and that is a prefix sum, computable in one
    /// cheap pass with no hashing, so afterwards every transaction's
    /// hash is independent (see `fold_hash_validate`).
    /// # Errors
    /// Returns an error if the fee-sink accumulator overflows `U256`.
    fn serial_prefix(&mut self) -> Result<(), ExecutorError> {
        let n = self.n;
        for cell in self.ctx.results.iter_mut().take(n) {
            let r = result_mut(cell);
            self.prefix.apply(r, "")?;
        }
        Ok(())
    }

    /// Fold, hash, and validation, in one overlap scope. The fold
    /// builds the delta; each hash lane hashes its chunk into the side
    /// array, then validates the same chunk (a read-only replay against
    /// the multi-version cache: every recorded read must still be the
    /// highest version below the reader, and a conviction is a wound).
    /// Validation hides under the fold, the longest pole, so it costs
    /// nothing on the wall. The fold joins first: the delta exists at
    /// that point, the streaming release point, while the
    /// hash-and-validate lanes are still running.
    ///
    /// Updates `self.hashes`/`hash_ns`/`fold_ns`/`val_ns`; returns the
    /// folded delta and the wounded indices for the caller to act on.
    fn fold_hash_validate(&mut self) -> (std::sync::Arc<PendingDelta>, Vec<usize>) {
        // The shared view is taken after the serial prefix's mutations.
        let tx_results = Results(&self.ctx.results[..self.n]);
        let t_h = std::time::Instant::now();
        let n_res = tx_results.len();
        let mut hashes: Vec<B256> = vec![B256::ZERO; n_res];
        let val_ns = std::sync::atomic::AtomicU64::new(0);
        let fold_ns = std::sync::atomic::AtomicU64::new(0);
        // The mv pipeline runs the tail sequentially on this one
        // thread. Its latency hides behind the next block's execution,
        // since the early release already shipped, so the goal is not
        // speed but quiet: four parallel lanes of keccak and hashmap
        // builds co-running with the executing span measured a real
        // increase in worker busy time (from memory bandwidth pressure
        // and the two shared caller cores; see the topology note in the
        // bench). Validation runs first, so a wound skips the wasted
        // fold and hash entirely. Pipeline tails run their lanes on the
        // caller cores (tail_on_workers is false there), so parallelism
        // costs the executing block nothing but memory bandwidth. A
        // fully serial tail works fine when it is much shorter than the
        // span it hides behind, but is wrong when it is not (on
        // micro-transaction blocks, the serial tail can exceed the
        // span, so the tail becomes the pacer). Opt back in with
        // KARDAMOM_STM_SERIAL_TAIL.
        let ctx = &self.ctx;
        let recycle = &self.deps.recycle;
        let delta_out = self.delta_out.as_ref();
        let lanes = &self.deps.lanes;
        let serial_tail = delta_out.is_some_and(|d| d.mv_tx.is_some())
            && std::env::var_os("KARDAMOM_STM_SERIAL_TAIL").is_some();
        let (delta_arc, wounded): (std::sync::Arc<PendingDelta>, Vec<usize>) = if serial_tail {
            serial_hash_and_validate(
                tx_results,
                ctx,
                &mut hashes,
                delta_out,
                recycle,
                &val_ns,
                &fold_ns,
            )
        } else {
            // Persistent lanes (crate::pool): hash and validate,
            // chunked by index, on threads the pool created once.
            // Per-block scoped spawns cost a large share of the tail
            // once the witness hash got cheap. The fold runs here, on
            // the tail thread, concurrently with the lanes and without
            // a join before the release point.

            // An empty block has no chunks to hash: `n_ch` stays 0, and
            // `lanes.run(0, ..)` is a no-op, rather than clamping
            // `n_res` up to 1 to keep the later `div_ceil` from a zero
            // divisor.
            let n_ch = std::num::NonZeroUsize::new(n_res)
                .map_or(0, |n| lanes.workers().get().min(n.get()));
            let chunk = if n_ch == 0 { 0 } else { n_res.div_ceil(n_ch) };
            let mut wounded_parts: Vec<Vec<usize>> = (0..n_ch).map(|_| Vec::new()).collect();
            let out = HashOut(hashes.as_mut_ptr());
            let wounded_out = WoundedOut(wounded_parts.as_mut_ptr());
            let mv = &ctx.mv;
            let val_ns_ref = &val_ns;
            let lane_metrics = &ctx.metrics;
            let results_ref = tx_results;
            let lane_body = |ci: usize| {
                let t_lane0 = std::time::Instant::now();
                let base = ci * chunk;
                let end = (base + chunk).min(n_res);
                let c = Chunk {
                    results: results_ref,
                    base,
                    end,
                };
                // SAFETY: chunk `ci` owns `[base, end)` exclusively; no
                // other chunk writes into it.
                unsafe { c.hash_into(&out) };
                let t0 = std::time::Instant::now();
                let local = c.validate(mv);
                val_ns_ref.fetch_add(nanos(t0.elapsed()), Ordering::Relaxed);
                lane_metrics
                    .commit_lane_ns
                    .fetch_add(nanos(t_lane0.elapsed()), Ordering::Relaxed);
                if !local.is_empty() {
                    // SAFETY: `ci` is this lane's own chunk index.
                    unsafe { wounded_out.set(ci, local) };
                }
            };
            std::thread::scope(|sc| {
                // One scoped thread only to drive the lanes, so the
                // fold can run on this thread concurrently and reach
                // the release point without waiting for the hash work.
                let driver = sc.spawn(|| {
                    lanes
                        .run(n_ch, &|_lane, i| lane_body(i))
                        .expect("tail lane panicked");
                });
                let t_fold = std::time::Instant::now();
                let delta_arc = std::sync::Arc::new(fold_inline(tx_results, recycle));
                fold_ns.fetch_add(nanos(t_fold.elapsed()), Ordering::Relaxed);
                if let Some(d) = &delta_out
                    && d.speculative
                {
                    d.release(ctx.env.block_number, delta_arc.clone(), false);
                }
                driver.join().expect("lane driver");
                // Lanes are done, since run() returned inside the
                // driver, so the borrow is over. Collect the per-chunk
                // lists in order.
                let wounded: Vec<usize> = wounded_parts.into_iter().flatten().collect();
                (delta_arc, wounded)
            })
        };
        self.hash_ns = self.hash_ns.saturating_add(nanos(t_h.elapsed()));
        self.val_ns = val_ns.load(Ordering::Relaxed);
        self.fold_ns = fold_ns.load(Ordering::Relaxed);
        self.hashes = hashes;
        (delta_arc, wounded)
    }

    /// The clean commit (no wound): unwrap the folded delta, release it
    /// conservatively if the consumer wants a post-verdict delta, then
    /// patch each receipt's write-set hash in place and ship the spent
    /// results to the reaper.
    fn commit_clean(&mut self, delta_arc: std::sync::Arc<PendingDelta>) {
        let t_d = std::time::Instant::now();
        // The consumer may still hold the released Arc, so clone then
        // (pipeline mode); a sole owner unwraps for free
        // (block-at-a-time, no release).
        self.delta = std::sync::Arc::try_unwrap(delta_arc).unwrap_or_else(|a| (*a).clone());
        // Conservative release: only now, when the delta can no longer
        // change.
        if let Some(d) = &self.delta_out
            && !d.speculative
        {
            d.release(
                self.ctx.env.block_number,
                std::sync::Arc::new(self.delta.clone()),
                false,
            );
        }
        // Serial epilogue: patch receipt hashes in place, move receipts
        // out, then ship the whole results vec (write sets and read
        // logs) to the reaper as one move. A per-element copy here
        // measured many milliseconds of pure memmove and ate the
        // overlap's win. The shared view is Copy and simply goes out of
        // use here; the arena becomes mutable again for the receipt
        // epilogue.
        let n = self.n;
        for (i, cell) in self.ctx.results.iter_mut().take(n).enumerate() {
            let r = result_mut(cell);
            if r.sink_touched {
                r.receipt.write_set_hash = self.hashes[i];
            }
            self.out_frags.extend(r.bal_frag.take());
            self.receipts.push(std::mem::take(&mut r.receipt));
        }
        // The spent results (write sets and read logs) ride the arena
        // to the reaper, which harvests their read buffers and recycles
        // the whole array, with no leftover Vec and no per-block moves.
        self.delta_ns = self.delta_ns.saturating_add(nanos(t_d.elapsed()));
    }

    /// Wound repair: a wound fired, so transactions from the first
    /// wound on re-execute against the exact computed prefix. Strictly
    /// sequential by nature, and rare enough that its cost is not worth
    /// optimizing.
    ///
    /// # Errors
    /// Returns an error if a repair execution fails.
    fn repair_wounded(&mut self, wounded_set: &HashSet<usize>) -> Result<(), ExecutorError> {
        // Phase 1 already ran over these results; this path recomputes
        // the same values for a kept-prefix transaction, so restart the
        // running sums from zero.
        self.prefix.cumulative = 0;
        self.prefix.sink_running = self.ctx.bound().sink_start_balance;
        // The prefix must start from the full pre-block view, layers
        // included: unsettled predecessor layers (oldest first, since
        // `MvView` probes them newest-first over `base`, so base is the
        // bottom) merged over the owned base delta. Dropping the layers
        // here reads pre-predecessor state into re-executed
        // transactions.
        let mut layered = self.ctx.base.clone();
        {
            let b = self.ctx.bound();
            for l in b.layers.iter().rev() {
                layered.merge_from(l);
            }
            // mv layers are newer than the delta layers (probed first
            // on the read path), so they merge last. `final_delta` is
            // the fold-shaped computation: a rare path, paying the fold
            // cost.
            for mv in b.mv_layers.iter().rev() {
                layered.merge_from(&mv.final_delta());
            }
        }
        let n = self.n;
        let spent: Vec<TxResult> = self
            .ctx
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
                let slot = self.ctx.slots[i]
                    .get()
                    .expect("slot set for every admitted tx");
                let (tx_idx, position, envelope) = (slot.tx_idx, slot.position, &slot.envelope);
                let mut scope =
                    Executor::new(self.ctx.snapshots.primary(), Some(&layered), self.ctx.env)?;
                // Repair capture replaces the wounded fragment: this
                // execution runs against the computed prefix, so its
                // capture (fee sink included) is canonical directly.
                let mut repair_frag = self.ctx.bal_base.map(|_| revm::state::bal::Bal::new());
                let bal_arg = match (repair_frag.as_mut(), self.ctx.bal_base) {
                    (Some(f), Some(b)) => {
                        Some((f, bal_index(b, i as u64, self.ctx.env.block_number)?))
                    }
                    _ => None,
                };
                let (mut receipt, ws) = scope.execute_tx(
                    tx_idx,
                    position,
                    envelope,
                    i as u64,
                    self.prefix.cumulative,
                    bal_arg,
                    None,
                )?;
                self.prefix.cumulative = receipt.cumulative_gas_used;
                receipt.transaction_index = i as u64;
                layered.apply(ws.clone());
                self.delta.apply(ws);
                self.receipts.push(receipt);
                // A repaired skip captures nothing, the same hole a
                // skipped transaction leaves on the streaming path.
                self.out_frags
                    .extend(repair_frag.filter(|f| !f.accounts.is_empty()));
                continue;
            }
            // Bounded by the block gas limit (see `serial_prefix`).
            self.prefix.apply(&mut r, " during repair")?;
            if r.sink_touched {
                r.receipt.write_set_hash = r.ws.hash();
            }
            self.out_frags.extend(r.bal_frag.take());
            layered.apply(r.ws.clone());
            self.delta.apply(r.ws);
            self.receipts.push(r.receipt);
        }
        // Corrected release: whoever consumed the speculative delta
        // must unwind onto this one. Sent in conservative mode too; it
        // is simply the first release then.
        if let Some(d) = &self.delta_out {
            d.release(
                self.ctx.env.block_number,
                std::sync::Arc::new(self.delta.clone()),
                d.speculative,
            );
        }
        Ok(())
    }

    /// Run every phase in order and build the outcome.
    ///
    /// # Errors
    /// Returns an error if wound repair fails to re-execute a
    /// transaction.
    #[allow(
        clippy::cast_precision_loss,
        reason = "avg_batch is a diagnostic ratio of small counters, far below f64's 2^52 mantissa limit"
    )]
    fn run(mut self) -> Result<StmOutcome, ExecutorError> {
        self.send_early_mv()?;
        let t_com = std::time::Instant::now();
        self.serial_prefix()?;
        let (delta_arc, wounded) = self.fold_hash_validate();
        let t_validate = std::time::Duration::from_nanos(self.val_ns);
        let wounds = wounded.len();
        let mut wounded_set: HashSet<usize> = wounded.into_iter().collect();
        // A transaction after a re-executed one may also be stale: once
        // any wound fires, later transactions are re-checked against
        // the live prefix.
        if let Some(first) = wounded_set.iter().copied().min() {
            for i in first..self.n {
                wounded_set.insert(i);
            }
        }
        // A speculative release, if any, was wrong when wounded: the
        // stale Arc is rebuilt on the repair path instead (a
        // `corrected` re-issue follows). Binding it to `_stale` in the
        // match, rather than an explicit `drop`, means it now releases
        // at the end of the wounded arm (after `repair_wounded`
        // returns) instead of before repair starts; nothing in this
        // file reads its `strong_count` or races its drop, so the
        // later release point is not observable.
        match (wounds, delta_arc) {
            (0, arc) => self.commit_clean(arc),
            (_, _stale) => self.repair_wounded(&wounded_set)?,
        }
        let t_commit = t_com.elapsed();
        if wounds > 0 {
            tracing::warn!(
                block = self.ctx.env.block_number,
                wounds,
                rerun = wounded_set.len(),
                "stm: wound — per-tx re-execution at canonical position"
            );
        }
        if std::env::var("KARDAMOM_STM_PHASE_TIMING").is_ok() {
            eprintln!(
                "phase block={} n={} feed+exec={:?} drain={:?} extract={:?} validate={:?} commit={:?} wounds={}",
                self.ctx.env.block_number,
                self.n,
                self.timing.exec_wall,
                self.timing.drain,
                self.t_extract,
                t_validate,
                t_commit,
                wounds
            );
        }
        Ok(self.into_outcome(wounds, t_commit))
    }

    /// Destructure the tail: the heavy parts (the multi-version cache
    /// and its versions, and the block's transaction slots) go to the
    /// reaper; the light rest drops here. `S` (the snapshots) stays
    /// inline, so no `'static` bound is needed on the payload. Then
    /// build the outcome from the block's metrics and this tail's own
    /// running totals.
    #[allow(
        clippy::cast_precision_loss,
        reason = "avg_batch is a diagnostic ratio of small counters, far below f64's 2^52 mantissa limit"
    )]
    fn into_outcome(self, wounds: usize, t_commit: std::time::Duration) -> StmOutcome {
        let Self {
            ctx,
            n,
            receipts,
            delta,
            hash_ns,
            delta_ns,
            out_frags,
            stats,
            deps,
            fold_ns,
            ..
        } = self;
        let TailStats {
            cold,
            edges,
            dispatch,
            feed:
                FeedTimings {
                    admit_ns,
                    feed_pre_ns,
                    feed_dag_ns,
                    decode_ns,
                    predict_ns,
                    redundant_edges,
                    fifo_covered,
                    feed_ns,
                },
        } = stats;
        let TailDeps {
            reaper,
            avg_tx_ns,
            recycle,
            ..
        } = deps;
        let bal_base = ctx.bal_base;
        let double_exit = ctx.double_exit.load(Ordering::SeqCst);
        let metrics = reap_and_learn(ctx, n, &avg_tx_ns, &reaper, &recycle);
        let m = &metrics;
        let prune_calls = m.prune_calls.load(Ordering::Relaxed);
        let completions = m.completions.load(Ordering::Relaxed);
        let (parallel_span_us, ramp_us) = span_and_ramp_us(m);
        StmOutcome {
            receipts,
            delta,
            bal: bal_base
                .is_some()
                .then(|| kardamom_exec_core::bal_ladder::merge_bal_fragments(out_frags)),
            wounds,
            fallback: wounds > 0,
            declined: false,
            learned_tx_ns: avg_tx_ns.load(Ordering::Relaxed),
            writes_own: m.writes_own.load(Ordering::Relaxed),
            writes_foreign: m.writes_foreign.load(Ordering::Relaxed),
            fifo_covered,
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
            double_exit,
            feed_us: feed_ns / 1_000,
            redundant_edges,
            steals: m.steals.load(Ordering::Relaxed),
            reads_total: m.reads_total.load(Ordering::Relaxed),
            reads_mv_hit: m.reads_mv_hit.load(Ordering::Relaxed),
            reads_base_hit: m.reads_base_hit.load(Ordering::Relaxed),
            reads_backend: m.reads_backend.load(Ordering::Relaxed),
            evm_us: m.evm_ns.load(Ordering::Relaxed) / 1_000,
            publish_us: m.publish_ns.load(Ordering::Relaxed) / 1_000,
            busy_us: m.busy_ns.load(Ordering::Relaxed) / 1_000,
            parallel_span_us,
            ramp_us,
            commit_us: u64::try_from(t_commit.as_micros()).unwrap_or(u64::MAX),
            commit_hash_us: hash_ns / 1_000,
            commit_delta_us: delta_ns / 1_000,
            decode_us: decode_ns / 1_000,
            predict_us: predict_ns / 1_000,
            admit_us: admit_ns / 1_000,
            commit_fold_us: fold_ns / 1_000,
            commit_lane_us: m.commit_lane_ns.load(Ordering::Relaxed) / 1_000,
            feed_pre_us: feed_pre_ns / 1_000,
            feed_dag_us: feed_dag_ns / 1_000,
            prune_us: m.prune_ns.load(Ordering::Relaxed) / 1_000,
            prune_calls,
            prune_forced: m.prune_forced.load(Ordering::Relaxed),
            avg_batch: if prune_calls == 0 {
                0.0
            } else {
                completions as f64 / prune_calls as f64
            },
            idle_us: m.idle_ns.load(Ordering::Relaxed) / 1_000,
        }
    }
}
