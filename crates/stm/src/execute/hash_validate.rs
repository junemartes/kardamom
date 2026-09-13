//! The block tail's hash-and-validate half: per-transaction write-set
//! hashing, multi-version read validation, and the delta fold that runs
//! alongside them. [`Tail::fold_hash_validate`] drives all three in one
//! overlap scope; `tail.rs` owns the serial prefix, the clean commit,
//! and wound repair that follow.

use super::config::nanos;
use super::graph::BlockCtx;
use super::metrics::{Metrics, StmOutcome, TxResult};
use super::recycle::{RecyclePools, SpentBlock};
use super::session::DeltaOut;
use super::tail::Tail;
use crate::FEE_SINK;
use alloy_primitives::B256;
use alloy_primitives::U256;
use kardamom_exec_core::delta::WriteSet;
use kardamom_exec_core::delta::{AccountFields, PendingDelta};
use kardamom_exec_core::error::ExecutorError;
use kardamom_types::StateDatabase;
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
            Self::verify_cell(cell, aborted)?;
        }
        Ok(())
    }

    /// One result slot's presence check. The `for` loop in
    /// [`Self::verify`] stays free of a branch.
    fn verify_cell(
        cell: &mut std::sync::OnceLock<Result<TxResult, ExecutorError>>,
        aborted: bool,
    ) -> Result<(), ExecutorError> {
        match cell.take() {
            Some(Ok(r)) => {
                let _ = cell.set(Ok(r));
                Ok(())
            }
            Some(Err(e)) => Err(e),
            None => Err(ExecutorError::State(if aborted {
                "stm pool: block aborted".into()
            } else {
                "stm pool: sealed block has unexecuted txs (scheduler bug)".into()
            })),
        }
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
pub(super) fn result_mut(
    cell: &mut std::sync::OnceLock<Result<TxResult, ExecutorError>>,
) -> &mut TxResult {
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
// ownership as `HashOut`. No lock is needed: each chunk writes only
// its own slot.
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
    /// The whole block's tx count, already proven `<= MAX_BLOCK_TXS`.
    /// [`Self::validate`] derives this chunk's indices from it, so no
    /// per-index `.expect(..)` repeats the bound.
    count: super::config::BlockTxCount,
}

impl Chunk<'_> {
    /// This chunk's hash pass: every touched-sink result's write-set
    /// hash, written through `out` at its own index.
    ///
    /// # Safety
    /// `[base, end)` must lie inside the calling chunk's exclusive range.
    unsafe fn hash_into(&self, out: &HashOut) {
        for i in self.base..self.end {
            // SAFETY: `i` lies in `[base, end)`, the caller's own range.
            unsafe { self.hash_one(out, i) };
        }
    }

    /// One index's hash step: written through `out` when the result
    /// touched the fee sink. The `for` loop in [`Self::hash_into`]
    /// stays free of a branch.
    ///
    /// # Safety
    /// `i` must lie in the calling chunk's exclusive range.
    unsafe fn hash_one(&self, out: &HashOut, i: usize) {
        let r = self.results.get(i);
        if r.sink_fee_delta.is_none() {
            return;
        }
        unsafe { out.set(i, r.ws.hash()) };
    }

    /// This chunk's validate pass: every wounded index — a recorded
    /// read the final mv lists no longer support.
    fn validate(&self, mv: &crate::mv::MvCache) -> Vec<usize> {
        // `BlockTxIndex` widens losslessly to `usize`: it is a `u32`
        // already proven `<= MAX_BLOCK_TXS` (4,096), far inside
        // `usize`'s range on every platform this workspace builds for.
        self.count
            .indices()
            .skip(self.base)
            // A lane whose `base` sits at or past `end` owns no index.
            // Width zero is the correct value.
            .take(self.end.saturating_sub(self.base))
            .filter(|idx| {
                self.results
                    .get(idx.get() as usize)
                    .reads
                    .iter()
                    .any(|rec| !mv.validate(idx.get(), rec))
            })
            .map(|idx| idx.get() as usize)
            .collect()
    }
}

/// Rewrite a per-transaction BAL fragment's fee-sink balance write(s) to
/// the computed canonical prefix value: the fragment-side mirror of the
/// commit pass's `WriteSet` sink rewrite. Workers execute against the
/// block-start sink, so their captured value is `start + own_fee`, not
/// the running sum the sequential capture records.
pub(super) fn rewrite_frag_sink(frag: &mut revm::state::bal::Bal, value: U256) {
    let Some(acct) = frag.accounts.get_mut(&FEE_SINK) else {
        return;
    };
    for w in &mut acct.account_info.balance.writes {
        w.1 = value;
    }
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
    sink_final: Option<AccountFields>,
}

impl Fold {
    /// Absorb one transaction's write set: every write merged,
    /// last-writer-wins, with the fee sink pulled out into
    /// `self.sink_final` instead of the delta.
    fn absorb(&mut self, ws: &WriteSet) {
        for (a, v) in &ws.accounts {
            self.absorb_account(*a, *v);
        }
        for (k, v) in &ws.storage {
            self.delta.storage.insert(*k, *v);
        }
        for (h, b) in &ws.code {
            self.delta.code.insert(*h, b.clone());
        }
    }

    /// One account write: pulled into `self.sink_final` for the fee
    /// sink, merged into the delta (last-writer-wins) otherwise. The
    /// `for` loop in [`Self::absorb`] stays free of a branch.
    fn absorb_account(&mut self, a: alloy_primitives::Address, v: AccountFields) {
        if a == FEE_SINK {
            self.sink_final = Some(v);
        } else {
            self.delta.accounts.insert(a, v);
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

/// [`serial_hash_and_validate`]'s two phase timers, bundled so the
/// function takes one argument for them instead of two loose ones. Both
/// fields are references, so this is `Copy`.
#[derive(Clone, Copy)]
struct SerialTimers<'a> {
    val_ns: &'a std::sync::atomic::AtomicU64,
    fold_ns: &'a std::sync::atomic::AtomicU64,
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
    timers: SerialTimers<'_>,
) -> (std::sync::Arc<PendingDelta>, Vec<usize>) {
    let SerialTimers { val_ns, fold_ns } = timers;
    let t0 = std::time::Instant::now();
    // `BlockTxIndex` widens losslessly to `usize`: it is a `u32` already
    // proven `<= MAX_BLOCK_TXS` (4,096), far inside `usize`'s range on
    // every platform this workspace builds for.
    let count = super::config::BlockTxCount::new(tx_results.len())
        .expect("admission caps a block at MAX_BLOCK_TXS");
    let wounded: Vec<usize> = count
        .indices()
        .filter(|idx| {
            tx_results
                .get(idx.get() as usize)
                .reads
                .iter()
                .any(|rec| !ctx.mv.validate(idx.get(), rec))
        })
        .map(|idx| idx.get() as usize)
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
    tx_results
        .iter()
        .enumerate()
        .filter(|(_, r)| r.sink_fee_delta.is_some())
        .for_each(|(i, r)| hashes[i] = r.ws.hash());
    (delta_arc, wounded)
}

/// [`lanes_hash_and_validate`]'s inputs, bundled so the function takes
/// one argument for them instead of seven loose parameters.
struct LaneHashInput<'a, S: StateDatabase> {
    tx_results: Results<'a>,
    count: super::config::BlockTxCount,
    n_res: usize,
    ctx: &'a BlockCtx<S>,
    delta_out: Option<&'a DeltaOut>,
    recycle: &'a RecyclePools,
    lanes: &'a crate::pool::WorkerPool,
}

/// The persistent-lanes tail (the production default): hash and
/// validate, chunked by index, on threads the pool created once.
/// Per-block scoped spawns cost a large share of the tail once the
/// witness hash got cheap. The fold runs here, on the tail thread,
/// concurrently with the lanes and without a join before the release
/// point.
fn lanes_hash_and_validate<S: StateDatabase>(
    input: &LaneHashInput<'_, S>,
    hashes: &mut [B256],
    timers: SerialTimers<'_>,
) -> (std::sync::Arc<PendingDelta>, Vec<usize>) {
    // Every field is `Copy` (a small integer, or a reference), so a
    // field access here copies it out instead of borrowing `input`.
    let tx_results = input.tx_results;
    let count = input.count;
    let n_res = input.n_res;
    let ctx = input.ctx;
    let delta_out = input.delta_out;
    let recycle = input.recycle;
    let lanes = input.lanes;
    let SerialTimers { val_ns, fold_ns } = timers;
    // An empty block has no chunks to hash: `lanes.run(0, ..)` is a
    // no-op.
    let (n_ch, chunk) = std::num::NonZeroUsize::new(n_res).map_or((0, 0), |n| {
        let n_ch = lanes.workers().get().min(n.get());
        (n_ch, n_res.div_ceil(n_ch))
    });
    let mut wounded_parts: Vec<Vec<usize>> = (0..n_ch).map(|_| Vec::new()).collect();
    let out = HashOut(hashes.as_mut_ptr());
    let wounded_out = WoundedOut(wounded_parts.as_mut_ptr());
    let mv = &ctx.mv;
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
            count,
        };
        // SAFETY: chunk `ci` owns `[base, end)` exclusively; no
        // other chunk writes into it.
        unsafe { c.hash_into(&out) };
        let t0 = std::time::Instant::now();
        let local = c.validate(mv);
        val_ns.fetch_add(nanos(t0.elapsed()), Ordering::Relaxed);
        lane_metrics
            .commit_lane_ns
            .fetch_add(nanos(t_lane0.elapsed()), Ordering::Relaxed);
        if !local.is_empty() {
            // SAFETY: `ci` is this lane's own chunk index.
            unsafe { wounded_out.set(ci, local) };
        }
    };
    std::thread::scope(|sc| {
        // One scoped thread only to drive the lanes, so the fold can
        // run on this thread concurrently and reach the release point
        // without waiting for the hash work.
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
        // The closure already turned a contained lane panic into this
        // thread's own panic; joining re-raises it here, so a lane
        // panic still fails the block instead of the tail thread
        // running past it with partial hashes.
        driver.join().expect("lane driver");
        // Lanes are done, since run() returned inside the driver, so
        // the borrow is over. Collect the per-chunk lists in order.
        let wounded: Vec<usize> = wounded_parts.into_iter().flatten().collect();
        (delta_arc, wounded)
    })
}

impl<S: StateDatabase + Sync> Tail<S> {
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
    pub(super) fn fold_hash_validate(&mut self) -> (std::sync::Arc<PendingDelta>, Vec<usize>) {
        // The shared view is taken after the serial prefix's mutations.
        let tx_results = Results(&self.ctx.results[..self.n]);
        let t_h = std::time::Instant::now();
        let n_res = tx_results.len();
        // Proves the `MAX_BLOCK_TXS` bound once, here, at the point
        // `n_res` is computed. Every index this tail derives from it
        // (the serial path and the chunked lanes) reuses this proof
        // instead of re-checking the bound as an `.expect(..)` claim.
        let count = super::config::BlockTxCount::new(n_res)
            .expect("admission caps a block at MAX_BLOCK_TXS");
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
                SerialTimers {
                    val_ns: &val_ns,
                    fold_ns: &fold_ns,
                },
            )
        } else {
            lanes_hash_and_validate(
                &LaneHashInput {
                    tx_results,
                    count,
                    n_res,
                    ctx,
                    delta_out,
                    recycle,
                    lanes,
                },
                &mut hashes,
                SerialTimers {
                    val_ns: &val_ns,
                    fold_ns: &fold_ns,
                },
            )
        };
        self.hash_ns = self.hash_ns.saturating_add(nanos(t_h.elapsed()));
        self.val_ns = val_ns.load(Ordering::Relaxed);
        self.fold_ns = fold_ns.load(Ordering::Relaxed);
        self.hashes = hashes;
        (delta_arc, wounded)
    }
}

/// Destructure a drained block: the heavy parts (the multi-version
/// cache and its versions, and the block's transaction slots) go to the
/// reaper; the light rest (the metrics) is returned. `S` (the
/// snapshots) stays inline in `ctx`, so no `'static` bound is needed on
/// the reap payload. Also feeds the stealing policy: mean
/// per-transaction execution time this block.
pub(super) fn reap_and_learn<S: StateDatabase>(
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
pub(super) fn span_and_ramp_us(m: &Metrics) -> (u64, u64) {
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

impl<S: StateDatabase + Sync> Tail<S> {
    /// Build the sealed outcome from the reaped metrics and the tail's
    /// own running totals.
    #[allow(
        clippy::cast_precision_loss,
        reason = "avg_batch is a diagnostic ratio of small counters, far below f64's 2^52 mantissa limit"
    )]
    pub(super) fn build_outcome(
        parts: super::tail::OutcomeParts,
        metrics: &Metrics,
        wounds: usize,
        t_commit: std::time::Duration,
    ) -> StmOutcome {
        let super::tail::OutcomeParts {
            receipts,
            delta,
            out_frags,
            bal_base,
            double_exit,
            hash_ns,
            delta_ns,
            fold_ns,
            avg_tx_ns,
            stats,
        } = parts;
        let super::tail::TailStats {
            cold,
            edges,
            dispatch,
            feed:
                super::metrics::FeedTimings {
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
        let m = metrics;
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
            commit_us: nanos(t_commit) / 1_000,
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
