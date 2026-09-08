//! The block tail's commit-and-repair half: the serial prefix pass, the
//! clean commit, wound repair, and the outcome build. See
//! `hash_validate.rs` for the fold, hash, and validate pass this half
//! calls into.

use super::config::{bal_index, nanos};
use super::graph::BlockCtx;
use super::hash_validate::{Results, reap_and_learn, result_mut, rewrite_frag_sink};
use super::metrics::{FeedTimings, Metrics, StmOutcome, TxResult};
use super::recycle::{RecyclePools, SpentBlock};
use super::session::{DeltaOut, MvRelease};
use crate::FEE_SINK;
use alloy_primitives::B256;
use alloy_primitives::U256;
use kardamom_exec_core::delta::PendingDelta;
use kardamom_exec_core::error::ExecutorError;
use kardamom_exec_core::exec_types::TxSlot;
use kardamom_exec_core::executor::Executor;
use kardamom_types::StateDatabase;
use revm::state::AccountInfo;
use std::collections::HashSet;
use std::sync::atomic::Ordering;

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
                entry.1.balance = self.sink_running;
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

/// One block's commit tail, held across its phases (extraction,
/// validation, wound repair, the canonical commit, and the teardown
/// hand-off): `run` calls the phase methods in order, each reading and
/// updating this same running state instead of passing it through
/// method arguments.
pub(super) struct Tail<S: StateDatabase + Sync> {
    pub(super) ctx: BlockCtx<S>,
    pub(super) n: usize,
    pub(super) delta_out: Option<DeltaOut>,
    timing: TailTiming,
    stats: TailStats,
    pub(super) deps: TailDeps,
    t_extract: std::time::Duration,
    prefix: Prefix,
    receipts: Vec<kardamom_types::Receipt>,
    delta: PendingDelta,
    pub(super) hashes: Vec<B256>,
    pub(super) hash_ns: u64,
    delta_ns: u64,
    pub(super) val_ns: u64,
    pub(super) fold_ns: u64,
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
        let fee_sum = self
            .ctx
            .results
            .iter()
            .take(self.n)
            .filter_map(std::sync::OnceLock::get)
            .filter_map(|r| r.as_ref().ok())
            .try_fold(U256::ZERO, |acc, r| {
                acc.checked_add(r.fee_delta).ok_or_else(|| {
                    ExecutorError::State(format!(
                        "stm: block {} fee-sum overflowed U256 summing per-tx fee credits",
                        self.ctx.env.block_number
                    ))
                })
            })?;
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
        let hashes = &self.hashes;
        let mut out_frags = std::mem::take(&mut self.out_frags);
        let mut receipts = std::mem::take(&mut self.receipts);
        for (i, cell) in self.ctx.results.iter_mut().take(n).enumerate() {
            Self::commit_one(cell, hashes[i], &mut out_frags, &mut receipts);
        }
        self.out_frags = out_frags;
        self.receipts = receipts;
        // The spent results (write sets and read logs) ride the arena
        // to the reaper, which harvests their read buffers and recycles
        // the whole array, with no leftover Vec and no per-block moves.
        self.delta_ns = self.delta_ns.saturating_add(nanos(t_d.elapsed()));
    }

    /// One result's clean-commit epilogue: patch the write-set hash
    /// when touched, then move out its BAL fragment and receipt. The
    /// `for` loop in [`Self::commit_clean`] stays free of a branch.
    fn commit_one(
        cell: &mut std::sync::OnceLock<Result<TxResult, ExecutorError>>,
        hash: B256,
        out_frags: &mut Vec<revm::state::bal::Bal>,
        receipts: &mut Vec<kardamom_types::Receipt>,
    ) {
        let r = result_mut(cell);
        if r.sink_touched {
            r.receipt.write_set_hash = hash;
        }
        out_frags.extend(r.bal_frag.take());
        receipts.push(std::mem::take(&mut r.receipt));
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
        for (i, r) in spent.into_iter().enumerate() {
            self.repair_one(i, r, wounded_set, &mut layered)?;
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

    /// One repaired transaction's step: re-execute against the
    /// corrected prefix when wounded, otherwise fold the original
    /// result the same way [`Self::serial_prefix`] did. The `for` loop
    /// in [`Self::repair_wounded`] stays free of a branch.
    ///
    /// # Errors
    /// Returns an error if a repair execution fails, or if the
    /// fee-sink accumulator overflows `U256`.
    fn repair_one(
        &mut self,
        i: usize,
        mut r: TxResult,
        wounded_set: &HashSet<usize>,
        layered: &mut PendingDelta,
    ) -> Result<(), ExecutorError> {
        if !wounded_set.contains(&i) {
            // Bounded by the block gas limit (see `serial_prefix`).
            self.prefix.apply(&mut r, " during repair")?;
            if r.sink_touched {
                r.receipt.write_set_hash = r.ws.hash();
            }
            self.out_frags.extend(r.bal_frag.take());
            layered.apply(r.ws.clone());
            self.delta.apply(r.ws);
            self.receipts.push(r.receipt);
            return Ok(());
        }
        // The slot holds the envelope for the whole block, so there is
        // no second copy in a parallel vec.
        let slot = self.ctx.slot(i);
        let (tx_idx, position, envelope) = (slot.tx_idx, slot.position, &slot.envelope);
        let mut scope = Executor::new(self.ctx.snapshots.primary(), Some(&*layered), self.ctx.env)?;
        // Repair capture replaces the wounded fragment: this execution
        // runs against the computed prefix, so its capture (fee sink
        // included) is canonical directly.
        let mut repair_frag = self.ctx.bal_base.map(|_| revm::state::bal::Bal::new());
        let bal_arg = match (repair_frag.as_mut(), self.ctx.bal_base) {
            (Some(f), Some(b)) => Some((f, bal_index(b, i as u64, self.ctx.env.block_number)?)),
            _ => None,
        };
        let slot = TxSlot {
            tx_idx,
            tx_position: position,
            tx_index_in_block: i as u64,
            cumulative_gas_used_before: self.prefix.cumulative,
        };
        let (mut receipt, ws) = scope.execute_tx(slot, envelope, bal_arg, None)?;
        self.prefix.cumulative = receipt.cumulative_gas_used;
        receipt.transaction_index = i as u64;
        layered.apply(ws.clone());
        self.delta.apply(ws);
        self.receipts.push(receipt);
        // A repaired skip captures nothing, the same hole a skipped
        // transaction leaves on the streaming path.
        self.out_frags
            .extend(repair_frag.filter(|f| !f.accounts.is_empty()));
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
            wounded_set.extend(first..self.n);
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
    /// reaper; the light rest returns as [`OutcomeParts`], for
    /// [`Self::build_outcome`] to read. `S` (the snapshots) stays
    /// inline, so no `'static` bound is needed on the reap payload.
    fn teardown(self) -> (OutcomeParts, Metrics) {
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
        let TailDeps {
            reaper,
            avg_tx_ns,
            recycle,
            ..
        } = deps;
        let bal_base = ctx.bal_base;
        let double_exit = ctx.double_exit.load(Ordering::SeqCst);
        let metrics = reap_and_learn(ctx, n, &avg_tx_ns, &reaper, &recycle);
        (
            OutcomeParts {
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
            },
            metrics,
        )
    }

    /// Reap the spent block and build its sealed outcome. `run` calls
    /// this last, after every phase (extraction, validation, wound
    /// repair, and the canonical commit) has run. See `hash_validate.rs`
    /// for [`Self::build_outcome`], which turns the reaped parts into
    /// the outcome.
    fn into_outcome(self, wounds: usize, t_commit: std::time::Duration) -> StmOutcome {
        let (parts, metrics) = self.teardown();
        Self::build_outcome(parts, &metrics, wounds, t_commit)
    }
}

/// [`Tail::teardown`]'s output: everything [`Tail::build_outcome`] needs
/// once the heavy per-transaction arrays have gone to the reaper.
pub(super) struct OutcomeParts {
    pub(super) receipts: Vec<kardamom_types::Receipt>,
    pub(super) delta: PendingDelta,
    pub(super) out_frags: Vec<revm::state::bal::Bal>,
    pub(super) bal_base: Option<u64>,
    pub(super) double_exit: u32,
    pub(super) hash_ns: u64,
    pub(super) delta_ns: u64,
    pub(super) fold_ns: u64,
    pub(super) avg_tx_ns: std::sync::Arc<std::sync::atomic::AtomicU64>,
    pub(super) stats: TailStats,
}
