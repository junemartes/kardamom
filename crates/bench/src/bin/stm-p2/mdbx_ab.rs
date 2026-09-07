//! The mdbx-backed A/B: the honest baseline.
//!
//! Every other number in this harness is measured against in-memory
//! Mock state, where a read costs a hash lookup and sequential
//! execution runs fast. That is the most hostile possible comparison
//! for a parallel engine, since it maximizes the scheduler's share of
//! the work. Here reads go to the real backend, so per-transaction
//! execution costs what it costs in production, and the coordination
//! overhead is measured against the right denominator.
//!
//! State is monotonic here: a committed block cannot be un-committed.
//! So the sweep cannot replay a block for each worker count against a
//! rewound database. Instead, each configuration gets a fresh database
//! and replays the whole sequence, with both engines reading the same
//! snapshot before it advances: snapshot, then sequential (timed,
//! canonical outputs), then STM (timed, checked byte for byte), then
//! commit the sequential delta, then the next snapshot.

use std::time::Instant;

use kardamom_engine::block_env::ExecEnv;
use kardamom_engine::delta::PendingDelta;
use kardamom_engine::exec_types::TxIndex;
use kardamom_footprint::classifier::Stats;
use kardamom_state::WriteBatch;
use kardamom_stm::execute::{PoolHandle, execute_block_sequential};
use kardamom_types::{BPosition, TxEnvelope};

use kardamom_bench::stm::BlockAt;

use super::alloc::{AllocDelta, snapshot as alloc_snapshot};
use super::common::{
    BlockOutputs, EngineOpts, MdbxHandles, RunOpts, StatsExt, Workload, assert_identical,
    open_mdbx_env, records,
};
use super::mdbx_report::{BlockSample, MdbxAbTotals, print_block_detail};

/// One worker-count/batch run of the mdbx A/B: the fixed configuration
/// plus the state that carries from one block to the next.
struct MdbxRun<'a> {
    w: &'a Workload<'a>,
    run: &'a RunOpts,
    wk: usize,
    env_for_reads: &'a kardamom_state::StateEnv,
    writer: &'a kardamom_state::WriterHandle,
    snapshot: kardamom_state::StateSnapshot,
    stats: Stats,
    global_idx: u64,
    totals: MdbxAbTotals,
}

impl MdbxRun<'_> {
    /// Run both engines on one block: sequential (the canonical
    /// output), then the pool (checked byte for byte against it), then
    /// commit the canonical delta and wait for the writer's
    /// post-commit snapshot.
    ///
    /// A view-open time in microseconds fits in a `u64` for any run
    /// this harness measures (up to ~584,000 years).
    #[allow(
        clippy::cast_possible_truncation,
        reason = "a view-open time in microseconds fits in a u64 for any run this harness measures (up to ~584,000 years)"
    )]
    fn run_block(
        &mut self,
        pool: &PoolHandle<'_, kardamom_state::StateSnapshot>,
        bi: usize,
        blk: &[TxEnvelope],
    ) -> anyhow::Result<()> {
        let is_flow = bi >= self.w.n_setup + self.w.warmup_blocks;
        let e = self.w.env_for(bi);
        let recs = records(self.global_idx, blk);
        self.global_idx += blk.len() as u64;
        let prog = std::env::var_os("KARDAMOM_BENCH_PROGRESS").is_some();
        if prog {
            eprintln!("[prog] block {bi}: seq start");
        }

        let seq = execute_sequential_side(&self.snapshot, e, &recs)?;

        if prog {
            eprintln!("[prog] block {bi}: seq done, preparing");
        }
        let prepared: Vec<_> = recs
            .iter()
            .map(|(t, _, en)| kardamom_stm::execute::prepare(en, *t, &self.stats))
            .collect();
        let t1 = Instant::now();
        // One independent view per worker, all at the block the writer
        // just published.
        let t_snap = Instant::now();
        let views = super::common::open_views(self.env_for_reads, self.wk)?;
        let snap_open = t_snap.elapsed().as_micros() as u64;
        let a1 = alloc_snapshot();
        if prog {
            eprintln!("[prog] block {bi}: pool submit");
        }
        let mut out =
            pool.run_block_prepared(views, PendingDelta::new(), e, &recs, prepared, &self.stats)?;
        let p_ms = t1.elapsed().as_secs_f64() * 1e3;
        let a2 = alloc_snapshot();
        let seq_alloc = AllocDelta::since(&seq.alloc_before, &a1);
        let stm_alloc = AllocDelta::since(&a1, &a2);
        if !seq.stm_only {
            assert_identical(&seq.outputs, &out, e.block_number, self.wk);
        }
        // This is off the measured window; a2 is snapped above. The
        // delta shell goes back to the fold pool.
        pool.recycle_delta(std::mem::take(&mut out.delta));
        if prog {
            eprintln!("[prog] block {bi}: pool done");
        }
        if self.run.per_block && is_flow {
            print_block_detail(e.block_number, &seq.outputs.receipts, &out);
        }
        if is_flow {
            self.totals.accumulate(&BlockSample {
                out: &out,
                seq_ms: seq.ms,
                stm_ms: p_ms,
                snap_open_us: snap_open,
                txs: recs.len() as u64,
                seq_alloc: &seq_alloc,
                stm_alloc: &stm_alloc,
            });
        }

        // Train on this block, using prior-blocks-only stats, then
        // commit it, so the next block reads it from mdbx.
        self.stats.train_block(&self.snapshot, &recs, e, None)?;
        let boundary = BlockAt(bi).boundary(self.global_idx);
        // In stm-only mode, the pool's delta is the only one. When both
        // engines run, the code checks they are byte-identical, so this
        // is the same state either way.
        let (fin_delta, fin_receipts) = if seq.stm_only {
            (out.delta, out.receipts)
        } else {
            (seq.outputs.delta, seq.outputs.receipts)
        };
        // Mirror the committed delta into the pool's pool-lifetime
        // backend cache before the writer applies it, so the next
        // block's reads hit warm entries instead of mdbx: every hot
        // cell in parcounter changes every block, so a per-block cache
        // alone could never hit.
        pool.advance_base(&fin_delta);
        let bd = fin_delta.finalize(e.block_number, fin_receipts);
        self.writer.delta_tx.send(WriteBatch::new(boundary, bd))?;
        // Wait for the writer to publish the post-commit view.
        self.snapshot = super::common::wait_for_block(self.writer, e.block_number);
        Ok(())
    }

    /// Run every block in `self.w.all_blocks`, in order.
    fn run_all(
        &mut self,
        pool: &PoolHandle<'_, kardamom_state::StateSnapshot>,
    ) -> anyhow::Result<()> {
        for (bi, blk) in self.w.all_blocks.iter().enumerate() {
            self.run_block(pool, bi, blk)?;
        }
        Ok(())
    }
}

/// The result of [`execute_sequential_side`].
struct SeqSide {
    outputs: BlockOutputs,
    ms: f64,
    stm_only: bool,
    alloc_before: AllocDelta,
}

/// Run the sequential engine on this block, the canonical output. Both
/// engines read the same snapshot, before it moves.
///
/// Setting `KARDAMOM_STM_ONLY` runs pool blocks back-to-back, with no
/// sequential run between them, so the worker core never parks between
/// an interleaved sequential run and the next pool block: a ramping
/// core clock would otherwise show up as a uniform slowdown across
/// every timed phase.
///
/// Decode once, outside both timers, and hand it to the sequential
/// engine the way `prepare` hands it to the parallel one, so decode
/// cost never lands on only one side's ratio.
fn execute_sequential_side(
    snapshot: &kardamom_state::StateSnapshot,
    e: ExecEnv,
    recs: &[(TxIndex, BPosition, TxEnvelope)],
) -> anyhow::Result<SeqSide> {
    let stm_only = std::env::var_os("KARDAMOM_STM_ONLY").is_some();
    let seq_decoded: Vec<Option<kardamom_stm::DecodedTx>> = recs
        .iter()
        .map(|(t, _, en)| kardamom_stm::DecodedTx::decode(&en.raw_tx, *t).ok())
        .collect();
    // Snapshot right before the timed sequential execution: decode's
    // allocations above are not part of this measured window.
    let a0 = alloc_snapshot();
    let t0 = Instant::now();
    let (seq_receipts, seq_delta) = if stm_only {
        (Vec::new(), PendingDelta::new())
    } else if std::env::var_os("KARDAMOM_SEQ_ON_THREAD").is_some() {
        // This is a discriminator: the pool's per-transaction cost is
        // notably higher than sequential's for pure-interpreter work.
        // Run the same sequential engine on a spawned thread pinned to
        // a worker core. If it slows to the pool's rate, the tax is
        // thread context: stack, arena, or placement. If it stays
        // fast, the tax is in the pool's own execution path.
        std::thread::scope(|sc| {
            sc.spawn(|| {
                let core: usize = std::env::var("KARDAMOM_SEQ_CORE")
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(3);
                let _ = core_affinity::set_for_current(core_affinity::CoreId { id: core });
                execute_block_sequential(snapshot, None, e, recs)
            })
            .join()
            .expect("seq thread")
        })?
    } else {
        kardamom_stm::execute::execute_block_sequential_decoded(
            snapshot,
            None,
            e,
            recs,
            &seq_decoded,
        )?
    };
    let s_ms = t0.elapsed().as_secs_f64() * 1e3;
    Ok(SeqSide {
        outputs: BlockOutputs {
            receipts: seq_receipts,
            delta: seq_delta,
        },
        ms: s_ms,
        stm_only,
        alloc_before: a0,
    })
}

/// A fresh mdbx environment's handles, plus the writer's initial
/// published snapshot, right after seeding.
struct MdbxRig {
    handles: MdbxHandles,
    snapshot: kardamom_state::StateSnapshot,
}

/// Open a fresh mdbx environment seeded with `genesis`, and return its
/// handles alongside the writer's initial published snapshot.
fn mdbx_rig(genesis: &[kardamom_types::AccountChange]) -> anyhow::Result<MdbxRig> {
    let handles = open_mdbx_env(genesis)?;
    let snapshot = super::common::initial_snapshot(&handles.writer);
    Ok(MdbxRig { handles, snapshot })
}

#[allow(
    clippy::similar_names,
    reason = "stats (footprint stats) and state (the per-block loop state) name different things; both are the clearest name for what they hold"
)]
pub(crate) fn run_mdbx_ab(w: &Workload<'_>, eng: &EngineOpts, run: &RunOpts) -> anyhow::Result<()> {
    let genesis = w.genesis();

    println!(
        "\n===== STM-P2 A/B [mdbx-backed state] =====\n{:>3} {:>8} {:>8} {:>8} {:>8} {:>8} {:>8} {:>8} {:>8} {:>7}",
        "w",
        "seq_ms",
        "stm_ms",
        "speedup",
        "busy_ms",
        "span_ms",
        "commit_ms",
        "feed_ms",
        "snap_ms",
        "util"
    );

    let configs = eng
        .worker_counts
        .iter()
        .flat_map(|&wk| run.prune_batches.iter().map(move |&batch| (wk, batch)));
    for (wk, batch) in configs {
        run_one_config(w, eng, run, &genesis, wk, batch)?;
    }
    println!("BYTE-IDENTICAL: every block, every worker count — verified");
    Ok(())
}

/// Run every block once, at one (worker count, prune batch) pair: open a
/// fresh mdbx env, drive the whole `w.all_blocks` sequence through both
/// engines, and print the totals row.
fn run_one_config(
    w: &Workload<'_>,
    eng: &EngineOpts,
    run: &RunOpts,
    genesis: &[kardamom_types::AccountChange],
    wk: usize,
    batch: usize,
) -> anyhow::Result<()> {
    let rig = mdbx_rig(genesis)?;
    // Keep an env handle. Each worker needs its own read transaction,
    // since mdbx serializes reads through one transaction's mutex.
    let env_for_reads = &rig.handles.env_for_reads;
    let writer = &rig.handles.writer;

    let cfg = eng.pool_config(wk, batch, true);
    let mut run_state = MdbxRun {
        w,
        run,
        wk,
        env_for_reads,
        writer,
        snapshot: rig.snapshot,
        stats: Stats::default(),
        global_idx: 0,
        totals: MdbxAbTotals::new(wk),
    };

    kardamom_stm::execute::with_pool(cfg, |pool| run_state.run_all(pool))?;

    run_state.totals.print(wk);
    Ok(())
}
