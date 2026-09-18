//! The in-memory Mock-state A/B sweep: the harshest baseline, where a
//! read costs nothing, so the scheduler carries the largest possible
//! share of the measured time.

use std::time::Instant;

use kardamom_bench::stm::BlockAt;
use kardamom_engine::block_env::ExecEnv;
use kardamom_engine::delta::PendingDelta;
use kardamom_engine::exec_types::TxIndex;
use kardamom_engine::state::MockStateDatabase;
use kardamom_footprint::classifier::Stats;
use kardamom_stm::execute::execute_block_sequential;
use kardamom_types::{BPosition, TxEnvelope};

use super::args::Args;
use super::common::{BlockOutputs, EngineOpts, StatsExt, Workload, assert_identical, records};

/// Pass 0 is the timed sequential baseline, plus caches. For each
/// block: the pre-block delta (the base), the canonical outputs, and a
/// snapshot of the stats as they stood before the block, so every STM
/// sweep sees the exact inputs the streaming executor would.
struct BlockCase {
    env: ExecEnv,
    recs: Vec<(TxIndex, BPosition, TxEnvelope)>,
    base: PendingDelta,
    stats: Stats,
    seq: BlockOutputs,
    is_flow: bool,
    /// The upstream-prepared decode and prediction: what the `tx_data`
    /// reader threads will hand the executor. Timed separately, since
    /// it is not on the feed's serial path.
    prep_us: u64,
}

/// The sweep uses one persistent pool for each worker count, with
/// blocks streamed through it, so there is no per-block thread cost,
/// matching the executor pipeline shape.
#[derive(Default)]
pub(crate) struct Row {
    workers: usize,
    batch: usize,
    wall: f64,
    fallbacks: usize,
    feed_us: u64,
    prep_us: u64,
    redundant: u64,
    steals: u64,
    /// The dispatch imbalance: the busiest thread's share, against an
    /// even split. Domain-affinity assignment collides when the domain
    /// count is close to the worker count.
    imbalance: f64,
    imb_n: u64,
    idle_threads: usize,
    busy_us: u64,
    span_us: u64,
    ramp_us: u64,
    commit_us: u64,
    decode_us: u64,
    predict_us: u64,
    admit_us: u64,
    prune_us: u64,
    prune_calls: u64,
    prune_forced: u64,
    avg_batch: f64,
    idle_us: u64,
}

impl Row {
    fn new(workers: usize, batch: usize) -> Self {
        Self {
            workers,
            batch,
            ..Self::default()
        }
    }

    /// Fold one flow block's outcome into this row's running totals.
    /// Returns the block's `prune_calls`, the weight `avg_batch` folds
    /// by, so the caller can divide once at the end of the sweep.
    #[allow(
        clippy::cast_precision_loss,
        reason = "dispatch and prune counts stay far under 2^52 (the f64 mantissa) for any sweep this harness runs"
    )]
    fn accumulate(
        &mut self,
        out: &kardamom_stm::execute::StmOutcome,
        ms: f64,
        prep_us: u64,
    ) -> f64 {
        self.wall += ms;
        self.fallbacks += usize::from(out.fallback);
        self.feed_us += out.feed_us;
        self.prep_us += prep_us;
        self.busy_us += out.busy_us;
        self.span_us += out.parallel_span_us;
        self.ramp_us += out.ramp_us;
        self.commit_us += out.commit_us;
        let total: u32 = out.dispatch.iter().sum();
        let maxd = *out.dispatch.iter().max().unwrap_or(&0);
        let used = out.dispatch.iter().filter(|c| **c > 0).count();
        if total > 0 {
            self.imbalance += f64::from(maxd) * out.dispatch.len() as f64 / f64::from(total);
            self.idle_threads += out.dispatch.len() - used;
            self.imb_n += 1;
        }
        self.redundant += out.redundant_edges;
        self.steals += out.steals;
        self.decode_us += out.decode_us;
        self.predict_us += out.predict_us;
        self.admit_us += out.admit_us;
        self.prune_us += out.prune_us;
        self.prune_calls += out.prune_calls;
        self.prune_forced += out.prune_forced;
        self.idle_us += out.idle_us;
        self.avg_batch += out.avg_batch * out.prune_calls as f64;
        out.prune_calls as f64
    }

    /// The average dispatch imbalance over the blocks this row has
    /// seen, or 0 if it has seen none.
    #[allow(
        clippy::cast_precision_loss,
        reason = "imb_n stays far under 2^52 for any sweep this harness runs"
    )]
    fn avg_imbalance(&self) -> f64 {
        if self.imb_n > 0 {
            self.imbalance / self.imb_n as f64
        } else {
            0.0
        }
    }

    /// The percentage of wall time this row's workers spent busy, or 0
    /// if the row has no span yet.
    #[allow(
        clippy::cast_precision_loss,
        reason = "span_us and busy_us stay far under 2^52 for any sweep this harness runs"
    )]
    fn utilization_pct(&self) -> f64 {
        if self.span_us > 0 {
            self.busy_us as f64 / (self.workers as f64 * self.span_us as f64) * 100.0
        } else {
            0.0
        }
    }
}

/// The result of one mock-state A/B sweep.
pub(crate) struct MockAbReport {
    wall_seq: f64,
    flow_gas: u64,
    flow_txs: usize,
    agg_edges: usize,
    agg_cold: usize,
    rows: Vec<Row>,
}

/// The sequential baseline pass: one `BlockCase` per block, plus the
/// flow-only wall time, gas, and transaction totals.
struct BaselinePass {
    cases: Vec<BlockCase>,
    wall_seq: f64,
    flow_gas: u64,
    flow_txs: usize,
}

/// Prepare (decode and predict) every record in a block, upstream and
/// untimed in production; timed here to measure that cost in isolation.
fn prepare_all(recs: &[(TxIndex, BPosition, TxEnvelope)], stats_before: &Stats) {
    for (t, _, e) in recs {
        let _ = kardamom_stm::execute::Prepared::new(e, *t, stats_before);
    }
}

/// [`run_baseline_pass`]'s state, carried from one block to the next.
struct BaselineRun<'a> {
    snap: &'a MockStateDatabase,
    chain_id: u64,
    n_setup: usize,
    delta: PendingDelta,
    stats: Stats,
    global_idx: u64,
    wall_seq: f64,
    flow_gas: u64,
    flow_txs: usize,
    cases: Vec<BlockCase>,
}

impl BaselineRun<'_> {
    /// Run one block: sequential execution, capture (untimed), and the
    /// upstream prepare cost, measured once. Records the flow-window
    /// sample when `bi` is past setup.
    #[allow(
        clippy::cast_possible_truncation,
        reason = "a prep time in microseconds fits in a u64 for any run this harness measures (up to ~584,000 years)"
    )]
    fn run_block(&mut self, bi: usize, blk: &[TxEnvelope]) -> anyhow::Result<()> {
        let is_flow = bi >= self.n_setup;
        let env = BlockAt(bi).env(self.chain_id);
        let recs = records(self.global_idx, blk);
        self.global_idx += blk.len() as u64;
        let base = self.delta.clone();
        let stats_before = self.stats.clone();

        let t0 = Instant::now();
        let (seq_receipts, seq_delta) =
            execute_block_sequential(self.snap, Some(&self.delta), env, &recs)?;
        let seq_ms = t0.elapsed().as_secs_f64() * 1e3;
        if is_flow {
            self.wall_seq += seq_ms;
            self.flow_gas += seq_receipts.last().map_or(0, |r| r.cumulative_gas_used);
            self.flow_txs += recs.len();
        }

        // This is an untimed capture pass. It trains the stats for the
        // next block, using prior-blocks-only data, like the live shadow.
        self.stats
            .train_block(self.snap, &recs, env, Some(&self.delta))?;

        self.delta.merge_from(&seq_delta);
        // This is the cost of preparing this block upstream, measured
        // once. The sweep re-prepares for each run, so each timed pass
        // starts from raw inputs.
        let t_prep = Instant::now();
        prepare_all(&recs, &stats_before);
        let prep_us = t_prep.elapsed().as_micros() as u64;
        self.cases.push(BlockCase {
            env,
            recs,
            base,
            stats: stats_before,
            seq: BlockOutputs {
                receipts: seq_receipts,
                delta: seq_delta,
            },
            is_flow,
            prep_us,
        });
        Ok(())
    }
}

/// Run the timed sequential baseline, once, and capture what every STM
/// sweep needs: the canonical outputs, the pre-block delta, and a
/// snapshot of the stats as they stood before the block.
fn run_baseline_pass(w: &Workload<'_>, snap: &MockStateDatabase) -> anyhow::Result<BaselinePass> {
    let mut run = BaselineRun {
        snap,
        chain_id: w.chain_id,
        n_setup: w.n_setup,
        delta: PendingDelta::new(),
        stats: Stats::default(),
        global_idx: 0,
        wall_seq: 0.0,
        flow_gas: 0,
        flow_txs: 0,
        cases: Vec::with_capacity(w.all_blocks.len()),
    };
    for (bi, blk) in w.all_blocks.iter().enumerate() {
        run.run_block(bi, blk)?;
    }
    Ok(BaselinePass {
        cases: run.cases,
        wall_seq: run.wall_seq,
        flow_gas: run.flow_gas,
        flow_txs: run.flow_txs,
    })
}

/// The result of a full worker-count and prune-batch sweep.
#[derive(Default)]
struct Sweep {
    rows: Vec<Row>,
    edges: usize,
    cold: usize,
}

impl Sweep {
    /// Run one (worker-count, batch) configuration, and fold its
    /// result into `self`. `collect_edges` (the sweep's first
    /// configuration only) is `self.rows.is_empty()`.
    fn push_config(
        &mut self,
        eng: &EngineOpts,
        wk: std::num::NonZeroUsize,
        batch: std::num::NonZeroUsize,
        snap: &MockStateDatabase,
        cases: &[BlockCase],
    ) -> anyhow::Result<()> {
        let result = run_one_config(eng, wk, batch, snap, cases, self.rows.is_empty())?;
        self.edges += result.edges;
        self.cold += result.cold;
        self.rows.push(result.row);
        Ok(())
    }
}

/// One (worker-count, batch) configuration's row, plus the edges/cold
/// counts collected only when `collect_edges` is set (the sweep's very
/// first configuration: these are graph-structure counts, the same for
/// every worker count and batch, so one sample suffices).
struct ConfigResult {
    row: Row,
    edges: usize,
    cold: usize,
}

/// [`run_one_config`]'s running totals across `cases`.
#[derive(Default)]
struct ConfigAccum {
    row: Row,
    batch_weight: f64,
    edges: usize,
    cold: usize,
}

impl ConfigAccum {
    /// Run one case, and fold its outcome into the running totals if
    /// it is a flow case. A setup case executes, but does not count
    /// toward the row or the edge/cold totals.
    fn fold_case(
        &mut self,
        pool: &kardamom_stm::execute::PoolHandle<'_, MockStateDatabase>,
        wk: usize,
        snap: &MockStateDatabase,
        case: &BlockCase,
        collect_edges: bool,
    ) -> anyhow::Result<()> {
        let outcome = run_one_case(pool, wk, snap, case)?;
        if !case.is_flow {
            return Ok(());
        }
        self.batch_weight += self.row.accumulate(&outcome.out, outcome.ms, case.prep_us);
        if collect_edges {
            self.edges += outcome.out.edges;
            self.cold += outcome.out.cold;
        }
        Ok(())
    }
}

fn run_one_config(
    eng: &EngineOpts,
    wk: std::num::NonZeroUsize,
    batch: std::num::NonZeroUsize,
    snap: &MockStateDatabase,
    cases: &[BlockCase],
    collect_edges: bool,
) -> anyhow::Result<ConfigResult> {
    let cfg = eng.pool_config(wk, batch, true);
    let wk = wk.get();
    let mut accum = ConfigAccum {
        row: Row::new(wk, batch.get()),
        ..ConfigAccum::default()
    };
    kardamom_stm::execute::with_pool(cfg, |pool| -> anyhow::Result<()> {
        for case in cases {
            accum.fold_case(pool, wk, snap, case, collect_edges)?;
        }
        Ok(())
    })?;
    if accum.batch_weight > 0.0 {
        accum.row.avg_batch /= accum.batch_weight;
    }
    Ok(ConfigResult {
        row: accum.row,
        edges: accum.edges,
        cold: accum.cold,
    })
}

/// One case's timed pool run, checked byte for byte against its
/// sequential baseline.
struct CaseOutcome {
    out: kardamom_stm::execute::StmOutcome,
    ms: f64,
}

fn run_one_case(
    pool: &kardamom_stm::execute::PoolHandle<'_, MockStateDatabase>,
    wk: usize,
    snap: &MockStateDatabase,
    case: &BlockCase,
) -> anyhow::Result<CaseOutcome> {
    let base = case.base.clone();
    // This is the upstream stage. In production, the tx_data readers do
    // this; here it runs just ahead of the timer. The point is that it
    // is not on the executor's serial feed.
    let prepared: Vec<kardamom_stm::execute::PreparedTx> = case
        .recs
        .iter()
        .map(|(t, p, e)| kardamom_stm::execute::PreparedTx {
            tx_idx: *t,
            position: *p,
            envelope: e.clone(),
            prepared: kardamom_stm::execute::Prepared::new(e, *t, &case.stats),
        })
        .collect();
    let t = Instant::now();
    let out = pool.run_block_prepared(
        vec![snap.clone(); wk],
        base,
        case.env,
        prepared,
        &case.stats,
    )?;
    let ms = t.elapsed().as_secs_f64() * 1e3;
    assert_identical(&case.seq, &out, case.env.block_number, wk);
    Ok(CaseOutcome { out, ms })
}

/// Run the STM sweep over every worker count and prune batch in `eng`,
/// against the cases the baseline pass captured.
fn sweep_worker_counts(
    eng: &EngineOpts,
    batches: &[std::num::NonZeroUsize],
    snap: &MockStateDatabase,
    cases: &[BlockCase],
) -> anyhow::Result<Sweep> {
    let configs = eng
        .worker_counts
        .iter()
        .flat_map(|&wk| batches.iter().map(move |&batch| (wk, batch)));
    let mut sweep = Sweep::default();
    for (wk, batch) in configs {
        sweep.push_config(eng, wk, batch, snap, cases)?;
    }
    Ok(sweep)
}

pub(crate) fn run_mock_ab(
    w: &Workload<'_>,
    eng: &EngineOpts,
    batches: &[std::num::NonZeroUsize],
    snap: &MockStateDatabase,
) -> anyhow::Result<MockAbReport> {
    let baseline = run_baseline_pass(w, snap)?;
    let sweep = sweep_worker_counts(eng, batches, snap, &baseline.cases)?;
    Ok(MockAbReport {
        wall_seq: baseline.wall_seq,
        flow_gas: baseline.flow_gas,
        flow_txs: baseline.flow_txs,
        agg_edges: sweep.edges,
        agg_cold: sweep.cold,
        rows: sweep.rows,
    })
}

/// Print the sweep header and the flow summary, then the per-row
/// tables.
pub(crate) fn print_mock_rows(a: &Args, report: &MockAbReport) {
    println!(
        "\n===== STM-P2 A/B [{}] pairs={} senders={} blocks={}x{} =====",
        a.scenario, a.pairs, a.senders, a.blocks, a.block_size
    );
    print_flow_summary(report);
    print_rows(report);
    print_wall_breakdown(report);
    println!("BYTE-IDENTICAL: every block, every worker count — verified");
}

#[allow(
    clippy::cast_precision_loss,
    reason = "gas and microsecond display values stay far under 2^52"
)]
fn print_flow_summary(report: &MockAbReport) {
    println!(
        "flow: txs={} gas={:.3}Ggas seq_wall={:.0}ms ({:.0} Mgas/s) sched: edges={} cold={}",
        report.flow_txs,
        report.flow_gas as f64 / 1e9,
        report.wall_seq,
        report.flow_gas as f64 / 1e6 / (report.wall_seq / 1e3),
        report.agg_edges,
        report.agg_cold,
    );
}

/// Print one row per worker count and prune batch: the timing table,
/// the steals and redundancy line, and the dispatch-imbalance summary.
#[allow(
    clippy::cast_precision_loss,
    reason = "gas, microsecond, and count display values stay far under 2^52"
)]
fn print_rows(report: &MockAbReport) {
    println!(
        "{:>3} {:>6} {:>9} {:>8} {:>9} {:>10} {:>10} {:>7} {:>9} {:>9} {:>6}",
        "w",
        "batch",
        "wall_ms",
        "speedup",
        "mgas/s",
        "feed_us",
        "decode_us",
        "pred_us",
        "prune_us",
        "idle_us",
        "wound"
    );
    for r in &report.rows {
        println!(
            "{:>3} {:>6} {:>9.0} {:>7.2}x {:>9.0} {:>10} {:>10} {:>7} {:>9} {:>9} {:>6.2}",
            r.workers,
            r.batch,
            r.wall,
            report.wall_seq / r.wall,
            report.flow_gas as f64 / 1e6 / (r.wall / 1e3),
            r.feed_us,
            r.prep_us,
            r.predict_us,
            r.prune_us,
            r.idle_us,
            r.avg_imbalance(),
        );
    }
    for r in &report.rows {
        println!(
            "  w={} steals={} redundant_edges={} idle_threads={} avg_batch={:.1}",
            r.workers, r.steals, r.redundant, r.idle_threads, r.avg_batch
        );
    }
    let tot_idle_threads: usize = report.rows.iter().map(|r| r.idle_threads).sum();
    let tot_blocks: u64 = report.rows.iter().map(|r| r.imb_n).sum();
    println!(
        "DISPATCH: empty-thread-slots {tot_idle_threads} over {tot_blocks} block-runs (domain-affinity collisions)"
    );
}

/// Print the wall-time breakdown (ramp, span, commit, busy, and
/// utilization) for each worker count.
#[allow(
    clippy::cast_precision_loss,
    reason = "microsecond and percentage display values stay far under 2^52"
)]
fn print_wall_breakdown(report: &MockAbReport) {
    println!(
        "\n----- WHERE THE WALL TIME GOES (per worker count) -----\n{:>3} {:>10} {:>10} {:>10} {:>10} {:>12}",
        "w", "ramp_ms", "span_ms", "commit_ms", "busy_ms", "utilization"
    );
    for r in &report.rows {
        println!(
            "{:>3} {:>10.1} {:>10.1} {:>10.1} {:>10.1} {:>11.1}%",
            r.workers,
            r.ramp_us as f64 / 1000.0,
            r.span_us as f64 / 1000.0,
            r.commit_us as f64 / 1000.0,
            r.busy_us as f64 / 1000.0,
            r.utilization_pct()
        );
    }
}
