//! `kardamom-stm-p0` produces the offline Block-STM report: oracle
//! critical-path analysis and footprint-classifier grading over real
//! workloads, offline, through the real engine.

use anyhow::Context;
use clap::Parser;
use kardamom_bench::ANVIL_MNEMONIC;
use kardamom_bench::mnemonic;
use kardamom_bench::stm::{Cell, StatsExt, TxObs, capture, classifier, oracle, uniswap};
use kardamom_types::TxEnvelope;
use std::collections::HashSet;
use std::num::NonZeroUsize;

/// Default `--pairs`.
const DEFAULT_PAIRS: NonZeroUsize = NonZeroUsize::new(4).unwrap();
/// Default `--senders`.
const DEFAULT_SENDERS: NonZeroUsize = NonZeroUsize::new(12).unwrap();

#[derive(Parser, Debug)]
#[command(name = "kardamom-stm-p0")]
struct Args {
    /// One of: uniswap, defi, transfers.
    #[arg(long, default_value = "uniswap")]
    scenario: String,
    /// Non-zero: an empty pair set leaves every flow op with nothing
    /// to divide by (see `stm::uniswap::UniswapParams::pairs`).
    #[arg(long, default_value_t = DEFAULT_PAIRS)]
    pairs: NonZeroUsize,
    /// Non-zero: `stm::workload::defi_blocks` divides the flow
    /// transaction budget by this count.
    #[arg(long, default_value_t = DEFAULT_SENDERS)]
    senders: NonZeroUsize,
    #[arg(long, default_value_t = 40)]
    blocks: usize,
    #[arg(long, default_value_t = 200)]
    block_size: usize,
    /// The percentage of flow operations that are swaps, for the
    /// uniswap scenario.
    #[arg(long, default_value_t = 70)]
    swap_share: u64,
    /// The percentage of swaps that hit a pair other than the home pair.
    #[arg(long, default_value_t = 10)]
    cross: u64,
    #[arg(long, default_value_t = 0.5)]
    train_frac: f64,
    /// Apply the accumulator treatment: exclude universal-write cells,
    /// touched by more than 95% of transactions, from the conflict graphs.
    #[arg(long, default_value_t = false)]
    accumulator: bool,
    /// Run the shadow replay: stream the capture through the live
    /// shadow loop, meaning predict with prior-blocks-only stats, grade,
    /// then train, for each block, from a cold start, and print the
    /// per-block curve the executor's footprint-shadow thread would
    /// emit. This is the shadow measurement, offline, with the same
    /// `grade_block`, cap, and exclusion the live thread uses.
    #[arg(long, default_value_t = false)]
    shadow: bool,
    #[arg(long, default_value = ".")]
    repo_root: String,
    #[arg(long, default_value_t = 412346)]
    chain_id: u64,
    /// An optional JSON report path.
    #[arg(long)]
    json: Option<String>,
}

/// A display ratio that reads 0.0, not a divide-by-zero panic or a
/// misleadingly clamped denominator, when `den` is 0 (an empty
/// capture: no observations at all).
#[allow(
    clippy::cast_precision_loss,
    reason = "these counts stay far under 2^52 (the f64 mantissa) for any offline analysis run this binary does"
)]
fn ratio(num: u64, den: u64) -> f64 {
    if den == 0 {
        0.0
    } else {
        num as f64 / den as f64
    }
}

/// [`shadow_replay`]'s running totals, folded one block at a time by
/// [`Self::fold_block`].
#[derive(Default)]
struct GradeTotals {
    gas: u64,
    cp_pred: u64,
    cp_orac: u64,
    miss: usize,
    over: usize,
    edges: usize,
    hit: usize,
    actual: usize,
    cold: usize,
    txs: usize,
}

impl GradeTotals {
    /// Grade block `b`'s `txs` against `stats` as they stood before
    /// this block, print the row, fold the block's grade into the
    /// running totals, then train `stats` on `txs`.
    fn fold_block(
        &mut self,
        stats: &mut classifier::Stats,
        b: u64,
        txs: &[TxObs],
        exclude: &HashSet<Cell>,
        grade_cap: usize,
    ) {
        use kardamom_footprint::grade::grade_block;
        let g = grade_block(stats, txs, exclude, grade_cap);
        println!(
            "{:>5} {:>5} {:>5} {:>9.4} {:>6} {:>6} {:>7} {:>7} {:>7.2}x {:>7.2}x",
            b,
            g.txs,
            g.cold_txs,
            g.hit_rate(),
            g.predicted_waves,
            g.predicted_width,
            g.missed_pairs,
            g.false_pairs,
            g.predicted_cp_ratio(),
            g.oracle_cp_ratio(),
        );
        self.gas += g.gas;
        self.cp_pred += g.predicted_cp_gas;
        self.cp_orac += g.oracle_cp_gas;
        self.miss += g.missed_pairs;
        self.over += g.false_pairs;
        self.edges += g.predicted_edges;
        self.hit += g.cells_hit;
        self.actual += g.cells_actual;
        self.cold += g.cold_txs;
        self.txs += g.txs;
        stats.learn_all(txs);
    }
}

/// The shadow loop, replayed offline: for each block, in stream order,
/// grade with the stats as they stood before the block, then train on
/// it. The executor's footprint-shadow thread does exactly this at
/// each boundary; see `engine::shadow`. `GRADE_CAP` mirrors the live constant.
fn shadow_replay(obs: &[TxObs], exclude: &HashSet<Cell>) {
    const GRADE_CAP: usize = 2_048;
    let max_block = obs.iter().map(|o| o.block).max().unwrap_or(0);
    let mut stats = classifier::Stats::default();
    println!("\n----- P1 SHADOW REPLAY (cold start, stream order) -----");
    println!(
        "{:>5} {:>5} {:>5} {:>9} {:>6} {:>6} {:>7} {:>7} {:>8} {:>8}",
        "block", "txs", "cold", "hit_rate", "waves", "width", "miss", "over", "cp_pred", "cp_orac"
    );
    let blocks_with_txs = (1..=max_block).filter_map(|b| {
        let txs: Vec<TxObs> = obs.iter().filter(|o| o.block == b).cloned().collect();
        (!txs.is_empty()).then_some((b, txs))
    });
    let mut totals = GradeTotals::default();
    for (b, txs) in blocks_with_txs {
        totals.fold_block(&mut stats, b, &txs, exclude, GRADE_CAP);
    }
    println!(
        "SHADOW AGG: txs={} cold={} hit_rate={:.4} cp_pred={:.2}x cp_oracle={:.2}x \
         false_independent={} ({:.4}/tx) over_merge={} ({:.2}% of {} predicted edges)",
        totals.txs,
        totals.cold,
        ratio(totals.hit as u64, totals.actual as u64),
        ratio(totals.gas, totals.cp_pred),
        ratio(totals.gas, totals.cp_orac),
        totals.miss,
        ratio(totals.miss as u64, totals.txs as u64),
        totals.over,
        ratio(totals.over as u64, totals.edges as u64) * 100.0,
        totals.edges,
    );
}

#[allow(
    clippy::cast_precision_loss,
    reason = "gas and exec-time display values stay far under 2^52 (the f64 mantissa) for any offline analysis run this binary does"
)]
fn main() -> anyhow::Result<()> {
    let a = Args::parse();
    let signer_count = kardamom_bench::signers::signer_count(a.senders)?;
    let signers = mnemonic::derive_signers(ANVIL_MNEMONIC, signer_count)?;

    let snap = kardamom_bench::stm::workload::funded_snapshot(&signers);

    let blocks = build_blocks(&a, &signers)?;
    eprintln!(
        "==> executing {} blocks ({} setup) through the engine...",
        blocks.all.len(),
        blocks.n_setup
    );
    let t0 = std::time::Instant::now();
    let obs_all = capture::run_capture(&snap, &blocks.all, a.chain_id)?;
    let exec_s = t0.elapsed().as_secs_f64();
    // These are flow-only observations, re-based so block numbers start at 1.
    let obs: Vec<TxObs> = obs_all
        .into_iter()
        .filter(|o| o.block > blocks.n_setup as u64)
        .map(|mut o| {
            o.block -= blocks.n_setup as u64;
            o
        })
        .collect();
    let gas: u64 = obs.iter().map(|o| o.gas).sum();
    eprintln!(
        "==> captured {} flow txs, {:.3} Ggas, sequential exec {:.1}s ({:.0} Mgas/s incl. capture)",
        obs.len(),
        gas as f64 / 1e9,
        exec_s,
        gas as f64 / 1e6 / exec_s
    );

    // The accumulator treatment: exclude universal write cells, if asked.
    let exclude: HashSet<Cell> = if a.accumulator {
        oracle::universal_writes(&obs, 0.95)
            .into_iter()
            .map(|(c, _)| c)
            .collect()
    } else {
        HashSet::new()
    };

    let t1 = std::time::Instant::now();
    let report = oracle::analyze(&obs, a.train_frac, &exclude);
    eprintln!("==> analysis {:.1}s", t1.elapsed().as_secs_f64());

    if a.shadow {
        shadow_replay(&obs, &exclude);
    }

    println!(
        "\n===== STM-P0 [{}] pairs={} senders={} blocks={}x{} accumulator={} =====",
        a.scenario, a.pairs, a.senders, a.blocks, a.block_size, a.accumulator
    );
    print!("{}", report.summary());

    // The classifier class shares, learned over all flow observations,
    // for reporting only.
    print_classifier(&obs);

    if let Some(path) = &a.json {
        write_json_report(path, &a, &report, gas)?;
    }
    Ok(())
}

/// All of a scenario's blocks, setup first, plus the setup count.
struct Blocks {
    all: Vec<Vec<TxEnvelope>>,
    n_setup: usize,
}

/// Build the setup and flow blocks for `a.scenario`.
fn build_blocks(
    a: &Args,
    signers: &[kardamom_bench::signers::DerivedSigner],
) -> anyhow::Result<Blocks> {
    let blocks = match a.scenario.as_str() {
        "uniswap" => {
            let w = uniswap::generate(
                &a.repo_root,
                signers,
                uniswap::UniswapParams {
                    chain_id: a.chain_id,
                    pairs: a.pairs,
                    flow_blocks: a.blocks,
                    txs_per_block: NonZeroUsize::new(a.block_size)
                        .context("--block-size must be non-zero for the uniswap scenario")?,
                    swap_share_pct: a.swap_share,
                    cross_pct: a.cross,
                },
            )?;
            kardamom_bench::stm::workload::ScenarioBlocks {
                setup: w.setup_blocks,
                flows: w.flow_blocks,
            }
        }
        "defi" => kardamom_bench::stm::workload::defi_blocks(
            &kardamom_bench::signers::SignerSet::new(signers.to_vec())?,
            a.chain_id,
            a.blocks,
            a.block_size,
            a.senders,
        )?,
        "transfers" => kardamom_bench::stm::workload::transfers_blocks(
            signers,
            a.chain_id,
            a.blocks,
            a.block_size,
        )?,
        other => anyhow::bail!("unknown scenario {other}"),
    };
    let n_setup = blocks.setup.len();
    let mut all = blocks.setup;
    all.extend(blocks.flows);
    Ok(Blocks { all, n_setup })
}

/// Print the class-share summary: how much of the workload the
/// classifier predicts as a fixed-slot access, over every flow
/// observation.
fn print_classifier(obs: &[TxObs]) {
    let stats = classifier::Stats::learn(obs);
    let (fixed, total) = stats.class_shares();
    println!(
        "CLASSIFIER: selectors={} slot-obs={} predicted-fixed={:.1}% unmodelled={:.1}%",
        stats.by_selector.len(),
        total,
        ratio(fixed, total) * 100.0,
        ratio(total - fixed, total) * 100.0,
    );
}

/// Write the oracle report, and the run's scenario knobs, as JSON.
fn write_json_report(
    path: &str,
    a: &Args,
    report: &oracle::Report,
    gas: u64,
) -> anyhow::Result<()> {
    let blocks: Vec<serde_json::Value> = report
        .blocks
        .iter()
        .map(|b| {
            serde_json::json!({"block": b.block, "txs": b.txs, "gas": b.gas,
                "cp_gas": b.critical_path_gas, "pairs": b.conflict_pairs})
        })
        .collect();
    let g = report.grading.as_ref().map(|g| {
        serde_json::json!({"holdout_txs": g.holdout_txs, "cold": g.cold_txs,
            "missed_pairs": g.missed_pairs, "false_pairs": g.false_pairs,
            "true_pairs": g.true_pairs, "predicted_pairs": g.predicted_pairs,
            "predicted_cp_gas": g.predicted_cp_gas, "oracle_cp_gas": g.oracle_cp_gas,
            "gas": g.gas})
    });
    std::fs::write(
        path,
        serde_json::to_vec_pretty(&serde_json::json!({
            "scenario": a.scenario, "pairs": a.pairs, "senders": a.senders,
            "blocks": a.blocks, "block_size": a.block_size,
            "accumulator": a.accumulator, "flow_gas": gas,
            "block_oracle": blocks, "grading": g,
        }))?,
    )?;
    eprintln!("==> wrote {path}");
    Ok(())
}
