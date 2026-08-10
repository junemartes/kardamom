//! `kardamom-stm-p0` — Block-STM P0 report: oracle critical-path analysis and
//! footprint-classifier grading over real workloads, offline, through
//! the real engine. (spec: docs/agents/block-stm-executor-spec.md)

use alloy_primitives::U256;
use clap::Parser;
use kardamom_bench::load::defi;
use kardamom_bench::mnemonic;
use kardamom_bench::stm::{Cell, capture, classifier, oracle, uniswap};
use kardamom_engine::state::MockStateDatabase;
use kardamom_types::TxEnvelope;
use std::collections::HashSet;

const ANVIL_MNEMONIC: &str = "test test test test test test test test test test test junk";

#[derive(Parser, Debug)]
#[command(name = "kardamom-stm-p0")]
struct Args {
    /// uniswap | defi | transfers
    #[arg(long, default_value = "uniswap")]
    scenario: String,
    #[arg(long, default_value_t = 4)]
    pairs: usize,
    #[arg(long, default_value_t = 12)]
    senders: usize,
    #[arg(long, default_value_t = 40)]
    blocks: usize,
    #[arg(long, default_value_t = 200)]
    block_size: usize,
    /// % of flow ops that are swaps (uniswap scenario).
    #[arg(long, default_value_t = 70)]
    swap_share: u64,
    /// % of swaps hitting a non-home pair.
    #[arg(long, default_value_t = 10)]
    cross: u64,
    #[arg(long, default_value_t = 0.5)]
    train_frac: f64,
    /// Apply the Accumulator treatment: exclude universal-write cells
    /// (>95% of txs) from the conflict graphs.
    #[arg(long, default_value_t = false)]
    accumulator: bool,
    /// P1 shadow replay: stream the capture through the LIVE shadow loop
    /// (predict with prior-blocks-only stats -> grade -> train, per block,
    /// cold start) and print the per-block curve the executor's
    /// footprint-shadow thread would emit. This is the P1 measurement,
    /// offline: same grade_block, same cap, same exclusion.
    #[arg(long, default_value_t = false)]
    shadow: bool,
    #[arg(long, default_value = ".")]
    repo_root: String,
    #[arg(long, default_value_t = 412346)]
    chain_id: u64,
    /// Predict from directly-observed hashes only (tier-1 accounts +
    /// tier-3 fixed slots), with NO keccak inversion and no derived
    /// mapping keys — the experiment that prices tier-2.
    #[arg(long, default_value_t = false)]
    no_derived: bool,
    /// Optional JSON report path.
    #[arg(long)]
    json: Option<String>,
}

/// The P1 loop, replayed offline: per block in stream order, grade with the
/// stats as they stood BEFORE the block, then train on it — the executor's
/// footprint-shadow thread does exactly this per boundary (engine::shadow).
/// GRADE_CAP mirrors the live constant.
fn shadow_replay(obs: &[capture::TxObs], exclude: &HashSet<Cell>) {
    use kardamom_footprint::grade::grade_block;
    const GRADE_CAP: usize = 2_048;
    let max_block = obs.iter().map(|o| o.block).max().unwrap_or(0);
    let mut stats = classifier::Stats::default();
    println!("\n----- P1 SHADOW REPLAY (cold start, stream order) -----");
    println!(
        "{:>5} {:>5} {:>5} {:>9} {:>6} {:>6} {:>7} {:>7} {:>8} {:>8}",
        "block", "txs", "cold", "hit_rate", "waves", "width", "miss", "over", "cp_pred", "cp_orac"
    );
    let (mut sum_gas, mut sum_cp_pred, mut sum_cp_orac) = (0u64, 0u64, 0u64);
    let (mut sum_miss, mut sum_over, mut sum_edges) = (0usize, 0usize, 0usize);
    let (mut sum_hit, mut sum_actual, mut sum_cold, mut sum_txs) = (0usize, 0usize, 0usize, 0usize);
    for b in 1..=max_block {
        let txs: Vec<capture::TxObs> = obs.iter().filter(|o| o.block == b).cloned().collect();
        if txs.is_empty() {
            continue;
        }
        let g = grade_block(&stats, &txs, exclude, GRADE_CAP);
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
        sum_gas += g.gas;
        sum_cp_pred += g.predicted_cp_gas;
        sum_cp_orac += g.oracle_cp_gas;
        sum_miss += g.missed_pairs;
        sum_over += g.false_pairs;
        sum_edges += g.predicted_edges;
        sum_hit += g.cells_hit;
        sum_actual += g.cells_actual;
        sum_cold += g.cold_txs;
        sum_txs += g.txs;
        for o in &txs {
            stats.learn_obs(o);
        }
    }
    println!(
        "SHADOW AGG: txs={} cold={} hit_rate={:.4} cp_pred={:.2}x cp_oracle={:.2}x \
         false_independent={} ({:.4}/tx) over_merge={} ({:.2}% of {} predicted edges)",
        sum_txs,
        sum_cold,
        sum_hit as f64 / sum_actual.max(1) as f64,
        sum_gas as f64 / sum_cp_pred.max(1) as f64,
        sum_gas as f64 / sum_cp_orac.max(1) as f64,
        sum_miss,
        sum_miss as f64 / sum_txs.max(1) as f64,
        sum_over,
        sum_over as f64 / sum_edges.max(1) as f64 * 100.0,
        sum_edges,
    );
}

fn main() -> anyhow::Result<()> {
    let a = Args::parse();
    let signers = mnemonic::derive_signers(ANVIL_MNEMONIC, (a.senders + 1) as u32)?;

    let mut b = MockStateDatabase::builder();
    for s in &signers {
        b = b.account(
            s.signer.address(),
            U256::from(10u128.pow(21)),
            0,
            alloy_primitives::KECCAK256_EMPTY,
        );
    }
    let snap = b.build();

    let (setup_blocks, flow_blocks): (Vec<Vec<TxEnvelope>>, Vec<Vec<TxEnvelope>>) =
        match a.scenario.as_str() {
            "uniswap" => {
                let w = uniswap::generate(
                    &a.repo_root,
                    &signers,
                    a.chain_id,
                    a.pairs,
                    a.blocks,
                    a.block_size,
                    a.swap_share,
                    a.cross,
                )?;
                (w.setup_blocks, w.flow_blocks)
            }
            "defi" => {
                // BenchDefi single-instance: the max-contention scenario.
                let (deploys, contracts) =
                    defi::deployment_txs(&signers, a.chain_id, 0, 1_000_000_000)?;
                let per_sender = (a.blocks * a.block_size) / a.senders + 2;
                let queues = defi::pregenerate_defi(
                    &signers,
                    a.chain_id,
                    &contracts,
                    per_sender,
                    0,
                    1_000_000_000,
                )?;
                let to_env = |t: &kardamom_bench::load::plan::PlannedTx,
                              sender: alloy_primitives::Address| {
                    TxEnvelope {
                        correlation_id: 0,
                        raw_tx: t.raw.clone().into(),
                        sender,
                        tx_hash: t.hash,
                    }
                };
                let setup: Vec<TxEnvelope> = deploys
                    .iter()
                    .map(|d| to_env(d, signers[0].signer.address()))
                    .collect();
                // Round-robin interleave into blocks.
                let mut cursors = vec![0usize; queues.len()];
                let mut flows = Vec::with_capacity(a.blocks);
                for _ in 0..a.blocks {
                    let mut blk = Vec::with_capacity(a.block_size);
                    let mut si = 0usize;
                    while blk.len() < a.block_size {
                        let q = &queues[si % queues.len()];
                        let c = &mut cursors[si % queues.len()];
                        if *c < q.len() {
                            blk.push(to_env(&q[*c], signers[si % queues.len()].signer.address()));
                            *c += 1;
                        }
                        si += 1;
                        if si > a.block_size * queues.len() * 2 {
                            break;
                        }
                    }
                    flows.push(blk);
                }
                (vec![setup], flows)
            }
            "transfers" => {
                use alloy_consensus::{SignableTransaction, TxLegacy};
                use alloy_eips::eip2718::Encodable2718;
                use alloy_network::TxSignerSync;
                use alloy_primitives::{TxKind, keccak256};
                let mut nonces = vec![0u64; signers.len()];
                let mut flows = Vec::with_capacity(a.blocks);
                for bidx in 0..a.blocks {
                    let mut blk = Vec::with_capacity(a.block_size);
                    for i in 0..a.block_size {
                        let si = (bidx * 7 + i) % signers.len();
                        let to = signers[(si + 1 + i % (signers.len() - 1)) % signers.len()]
                            .signer
                            .address();
                        let mut tx = TxLegacy {
                            chain_id: Some(a.chain_id),
                            nonce: nonces[si],
                            gas_price: 1_000_000_000,
                            gas_limit: 21_000,
                            to: TxKind::Call(to),
                            value: U256::from(1000u64),
                            input: Default::default(),
                        };
                        nonces[si] += 1;
                        let sig = signers[si].signer.sign_transaction_sync(&mut tx).unwrap();
                        let env: alloy_consensus::TxEnvelope = tx.into_signed(sig).into();
                        let raw = env.encoded_2718();
                        blk.push(TxEnvelope {
                            correlation_id: 0,
                            raw_tx: raw.clone().into(),
                            sender: signers[si].signer.address(),
                            tx_hash: keccak256(&raw),
                        });
                    }
                    flows.push(blk);
                }
                (Vec::new(), flows)
            }
            other => anyhow::bail!("unknown scenario {other}"),
        };

    let n_setup = setup_blocks.len();
    let mut all_blocks = setup_blocks;
    all_blocks.extend(flow_blocks);
    eprintln!(
        "==> executing {} blocks ({} setup) through the engine...",
        all_blocks.len(),
        n_setup
    );
    let t0 = std::time::Instant::now();
    let obs_all = capture::run_capture(&snap, &all_blocks, a.chain_id);
    let exec_s = t0.elapsed().as_secs_f64();
    // Flow-only observations, re-based so block numbers start at 1.
    let obs: Vec<capture::TxObs> = obs_all
        .into_iter()
        .filter(|o| o.block > n_setup as u64)
        .map(|mut o| {
            o.block -= n_setup as u64;
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

    // Accumulator treatment: exclude universal write cells if asked.
    let exclude: HashSet<Cell> = if a.accumulator {
        oracle::universal_writes(&obs, 0.95)
            .into_iter()
            .map(|(c, _)| c)
            .collect()
    } else {
        HashSet::new()
    };

    let t1 = std::time::Instant::now();
    let report = oracle::analyze_with(&obs, a.train_frac, &exclude, !a.no_derived);
    eprintln!("==> analysis {:.1}s", t1.elapsed().as_secs_f64());

    if a.shadow {
        shadow_replay(&obs, &exclude);
    }

    println!(
        "\n===== STM-P0 [{}] pairs={} senders={} blocks={}x{} accumulator={} =====",
        a.scenario, a.pairs, a.senders, a.blocks, a.block_size, a.accumulator
    );
    print!("{}", report.summary());

    // Classifier class shares (learned over ALL flow obs, reporting only).
    let stats = classifier::Stats::learn(&obs);
    let (solved, fixed, total) = stats.class_shares();
    println!(
        "CLASSIFIER: selectors={} slot-obs={} derived={:.1}% fixed={:.1}% unpredictable={:.1}%",
        stats.by_selector.len(),
        total,
        solved as f64 / total.max(1) as f64 * 100.0,
        fixed as f64 / total.max(1) as f64 * 100.0,
        (total - solved - fixed) as f64 / total.max(1) as f64 * 100.0,
    );

    if let Some(path) = &a.json {
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
    }
    Ok(())
}
