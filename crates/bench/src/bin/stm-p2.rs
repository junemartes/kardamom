//! `kardamom-stm-p2` — Block-STM P2 offline A/B: the same generated blocks
//! through the sequential engine (`ExecScope`) and the STM engine
//! (`kardamom-stm`), asserting BYTE-IDENTICAL receipts + delta on every
//! block, and reporting wall-clock per worker count.
//! (spec: docs/agents/block-stm-executor-spec.md §P2 / "Measurement plan")
//!
//! Protocol per block, mirroring production learning dynamics:
//! 1. timed sequential run (the baseline and the canonical outputs),
//! 2. timed STM run per worker count, each byte-compared against (1),
//! 3. untimed capture pass training the footprint stats (prior-blocks-only
//!    stats feed each block's schedule — cold start, stream order).
//!
//! Wall-clock numbers are indicative (shared dev host); the assertion is
//! the point — the speedup column is the shape, not a benchmark citation.

use std::time::Instant;

use alloy_primitives::U256;
use clap::Parser;
use kardamom_bench::load::defi;
use kardamom_bench::mnemonic;
use kardamom_bench::stm::uniswap;
use kardamom_engine::block_env::ExecEnv;
use kardamom_engine::delta::PendingDelta;
use kardamom_engine::exec_types::TxIndex;
use kardamom_engine::executor::{ExecScope, TouchSet};
use kardamom_engine::state::MockStateDatabase;
use kardamom_footprint::classifier::Stats;
use kardamom_footprint::{Cell, TxObs, envelope_view};
use kardamom_stm::execute::execute_block_sequential;
use kardamom_types::{BPosition, Receipt, TxEnvelope};

const ANVIL_MNEMONIC: &str = "test test test test test test test test test test test junk";

#[derive(Parser, Debug)]
#[command(name = "kardamom-stm-p2")]
struct Args {
    /// uniswap | defi | transfers
    #[arg(long, default_value = "uniswap")]
    scenario: String,
    #[arg(long, default_value_t = 8)]
    pairs: usize,
    #[arg(long, default_value_t = 96)]
    senders: usize,
    #[arg(long, default_value_t = 16)]
    blocks: usize,
    #[arg(long, default_value_t = 500)]
    block_size: usize,
    #[arg(long, default_value_t = 70)]
    swap_share: u64,
    #[arg(long, default_value_t = 10)]
    cross: u64,
    /// Worker counts to sweep, comma-separated.
    #[arg(long, default_value = "1,2,4,8,12")]
    workers: String,
    #[arg(long, default_value = ".")]
    repo_root: String,
    #[arg(long, default_value_t = 412346)]
    chain_id: u64,
    /// Print per-block lines.
    #[arg(long, default_value_t = false)]
    per_block: bool,
    /// DAG prune batch sizes to sweep (completions applied per graph-lock
    /// acquisition; 1 = update on every completion).
    #[arg(long, default_value = "1")]
    prune_batch: String,
}

fn records(base_idx: u64, envs: &[TxEnvelope]) -> Vec<(TxIndex, BPosition, TxEnvelope)> {
    envs.iter()
        .enumerate()
        .map(|(i, e)| {
            let g = base_idx + i as u64;
            (TxIndex(g), BPosition::from_index(g), e.clone())
        })
        .collect()
}

fn assert_identical(
    seq: &[Receipt],
    seq_delta: &PendingDelta,
    stm: &[Receipt],
    stm_delta: &PendingDelta,
    block: u64,
    w: usize,
) {
    if seq != stm {
        let i = seq
            .iter()
            .zip(stm.iter())
            .position(|(a, b)| a != b)
            .unwrap_or(seq.len().min(stm.len()));
        panic!("block {block} workers {w}: receipt divergence at tx {i}");
    }
    assert!(
        seq_delta.accounts == stm_delta.accounts
            && seq_delta.storage == stm_delta.storage
            && seq_delta.code == stm_delta.code,
        "block {block} workers {w}: delta divergence"
    );
}

fn main() -> anyhow::Result<()> {
    let a = Args::parse();
    let worker_counts: Vec<usize> = a
        .workers
        .split(',')
        .map(|s| s.trim().parse().expect("workers csv"))
        .collect();
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
                // BenchDefi single-instance — mirrors stm-p0's construction.
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
        "==> A/B over {} blocks ({} setup) workers={:?}",
        all_blocks.len(),
        n_setup,
        worker_counts
    );

    // ---- Pass 0: timed sequential baseline + caches -------------------
    // Per block: the pre-block delta (base), the canonical outputs, and a
    // SNAPSHOT of the stats as they stood before the block (so every STM
    // sweep sees the exact inputs the streaming executor would).
    struct BlockCase {
        env: ExecEnv,
        recs: Vec<(TxIndex, BPosition, TxEnvelope)>,
        base: PendingDelta,
        stats: Stats,
        seq_receipts: Vec<Receipt>,
        seq_delta: PendingDelta,
        is_flow: bool,
        /// Upstream-prepared decode+prediction — what P3's tx_data readers
        /// will hand the executor. Timed separately: it is NOT on the
        /// feed's serial path.
        prep_us: u64,
    }
    let mut cases: Vec<BlockCase> = Vec::with_capacity(all_blocks.len());
    let mut delta = PendingDelta::new();
    let mut stats = Stats::default();
    let mut global_idx = 0u64;
    let mut wall_seq = 0f64;
    let mut flow_gas = 0u64;
    let mut flow_txs = 0usize;

    for (bi, blk) in all_blocks.iter().enumerate() {
        let is_flow = bi >= n_setup;
        let env = ExecEnv {
            chain_id: a.chain_id,
            block_number: bi as u64 + 1,
            l2_timestamp: 1_700_000_000 + bi as u64 * 2,
        };
        let recs = records(global_idx, blk);
        global_idx += blk.len() as u64;
        let base = delta.clone();
        let stats_before = stats.clone();

        let t0 = Instant::now();
        let (seq_receipts, seq_delta) = execute_block_sequential(&snap, Some(&delta), env, &recs)?;
        let seq_ms = t0.elapsed().as_secs_f64() * 1e3;
        if is_flow {
            wall_seq += seq_ms;
            flow_gas += seq_receipts
                .last()
                .map(|r| r.cumulative_gas_used)
                .unwrap_or(0);
            flow_txs += recs.len();
        }

        // Untimed capture pass: train the stats for the NEXT block
        // (prior-blocks-only, like the live shadow).
        {
            let mut scope = ExecScope::new(&snap, Some(&delta), env)?;
            let mut cumulative = 0u64;
            for (i, (tx_idx, position, envelope)) in recs.iter().enumerate() {
                let mut touches = TouchSet::default();
                let (receipt, ws) = scope.execute_tx(
                    *tx_idx,
                    *position,
                    envelope,
                    i as u64,
                    cumulative,
                    None,
                    Some(&mut touches),
                )?;
                cumulative = receipt.cumulative_gas_used;
                let (to, selector, args, has_value) = envelope_view(&envelope.raw_tx);
                let mut reads: Vec<Cell> = touches
                    .slot_reads
                    .iter()
                    .map(|(ad, k)| Cell::Slot(*ad, *k))
                    .collect();
                reads.sort_unstable();
                reads.dedup();
                let mut writes: Vec<Cell> = ws
                    .accounts
                    .iter()
                    .map(|(ad, _)| Cell::Account(*ad))
                    .chain(ws.storage.iter().map(|((ad, k), _)| Cell::Slot(*ad, *k)))
                    .collect();
                writes.sort_unstable();
                writes.dedup();
                stats.learn_obs(&TxObs {
                    index: i as u64,
                    block: env.block_number,
                    sender: envelope.sender,
                    to,
                    selector,
                    args,
                    gas: receipt.gas_used,
                    has_value,
                    reads,
                    writes,
                });
            }
        }

        delta.merge_from(&seq_delta);
        // Cost of preparing this block upstream (measured once; the sweep
        // re-prepares per run so each timed pass starts from raw inputs).
        let t_prep = Instant::now();
        for (t, _, e) in &recs {
            let _ = kardamom_stm::execute::prepare(e, *t, &stats_before);
        }
        let prep_us = t_prep.elapsed().as_micros() as u64;
        cases.push(BlockCase {
            env,
            recs,
            base,
            stats: stats_before,
            seq_receipts,
            seq_delta,
            is_flow,
            prep_us,
        });
    }

    // ---- Sweep: ONE persistent pool per worker count, blocks streamed
    // through it — no per-block thread cost, matching the executor
    // pipeline shape.
    let batches: Vec<usize> = a
        .prune_batch
        .split(',')
        .map(|s| s.trim().parse().expect("prune-batch csv"))
        .collect();
    struct Row {
        workers: usize,
        batch: usize,
        wall: f64,
        fallbacks: usize,
        feed_us: u64,
        prep_us: u64,
        redundant: u64,
        /// Dispatch imbalance: busiest thread's share vs an even split.
        /// Domain-affinity assignment collides when domains ~ workers.
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
    let mut rows: Vec<Row> = Vec::new();
    let (mut agg_edges, mut agg_cold) = (0usize, 0usize);
    for &w in &worker_counts {
        for &batch in &batches {
            let cfg = kardamom_stm::execute::PoolConfig {
                workers: w,
                prune_batch: batch,
            };
            let mut row = Row {
                workers: w,
                batch,
                wall: 0.0,
                fallbacks: 0,
                feed_us: 0,
                prep_us: 0,
                redundant: 0,
                imbalance: 0.0,
                imb_n: 0,
                idle_threads: 0,
                busy_us: 0,
                span_us: 0,
                ramp_us: 0,
                commit_us: 0,
                decode_us: 0,
                predict_us: 0,
                admit_us: 0,
                prune_us: 0,
                prune_calls: 0,
                prune_forced: 0,
                avg_batch: 0.0,
                idle_us: 0,
            };
            let mut batch_weight = 0f64;
            kardamom_stm::execute::with_pool(&snap, cfg, |pool| -> anyhow::Result<()> {
                for case in &cases {
                    let base = case.base.clone();
                    // Upstream stage — in production the tx_data readers,
                    // here just ahead of the timer: the point is that it
                    // is NOT on the executor's serial feed.
                    let prepared: Vec<_> = case
                        .recs
                        .iter()
                        .map(|(t, _, e)| kardamom_stm::execute::prepare(e, *t, &case.stats))
                        .collect();
                    let t = Instant::now();
                    let out =
                        pool.run_block_prepared(base, case.env, &case.recs, prepared, &case.stats)?;
                    let ms = t.elapsed().as_secs_f64() * 1e3;
                    assert_identical(
                        &case.seq_receipts,
                        &case.seq_delta,
                        &out.receipts,
                        &out.delta,
                        case.env.block_number,
                        w,
                    );
                    if case.is_flow {
                        row.wall += ms;
                        row.fallbacks += out.fallback as usize;
                        row.feed_us += out.feed_us;
                        row.prep_us += case.prep_us;
                        row.busy_us += out.busy_us;
                        row.span_us += out.parallel_span_us;
                        row.ramp_us += out.ramp_us;
                        row.commit_us += out.commit_us;
                        let total: u32 = out.dispatch.iter().sum();
                        let maxd = *out.dispatch.iter().max().unwrap_or(&0);
                        let used = out.dispatch.iter().filter(|c| **c > 0).count();
                        if total > 0 {
                            row.imbalance += maxd as f64 * out.dispatch.len() as f64 / total as f64;
                            row.idle_threads += out.dispatch.len() - used;
                            row.imb_n += 1;
                        }
                        row.redundant += out.redundant_edges;
                        row.decode_us += out.decode_us;
                        row.predict_us += out.predict_us;
                        row.admit_us += out.admit_us;
                        row.prune_us += out.prune_us;
                        row.prune_calls += out.prune_calls;
                        row.prune_forced += out.prune_forced;
                        row.idle_us += out.idle_us;
                        row.avg_batch += out.avg_batch * out.prune_calls as f64;
                        batch_weight += out.prune_calls as f64;
                        if rows.is_empty() {
                            agg_edges += out.edges;
                            agg_cold += out.cold;
                        }
                    }
                }
                Ok(())
            })?;
            if batch_weight > 0.0 {
                row.avg_batch /= batch_weight;
            }
            rows.push(row);
        }
    }

    println!(
        "\n===== STM-P2 A/B [{}] pairs={} senders={} blocks={}x{} =====",
        a.scenario, a.pairs, a.senders, a.blocks, a.block_size
    );
    println!(
        "flow: txs={} gas={:.3}Ggas seq_wall={:.0}ms ({:.0} Mgas/s) sched: edges={} cold={}",
        flow_txs,
        flow_gas as f64 / 1e9,
        wall_seq,
        flow_gas as f64 / 1e6 / (wall_seq / 1e3),
        agg_edges,
        agg_cold,
    );
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
    for r in &rows {
        println!(
            "{:>3} {:>6} {:>9.0} {:>7.2}x {:>9.0} {:>10} {:>10} {:>7} {:>9} {:>9} {:>6.2}",
            r.workers,
            r.batch,
            r.wall,
            wall_seq / r.wall,
            flow_gas as f64 / 1e6 / (r.wall / 1e3),
            r.feed_us,
            r.prep_us,
            r.predict_us,
            r.prune_us,
            r.idle_us,
            if r.imb_n > 0 {
                r.imbalance / r.imb_n as f64
            } else {
                0.0
            },
        );
    }
    let _ = |r: &Row| (r.avg_batch, r.redundant, r.idle_threads);
    let tot_idle_threads: usize = rows.iter().map(|r| r.idle_threads).sum();
    let tot_blocks: u64 = rows.iter().map(|r| r.imb_n).sum();
    println!(
        "DISPATCH: empty-thread-slots {} over {} block-runs (domain-affinity collisions)",
        tot_idle_threads, tot_blocks
    );
    println!(
        "\n----- WHERE THE WALL TIME GOES (per worker count) -----\n{:>3} {:>10} {:>10} {:>10} {:>10} {:>12}",
        "w", "ramp_ms", "span_ms", "commit_ms", "busy_ms", "utilization"
    );
    for r in &rows {
        let util = if r.span_us > 0 {
            r.busy_us as f64 / (r.workers as f64 * r.span_us as f64) * 100.0
        } else {
            0.0
        };
        println!(
            "{:>3} {:>10.1} {:>10.1} {:>10.1} {:>10.1} {:>11.1}%",
            r.workers,
            r.ramp_us as f64 / 1000.0,
            r.span_us as f64 / 1000.0,
            r.commit_us as f64 / 1000.0,
            r.busy_us as f64 / 1000.0,
            util
        );
    }
    println!("BYTE-IDENTICAL: every block, every worker count — verified");
    Ok(())
}
