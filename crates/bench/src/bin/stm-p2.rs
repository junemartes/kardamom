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

use std::collections::HashSet;
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
use kardamom_stm::execute::{execute_block_sequential, execute_block_stm};
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

    let mut delta = PendingDelta::new();
    let mut stats = Stats::default();
    let mut exclude = HashSet::new();
    exclude.insert(Cell::Account(kardamom_stm::FEE_SINK));
    let mut global_idx = 0u64;

    let mut wall_seq = 0f64;
    let mut wall_stm: Vec<f64> = vec![0.0; worker_counts.len()];
    let mut fallbacks: Vec<usize> = vec![0; worker_counts.len()];
    let mut flow_gas = 0u64;
    let (mut agg_edges, mut agg_cold, mut flow_txs) = (0usize, 0usize, 0usize);

    eprintln!(
        "==> A/B over {} blocks ({} setup) workers={:?}",
        all_blocks.len(),
        n_setup,
        worker_counts
    );
    if a.per_block {
        println!(
            "{:>5} {:>5} {:>5} {:>7} {:>9}  {}",
            "block",
            "txs",
            "cold",
            "edges",
            "seq_ms",
            worker_counts
                .iter()
                .map(|w| format!("{:>8}", format!("w{w}_ms")))
                .collect::<String>()
        );
    }

    for (bi, blk) in all_blocks.iter().enumerate() {
        let is_flow = bi >= n_setup;
        let env = ExecEnv {
            chain_id: a.chain_id,
            block_number: bi as u64 + 1,
            l2_timestamp: 1_700_000_000 + bi as u64 * 2,
        };
        let recs = records(global_idx, blk);
        global_idx += blk.len() as u64;

        // 1. Timed sequential baseline.
        let t0 = Instant::now();
        let (seq_receipts, seq_delta) = execute_block_sequential(&snap, Some(&delta), env, &recs)?;
        let seq_ms = t0.elapsed().as_secs_f64() * 1e3;

        // 2. Timed STM per worker count, byte-compared.
        let mut stm_ms = Vec::with_capacity(worker_counts.len());
        for (wi, &w) in worker_counts.iter().enumerate() {
            let t = Instant::now();
            let out = execute_block_stm(&snap, Some(&delta), env, &recs, &stats, w)?;
            let ms = t.elapsed().as_secs_f64() * 1e3;
            assert_identical(
                &seq_receipts,
                &seq_delta,
                &out.receipts,
                &out.delta,
                env.block_number,
                w,
            );
            if is_flow {
                wall_stm[wi] += ms;
                fallbacks[wi] += out.fallback as usize;
                if wi == 0 {
                    agg_edges += out.edges;
                    agg_cold += out.cold;
                }
            }
            stm_ms.push(ms);
        }

        // 3. Untimed capture pass: train the stats for the NEXT block
        //    (prior-blocks-only, like the live shadow).
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

        if is_flow {
            wall_seq += seq_ms;
            flow_gas += seq_receipts
                .last()
                .map(|r| r.cumulative_gas_used)
                .unwrap_or(0);
            flow_txs += recs.len();
        }
        if a.per_block {
            println!(
                "{:>5} {:>5} {:>5} {:>7} {:>9.1}  {}",
                env.block_number,
                recs.len(),
                if is_flow { "-" } else { "s" },
                "-",
                seq_ms,
                stm_ms
                    .iter()
                    .map(|m| format!("{m:>8.1}"))
                    .collect::<String>()
            );
        }

        delta.merge_from(&seq_delta);
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
    for (wi, &w) in worker_counts.iter().enumerate() {
        println!(
            "workers={:<2} wall={:>7.0}ms speedup={:>5.2}x mgas/s={:>6.0} fallbacks={}",
            w,
            wall_stm[wi],
            wall_seq / wall_stm[wi],
            flow_gas as f64 / 1e6 / (wall_stm[wi] / 1e3),
            fallbacks[wi],
        );
    }
    println!("BYTE-IDENTICAL: every block, every worker count — verified");
    Ok(())
}
