//! `kardamom-semantics` — the Target-C runner for the chain-semantics suite.
//!
//! The scenario drivers in `e2e::scenarios` talk only to externally
//! observable seams (ingress JSON-RPC + per-service `/metrics`), so the same
//! code that runs against the single-host Target-L stack in
//! `tests/chain_semantics.rs` runs unchanged against the real 12-node DinD
//! cluster — this binary just points them at cluster addresses instead of
//! per-test temp ports. Divergence between the two targets is itself signal.
//!
//! Invoked by `deploy/cluster/scripts/ci-cluster.sh` (the `semantics` shard of
//! `cluster-e2e.yml`) from the runner, which can reach the cluster bridge
//! directly: executor/validator/sequencer metrics all bind `0.0.0.0` in their
//! Nomad jobs and `chaos.sh` already scrapes them over plain HTTP.
//!
//! Exits non-zero on the first failing case, like every other CI gate here.
//!
//! ```text
//! kardamom-semantics \
//!   --rpc http://192.168.56.31:8545 --chain-id 412346 \
//!   --executor-metrics 192.168.56.41:9004,192.168.56.42:9004,192.168.56.43:9004 \
//!   --sequencer-metrics 192.168.56.21:9001,192.168.56.22:9001 \
//!   --validator-metrics 192.168.56.61:9006 \
//!   --cases nonce-unordered,nonce-gap,rpc-liveness,consistency
//! ```

use std::net::SocketAddr;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use e2e::harness::l2::L2Client;
use e2e::scenarios::{Target, consistency, l1_batch, nonce_gap, nonce_unordered, rpc_liveness};

#[derive(Debug, Parser)]
#[command(
    name = "kardamom-semantics",
    version,
    about = "chain-semantics scenarios against a live kardamom cluster"
)]
struct Args {
    /// Ingress JSON-RPC endpoint.
    #[arg(long)]
    rpc: String,
    /// L2 chain id (must match the cluster's genesis).
    #[arg(long, default_value_t = 412_346)]
    chain_id: u64,
    /// Executor `/metrics` addresses. Any replica answers; the first that
    /// responds is used.
    #[arg(long, value_delimiter = ',')]
    executor_metrics: Vec<SocketAddr>,
    /// Sequencer `/metrics` addresses (all replicas; counters are summed).
    #[arg(long, value_delimiter = ',')]
    sequencer_metrics: Vec<SocketAddr>,
    /// Validator `/metrics` address. Required for the `consistency` case.
    #[arg(long)]
    validator_metrics: Option<SocketAddr>,
    /// Ingress `/metrics` address. The deployed ingress binds loopback-only,
    /// so this is usually absent and the cases that need it are skipped.
    #[arg(long)]
    ingress_metrics: Option<SocketAddr>,
    /// The ingress's `--pending-receipt-timeout-ms`. The nonce-gap case
    /// derives its latency bounds from this, so it must match the deployment
    /// (30 s unless the job overrides it).
    #[arg(long, default_value_t = 30_000)]
    pending_receipt_timeout_ms: u64,
    /// Comma-separated case list.
    #[arg(
        long,
        value_delimiter = ',',
        default_value = "nonce-unordered,nonce-gap,rpc-liveness,consistency"
    )]
    cases: Vec<String>,
    /// First dev-mnemonic account index this run may use. The cluster's
    /// funded-account ledger (see ci-cluster.sh) assigns disjoint ranges to
    /// each check; the semantics shard owns its own block.
    #[arg(long, default_value_t = 1)]
    account_base: usize,

    /// In-cluster anvil L1 JSON-RPC endpoint. Required for the `l1-batch`
    /// case (the live batcher's L2 → L1 round trip, #39).
    #[arg(long)]
    l1_rpc: Option<String>,

    /// `KardamomL2Settlement` proxy address on that L1. Required for
    /// `l1-batch`.
    #[arg(long)]
    settlement: Option<alloy_primitives::Address>,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<()> {
    let args = Args::parse();
    anyhow::ensure!(
        !args.executor_metrics.is_empty(),
        "--executor-metrics is required (at least one replica)"
    );

    let park = Duration::from_millis(args.pending_receipt_timeout_ms);
    // Client bound above the server-side park so a gap case observes the
    // ingress's own -32000 rather than a client abort.
    let rpc = L2Client::new(&args.rpc, park * 3 + Duration::from_secs(5))
        .context("build ingress JSON-RPC client")?;

    // Executor replicas: pick one that answers. A cluster always has three;
    // any of them serves the same counters.
    let executor = pick_live(&args.executor_metrics)
        .await
        .context("no executor /metrics endpoint answered")?;

    let target = Target {
        rpc,
        chain_id: args.chain_id,
        pending_receipt_timeout: park,
        // The deployed ingress binds metrics on loopback; when it is not
        // reachable this points at the executor so the struct stays valid,
        // and the cases that actually read ingress metrics are skipped below.
        ingress_metrics: args.ingress_metrics.unwrap_or(executor),
        executor_metrics: executor,
        sequencer_metrics: args.sequencer_metrics.clone(),
        validator_metrics: args.validator_metrics,
    };

    let base = args.account_base;
    for case in &args.cases {
        println!("==> ===== SEMANTICS CASE: {case} =====");
        let started = std::time::Instant::now();
        let l1 = match (&args.l1_rpc, args.settlement) {
            (Some(r), Some(s)) => Some((r.as_str(), s)),
            _ => None,
        };
        let result = run_case(case, &target, base, args.ingress_metrics.is_some(), l1).await;
        match result {
            Ok(()) => println!("==> SEMANTICS CASE {case}: PASS ({:?})", started.elapsed()),
            Err(e) => {
                println!("SEMANTICS FAIL: case {case}: {e:#}");
                std::process::exit(1);
            }
        }
    }
    println!("==> semantics verdict PASS ({} cases)", args.cases.len());
    Ok(())
}

async fn run_case(
    case: &str,
    t: &Target,
    base: usize,
    have_ingress_metrics: bool,
    l1: Option<(&str, alloy_primitives::Address)>,
) -> Result<()> {
    match case {
        "nonce-unordered" => {
            nonce_unordered::run(
                t,
                nonce_unordered::Params {
                    senders: 4,
                    // Smaller than Target L: the cluster's boundary tick is
                    // 2 s (vs 250 ms locally), so a wide run would spend the
                    // whole case waiting for blocks rather than proving
                    // anything extra.
                    txs_per_sender: 16,
                    sender_base: base,
                    ..nonce_unordered::Params::default()
                },
            )
            .await
        }
        "nonce-gap" => {
            nonce_gap::run(
                t,
                nonce_gap::Params {
                    gapped: base + 5,
                    bystander: base + 6,
                    disorder: base + 7,
                },
            )
            .await
        }
        "rpc-liveness" => {
            if !have_ingress_metrics {
                // The queue-depth probe is the only part needing ingress
                // metrics, and it is covered at Target L.
                println!("    (ingress /metrics not reachable — queue-depth probe skipped)");
            }
            rpc_liveness::run(
                t,
                rpc_liveness::Params {
                    sender: base + 8,
                    parked_sender: base + 9,
                },
            )
            .await
        }
        "consistency" => {
            anyhow::ensure!(
                t.validator_metrics.is_some(),
                "the consistency case needs --validator-metrics"
            );
            consistency::run(
                t,
                consistency::Params {
                    senders: 3,
                    transfers_per_sender: 12,
                    sender_base: base + 10,
                    // tx_bal is UDP multicast here, not IPC: a dropped BAL
                    // leaves a block unverified, which the design explicitly
                    // tolerates (it is never a divergence). Budget a few for
                    // the workload's own blocks; a validator receiving NO
                    // BALs still blows through this.
                    max_bal_missing: 3.0,
                },
            )
            .await
        }
        "l1-batch" => {
            let (rpc, settlement) = match l1 {
                Some(pair) => pair,
                None => anyhow::bail!("the l1-batch case needs --l1-rpc and --settlement"),
            };
            l1_batch::l1_batch(t, rpc, settlement).await
        }
        other => anyhow::bail!("unknown case {other}"),
    }
}

/// First address whose `/metrics` answers.
async fn pick_live(addrs: &[SocketAddr]) -> Option<SocketAddr> {
    for a in addrs {
        if e2e::harness::metrics::scrape(*a).await.is_ok() {
            return Some(*a);
        }
    }
    None
}
