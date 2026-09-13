//! `kardamom-semantics` — the Target-C runner for the chain-semantics suite.
//!
//! The scenario drivers in `e2e::scenarios` talk only to external seams: the
//! ingress JSON-RPC and the per-service `/metrics`. So the same code that
//! runs against the single-host Target-L stack in
//! `tests/chain_semantics/main.rs` also runs against the real 12-node
//! `DinD` cluster. This binary points the drivers at cluster addresses
//! instead of per-test temp ports. A difference between the two targets
//! is itself a signal.
//!
//! `deploy/cluster/scripts/ci-cluster.sh` starts this binary (the
//! `semantics` shard of `cluster-e2e.yml`) from the runner. The runner can
//! reach the cluster bridge directly: the executor, validator, and
//! sequencer metrics all bind `0.0.0.0` in their Nomad jobs, and `chaos.sh`
//! already scrapes them over plain HTTP.
//!
//! Exits with a non-zero code on the first failing case, like every other
//! CI gate here.
//!
//! ```text
//! kardamom-semantics \
//!   --rpc http://<ingress-0>:8545 --chain-id 412346 \
//!   --executor-metrics <executor-0>:9004,<executor-1>:9004,<executor-2>:9004 \
//!   --sequencer-metrics <sequencer-0>:9001,<sequencer-1>:9001 \
//!   --validator-metrics <aux-0>:9006 \
//!   --cases nonce-unordered,nonce-gap,rpc-liveness,consistency
//! ```

use std::net::SocketAddr;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use e2e::harness::l2::L2Client;
use e2e::scenarios::{
    Target, consistency, l1_batch, nonce_gap, nonce_unordered, rpc_liveness, rpc_vectors,
};

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
    /// Executor `/metrics` addresses. Any replica can answer. This binary
    /// uses the first one that answers.
    #[arg(long, value_delimiter = ',')]
    executor_metrics: Vec<SocketAddr>,
    /// Sequencer `/metrics` addresses (all replicas). The binary sums the
    /// counters.
    #[arg(long, value_delimiter = ',')]
    sequencer_metrics: Vec<SocketAddr>,
    /// Validator `/metrics` address. Required for the `consistency` case.
    #[arg(long)]
    validator_metrics: Option<SocketAddr>,
    /// Ingress `/metrics` address. The deployed ingress binds to loopback
    /// only. So this is usually absent, and the cases that need it are
    /// skipped.
    #[arg(long)]
    ingress_metrics: Option<SocketAddr>,
    /// The ingress's `--pending-receipt-timeout-ms` value. The nonce-gap
    /// case derives its latency limits from this value. It must match the
    /// deployment (30 s unless the job sets another value).
    #[arg(long, default_value_t = 30_000)]
    pending_receipt_timeout_ms: u64,
    /// Comma-separated case list.
    #[arg(
        long,
        value_delimiter = ',',
        default_value = "nonce-unordered,nonce-gap,rpc-liveness,rpc-vectors,consistency"
    )]
    cases: Vec<String>,
    /// First dev-mnemonic account index this run may use. The cluster's
    /// funded-account ledger (see ci-cluster.sh) gives each check its own
    /// range of accounts. The semantics shard owns its own block of
    /// accounts.
    #[arg(long, default_value_t = 1)]
    account_base: usize,

    /// In-cluster anvil L1 JSON-RPC endpoint. Required for the `l1-batch`
    /// case: the live batcher's L2 to L1 round trip.
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
    // Set the client timeout above the server park time. This way, a gap
    // case sees the ingress's own -32000 error, not a client abort.
    // `--pending-receipt-timeout-ms` is operator-supplied, so multiplying
    // and adding it must not panic on an extreme value; `Duration` has no
    // real "wrong" saturated result here, since a saturated timeout is
    // just a very patient client.
    let rpc = L2Client::new(
        &args.rpc,
        park.saturating_mul(3)
            .saturating_add(Duration::from_secs(5)),
    )
    .context("build ingress JSON-RPC client")?;

    // Pick one executor replica that answers. A cluster always has three
    // replicas, and each one serves the same counters.
    let executor = pick_live(&args.executor_metrics)
        .await
        .context("no executor /metrics endpoint answered")?;

    let target = Target {
        rpc,
        chain_id: args.chain_id,
        pending_receipt_timeout: park,
        // The deployed ingress binds metrics on loopback. When it is not
        // reachable, this field points at the executor instead, so the
        // struct stays valid. The cases that read ingress metrics are
        // skipped below.
        ingress_metrics: args.ingress_metrics.unwrap_or(executor),
        executor_metrics: executor,
        sequencer_metrics: args.sequencer_metrics.clone(),
        validator_metrics: args.validator_metrics,
    };

    let base = args.account_base;
    let l1 = match (&args.l1_rpc, args.settlement) {
        (Some(r), Some(s)) => Some((r.as_str(), s)),
        _ => None,
    };
    for case in &args.cases {
        run_and_report_case(case, &target, base, args.ingress_metrics.is_some(), l1).await;
    }
    println!("==> semantics verdict PASS ({} cases)", args.cases.len());
    Ok(())
}

/// Run one semantics case, print PASS/FAIL, and exit the process on the
/// first failure — a case suite stops at the first violation.
async fn run_and_report_case(
    case: &str,
    target: &Target,
    base: usize,
    have_ingress_metrics: bool,
    l1: Option<(&str, alloy_primitives::Address)>,
) {
    println!("==> ===== SEMANTICS CASE: {case} =====");
    let started = std::time::Instant::now();
    match run_case(case, target, base, have_ingress_metrics, l1).await {
        Ok(()) => println!("==> SEMANTICS CASE {case}: PASS ({:?})", started.elapsed()),
        Err(e) => {
            println!("SEMANTICS FAIL: case {case}: {e:#}");
            std::process::exit(1);
        }
    }
}

/// This shard's account range, anchored at the operator-supplied
/// `--account-base`. Every case derives its own accounts from this as
/// `base + offset`, checked: the sum must not silently wrap into another
/// case's range.
struct Accounts {
    base: usize,
}

/// `nonce_params`'s sender count.
const NONCE_SENDERS: std::num::NonZeroUsize = std::num::NonZeroUsize::new(4).unwrap();
/// `nonce_params`'s txs-per-sender count.
const NONCE_TXS_PER_SENDER: std::num::NonZeroUsize = std::num::NonZeroUsize::new(16).unwrap();
/// `consistency_params`'s sender count.
const CONSISTENCY_SENDERS: std::num::NonZeroUsize = std::num::NonZeroUsize::new(3).unwrap();
/// `consistency_params`'s transfers-per-sender count.
const CONSISTENCY_TRANSFERS_PER_SENDER: std::num::NonZeroUsize =
    std::num::NonZeroUsize::new(12).unwrap();

impl Accounts {
    /// `self.base + offset`, checked.
    ///
    /// # Errors
    /// Returns an error when `self.base + offset` overflows.
    fn at(&self, offset: usize) -> Result<usize> {
        self.base
            .checked_add(offset)
            .with_context(|| format!("--account-base {} + {offset} overflows", self.base))
    }

    /// `nonce_unordered::Params` for this shard. `txs_per_sender` is
    /// smaller than Target-L's: the cluster's boundary tick is 2s (250ms
    /// locally), so a larger run would spend the whole case waiting for
    /// blocks, without proving more.
    fn nonce_params(&self) -> nonce_unordered::Params {
        nonce_unordered::Params {
            senders: NONCE_SENDERS,
            txs_per_sender: NONCE_TXS_PER_SENDER,
            sender_base: self.base,
            ..nonce_unordered::Params::default()
        }
    }

    /// `consistency::Params` for this shard.
    ///
    /// # Errors
    /// Returns an error when `self.base + 10` overflows.
    fn consistency_params(&self) -> Result<consistency::Params> {
        Ok(consistency::Params {
            senders: CONSISTENCY_SENDERS,
            transfers_per_sender: CONSISTENCY_TRANSFERS_PER_SENDER,
            sender_base: self.at(10)?,
            // Here tx_bal uses UDP multicast, not IPC. A dropped BAL leaves a
            // block unverified, and the design allows this (it is never a
            // divergence). This budget covers a few drops from the
            // workload's own blocks. A validator that receives no BALs still
            // goes over this budget.
            max_bal_missing: 3.0,
        })
    }
}

/// Run the `l1-batch` case, or fail with a clear message when `--l1-rpc`
/// and `--settlement` were not given.
///
/// # Errors
/// Returns an error when `l1` is `None`, or under the same conditions as
/// [`l1_batch::l1_batch`].
async fn l1_batch_case(t: &Target, l1: Option<(&str, alloy_primitives::Address)>) -> Result<()> {
    let Some((rpc, settlement)) = l1 else {
        anyhow::bail!("the l1-batch case needs --l1-rpc and --settlement");
    };
    l1_batch::l1_batch(t, rpc, settlement).await
}

async fn run_case(
    case: &str,
    t: &Target,
    base: usize,
    have_ingress_metrics: bool,
    l1: Option<(&str, alloy_primitives::Address)>,
) -> Result<()> {
    let accounts = Accounts { base };
    match case {
        "nonce-unordered" => nonce_unordered::run(t, accounts.nonce_params()).await,
        "nonce-gap" => {
            nonce_gap::run(
                t,
                nonce_gap::Params {
                    gapped: accounts.at(5)?,
                    bystander: accounts.at(6)?,
                    disorder: accounts.at(7)?,
                },
            )
            .await
        }
        "rpc-liveness" => {
            if !have_ingress_metrics {
                // Only the queue-depth probe needs ingress metrics, and
                // Target L already covers it.
                println!("    (ingress /metrics not reachable — queue-depth probe skipped)");
            }
            rpc_liveness::run(
                t,
                rpc_liveness::Params {
                    sender: accounts.at(8)?,
                    parked_sender: accounts.at(9)?,
                },
            )
            .await
        }
        "consistency" => {
            anyhow::ensure!(
                t.validator_metrics.is_some(),
                "the consistency case needs --validator-metrics"
            );
            consistency::run(t, accounts.consistency_params()?).await
        }
        "l1-batch" => l1_batch_case(t, l1).await,
        "rpc-vectors" => {
            rpc_vectors::run(
                t,
                rpc_vectors::Params {
                    sender: accounts.at(13)?,
                    recipient: accounts.at(14)?,
                },
            )
            .await
        }
        other => anyhow::bail!("unknown case {other}"),
    }
}

/// First address whose `/metrics` answers.
async fn pick_live(addrs: &[SocketAddr]) -> Option<SocketAddr> {
    use futures::StreamExt;

    let stream = futures::stream::iter(addrs)
        .filter_map(|a| async move { e2e::harness::metrics::scrape(*a).await.ok().map(|_| *a) });
    std::pin::pin!(stream).next().await
}
