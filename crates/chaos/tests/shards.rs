//! The chaos shards of the container cluster e2e. Each test brings its
//! own cluster up through `OpenTofu` and Ansible, runs the smoke gate,
//! then its shard's cases, and tears the cluster down on success. On
//! a failure the cluster stays up for diagnostics.
//!
//! Gated on the `cluster-e2e` feature and `#[ignore]`: run one shard
//! with
//!
//! ```text
//! cargo test -p kardamom-chaos --release --features cluster-e2e \
//!     --test shards -- --ignored chaos_executor
//! ```
//!
//! `KARDAMOM_CHAOS_REUSE=1` skips the bring-up and the teardown and runs
//! against the cluster that is up, for a local iteration loop.
//! `KARDAMOM_CHAOS_CASES` (space-separated) overrides the shard's case
//! list, `KARDAMOM_CHAOS_GATE_ACCOUNT` the smoke gate's funded account,
//! and `CHAOS_ACCT_BASE` the first case account, so a reuse run on a used
//! chain takes unused accounts. `KARDAMOM_CHAOS_CLUSTER_VARS` passes extra Ansible variables to
//! the convergence playbook as one JSON object. `KARDAMOM_CHAOS_CONTROLLER`
//! names a running `kardamom-orchestrator` container (the checkout at
//! `/work`) to run tofu and Ansible in, where the host preparation has
//! root. On a host without passwordless sudo and without the
//! controller, set the host sysctls and the bridge's multicast snooping
//! once by hand and pass `{"ansible_become": false}` in
//! `KARDAMOM_CHAOS_CLUSTER_VARS`: the host preparation then reads the
//! settings it would have written.

#![cfg(feature = "cluster-e2e")]

use std::time::Duration;

use kardamom_chaos::rpc::Rpc;
use kardamom_chaos::{Harness, Knobs, Lifecycle, Shard};

/// The funded account of the smoke gate. A reuse run on a used chain
/// passes another, unused, account through `KARDAMOM_CHAOS_GATE_ACCOUNT`.
fn gate_account() -> anyhow::Result<u32> {
    std::env::var("KARDAMOM_CHAOS_GATE_ACCOUNT").map_or(Ok(0), |v| {
        v.parse()
            .map_err(|e| anyhow::anyhow!("KARDAMOM_CHAOS_GATE_ACCOUNT: {e}"))
    })
}

fn reuse() -> bool {
    std::env::var("KARDAMOM_CHAOS_REUSE").is_ok_and(|v| v == "1")
}

fn case_list(shard: Shard) -> Vec<String> {
    std::env::var("KARDAMOM_CHAOS_CASES").map_or_else(
        |_| shard.cases().iter().map(|c| (*c).to_string()).collect(),
        |v| v.split_whitespace().map(str::to_string).collect(),
    )
}

async fn run_shard(shard: Shard) -> anyhow::Result<()> {
    let knobs = Knobs::read(shard.env())?;
    let lifecycle = Lifecycle::in_workspace();
    let contract = if reuse() {
        lifecycle.contract()?
    } else {
        lifecycle.up(&shard.deploy_vars()).await?
    };
    let mut harness = Harness::new(contract, knobs, lifecycle.clone())?;
    kardamom_chaos::log(format!("smoke gate against {}", harness.rpc_url));
    Rpc::new(&harness.rpc_url, harness.knobs.chain_id)?
        .transfer_smoke(gate_account()?, Duration::from_secs(60))
        .await?;
    let cases = case_list(shard);
    kardamom_chaos::log(format!(
        "chaos suite: shard={} cases=[{}] tps={} case_s={}",
        shard.name(),
        cases.join(" "),
        harness.knobs.tps,
        harness.knobs.case_window.as_secs()
    ));
    for case in &cases {
        harness.run_case(case).await?;
    }
    kardamom_chaos::log(format!("chaos suite PASSED ({})", cases.join(" ")));
    if !reuse() {
        lifecycle.down().await?;
    }
    Ok(())
}

async fn shard_test(shard: Shard) {
    if let Err(e) = run_shard(shard).await {
        eprintln!("{e:#}");
        panic!(
            "shard {} failed; the cluster stays up for diagnostics",
            shard.name()
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "brings a container cluster up; needs Docker, OpenTofu, Ansible, and the prebuilt artifacts"]
async fn chaos_executor() {
    shard_test(Shard::Executor).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "brings a container cluster up; needs Docker, OpenTofu, Ansible, and the prebuilt artifacts"]
async fn chaos_ingress() {
    shard_test(Shard::Ingress).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "brings a container cluster up; needs Docker, OpenTofu, Ansible, and the prebuilt artifacts"]
async fn chaos_sequencer() {
    shard_test(Shard::Sequencer).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "brings a container cluster up; needs Docker, OpenTofu, Ansible, and the prebuilt artifacts"]
async fn chaos_cluster() {
    shard_test(Shard::Cluster).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "brings a container cluster up; needs Docker, OpenTofu, Ansible, and the prebuilt artifacts"]
async fn chaos_retention() {
    shard_test(Shard::Retention).await;
}
