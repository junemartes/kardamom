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
//! the convergence playbook as one JSON object. On a host without
//! passwordless sudo, set the host sysctls and the bridge's multicast
//! snooping once by hand and pass `{"ansible_become": false}` in
//! `KARDAMOM_CHAOS_CLUSTER_VARS`: the host preparation then reads the
//! settings it would have written.

#![cfg(feature = "cluster-e2e")]

use kardamom_chaos::lifecycle::DeployVars;
use kardamom_chaos::stages::VerdictMode;
use kardamom_chaos::{Harness, Knobs, Lifecycle, Shard};

/// The funded account of the smoke gate. A reuse run on a used chain
/// passes another, unused, account through `KARDAMOM_CHAOS_GATE_ACCOUNT`.
fn gate_account() -> anyhow::Result<u32> {
    std::env::var("KARDAMOM_CHAOS_GATE_ACCOUNT").map_or(Ok(0), |v| {
        v.parse()
            .map_err(|e| anyhow::anyhow!("KARDAMOM_CHAOS_GATE_ACCOUNT: {e}"))
    })
}

/// The two shards without chaos: the sustained load, and the
/// chain-semantics suite.
#[derive(Debug, Clone, Copy)]
enum Stage {
    Load,
    Semantics,
}

impl Stage {
    fn name(self) -> &'static str {
        match self {
            Self::Load => "load",
            Self::Semantics => "semantics",
        }
    }

    /// The shard's values below the environment: the load shard soaks
    /// five minutes at 300 tps, the invariant-gate rate every 4-core
    /// runner sustains; the semantics shard runs no load.
    fn env(self) -> &'static [(&'static str, &'static str)] {
        match self {
            Self::Load => &[
                ("LOAD_DURATION_S", "300"),
                ("LOAD_TARGET_TPS", "300"),
                ("LOAD_SENDERS", "6"),
            ],
            Self::Semantics => &[("RUN_LOAD", "0")],
        }
    }

    async fn run(self, harness: &Harness) -> anyhow::Result<()> {
        match self {
            Self::Load => harness.soak().await,
            Self::Semantics => harness.semantics().await,
        }
    }
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
    harness.smoke_gate(gate_account()?).await?;
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
    harness.ingress_churn().await?;
    harness.validator_verdict(VerdictMode::Progress).await?;
    kardamom_chaos::log("cluster-e2e PASSED");
    if !reuse() {
        lifecycle.down().await?;
    }
    Ok(())
}

async fn run_stage(stage: Stage) -> anyhow::Result<()> {
    let knobs = Knobs::read(stage.env())?;
    let lifecycle = Lifecycle::in_workspace();
    let contract = if reuse() {
        lifecycle.contract()?
    } else {
        lifecycle.up(&DeployVars::default()).await?
    };
    let harness = Harness::new(contract, knobs, lifecycle.clone())?;
    harness.smoke_gate(gate_account()?).await?;
    stage.run(&harness).await?;
    harness.ingress_churn().await?;
    harness.validator_verdict(VerdictMode::Sync).await?;
    kardamom_chaos::log("cluster-e2e PASSED");
    if !reuse() {
        lifecycle.down().await?;
    }
    Ok(())
}

async fn stage_test(stage: Stage) {
    if let Err(e) = run_stage(stage).await {
        eprintln!("{e:#}");
        panic!(
            "shard {} failed; the cluster stays up for diagnostics",
            stage.name()
        );
    }
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
async fn load() {
    stage_test(Stage::Load).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "brings a container cluster up; needs Docker, OpenTofu, Ansible, and the prebuilt artifacts"]
async fn semantics() {
    stage_test(Stage::Semantics).await;
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
