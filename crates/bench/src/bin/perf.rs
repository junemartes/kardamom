//! `kardamom-perf` — cluster performance pipeline CLI.
//!
//! Automates the saturation campaign: bring the deploy/cluster DinD stack up
//! fresh, ramp offered load to the sustainable edge, hold a steady soak while
//! async-profiler samples the sealer Raft leader, and fold everything into a
//! report directory (`load-report.json`, `flame.html`/`flame.svg`,
//! `stacks.collapsed`, `summary.md`).
//!
//!   kardamom-perf up                # fresh stack (build + purge + deploy)
//!   kardamom-perf run --fresh       # up, then ramp → profile → report
//!   kardamom-perf run               # reuse the running stack (fresh chain!)
//!   kardamom-perf report --dir OUT  # re-render summary.md from artifacts
//!
//! Account budget: the pipeline assumes the fresh chain `up` deploys —
//! genesis accounts #1..#6 drive the discovery ramp and #7..#15 the profiled
//! soak (#0/#16 belong to the deploy's smoke gates). Rerunning against a
//! used chain needs a fresh `up` first (nonces are managed locally; ingress
//! has no eth_getTransactionCount).

use std::path::PathBuf;
use std::time::Duration;

use alloy_primitives::{Address, U256};
use anyhow::Context;
use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;

use kardamom_bench::load::{self, ANVIL_MNEMONIC, Completeness, LoadConfig};
use kardamom_bench::perf::{OutDir, cluster, profile, report};

#[derive(Parser, Debug)]
#[command(
    name = "kardamom-perf",
    about = "Cluster perf pipeline: up → ramp to the edge → profile the sealer leader → report."
)]
struct Args {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// Bring the cluster up fresh: build images/binaries, purge the previous
    /// deployment, wipe state, run ci-cluster.sh (KEEP=1, no load/chaos).
    Up {
        /// Skip the builder-image/binary build (reuse staged target/release).
        #[arg(long)]
        skip_build: bool,
        /// Repo root (defaults to the current directory).
        #[arg(long, default_value = ".")]
        repo_root: PathBuf,
    },
    /// Ramp to the sustainable edge, then soak while profiling the sealer
    /// leader; write the report directory.
    Run(RunArgs),
    /// Re-render summary.md from an existing run directory.
    Report {
        /// A directory previously produced by `kardamom-perf run`.
        #[arg(long)]
        dir: PathBuf,
    },
}

#[derive(Parser, Debug)]
struct RunArgs {
    /// Run `up` first (fresh chain — required unless the stack was just
    /// deployed and its ramp/profile accounts are unused).
    #[arg(long)]
    fresh: bool,
    /// With --fresh: skip the build step.
    #[arg(long)]
    skip_build: bool,
    /// Repo root (defaults to the current directory).
    #[arg(long, default_value = ".")]
    repo_root: PathBuf,
    /// Ingress JSON-RPC URL.
    #[arg(long, default_value = "http://192.168.56.31:8545")]
    rpc: String,
    /// L2 chain id (deploy/cluster genesis).
    #[arg(long, default_value_t = 412346)]
    chain_id: u64,
    /// Ramp ceiling (tx/s) for edge discovery.
    #[arg(long, default_value_t = 4000)]
    ceiling: u32,
    /// Ramp increment per step (tx/s).
    #[arg(long, default_value_t = 250)]
    ramp_step: u32,
    /// Seconds held per ramp step.
    #[arg(long, default_value_t = 12)]
    ramp_step_secs: u64,
    /// Profiled soak rate as a fraction of the discovered max.
    #[arg(long, default_value_t = 0.8)]
    soak_fraction: f64,
    /// Profiled soak length (must cover warmup + both profiler passes).
    #[arg(long, default_value_t = 240)]
    soak_secs: u64,
    /// Collapsed-capture length within the soak.
    #[arg(long, default_value_t = 60)]
    profile_secs: u64,
    /// Drive load via kardamom_sendRawTransactionAsync + a WebSocket
    /// receipt subscription (in-flight txs hold no connections) instead of
    /// the parked eth_sendRawTransaction.
    #[arg(long, default_value_t = false)]
    subscribe: bool,
    /// Output base directory (a timestamped subdir is created).
    #[arg(long, default_value = "target/perf")]
    out: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
    match Args::parse().cmd {
        Cmd::Up {
            skip_build,
            repo_root,
        } => cluster::up(&repo_root, skip_build),
        Cmd::Run(a) => run(a).await,
        Cmd::Report { dir } => rerender(dir),
    }
}

/// Shared knobs for both load phases. Senders/offset/rate/shape vary.
#[allow(clippy::too_many_arguments)]
fn load_cfg(a: &RunArgs, out: PathBuf) -> LoadConfig {
    LoadConfig {
        rpc: a.rpc.clone(),
        chain_id: Some(a.chain_id),
        duration: Duration::from_secs(0),
        target_tps: 0,
        senders: 0,
        sender_offset: 0,
        nonce_start: 0,
        mnemonic: ANVIL_MNEMONIC.to_string(),
        to: "0x000000000000000000000000000000000000dEaD"
            .parse::<Address>()
            .unwrap(),
        value: U256::from(1),
        gas_price: 1_000_000_000,
        max_in_flight: 1024,
        max_gap: 5,
        drain_timeout: Duration::from_secs(90),
        retry_submit: 2,
        ramp_step_tps: a.ramp_step,
        ramp_step_secs: a.ramp_step_secs,
        soak_fraction: a.soak_fraction,
        completeness: Completeness::Accepted,
        assert_all_delivered: true,
        chaos_mode: false,
        scrape: vec!["executor".into(), "ingress".into(), "sequencer".into()],
        metrics_via_docker: true,
        subscribe: a.subscribe,
        executor_nodes: vec![
            "kardamom-executor-0".into(),
            "kardamom-executor-1".into(),
            "kardamom-executor-2".into(),
        ],
        ingress_node: "kardamom-ingress-0".into(),
        sequencer_nodes: vec!["kardamom-sequencer-0".into(), "kardamom-sequencer-1".into()],
        output: Some(out),
    }
}

async fn run(a: RunArgs) -> anyhow::Result<()> {
    if a.fresh {
        cluster::up(&a.repo_root, a.skip_build)?;
    }
    let out = OutDir::create(&a.out)?;
    println!("==> output dir: {}", out.0.display());

    // Phase 1 — edge discovery: ramp with a token soak (the profiled soak is
    // phase 2, on its own accounts). Accounts #1..#6.
    println!("==> phase 1: ramp to the edge (ceiling {} tx/s)", a.ceiling);
    let mut cfg = load_cfg(&a, out.path("discovery-report.json"));
    cfg.target_tps = a.ceiling;
    cfg.senders = 6;
    cfg.sender_offset = 1;
    cfg.duration = Duration::from_secs(1);
    // Discovery probes past the edge on purpose — retried submits whose first
    // attempt landed show up as past-nonce drops there. Strict delivery
    // gating belongs to the profiled soak; here only the per-step
    // sustainability signals matter.
    cfg.assert_all_delivered = false;
    load::run(cfg).await.context("discovery ramp")?;
    let discovery = report::read_load_report(&out.path("discovery-report.json"))?;
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let soak_rate =
        ((f64::from(discovery.discovered_max_tps) * a.soak_fraction).round() as u32).max(1);
    println!(
        "==> discovered max {} tx/s → profiled soak at {} tx/s",
        discovery.discovered_max_tps, soak_rate
    );

    // Phase 2 — steady soak (chaos-mode = fixed rate, no ramp) on accounts
    // #7..#15, with the profiler attached once the rate is established.
    let mut cfg = load_cfg(&a, out.path("load-report.json"));
    cfg.target_tps = soak_rate;
    cfg.senders = 9;
    cfg.sender_offset = 7;
    cfg.chaos_mode = true;
    cfg.duration = Duration::from_secs(a.soak_secs);
    let load_task = tokio::spawn(load::run(cfg));

    // Warmup so leader detection sees steady-state CPU, not ramp transients.
    tokio::time::sleep(Duration::from_secs(20)).await;
    let (leader, collapsed, cpu_snapshot) = {
        let profile_secs = a.profile_secs;
        let out_dir = out.0.clone();
        tokio::task::spawn_blocking(move || -> anyhow::Result<_> {
            let leader = cluster::detect_sealer_leader()?;
            println!("==> sealer leader (busiest under load): {leader}");
            profile::stage(&leader, &out_dir.join("cache"))?;
            let collapsed = profile::run(&leader, profile_secs, &out_dir)?;
            let cpu = cluster::cpu_sample(cluster::NODES)?;
            std::fs::write(
                out_dir.join("cpu-snapshot.txt"),
                cpu.iter()
                    .map(|(n, p)| format!("{n} {p:.1}%\n"))
                    .collect::<String>(),
            )?;
            Ok((leader, collapsed, cpu))
        })
        .await??
    };

    let load_pass = load_task.await?.context("profiled soak")?;
    let soak_report = report::read_load_report(&out.path("load-report.json"))?;

    let summary = report::analyze_collapsed(&collapsed);
    let svg = profile::render_svg(
        &out.0,
        &format!(
            "kardamom sealer leader @ {soak_rate} tx/s — itimer {}s",
            a.profile_secs
        ),
    )?;
    let md = report::write_summary(
        &out,
        &discovery,
        &soak_report,
        &leader,
        &cpu_snapshot,
        &summary,
    )?;

    println!("==> report: {}", md.display());
    if let Some(svg) = svg {
        println!("==> flamegraph: {}", svg.display());
    }
    println!("==> raw: {}", out.0.display());
    anyhow::ensure!(
        load_pass,
        "profiled soak verdict FAILED — see load-report.json"
    );
    Ok(())
}

fn rerender(dir: PathBuf) -> anyhow::Result<()> {
    let out = OutDir(dir);
    let soak_report = report::read_load_report(&out.path("load-report.json"))?;
    // An older or partial run directory may lack the discovery report; fall
    // back to the soak report so the summary still renders (with no ramp).
    let discovery = report::read_load_report(&out.path("discovery-report.json"))
        .unwrap_or_else(|_| soak_report.clone());
    let collapsed = std::fs::read_to_string(out.path("stacks.collapsed"))?;
    let cpu: Vec<(String, f64)> = std::fs::read_to_string(out.path("cpu-snapshot.txt"))
        .unwrap_or_default()
        .lines()
        .filter_map(|l| {
            let (n, p) = l.rsplit_once(' ')?;
            Some((n.to_string(), p.trim_end_matches('%').parse().ok()?))
        })
        .collect();
    let summary = report::analyze_collapsed(&collapsed);
    let md = report::write_summary(
        &out,
        &discovery,
        &soak_report,
        "(from artifacts)",
        &cpu,
        &summary,
    )?;
    println!("==> report: {}", md.display());
    Ok(())
}
