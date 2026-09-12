//! `kardamom-perf` is the cluster performance pipeline CLI.
//!
//! It automates the saturation campaign: bring the deploy and cluster
//! `DinD` stack up fresh, ramp offered load to the sustainable edge,
//! hold a steady soak while async-profiler samples the sealer Raft
//! leader, and fold everything into a report directory
//! (`load-report.json`, `flame.html`, `flame.svg`, `stacks.collapsed`,
//! `summary.md`).
//!
//!   kardamom-perf up                # Bring up a fresh stack: build, purge, deploy.
//!   kardamom-perf run --fresh       # up, then ramp, profile, and report.
//!   kardamom-perf run               # Reuse the running stack. This uses a fresh chain.
//!   kardamom-perf report --dir OUT  # Re-render summary.md from artifacts.
//!
//! Account budget: the pipeline assumes the fresh chain `up` deploys.
//! Genesis accounts 1 through 6 drive the discovery ramp, and 7
//! through 15 drive the profiled soak. Accounts 0 and 16 belong to the
//! deploy's smoke gates. Rerunning against a used chain needs a fresh
//! `up` first, since nonces are managed locally and ingress has no
//! `eth_getTransactionCount`.

use std::num::{NonZeroU32, NonZeroU64};
use std::path::PathBuf;
use std::time::Duration;

use anyhow::Context;
use clap::{Parser, Subcommand};

use kardamom_bench::load::{self, LoadConfig, SenderRange};
use kardamom_bench::perf::{OutDir, cluster, profile, report};

/// Default `--chain-id`.
const DEFAULT_CHAIN_ID: NonZeroU64 = NonZeroU64::new(412_346).unwrap();
/// Default `--ceiling`.
const DEFAULT_CEILING: NonZeroU32 = NonZeroU32::new(4000).unwrap();
/// Default `--ramp-step`.
const DEFAULT_RAMP_STEP: NonZeroU32 = NonZeroU32::new(250).unwrap();
/// Default `--ramp-step-secs`.
const DEFAULT_RAMP_STEP_SECS: NonZeroU64 = NonZeroU64::new(12).unwrap();
/// `load_cfg`'s fixed `max_in_flight`: the perf suite runs on
/// dedicated hardware, well above `kardamom-load`'s default.
const PERF_MAX_IN_FLIGHT: NonZeroU32 = NonZeroU32::new(1024).unwrap();
/// The ramp phase's sender-range width (accounts 1 through 15;
/// accounts 16 and 17 are the deploy gates' re-smoke accounts).
const RAMP_SENDER_RANGE_WIDTH: NonZeroU32 = NonZeroU32::new(15).unwrap();
/// The soak phase's sender-range width (accounts 18 through 33).
const SOAK_SENDER_RANGE_WIDTH: NonZeroU32 = NonZeroU32::new(16).unwrap();

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
    /// Bring the cluster up fresh: build images and binaries, purge the
    /// previous deployment, wipe state, and run ci-cluster.sh with
    /// KEEP=1 and no load or chaos stages.
    Up {
        /// Skip the builder-image and binary build. Reuse the staged
        /// target/release.
        #[arg(long)]
        skip_build: bool,
        /// The repo root. Defaults to the current directory.
        #[arg(long, default_value = ".")]
        repo_root: PathBuf,
    },
    /// Ramp to the sustainable edge, then soak while profiling the
    /// sealer leader. Writes the report directory.
    Run(RunArgs),
    /// Re-render summary.md from an existing run directory.
    Report {
        /// A directory `kardamom-perf run` produces.
        #[arg(long)]
        dir: PathBuf,
    },
}

#[derive(Parser, Debug)]
struct RunArgs {
    /// Run `up` first, for a fresh chain. This is required unless the
    /// stack was just deployed and its ramp and profile accounts are
    /// unused.
    #[arg(long)]
    fresh: bool,
    /// With --fresh, skip the build step.
    #[arg(long)]
    skip_build: bool,
    /// The repo root. Defaults to the current directory.
    #[arg(long, default_value = ".")]
    repo_root: PathBuf,
    /// The ingress JSON-RPC URL.
    #[arg(long, default_value = "http://192.168.56.31:8545")]
    rpc: String,
    /// The workload for the ramp and soak phases: transfers or defi.
    #[arg(long, default_value = "transfers", value_parser = clap::builder::ValueParser::new(|s: &str| s.parse::<kardamom_bench::load::Workload>().map_err(|e| e.to_string())))]
    workload: kardamom_bench::load::Workload,
    /// The L2 chain ID for the deploy and cluster genesis.
    #[arg(long, default_value_t = DEFAULT_CHAIN_ID)]
    chain_id: NonZeroU64,
    /// The ramp ceiling, in tx/s, for edge discovery. Non-zero: it is
    /// a `clamp` and a `while` ceiling in `load::ramp_to_max`.
    #[arg(long, default_value_t = DEFAULT_CEILING)]
    ceiling: NonZeroU32,
    /// The ramp increment for each step, in tx/s.
    #[arg(long, default_value_t = DEFAULT_RAMP_STEP)]
    ramp_step: NonZeroU32,
    /// The number of seconds held at each ramp step.
    #[arg(long, default_value_t = DEFAULT_RAMP_STEP_SECS)]
    ramp_step_secs: NonZeroU64,
    /// The profiled soak rate, as a fraction of the discovered maximum.
    #[arg(long, default_value_t = 0.8)]
    soak_fraction: f64,
    /// The profiled soak length. This must cover warmup and both
    /// profiler passes.
    #[arg(long, default_value_t = 240)]
    soak_secs: u64,
    /// The collapsed-capture length within the soak.
    #[arg(long, default_value_t = 60)]
    profile_secs: u64,
    /// Drive load through `kardamom_sendRawTransactionAsync` and a
    /// WebSocket receipt subscription, so an in-flight transaction
    /// holds no connection, instead of the parked `eth_sendRawTransaction`.
    #[arg(long, default_value_t = false)]
    subscribe: bool,
    /// The output base directory. The code creates a timestamped subdirectory.
    #[arg(long, default_value = "target/perf")]
    out: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    kardamom_obs::bin::init_tracing();
    match Args::parse().cmd {
        Cmd::Up {
            skip_build,
            repo_root,
        } => cluster::up(&repo_root, skip_build),
        Cmd::Run(a) => run(a).await,
        Cmd::Report { dir } => rerender(dir),
    }
}

/// The settings shared by both load phases. Senders, offset, rate, and
/// shape vary between phases.
///
/// Every field this run needs to differ from [`LoadConfig::default`]'s
/// sensible defaults is set explicitly below; `sender_range` is left at
/// its placeholder default, since both call sites overwrite it before
/// `load::run` ever reads it, and the node-topology, mnemonic, and
/// transfer-sink fields the perf suite shares with `kardamom-load`'s own
/// defaults come along for free.
fn load_cfg(a: &RunArgs, out: PathBuf) -> LoadConfig {
    LoadConfig {
        workload: a.workload,
        rpc: a.rpc.clone(),
        chain_id: Some(a.chain_id.get()),
        duration: Duration::from_secs(0),
        target_tps: NonZeroU32::MIN,
        max_in_flight: PERF_MAX_IN_FLIGHT,
        ramp_step_tps: a.ramp_step,
        ramp_step_secs: a.ramp_step_secs,
        soak_fraction: a.soak_fraction,
        assert_all_delivered: true,
        // The perf suite exists for edge discovery on dedicated hardware.
        // Always ramp, and scrape the sequencer too (unlike
        // `kardamom-load`'s default, which does not).
        scrape: vec!["executor".into(), "ingress".into(), "sequencer".into()],
        subscribe: a.subscribe,
        // Blocking runs confirm through the WebSocket feed: one HTTP call
        // per transaction instead of two. The per-transaction receipt
        // re-fetch alone would be another full-rate request stream
        // through the proxy and ingress.
        feed_confirm: true,
        output: Some(out),
        ..LoadConfig::default()
    }
}

#[allow(
    clippy::format_collect,
    reason = "one format! per container reads more directly here than a write! fold into a growing String"
)]
async fn run(a: RunArgs) -> anyhow::Result<()> {
    if a.fresh {
        cluster::up(&a.repo_root, a.skip_build)?;
    }
    let out = OutDir::create(&a.out)?;
    println!("==> output dir: {}", out.0.display());

    // Phase 1 is edge discovery: ramp with a token soak. The profiled soak
    // is phase 2, on its own accounts. This phase uses accounts 1 through
    // 15. The per-sender in-flight depth, approximately
    // (rate / senders) * latency, must stay inside the sequencer's
    // max_pending_per_sender window of 16. Otherwise the harness's own
    // nonce reordering trips evictions that look like the pipeline's edge.
    // 15 senders keep a 10k tx/s ramp step at about 7 in flight per sender.
    // Accounts 16 and 17 are the deploy gates' re-smoke accounts; never
    // touch them here.
    println!("==> phase 1: ramp to the edge (ceiling {} tx/s)", a.ceiling);
    let mut cfg = load_cfg(&a, out.path("discovery-report.json"));
    cfg.target_tps = a.ceiling;
    cfg.sender_range = SenderRange::new(1, RAMP_SENDER_RANGE_WIDTH)?;
    cfg.duration = Duration::from_secs(1);
    // Discovery probes past the edge on purpose. A retried submit whose
    // first attempt landed shows up there as a past-nonce drop. Strict
    // delivery gating belongs to the profiled soak; here, only the
    // per-step sustainability signals matter.
    cfg.assert_all_delivered = false;
    load::run(cfg).await.context("discovery ramp")?;
    let discovery = report::read_load_report(&out.path("discovery-report.json"))?;
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "a discovered tx/s rate stays far under i64::MAX, rounded for display"
    )]
    let soak_rate =
        NonZeroU32::new((f64::from(discovery.discovered_max_tps) * a.soak_fraction).round() as u32)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "discovery found {} tx/s, which rounds to 0 at --soak-fraction {}; \
                     the profiled soak has nothing to run at",
                    discovery.discovered_max_tps,
                    a.soak_fraction
                )
            })?;
    println!(
        "==> discovered max {} tx/s → profiled soak at {} tx/s",
        discovery.discovered_max_tps, soak_rate
    );

    // Phase 2 is a steady soak: chaos mode means a fixed rate, with no
    // ramp, on the dedicated genesis accounts 18 through 33 (16 fresh
    // senders, with the same per-sender in-flight arithmetic as the
    // ramp, sized for an 8k tx/s soak). The profiler attaches once the
    // rate is established.
    let mut cfg = load_cfg(&a, out.path("load-report.json"));
    cfg.target_tps = soak_rate;
    cfg.sender_range = SenderRange::new(18, SOAK_SENDER_RANGE_WIDTH)?;
    cfg.chaos_mode = true;
    cfg.duration = Duration::from_secs(a.soak_secs);
    let load_task = tokio::spawn(load::run(cfg));

    // Warm up, so leader detection sees steady-state CPU, not ramp transients.
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
    // An older or partial run directory can lack the discovery report.
    // Fall back to the soak report, so the summary still renders, with
    // no ramp.
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
