//! `kardamom-bench` — open-loop RPC load generator.
//!
//! Run against a kardamom node and report client-side percentile latencies
//! per method, so Prometheus dashboards have something to plot.

use std::path::PathBuf;
use std::time::Duration;

use clap::Parser;
use jsonrpsee::http_client::HttpClientBuilder;
use tracing_subscriber::EnvFilter;

use kardamom_bench::config::{self, CallsCfg, FileConfig, MixCfg, Workload};
use kardamom_bench::{generator, report};

/// Open-loop RPC load generator for kardamom.
#[derive(Parser, Debug)]
#[command(name = "kardamom-bench", about = "Open-loop RPC load generator.")]
struct Args {
    /// Target RPC URL. Required (unless set in `--config`).
    #[arg(long)]
    rpc: Option<String>,

    /// Workload kind.
    #[arg(long, value_enum)]
    workload: Option<Workload>,

    /// Requests per second (target).
    #[arg(long)]
    rate: Option<u32>,

    /// Test duration, e.g. `30s`, `2m`.
    #[arg(long, value_parser = humantime::parse_duration)]
    duration: Option<Duration>,

    /// Number of derived signers (also caps in-flight in-flight requests at
    /// `concurrency * 4`).
    #[arg(long)]
    concurrency: Option<u32>,

    /// Transfer/call ratio for `mixed`, e.g. `1:4`.
    #[arg(long = "mix-ratio")]
    mix_ratio: Option<String>,

    /// Warm-up window — bench runs but the recorded histograms are empty.
    #[arg(long, value_parser = humantime::parse_duration)]
    warmup: Option<Duration>,

    /// Write the report as JSON to this path in addition to printing it.
    #[arg(long)]
    output: Option<String>,

    /// Path to a TOML config file. CLI flags override individual fields.
    #[arg(long, value_name = "PATH")]
    config: Option<PathBuf>,

    /// `calls` workload target contract address.
    #[arg(long = "calls-contract")]
    calls_contract: Option<alloy_primitives::Address>,
}

fn parse_mix(s: &str) -> anyhow::Result<MixCfg> {
    let (t, c) = s
        .split_once(':')
        .ok_or_else(|| anyhow::anyhow!("mix-ratio must be `<transfers>:<calls>`, got `{s}`"))?;
    let transfers: u32 = t
        .parse()
        .map_err(|e| anyhow::anyhow!("bad transfers: {e}"))?;
    let calls: u32 = c.parse().map_err(|e| anyhow::anyhow!("bad calls: {e}"))?;
    Ok(MixCfg { transfers, calls })
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();

    let file_config = if let Some(path) = &args.config {
        Some(FileConfig::load(path)?)
    } else {
        None
    };

    let cli_overrides = FileConfig {
        rpc: args.rpc.clone(),
        workload: args.workload,
        rate: args.rate,
        duration: args.duration,
        concurrency: args.concurrency,
        warmup: args.warmup,
        output: args.output.clone(),
        mix: args.mix_ratio.as_deref().map(parse_mix).transpose()?,
        calls: args.calls_contract.map(|contract| CallsCfg { contract }),
        mnemonic: None,
        contracts: vec![],
    };

    let cfg = config::resolve(file_config, cli_overrides)?;

    tracing::info!(
        rpc = %cfg.rpc,
        workload = ?cfg.workload,
        rate = cfg.rate,
        duration = ?cfg.duration,
        concurrency = cfg.concurrency,
        "starting bench"
    );

    let client = HttpClientBuilder::default()
        .request_timeout(Duration::from_secs(30))
        .max_concurrent_requests((cfg.concurrency as usize) * 4 + 16)
        .build(&cfg.rpc)?;

    let outputs = generator::run(client, cfg.clone()).await?;
    let bench_report = report::build_report(&cfg, &outputs.counters, outputs.histograms);

    report::print_terminal(&bench_report);
    if let Some(path) = &cfg.output {
        report::write_json(std::path::Path::new(path), &bench_report)?;
        eprintln!("wrote report to {path}");
    }
    Ok(())
}
