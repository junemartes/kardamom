//! `kardamom-bench-harness` — single-process node + bench with a
//! `tracing-flame` recording scoped to the measurement window.
//!
//! See `kardamom_bench::harness` for the mechanism.

use std::path::PathBuf;
use std::time::Duration;

use alloy_primitives::Address;
use clap::Parser;

use kardamom_bench::config::{self, CallsCfg, FileConfig, MixCfg, Workload};
use kardamom_bench::harness::{HarnessArgs, run_harness};

/// Single-process bench harness with scoped flame recording.
#[derive(Parser, Debug)]
#[command(
    name = "kardamom-bench-harness",
    about = "Run the bench against an in-process kardamom node and write flame.folded for the measurement window only."
)]
struct Args {
    /// Path to write `flame.folded`. Render with `inferno-flamegraph`.
    #[arg(long, default_value = "flame.folded")]
    flame_out: PathBuf,

    /// Chain ID for the in-process node.
    #[arg(long, default_value_t = 412346)]
    chain_id: u64,

    /// Bench config TOML. Must contain a `[mnemonic]` section (and
    /// `[[contracts]]` if workload requires `eth_call`).
    #[arg(long, value_name = "PATH")]
    config: PathBuf,

    /// Workload kind (overrides the file).
    #[arg(long, value_enum)]
    workload: Option<Workload>,

    /// Requests per second (target).
    #[arg(long)]
    rate: Option<u32>,

    /// Test duration, e.g. `30s`, `2m`.
    #[arg(long, value_parser = humantime::parse_duration)]
    duration: Option<Duration>,

    /// Number of derived signers (also caps in-flight at `concurrency * 4`).
    #[arg(long)]
    concurrency: Option<u32>,

    /// Warm-up window (flame recording disabled during warmup).
    #[arg(long, value_parser = humantime::parse_duration)]
    warmup: Option<Duration>,

    /// Transfer/call ratio for `mixed`, e.g. `1:4`.
    #[arg(long = "mix-ratio")]
    mix_ratio: Option<String>,

    /// Address of contract to `eth_call` for the calls/mixed workloads.
    #[arg(long = "calls-contract")]
    calls_contract: Option<Address>,

    /// Write bench report JSON here in addition to stdout.
    #[arg(long)]
    report_json: Option<PathBuf>,
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
    let args = Args::parse();

    let file_config = FileConfig::load(&args.config)?;

    let cli_overrides = FileConfig {
        rpc: Some("in-process".to_string()),
        workload: args.workload,
        rate: args.rate,
        duration: args.duration,
        concurrency: args.concurrency,
        warmup: args.warmup,
        output: None,
        mix: args.mix_ratio.as_deref().map(parse_mix).transpose()?,
        calls: args.calls_contract.map(|contract| CallsCfg { contract }),
        mnemonic: None,
        contracts: vec![],
    };

    let config = config::resolve(Some(file_config.clone()), cli_overrides)?;

    run_harness(HarnessArgs {
        chain_id: args.chain_id,
        file_config,
        config,
        flame_out: args.flame_out,
        report_json: args.report_json,
    })
    .await
}
