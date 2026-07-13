//! `kardamom-load` — open-loop sustained-load + chaos verification harness.
//!
//! Drives a paced, pre-generated tx stream through ingress, tracks every tx to
//! a receipt (must-deliver), scrapes cluster metrics for drop/liveness
//! signals, and exits non-zero on a failing verdict. See
//! `kardamom_bench::load` for the algorithm.

use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

use alloy_primitives::{Address, U256};
use clap::Parser;
use tracing_subscriber::EnvFilter;

use kardamom_bench::load::{self, ANVIL_MNEMONIC, Completeness, LoadConfig};

#[derive(Parser, Debug)]
#[command(
    name = "kardamom-load",
    about = "Sustained-load + chaos verification harness."
)]
struct Args {
    /// Ingress JSON-RPC URL.
    #[arg(long)]
    rpc: String,

    /// L2 chain id (probed via eth_chainId when omitted).
    #[arg(long)]
    chain_id: Option<u64>,

    /// Soak duration.
    #[arg(long, value_parser = humantime::parse_duration, default_value = "300s")]
    duration: Duration,

    /// Ramp ceiling (soak mode) / fixed rate (chaos mode), tx/s.
    #[arg(long, default_value_t = 200)]
    target_tps: u32,

    /// Number of sender accounts.
    #[arg(long, default_value_t = 16)]
    senders: u32,

    /// First account index in the mnemonic table (reserve low accounts).
    #[arg(long = "sender-offset", default_value_t = 0)]
    sender_offset: u32,

    /// Per-sender starting nonce.
    #[arg(long = "nonce-start", default_value_t = 0)]
    nonce_start: u64,

    /// BIP-39 mnemonic the senders derive from.
    #[arg(long, default_value = ANVIL_MNEMONIC)]
    mnemonic: String,

    /// Transfer sink address.
    #[arg(long, default_value = "0x000000000000000000000000000000000000dEaD")]
    to: String,

    /// Wei per transfer.
    #[arg(long, default_value_t = 1)]
    value: u64,

    /// Legacy gas price (wei).
    #[arg(long = "gas-price", default_value_t = 1_000_000_000)]
    gas_price: u128,

    /// Max outstanding submits (open-loop back-pressure bound).
    #[arg(long = "max-in-flight", default_value_t = 256)]
    max_in_flight: u32,

    /// Max allowed sealer-minus-executor block gap.
    #[arg(long = "max-gap", default_value_t = 5)]
    max_gap: u64,

    /// How long to keep draining receipts after the send window.
    #[arg(long = "drain-timeout", value_parser = humantime::parse_duration, default_value = "90s")]
    drain_timeout: Duration,

    /// Per-submit retry attempts on transient failure.
    #[arg(long = "retry-submit", default_value_t = 2)]
    retry_submit: u32,

    /// Ramp increment per step (tx/s); 0 ⇒ target_tps/8.
    #[arg(long = "ramp-step-tps", default_value_t = 0)]
    ramp_step_tps: u32,

    /// Seconds held per ramp step.
    #[arg(long = "ramp-step-secs", default_value_t = 15)]
    ramp_step_secs: u64,

    /// Fraction of the discovered max to soak at.
    #[arg(long = "soak-fraction", default_value_t = 0.8)]
    soak_fraction: f64,

    /// Completeness criterion: accepted | offered.
    #[arg(long, default_value = "accepted")]
    completeness: String,

    /// Fail unless completeness is met.
    #[arg(long = "assert-all-delivered", default_value_t = false)]
    assert_all_delivered: bool,

    /// Chaos framing: skip ramp; tolerate transient gaps/outages.
    #[arg(long = "chaos-mode", default_value_t = false)]
    chaos_mode: bool,

    /// Comma-separated services to scrape. (No `sealer`: cluster-only — ordering
    /// is the Aeron Cluster (Raft), which has no `kardamom_service_up` endpoint.)
    #[arg(long, default_value = "executor,ingress")]
    scrape: String,

    /// docker-exec scrape (true) vs direct curl (false).
    #[arg(long = "metrics-via-docker", default_value_t = true, action = clap::ArgAction::Set)]
    metrics_via_docker: bool,

    /// Executor node-container names (comma-separated).
    #[arg(
        long = "executor-nodes",
        default_value = "kardamom-executor-0,kardamom-executor-1,kardamom-executor-2"
    )]
    executor_nodes: String,

    /// Sealer node-container name.
    #[arg(long = "sealer-node", default_value = "kardamom-sealer-0")]
    sealer_node: String,

    /// Ingress node-container name.
    #[arg(long = "ingress-node", default_value = "kardamom-ingress-0")]
    ingress_node: String,

    /// Sequencer node-container names (comma-separated).
    #[arg(
        long = "sequencer-nodes",
        default_value = "kardamom-sequencer-0,kardamom-sequencer-1"
    )]
    sequencer_nodes: String,

    /// Write the JSON report to this path.
    #[arg(long)]
    output: Option<PathBuf>,
}

fn csv(s: &str) -> Vec<String> {
    s.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(ToString::to_string)
        .collect()
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();

    let completeness = match args.completeness.to_lowercase().as_str() {
        "accepted" => Completeness::Accepted,
        "offered" => Completeness::Offered,
        other => anyhow::bail!("--completeness must be 'accepted' or 'offered', got '{other}'"),
    };
    let to = Address::from_str(args.to.trim())
        .map_err(|e| anyhow::anyhow!("--to is not a valid address: {e}"))?;
    let ramp_step_tps = if args.ramp_step_tps == 0 {
        (args.target_tps / 8).max(1)
    } else {
        args.ramp_step_tps
    };

    let cfg = LoadConfig {
        rpc: args.rpc,
        chain_id: args.chain_id,
        duration: args.duration,
        target_tps: args.target_tps,
        senders: args.senders,
        sender_offset: args.sender_offset,
        nonce_start: args.nonce_start,
        mnemonic: args.mnemonic,
        to,
        value: U256::from(args.value),
        gas_price: args.gas_price,
        max_in_flight: args.max_in_flight,
        max_gap: args.max_gap,
        drain_timeout: args.drain_timeout,
        retry_submit: args.retry_submit,
        ramp_step_tps,
        ramp_step_secs: args.ramp_step_secs,
        soak_fraction: args.soak_fraction,
        completeness,
        assert_all_delivered: args.assert_all_delivered,
        chaos_mode: args.chaos_mode,
        scrape: csv(&args.scrape),
        metrics_via_docker: args.metrics_via_docker,
        executor_nodes: csv(&args.executor_nodes),
        sealer_node: args.sealer_node,
        ingress_node: args.ingress_node,
        sequencer_nodes: csv(&args.sequencer_nodes),
        output: args.output,
    };

    let pass = load::run(cfg).await?;
    if !pass {
        std::process::exit(1);
    }
    Ok(())
}
