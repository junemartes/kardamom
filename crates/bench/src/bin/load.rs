//! `kardamom-load` is an open-loop sustained-load and chaos
//! verification harness.
//!
//! It drives a paced, pre-generated transaction stream through
//! ingress, tracks every transaction to a receipt (must-deliver),
//! reads cluster metrics for drop and liveness signals, and exits
//! with a non-zero code on a failing verdict. See
//! `kardamom_bench::load` for the algorithm.

use std::num::{NonZeroU32, NonZeroU64};
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

use alloy_primitives::{Address, U256};
use clap::Parser;

use kardamom_bench::load::{self, ANVIL_MNEMONIC, Completeness, LoadConfig, SenderRange};

/// Default `--target-tps`.
const DEFAULT_TARGET_TPS: NonZeroU32 = NonZeroU32::new(200).unwrap();
/// Default `--senders`.
const DEFAULT_SENDERS: NonZeroU32 = NonZeroU32::new(16).unwrap();
/// Default `--max-in-flight`.
const DEFAULT_MAX_IN_FLIGHT: NonZeroU32 = NonZeroU32::new(256).unwrap();
/// Default `--ramp-step-secs`.
const DEFAULT_RAMP_STEP_SECS: NonZeroU64 = NonZeroU64::new(15).unwrap();

#[derive(Parser, Debug)]
#[command(
    name = "kardamom-load",
    about = "Sustained-load + chaos verification harness."
)]
#[allow(
    clippy::struct_excessive_bools,
    reason = "each bool is an independent CLI flag; grouping them would rename flags"
)]
struct Args {
    /// The ingress JSON-RPC URL.
    #[arg(long)]
    rpc: String,

    /// The L2 chain ID. When omitted, the code probes it with `eth_chainId`.
    #[arg(long)]
    chain_id: Option<u64>,

    /// The soak duration.
    #[arg(long, value_parser = humantime::parse_duration, default_value = "300s")]
    duration: Duration,

    /// The ramp ceiling in soak mode, or the fixed rate in chaos mode,
    /// in tx/s. Non-zero: it is a `clamp` and a `while` ceiling below.
    #[arg(long, default_value_t = DEFAULT_TARGET_TPS)]
    target_tps: NonZeroU32,

    /// The number of sender accounts. Non-zero: `SignerSet::new`
    /// rejects an empty derived-signer set.
    #[arg(long, default_value_t = DEFAULT_SENDERS)]
    senders: NonZeroU32,

    /// The first account index in the mnemonic table. This reserves
    /// the low accounts.
    #[arg(long = "sender-offset", default_value_t = 0)]
    sender_offset: u32,

    /// The starting nonce for each sender.
    #[arg(long = "nonce-start", default_value_t = 0)]
    nonce_start: u64,

    /// The BIP-39 mnemonic the senders derive from.
    #[arg(long, default_value = ANVIL_MNEMONIC)]
    mnemonic: String,

    /// The transfer sink address.
    #[arg(long, default_value = "0x000000000000000000000000000000000000dEaD")]
    to: String,

    /// The wei value of each transfer.
    #[arg(long, default_value_t = 1)]
    value: u64,

    /// The legacy gas price, in wei.
    #[arg(long = "gas-price", default_value_t = 1_000_000_000)]
    gas_price: u128,

    /// The limit on outstanding submits. This bounds open-loop back pressure.
    #[arg(long = "max-in-flight", default_value_t = DEFAULT_MAX_IN_FLIGHT)]
    max_in_flight: NonZeroU32,

    /// The maximum allowed gap between the sealer and executor block.
    #[arg(long = "max-gap", default_value_t = 5)]
    max_gap: u64,

    /// How long to keep draining receipts after the send window ends.
    #[arg(long = "drain-timeout", value_parser = humantime::parse_duration, default_value = "90s")]
    drain_timeout: Duration,

    /// Submit through `kardamom_sendRawTransactionAsync` and a
    /// WebSocket receipt subscription, instead of the parked
    /// `eth_sendRawTransaction`.
    #[arg(long, default_value_t = false)]
    subscribe: bool,

    /// In blocking mode, confirm receipts through the WebSocket feed
    /// instead of a per-transaction `eth_getTransactionReceipt` re-fetch.
    /// This halves the HTTP request load.
    #[arg(long, default_value_t = false)]
    feed_confirm: bool,
    /// The workload family: plain transfers, or the `DeFi` mix of a
    /// CLOB, a swap pool, and a vault, with gas-centric reporting.
    #[arg(long, default_value = "transfers", value_parser = clap::builder::ValueParser::new(|s: &str| s.parse::<kardamom_bench::load::Workload>().map_err(|e| e.to_string())))]
    workload: kardamom_bench::load::Workload,

    /// The number of per-submit retry attempts on a transient failure.
    #[arg(long = "retry-submit", default_value_t = 2)]
    retry_submit: u32,

    /// The ramp increment for each step, in tx/s. Unset means auto:
    /// `target_tps` / 8.
    #[arg(long = "ramp-step-tps")]
    ramp_step_tps: Option<NonZeroU32>,

    /// The number of seconds held at each ramp step.
    #[arg(long = "ramp-step-secs", default_value_t = DEFAULT_RAMP_STEP_SECS)]
    ramp_step_secs: NonZeroU64,

    /// The fraction of the discovered maximum rate to soak at.
    #[arg(long = "soak-fraction", default_value_t = 0.8)]
    soak_fraction: f64,

    /// The completeness criterion: accepted or offered.
    #[arg(long, default_value = "accepted", value_parser = clap::builder::ValueParser::new(|s: &str| s.parse::<Completeness>().map_err(|e| e.to_string())))]
    completeness: Completeness,

    /// Fail the run unless the completeness criterion is met.
    #[arg(long = "assert-all-delivered", default_value_t = false)]
    assert_all_delivered: bool,

    /// The chaos framing: skip the ramp, and tolerate transient gaps
    /// or outages.
    #[arg(long = "chaos-mode", default_value_t = false)]
    chaos_mode: bool,

    /// The fixed-rate framing: skip the ramp, and soak at `target_tps`
    /// with the strict verdict. Use this for CI invariant gating on a
    /// shared or weak host, where edge discovery measures the
    /// hypervisor rather than the stack.
    #[arg(long = "fixed-rate", default_value_t = false)]
    fixed_rate: bool,

    /// The services to scrape, comma-separated: executor, ingress, or
    /// sequencer. The `kardamom_sealer_*` values ride the executor
    /// scrape, because the clustered sealer, an Aeron Cluster Raft
    /// group, has no endpoint of its own; the executors re-export its
    /// boundary stream from cluster egress.
    #[arg(long, default_value = "executor,ingress")]
    scrape: String,

    /// Scrape through docker exec when true, or a direct curl when false.
    #[arg(long = "metrics-via-docker", default_value_t = true, action = clap::ArgAction::Set)]
    metrics_via_docker: bool,

    /// The executor node container names, comma-separated.
    #[arg(
        long = "executor-nodes",
        default_value = "kardamom-executor-0,kardamom-executor-1,kardamom-executor-2"
    )]
    executor_nodes: String,

    /// The ingress node container name.
    #[arg(long = "ingress-node", default_value = "kardamom-ingress-0")]
    ingress_node: String,

    /// The sequencer node container names, comma-separated.
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
    kardamom_obs::bin::init_tracing();

    let args = Args::parse();

    let to = Address::from_str(args.to.trim())
        .map_err(|e| anyhow::anyhow!("--to is not a valid address: {e}"))?;
    // Unset `--ramp-step-tps` means "auto": target_tps / 8. Every
    // downstream use reads the resulting `NonZeroU32`; a `--target-tps`
    // too low for that to clear 0 is a config error, not a value to
    // silently floor.
    let ramp_step_tps = match args.ramp_step_tps {
        Some(explicit) => explicit,
        None => args
            .target_tps
            .get()
            .checked_div(8)
            .and_then(NonZeroU32::new)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "auto --ramp-step-tps needs --target-tps >= 8; target_tps={} auto-computes to 0 — pass --ramp-step-tps explicitly",
                    args.target_tps
                )
            })?,
    };

    let cfg = LoadConfig {
        workload: args.workload,
        rpc: args.rpc,
        chain_id: args.chain_id,
        duration: args.duration,
        target_tps: args.target_tps,
        sender_range: SenderRange::new(args.sender_offset, args.senders)?,
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
        completeness: args.completeness,
        assert_all_delivered: args.assert_all_delivered,
        chaos_mode: args.chaos_mode,
        fixed_rate: args.fixed_rate,
        scrape: csv(&args.scrape),
        metrics_via_docker: args.metrics_via_docker,
        subscribe: args.subscribe,
        feed_confirm: args.feed_confirm,
        executor_nodes: csv(&args.executor_nodes),
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

#[cfg(test)]
mod tests {
    use clap::CommandFactory;

    use super::Args;

    /// Each flag carries its own help text.
    #[test]
    fn retry_submit_and_subscribe_have_their_own_help_text() {
        let cmd = Args::command();
        let retry = cmd
            .get_arguments()
            .find(|a| a.get_id() == "retry_submit")
            .expect("retry_submit argument exists");
        let retry_help = retry.get_help().expect("retry_submit has help").to_string();
        assert!(
            retry_help.contains("retry attempts"),
            "unexpected help text for --retry-submit: {retry_help}"
        );

        let subscribe = cmd
            .get_arguments()
            .find(|a| a.get_id() == "subscribe")
            .expect("subscribe argument exists");
        let subscribe_help = subscribe
            .get_help()
            .expect("subscribe has help")
            .to_string();
        assert!(
            subscribe_help.contains("kardamom_sendRawTransactionAsync"),
            "unexpected help text for --subscribe: {subscribe_help}"
        );
    }
}
