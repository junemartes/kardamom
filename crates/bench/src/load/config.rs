//! Harness configuration (built by the CLI) + the serialized report types.

use std::path::PathBuf;
use std::time::Duration;

use alloy_primitives::{Address, U256};
use serde::Serialize;

use crate::load::accounting::Verdict;

/// Default Anvil/Hardhat test mnemonic (genesis prefunds accounts #0..#15).
pub const ANVIL_MNEMONIC: &str = "test test test test test test test test test test test junk";

/// Which set of txs must be 100% receipted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Completeness {
    /// Every tx ingress *accepted* must receipt (chaos-safe: a submit that
    /// fails during an outage is retried, not held against must-deliver).
    Accepted,
    /// Every tx *offered* must receipt (strict; non-chaos soak only).
    Offered,
}

/// Full harness configuration (built by the CLI).
#[derive(Debug, Clone)]
pub struct LoadConfig {
    /// Ingress JSON-RPC URL.
    pub rpc: String,
    /// L2 chain id (probed via `eth_chainId` when `None`).
    pub chain_id: Option<u64>,
    /// Soak duration.
    pub duration: Duration,
    /// Ramp ceiling / chaos-mode fixed rate (tx/s).
    pub target_tps: u32,
    /// Number of sender accounts.
    pub senders: u32,
    /// First account index in the mnemonic table (reserve low accounts).
    pub sender_offset: u32,
    /// Per-sender starting nonce.
    pub nonce_start: u64,
    /// BIP-39 mnemonic the senders derive from.
    pub mnemonic: String,
    /// Transfer sink address.
    pub to: Address,
    /// Wei per transfer.
    pub value: U256,
    /// Workload family: plain transfers, or the DeFi mix (CLOB + swap
    /// pool + vault; see `load::defi`). DeFi deploys its contracts from
    /// the FIRST sender before the ramp and reports gas-centric throughput.
    pub workload: Workload,
    /// Legacy gas price (wei).
    pub gas_price: u128,
    /// Max outstanding submits (open-loop back-pressure bound).
    pub max_in_flight: u32,
    /// Max allowed sealer-minus-executor block gap.
    pub max_gap: u64,
    /// How long to keep draining receipts after the send window.
    pub drain_timeout: Duration,
    /// Per-submit retry attempts on transient failure.
    pub retry_submit: u32,
    /// Ramp increment per step (tx/s).
    pub ramp_step_tps: u32,
    /// Seconds held per ramp step.
    pub ramp_step_secs: u64,
    /// Fraction of the discovered max to soak at.
    pub soak_fraction: f64,
    /// Completeness criterion.
    pub completeness: Completeness,
    /// Fail unless completeness is met.
    pub assert_all_delivered: bool,
    /// Chaos framing (skip ramp; tolerate transient blips).
    pub chaos_mode: bool,
    /// Fixed-rate framing: skip the ramp and soak at `target_tps` with the
    /// STRICT (non-chaos) verdict. For CI invariant gating on weak/shared
    /// hosts: edge discovery there measures the hypervisor, not the stack —
    /// pass/fail becomes host luck (the load shard's 800→18 ceiling swings).
    /// Correctness (zero loss, gaps, keep-pace) is rate-independent; gate on
    /// it at a rate the weakest runner sustains, and leave performance
    /// numbers to the perf suite on dedicated hardware.
    pub fixed_rate: bool,
    /// Services to scrape.
    pub scrape: Vec<String>,
    /// `docker exec` scrape vs direct.
    pub metrics_via_docker: bool,
    /// Submit via `kardamom_sendRawTransactionAsync` + receive receipts on a
    /// `kardamom_subscribeReceipts` WebSocket feed, instead of the parked
    /// `eth_sendRawTransaction`. In-flight txs then hold no connections.
    pub subscribe: bool,
    /// Blocking mode only: confirm receipts via the WebSocket feed instead
    /// of a per-tx `eth_getTransactionReceipt` re-fetch after each accepted
    /// submit. Halves the harness's HTTP request load (2 → 1 calls per tx —
    /// at a 10k tx/s target the re-fetches alone are another 10k rps through
    /// the proxy + ingress) with identical verification integrity: every
    /// accepted tx still ends confirmed (feed), re-polled (drain), or
    /// counted `missing`. No effect in subscribe mode (already feed-driven).
    pub feed_confirm: bool,
    /// Executor node-container names.
    pub executor_nodes: Vec<String>,
    /// Ingress node-container name.
    pub ingress_node: String,
    /// Sequencer node-container names.
    pub sequencer_nodes: Vec<String>,
    /// Optional JSON report path.
    pub output: Option<PathBuf>,
}

/// Workload family driven by the harness.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, serde::Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum Workload {
    #[default]
    Transfers,
    Defi,
}

impl std::str::FromStr for Workload {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "transfers" => Ok(Self::Transfers),
            "defi" => Ok(Self::Defi),
            other => anyhow::bail!("unknown workload {other:?} (transfers|defi)"),
        }
    }
}

/// One ramp step's sustainability evaluation.
#[derive(Debug, Clone, Serialize, serde::Deserialize)]
pub struct RampStep {
    /// Offered rate for this step (tx/s).
    pub rate: u32,
    /// Accepted / offered over the step.
    pub accept_ratio: f64,
    /// Executors advanced and stayed within `max_gap`.
    pub gap_ok: bool,
    /// No sequencer drops/evictions over the step.
    pub seq_clean: bool,
    /// All three signals held.
    pub sustainable: bool,
    /// Receipt latency over THIS step only (µs) — localizes where in the
    /// ramp the tail degrades.
    #[serde(default)]
    pub lat_p50_us: u64,
    #[serde(default)]
    pub lat_p95_us: u64,
    /// Gas consumed by receipts confirmed during this step (Mgas/s = this
    /// over the step duration).
    #[serde(default)]
    pub gas_used: u64,
    #[serde(default)]
    pub lat_p99_us: u64,
}

/// Serialized harness report.
#[derive(Debug, Clone, Serialize, serde::Deserialize)]
pub struct LoadReport {
    /// `"soak"` or `"chaos"`.
    pub mode: String,
    /// Configured ramp ceiling / chaos rate.
    pub target_tps: u32,
    /// Highest sustainable ramp rate (== target in chaos mode).
    pub discovered_max_tps: u32,
    /// Rate the soak ran at.
    pub soak_rate_tps: u32,
    /// Soak duration in seconds.
    pub duration_secs: f64,
    /// Ramp curve.
    pub ramp: Vec<RampStep>,
    /// Receipt latency p50 (µs).
    pub lat_p50_us: u64,
    /// Receipt latency p95 (µs).
    #[serde(default)]
    pub lat_p95_us: u64,
    /// Receipt latency p99 (µs).
    pub lat_p99_us: u64,
    /// Receipt latency max (µs).
    pub lat_max_us: u64,
    /// Total gas consumed by receipted txs over the soak window.
    #[serde(default)]
    pub total_gas: u64,
    /// Workload the run drove (`transfers` or `defi`).
    #[serde(default)]
    pub workload: String,
    /// Completeness + drop accounting + keep-pace.
    pub verdict: Verdict,
}
