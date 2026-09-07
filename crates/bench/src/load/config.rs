//! This module has the harness configuration, built by the CLI, and the
//! serialized report types.

use std::num::{NonZeroU32, NonZeroU64};
use std::path::PathBuf;
use std::time::Duration;

use alloy_primitives::{Address, U256};
use serde::Serialize;

use crate::load::accounting::Verdict;

/// The default Anvil and Hardhat test mnemonic. Genesis prefunds
/// accounts 0 through 15.
pub use crate::ANVIL_MNEMONIC;

/// The set of transactions that must be 100% receipted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Completeness {
    /// Every transaction ingress accepts must get a receipt. This is
    /// chaos-safe: a submit that fails during an outage is retried,
    /// not counted against must-deliver.
    Accepted,
    /// Every transaction offered must get a receipt. This is strict,
    /// for a non-chaos soak only.
    Offered,
}

impl std::str::FromStr for Completeness {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "accepted" => Ok(Self::Accepted),
            "offered" => Ok(Self::Offered),
            other => anyhow::bail!("unknown completeness {other:?} (accepted|offered)"),
        }
    }
}

/// The senders a run uses: `count` accounts starting at `offset` in the
/// mnemonic table. [`SenderRange::new`] checks the two add without
/// overflow once, at the boundary, so every reader can trust the pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SenderRange {
    offset: u32,
    count: NonZeroU32,
    /// `offset + count`, computed once at construction time.
    derive_count: u32,
}

impl SenderRange {
    /// Build a range. Returns an error only if `offset + count` would
    /// overflow `u32`.
    ///
    /// # Errors
    ///
    /// Returns an error if `offset + count` overflows `u32`.
    pub fn new(offset: u32, count: NonZeroU32) -> anyhow::Result<Self> {
        let derive_count = offset
            .checked_add(count.get())
            .ok_or_else(|| anyhow::anyhow!("sender_offset + senders overflows u32"))?;
        Ok(Self {
            offset,
            count,
            derive_count,
        })
    }

    /// The first account index in the mnemonic table.
    #[must_use]
    pub fn offset(&self) -> u32 {
        self.offset
    }

    /// The number of sender accounts.
    #[must_use]
    pub fn count(&self) -> NonZeroU32 {
        self.count
    }

    /// `offset + count`: the number of signers to derive so that
    /// `signers[offset..]` has exactly `count` elements.
    #[must_use]
    pub fn derive_count(&self) -> u32 {
        self.derive_count
    }
}

/// The full harness configuration, built by the CLI.
#[derive(Debug, Clone)]
#[allow(
    clippy::struct_excessive_bools,
    reason = "each bool is an independent CLI flag, built from Args; grouping them would rename flags"
)]
pub struct LoadConfig {
    /// The ingress JSON-RPC URL.
    pub rpc: String,
    /// The L2 chain ID. When `None`, the code probes it with `eth_chainId`.
    pub chain_id: Option<u64>,
    /// The soak duration.
    pub duration: Duration,
    /// The ramp ceiling, or the chaos-mode fixed rate, in tx/s.
    /// Non-zero: `ramp_to_max`'s `while rate <= target_tps` loop and the
    /// soak-rate `clamp(1, target_tps)` both need a real upper bound.
    pub target_tps: NonZeroU32,
    /// The sender accounts this run uses.
    pub sender_range: SenderRange,
    /// The starting nonce for each sender.
    pub nonce_start: u64,
    /// The BIP-39 mnemonic the senders derive from.
    pub mnemonic: String,
    /// The transfer sink address.
    pub to: Address,
    /// The wei value of each transfer.
    pub value: U256,
    /// The workload family: plain transfers, or the `DeFi` mix of a CLOB,
    /// a swap pool, and a vault. See `load::defi`. The `DeFi` workload
    /// deploys its contracts from the first sender before the ramp, and
    /// reports gas-centric throughput.
    pub workload: Workload,
    /// The legacy gas price, in wei.
    pub gas_price: u128,
    /// The limit on outstanding submits. This bounds open-loop back
    /// pressure. Non-zero: `Semaphore::new` with 0 permits would never
    /// admit a submit.
    pub max_in_flight: NonZeroU32,
    /// The maximum allowed gap between the sealer and the executor block.
    pub max_gap: u64,
    /// How long to keep draining receipts after the send window ends.
    pub drain_timeout: Duration,
    /// The number of per-submit retry attempts on a transient failure.
    pub retry_submit: u32,
    /// The ramp increment for each step, in tx/s. Non-zero: it is a
    /// divisor at `per_sender_estimate`, and a step count of 0 would
    /// never advance the ramp.
    pub ramp_step_tps: NonZeroU32,
    /// The number of seconds held at each ramp step. Non-zero: it is a
    /// divisor in the ramp's `Mgas/s` display.
    pub ramp_step_secs: NonZeroU64,
    /// The fraction of the discovered maximum rate to soak at.
    pub soak_fraction: f64,
    /// The completeness criterion.
    pub completeness: Completeness,
    /// Fail the run unless the completeness criterion is met.
    pub assert_all_delivered: bool,
    /// The chaos framing: skip the ramp, and tolerate transient blips.
    pub chaos_mode: bool,
    /// The fixed-rate framing: skip the ramp, and soak at `target_tps`
    /// with the strict, non-chaos verdict.
    ///
    /// Use this for CI invariant gating on a weak or shared host. Edge
    /// discovery on such a host measures the hypervisor, not the stack,
    /// so pass or fail becomes host luck: the sustainable ceiling is
    /// host-dependent. Correctness, meaning zero loss, no gaps, and
    /// keeping pace, does not depend on rate. Gate on correctness at a
    /// rate the weakest runner can sustain, and leave performance
    /// numbers to the perf suite on dedicated hardware.
    pub fixed_rate: bool,
    /// The services to scrape.
    pub scrape: Vec<String>,
    /// Scrape through `docker exec`, instead of a direct scrape.
    pub metrics_via_docker: bool,
    /// Submit through `kardamom_sendRawTransactionAsync`, and receive
    /// receipts on a `kardamom_subscribeReceipts` WebSocket feed, instead
    /// of the parked `eth_sendRawTransaction`. An in-flight transaction
    /// then holds no connection.
    pub subscribe: bool,
    /// In blocking mode only: confirm receipts through the WebSocket feed,
    /// instead of a per-transaction `eth_getTransactionReceipt` re-fetch
    /// after each accepted submit. This halves the harness's HTTP request
    /// load, from 2 calls per transaction to 1. At a 10k tx/s target, the
    /// re-fetches alone would add another 10k requests per second through
    /// the proxy and ingress. Verification stays just as strict: every
    /// accepted transaction still ends up confirmed by the feed, re-polled
    /// by the drain, or counted as `missing`. This setting has no effect
    /// in subscribe mode, which is already feed-driven.
    pub feed_confirm: bool,
    /// The executor node container names.
    pub executor_nodes: Vec<String>,
    /// The ingress node container name.
    pub ingress_node: String,
    /// The sequencer node container names.
    pub sequencer_nodes: Vec<String>,
    /// An optional JSON report path.
    pub output: Option<PathBuf>,
}

impl LoadConfig {
    /// The submit mode: subscribe when `subscribe` is set, blocking
    /// otherwise.
    pub(crate) fn submit_mode(&self) -> crate::load::engine::SubmitMode {
        if self.subscribe {
            crate::load::engine::SubmitMode::Subscribe
        } else {
            crate::load::engine::SubmitMode::Blocking
        }
    }

    /// Whether a submit should confirm through the feed instead of a
    /// per-transaction re-fetch: `feed_confirm` has no effect in
    /// subscribe mode, which is already feed-driven.
    pub(crate) fn feed_confirm_on(&self) -> bool {
        self.feed_confirm && !self.subscribe
    }
}

impl Default for LoadConfig {
    /// Sensible defaults, matching `kardamom-load`'s own CLI defaults
    /// where one exists. `kardamom-perf`'s `load_cfg` builds on this
    /// with `..Default::default()`, setting only the dozen fields its
    /// ramp and soak phases actually vary, instead of repeating every
    /// field. `rpc`, `chain_id`, and `sender_range` have no sensible
    /// default beyond a placeholder: every real caller sets them.
    fn default() -> Self {
        Self {
            rpc: String::new(),
            chain_id: None,
            duration: Duration::from_secs(300),
            target_tps: NonZeroU32::new(200).expect("200 != 0"),
            sender_range: SenderRange::new(0, NonZeroU32::new(16).expect("16 != 0"))
                .expect("0 + 16 never overflows"),
            nonce_start: 0,
            mnemonic: ANVIL_MNEMONIC.to_string(),
            to: "0x000000000000000000000000000000000000dEaD"
                .parse()
                .expect("valid address literal"),
            value: U256::from(1u64),
            workload: Workload::default(),
            gas_price: 1_000_000_000,
            max_in_flight: NonZeroU32::new(256).expect("256 != 0"),
            max_gap: 5,
            drain_timeout: Duration::from_secs(90),
            retry_submit: 2,
            ramp_step_tps: NonZeroU32::MIN,
            ramp_step_secs: NonZeroU64::new(15).expect("15 != 0"),
            soak_fraction: 0.8,
            completeness: Completeness::Accepted,
            assert_all_delivered: false,
            chaos_mode: false,
            fixed_rate: false,
            scrape: vec!["executor".into(), "ingress".into()],
            metrics_via_docker: true,
            subscribe: false,
            feed_confirm: false,
            executor_nodes: vec![
                "kardamom-executor-0".into(),
                "kardamom-executor-1".into(),
                "kardamom-executor-2".into(),
            ],
            ingress_node: "kardamom-ingress-0".into(),
            sequencer_nodes: vec!["kardamom-sequencer-0".into(), "kardamom-sequencer-1".into()],
            output: None,
        }
    }
}

/// The workload family the harness drives.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, serde::Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum Workload {
    #[default]
    Transfers,
    Defi,
}

impl Workload {
    /// The lowercase name this variant parses from and prints as.
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Transfers => "transfers",
            Self::Defi => "defi",
        }
    }
}

impl std::fmt::Display for Workload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
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

/// The sustainability evaluation of one ramp step.
#[derive(Debug, Clone, Serialize, serde::Deserialize)]
pub struct RampStep {
    /// The offered rate for this step, in tx/s.
    pub rate: u32,
    /// The accepted-to-offered ratio over the step.
    pub accept_ratio: f64,
    /// Whether the executors advanced and stayed within `max_gap`.
    pub gap_ok: bool,
    /// Whether the sequencer had no drops or evictions over the step.
    pub seq_clean: bool,
    /// Whether all three signals held.
    pub sustainable: bool,
    /// The receipt latency during this step only, in microseconds. This
    /// shows where in the ramp the tail latency degrades.
    #[serde(default)]
    pub lat_p50_us: u64,
    #[serde(default)]
    pub lat_p95_us: u64,
    /// The gas used by receipts confirmed during this step. Divide by
    /// the step duration to get Mgas/s.
    #[serde(default)]
    pub gas_used: u64,
    #[serde(default)]
    pub lat_p99_us: u64,
}

/// The serialized harness report.
#[derive(Debug, Clone, Serialize, serde::Deserialize)]
pub struct LoadReport {
    /// `"soak"` or `"chaos"`.
    pub mode: String,
    /// The configured ramp ceiling, or the chaos rate.
    pub target_tps: u32,
    /// The highest sustainable ramp rate. This equals `target_tps` in
    /// chaos mode.
    pub discovered_max_tps: u32,
    /// The rate the soak ran at.
    pub soak_rate_tps: u32,
    /// The soak duration, in seconds.
    pub duration_secs: f64,
    /// The ramp curve.
    pub ramp: Vec<RampStep>,
    /// The receipt latency p50, in microseconds.
    pub lat_p50_us: u64,
    /// The receipt latency p95, in microseconds.
    #[serde(default)]
    pub lat_p95_us: u64,
    /// The receipt latency p99, in microseconds.
    pub lat_p99_us: u64,
    /// The receipt latency maximum, in microseconds.
    pub lat_max_us: u64,
    /// The total gas used by receipted transactions over the soak window.
    #[serde(default)]
    pub total_gas: u64,
    /// The workload the run drove: `transfers` or `defi`.
    #[serde(default)]
    pub workload: String,
    /// The completeness, drop accounting, and keep-pace verdict.
    pub verdict: Verdict,
}
