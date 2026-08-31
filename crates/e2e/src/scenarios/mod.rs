//! Target-agnostic chain-semantics scenario drivers.
//!
//! Each scenario proves one part of the spec
//! (`docs/agents/chain-semantics-e2e-suite-spec.md`) through external seams
//! only: the ingress JSON-RPC and the per-service Prometheus endpoints.
//! Nothing here knows how the pipeline started. So the same drivers run
//! against the Target-L local stack today, and later against the Target-C
//! `ci-cluster.sh` DinD cluster (PR-4 swaps the metrics transport for
//! docker-exec probes behind this same struct).

pub mod bridge;
pub mod consistency;
pub mod crash_recovery;
pub mod da_parity;
pub mod derivation;
pub mod divergence;
pub mod l1_batch;
pub mod nonce_gap;
pub mod nonce_unordered;
pub mod rpc_liveness;
pub mod rpc_vectors;
pub mod upgrade;
pub mod xchain;
pub mod xchain_da_parity;
pub mod xchain_two_stacks;

use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use alloy_primitives::{B256, U256};
use anyhow::{Context, Result};

use crate::harness::l2::L2Client;
use crate::harness::metrics::{self, Scrape};

/// JSON-RPC error codes the ingress contract pins
/// (`crates/ingress/src/error.rs`).
pub const CODE_TIMEOUT: i32 = -32000;
pub const CODE_INVALID: i32 = -32602;
pub const CODE_INTERNAL: i32 = -32603;

/// Metric names asserted across scenarios.
pub const EXEC_TX_APPLIED: &str = "kardamom_executor_tx_applied_total";
pub const EXEC_BLOCK_NUMBER: &str = "kardamom_executor_block_number";
pub const SEQ_DROPPED_PAST: &str = "kardamom_sequencer_tx_dropped_past_total";
pub const SEQ_EVICTIONS: &str = "kardamom_sequencer_pending_evictions_total";
pub const SEQ_REMOTE_EPOCHS_RELAYED: &str = "kardamom_sequencer_remote_epochs_relayed_total";
pub const SEQ_REMOTE_MESSAGES_RELAYED: &str = "kardamom_sequencer_remote_messages_relayed_total";
pub const INGRESS_QUEUE_DEPTH: &str = "kardamom_ingress_queue_depth";
pub const VALIDATOR_COMMITTED_BLOCK: &str = "validator_committed_block";
pub const VALIDATOR_BLOCKS_VERIFIED: &str = "validator_blocks_verified_total";
pub const VALIDATOR_BAL_MISSING: &str = "validator_bal_missing_total";
pub const VALIDATOR_EPOCHS_VERIFIED: &str = "validator_epochs_verified_total";
pub const VALIDATOR_EPOCH_FAULTS: &str = "validator_epoch_faults_total";
pub const VALIDATOR_DIVERGENCE: &str = "validator_divergence_total";
pub const TRIE_SHADOW_CHECKS: &str = "kardamom_state_trie_shadow_checks_total";
pub const TRIE_SHADOW_MISMATCH: &str = "kardamom_state_trie_shadow_mismatch_total";

/// One pipeline under test, reduced to its observable seams.
pub struct Target {
    pub rpc: L2Client,
    pub chain_id: u64,
    /// The ingress-side submit park limit. Latency checks derive from this
    /// value.
    pub pending_receipt_timeout: Duration,
    pub ingress_metrics: SocketAddr,
    pub executor_metrics: SocketAddr,
    pub sequencer_metrics: Vec<SocketAddr>,
    /// Present when the stack runs a validator.
    pub validator_metrics: Option<SocketAddr>,
}

impl Target {
    pub async fn executor_metric(&self, name: &str) -> Result<f64> {
        let s = metrics::scrape(self.executor_metrics).await?;
        s.value(name)
            .with_context(|| format!("executor metric {name} absent"))
    }

    pub async fn validator_metric(&self, name: &str) -> Result<f64> {
        let addr = self
            .validator_metrics
            .context("target has no validator (StackConfig::validator)")?;
        let s = metrics::scrape(addr).await?;
        s.value(name)
            .with_context(|| format!("validator metric {name} absent"))
    }

    pub async fn ingress_metric(&self, name: &str) -> Result<f64> {
        let s = metrics::scrape(self.ingress_metrics).await?;
        s.value(name)
            .with_context(|| format!("ingress metric {name} absent"))
    }

    /// Sum of `name` across every sequencer replica. A missing value counts
    /// as 0, because a counter appears only after its first increase.
    pub async fn sequencer_metric_sum(&self, name: &str) -> Result<f64> {
        let mut sum = 0.0;
        for addr in &self.sequencer_metrics {
            let s: Scrape = metrics::scrape(*addr).await?;
            sum += s.value(name).unwrap_or(0.0);
        }
        Ok(sum)
    }

    /// The validator's verdict on the run so far — the interop scenarios'
    /// LOAD-BEARING assertion (S12/S14): the validator must have COMMITTED
    /// past the executor's current durable head (so the whole-block path
    /// actually executed the interop blocks instead of fail-stopping — a
    /// dead validator times out here with its metrics port refusing), it
    /// must have VERIFIED blocks (non-vacuity), and it must have proven no
    /// divergence.
    pub async fn assert_validator_verdict(&self, what: &str) -> Result<()> {
        let addr = self
            .validator_metrics
            .context("target has no validator (StackConfig::validator)")?;
        let target_block = self.executor_metric(EXEC_BLOCK_NUMBER).await?;
        metrics::poll_until(
            &format!("{what}: validator committed >= {target_block}"),
            Duration::from_secs(90),
            Duration::from_millis(250),
            || async {
                let s = metrics::scrape(addr).await?;
                let committed = s.value(VALIDATOR_COMMITTED_BLOCK).unwrap_or(0.0);
                Ok((committed >= target_block).then_some(committed))
            },
        )
        .await?;
        let s = metrics::scrape(addr).await?;
        // Counters only materialise on first increment: absent == 0.
        let divergence = s.value(VALIDATOR_DIVERGENCE).unwrap_or(0.0);
        anyhow::ensure!(
            divergence == 0.0,
            "{what}: validator recorded {divergence} divergence(s)"
        );
        let verified = s.value(VALIDATOR_BLOCKS_VERIFIED).unwrap_or(0.0);
        anyhow::ensure!(
            verified > 0.0,
            "{what}: validator verified no blocks — its verdict is vacuous"
        );
        Ok(())
    }

    /// Wait until the executor's applied-tx counter reaches `at_least`.
    pub async fn wait_executor_applied(&self, at_least: f64, timeout: Duration) -> Result<f64> {
        metrics::poll_until(
            &format!("executor {EXEC_TX_APPLIED} >= {at_least}"),
            timeout,
            Duration::from_millis(200),
            || async {
                let v = self.executor_metric(EXEC_TX_APPLIED).await.unwrap_or(0.0);
                Ok((v >= at_least).then_some(v))
            },
        )
        .await
    }
}

// ---------------------------------------------------------------------------
// Shared receipt helpers. Every scenario that reads an L2 receipt uses
// these helpers, instead of writing its own JSON code.
// ---------------------------------------------------------------------------

/// Wait for the L2 receipt of `hash`, which for a deposit is its
/// `source_hash`.
pub async fn await_l2_receipt(t: &Target, hash: B256, what: &str) -> Result<serde_json::Value> {
    metrics::poll_until(
        &format!("L2 receipt for {what}"),
        Duration::from_secs(60),
        Duration::from_millis(250),
        || async { Ok(t.rpc.receipt(hash).await.result.ok().flatten()) },
    )
    .await
}

/// A string field of receipt `r` (`"status"`, `"blockNumber"`, …).
pub fn receipt_field<'a>(r: &'a serde_json::Value, key: &str) -> Option<&'a str> {
    r.get(key).and_then(|v| v.as_str())
}

/// Where the chain placed the receipt's transaction, as
/// `(blockNumber, transactionIndex)`. Both values are hex-parsed.
pub fn receipt_placement(r: &serde_json::Value) -> Result<(u64, u64)> {
    let hex_u64 = |key: &str| -> Result<u64> {
        let s = receipt_field(r, key).with_context(|| format!("receipt has no {key}: {r}"))?;
        u64::from_str_radix(s.trim_start_matches("0x"), 16)
            .with_context(|| format!("parse receipt {key} {s:?}"))
    };
    Ok((hex_u64("blockNumber")?, hex_u64("transactionIndex")?))
}

/// The transaction behind receipt `r` executed successfully (`status == 0x1`).
pub fn assert_receipt_ok(r: &serde_json::Value, what: &str) -> Result<()> {
    anyhow::ensure!(
        receipt_field(r, "status") == Some("0x1"),
        "{what}: receipt not successful (reverted): {r}"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Shared state-DB reads. These open a live service's state directory as
// read-only mdbx. mdbx supports many processes: the reader takes an MVCC
// snapshot and never blocks or disturbs the writer.
// ---------------------------------------------------------------------------

/// Read-only mdbx open of a service's state dir.
pub(crate) fn open_state_ro(dir: &Path) -> Result<kardamom_state::StateEnv> {
    kardamom_state::StateEnvBuilder::new(dir)
        .read_only(true)
        .open()
        .context("open state dir read-only")
}

/// The validator's current committed MPT state root, read from its live
/// state DB.
///
/// A future `eth_getProof`-style API, or a root-carrying metric, could
/// replace this seam. Today nothing exposes roots. This is the same gap a
/// real withdrawer faces: it cannot get the `stateRoot` argument that
/// `finalizeWithdrawal` needs. This is the parity target: an
/// independent computation, not a value the DA path produced.
pub fn read_validator_state_root(state_dir: &Path) -> Result<Option<B256>> {
    let env = open_state_ro(state_dir)?;
    let snap = kardamom_state::StateSnapshot::open(&env).context("snapshot validator state")?;
    snap.state_root().context("read validator state root")
}

/// A consistent read of the `KardamomChainState` predeploy, together with
/// the block number it belongs to.
///
/// Every field comes from one `StateSnapshot`: a single long-lived read
/// transaction. This is what makes the exact checks in the verified-L1
/// scenarios
/// possible. Reading the head block and the beacon separately would race the
/// chain: the beacon could advance between the two reads, and an exact
/// `beats == head - activation_block + 1` check would then flap.
#[derive(Debug, Clone, Copy)]
pub struct ChainStateView {
    /// Highest block committed in the state DB this was read from.
    pub block_number: u64,
    /// Raw activation timestamp (ms) of the health-check feature. 0 means
    /// the feature is never scheduled.
    pub activation: U256,
    /// Health beacon, unpacked: `(beats, block_number, timestamp_ms)`.
    pub beacon: (u64, u64, u64),
}

impl ChainStateView {
    /// Beats recorded so far.
    pub fn beats(&self) -> u64 {
        self.beacon.0
    }
}

/// Read the chain-state predeploy from a node's state DB.
///
/// This reads mdbx directly, not RPC, because the ingress serves neither
/// `eth_call` nor `eth_getStorageAt`. The read is read-only and MVCC, so it
/// never blocks the running node's writer.
pub fn read_chain_state(state_dir: &Path) -> Result<ChainStateView> {
    use kardamom_exec_core::features::{
        FEATURE_HEALTH_CHECK, HEALTH_BEACON_SLOT, activation_slot, unpack_beacon,
    };
    use kardamom_types::StateDatabase;
    use kardamom_types::upgrades::CHAIN_STATE;

    let env = open_state_ro(state_dir)?;
    let snap = kardamom_state::StateSnapshot::open(&env).context("snapshot state")?;
    let block_number = snap.block_number();
    let activation = snap
        .storage(CHAIN_STATE, activation_slot(FEATURE_HEALTH_CHECK))
        .context("read activation slot")?;
    let beacon = unpack_beacon(
        snap.storage(CHAIN_STATE, HEALTH_BEACON_SLOT)
            .context("read beacon slot")?,
    );
    Ok(ChainStateView {
        block_number,
        activation,
        beacon,
    })
}

/// Snapshot of the sequencer health counters that a semantics scenario
/// needs to stay flat: no past-nonce drops, and no reorder-buffer sheds.
pub struct SeqCounters {
    pub dropped_past: f64,
    pub evictions: f64,
}

impl SeqCounters {
    pub async fn snapshot(t: &Target) -> Result<Self> {
        Ok(Self {
            dropped_past: t.sequencer_metric_sum(SEQ_DROPPED_PAST).await?,
            evictions: t.sequencer_metric_sum(SEQ_EVICTIONS).await?,
        })
    }

    pub async fn assert_flat(&self, t: &Target, context: &str) -> Result<()> {
        let now = Self::snapshot(t).await?;
        anyhow::ensure!(
            now.dropped_past == self.dropped_past,
            "{context}: sequencer dropped_past grew {} -> {}",
            self.dropped_past,
            now.dropped_past
        );
        anyhow::ensure!(
            now.evictions == self.evictions,
            "{context}: sequencer pending evictions grew {} -> {}",
            self.evictions,
            now.evictions
        );
        Ok(())
    }
}
