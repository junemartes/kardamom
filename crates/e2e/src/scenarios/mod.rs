//! Target-agnostic chain-semantics scenario drivers.
//!
//! Each scenario proves one slice of the spec
//! (`docs/agents/chain-semantics-e2e-suite-spec.md`) through externally
//! observable seams only: the ingress JSON-RPC and the per-service
//! Prometheus endpoints. Nothing here knows how the pipeline was brought up,
//! so the same drivers run against the Target-L local stack today and the
//! Target-C `ci-cluster.sh` DinD cluster later (PR-4 swaps the metrics
//! transport for docker-exec probes behind this same struct).

pub mod bridge;
pub mod consistency;
pub mod crash_recovery;
pub mod da_parity;
pub mod divergence;
pub mod l1_batch;
pub mod nonce_gap;
pub mod nonce_unordered;
pub mod rpc_liveness;

use std::net::SocketAddr;
use std::time::Duration;

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
pub const INGRESS_QUEUE_DEPTH: &str = "kardamom_ingress_queue_depth";
pub const VALIDATOR_COMMITTED_BLOCK: &str = "validator_committed_block";
pub const VALIDATOR_BLOCKS_VERIFIED: &str = "validator_blocks_verified_total";
pub const VALIDATOR_BAL_MISSING: &str = "validator_bal_missing_total";
pub const VALIDATOR_DIVERGENCE: &str = "validator_divergence_total";
pub const TRIE_SHADOW_CHECKS: &str = "kardamom_state_trie_shadow_checks_total";
pub const TRIE_SHADOW_MISMATCH: &str = "kardamom_state_trie_shadow_mismatch_total";

/// One pipeline under test, reduced to its observable seams.
pub struct Target {
    pub rpc: L2Client,
    pub chain_id: u64,
    /// The ingress-side submit park bound; latency assertions derive from it.
    pub pending_receipt_timeout: Duration,
    pub ingress_metrics: SocketAddr,
    pub executor_metrics: SocketAddr,
    pub sequencer_metrics: Vec<SocketAddr>,
    /// Present when the stack runs a validator (S6/S7).
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

    /// Sum of `name` across every sequencer replica (absent ⇒ 0: counters
    /// only materialise on first increment).
    pub async fn sequencer_metric_sum(&self, name: &str) -> Result<f64> {
        let mut sum = 0.0;
        for addr in &self.sequencer_metrics {
            let s: Scrape = metrics::scrape(*addr).await?;
            sum += s.value(name).unwrap_or(0.0);
        }
        Ok(sum)
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

/// Snapshot of the sequencer health counters a semantics scenario requires
/// to stay flat (no past-nonce drops, no reorder-buffer sheds).
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
