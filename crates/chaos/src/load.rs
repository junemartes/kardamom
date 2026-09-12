//! The steady background load of a case, run in-process through the
//! `kardamom-load` harness library. One dedicated funded account, one
//! sender, nonces from 0, in chaos mode: a fixed rate, and every
//! accepted transaction must get a receipt eventually.

use std::num::{NonZeroU32, NonZeroU64};
use std::path::PathBuf;
use std::time::Duration;

use alloy_primitives::{Address, U256, address};
use anyhow::Context;
use kardamom_bench::ANVIL_MNEMONIC;
use kardamom_bench::load::{self, Completeness, LoadConfig, SenderRange, Workload};
use serde::Deserialize;

/// The burn address of every load transfer.
const SINK: Address = address!("000000000000000000000000000000000000dEaD");

/// The verdict fields the suite reads back from the report.
#[derive(Debug, Clone, Deserialize)]
pub struct Verdict {
    pub pass: bool,
    #[serde(default)]
    pub failures: Vec<String>,
    pub missing: u64,
    #[serde(default)]
    pub seq_dropped: Option<i64>,
}

#[derive(Debug, Clone, Deserialize)]
struct Report {
    verdict: Verdict,
}

/// The inputs of one case's load.
#[derive(Debug, Clone)]
pub struct LoadSpec {
    pub rpc_url: String,
    pub chain_id: u64,
    pub account: u32,
    pub duration: Duration,
    pub tps: NonZeroU32,
    pub retry_submit: u32,
    pub max_gap: u64,
    pub drain_timeout: Duration,
    pub report_path: PathBuf,
    /// The node container names the harness scrapes.
    pub executor_nodes: Vec<String>,
    pub ingress_node: String,
    pub sequencer_nodes: Vec<String>,
}

impl LoadSpec {
    fn config(&self) -> anyhow::Result<LoadConfig> {
        let sender_range =
            SenderRange::new(self.account, NonZeroU32::new(1).context("one sender")?)?;
        let ramp_step_tps =
            NonZeroU32::new(self.tps.get().checked_div(8).unwrap_or(1)).context("ramp step")?;
        Ok(LoadConfig {
            workload: Workload::Transfers,
            rpc: self.rpc_url.clone(),
            chain_id: Some(self.chain_id),
            duration: self.duration,
            target_tps: self.tps,
            sender_range,
            nonce_start: 0,
            mnemonic: ANVIL_MNEMONIC.to_string(),
            to: SINK,
            value: U256::from(1),
            gas_price: 1_000_000_000,
            max_in_flight: NonZeroU32::new(256).context("max in flight")?,
            max_gap: self.max_gap,
            drain_timeout: self.drain_timeout,
            retry_submit: self.retry_submit,
            ramp_step_tps,
            ramp_step_secs: NonZeroU64::new(15).context("ramp step secs")?,
            soak_fraction: 0.8,
            completeness: Completeness::Accepted,
            assert_all_delivered: true,
            chaos_mode: true,
            fixed_rate: false,
            scrape: vec!["executor".into(), "ingress".into(), "sequencer".into()],
            metrics_via_docker: true,
            subscribe: false,
            feed_confirm: false,
            executor_nodes: self.executor_nodes.clone(),
            ingress_node: self.ingress_node.clone(),
            sequencer_nodes: self.sequencer_nodes.clone(),
            output: Some(self.report_path.clone()),
        })
    }
}

/// A running load, until its window and drain end.
pub struct LoadRun {
    task: tokio::task::JoinHandle<anyhow::Result<bool>>,
    report_path: PathBuf,
}

impl LoadRun {
    /// Start the load of `spec` on the current runtime.
    ///
    /// # Errors
    ///
    /// Returns an error if the configuration is invalid.
    pub fn start(spec: &LoadSpec) -> anyhow::Result<Self> {
        let cfg = spec.config()?;
        Ok(Self {
            task: tokio::spawn(load::run(cfg)),
            report_path: spec.report_path.clone(),
        })
    }

    /// Whether the load task has already ended, which before the window
    /// closes means it died.
    #[must_use]
    pub fn is_finished(&self) -> bool {
        self.task.is_finished()
    }

    /// Wait for the window and the drain, then read the verdict from the
    /// report file. The harness's own pass or fail return is folded into
    /// the verdict it wrote.
    ///
    /// # Errors
    ///
    /// Returns an error if the harness failed before a verdict, or the
    /// report is missing or unreadable.
    pub async fn finish(self) -> anyhow::Result<Verdict> {
        let outcome = self.task.await.context("join the load task")?;
        let report_text = std::fs::read_to_string(&self.report_path).with_context(|| {
            format!(
                "load report {} missing (harness: {outcome:?})",
                self.report_path.display()
            )
        })?;
        let report: Report = serde_json::from_str(&report_text)
            .with_context(|| format!("decode {}", self.report_path.display()))?;
        Ok(report.verdict)
    }

    /// Abort the load, for cleanup after a failed case.
    pub fn abort(&self) {
        self.task.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_report_verdict_decodes() {
        let text = r#"{"mode":"chaos","verdict":{"pass":false,"failures":["x"],"offered":10,
            "accepted":10,"receipted":9,"missing":1,"unlanded":0,"bad_status":0,
            "inferred_ingress_drop":null,"seq_dropped":2,"keep_pace":[]}}"#;
        let report: Report = serde_json::from_str(text).unwrap();
        assert!(!report.verdict.pass);
        assert_eq!(report.verdict.missing, 1);
        assert_eq!(report.verdict.seq_dropped, Some(2));
    }
}
