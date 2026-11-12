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
use kardamom_bench::load::{
    self, CalldataMix, Completeness, LoadConfig, MetricsTargets, NonceStart, Progress, SenderRange,
    Workload,
};
use serde::Deserialize;

/// The burn address of every load transfer.
pub const SINK: Address = address!("000000000000000000000000000000000000dEaD");

/// The verdict fields the suite reads back from the report.
#[derive(Debug, Clone, Deserialize)]
pub struct Verdict {
    pub pass: bool,
    #[serde(default)]
    pub failures: Vec<String>,
    /// The undelivered transactions: the accepted ones with no receipt,
    /// plus the unlanded ones when the completeness counts them.
    pub missing: u64,
    /// The offered transactions that the ingress refused through all
    /// retries, so they never got an ack.
    #[serde(default)]
    pub unlanded: u64,
    #[serde(default)]
    pub offered: u64,
    #[serde(default)]
    pub accepted: u64,
    #[serde(default)]
    pub seq_dropped: Option<i64>,
    /// The receipt latency of the run, from the top of the report.
    #[serde(skip)]
    pub latency: Latency,
}

/// The receipt latency percentiles of one run, in microseconds. A
/// blocking submit parks at the ingress until its receipt, so this is
/// also the time each submit held its in-flight slot.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
pub struct Latency {
    #[serde(default)]
    pub lat_p50_us: u64,
    #[serde(default)]
    pub lat_p95_us: u64,
    #[serde(default)]
    pub lat_p99_us: u64,
    #[serde(default)]
    pub lat_max_us: u64,
}

impl std::fmt::Display for Latency {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "receipt latency p50={}ms p95={}ms p99={}ms max={}ms",
            self.lat_p50_us / 1000,
            self.lat_p95_us / 1000,
            self.lat_p99_us / 1000,
            self.lat_max_us / 1000
        )
    }
}

#[derive(Debug, Clone, Deserialize)]
struct Report {
    verdict: Verdict,
    #[serde(flatten)]
    latency: Latency,
}

/// The inputs of one case's load.
#[derive(Debug, Clone)]
pub struct LoadSpec {
    pub rpc_url: String,
    /// The other ingress RPC URLs; the drain asks them for a receipt the
    /// submit ingress no longer holds.
    pub receipt_rpcs: Vec<String>,
    pub chain_id: u64,
    pub account: u32,
    /// The first nonce the load signs. A case load starts a fresh
    /// account at 0; the recovery probe continues the case's account.
    pub nonce_start: u64,
    /// What must get a receipt: every accepted submit for a case load,
    /// every offered one for the recovery probe.
    pub completeness: Completeness,
    /// A probe submits at the target rate from its first second, with
    /// no ramp.
    pub fixed_rate: bool,
    pub duration: Duration,
    pub tps: NonZeroU32,
    /// The most blocking submits in flight at once. Each one holds its
    /// slot until its receipt, so the rate the load reaches is at most
    /// this count over the receipt latency.
    pub max_in_flight: NonZeroU32,
    pub retry_submit: u32,
    /// How long one submit waits out the refusals of a paused ingress,
    /// from the first one. Zero turns the wait off.
    pub pause_wait: Duration,
    pub max_gap: u64,
    pub drain_timeout: Duration,
    pub report_path: PathBuf,
    /// The exporters the harness scrapes, over the bridge.
    pub metrics: MetricsTargets,
}

impl LoadSpec {
    fn config(&self) -> anyhow::Result<LoadConfig> {
        let sender_range =
            SenderRange::new(self.account, NonZeroU32::new(1).context("one sender")?)?;
        let ramp_step_tps =
            NonZeroU32::new(self.tps.get().checked_div(8).unwrap_or(1)).context("ramp step")?;
        Ok(LoadConfig {
            workload: Workload::Transfers,
            calldata: CalldataMix::default(),
            rpc: self.rpc_url.clone(),
            receipt_rpcs: self.receipt_rpcs.clone(),
            chain_id: Some(self.chain_id),
            duration: self.duration,
            target_tps: self.tps,
            sender_range,
            nonce_start: NonceStart::Fixed(self.nonce_start),
            mnemonic: ANVIL_MNEMONIC.to_string(),
            to: SINK,
            value: U256::from(1),
            gas_price: 1_000_000_000,
            max_in_flight: self.max_in_flight,
            max_gap: self.max_gap,
            drain_timeout: self.drain_timeout,
            retry_submit: self.retry_submit,
            pause_wait: self.pause_wait,
            ramp_step_tps,
            ramp_step_secs: NonZeroU64::new(15).context("ramp step secs")?,
            soak_fraction: 0.8,
            completeness: self.completeness,
            assert_all_delivered: true,
            chaos_mode: true,
            fixed_rate: self.fixed_rate,
            scrape: vec!["executor".into(), "ingress".into(), "sequencer".into()],
            subscribe: false,
            feed_confirm: false,
            metrics: self.metrics.clone(),
            output: Some(self.report_path.clone()),
        })
    }
}

/// A running load, until its window and drain end.
pub struct LoadRun {
    task: tokio::task::JoinHandle<anyhow::Result<bool>>,
    report_path: PathBuf,
    /// The signed run's own counters and submission window.
    ready: tokio::sync::watch::Receiver<Option<Progress>>,
}

impl LoadRun {
    /// Start the load of `spec` on the current runtime.
    ///
    /// # Errors
    ///
    /// Returns an error if the configuration is invalid.
    pub fn start(spec: &LoadSpec) -> anyhow::Result<Self> {
        Ok(Self::from_config(spec.config()?, spec.report_path.clone()))
    }

    /// Start a run from a full configuration, for the stages whose shape
    /// [`LoadSpec`] does not express: a fixed rate, many senders, defi.
    /// `report_path` is where `cfg.output` writes the report.
    #[must_use]
    pub fn from_config(cfg: LoadConfig, report_path: PathBuf) -> Self {
        let (signal, ready) = tokio::sync::watch::channel(None);
        // The first signed batch precedes submission. The injection gate
        // waits for the prepared run and its own accepted traffic before the fault.
        let task = tokio::spawn(async move {
            let prepared = load::prepare(cfg).await?;
            signal.send_replace(Some(prepared.progress()));
            prepared.run().await
        });
        Self {
            task,
            report_path,
            ready,
        }
    }

    /// Whether the load signed its first batch and published its observer.
    #[must_use]
    pub fn is_ready(&self) -> bool {
        self.ready.borrow().is_some()
    }

    pub(crate) fn is_flowing(&self) -> bool {
        self.ready
            .borrow()
            .as_ref()
            .is_some_and(Progress::is_flowing)
    }

    pub(crate) fn assert_submitting(&self, case: &str) -> anyhow::Result<()> {
        let submitting = self
            .ready
            .borrow()
            .as_ref()
            .is_some_and(Progress::is_submitting);
        anyhow::ensure!(
            submitting && !self.is_finished(),
            "{}: {case}: the case load stops submitting before fault recovery completes; the load window does not cover the case",
            crate::FAIL_PREFIX
        );
        Ok(())
    }

    pub(crate) fn finish_submissions(&self) {
        if let Some(progress) = self.progress() {
            progress.finish_submissions();
        }
    }

    /// The observer of the signed run, once the load is ready. A case
    /// body ends the submissions through it.
    pub(crate) fn progress(&self) -> Option<Progress> {
        self.ready.borrow().clone()
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
    pub async fn finish(mut self) -> anyhow::Result<Verdict> {
        let outcome = (&mut self.task)
            .await
            .context("join the load task")
            .and_then(std::convert::identity)
            .inspect_err(|error| self.save_incomplete(Some(error)))?;
        let report_text = std::fs::read_to_string(&self.report_path).with_context(|| {
            format!(
                "load report {} missing (harness: {outcome:?})",
                self.report_path.display()
            )
        })?;
        let report: Report = serde_json::from_str(&report_text)
            .with_context(|| format!("decode {}", self.report_path.display()))?;
        anyhow::ensure!(
            outcome == report.verdict.pass,
            "load return and report verdict disagree"
        );
        Ok(Verdict {
            latency: report.latency,
            ..report.verdict
        })
    }

    fn save_incomplete(&self, failure: Option<&anyhow::Error>) {
        let progress = self.ready.borrow().as_ref().map(Progress::snapshot);
        let report = serde_json::json!({
            "aborted_before_final_verdict": true,
            "failure": failure.map(|error| format!("{error:#}")),
            "report": self.report_path,
            "progress": progress,
        });
        let path = self.report_path.with_extension("aborted.json");
        let saved = serde_json::to_vec_pretty(&report)
            .map_err(anyhow::Error::from)
            .and_then(|bytes| std::fs::write(&path, bytes).map_err(anyhow::Error::from));
        if let Err(error) = saved {
            crate::log(format!(
                "failed to save aborted load evidence at {}: {error:#}",
                path.display()
            ));
        }
    }
}

/// A run that ends before its verdict, on a failed case or a case past
/// its deadline, stops its load and keeps the evidence of how far it got.
impl Drop for LoadRun {
    fn drop(&mut self) {
        if !self.task.is_finished() {
            self.save_incomplete(None);
            self.task.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_report_verdict_decodes() {
        let text = r#"{"mode":"chaos","lat_p50_us":19841000,"lat_p95_us":29442000,
            "lat_p99_us":29540000,"lat_max_us":29655000,
            "verdict":{"pass":false,"failures":["x"],"offered":10,
            "accepted":10,"receipted":9,"missing":1,"unlanded":0,"bad_status":0,
            "inferred_ingress_drop":null,"seq_dropped":2,"keep_pace":[]}}"#;
        let report: Report = serde_json::from_str(text).unwrap();
        assert!(!report.verdict.pass);
        assert_eq!(report.verdict.missing, 1);
        assert_eq!(report.verdict.seq_dropped, Some(2));
        assert_eq!(
            report.latency.to_string(),
            "receipt latency p50=19841ms p95=29442ms p99=29540ms max=29655ms"
        );
    }

    /// A run whose load task is `task`, for the finish checks.
    fn stub_run(
        report_path: PathBuf,
        task: impl std::future::Future<Output = anyhow::Result<bool>> + Send + 'static,
    ) -> LoadRun {
        let (_, ready) = tokio::sync::watch::channel(None);
        LoadRun {
            task: tokio::spawn(task),
            report_path,
            ready,
        }
    }

    #[tokio::test]
    async fn a_failed_load_cannot_reuse_a_stale_passing_report() {
        let directory = tempfile::tempdir().unwrap();
        let report_path = directory.path().join("report.json");
        std::fs::write(&report_path, r#"{"verdict":{"pass":true,"missing":0}}"#).unwrap();
        let aborted_path = report_path.with_extension("aborted.json");
        let run = stub_run(report_path, async {
            anyhow::bail!("load failed before report")
        });
        assert!(
            run.finish()
                .await
                .unwrap_err()
                .to_string()
                .contains("load failed before report")
        );
        let aborted: serde_json::Value =
            serde_json::from_slice(&std::fs::read(aborted_path).unwrap()).unwrap();
        assert_eq!(aborted["aborted_before_final_verdict"], true);
        assert_eq!(aborted["failure"], "load failed before report");
    }

    #[tokio::test]
    async fn the_report_must_agree_with_the_load_return() {
        let directory = tempfile::tempdir().unwrap();
        let report_path = directory.path().join("report.json");
        std::fs::write(&report_path, r#"{"verdict":{"pass":true,"missing":0}}"#).unwrap();
        let run = stub_run(report_path, async { Ok(false) });
        assert!(run.finish().await.is_err());
    }
}
