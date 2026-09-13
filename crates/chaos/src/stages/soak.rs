//! The sustained-load stage: two fixed-rate runs of the load harness in
//! process, the transfers workload and then the defi workload, each
//! with the all-delivered invariant. A host sampler prints the load
//! average every 30 s while the runs last; the CI logs carry it as the
//! runner-contention evidence.

use std::num::{NonZeroU32, NonZeroU64};
use std::path::PathBuf;
use std::time::Duration;

use alloy_primitives::U256;
use kardamom_bench::ANVIL_MNEMONIC;
use kardamom_bench::load::{Completeness, LoadConfig, SenderRange, Workload};

use crate::harness::Harness;
use crate::knobs::SoakRun;
use crate::load::{LoadRun, SINK};

/// The first funded account of the transfers run; #0 is the smoke gate.
const FIRST_SENDER: u32 = 1;
const MAX_IN_FLIGHT: NonZeroU32 = NonZeroU32::new(256).unwrap();
const RAMP_STEP_SECS: NonZeroU64 = NonZeroU64::new(15).unwrap();
const DRAIN_TIMEOUT: Duration = Duration::from_secs(90);
const RETRY_SUBMIT: u32 = 2;
const GAS_PRICE: u128 = 1_000_000_000;
const SOAK_FRACTION: f64 = 0.8;
const SAMPLE_EVERY: Duration = Duration::from_secs(30);

/// One run of the stage: the workload, its values, the first sender and
/// the report file.
struct Run<'a> {
    workload: Workload,
    values: &'a SoakRun,
    first_sender: u32,
    report: PathBuf,
}

impl Run<'_> {
    fn name(&self) -> &'static str {
        match self.workload {
            Workload::Transfers => "transfers",
            Workload::Defi => "defi",
        }
    }
}

impl Harness {
    /// Run the sustained-load stage: transfers, then defi. Each run must
    /// pass the harness verdict (every accepted transaction receipted,
    /// keep-pace gap within bound).
    ///
    /// # Errors
    ///
    /// Returns an error if a run fails to start or its verdict fails.
    pub async fn soak(&self) -> anyhow::Result<()> {
        let stages = &self.knobs.stages;
        let _sampler = HostSampler::start();
        crate::log(format!(
            "load runner: name={} host={} cpus={} {}",
            runner_name(),
            hostname(),
            std::thread::available_parallelism().map_or(0, std::num::NonZeroUsize::get),
            host_sample()
        ));
        let transfers = Run {
            workload: Workload::Transfers,
            values: &stages.transfers,
            first_sender: FIRST_SENDER,
            report: std::env::temp_dir().join("kardamom-load.json"),
        };
        self.soak_run(&transfers).await?;
        let defi = Run {
            workload: Workload::Defi,
            values: &stages.defi,
            first_sender: FIRST_SENDER.saturating_add(stages.transfers.senders.get()),
            report: std::env::temp_dir().join("kardamom-load-defi.json"),
        };
        self.soak_run(&defi).await
    }

    async fn soak_run(&self, run: &Run<'_>) -> anyhow::Result<()> {
        crate::log(format!(
            "load test: {} fixed-rate invariant gate (duration={}s rate={}tps senders={})",
            run.name(),
            run.values.duration.as_secs(),
            run.values.tps,
            run.values.senders
        ));
        let verdict = LoadRun::from_config(self.soak_config(run)?, run.report.clone())
            .finish()
            .await?;
        anyhow::ensure!(
            verdict.pass,
            "load verdict FAIL for {}: {}",
            run.name(),
            verdict.failures.join("; ")
        );
        crate::log(format!("load verdict PASS for {}", run.name()));
        Ok(())
    }

    fn soak_config(&self, run: &Run<'_>) -> anyhow::Result<LoadConfig> {
        let ramp_step_tps = NonZeroU32::new(run.values.tps.get().checked_div(8).unwrap_or(1))
            .unwrap_or(NonZeroU32::MIN);
        Ok(LoadConfig {
            workload: run.workload,
            rpc: self.rpc_url.clone(),
            chain_id: Some(self.knobs.chain_id),
            duration: run.values.duration,
            target_tps: run.values.tps,
            sender_range: SenderRange::new(run.first_sender, run.values.senders)?,
            nonce_start: 0,
            mnemonic: ANVIL_MNEMONIC.to_string(),
            to: SINK,
            value: U256::from(1),
            gas_price: GAS_PRICE,
            max_in_flight: MAX_IN_FLIGHT,
            max_gap: self.knobs.load_max_gap,
            drain_timeout: DRAIN_TIMEOUT,
            retry_submit: RETRY_SUBMIT,
            ramp_step_tps,
            ramp_step_secs: RAMP_STEP_SECS,
            soak_fraction: SOAK_FRACTION,
            completeness: Completeness::Accepted,
            assert_all_delivered: true,
            chaos_mode: false,
            fixed_rate: true,
            scrape: vec!["executor".into(), "ingress".into(), "sequencer".into()],
            metrics_via_docker: true,
            subscribe: false,
            feed_confirm: false,
            executor_nodes: self
                .probes
                .executors
                .iter()
                .map(|n| n.container.clone())
                .collect(),
            ingress_node: self.probes.ingresses[0].container.clone(),
            sequencer_nodes: self
                .probes
                .sequencers
                .iter()
                .map(|n| n.container.clone())
                .collect(),
            output: Some(run.report.clone()),
        })
    }
}

/// Prints one host sample every 30 s until dropped.
struct HostSampler {
    task: tokio::task::JoinHandle<()>,
}

impl HostSampler {
    fn start() -> Self {
        Self {
            task: tokio::spawn(Self::sample_loop()),
        }
    }

    async fn sample_loop() {
        loop {
            tokio::time::sleep(SAMPLE_EVERY).await;
            crate::log(format!("load runner sample: {}", host_sample()));
        }
    }
}

impl Drop for HostSampler {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn host_sample() -> String {
    format!(
        "loadavg=[{}] mem_avail_kb={}",
        loadavg(),
        mem_available_kb()
    )
}

fn loadavg() -> String {
    std::fs::read_to_string("/proc/loadavg").map_or_else(
        |_| "?".to_string(),
        |s| s.split_whitespace().take(3).collect::<Vec<_>>().join(" "),
    )
}

fn mem_available_kb() -> String {
    std::fs::read_to_string("/proc/meminfo")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("MemAvailable:"))
                .and_then(|l| l.split_whitespace().nth(1))
                .map(str::to_string)
        })
        .unwrap_or_else(|| "?".to_string())
}

fn runner_name() -> String {
    std::env::var("RUNNER_NAME").unwrap_or_else(|_| "local".to_string())
}

fn hostname() -> String {
    std::fs::read_to_string("/proc/sys/kernel/hostname")
        .map_or_else(|_| "?".to_string(), |s| s.trim().to_string())
}
