//! Final load reports retain the duration of the paced window.

use super::config::ClassReport;
use super::plan::TxClass;
use super::{LoadReport, LoadRun, RampStep, accounting};
use std::time::Duration;

pub(super) struct ReportInput {
    pub(super) verdict: accounting::Verdict,
    pub(super) ramp: Vec<RampStep>,
    pub(super) discovered_max: u32,
    pub(super) soak_rate: u32,
    pub(super) scrape_fallbacks: u64,
    pub(super) executor_queries_served_delta: Vec<(String, Option<u64>)>,
    pub(super) duration: Duration,
}

impl LoadRun<'_> {
    /// Assemble the final [`LoadReport`] from the run's timing, the
    /// tracker's latency percentiles and gas total, and the verdict.
    pub(super) fn build_report(&self, input: ReportInput) -> LoadReport {
        let ReportInput {
            verdict,
            ramp,
            discovered_max,
            soak_rate,
            scrape_fallbacks,
            executor_queries_served_delta,
            duration,
        } = input;
        let lat = self.tracker.latency_us();
        LoadReport {
            mode: if self.cfg.chaos_mode {
                "chaos"
            } else if self.cfg.fixed_rate {
                "fixed"
            } else {
                "soak"
            }
            .to_string(),
            target_tps: self.cfg.target_tps.get(),
            discovered_max_tps: discovered_max,
            soak_rate_tps: soak_rate,
            duration_secs: duration.as_secs_f64(),
            ramp,
            lat_p50_us: lat.p50,
            lat_p95_us: lat.p95,
            lat_p99_us: lat.p99,
            lat_max_us: lat.max,
            total_gas: self.tracker.total_gas(),
            workload: self.cfg.workload.to_string(),
            classes: self.tracker.class_report(),
            scrape_fallbacks,
            executor_queries_served_delta,
            verdict,
        }
    }
}

impl ClassReport {
    /// Print one line for `class`: the receipts, the failed receipts,
    /// and the receipt latency in microseconds.
    pub(super) fn print(&self, class: TxClass) {
        println!(
            "  {:<8} receipted={}  failed={}  p50={}us p95={}us p99={}us max={}us",
            class.as_str(),
            self.receipted,
            self.failed,
            self.lat_p50_us,
            self.lat_p95_us,
            self.lat_p99_us,
            self.lat_max_us
        );
    }
}
