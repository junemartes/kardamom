//! This module collects and reports bench results.

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::time::Duration;

use hdrhistogram::Histogram;
use serde::Serialize;

/// Inputs to [`build_report`]. This struct holds the configured settings
/// and the workload name: everything the report needs that dispatch
/// itself does not measure.
#[derive(Debug, Clone, Copy)]
pub struct ReportInputs<'a> {
    /// A human-readable label, for example `"transfers"`. This comes
    /// from [`crate::workflow::BenchWorkflow::name`].
    pub workload_name: &'a str,
    /// The number of pre-signed transactions in the queue of each
    /// sender task.
    pub txs_per_task: u32,
    /// The configured limit on outstanding requests across all senders.
    pub max_in_flight: u32,
    /// The number of sender tasks.
    pub concurrency: u32,
    /// The `--timeout` setting the run used. This is not the measured
    /// wall-clock time; that value is `measurement_duration` on the
    /// outputs.
    pub configured_timeout: Duration,
}

/// A per-RPC-method summary in a [`BenchReport`].
#[derive(Debug, Serialize)]
pub struct MethodReport {
    /// The RPC method name, for example `"eth_sendRawTransaction"`.
    pub method: String,
    /// The number of completed requests recorded for this method.
    pub samples: u64,
    /// The p50 latency, in microseconds.
    pub p50_us: u64,
    /// The p90 latency, in microseconds.
    pub p90_us: u64,
    /// The p99 latency, in microseconds.
    pub p99_us: u64,
    /// The p99.9 latency, in microseconds.
    pub p999_us: u64,
    /// The maximum observed latency, in microseconds.
    pub max_us: u64,
}

/// The top-level bench result. The code serializes this to JSON and
/// prints it to the terminal.
#[derive(Debug, Serialize)]
pub struct BenchReport {
    /// The workload label from [`ReportInputs::workload_name`].
    pub workload: String,
    /// The `txs_per_task` setting the run used.
    pub txs_per_task: u32,
    /// The `max_in_flight` setting the run used.
    pub max_in_flight: u32,
    /// The configured `--timeout`, in seconds. This is the safety
    /// timeout, not the measured wall-clock time.
    pub timeout_secs: f64,
    /// The concurrency setting the run used.
    pub concurrency: u32,
    /// The measured throughput: completed requests divided by the
    /// measurement wall-clock time. This can differ from the configured
    /// `max_in_flight` and `txs_per_task` values, because the bench is
    /// closed-loop and saturates the node.
    pub throughput_rps: f64,
    /// The wall-clock duration of the measurement window, in seconds.
    pub measurement_secs: f64,
    /// The total requests dispatched. This equals `ok + err`.
    pub sent: u64,
    /// The requests that returned `Ok` from the RPC client.
    pub ok: u64,
    /// The requests that returned `Err` from the RPC client.
    pub err: u64,
    /// One entry for each RPC method observed.
    pub methods: Vec<MethodReport>,
}

/// The dispatch-side counters. The per-task `TaskAccum` collects these,
/// and `Benchmark::dispatch` reports them through `Outputs`.
pub struct Counters {
    /// The total requests dispatched. This equals `ok + err` in the
    /// closed-loop dispatcher.
    pub sent: u64,
    /// The requests that returned `Ok` from the RPC client.
    pub ok: u64,
    /// The requests that returned `Err` from the RPC client.
    pub err: u64,
}

/// Build a `BenchReport` from the dispatch-side counters and the
/// histogram map merged across tasks.
///
/// `inputs` carries the settings the run used. `measurement_duration`
/// is the wall-clock time observed inside `Benchmark::dispatch`.
#[must_use]
pub fn build_report(
    inputs: ReportInputs<'_>,
    counters: &Counters,
    histograms: BTreeMap<String, Histogram<u64>>,
    measurement_duration: Duration,
) -> BenchReport {
    let methods: Vec<MethodReport> = histograms
        .into_iter()
        .filter(|(_, h)| !h.is_empty())
        .map(|(method, h)| MethodReport {
            samples: h.len(),
            p50_us: h.value_at_quantile(0.50),
            p90_us: h.value_at_quantile(0.90),
            p99_us: h.value_at_quantile(0.99),
            p999_us: h.value_at_quantile(0.999),
            max_us: h.max(),
            method,
        })
        .collect();

    let measured_completions: u64 = methods.iter().map(|m| m.samples).sum();
    let measurement_secs = measurement_duration.as_secs_f64();
    // This ratio is for display only. Precision loss above 2^53 completions
    // does not matter for a wall-clock-bounded bench.
    #[allow(clippy::cast_precision_loss)]
    let throughput_rps = if measurement_secs > 0.0 {
        measured_completions as f64 / measurement_secs
    } else {
        0.0
    };

    BenchReport {
        workload: inputs.workload_name.to_string(),
        txs_per_task: inputs.txs_per_task,
        max_in_flight: inputs.max_in_flight,
        timeout_secs: inputs.configured_timeout.as_secs_f64(),
        concurrency: inputs.concurrency,
        throughput_rps,
        measurement_secs,
        sent: counters.sent,
        ok: counters.ok,
        err: counters.err,
        methods,
    }
}

/// Print a one-line summary and one line per RPC method to stdout.
/// Both binaries use this function. A downstream caller that reads the
/// JSON serialization should read [`BenchReport`] directly instead.
pub fn print_terminal(report: &BenchReport) {
    println!(
        "workload={}  txs/task={}  max_in_flight={}  concurrency={}  \
         measured={:.2}s  throughput={:.0}rps  sent={}  ok={}  err={}",
        report.workload,
        report.txs_per_task,
        report.max_in_flight,
        report.concurrency,
        report.measurement_secs,
        report.throughput_rps,
        report.sent,
        report.ok,
        report.err,
    );
    for m in &report.methods {
        println!(
            "{:<24} p50={:<8} p90={:<8} p99={:<8} p99.9={:<8} max={:<8} n={}",
            m.method,
            fmt_us(m.p50_us),
            fmt_us(m.p90_us),
            fmt_us(m.p99_us),
            fmt_us(m.p999_us),
            fmt_us(m.max_us),
            m.samples,
        );
    }
}

/// Serialize `report` as pretty JSON to `path`. This creates parent
/// directories as needed.
///
/// # Errors
///
/// Returns an error if the code cannot create the parent directory,
/// if JSON serialization fails, or if the write fails.
pub fn write_json(path: &Path, report: &BenchReport) -> anyhow::Result<()> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)?;
    }
    let text = serde_json::to_string_pretty(report)?;
    fs::write(path, text)?;
    Ok(())
}

// This formatting is for display only. `u64` to `f64` precision loss
// above 2^53 microseconds, about 285 years, does not matter for
// latency samples.
#[allow(clippy::cast_precision_loss)]
fn fmt_us(us: u64) -> String {
    if us >= 1_000_000 {
        format!("{:.1}s", us as f64 / 1_000_000.0)
    } else if us >= 1_000 {
        format!("{:.1}ms", us as f64 / 1_000.0)
    } else {
        format!("{us}µs")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fmt_us_below_one_ms() {
        assert_eq!(fmt_us(0), "0µs");
        assert_eq!(fmt_us(999), "999µs");
    }

    #[test]
    fn fmt_us_milliseconds_range() {
        assert_eq!(fmt_us(1_000), "1.0ms");
        assert_eq!(fmt_us(12_500), "12.5ms");
        assert_eq!(fmt_us(999_999), "1000.0ms");
    }

    #[test]
    fn fmt_us_seconds_range() {
        assert_eq!(fmt_us(1_000_000), "1.0s");
        assert_eq!(fmt_us(2_500_000), "2.5s");
    }
}
