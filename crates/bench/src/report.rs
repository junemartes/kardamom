//! Bench result aggregation and reporting.

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::time::Duration;

use hdrhistogram::Histogram;
use serde::Serialize;

/// Inputs to [`build_report`]. Carries the configured knobs and the
/// workload name — everything the report needs that isn't measured from
/// the dispatch itself.
#[derive(Debug, Clone, Copy)]
pub struct ReportInputs<'a> {
    /// Human-readable label, e.g. `"transfers"`. Comes from
    /// [`crate::workflow::BenchWorkflow::name`].
    pub workload_name: &'a str,
    /// Pre-signed transactions queued per sender task.
    pub txs_per_task: u32,
    /// Configured cap on outstanding requests across all senders.
    pub max_in_flight: u32,
    /// Number of sender tasks.
    pub concurrency: u32,
    /// The `--duration` knob the run was configured with (not the measured
    /// wall clock — that's `measurement_duration` on the outputs).
    pub configured_duration: Duration,
}

/// Per-RPC-method summary in a [`BenchReport`].
#[derive(Debug, Serialize)]
pub struct MethodReport {
    /// RPC method name, e.g. `"eth_sendRawTransaction"`.
    pub method: String,
    /// Number of completed requests recorded for this method.
    pub samples: u64,
    /// p50 latency in microseconds.
    pub p50_us: u64,
    /// p90 latency in microseconds.
    pub p90_us: u64,
    /// p99 latency in microseconds.
    pub p99_us: u64,
    /// p99.9 latency in microseconds.
    pub p999_us: u64,
    /// Maximum observed latency in microseconds.
    pub max_us: u64,
}

/// Top-level bench result, serialized to JSON and printed to terminal.
#[derive(Debug, Serialize)]
pub struct BenchReport {
    /// Workload label from [`ReportInputs::workload_name`].
    pub workload: String,
    /// `txs_per_task` knob the run was configured with.
    pub txs_per_task: u32,
    /// `max_in_flight` knob the run was configured with.
    pub max_in_flight: u32,
    /// Configured `--duration` in seconds (the safety timeout, not the
    /// measured wall clock).
    pub duration_secs: f64,
    /// Concurrency knob the run was configured with.
    pub concurrency: u32,
    /// Measured throughput: completed requests / measurement wall clock.
    /// Differs from the configured `max_in_flight`/`txs_per_task` because
    /// the bench is closed-loop and saturates the node.
    pub throughput_rps: f64,
    /// Wall-clock duration of the measurement window, in seconds.
    pub measurement_secs: f64,
    /// Total requests dispatched (= `ok + err`).
    pub sent: u64,
    /// Requests that returned `Ok` from the RPC client.
    pub ok: u64,
    /// Requests that returned `Err` from the RPC client.
    pub err: u64,
    /// One entry per RPC method observed.
    pub methods: Vec<MethodReport>,
}

/// Dispatch-side counters accumulated by the per-task `TaskAccum` and
/// surfaced through `Benchmark::dispatch` `Outputs`.
pub struct Counters {
    /// Total requests dispatched. Equals `ok + err` in the closed-loop
    /// dispatcher.
    pub sent: u64,
    /// Requests that returned `Ok` from the RPC client.
    pub ok: u64,
    /// Requests that returned `Err` from the RPC client.
    pub err: u64,
}

/// Assemble a `BenchReport` from the dispatch-side counters and the
/// merged-across-tasks histogram map.
///
/// `inputs` carries the knobs that configured the run;
/// `measurement_duration` is the wall-clock observed inside
/// `Benchmark::dispatch`.
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
    // Display-only ratio; precision loss above 2^53 completions is
    // irrelevant for a wall-clock-bounded bench.
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
        duration_secs: inputs.configured_duration.as_secs_f64(),
        concurrency: inputs.concurrency,
        throughput_rps,
        measurement_secs,
        sent: counters.sent,
        ok: counters.ok,
        err: counters.err,
        methods,
    }
}

/// Print a one-line summary + one line per RPC method to stdout. Used by
/// both binaries; downstream callers reading the JSON serialization
/// should pull from [`BenchReport`] directly.
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

/// Serialize `report` as pretty JSON to `path`. Creates parent dirs.
///
/// # Errors
///
/// Errors if the parent directory can't be created, the JSON
/// serialization fails, or the write fails.
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

// Display-only formatting; `u64 → f64` precision loss above 2^53 µs
// (~285 years) is irrelevant for latency samples.
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
