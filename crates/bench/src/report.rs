//! Bench result aggregation and reporting.

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::time::Duration;

use hdrhistogram::Histogram;
use serde::Serialize;

use crate::config::{Config, Workload};

#[derive(Debug, Serialize)]
pub struct MethodReport {
    pub method: String,
    pub samples: u64,
    pub p50_us: u64,
    pub p90_us: u64,
    pub p99_us: u64,
    pub p999_us: u64,
    pub max_us: u64,
}

#[derive(Debug, Serialize)]
pub struct BenchReport {
    pub workload: String,
    pub rate_rps: u32,
    pub duration_secs: f64,
    pub concurrency: u32,
    pub sent: u64,
    pub ok: u64,
    pub err: u64,
    pub dropped: u64,
    pub methods: Vec<MethodReport>,
}

pub struct Counters {
    pub sent: u64,
    pub ok: u64,
    pub err: u64,
    pub dropped: u64,
}

pub fn build_report(
    config: &Config,
    counters: &Counters,
    histograms: BTreeMap<String, Histogram<u64>>,
) -> BenchReport {
    let workload = match config.workload {
        Workload::Transfers => "transfers",
        Workload::Calls => "calls",
        Workload::Mixed => "mixed",
    }
    .to_string();

    let methods = histograms
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

    BenchReport {
        workload,
        rate_rps: config.rate,
        duration_secs: config.duration.as_secs_f64(),
        concurrency: config.concurrency,
        sent: counters.sent,
        ok: counters.ok,
        err: counters.err,
        dropped: counters.dropped,
        methods,
    }
}

pub fn print_terminal(report: &BenchReport) {
    println!(
        "workload={}  rate={}rps  duration={}  concurrency={}  sent={}  ok={}  err={}  dropped={}",
        report.workload,
        report.rate_rps,
        humantime::format_duration(Duration::from_secs_f64(report.duration_secs)),
        report.concurrency,
        report.sent,
        report.ok,
        report.err,
        report.dropped,
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

fn fmt_us(us: u64) -> String {
    if us >= 1_000_000 {
        format!("{:.1}s", us as f64 / 1_000_000.0)
    } else if us >= 1_000 {
        format!("{:.1}ms", us as f64 / 1_000.0)
    } else {
        format!("{}µs", us)
    }
}

/// `humantime` is used purely for terminal formatting. Mini-impl so we don't
/// take the full crate dep just for `format_duration`.
mod humantime {
    use std::time::Duration;
    pub fn format_duration(d: Duration) -> String {
        let secs = d.as_secs();
        if secs >= 60 {
            format!("{}m{}s", secs / 60, secs % 60)
        } else if secs > 0 || d.subsec_millis() == 0 {
            format!("{}s", secs)
        } else {
            format!("{}ms", d.as_millis())
        }
    }
}
