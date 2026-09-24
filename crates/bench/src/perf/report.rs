//! This module assembles the perf report. It folds the load report and
//! the collapsed-stack profile into a human-readable `summary.md`.

use std::collections::HashMap;
use std::path::Path;

use anyhow::Context;

use crate::load::{LoadReport, RampStep};
use crate::perf::OutDir;

/// One aggregated row of the profile: a frame's share of samples.
#[derive(Debug, Clone)]
pub struct FrameShare {
    frame: String,
    pct: f64,
}

/// Aggregate collapsed stacks into leaf shares and inclusive shares,
/// for a fixed set of infrastructure buckets. This lets the summary
/// answer "where does the CPU go" without the reader opening the
/// flame graph.
pub struct ProfileSummary {
    total_samples: u64,
    top_leaves: Vec<FrameShare>,
    buckets: Vec<FrameShare>,
}

/// The buckets that partition the sealer's CPU story. The code matches
/// these by substring against every frame in a stack. Attribution is
/// inclusive, the first match wins, and the buckets are ordered from
/// most to least specific.
const BUCKETS: &[(&str, &str)] = &[
    ("idle-strategy spin/yield", "BackoffIdleStrategy.idle"),
    (
        "UDP receive (media driver poll)",
        "DataTransportPoller.poll",
    ),
    (
        "UDP send (replication + egress)",
        "DatagramChannelImpl.write",
    ),
    ("thread park/unpark", "LockSupport.park"),
    ("archive recording", "io/aeron/archive/"),
    ("sealer service logic", "io/kardamom/sealer/"),
];

/// The first `BUCKETS` name whose pattern `stack` contains, if any.
fn bucket_for(stack: &str) -> Option<&'static str> {
    BUCKETS
        .iter()
        .find(|(_, pat)| stack.contains(pat))
        .map(|(name, _)| *name)
}

/// Fold one collapsed-stack line (`"<stack> <count>"`) into `leaves`
/// and `buckets`, and return the running sample total: `total` plus
/// this line's count, or `total` unchanged if the line does not parse.
fn fold_collapsed_line<'a>(
    line: &'a str,
    total: u64,
    leaves: &mut HashMap<&'a str, u64>,
    buckets: &mut HashMap<&'static str, u64>,
) -> u64 {
    let Some((stack, n)) = line.rsplit_once(' ') else {
        return total;
    };
    let Ok(n) = n.parse::<u64>() else {
        return total;
    };
    if let Some(leaf) = stack.rsplit(';').next() {
        let entry = leaves.entry(leaf).or_default();
        *entry = entry.saturating_add(n);
    }
    if let Some(name) = bucket_for(stack) {
        let entry = buckets.entry(name).or_default();
        *entry = entry.saturating_add(n);
    }
    total.saturating_add(n)
}

#[allow(
    clippy::cast_precision_loss,
    reason = "sample counts stay far under 2^52 for any collapsed-stack file this harness produces"
)]
#[must_use]
pub fn analyze_collapsed(collapsed: &str) -> ProfileSummary {
    let mut total = 0u64;
    let mut leaves: HashMap<&str, u64> = HashMap::new();
    let mut buckets: HashMap<&str, u64> = HashMap::new();

    for line in collapsed.lines() {
        total = fold_collapsed_line(line, total, &mut leaves, &mut buckets);
    }

    let pct = |n: u64| {
        if total == 0 {
            0.0
        } else {
            n as f64 / total as f64 * 100.0
        }
    };
    let mut top_leaves: Vec<FrameShare> = leaves
        .into_iter()
        .map(|(frame, n)| FrameShare {
            frame: frame.to_string(),
            pct: pct(n),
        })
        .collect();
    top_leaves.sort_by(|a, b| b.pct.total_cmp(&a.pct));
    top_leaves.truncate(15);

    let buckets = BUCKETS
        .iter()
        .map(|(name, _)| FrameShare {
            frame: (*name).to_string(),
            pct: pct(buckets.get(name).copied().unwrap_or(0)),
        })
        .collect();

    ProfileSummary {
        total_samples: total,
        top_leaves,
        buckets,
    }
}

/// Write `summary.md`. The ramp and edge numbers come from the
/// discovery run. The delivery verdict, latency, and soak shape come
/// from the profiled soak, a chaos-mode fixed-rate run that carries no
/// ramp of its own.
///
/// # Errors
///
/// Returns an error if `summary.md` cannot be written to `out_dir`.
pub fn write_summary(
    out: &OutDir,
    discovery: &LoadReport,
    soak: &LoadReport,
    leader: &str,
    cpu_snapshot: &[(String, f64)],
    profile: &ProfileSummary,
) -> anyhow::Result<std::path::PathBuf> {
    let mut md = String::new();
    write_header(&mut md, discovery, soak, leader)?;
    write_ramp_table(&mut md, discovery)?;
    write_cpu_table(&mut md, cpu_snapshot)?;
    write_profile_tables(&mut md, profile)?;

    let path = out.path("summary.md");
    std::fs::write(&path, md).context("write summary.md")?;
    Ok(path)
}

/// The headline: discovery and soak rates, the delivery verdict,
/// receipt latency, and the profiled node.
fn write_header(
    md: &mut String,
    discovery: &LoadReport,
    soak: &LoadReport,
    leader: &str,
) -> anyhow::Result<()> {
    use std::fmt::Write as _;

    writeln!(md, "# kardamom perf run\n")?;
    writeln!(
        md,
        "- discovered max: **{} tx/s** (ramp ceiling {} tx/s) · profiled soak {} tx/s for {:.0}s",
        discovery.discovered_max_tps, discovery.target_tps, soak.soak_rate_tps, soak.duration_secs
    )?;
    let v = &soak.verdict;
    writeln!(
        md,
        "- delivery (soak): offered {} · receipted {} · missing {} · unlanded {} → **{}**",
        v.offered,
        v.receipted,
        v.missing,
        v.unlanded,
        if v.pass { "PASS" } else { "FAIL" }
    )?;
    writeln!(
        md,
        "- receipt latency (soak): p50 {} ms · p95 {} ms · p99 {} ms · max {} ms",
        soak.lat_p50_us / 1000,
        soak.lat_p95_us / 1000,
        soak.lat_p99_us / 1000,
        soak.lat_max_us / 1000
    )?;
    writeln!(
        md,
        "- profiled node: **{leader}** (busiest sealer under load = Raft leader)\n"
    )?;
    Ok(())
}

/// The discovery-run ramp table: one row per step.
fn write_ramp_table(md: &mut String, discovery: &LoadReport) -> anyhow::Result<()> {
    use std::fmt::Write as _;

    writeln!(md, "## Ramp (discovery run)\n")?;
    writeln!(
        md,
        "| rate (tx/s) | accept | p50 ms | p95 ms | p99 ms | keep-pace | seq clean | verdict |"
    )?;
    writeln!(md, "|---:|---:|---:|---:|---:|:--|:--|:--|")?;
    discovery
        .ramp
        .iter()
        .try_for_each(|s| write_ramp_row(md, s))
}

/// Write one ramp step's table row.
fn write_ramp_row(md: &mut String, s: &RampStep) -> anyhow::Result<()> {
    use std::fmt::Write as _;
    writeln!(
        md,
        "| {} | {:.3} | {} | {} | {} | {} | {} | {} |",
        s.rate,
        s.accept_ratio,
        s.lat_p50_us / 1000,
        s.lat_p95_us / 1000,
        s.lat_p99_us / 1000,
        s.gap_ok,
        s.seq_clean,
        ramp_verdict(s.sustainable),
    )?;
    Ok(())
}

/// The ramp-step verdict string: `"sustainable"` or `"UNSUSTAINABLE"`.
const fn ramp_verdict(sustainable: bool) -> &'static str {
    if sustainable {
        "sustainable"
    } else {
        "UNSUSTAINABLE"
    }
}

/// The mid-soak per-node CPU snapshot table.
fn write_cpu_table(md: &mut String, cpu_snapshot: &[(String, f64)]) -> anyhow::Result<()> {
    use std::fmt::Write as _;

    writeln!(md, "\n## CPU by node (mid-soak snapshot)\n")?;
    writeln!(md, "| container | cpu % |")?;
    writeln!(md, "|:--|---:|")?;
    for (name, pct) in cpu_snapshot {
        writeln!(md, "| {name} | {pct:.1} |")?;
    }
    Ok(())
}

/// The sealer-leader profile: bucket attribution, hottest leaf frames,
/// and the artifacts note.
fn write_profile_tables(md: &mut String, profile: &ProfileSummary) -> anyhow::Result<()> {
    use std::fmt::Write as _;

    writeln!(
        md,
        "\n## Sealer-leader profile ({} samples)\n",
        profile.total_samples
    )?;
    writeln!(md, "Inclusive attribution by bucket:\n")?;
    writeln!(md, "| bucket | share |")?;
    writeln!(md, "|:--|---:|")?;
    for b in &profile.buckets {
        writeln!(md, "| {} | {:.1}% |", b.frame, b.pct)?;
    }
    writeln!(md, "\nHottest leaf frames:\n")?;
    writeln!(md, "| leaf | share |")?;
    writeln!(md, "|:--|---:|")?;
    for l in &profile.top_leaves {
        writeln!(md, "| `{}` | {:.1}% |", l.frame, l.pct)?;
    }
    writeln!(
        md,
        "\nArtifacts: `flame.html` (interactive, open in a browser), `flame.svg` \
         (static), `stacks.collapsed` (raw), `load-report.json`.\n"
    )?;
    Ok(())
}

/// Read the load report the harness wrote to disk.
///
/// # Errors
///
/// Returns an error if the file cannot be read, or its JSON does not
/// match [`LoadReport`].
pub fn read_load_report(path: &Path) -> anyhow::Result<LoadReport> {
    let raw = std::fs::read_to_string(path).context("read load report json")?;
    serde_json::from_str(&raw).context("parse load report json")
}
