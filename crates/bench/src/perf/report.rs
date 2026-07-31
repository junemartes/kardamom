//! Perf-report assembly: fold the load report and the collapsed-stack
//! profile into a human-readable `summary.md`.

use std::collections::HashMap;
use std::path::Path;

use anyhow::Context;

use crate::load::LoadReport;
use crate::perf::OutDir;

/// One aggregated row of the profile: a frame's share of samples.
#[derive(Debug, Clone)]
pub struct FrameShare {
    pub frame: String,
    pub pct: f64,
}

/// Aggregate collapsed stacks into leaf shares and inclusive shares for a
/// fixed set of infrastructure buckets, so the summary can answer "where does
/// the CPU go" without the reader opening the flamegraph.
pub struct ProfileSummary {
    pub total_samples: u64,
    pub top_leaves: Vec<FrameShare>,
    pub buckets: Vec<FrameShare>,
}

/// Buckets that partition the sealer's CPU story. Matched by substring
/// against every frame in a stack (inclusive attribution, first match wins,
/// ordered from most to least specific).
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

pub fn analyze_collapsed(collapsed: &str) -> ProfileSummary {
    let mut total = 0u64;
    let mut leaves: HashMap<&str, u64> = HashMap::new();
    let mut buckets: HashMap<&str, u64> = HashMap::new();

    for line in collapsed.lines() {
        let Some((stack, n)) = line.rsplit_once(' ') else {
            continue;
        };
        let Ok(n) = n.parse::<u64>() else { continue };
        total += n;
        if let Some(leaf) = stack.rsplit(';').next() {
            *leaves.entry(leaf).or_default() += n;
        }
        for (name, pat) in BUCKETS {
            if stack.contains(pat) {
                *buckets.entry(name).or_default() += n;
                break;
            }
        }
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

/// Write `summary.md`. The ramp/edge numbers come from the discovery run;
/// the delivery verdict, latency, and soak shape come from the profiled soak
/// (a chaos-mode fixed-rate run, which carries no ramp of its own).
pub fn write_summary(
    out: &OutDir,
    discovery: &LoadReport,
    soak: &LoadReport,
    leader: &str,
    cpu_snapshot: &[(String, f64)],
    profile: &ProfileSummary,
) -> anyhow::Result<std::path::PathBuf> {
    use std::fmt::Write as _;

    let mut md = String::new();
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

    writeln!(md, "## Ramp (discovery run)\n")?;
    writeln!(
        md,
        "| rate (tx/s) | accept | p50 ms | p95 ms | p99 ms | keep-pace | seq clean | verdict |"
    )?;
    writeln!(md, "|---:|---:|---:|---:|---:|:--|:--|:--|")?;
    for s in &discovery.ramp {
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
            if s.sustainable {
                "sustainable"
            } else {
                "UNSUSTAINABLE"
            }
        )?;
    }

    writeln!(md, "\n## CPU by node (mid-soak snapshot)\n")?;
    writeln!(md, "| container | cpu % |")?;
    writeln!(md, "|:--|---:|")?;
    for (name, pct) in cpu_snapshot {
        writeln!(md, "| {name} | {pct:.1} |")?;
    }

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

    let path = out.path("summary.md");
    std::fs::write(&path, md).context("write summary.md")?;
    Ok(path)
}

/// Read the load report the harness wrote to disk.
pub fn read_load_report(path: &Path) -> anyhow::Result<LoadReport> {
    let raw = std::fs::read_to_string(path).context("read load report json")?;
    serde_json::from_str(&raw).context("parse load report json")
}
