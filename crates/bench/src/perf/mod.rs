//! `kardamom-perf` — cluster performance pipeline: fresh stack, ramp to the
//! edge, profile the sealer leader under steady load, produce a report.
//!
//! The pipeline drives the same deploy/cluster DinD stack the cluster-e2e CI
//! uses (via `ci-cluster.sh` from the orchestrator container) and reuses the
//! `kardamom-load` harness as a library for the load phases. Profiling
//! attaches async-profiler (itimer mode — no perf_events needed inside the
//! nested containers) to the JVM of whichever sealer node is the current
//! Raft leader, detected as the busiest sealer container under load.

pub mod cluster;
pub mod profile;
pub mod report;

use std::path::PathBuf;

/// Where a pipeline invocation writes everything it produces.
///
/// Layout: `<root>/load-report.json`, `flame.html`, `flame.svg`,
/// `stacks.collapsed`, `cpu-snapshot.txt`, `summary.md`.
#[derive(Debug, Clone)]
pub struct OutDir(pub PathBuf);

impl OutDir {
    /// Create `<base>/<utc-timestamp>/` and return it.
    pub fn create(base: &std::path::Path) -> anyhow::Result<Self> {
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs();
        let dir = base.join(format!("perf-{stamp}"));
        std::fs::create_dir_all(&dir)?;
        Ok(Self(dir))
    }

    pub fn path(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}
