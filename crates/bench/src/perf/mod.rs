//! `kardamom-perf` is a cluster performance pipeline. It builds a fresh
//! stack, ramps to the edge, profiles the sealer leader under steady
//! load, and produces a report.
//!
//! The pipeline drives the same deploy and cluster DinD stack the
//! cluster-e2e CI uses, through `ci-cluster.sh` from the orchestrator
//! container, and reuses the `kardamom-load` harness as a library for
//! the load phases. Profiling attaches async-profiler, in itimer mode
//! so no perf_events are needed inside the nested containers, to the
//! JVM of whichever sealer node is the current Raft leader. The
//! pipeline detects the leader as the busiest sealer container under load.

pub mod cluster;
pub mod profile;
pub mod report;

use std::path::PathBuf;

/// Where one pipeline run writes everything it produces.
///
/// The layout is `<root>/load-report.json`, `flame.html`, `flame.svg`,
/// `stacks.collapsed`, `cpu-snapshot.txt`, and `summary.md`.
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
