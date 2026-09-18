//! The Java Aeron Cluster (Raft) sealer, as local JVM child processes.
//!
//! `ClusterNode` is all-in-one: an embedded media driver, archive,
//! consensus module, and `SealerClusteredService`, configured only with
//! system properties. This is the same invocation as
//! `deploy/cluster/nomad/cluster.nomad.hcl`, pointed at loopback
//! endpoints. The semantics suite runs a single member (a Raft quorum of
//! 1): canonical ordering with no fault-tolerance machinery, which
//! belongs to the chaos suite.

use std::num::{NonZeroU64, NonZeroUsize};
use std::path::Path;
use std::process::Command;
use std::time::Duration;

use anyhow::{Context, Result};
use kardamom_obs::testkit::free_udp_port;

use super::proc::{ExistingFile, Proc, resolve_artifact, wait_for_log_line};

/// Find `kardamom-cluster-node.jar`. Use `KARDAMOM_CLUSTER_JAR` if set,
/// otherwise use the gradle shadowJar output in the repo.
///
/// # Errors
/// Returns an error when neither location holds a file.
fn cluster_jar(repo_root: &Path) -> Result<ExistingFile> {
    resolve_artifact(
        "KARDAMOM_CLUSTER_JAR",
        repo_root.join("cluster/sealer-service/service/build/libs/kardamom-cluster-node.jar"),
        "run `just cluster-jar` (gradle :service:shadowJar) or set KARDAMOM_CLUSTER_JAR",
    )
}

pub struct SealerCluster {
    pub procs: Vec<Proc>,
    /// The Rust services' `[cluster] ingress_endpoints` value.
    pub ingress_endpoints: String,
}

impl SealerCluster {
    /// Launch `members` `ClusterNode` JVMs on loopback (1 for semantics
    /// runs, 3 for a real quorum). This blocks until every member logs
    /// "cluster node up" and member 0 reports a role (LEADER, for a
    /// single member).
    ///
    /// `remote_origins` is the sealer's remote-origin allowlist. Empty
    /// disables interop on the cluster.
    ///
    /// # Errors
    /// Returns an error when the cluster jar is missing, when a member
    /// fails to spawn or does not log "cluster node up" within 60s, or
    /// when no member reports LEADER within 60s.
    pub fn launch(
        root: &Path,
        repo_root: &Path,
        members: NonZeroUsize,
        tick_ms: NonZeroU64,
        remote_origins: &[u64],
    ) -> Result<Self> {
        let mut launch = SealerLaunch::new(root, repo_root, members, tick_ms, remote_origins)?;
        launch.spawn_all()?;
        launch.await_ready()?;
        Ok(launch.finish())
    }
}

/// Bring-up state for a sealer cluster: the reserved endpoints, the JVM
/// command-line fragments built from them, and the members spawned so
/// far. Each bring-up step reads this as state instead of taking it as
/// loose parameters.
struct SealerLaunch<'a> {
    root: &'a Path,
    jar: ExistingFile,
    members: NonZeroUsize,
    tick_ms: NonZeroU64,
    /// The sealer's remote-origin allowlist. Empty disables interop.
    remote_origins: &'a [u64],
    members_str: String,
    ingress_endpoints: String,
    procs: Vec<Proc>,
}

impl<'a> SealerLaunch<'a> {
    fn new(
        root: &'a Path,
        repo_root: &Path,
        members: NonZeroUsize,
        tick_ms: NonZeroU64,
        remote_origins: &'a [u64],
    ) -> Result<Self> {
        let jar = cluster_jar(repo_root)?;
        let endpoint_sets = Self::reserve_endpoint_sets(members);
        let (members_str, ingress_endpoints) = Self::format_member_strings(&endpoint_sets);
        Ok(Self {
            root,
            jar,
            members,
            tick_ms,
            remote_origins,
            members_str,
            ingress_endpoints,
            procs: Vec::new(),
        })
    }

    /// Five UDP endpoints per member: ingress, consensus, log, catchup,
    /// archive.
    fn reserve_endpoint_sets(members: NonZeroUsize) -> Vec<[u16; 5]> {
        (0..members.get())
            .map(|_| std::array::from_fn(|_| free_udp_port().port()))
            .collect()
    }

    /// The `-Dkardamom.cluster.members` value every member's JVM shares,
    /// and the Rust services' `[cluster] ingress_endpoints` value.
    fn format_member_strings(endpoint_sets: &[[u16; 5]]) -> (String, String) {
        let members_str = endpoint_sets
            .iter()
            .enumerate()
            .map(|(id, p)| {
                format!(
                    "{id},127.0.0.1:{},127.0.0.1:{},127.0.0.1:{},127.0.0.1:{},127.0.0.1:{}",
                    p[0], p[1], p[2], p[3], p[4]
                )
            })
            .collect::<Vec<_>>()
            .join("|");
        let ingress_endpoints = endpoint_sets
            .iter()
            .enumerate()
            .map(|(id, p)| format!("{id}=127.0.0.1:{}", p[0]))
            .collect::<Vec<_>>()
            .join(",");
        (members_str, ingress_endpoints)
    }

    /// Spawn one `ClusterNode` JVM, member `id` of this cluster.
    fn spawn_member(&self, id: usize) -> Result<Proc> {
        let node_root = self.root.join(format!("sealer-{id}"));
        std::fs::create_dir_all(&node_root)?;
        let mut cmd = Command::new("java");
        cmd.args([
            "-Xmx384m",
            "--add-opens",
            "java.base/sun.nio.ch=ALL-UNNAMED",
            "--add-opens",
            "java.base/java.util.zip=ALL-UNNAMED",
            "--add-opens",
            "java.base/jdk.internal.misc=ALL-UNNAMED",
        ])
        .arg(format!("-Dkardamom.cluster.memberId={id}"))
        .arg(format!("-Dkardamom.cluster.members={}", self.members_str))
        .arg(format!("-Daeron.dir={}", node_root.join("aeron").display()))
        .arg(format!(
            "-Dkardamom.cluster.dir={}",
            node_root.join("cluster").display()
        ))
        .arg(format!(
            "-Dkardamom.archive.dir={}",
            node_root.join("archive").display()
        ))
        .arg("-Dkardamom.cluster.ingressStreamId=101")
        .arg(format!("-Dkardamom.cluster.tickMs={}", self.tick_ms))
        .arg(format!(
            "-Dkardamom.cluster.remoteOrigins={}",
            self.remote_origins
                .iter()
                .map(u64::to_string)
                .collect::<Vec<_>>()
                .join(",")
        ))
        // The cluster node is also an Aeron client, so it hits the same
        // 10 s limit when the driver conductor starves. See
        // `services::driver_timeout_ms` for the same value, spelled
        // Java's way.
        .arg(format!(
            "-Daeron.driver.timeout={}",
            super::services::driver_timeout_ms()
        ))
        .args([
            "-cp",
            &self.jar.to_string(),
            "io.kardamom.sealer.cluster.ClusterNode",
        ]);
        Proc::spawn(
            &format!("sealer-{id}"),
            cmd,
            self.root.join(format!("sealer-{id}.log")),
        )
    }

    /// Spawn every member.
    fn spawn_all(&mut self) -> Result<()> {
        self.procs = (0..self.members.get())
            .map(|id| self.spawn_member(id))
            .collect::<Result<_>>()?;
        Ok(())
    }

    /// Wait for every spawned member to log "cluster node up", then for
    /// one of them to report LEADER.
    fn await_ready(&mut self) -> Result<()> {
        for proc in &mut self.procs {
            wait_for_log_line(proc, "cluster node up", Duration::from_secs(60))
                .context("sealer member startup")?;
        }
        self.await_leader()
    }

    /// Poll member logs for a LEADER line. A 1-member cluster elects
    /// itself; with more members, any one of them can gain the role.
    fn await_leader(&self) -> Result<()> {
        super::metrics::poll_sync(
            "a sealer member to become LEADER",
            Duration::from_secs(60),
            Duration::from_millis(100),
            || Ok(self.any_member_is_leader().then_some(())),
        )
        .map_err(|_| {
            let tails: Vec<String> = self
                .procs
                .iter()
                .map(|p| format!("--- {} ---\n{}", p.name, p.log_tail(30)))
                .collect();
            anyhow::anyhow!(
                "no sealer member became LEADER in 60s:\n{}",
                tails.join("\n")
            )
        })
    }

    /// Whether any member's log has printed a LEADER role line.
    fn any_member_is_leader(&self) -> bool {
        self.procs
            .iter()
            .any(|p| std::fs::read_to_string(&p.log_path).is_ok_and(|s| s.contains("role=LEADER")))
    }

    fn finish(self) -> SealerCluster {
        SealerCluster {
            procs: self.procs,
            ingress_endpoints: self.ingress_endpoints,
        }
    }
}
