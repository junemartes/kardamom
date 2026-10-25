//! The Java Aeron Cluster (Raft) sealer, as local JVM child processes.
//!
//! `ClusterNode` is all-in-one: an embedded media driver, archive,
//! consensus module, and `SealerClusteredService`, configured only with
//! system properties. This is the same invocation as
//! `deploy/cluster/nomad/cluster.nomad.hcl`, pointed at loopback
//! endpoints. The semantics suite runs a single member (a Raft quorum of
//! 1): canonical ordering with no fault-tolerance machinery, which
//! belongs to the chaos suite.

use std::net::SocketAddr;
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use anyhow::{Context, Result};
use kardamom_obs::testkit::{free_port, free_udp_port};

use super::config::StackConfig;
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
    /// The admin endpoint of each member, in member order. Empty when the
    /// stack does not run the carry window.
    pub admin: Vec<SocketAddr>,
    /// The launch settings, kept so that a member can start again.
    launch: SealerLaunch,
}

/// The carry window settings of the members. See
/// [`crate::harness::CarryOptions`].
struct MemberCarry {
    retention: std::num::NonZeroU32,
    /// The `aeron.dir` of the shared media driver: member 0 feeds it.
    feed_dir: PathBuf,
    /// The admin port of each member, in member order.
    admin_ports: Vec<u16>,
    /// The carry mode from genesis ([`crate::harness::CarryOptions::genesis`]).
    genesis: bool,
}

impl SealerCluster {
    /// Launch `cfg.sealer_members` `ClusterNode` JVMs on loopback (1 for
    /// semantics runs, 3 for a real quorum). This blocks until every
    /// member logs "cluster node up" and member 0 reports a role (LEADER,
    /// for a single member). `feed_dir` is the `aeron.dir` of the shared
    /// media driver.
    ///
    /// # Errors
    /// Returns an error when the cluster jar is missing, when a member
    /// fails to spawn or does not log "cluster node up" within 60s, or
    /// when no member reports LEADER within 60s.
    pub fn launch(
        root: &Path,
        repo_root: &Path,
        cfg: &StackConfig,
        feed_dir: &Path,
    ) -> Result<Self> {
        let mut launch = SealerLaunch::new(root, repo_root, cfg, feed_dir)?;
        launch.spawn_all()?;
        launch.await_ready()?;
        Ok(launch.finish())
    }

    /// Kill member `id` and start it again on the same directories. The
    /// member resumes from its own snapshot and log. The call returns once
    /// the new process logs "cluster node up".
    ///
    /// # Errors
    /// Returns an error when the member fails to spawn or does not come up
    /// within 60s.
    pub fn restart_member(&mut self, id: usize) -> Result<()> {
        self.procs[id].kill();
        let mut proc = self.launch.spawn_member(id, "-restarted")?;
        wait_for_log_line(&mut proc, "cluster node up", Duration::from_mins(1))
            .context("restarted sealer member startup")?;
        self.procs[id] = proc;
        Ok(())
    }
}

/// Bring-up state for a sealer cluster: the reserved endpoints, the JVM
/// command-line fragments built from them, and the members spawned so
/// far. Each bring-up step reads this as state instead of taking it as
/// loose parameters. The cluster keeps it, so that a member can start
/// again with the same settings.
struct SealerLaunch {
    root: PathBuf,
    jar: ExistingFile,
    members: NonZeroUsize,
    tick_ms: NonZeroU64,
    /// The sealer's remote-origin allowlist. Empty disables interop.
    remote_origins: Vec<u64>,
    /// `-Dkardamom.cluster.orderingWindow`: 20 with priority fees on, 0
    /// for first come, first served.
    ordering_window: u32,
    carry: Option<MemberCarry>,
    members_str: String,
    ingress_endpoints: String,
    procs: Vec<Proc>,
}

impl SealerLaunch {
    fn new(root: &Path, repo_root: &Path, cfg: &StackConfig, feed_dir: &Path) -> Result<Self> {
        let jar = cluster_jar(repo_root)?;
        let endpoint_sets = Self::reserve_endpoint_sets(cfg.sealer_members);
        let (members_str, ingress_endpoints) = Self::format_member_strings(&endpoint_sets);
        let carry = cfg.carry.map(|c| MemberCarry {
            retention: c.retention,
            feed_dir: feed_dir.to_path_buf(),
            admin_ports: (0..cfg.sealer_members.get())
                .map(|_| free_port().port())
                .collect(),
            genesis: c.genesis,
        });
        Ok(Self {
            root: root.to_path_buf(),
            jar,
            members: cfg.sealer_members,
            tick_ms: cfg.cluster_tick_ms,
            remote_origins: cfg.remote_origins.clone(),
            ordering_window: if cfg.priority_fees { 20 } else { 0 },
            carry,
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
    /// and the Rust services' `[cluster] ingress_endpoints` value. Member
    /// `id` listens on the loopback address `127.0.0.<id + 1>`, so the
    /// admin endpoints of the peers of a member have distinct addresses
    /// (see [`MemberCarry::add_member_args`]).
    fn format_member_strings(endpoint_sets: &[[u16; 5]]) -> (String, String) {
        let hosts: Vec<String> = (1..=endpoint_sets.len())
            .map(|n| format!("127.0.0.{n}"))
            .collect();
        let members_str = endpoint_sets
            .iter()
            .zip(&hosts)
            .enumerate()
            .map(|(id, (p, h))| {
                format!(
                    "{id},{h}:{},{h}:{},{h}:{},{h}:{},{h}:{}",
                    p[0], p[1], p[2], p[3], p[4]
                )
            })
            .collect::<Vec<_>>()
            .join("|");
        let ingress_endpoints = endpoint_sets
            .iter()
            .zip(&hosts)
            .enumerate()
            .map(|(id, (p, h))| format!("{id}={h}:{}", p[0]))
            .collect::<Vec<_>>()
            .join(",");
        (members_str, ingress_endpoints)
    }

    /// Spawn one `ClusterNode` JVM, member `id` of this cluster. Its log
    /// is `sealer-<id><log_tag>.log`.
    fn spawn_member(&self, id: usize, log_tag: &str) -> Result<Proc> {
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
        // Every member starts in a new temporary directory, so this is
        // always the first start of a new cluster.
        .arg("-Dkardamom.cluster.bootstrap=true")
        .arg("-Dkardamom.cluster.ingressStreamId=101")
        .arg(format!("-Dkardamom.cluster.tickMs={}", self.tick_ms))
        .arg(format!(
            "-Dkardamom.cluster.orderingWindow={}",
            self.ordering_window
        ))
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
        ));
        if let Some(carry) = &self.carry {
            carry.add_member_args(&mut cmd, id);
        }
        cmd.args([
            "-cp",
            &self.jar.to_string(),
            "io.kardamom.sealer.cluster.ClusterNode",
        ]);
        Proc::spawn(
            &format!("sealer-{id}"),
            cmd,
            self.root.join(format!("sealer-{id}{log_tag}.log")),
        )
    }

    /// Spawn every member.
    fn spawn_all(&mut self) -> Result<()> {
        self.procs = (0..self.members.get())
            .map(|id| self.spawn_member(id, ""))
            .collect::<Result<_>>()?;
        Ok(())
    }

    /// Wait for every spawned member to log "cluster node up", then for
    /// one of them to report LEADER.
    fn await_ready(&mut self) -> Result<()> {
        for proc in &mut self.procs {
            wait_for_log_line(proc, "cluster node up", Duration::from_mins(1))
                .context("sealer member startup")?;
        }
        self.await_leader()
    }

    /// Poll member logs for a LEADER line. A 1-member cluster elects
    /// itself; with more members, any one of them can gain the role.
    fn await_leader(&self) -> Result<()> {
        super::metrics::poll_sync(
            "a sealer member to become LEADER",
            Duration::from_mins(1),
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

    fn finish(mut self) -> SealerCluster {
        let admin = self.carry.as_ref().map_or_else(Vec::new, |c| {
            c.admin_ports
                .iter()
                .map(|port| SocketAddr::from(([127, 0, 0, 1], *port)))
                .collect()
        });
        SealerCluster {
            procs: std::mem::take(&mut self.procs),
            ingress_endpoints: self.ingress_endpoints.clone(),
            admin,
            launch: self,
        }
    }
}

impl MemberCarry {
    /// The carry window settings of member `id`. In carry mode from
    /// genesis, every member takes the same genesis features: the sealer
    /// requires the setting to match on every member.
    ///
    /// Each member serves its admin endpoint on its own port, because the
    /// members share one host and the endpoint binds every address. The
    /// peer probe of a member asks each peer address on the port of the
    /// member, so here it asks the member itself, once for each peer. The
    /// start hold then ends once the member is stable, as with no peers,
    /// and the join watchdog of a member sees the member as each of its
    /// peers.
    fn add_member_args(&self, cmd: &mut Command, id: usize) {
        cmd.arg(format!(
            "-Dkardamom.cluster.adminPort={}",
            self.admin_ports[id]
        ))
        .arg(format!("-Dkardamom.cluster.retention={}", self.retention));
        if self.genesis {
            cmd.arg(format!(
                "-Dkardamom.cluster.genesisFeatures={}",
                super::config::GENESIS_FEATURES
            ))
            .arg(format!(
                "-Dkardamom.cluster.snapshotIntervalS={}",
                super::config::GENESIS_SNAPSHOT_INTERVAL_S
            ));
        }
        if id == 0 {
            cmd.arg(format!(
                "-Dkardamom.cluster.feedAeronDir={}",
                self.feed_dir.display()
            ));
        }
    }
}
