//! The Java Aeron Cluster (Raft) sealer, as local JVM child processes.
//!
//! `ClusterNode` is all-in-one (embedded media driver + archive + consensus
//! module + `SealerClusteredService`), configured purely by system
//! properties — the same invocation as `deploy/cluster/nomad/cluster.nomad.hcl`,
//! pointed at loopback endpoints. The semantics suite runs a single member
//! (Raft quorum of 1): canonical ordering without the fault-tolerance
//! machinery, which belongs to the chaos suite.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use anyhow::{Context, Result};

use super::proc::{Proc, free_udp_port, wait_for_log_line};

/// Locate `kardamom-cluster-node.jar`: `KARDAMOM_CLUSTER_JAR`, else the
/// gradle shadowJar output in the repo.
pub fn cluster_jar(repo_root: &Path) -> Result<PathBuf> {
    if let Ok(p) = std::env::var("KARDAMOM_CLUSTER_JAR") {
        let p = PathBuf::from(p);
        anyhow::ensure!(
            p.is_file(),
            "KARDAMOM_CLUSTER_JAR={} not found",
            p.display()
        );
        return Ok(p);
    }
    let built =
        repo_root.join("cluster/sealer-service/service/build/libs/kardamom-cluster-node.jar");
    anyhow::ensure!(
        built.is_file(),
        "cluster jar not found at {} — run `just cluster-jar` (gradle :service:shadowJar) or \
         set KARDAMOM_CLUSTER_JAR",
        built.display()
    );
    Ok(built)
}

pub struct SealerCluster {
    pub procs: Vec<Proc>,
    /// The Rust services' `[cluster] ingress_endpoints` value.
    pub ingress_endpoints: String,
}

impl SealerCluster {
    /// Launch `members` ClusterNode JVMs on loopback (1 for semantics runs;
    /// 3 exercises a real quorum). Blocks until every member logs
    /// "cluster node up" and member 0 reports a role (single-member: LEADER).
    pub fn launch(root: &Path, repo_root: &Path, members: usize, tick_ms: u64) -> Result<Self> {
        anyhow::ensure!(members >= 1, "cluster needs at least one member");
        let jar = cluster_jar(repo_root)?;

        // Five endpoints per member: ingress,consensus,log,catchup,archive.
        let mut endpoint_sets: Vec<[u16; 5]> = Vec::with_capacity(members);
        for _ in 0..members {
            endpoint_sets.push([
                free_udp_port()?,
                free_udp_port()?,
                free_udp_port()?,
                free_udp_port()?,
                free_udp_port()?,
            ]);
        }
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

        let mut procs = Vec::with_capacity(members);
        for id in 0..members {
            let node_root = root.join(format!("sealer-{id}"));
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
            .arg(format!("-Dkardamom.cluster.members={members_str}"))
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
            .arg(format!("-Dkardamom.cluster.tickMs={tick_ms}"))
            // The cluster node is an Aeron client too, so it walks off the
            // same 10 s cliff when the driver conductor is starved — see
            // `services::driver_timeout_ms`. Same value, Java's spelling.
            .arg(format!(
                "-Daeron.driver.timeout={}",
                super::services::driver_timeout_ms()
            ))
            .args([
                "-cp",
                &jar.display().to_string(),
                "io.kardamom.sealer.cluster.ClusterNode",
            ]);
            procs.push(Proc::spawn(
                &format!("sealer-{id}"),
                cmd,
                root.join(format!("sealer-{id}.log")),
            )?);
        }

        for proc in procs.iter_mut() {
            wait_for_log_line(proc, "cluster node up", Duration::from_secs(60))
                .context("sealer member startup")?;
        }
        // Leadership: a 1-member cluster elects itself; with more members any
        // one gains the role. Poll member logs for a LEADER line.
        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        loop {
            let led = procs.iter().any(|p| {
                std::fs::read_to_string(&p.log_path)
                    .map(|s| s.contains("role=LEADER"))
                    .unwrap_or(false)
            });
            if led {
                break;
            }
            if std::time::Instant::now() >= deadline {
                let tails: Vec<String> = procs
                    .iter()
                    .map(|p| format!("--- {} ---\n{}", p.name, p.log_tail(30)))
                    .collect();
                anyhow::bail!(
                    "no sealer member became LEADER in 60s:\n{}",
                    tails.join("\n")
                );
            }
            std::thread::sleep(Duration::from_millis(100));
        }

        Ok(Self {
            procs,
            ingress_endpoints,
        })
    }
}
