//! A sequencer resize warms gaining lanes in shadow mode, switches the ingress
//! map, drains old lanes for one transaction TTL, then installs the steady job.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use anyhow::Context;
use kardamom_types::shard_map::ShardMap;

use crate::contract::NodeContract;
use crate::lifecycle::NOMAD_HTTP_PORT;
use crate::metrics::{Scrape, Target};

mod readiness;
use crate::nomad::Nomad;
use crate::poll::{self, Budget};
use crate::probes::{Probed, SEQUENCER_LANE0_PORT};
use readiness::LaneReadiness;

const MAP: &str = "config/shard-map.toml";
const NEXT_MAP: &str = "config/shard-map.next.toml";
const GROUP_VARS: &str = "ansible/group_vars/all.yml";
/// Lane `n` exports on `9001 + 10n`.
const LANE_STRIDE: u16 = 10;
const MAX_LANES: u32 = 8;
/// A submitted job must place and start within five minutes.
const CONVERGE_SECS: u64 = 300;
const METRIC_INTERVAL: Duration = Duration::from_secs(2);
/// The metric waits get one TTL plus this margin.
const WAIT_MARGIN: Duration = Duration::from_mins(1);
const RESYNC: &str = "kardamom_sequencer_resync_mode";
const PENDING: &str = "kardamom_sequencer_pending_depth";
const SHADOW: &str = "kardamom_sequencer_shadow_vslots";

/// One resize of the active lane count.
pub struct Resize {
    cluster_dir: PathBuf,
    nomad_addr: String,
    sequencers: BTreeMap<String, Probed>,
    tx_ttl: Duration,
    target: u32,
    dry_run: bool,
    scrape: Scrape,
}

impl Resize {
    /// Build a resize to `target` lanes of the cluster the contract
    /// describes. `dry_run` renders and logs, and submits nothing.
    ///
    /// # Errors
    ///
    /// Returns an error if `target` is outside 1..=8, the contract has
    /// no control node, or the transaction TTL cannot be read.
    pub fn new(
        cluster_dir: &Path,
        contract: &NodeContract,
        target: u32,
        dry_run: bool,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            (1..=MAX_LANES).contains(&target),
            "target lanes must be 1..{MAX_LANES}"
        );
        let group_vars = std::fs::read_to_string(cluster_dir.join(GROUP_VARS))
            .with_context(|| format!("read {GROUP_VARS}"))?;
        Ok(Self {
            cluster_dir: cluster_dir.to_path_buf(),
            nomad_addr: contract.nomad_addr(NOMAD_HTTP_PORT)?,
            sequencers: contract
                .of_role("sequencer")
                .into_iter()
                .map(|n| {
                    (
                        n.name.clone(),
                        Probed {
                            container: n.container.clone(),
                            ip: n.ip,
                        },
                    )
                })
                .collect(),
            tx_ttl: tx_ttl_in(&group_vars)?,
            target,
            dry_run,
            scrape: Scrape::new(),
        })
    }

    /// Run the rollout to completion.
    ///
    /// # Errors
    ///
    /// Returns an error on a failed precondition, render, submit, or wait.
    pub async fn run(&self) -> anyhow::Result<()> {
        let map = self.path(MAP);
        anyhow::ensure!(
            map.is_file(),
            "no {MAP}; deploy an initial shard map with Ansible"
        );
        anyhow::ensure!(
            !self.path(NEXT_MAP).exists(),
            "a resize is in flight ({NEXT_MAP} exists); finish or remove it first"
        );
        let previous: ShardMap = toml::from_str(&std::fs::read_to_string(&map)?)?;
        let current = u32::from(previous.active_lanes());
        anyhow::ensure!(current != self.target, "already at {} lanes", self.target);
        self.preflight(current).await?;
        crate::log(format!(
            "resize {current} -> {} lanes (tx_ttl {}s)",
            self.target,
            self.tx_ttl.as_secs()
        ));
        let next = previous.rebalance(self.target)?;
        if !self.dry_run {
            std::fs::write(self.path(NEXT_MAP), toml::to_string(&next)?)?;
        }
        let gaining: Vec<u32> = (0..next.active_lanes())
            .filter(|lane| {
                !next
                    .vslot_set(*lane)
                    .difference(&previous.vslot_set(*lane))
                    .is_empty()
            })
            .map(u32::from)
            .collect();
        crate::log(format!(
            "lanes that gain vslots (shadow first): {}",
            join(&gaining)
        ));
        self.run_sequencer(&next, Some(&previous), "overlap: new lanes in shadow mode")
            .await?;
        self.wait_running("sequencer").await?;
        self.wait_lanes(&gaining, SHADOW, "warm-up").await?;
        if !self.dry_run {
            std::fs::copy(self.path(NEXT_MAP), &map).context("promote the next map")?;
        }
        let version = next.version();
        self.run_job(
            "ingress",
            &format!("ingress on map {version}"),
            serde_json::json!({}),
        )
        .await?;
        self.wait_running("ingress").await?;
        crate::log(format!(
            "drain: waiting tx_ttl ({}s)",
            self.tx_ttl.as_secs()
        ));
        if !self.dry_run {
            tokio::time::sleep(self.tx_ttl).await;
        }
        let old: Vec<u32> = (0..current).collect();
        self.wait_lanes(&old, PENDING, "drain").await?;
        self.run_sequencer(&next, None, "steady: final vslot sets")
            .await?;
        self.wait_running("sequencer").await?;
        if !self.dry_run {
            std::fs::remove_file(self.path(NEXT_MAP)).context("remove the next map")?;
        }
        crate::log(format!("done: {} active lanes.", self.target));
        self.log_commit_advice();
        Ok(())
    }

    fn path(&self, rel: &str) -> PathBuf {
        self.cluster_dir.join(rel)
    }

    /// Every expected replica must prove both idle gauges in one scrape.
    async fn preflight(&self, current: u32) -> anyhow::Result<()> {
        self.wait_ready(
            &(0..current).collect::<Vec<_>>(),
            &[RESYNC, PENDING],
            "preflight",
        )
        .await
    }

    async fn wait_lanes(&self, lanes: &[u32], metric: &str, what: &str) -> anyhow::Result<()> {
        self.wait_ready(lanes, &[metric], what).await
    }

    async fn wait_ready(&self, lanes: &[u32], metrics: &[&str], what: &str) -> anyhow::Result<()> {
        if self.dry_run {
            return Ok(());
        }
        let ready = LaneReadiness::new(self, lanes, metrics).await?;
        let budget = Budget::new(self.tx_ttl.saturating_add(WAIT_MARGIN), METRIC_INTERVAL);
        let outcome = poll::until(budget, |_| async {
            Ok(ready.sample().await?.then_some(()))
        })
        .await?;
        outcome.or_fail(|t| anyhow::anyhow!(
            "{what}: every expected replica of lanes {} must answer {} with zero on the current job version within {}s",
            join(lanes), metrics.join(", "), t.as_secs()
        ))?;
        Ok(())
    }

    async fn run_sequencer(
        &self,
        next: &ShardMap,
        previous: Option<&ShardMap>,
        what: &str,
    ) -> anyhow::Result<()> {
        let vars = serde_json::json!({
            "shard_table": serde_json::to_string(next.table().as_slice())?,
            "previous_shard_table": serde_json::to_string(previous.map_or(&[][..], |map| map.table().as_slice()))?,
        });
        self.run_job("sequencer", what, vars).await
    }

    async fn run_job(
        &self,
        service: &str,
        what: &str,
        vars: serde_json::Value,
    ) -> anyhow::Result<()> {
        crate::log(format!("ansible resize submission: {service} ({what})"));
        if self.dry_run {
            return Ok(());
        }
        let extra = serde_json::json!({
            "workloads_resize_job": service,
            "workloads_resize_vars": vars,
        });
        let status = tokio::process::Command::new("ansible-playbook")
            .args([
                "-i",
                "localhost,",
                "ansible/resize.yml",
                "--extra-vars",
                &extra.to_string(),
            ])
            .current_dir(&self.cluster_dir)
            .env("NOMAD_ADDR", &self.nomad_addr)
            .stdin(Stdio::null())
            .status()
            .await
            .context("spawn Ansible resize submission")?;
        anyhow::ensure!(
            status.success(),
            "resize submission for {service} failed with {status}"
        );
        Ok(())
    }

    /// Every desired group has its full count running on the current job version.
    async fn wait_running(&self, job: &str) -> anyhow::Result<()> {
        if self.dry_run {
            return Ok(());
        }
        let nomad = Nomad::new(&self.nomad_addr)?;
        let desired = nomad.job(job).await?;
        let outcome = poll::until(Budget::secs(CONVERGE_SECS, 5), |_| async {
            let allocs = nomad.allocations(job).await?;
            Ok(desired.running(&allocs).map(|_| ()))
        })
        .await?;
        outcome.or_fail(|t| anyhow::anyhow!("job {job} did not converge in {}s", t.as_secs()))?;
        Ok(())
    }

    fn log_commit_advice(&self) {
        if matches!(self.target, 1 | 2 | 4 | 8) {
            crate::log(format!(
                "commit together: {MAP}, {GROUP_VARS} (partition_count: {}).",
                self.target
            ));
            return;
        }
        crate::log(format!(
            "{} lanes is a transient count: the contract accepts a steady count of 1, 2, 4, or 8. Resize again toward one of those before you commit the map.",
            self.target
        ));
    }
}

fn lane_target(replica: &Probed, lane: u32) -> Target {
    Target::bridged(replica.ip, &replica.container, lane_port(lane))
}

fn lane_port(lane: u32) -> u16 {
    let stride = u16::try_from(lane)
        .unwrap_or(u16::MAX)
        .saturating_mul(LANE_STRIDE);
    SEQUENCER_LANE0_PORT.saturating_add(stride)
}

fn join(lanes: &[u32]) -> String {
    if lanes.is_empty() {
        return "none".to_string();
    }
    lanes
        .iter()
        .map(u32::to_string)
        .collect::<Vec<_>>()
        .join(" ")
}

/// The transaction TTL of the deployment, rounded up to whole seconds.
fn tx_ttl_in(group_vars: &str) -> anyhow::Result<Duration> {
    let millis: u64 = group_vars
        .lines()
        .find_map(|l| l.strip_prefix("tx_ttl_ms:"))
        .and_then(|v| v.split('#').next())
        .and_then(|v| v.trim().parse().ok())
        .context("could not read tx_ttl_ms from group_vars")?;
    Ok(Duration::from_secs(millis.div_ceil(1000)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn dry_run_preserves_the_map_and_creates_no_resize_marker() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("config")).unwrap();
        let original = toml::to_string(&ShardMap::identity(2).unwrap()).unwrap();
        std::fs::write(dir.path().join(MAP), &original).unwrap();
        let resize = Resize {
            cluster_dir: dir.path().to_path_buf(),
            nomad_addr: "http://127.0.0.1:1".to_string(),
            sequencers: BTreeMap::new(),
            tx_ttl: Duration::from_secs(1),
            target: 3,
            dry_run: true,
            scrape: Scrape::new(),
        };
        resize.run().await.unwrap();
        assert_eq!(
            std::fs::read_to_string(dir.path().join(MAP)).unwrap(),
            original
        );
        assert!(!dir.path().join(NEXT_MAP).exists());
        assert!(!dir.path().join("nomad/sequencer.nomad.hcl").exists());
    }

    #[test]
    fn the_ttl_rounds_up_and_ignores_a_comment() {
        assert_eq!(
            tx_ttl_in("x: 1\ntx_ttl_ms: 30500 # half\n").unwrap(),
            Duration::from_secs(31)
        );
        assert!(tx_ttl_in("x: 1\n").is_err());
    }
}
