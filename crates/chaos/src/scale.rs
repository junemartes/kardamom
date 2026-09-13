//! The sequencer lane resize, the shadow-first overlap rollout of
//! `docs/specs/dynamic-sequencer-sizing.md`: render the next map and the
//! overlap job, let the gaining lanes warm up in shadow mode, move the
//! ingress to the new map, drain the old lanes for one transaction TTL,
//! then render the steady job. The renders stay in the two Python
//! generators; this module runs the rollout and waits on the replicas'
//! metrics.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use anyhow::Context;

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
const JOB: &str = "nomad/sequencer.nomad.hcl";
const INGRESS_JOB: &str = "nomad/ingress.nomad.hcl";
const GROUP_VARS: &str = "ansible/group_vars/all.yml";
const DIGESTS: &str = "images.digests";
const RENDER_MAP: &str = "scripts/render-shard-map.py";
const RENDER_JOB: &str = "scripts/render-sequencer-job.py";
/// Lane `n` exports on `9001 + 10n`.
const LANE_STRIDE: u16 = 10;
const MAX_LANES: u32 = 8;
/// A submitted job must place and start within five minutes.
const CONVERGE_SECS: u64 = 300;
const METRIC_INTERVAL: Duration = Duration::from_secs(2);
/// The metric waits get one TTL plus this margin.
const WAIT_MARGIN: Duration = Duration::from_secs(60);
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
            "no {MAP}; render one with {RENDER_MAP} --identity <lanes>"
        );
        anyhow::ensure!(
            !self.path(NEXT_MAP).exists(),
            "a resize is in flight ({NEXT_MAP} exists); finish or remove it first"
        );
        let current = lanes_of_map(&std::fs::read_to_string(&map)?)?;
        anyhow::ensure!(current != self.target, "already at {} lanes", self.target);
        self.preflight(current).await?;
        crate::log(format!(
            "resize {current} -> {} lanes (tx_ttl {}s)",
            self.target,
            self.tx_ttl.as_secs()
        ));
        let target = self.target.to_string();
        self.render(RENDER_MAP, &["--from", MAP, "--lanes", &target], NEXT_MAP)?;
        self.render(RENDER_JOB, &["--map", NEXT_MAP, "--from", MAP], JOB)?;
        let gaining = gaining_lanes(&std::fs::read_to_string(self.path(JOB))?, self.target);
        crate::log(format!(
            "lanes that gain vslots (shadow first): {}",
            join(&gaining)
        ));
        self.run_job(JOB, "sequencer", "overlap: new lanes in shadow mode")
            .await?;
        self.wait_running("sequencer").await?;
        self.wait_lanes(&gaining, SHADOW, "warm-up").await?;
        std::fs::copy(self.path(NEXT_MAP), &map).context("promote the next map")?;
        let version = map_version(&std::fs::read_to_string(&map)?);
        self.run_job(INGRESS_JOB, "ingress", &format!("ingress on map {version}"))
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
        self.render(RENDER_JOB, &["--map", MAP], JOB)?;
        self.run_job(JOB, "sequencer", "steady: final vslot sets")
            .await?;
        self.wait_running("sequencer").await?;
        std::fs::remove_file(self.path(NEXT_MAP)).context("remove the next map")?;
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

    /// Run a generator and write its output to `out` under the cluster
    /// directory.
    fn render(&self, script: &str, args: &[&str], out: &str) -> anyhow::Result<()> {
        let output = std::process::Command::new("python3")
            .arg(self.path(script))
            .args(args)
            .current_dir(&self.cluster_dir)
            .stdin(Stdio::null())
            .output()
            .with_context(|| format!("run {script}"))?;
        anyhow::ensure!(
            output.status.success(),
            "{script} failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
        std::fs::write(self.path(out), output.stdout).with_context(|| format!("write {out}"))
    }

    /// The pinned image of a service from the digest manifest, when the
    /// deployment wrote one.
    fn image_ref(&self, service: &str) -> Option<String> {
        let manifest = std::fs::read_to_string(self.path(DIGESTS)).ok()?;
        image_ref_in(&manifest, service)
    }

    async fn run_job(&self, file: &str, service: &str, what: &str) -> anyhow::Result<()> {
        let image = self
            .image_ref(service)
            .map(|r| format!("image_ref={r}"))
            .into_iter()
            .flat_map(|v| ["-var".to_string(), v]);
        let args: Vec<String> = ["job".to_string(), "run".to_string()]
            .into_iter()
            .chain(image)
            .chain(std::iter::once(file.to_string()))
            .collect();
        crate::log(format!("nomad {} ({what})", args.join(" ")));
        if self.dry_run {
            return Ok(());
        }
        let status = tokio::process::Command::new("nomad")
            .args(&args)
            .current_dir(&self.cluster_dir)
            .env("NOMAD_ADDR", &self.nomad_addr)
            .stdin(Stdio::null())
            .status()
            .await
            .context("spawn nomad job run")?;
        anyhow::ensure!(
            status.success(),
            "nomad job run {file} failed with {status}"
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
                "commit together: {MAP}, {JOB}, {GROUP_VARS} (partition_count: {}), {INGRESS_JOB} (\"--shards\", \"{}\").",
                self.target, self.target
            ));
            return;
        }
        crate::log(format!(
            "{} lanes is a transient count: the contract accepts a steady count of 1, 2, 4, or 8. Resize again toward one of those before you commit the map and the job.",
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

/// The active lane count of a shard map: the highest lane in the table
/// plus one.
///
/// # Errors
///
/// Returns an error if the map has no table.
pub fn lanes_of_map(toml: &str) -> anyhow::Result<u32> {
    let start = toml.find("table").context("shard map has no table")?;
    let body = &toml[start..];
    let open = body.find('[').context("shard map table has no [")?;
    let close = body.find(']').context("shard map table has no ]")?;
    let max = body[open.saturating_add(1)..close]
        .split(',')
        .filter_map(|s| s.trim().parse::<u32>().ok())
        .max()
        .context("shard map table is empty")?;
    Ok(max.saturating_add(1))
}

/// The `version = N` line of a shard map.
fn map_version(toml: &str) -> String {
    toml.lines()
        .find_map(|l| l.strip_prefix("version = "))
        .unwrap_or("?")
        .trim()
        .to_string()
}

/// The lanes whose group in the rendered job carries `--shadow-vslots`.
fn gaining_lanes(job: &str, target: u32) -> Vec<u32> {
    (0..target)
        .filter(|lane| group_block(job, *lane).is_some_and(|b| b.contains("\"--shadow-vslots\"")))
        .collect()
}

/// The text of `group "seq-<lane>"` up to its closing brace at two
/// spaces of indentation.
fn group_block(job: &str, lane: u32) -> Option<&str> {
    let start = job.find(&format!("group \"seq-{lane}\""))?;
    let rest = &job[start..];
    let end = rest.find("\n  }").unwrap_or(rest.len());
    Some(&rest[..end])
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

/// The last `<service> <ref>` line of the digest manifest.
fn image_ref_in(manifest: &str, service: &str) -> Option<String> {
    manifest
        .lines()
        .filter_map(|l| {
            let mut fields = l.split_whitespace();
            (fields.next()? == service).then(|| fields.next())?
        })
        .next_back()
        .map(str::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_map_gives_its_lane_count_and_version() {
        let map = "version = 2\ntable = [\n  0, 1, 2, 0,\n]\n";
        assert_eq!(lanes_of_map(map).unwrap(), 3);
        assert_eq!(map_version(map), "2");
        assert!(lanes_of_map("nothing").is_err());
    }

    #[test]
    fn a_lane_gains_when_its_group_runs_in_shadow_mode() {
        let job = "  group \"seq-0\" {\n    args = [\"--vslots\", \"1\"]\n  }\n  group \"seq-1\" {\n    args = [\"--shadow-vslots\", \"3\"]\n  }\n";
        assert_eq!(gaining_lanes(job, 2), vec![1]);
        assert_eq!(gaining_lanes(job, 1), Vec::<u32>::new());
    }

    #[test]
    fn the_ttl_rounds_up_and_ignores_a_comment() {
        assert_eq!(
            tx_ttl_in("x: 1\ntx_ttl_ms: 30500 # half\n").unwrap(),
            Duration::from_secs(31)
        );
        assert!(tx_ttl_in("x: 1\n").is_err());
    }

    #[test]
    fn the_last_manifest_line_of_a_service_wins() {
        let manifest = "sequencer r@sha256:a\ningress r@sha256:b\nsequencer r@sha256:c\n";
        assert_eq!(
            image_ref_in(manifest, "sequencer").as_deref(),
            Some("r@sha256:c")
        );
        assert!(image_ref_in(manifest, "executor").is_none());
        assert_eq!(lane_port(2), 9021);
    }
}
