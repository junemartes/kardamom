//! The dynamic sequencer sizing cases: a scale-out and scale-in under
//! load, and the executor lookup blackout. Both run last in their
//! shard: the resize leaves the shard map at a later version, so the
//! account-to-shard table no longer pins the cases after it.

use std::path::{Path, PathBuf};

use anyhow::Context;

use crate::accounts::ACCT_VSLOT;
use crate::harness::Harness;
use crate::poll::{self, Budget};

const LOOKUPS: &str = "kardamom_sequencer_nonce_lookups_total";

/// The vslot-to-lane table of a shard map file.
fn table_of(toml: &str) -> anyhow::Result<Vec<u32>> {
    let start = toml.find("table").context("shard map has no table")?;
    let body = &toml[start..];
    let open = body.find('[').context("shard map table has no [")?;
    let close = body.find(']').context("shard map table has no ]")?;
    body[open.saturating_add(1)..close]
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| {
            s.parse::<u32>()
                .with_context(|| format!("shard map entry {s}"))
        })
        .collect()
}

/// The funded accounts whose vslot moves to a new lane under the next
/// map: the fewest-moves render of `render-shard-map.py` to three
/// lanes, against the current `config/shard-map.toml`.
///
/// # Errors
///
/// Returns an error if the renderer fails or a map does not parse.
pub(crate) async fn moved_accounts(cluster_dir: &Path) -> anyhow::Result<Vec<u32>> {
    let current_path = cluster_dir.join("config/shard-map.toml");
    let current =
        table_of(&std::fs::read_to_string(&current_path).context("read shard-map.toml")?)?;
    let out = tokio::process::Command::new("python3")
        .arg(cluster_dir.join("scripts/render-shard-map.py"))
        .args(["--from", &current_path.to_string_lossy(), "--lanes", "3"])
        .output()
        .await
        .context("run render-shard-map.py")?;
    anyhow::ensure!(
        out.status.success(),
        "render-shard-map.py failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let next = table_of(&String::from_utf8_lossy(&out.stdout))?;
    Ok(ACCT_VSLOT
        .iter()
        .enumerate()
        .filter(|(_, v)| {
            let v = usize::from(**v);
            current.get(v) != next.get(v)
        })
        .map(|(i, _)| u32::try_from(i).unwrap_or(u32::MAX))
        .collect())
}

/// The two files a resize rewrites in the checkout, restored when the
/// case ends so the next run starts from map version 0.
struct MapFiles {
    paths: [PathBuf; 2],
    contents: [Vec<u8>; 2],
}

impl MapFiles {
    fn snapshot(cluster_dir: &Path) -> anyhow::Result<Self> {
        let paths = [
            cluster_dir.join("config/shard-map.toml"),
            cluster_dir.join("nomad/sequencer.nomad.hcl"),
        ];
        let contents = [
            std::fs::read(&paths[0]).context("read shard-map.toml")?,
            std::fs::read(&paths[1]).context("read sequencer.nomad.hcl")?,
        ];
        Ok(Self { paths, contents })
    }

    fn restore(&self) {
        self.paths.iter().zip(&self.contents).for_each(|(p, c)| {
            if let Err(e) = std::fs::write(p, c) {
                crate::log(format!(
                    "resize: WARNING could not restore {}: {e}",
                    p.display()
                ));
            }
        });
    }
}

/// Run `scripts/scale-sequencers.sh <lanes>` to completion, capturing
/// its output.
async fn scale(cluster_dir: &Path, lanes: u32) -> anyhow::Result<String> {
    let out = tokio::process::Command::new("./scripts/scale-sequencers.sh")
        .arg(lanes.to_string())
        .current_dir(cluster_dir)
        .output()
        .await
        .context("run scale-sequencers.sh")?;
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    anyhow::ensure!(
        out.status.success(),
        "scale-sequencers.sh {lanes} failed:\n{}",
        tail(&text, 40)
    );
    Ok(text)
}

fn tail(text: &str, n: usize) -> String {
    let lines: Vec<&str> = text.lines().collect();
    lines[lines.len().saturating_sub(n)..].join("\n")
}

async fn map_version_is(h: &Harness, ingress: usize, want: i64) -> bool {
    h.probes
        .ingress_map_version(&h.probes.ingresses[ingress])
        .await
        == Some(want)
}

async fn wait_map_version(h: &Harness, ingress: usize, want: i64) -> anyhow::Result<()> {
    let outcome = poll::until(Budget::secs(60, 3), |_| async move {
        Ok::<_, anyhow::Error>(map_version_is(h, ingress, want).await.then_some(()))
    })
    .await?;
    outcome
        .or_fail(|t| {
            crate::chaos_fail!(
                "resize: ingress-{ingress} on map version {want}: not reached within {}s",
                t.as_secs()
            )
        })
        .map(|_| ())
}

/// Scale from 2 to 3 lanes under load, hard-kill one replica of the
/// new lane during the overlap, then scale back to 2. The load is a
/// sender whose vslot moves, with a wide submit retry to ride the
/// ingress roll, and its verdict is the moved-sender nonce-gap check.
pub(crate) async fn scale_out_in(h: &mut Harness) -> anyhow::Result<()> {
    let cluster_dir = h.lifecycle.cluster_dir().to_path_buf();
    anyhow::ensure!(
        map_version_is(h, 0, 0).await,
        "{}: resize: ingress-0 does not run map version 0 before the case",
        crate::FAIL_PREFIX
    );
    let files = MapFiles::snapshot(&cluster_dir)?;
    let result = scale_out_in_body(h, &cluster_dir).await;
    files.restore();
    result
}

async fn scale_out_in_body(h: &mut Harness, cluster_dir: &Path) -> anyhow::Result<()> {
    let dir = cluster_dir.to_path_buf();
    let scale_out = tokio::spawn(async move { scale(&dir, 3).await });
    let (hs, scale_ref): (&Harness, &tokio::task::JoinHandle<anyhow::Result<String>>) =
        (h, &scale_out);
    let outcome = poll::until(Budget::secs(300, 3), |_| async move {
        anyhow::ensure!(
            !scale_ref.is_finished(),
            "{}: resize: scale-out exited before lane 2 ran",
            crate::FAIL_PREFIX
        );
        Ok(hs
            .nomad
            .running_in_group("sequencer", "seq-2")
            .await?
            .len()
            .ge(&2)
            .then_some(()))
    })
    .await?;
    outcome.or_fail(|_| {
        crate::chaos_fail!("resize: lane 2 did not reach 2 running replicas within 300s")
    })?;
    crate::log(
        "resize: lane 2 runs in shadow mode; hard-killing one of its replicas during the overlap",
    );
    let nodes: Vec<String> = h
        .probes
        .sequencers
        .iter()
        .map(|n| n.container.clone())
        .collect();
    let refs: Vec<&str> = nodes.iter().map(String::as_str).collect();
    h.inject_hard(&refs, "sequencer-2").await?;
    h.assert_count("sequencer", 6, h.knobs.restart_slo).await?;
    let out_log = scale_out
        .await
        .context("join the scale-out task")?
        .map_err(|e| crate::chaos_fail!("resize: scale-out failed: {e}"))?;
    crate::log(format!(
        "resize: scale-out 2 -> 3 done ({} steps)",
        out_log.lines().filter(|l| l.starts_with("==>")).count()
    ));
    wait_map_version(h, 0, 1).await?;
    wait_map_version(h, 1, 1).await?;
    h.assert_progress().await?;
    scale(cluster_dir, 2)
        .await
        .map_err(|e| crate::chaos_fail!("resize: scale-in failed: {e}"))?;
    crate::log("resize: scale-in 3 -> 2 done");
    let hs: &Harness = h;
    let outcome = poll::until(Budget::secs(120, 3), |_| async move {
        Ok::<_, anyhow::Error>(
            hs.nomad
                .running_in_group("sequencer", "seq-2")
                .await?
                .is_empty()
                .then_some(()),
        )
    })
    .await?;
    outcome
        .or_fail(|_| crate::chaos_fail!("resize: lane 2 group stopped: not reached within 120s"))?;
    h.assert_count("sequencer", 4, h.knobs.restart_slo).await?;
    wait_map_version(h, 0, 2).await?;
    h.assert_progress().await
}

/// The failed-lookup outcomes of lane 0's replica on node 0.
async fn lookups_failed(h: &Harness) -> i64 {
    let t = h
        .probes
        .seq_lane0_metric_where(0, LOOKUPS, "outcome=\"timeout\"")
        .await
        .unwrap_or(0);
    let e = h
        .probes
        .seq_lane0_metric_where(0, LOOKUPS, "outcome=\"error\"")
        .await
        .unwrap_or(0);
    t.saturating_add(e)
}

async fn lookups_ok(h: &Harness) -> i64 {
    h.probes
        .seq_lane0_metric_where(0, LOOKUPS, "outcome=\"ok\"")
        .await
        .unwrap_or(0)
}

/// Blackhole every executor from the sequencer node, hard-kill lane 0's
/// replica so it comes back cold, and require its lookups to fail while
/// the twin keeps the lane live; then restore the routes, kill it once
/// more, and require an answered lookup.
pub(crate) async fn lookup_blackout(h: &mut Harness) -> anyhow::Result<()> {
    let node = h.probes.sequencers[0].container.clone();
    let executors: Vec<String> = h
        .probes
        .executors
        .iter()
        .map(|e| e.ip.to_string())
        .collect();
    let base_fail = lookups_failed(h).await;
    for e in &executors {
        h.nodes
            .exec(&node, &format!("ip route add blackhole {e}/32"))
            .await
            .map_err(|err| {
                crate::chaos_fail!("lookup-blackout: could not blackhole {e} on {node}: {err}")
            })?;
    }
    crate::log(format!(
        "lookup-blackout: executors blackholed from {node}; hard-killing lane 0's replica there"
    ));
    let phase1 = blackout_phase(h, &node, base_fail).await;
    for e in &executors {
        let _ = h
            .nodes
            .exec(&node, &format!("ip route del blackhole {e}/32"))
            .await;
    }
    phase1.map_err(|e| crate::chaos_fail!("lookup-blackout: blackout phase failed: {e}"))?;
    crate::log(
        "lookup-blackout: routes restored; hard-killing the replica again for an answered lookup",
    );
    let base_ok = lookups_ok(h).await;
    h.inject_hard(&[&node], "sequencer-0").await?;
    h.assert_count("sequencer", 4, h.knobs.restart_slo).await?;
    let hs: &Harness = h;
    let outcome = poll::until(Budget::secs(120, 3), |_| async move {
        Ok::<_, anyhow::Error>((lookups_ok(hs).await > base_ok).then_some(()))
    })
    .await?;
    outcome.or_fail(|_| {
        crate::chaos_fail!(
            "lookup-blackout: cold replica's lookup answered: not reached within 120s"
        )
    })?;
    h.assert_progress().await
}

async fn blackout_phase(h: &mut Harness, node: &str, base_fail: i64) -> anyhow::Result<()> {
    h.inject_hard(&[node], "sequencer-0").await?;
    h.assert_progress().await?;
    h.assert_count("sequencer", 4, h.knobs.restart_slo).await?;
    let hs: &Harness = h;
    let outcome = poll::until(Budget::secs(120, 3), |_| async move {
        Ok::<_, anyhow::Error>((lookups_failed(hs).await > base_fail).then_some(()))
    })
    .await?;
    outcome
        .or_fail(|_| crate::chaos_fail!("lookup-blackout: cold replica's lookups failed with no executor reachable: not reached within 120s"))
        .map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_a_shard_map_table() {
        let toml = "version = 0\ntable = [\n  0, 1, 0, 1,\n  0, 1,\n]\n";
        assert_eq!(table_of(toml).unwrap(), [0, 1, 0, 1, 0, 1]);
        assert!(table_of("nothing").is_err());
    }
}
