//! The dynamic sequencer sizing cases: a scale-out and scale-in under
//! load. The resize leaves the shard map at a later version, so the
//! account-to-shard table no longer pins the cases after it.

use std::path::{Path, PathBuf};

use anyhow::Context;

use crate::accounts::ACCT_VSLOT;
use crate::harness::Harness;
use crate::poll::{self, Budget};
use crate::scale::Resize;

/// The funded accounts whose slots move when the current map grows to three lanes.
///
/// # Errors
///
/// Returns an error if the map cannot be read or rebalanced.
pub(crate) fn moved_accounts(cluster_dir: &Path) -> anyhow::Result<Vec<u32>> {
    let text = std::fs::read_to_string(cluster_dir.join("config/shard-map.toml"))?;
    let current: kardamom_types::shard_map::ShardMap = toml::from_str(&text)?;
    let next = current.rebalance(3)?;
    Ok(ACCT_VSLOT
        .iter()
        .enumerate()
        .filter(|(_, v)| {
            let v = usize::from(**v);
            current.table()[v] != next.table()[v]
        })
        .map(|(i, _)| u32::try_from(i).unwrap_or(u32::MAX))
        .collect())
}

/// The map is restored after the case so the next run starts from version zero.
struct MapFile {
    path: PathBuf,
    contents: Vec<u8>,
}

impl MapFile {
    fn snapshot(cluster_dir: &Path) -> anyhow::Result<Self> {
        let path = cluster_dir.join("config/shard-map.toml");
        let contents = std::fs::read(&path).context("read shard-map.toml")?;
        Ok(Self { path, contents })
    }

    fn restore(&self) {
        if let Err(e) = std::fs::write(&self.path, &self.contents) {
            crate::log(format!(
                "resize: WARNING could not restore {}: {e}",
                self.path.display()
            ));
        }
    }
}

/// The in-process resize to `lanes`, the rollout `scale-sequencers`
/// runs for an operator.
fn resize(h: &Harness, cluster_dir: &Path, lanes: u32) -> anyhow::Result<Resize> {
    Resize::new(cluster_dir, &h.contract, lanes, false)
}

/// Print the lane report of every sequencer replica of `lanes` lanes:
/// the evidence behind a refused scale step, since the pre-flight guard
/// samples each replica once and stops at the first hit.
async fn report_lanes(h: &Harness, lanes: u8) {
    for line in h.probes.sequencer_lane_report(lanes).await {
        crate::log(format!("resize: {line}"));
    }
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
    let files = MapFile::snapshot(&cluster_dir)?;
    let result = scale_out_in_body(h, &cluster_dir).await;
    files.restore();
    result
}

/// The scale-out task, which runs while the case waits for lane 2.
struct ScaleOut(tokio::task::JoinHandle<anyhow::Result<()>>);

impl ScaleOut {
    /// The failure of a scale-out that ended before lane 2 ran. It
    /// carries the error of the rollout, for example a refused preflight.
    async fn early_exit(&mut self) -> anyhow::Error {
        let cause = match (&mut self.0).await {
            Ok(Ok(())) => "it reported success".to_string(),
            Ok(Err(e)) => format!("{e:#}"),
            Err(e) => format!("the task did not join: {e}"),
        };
        crate::chaos_fail!("resize: scale-out exited before lane 2 ran: {cause}")
    }

    async fn finish(self) -> anyhow::Result<()> {
        self.0.await.context("join the scale-out task")?
    }
}

async fn lane_two_running(h: &Harness) -> anyhow::Result<()> {
    let outcome = poll::until(Budget::secs(300, 3), |_| async move {
        Ok::<_, anyhow::Error>(
            h.nomad
                .running_in_group("sequencer", "seq-2")
                .await?
                .len()
                .ge(&2)
                .then_some(()),
        )
    })
    .await?;
    outcome
        .or_fail(|_| {
            crate::chaos_fail!("resize: lane 2 did not reach 2 running replicas within 300s")
        })
        .map(|_| ())
}

async fn scale_out_in_body(h: &mut Harness, cluster_dir: &Path) -> anyhow::Result<()> {
    let to_three = resize(h, cluster_dir, 3)?;
    let mut scale_out = ScaleOut(tokio::spawn(async move { to_three.run().await }));
    tokio::select! {
        err = scale_out.early_exit() => {
            report_lanes(h, 2).await;
            return Err(err);
        }
        running = lane_two_running(h) => running?,
    }
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
    if let Err(e) = scale_out.finish().await {
        report_lanes(h, 3).await;
        return Err(crate::chaos_fail!("resize: scale-out failed: {e}"));
    }
    crate::log("resize: scale-out 2 -> 3 done");
    wait_map_version(h, 0, 1).await?;
    wait_map_version(h, 1, 1).await?;
    h.assert_progress().await?;
    if let Err(e) = resize(h, cluster_dir, 2)?.run().await {
        report_lanes(h, 3).await;
        return Err(crate::chaos_fail!("resize: scale-in failed: {e}"));
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn an_early_exit_carries_the_rollout_error() {
        let mut failed = ScaleOut(tokio::spawn(async {
            Err(anyhow::anyhow!("preflight: lanes 0 1 not idle"))
        }));
        let message = failed.early_exit().await.to_string();
        assert!(message.contains("exited before lane 2 ran"), "{message}");
        assert!(
            message.contains("preflight: lanes 0 1 not idle"),
            "{message}"
        );
        let mut done = ScaleOut(tokio::spawn(async { Ok(()) }));
        assert!(
            done.early_exit()
                .await
                .to_string()
                .contains("reported success")
        );
    }

    #[test]
    fn selects_accounts_that_cross_lanes() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("config")).unwrap();
        let map = kardamom_types::shard_map::ShardMap::identity(2).unwrap();
        std::fs::write(
            dir.path().join("config/shard-map.toml"),
            toml::to_string(&map).unwrap(),
        )
        .unwrap();
        let accounts = moved_accounts(dir.path()).unwrap();
        assert!(!accounts.is_empty());
        assert!(
            accounts
                .iter()
                .all(|account| ACCT_VSLOT[usize::try_from(*account).unwrap()] < 86)
        );
    }
}
