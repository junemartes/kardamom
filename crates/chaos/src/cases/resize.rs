//! The dynamic sequencer sizing cases: a scale-out and scale-in under
//! load, and the executor lookup blackout. Both run last in their
//! shard: the resize leaves the shard map at a later version, so the
//! account-to-shard table no longer pins the cases after it.

use std::path::{Path, PathBuf};

use anyhow::Context;

use crate::accounts::ACCT_VSLOT;
use crate::harness::Harness;
use crate::poll::{self, Budget};
use crate::scale::Resize;

const LOOKUPS: &str = "kardamom_sequencer_nonce_lookups_total";

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
    let files = MapFiles::snapshot(&cluster_dir)?;
    let result = scale_out_in_body(h, &cluster_dir).await;
    files.restore();
    result
}

async fn scale_out_in_body(h: &mut Harness, cluster_dir: &Path) -> anyhow::Result<()> {
    let to_three = resize(h, cluster_dir, 3)?;
    let scale_out = tokio::spawn(async move { to_three.run().await });
    let (hs, scale_ref): (&Harness, &tokio::task::JoinHandle<anyhow::Result<()>>) = (h, &scale_out);
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
    if let Err(e) = scale_out.await.context("join the scale-out task")? {
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
/// the twin keeps the lane live. Then restore the routes, drop only the
/// executors' UDP on the node, kill the replica once more, and require
/// an answered lookup.
///
/// Both phases hard-kill the replica, and the replacement is a new
/// process whose counters start at zero, so each wait counts from zero:
/// an answered lookup is a one-off (the floor is known after it), so the
/// newborn's `ok` count stays at exactly one and never rises past the
/// killed process's count.
///
/// Why phase 2 drops the UDP: a receipt-proven floor makes the sequencer
/// skip the lookup by design, and with a live twin the first receipt
/// often beats the first park. Receipts ride UDP from the executors; the
/// lookup is TCP to the executors. In phase 1 the route blackhole blocks
/// both, so every park requests a lookup that fails.
pub(crate) async fn lookup_blackout(h: &mut Harness) -> anyhow::Result<()> {
    let node = h.probes.sequencers[0].container.clone();
    let executors: Vec<String> = h
        .probes
        .executors
        .iter()
        .map(|e| e.ip.to_string())
        .collect();
    for e in &executors {
        h.nodes
            .exec(&node, &format!("ip route add blackhole {e}/32"))
            .await
            .map_err(|err| {
                crate::chaos_fail!("lookup-blackout: could not blackhole {e} on {node}: {err}")
            })?;
    }
    cache_traffic(h, &node, "-I").await?;
    crate::log(format!(
        "lookup-blackout: executors blackholed and the cache ports dropped on {node}; hard-killing lane 0's replica there"
    ));
    let phase1 = blackout_phase(h, &node).await;
    let _ = cache_traffic(h, &node, "-D").await;
    for e in &executors {
        let _ = h
            .nodes
            .exec(&node, &format!("ip route del blackhole {e}/32"))
            .await;
    }
    lookup_snapshot(h, &node, "lookup-blackout: blackout phase").await;
    phase1.map_err(|e| crate::chaos_fail!("lookup-blackout: blackout phase failed: {e}"))?;
    crate::log(format!(
        "lookup-blackout: routes restored; dropping the executors' UDP on {node} (receipts, not lookups), then hard-killing the replica again for an answered lookup"
    ));
    for e in &executors {
        h.nodes
            .exec(
                &node,
                &format!("iptables -w 5 -I INPUT -p udp -s {e} -j DROP"),
            )
            .await
            .map_err(|err| {
                crate::chaos_fail!("lookup-blackout: could not drop UDP from {e} on {node}: {err}")
            })?;
    }
    let phase2 = answered_phase(h, &node).await;
    // The block is short: the lookup answers within seconds of the
    // restart, and the node's other replica needs its receipts back.
    for e in &executors {
        let _ = h
            .nodes
            .exec(
                &node,
                &format!("iptables -w 5 -D INPUT -p udp -s {e} -j DROP"),
            )
            .await;
    }
    lookup_snapshot(h, &node, "lookup-blackout: answered phase").await;
    phase2.map_err(|e| crate::chaos_fail!("lookup-blackout: answered-lookup phase failed: {e}"))?;
    h.assert_progress().await
}

/// The Redis and Sentinel ports of the cache layer.
const CACHE_PORTS: [u16; 2] = [6379, 26379];

/// Insert (`-I`) or delete (`-D`) the rules that drop this node's cache
/// traffic. The blackout phase needs them: since the cache flag day a
/// nonce lookup can be answered from Redis, and then no lookup reaches
/// the blackholed executors and none fails. Run 35061276238 showed
/// exactly that, with `outcome="redis"` and no timeout. The rules are
/// TCP only, so the node keeps the Aeron traffic it needs, which is UDP.
async fn cache_traffic(h: &Harness, node: &str, rule: &str) -> anyhow::Result<()> {
    for port in CACHE_PORTS {
        h.nodes
            .exec(
                node,
                &format!("iptables -w 5 {rule} OUTPUT -p tcp --dport {port} -j DROP"),
            )
            .await
            .map_err(|e| {
                crate::chaos_fail!("lookup-blackout: iptables {rule} port {port} on {node}: {e}")
            })?;
    }
    Ok(())
}

async fn blackout_phase(h: &mut Harness, node: &str) -> anyhow::Result<()> {
    h.inject_hard(&[node], "sequencer-0").await?;
    h.assert_progress().await?;
    h.assert_count("sequencer", 4, h.knobs.restart_slo).await?;
    let hs: &Harness = h;
    let outcome = poll::until(Budget::secs(120, 3), |_| async move {
        Ok::<_, anyhow::Error>((lookups_failed(hs).await > 0).then_some(()))
    })
    .await?;
    outcome
        .or_fail(|_| crate::chaos_fail!("lookup-blackout: cold replica's lookups failed with no executor reachable: not reached within 120s"))
        .map(|_| ())
}

async fn answered_phase(h: &mut Harness, node: &str) -> anyhow::Result<()> {
    h.inject_hard(&[node], "sequencer-0").await?;
    h.assert_count("sequencer", 4, h.knobs.restart_slo).await?;
    let hs: &Harness = h;
    let outcome = poll::until(Budget::secs(120, 3), |_| async move {
        Ok::<_, anyhow::Error>((lookups_ok(hs).await > 0).then_some(()))
    })
    .await?;
    outcome
        .or_fail(|_| {
            crate::chaos_fail!(
                "lookup-blackout: cold replica's lookup answered: not reached within 120s"
            )
        })
        .map(|_| ())
}

/// The lane report of lane 0 and the tail of the replica's own log, so a
/// red run tells the cases apart: no park at all, a park with the floor
/// already known (no request), or a lookup that ran. Never fails the
/// case.
async fn lookup_snapshot(h: &Harness, node: &str, ctx: &str) {
    for line in h.probes.sequencer_lane_report(1).await {
        crate::log(format!("{ctx}: {line}"));
    }
    let Some(inner) = h.nodes.inner_container(node, "sequencer-0").await else {
        crate::log(format!("{ctx}: no inner sequencer-0 container on {node}"));
        return;
    };
    let tail = h
        .nodes
        .inner_logs(node, &inner, 40)
        .await
        .unwrap_or_default();
    crate::log(format!("{ctx}: log tail of {inner} on {node}\n{tail}"));
}

#[cfg(test)]
mod tests {
    use super::*;

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
