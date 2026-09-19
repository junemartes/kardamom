//! The whole-fleet cases: every replica of one role goes down at once.
//! No peer is left to lean on, so the sealers must come back from their
//! own logs and snapshots, and the executors from their own disks or
//! their own checkpoints. The load keeps submitting through the
//! outage and after it, so the verdict proves the chain accepts new
//! transactions again and receipts them correctly.

use std::time::Duration;

use crate::cases::cluster::sealer;
use crate::cases::component::{RESTORED, executor_containers, wait_peer_checkpoint, wipe_dirs};
use crate::evidence::CountWait;
use crate::harness::Harness;
use crate::nomad::Streams;
use crate::poll::{self, Budget};
use crate::probes::CLUSTER_TASK;

/// The three sealer nodes, by member id.
fn sealers(h: &Harness) -> anyhow::Result<Vec<String>> {
    (0..3).map(|id| sealer(h, id)).collect()
}

fn names(nodes: &[String]) -> Vec<&str> {
    nodes.iter().map(String::as_str).collect()
}

/// Kill all three sealer nodes. No member is left, so the pipeline
/// must stall. Then every node returns with its own log and snapshots,
/// the members elect a leader among themselves, and the backlog
/// drains. The load window outlasts the return, so the verdict covers
/// transactions submitted to the recovered cluster.
pub(crate) async fn cluster_total_loss_recover(h: &mut Harness) -> anyhow::Result<()> {
    let ctx = "cluster-total-loss-recover";
    let nodes = sealers(h)?;
    crate::log(format!(
        "{ctx}: docker kill ALL sealer nodes ({}) → no member left",
        nodes.join(" ")
    ));
    h.kill_nodes(&names(&nodes)).await?;
    h.assert_executor_stalled(Duration::from_secs(15)).await?;
    crate::log(format!("{ctx}: docker start all three sealer nodes"));
    h.start_nodes(&nodes).await?;
    h.assert_count(CLUSTER_TASK, 3, h.knobs.reschedule_slo)
        .await?;
    let leader = h.evidence.cluster_leader(h.knobs.leader_slo).await?;
    crate::log(format!("{ctx}: members elected memberId={leader}"));
    h.assert_executor_progress(Duration::from_secs(180)).await
}

/// Kill all three executor nodes. Every exporter goes dark, so the
/// outage is real and not a survivor answering for the fleet. Then the
/// nodes return, each executor resumes from its own state directory,
/// and the fleet catches up on the backlog the sealers kept ordering.
pub(crate) async fn executor_fleet_loss_recover(h: &mut Harness) -> anyhow::Result<()> {
    let ctx = "executor-fleet-loss-recover";
    let nodes = executor_containers(h);
    crate::log(format!(
        "{ctx}: docker kill ALL executor nodes ({})",
        nodes.join(" ")
    ));
    h.kill_nodes(&names(&nodes)).await?;
    await_exporters_dark(h, ctx).await?;
    crate::log(format!("{ctx}: docker start all three executor nodes"));
    h.start_nodes(&nodes).await?;
    h.assert_count("executor", 3, h.knobs.reschedule_slo)
        .await?;
    await_exporter_back(h, ctx).await?;
    h.assert_executor_progress(Duration::from_secs(180)).await
}

/// Kill all three executor nodes and wipe every state database. Each
/// node keeps its own checkpoints, and no peer is live to serve one,
/// so every executor must restore from its local checkpoint and replay
/// the tail. Three restore lines prove that no executor re-synced from
/// genesis or waited for a peer.
pub(crate) async fn executor_fleet_wipe_recover(h: &mut Harness) -> anyhow::Result<()> {
    let ctx = "executor-fleet-wipe-recover";
    let nodes = executor_containers(h);
    for node in &nodes {
        wait_peer_checkpoint(h, node, ctx).await?;
    }
    let baseline = h
        .evidence
        .count_lines("executor", RESTORED, Streams::Both)
        .await?;
    crate::log(format!(
        "{ctx}: docker kill ALL executor nodes ({}) and wipe every state DB (checkpoints kept)",
        nodes.join(" ")
    ));
    h.kill_nodes(&names(&nodes)).await?;
    await_exporters_dark(h, ctx).await?;
    for node in &nodes {
        wipe_dirs(h, node, ctx, "rm -rf /opt/kardamom/state/*").await?;
    }
    h.start_nodes(&nodes).await?;
    h.assert_count("executor", 3, h.knobs.reschedule_slo)
        .await?;
    await_exporter_back(h, ctx).await?;
    h.evidence
        .wait_count_reaches(
            &CountWait {
                job: "executor",
                needle: RESTORED,
                baseline,
                timeout: Duration::from_secs(180),
                interval: Duration::from_secs(6),
                streams: Streams::Both,
                fail_msg: "executor-fleet-wipe-recover: not every executor restored from its local checkpoint",
            },
            baseline + nodes.len(),
        )
        .await?;
    crate::log(format!(
        "{ctx}: all {} executors restored from their own checkpoints",
        nodes.len()
    ));
    h.assert_executor_progress(Duration::from_secs(180)).await
}

/// Wait until no executor exporter answers. A node kill that leaves
/// one exporter up proves nothing about the fleet.
async fn await_exporters_dark(h: &Harness, ctx: &str) -> anyhow::Result<()> {
    let outcome = poll::until(Budget::secs(60, 3), |_| async move {
        Ok(all_exporters_dark(h).await.then_some(()))
    })
    .await?;
    let ((), elapsed) = outcome.or_fail(|t| {
        crate::chaos_fail!(
            "{ctx}: an executor exporter still answers {}s after the fleet kill — outage not observed",
            t.as_secs()
        )
    })?;
    crate::log(format!(
        "{ctx}: outage observed (every executor exporter dark after {}s)",
        elapsed.as_secs()
    ));
    Ok(())
}

/// Wait until an executor exporter answers again. A returned node's
/// allocation runs before its exporter binds, and the progress check
/// needs a baseline from a live exporter.
async fn await_exporter_back(h: &Harness, ctx: &str) -> anyhow::Result<()> {
    let outcome = poll::until(
        Budget::new(h.knobs.reschedule_slo, Duration::from_secs(3)),
        |_| async move { Ok(h.probes.executor_progress().await.map(|_| ())) },
    )
    .await?;
    let ((), elapsed) = outcome.or_fail(|t| {
        crate::chaos_fail!(
            "{ctx}: no executor exporter answers {}s after the fleet returned",
            t.as_secs()
        )
    })?;
    crate::log(format!(
        "{ctx}: an executor exporter answers again after {}s",
        elapsed.as_secs()
    ));
    Ok(())
}

async fn all_exporters_dark(h: &Harness) -> bool {
    let mut dark = true;
    for i in 0..h.probes.executors.len() {
        dark &= h.probes.exec_metrics(i).await.is_none();
    }
    dark
}
