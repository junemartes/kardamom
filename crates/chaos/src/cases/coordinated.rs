//! The coordinated cases: failures that cross the redundancy of one
//! role, or cross the roles. Both ingresses die at once, both replicas
//! of one sequencer lane die at once, and every pipeline node dies at
//! once. The single-replica cases prove that a twin covers a loss;
//! these prove the recovery when no twin is left. The recovery probe
//! that ends every case then submits through both ingresses, with the
//! case's sender and with a fresh one.

use std::time::Duration;

use crate::cases::fleet::{await_exporter_back, await_exporters_dark};
use crate::harness::Harness;
use crate::probes::CLUSTER_TASK;

/// The gauge of buffered refs below their sender's floor. It is zero
/// unless a restart or a rewind stranded a ref.
const REF_BELOW_FLOOR: &str = "kardamom_sequencer_ref_below_floor";

/// The jobs a blackout takes down, with their allocation counts.
const PIPELINE_JOBS: [(&str, usize); 6] = [
    (CLUSTER_TASK, 3),
    ("executor", 3),
    ("sequencer", 4),
    ("ingress", 2),
    ("redis", 5),
    ("state-mirror", 3),
];

/// Hard-kill both ingress tasks. The whole client edge is gone: no
/// submit lands and no receipt is served. Nomad restarts both, both
/// exporters must answer, and the pipeline must progress. The probe
/// then submits through each ingress.
pub(crate) async fn ingress_pair_loss_recover(h: &mut Harness) -> anyhow::Result<()> {
    let ctx = "ingress-pair-loss-recover";
    let nodes: Vec<String> = h
        .probes
        .ingresses
        .iter()
        .map(|n| n.container.clone())
        .collect();
    crate::log(format!(
        "{ctx}: hard-kill BOTH ingress tasks ({})",
        nodes.join(" ")
    ));
    for node in &nodes {
        h.inject_hard(&[node], "ingress").await?;
    }
    h.assert_count("ingress", nodes.len(), h.knobs.restart_slo)
        .await?;
    h.assert_ingress_pair_live(ctx).await?;
    h.assert_progress().await
}

/// Hard-kill both replicas of sequencer lane 0, one on each sequencer
/// node. No twin keeps the lane: its senders are unordered until a
/// replica returns with empty state and learns each sender's floor
/// from the executors. Both replicas must come back healthy, no ref may
/// sit below a floor, and the pipeline must progress. The load is
/// pinned to shard 0, so the case's sender rides the killed lane.
pub(crate) async fn sequencer_lane_loss_recover(h: &mut Harness) -> anyhow::Result<()> {
    let ctx = "sequencer-lane-loss-recover";
    let nodes: Vec<String> = h
        .probes
        .sequencers
        .iter()
        .map(|n| n.container.clone())
        .collect();
    crate::log(format!(
        "{ctx}: hard-kill BOTH replicas of lane 0 ({})",
        nodes.join(" ")
    ));
    for node in &nodes {
        h.inject_hard(&[node], "sequencer-0").await?;
    }
    h.assert_count("sequencer", 4, h.knobs.restart_slo).await?;
    for i in 0..nodes.len() {
        let target = h.probes.sequencer_lane0_target(i);
        h.assert_replica_healthy(&target, Duration::from_secs(90))
            .await?;
    }
    h.assert_progress().await?;
    assert_no_ref_below_floor(h, ctx).await
}

/// No replica of lane 0 holds a buffered ref below its sender's floor.
async fn assert_no_ref_below_floor(h: &Harness, ctx: &str) -> anyhow::Result<()> {
    for i in 0..h.probes.sequencers.len() {
        let stranded = h.probes.seq_lane0_metric(i, REF_BELOW_FLOOR).await;
        anyhow::ensure!(
            stranded == Some(0),
            "{}: {ctx}: lane 0's replica on sequencer node {i} reports {stranded:?} refs below a floor (need 0): a restart or a rewind stranded a ref, and its sender is stuck",
            crate::FAIL_PREFIX
        );
    }
    crate::log(format!("{ctx}: no ref below a floor on lane 0"));
    Ok(())
}

/// Kill every pipeline node at once: the ingresses, the sequencers, the
/// sealers, the executors and the aux node with the validator, the
/// batcher and the Redis primary. Only the control node stays, with the
/// orchestrator and the L1. Then start them all in one call and let the
/// services find each other, as an operator gets after a power loss.
/// Every job must return to its count, the sealers must elect a leader,
/// an executor exporter must answer, both ingresses must be live, and
/// the pipeline must progress.
pub(crate) async fn pipeline_blackout_recover(h: &mut Harness) -> anyhow::Result<()> {
    let ctx = "pipeline-blackout-recover";
    let nodes = h.nodes.pipeline_nodes().await?;
    crate::log(format!(
        "{ctx}: docker kill ALL {} pipeline nodes ({})",
        nodes.len(),
        nodes.join(" ")
    ));
    let names: Vec<&str> = nodes.iter().map(String::as_str).collect();
    h.kill_nodes(&names).await?;
    await_exporters_dark(h, ctx).await?;
    crate::log(format!("{ctx}: docker start all {} nodes", nodes.len()));
    h.start_nodes(&nodes).await?;
    for (job, count) in PIPELINE_JOBS {
        h.assert_count(job, count, h.knobs.reschedule_slo).await?;
    }
    let leader = h
        .evidence
        .cluster_leader(crate::cases::fleet::FULL_RESTART_ELECTION)
        .await?;
    crate::log(format!("{ctx}: members elected memberId={leader}"));
    await_exporter_back(h, ctx).await?;
    h.assert_ingress_pair_live(ctx).await?;
    h.assert_executor_progress(Duration::from_mins(3)).await
}
