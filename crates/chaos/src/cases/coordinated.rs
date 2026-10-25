//! The coordinated cases: failures that cross the redundancy of one
//! role, or cross the roles. Both ingresses die at once, both replicas
//! of one sequencer lane die at once, and every pipeline node dies at
//! once. The single-replica cases prove that a twin covers a loss;
//! these prove the recovery when no twin is left. The recovery probe
//! that ends every case then submits through both ingresses, with the
//! case's sender and with a fresh one.

use std::time::Duration;

use crate::cases::da_watcher::assert_no_origin_gap;
use crate::cases::fleet::{await_exporter_back, await_exporters_dark};
use crate::harness::Harness;
use crate::probes::CLUSTER_TASK;

/// The gauge of buffered refs below their sender's floor. It is zero
/// unless a restart or a rewind stranded a ref. Only the v1 loop exports
/// it. See [`crate::knobs::SequencerLoop::can_strand_refs`].
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

/// The egress job of a carry-born cluster, with its allocation count (the
/// `egress_count` default of the egress job).
const EGRESS_JOB: (&str, usize) = ("egress", 2);

/// The sealer's line of a void decision, one for each member and entry.
const VOID_DECIDED: &str = "result=DECIDED";

/// Hard-kill both ingress tasks. The whole client edge is gone: no
/// submit lands and no receipt is served. Nomad restarts both, both
/// exporters must answer, and the pipeline must progress. The probe
/// then submits through each ingress.
pub(crate) async fn ingress_pair_loss_recover(h: &mut Harness) -> anyhow::Result<()> {
    let ctx = "ingress-pair-loss-recover";
    let nodes = h.probes.ingress_containers();
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
/// sit below a floor (v1 loop), and the pipeline must progress. The load is
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
/// The v2 loop cannot strand a ref and exports no such gauge, so the
/// check passes on it. The recovery probe then proves that the senders
/// send again.
pub(crate) async fn assert_no_ref_below_floor(h: &Harness, ctx: &str) -> anyhow::Result<()> {
    if !h.knobs.sequencer_loop.can_strand_refs() {
        crate::log(format!(
            "{ctx}: the v2 loop expires every transaction it holds, so no ref can sit below a floor"
        ));
        return Ok(());
    }
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
///
/// In reference mode, the kill can lose the transaction data of an entry
/// that the sealers already ordered. No consumer can execute that entry.
/// Each consumer asks the sealer to void it, and the chain moves again
/// only after the void record. The case prints the number of voided
/// entries as evidence. It does not assert the number: a kill between two
/// entries loses nothing. On a carry-born cluster, the sealer carries the
/// bytes of every entry and decides no void under decision version 5, so
/// the number must be 0, and the egress job must return too.
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
    let egress = h.knobs.genesis_carry.then_some(EGRESS_JOB);
    for (job, count) in PIPELINE_JOBS.into_iter().chain(egress) {
        h.assert_count(job, count, h.knobs.reschedule_slo).await?;
    }
    let elected = h
        .evidence
        .cluster_elected(crate::cases::fleet::FULL_RESTART_ELECTION)
        .await?;
    crate::log(format!(
        "{ctx}: members elected memberId={} (term {} closed on a majority)",
        elected.leader, elected.term
    ));
    await_exporter_back(h, ctx).await?;
    h.assert_ingress_pair_live(ctx).await?;
    h.assert_executor_progress(Duration::from_mins(3)).await?;
    // The da-watcher died with the sealers. An epoch it published but the
    // sealers never committed is published again: from the sealer's origin
    // at the restart, or by the re-publish of the unconfirmed epochs. The
    // sealer's origin then reaches the da-watcher's last publish.
    assert_no_origin_gap(h, ctx).await?;
    let voided = h
        .evidence
        .count_lines(
            CLUSTER_TASK,
            VOID_DECIDED,
            crate::nomad::Streams::StdoutOnly,
        )
        .await?;
    crate::log(format!(
        "{ctx}: void decisions in the sealer logs: {voided} (one line per member per entry)"
    ));
    anyhow::ensure!(
        !h.knobs.genesis_carry || voided == 0,
        "{}: {ctx}: the sealer decided {voided} voids under decision version 5",
        crate::FAIL_PREFIX
    );
    Ok(())
}
