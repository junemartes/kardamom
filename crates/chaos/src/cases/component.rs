//! The component cases: executor, ingress, and sequencer kills, the
//! node failure, and the checkpoint drills.

use std::time::Duration;

use crate::evidence::CountWait;
use crate::harness::Harness;
use crate::nomad::Streams;
use crate::poll::{self, Budget};

const RESTORED: &str = "restored state from checkpoint";
const FETCHED: &str = "fetched checkpoint from peer";

pub(crate) async fn graceful_executor(h: &mut Harness) -> anyhow::Result<()> {
    h.inject_graceful("executor").await?;
    h.assert_count("executor", 3, h.knobs.restart_slo).await
}

pub(crate) async fn hard_executor(h: &mut Harness) -> anyhow::Result<()> {
    let nodes = executor_containers(h);
    let refs: Vec<&str> = nodes.iter().map(String::as_str).collect();
    h.inject_hard(&refs, "executor").await?;
    h.assert_count("executor", 3, h.knobs.restart_slo).await
}

/// A count of 2, not 1: with a killed marker set, the replacement check
/// requires the killed replica to come back, instead of letting the
/// untouched peer satisfy `>= 1` on the first poll.
pub(crate) async fn graceful_ingress(h: &mut Harness) -> anyhow::Result<()> {
    h.inject_graceful("ingress").await?;
    h.assert_count("ingress", 2, h.knobs.restart_slo).await
}

/// The hard-kill victim rotates by run id: ingress is active/active and
/// symmetric, and a blast radius pinned to one replica would never
/// prove the twin can die.
pub(crate) async fn hard_ingress(h: &mut Harness) -> anyhow::Result<()> {
    let victim = h.container(&format!("ingress-{}", h.knobs.ingress_victim))?;
    h.inject_hard(&[&victim], "ingress").await?;
    h.assert_count("ingress", 2, h.knobs.restart_slo).await
}

/// Sequencers run two racing replicas per lane, one job group per lane.
/// A kill does not stall its lane: the twin keeps ordering, so these
/// cases also check live progress. The load is pinned to shard 0, and
/// the stop targets a `seq-0` allocation.
pub(crate) async fn graceful_sequencer(h: &mut Harness) -> anyhow::Result<()> {
    h.inject_graceful_group("sequencer", "seq-0").await?;
    h.assert_progress().await?;
    h.assert_count("sequencer", 4, h.knobs.restart_slo).await
}

/// The explicit task name matters: `sequencer` alone would match both
/// lane tasks on the node.
pub(crate) async fn hard_sequencer(h: &mut Harness) -> anyhow::Result<()> {
    let node = h.container("sequencer-0")?;
    h.inject_hard(&[&node], "sequencer-0").await?;
    h.assert_progress().await?;
    h.assert_count("sequencer", 4, h.knobs.restart_slo).await
}

/// Hard-kill lane 0's replica on node 0. Its twin on node 1 keeps the
/// lane live with no stall, the killed replica restarts to full
/// strength, and it comes back healthy.
pub(crate) async fn sequencer_replica_kill(h: &mut Harness) -> anyhow::Result<()> {
    let node = h.container("sequencer-0")?;
    h.inject_hard(&[&node], "sequencer-0").await?;
    h.assert_progress().await?;
    h.assert_count("sequencer", 4, h.knobs.restart_slo).await?;
    let target = h.probes.sequencer_lane0_target(0);
    h.assert_replica_healthy(&target, Duration::from_secs(90))
        .await
}

/// Kill the whole executor-2 node. With three executor nodes and
/// distinct hosts, the lost replica cannot reschedule to a peer, so the
/// fleet degrades to two and must keep progressing; bringing the node
/// back recovers three. The outage is observed first: the survivors
/// would satisfy a bare `>= 2` instantly.
pub(crate) async fn node_failure_executor(h: &mut Harness) -> anyhow::Result<()> {
    let victim = h.container("executor-2")?;
    crate::log(format!("node-failure: docker kill {victim} (whole node)"));
    h.kill_nodes(&[&victim]).await?;
    let hs: &Harness = h;
    let outcome = poll::until(Budget::secs(60, 3), |_| async move {
        Ok(hs.probes.exec_metrics(2).await.is_none().then_some(()))
    })
    .await?;
    let ((), dark_after) = outcome.or_fail(|t| {
        crate::chaos_fail!(
            "node-failure: executor-2's exporter still answering {}s after the node kill — outage not observed",
            t.as_secs()
        )
    })?;
    crate::log(format!(
        "node-failure: outage observed (executor-2 exporter dark after {}s)",
        dark_after.as_secs()
    ));
    h.assert_count("executor", 2, h.knobs.restart_slo).await?;
    h.assert_executor_progress(Duration::from_secs(180)).await?;
    crate::log(format!(
        "node-failure: docker start {victim} (node returns)"
    ));
    h.nodes
        .start(&victim)
        .await
        .map_err(|e| crate::chaos_fail!("could not restart node {victim}: {e}"))?;
    h.assert_count("executor", 3, h.knobs.reschedule_slo).await
}

/// The data-loss drill: wipe executor-0's state and checkpoints, then
/// restore one checkpoint from executor-1. Replicas are deterministic
/// state machines at the same block, so a peer checkpoint is a valid
/// restore source, and the restart replays only the tail instead of
/// re-syncing from genesis. The restore log line proves the path.
pub(crate) async fn state_checkpoint_restore(h: &mut Harness) -> anyhow::Result<()> {
    let victim = h.container("executor-0")?;
    let donor = h.container("executor-1")?;
    wait_peer_checkpoint(h, &donor, "state-checkpoint-restore").await?;
    let baseline = h
        .evidence
        .count_lines("executor", RESTORED, Streams::Both)
        .await?;
    crate::log(
        "state-checkpoint-restore: killing executor-0 + wiping its state DB and checkpoints",
    );
    h.inject_hard(&[&victim], "executor").await?;
    wipe_state(h, &victim, "state-checkpoint-restore").await?;
    crate::log("state-checkpoint-restore: re-replicating checkpoints from executor-1");
    copy_checkpoint(h, &donor, &victim, baseline).await?;
    h.assert_executor_progress(Duration::from_secs(180)).await?;
    h.assert_count("executor", 3, h.knobs.reschedule_slo)
        .await?;
    h.evidence
        .wait_count_gt(&CountWait {
            job: "executor",
            needle: RESTORED,
            baseline,
            timeout: Duration::from_secs(120),
            interval: Duration::from_secs(6),
            streams: Streams::Both,
            fail_msg: "state-checkpoint-restore: executor-0 did NOT restore from checkpoint — fell back to genesis re-sync",
        })
        .await?;
    crate::log(
        "state-checkpoint-restore: executor-0 restored from checkpoint + rejoined (no genesis re-sync)",
    );
    Ok(())
}

/// The full-resync drill: wipe executor-1's state and checkpoints and
/// let it repair itself. The cluster keeps a bounded canonical window,
/// so the restarted executor must fetch a peer checkpoint before its
/// first join, restore it, and resume from there. The victim differs
/// from the checkpoint-restore drill so the two stay independent.
pub(crate) async fn replay_window_resync(h: &mut Harness) -> anyhow::Result<()> {
    let victim = h.container("executor-1")?;
    let donor = h.container("executor-0")?;
    wait_peer_checkpoint(h, &donor, "replay-window-resync").await?;
    crate::log("replay-window-resync: killing executor-1 + wiping its state DB and checkpoints");
    h.inject_hard(&[&victim], "executor").await?;
    wipe_state(h, &victim, "replay-window-resync").await?;
    h.assert_executor_progress(Duration::from_secs(180)).await?;
    h.assert_count("executor", 3, h.knobs.reschedule_slo)
        .await?;
    let (hs, victim_ref): (&Harness, &str) = (h, &victim);
    let outcome = poll::until(Budget::secs(90, 5), |_| async move {
        Ok(self_heal_lines(hs, victim_ref)
            .await
            .filter(|(f, r)| *f && *r))
    })
    .await?;
    let (_, elapsed) = outcome.or_fail(|_| {
        crate::chaos_fail!("replay-window-resync: executor-1 did not fetch and restore a peer checkpoint within 90s (self-heal path not taken, or restore missing)")
    })?;
    crate::log(format!(
        "replay-window-resync: executor-1 self-healed from a peer checkpoint (fetch + restore + rejoin, {}s)",
        elapsed.as_secs()
    ));
    Ok(())
}

/// `(fetched, restored)` from the victim's current inner container log.
async fn self_heal_lines(h: &Harness, victim: &str) -> Option<(bool, bool)> {
    let inner = h.nodes.inner_container(victim, "executor").await?;
    let logs = h.nodes.inner_logs(victim, &inner, 100_000).await?;
    Some((logs.contains(FETCHED), logs.contains(RESTORED)))
}

fn executor_containers(h: &Harness) -> Vec<String> {
    h.probes
        .executors
        .iter()
        .map(|n| n.container.clone())
        .collect()
}

/// Wait for a checkpoint on a donor node. Donors checkpoint every 20s
/// while live, once the chain moves.
pub(crate) async fn wait_peer_checkpoint(h: &Harness, node: &str, ctx: &str) -> anyhow::Result<()> {
    crate::log(format!("{ctx}: waiting for a checkpoint on {node}"));
    let script = "ls /opt/kardamom/checkpoints/checkpoint-* >/dev/null 2>&1";
    let outcome = poll::until(Budget::secs(75, 5), |_| async move {
        Ok(h.nodes.exec_status(node, script).await?.then_some(()))
    })
    .await?;
    outcome
        .or_fail(|_| crate::chaos_fail!("{ctx}: {node} produced no checkpoint"))
        .map(|_| ())
}

async fn wipe_state(h: &Harness, node: &str, ctx: &str) -> anyhow::Result<()> {
    h.nodes
        .exec(
            node,
            "rm -rf /opt/kardamom/state/* /opt/kardamom/checkpoints/*",
        )
        .await
        .map(|_| ())
        .map_err(|e| crate::chaos_fail!("{ctx}: could not wipe {node} state: {e}"))
}

/// Copy one complete checkpoint from the donor. Visible checkpoint
/// directories are immutable; the retry covers the window where the
/// picked one gets pruned mid-copy. A restarted victim that self-heals
/// from a peer first satisfies the case's assertion with the same log
/// line, so the copy stops when the restore count moved.
async fn copy_checkpoint(
    h: &Harness,
    donor: &str,
    victim: &str,
    baseline: usize,
) -> anyhow::Result<()> {
    let outcome = poll::until(Budget::secs(6, 2), |_| async move {
        copy_checkpoint_once(h, donor, victim, baseline).await
    })
    .await?;
    outcome
        .or_fail(|_| crate::chaos_fail!("state-checkpoint-restore: checkpoint copy failed"))
        .map(|_| ())
}

async fn copy_checkpoint_once(
    h: &Harness,
    donor: &str,
    victim: &str,
    baseline: usize,
) -> anyhow::Result<Option<()>> {
    if h.evidence
        .count_lines("executor", RESTORED, Streams::Both)
        .await?
        > baseline
    {
        crate::log(
            "state-checkpoint-restore: executor-0 self-healed from a peer before the harness copy landed",
        );
        return Ok(Some(()));
    }
    let name = h
        .nodes
        .exec(donor, "ls -d /opt/kardamom/checkpoints/checkpoint-* 2>/dev/null | sort | tail -1 | xargs -rn1 basename")
        .await
        .unwrap_or_default();
    if name.is_empty() {
        return Ok(None);
    }
    h.nodes
        .exec(victim, "rm -rf /opt/kardamom/checkpoints/*")
        .await?;
    let tar = h
        .nodes
        .exec_bytes(
            donor,
            &format!("tar -C /opt/kardamom --warning=no-file-changed -cf - checkpoints/{name}"),
            1,
        )
        .await?;
    // Extract into a staging directory and rename into place, as the
    // checkpoint writer does: the restarted executor may start while the
    // copy is in flight, and a visible but partial checkpoint would be
    // restored torn.
    let staging = "/opt/kardamom/checkpoints/.harness-staging";
    h.nodes
        .exec_with_stdin(
            victim,
            &format!("rm -rf {staging} && mkdir -p {staging} && tar -C {staging} -xf -"),
            tar,
        )
        .await?;
    h.nodes
        .exec(
            victim,
            &format!("mv {staging}/checkpoints/{name} /opt/kardamom/checkpoints/{name} && rm -rf {staging}"),
        )
        .await?;
    let complete = h
        .nodes
        .exec_status(
            victim,
            &format!("test -s /opt/kardamom/checkpoints/{name}/MANIFEST && test -s /opt/kardamom/checkpoints/{name}/mdbx.dat"),
        )
        .await?;
    if !complete {
        crate::log(format!(
            "state-checkpoint-restore: copy of {name} incomplete (raced the writer's prune?); retrying"
        ));
    }
    Ok(complete.then_some(()))
}
