//! The component cases: executor, ingress, and sequencer kills, the
//! node failure, and the checkpoint drills.

use std::time::Duration;

use super::cluster::a_follower;
use super::exec_stream::ExecStreamSessions;
use super::recorded_cursor::BestRecorded;
use crate::evidence::CountWait;
use crate::harness::Harness;
use crate::nomad::Streams;
use crate::poll::{self, Budget};
use crate::probes::CLUSTER_TASK;
use kardamom_state::checkpoint::replay::LOG_DIR;

pub(crate) const RESTORED: &str = "restored state from checkpoint";
pub(crate) const FETCHED: &str = "fetched checkpoint from peer";
/// The block log of a peer after its base, with `--checkpoint-log`.
const LOG_FETCHED: &str = "fetched the block log from a peer";
/// The block log applied after a restored base, with `--checkpoint-log`.
const LOG_APPLIED: &str = "applied the block log after the base checkpoint";

/// The restarted executor must also record a new session of its stream
/// and move its recorded cursor (see [`ExecStreamSessions`]).
pub(crate) async fn graceful_executor(h: &mut Harness) -> anyhow::Result<()> {
    let sessions = ExecStreamSessions::read(h).await;
    h.inject_graceful("executor").await?;
    h.assert_count("executor", 3, h.knobs.restart_slo).await?;
    sessions
        .assert_restart_records(h, "graceful-executor")
        .await
}

/// While the killed executor is down, the sealer's best recorded cursor
/// keeps moving, or on a carry-born cluster the sealer holds no recorded
/// cursor at all (see [`BestRecorded`]). The restarted executor must also
/// record a new session of its stream and move its recorded cursor (see
/// [`ExecStreamSessions`]).
pub(crate) async fn hard_executor(h: &mut Harness) -> anyhow::Result<()> {
    let ctx = "hard-executor";
    let sessions = ExecStreamSessions::read(h).await;
    let best = BestRecorded::read(h, ctx).await?;
    let nodes = executor_containers(h);
    let refs: Vec<&str> = nodes.iter().map(String::as_str).collect();
    h.inject_hard(&refs, "executor").await?;
    best.assert_moves(h, ctx).await?;
    h.assert_count("executor", 3, h.knobs.restart_slo).await?;
    sessions.assert_restart_records(h, ctx).await
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

/// The index of the executor whose node a node drill takes: the executor
/// of a follower member. On the sealer nodes, the node of executor `i`
/// also runs member `i`, so the drill also takes a member, and a follower
/// keeps the election out of it.
async fn node_victim(h: &Harness) -> anyhow::Result<usize> {
    let leader = h.evidence.cluster_leader(h.knobs.leader_slo).await?;
    Ok(usize::try_from(a_follower(leader))?)
}

/// Kill the whole node of one executor. With three executor nodes and
/// distinct hosts, the lost replica cannot reschedule to a peer, so the
/// fleet degrades to two and must keep progressing; bringing the node
/// back recovers three, and the member on a sealer node returns too. The
/// outage is observed first: the survivors would satisfy a bare `>= 2`
/// instantly.
pub(crate) async fn node_failure_executor(h: &mut Harness) -> anyhow::Result<()> {
    let i = node_victim(h).await?;
    let victim = h.probes.executors[i].container.clone();
    crate::log(format!(
        "node-failure: docker kill {victim} (whole node of executor {i})"
    ));
    h.kill_nodes(&[&victim]).await?;
    let hs: &Harness = h;
    let outcome = poll::until(Budget::secs(60, 3), |_| async move {
        Ok(hs.probes.exec_metrics(i).await.is_none().then_some(()))
    })
    .await?;
    let ((), dark_after) = outcome.or_fail(|t| {
        crate::chaos_fail!(
            "node-failure: executor {i}'s exporter still answering {}s after the node kill — outage not observed",
            t.as_secs()
        )
    })?;
    crate::log(format!(
        "node-failure: outage observed (executor {i} exporter dark after {}s)",
        dark_after.as_secs()
    ));
    h.assert_count("executor", 2, h.knobs.restart_slo).await?;
    h.assert_executor_progress(Duration::from_mins(3)).await?;
    crate::log(format!(
        "node-failure: docker start {victim} (node returns)"
    ));
    h.nodes
        .start(&victim)
        .await
        .map_err(|e| crate::chaos_fail!("could not restart node {victim}: {e}"))?;
    h.assert_count("executor", 3, h.knobs.reschedule_slo)
        .await?;
    h.assert_count(CLUSTER_TASK, 3, h.knobs.reschedule_slo)
        .await
}

/// The machine-replacement drill: the node of one executor is replaced
/// the way a cloud provider replaces a server. The container comes back
/// with a higher generation, on another address and with empty volumes,
/// and gets the substrate play a new machine gets. Consul must forget the
/// old record, Nomad must place the lost executor on the new client, and
/// the executor must catch up from nothing: the chain resolves the node
/// by name, so nothing but the address plan of the Terraform root moves.
/// On the sealer nodes, the member of the node must return too. A
/// restarted node (`node-failure-executor`) keeps its address and its
/// disks; this case is the path that loses both.
pub(crate) async fn node_replace_executor(h: &mut Harness) -> anyhow::Result<()> {
    let node = h.knobs.executor_nodes.node(node_victim(h).await?);
    h.replace_node(&node, "node-replace").await?;
    h.assert_count("executor", 3, h.knobs.reschedule_slo)
        .await?;
    h.assert_count(CLUSTER_TASK, 3, h.knobs.reschedule_slo)
        .await?;
    h.assert_executor_progress(Duration::from_mins(3)).await?;
    crate::log(format!(
        "node-replace: executor placed on the new {node}; waiting for it to catch up from empty disks"
    ));
    h.assert_executors_converged("node-replace").await
}

/// The data-loss drill: wipe executor 0's state and checkpoints, then
/// restore one checkpoint from executor 1. Replicas are deterministic
/// state machines at the same block, so a peer checkpoint is a valid
/// restore source, and the restart replays only the tail instead of
/// re-syncing from genesis. The restore log line proves the path. The
/// state paths are the same on an executor node and on a sealer node.
pub(crate) async fn state_checkpoint_restore(h: &mut Harness) -> anyhow::Result<()> {
    let victim = h.probes.executors[0].container.clone();
    let donor = h.probes.executors[1].container.clone();
    wait_peer_checkpoint(h, &donor, "state-checkpoint-restore").await?;
    let baseline = h
        .evidence
        .count_lines("executor", RESTORED, Streams::Both)
        .await?;
    crate::log(
        "state-checkpoint-restore: killing executor 0 + wiping its state DB and checkpoints",
    );
    h.inject_hard(&[&victim], "executor").await?;
    wipe_state(h, &victim, "state-checkpoint-restore").await?;
    crate::log("state-checkpoint-restore: re-replicating checkpoints from executor 1");
    copy_checkpoint(h, &donor, &victim, baseline).await?;
    h.assert_executor_progress(Duration::from_mins(3)).await?;
    h.assert_count("executor", 3, h.knobs.reschedule_slo)
        .await?;
    h.evidence
        .wait_count_gt(&CountWait {
            job: "executor",
            needle: RESTORED,
            baseline,
            timeout: Duration::from_mins(2),
            interval: Duration::from_secs(6),
            streams: Streams::Both,
            fail_msg: "state-checkpoint-restore: executor 0 did NOT restore from checkpoint — fell back to genesis re-sync",
        })
        .await?;
    crate::log(
        "state-checkpoint-restore: executor 0 restored from checkpoint + rejoined (no genesis re-sync)",
    );
    Ok(())
}

/// The full-resync drill: wipe executor 1's state and checkpoints and
/// let it repair itself. The cluster keeps a bounded canonical window,
/// so the restarted executor must fetch a peer checkpoint before its
/// first join, restore it, and resume from there. A carry executor does
/// the same: the restore of a fresh state directory runs before it opens
/// the member feed or the egress. The victim differs from the
/// checkpoint-restore drill so the two stay independent.
pub(crate) async fn replay_window_resync(h: &mut Harness) -> anyhow::Result<()> {
    let victim = h.probes.executors[1].container.clone();
    let donor = h.probes.executors[0].container.clone();
    wait_peer_checkpoint(h, &donor, "replay-window-resync").await?;
    crate::log("replay-window-resync: killing executor 1 + wiping its state DB and checkpoints");
    h.inject_hard(&[&victim], "executor").await?;
    wipe_state(h, &victim, "replay-window-resync").await?;
    h.assert_executor_progress(Duration::from_mins(3)).await?;
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
        crate::chaos_fail!("replay-window-resync: executor 1 did not fetch and restore a peer checkpoint within 90s (self-heal path not taken, or restore missing)")
    })?;
    crate::log(format!(
        "replay-window-resync: executor 1 self-healed from a peer checkpoint (fetch + restore + rejoin, {}s)",
        elapsed.as_secs()
    ));
    assert_log_applied(h, &victim).await
}

/// With the block log on, the victim also fetched the log of the peer and
/// applied it after the base: the restore reached past the base.
async fn assert_log_applied(h: &Harness, victim: &str) -> anyhow::Result<()> {
    if !h.knobs.checkpoint_log {
        return Ok(());
    }
    let logs = match h.nodes.inner_container(victim, "executor").await {
        Some(inner) => h.nodes.inner_logs(victim, &inner, 100_000).await,
        None => None,
    }
    .unwrap_or_default();
    let missing: Vec<&str> = [LOG_FETCHED, LOG_APPLIED]
        .into_iter()
        .filter(|line| !logs.contains(line))
        .collect();
    if !missing.is_empty() {
        return Err(crate::chaos_fail!(
            "replay-window-resync: the block log is on, but {victim} logged no {missing:?}"
        ));
    }
    crate::log("replay-window-resync: executor 1 fetched the block log of the peer and applied it");
    Ok(())
}

/// `(fetched, restored)` from the victim's current inner container log.
async fn self_heal_lines(h: &Harness, victim: &str) -> Option<(bool, bool)> {
    let inner = h.nodes.inner_container(victim, "executor").await?;
    let logs = h.nodes.inner_logs(victim, &inner, 100_000).await?;
    Some((logs.contains(FETCHED), logs.contains(RESTORED)))
}

pub(crate) fn executor_containers(h: &Harness) -> Vec<String> {
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

/// Wipe the state DB and the checkpoints of `node`.
async fn wipe_state(h: &Harness, node: &str, ctx: &str) -> anyhow::Result<()> {
    wipe_dirs(
        h,
        node,
        ctx,
        "rm -rf /opt/kardamom/state/* /opt/kardamom/checkpoints/*",
    )
    .await
}

/// Run the wipe `script` on `node`.
pub(crate) async fn wipe_dirs(
    h: &Harness,
    node: &str,
    ctx: &str,
    script: &str,
) -> anyhow::Result<()> {
    h.nodes
        .exec(node, script)
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
            "state-checkpoint-restore: executor 0 self-healed from a peer before the harness copy landed",
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
    // The donor's writer can prune the picked checkpoint while tar reads
    // it ("Cannot stat"). That is the same race as a torn copy, so it
    // takes the same retry with a fresh pick.
    let copy = CheckpointCopy {
        name,
        log: h.knobs.checkpoint_log,
    };
    let tar = match h
        .nodes
        .exec_bytes(
            donor,
            &format!(
                "tar -C /opt/kardamom --warning=no-file-changed -cf - {}",
                copy.sources()
            ),
            1,
        )
        .await
    {
        Ok(tar) => tar,
        Err(e) => {
            crate::log(format!(
                "state-checkpoint-restore: read of {} on the donor failed (pruned mid-copy?); retrying: {e}",
                copy.name
            ));
            return Ok(None);
        }
    };
    // Extract into a staging directory and rename into place, as the
    // checkpoint writer does: the restarted executor may start while the
    // copy is in flight, and a visible but partial checkpoint would be
    // restored torn.
    h.nodes
        .exec_with_stdin(
            victim,
            &format!("rm -rf {STAGING} && mkdir -p {STAGING} && tar -C {STAGING} -xf -"),
            tar,
        )
        .await?;
    h.nodes.exec(victim, &copy.install()).await?;
    let name = copy.name;
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

/// The staging directory of a checkpoint copy on the victim.
const STAGING: &str = "/opt/kardamom/checkpoints/.harness-staging";

/// One checkpoint copy from the donor to the victim: the newest base
/// `name`, and with the block log on, the log next to it. With the log
/// on, a base is rare, so the newest base can be many blocks old. The
/// log carries the blocks after the base. Without it, the victim
/// restores at the old base and executes the full tail.
struct CheckpointCopy {
    name: String,
    log: bool,
}

impl CheckpointCopy {
    /// The paths under `/opt/kardamom` that the donor sends.
    fn sources(&self) -> String {
        let base = format!("checkpoints/{}", self.name);
        if self.log {
            format!("{base} checkpoints/{LOG_DIR}")
        } else {
            base
        }
    }

    /// The script that moves the staged copy into place on the victim.
    /// The log moves first. A restore reads the log when the base
    /// becomes visible, so the base must not come before the log. A
    /// reader of the log stops at a torn last record, so a log copied
    /// during an append is a valid log.
    fn install(&self) -> String {
        let base = format!(
            "mv {STAGING}/checkpoints/{name} /opt/kardamom/checkpoints/{name}",
            name = self.name
        );
        let log =
            format!("mv {STAGING}/checkpoints/{LOG_DIR} /opt/kardamom/checkpoints/{LOG_DIR} && ");
        let first = if self.log { log.as_str() } else { "" };
        format!("{first}{base} && rm -rf {STAGING}")
    }
}

#[cfg(test)]
mod tests {
    use super::CheckpointCopy;

    #[test]
    fn a_copy_with_the_log_sends_the_log_and_installs_it_before_the_base() {
        let copy = CheckpointCopy {
            name: "checkpoint-000000000000000089".to_string(),
            log: true,
        };
        assert_eq!(
            copy.sources(),
            "checkpoints/checkpoint-000000000000000089 checkpoints/log"
        );
        let install = copy.install();
        let log = install.find("checkpoints/log /opt").unwrap();
        let base = install
            .find("checkpoints/checkpoint-000000000000000089 /opt")
            .unwrap();
        assert!(
            log < base,
            "the log must be in place before the base: {install}"
        );
    }

    #[test]
    fn a_copy_without_the_log_sends_only_the_base() {
        let copy = CheckpointCopy {
            name: "checkpoint-000000000000000089".to_string(),
            log: false,
        };
        assert_eq!(copy.sources(), "checkpoints/checkpoint-000000000000000089");
        assert!(!copy.install().contains("/log"), "{}", copy.install());
    }
}
