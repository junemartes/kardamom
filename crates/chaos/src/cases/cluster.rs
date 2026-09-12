//! The clustered-sealer (Raft) cases. Progress is measured at the
//! executor, since the Java cluster node has no Prometheus endpoint:
//! the cluster commits blocks out its egress, and the executor applies
//! them.

use std::cell::RefCell;
use std::time::Duration;

use crate::evidence::CountWait;
use crate::harness::Harness;
use crate::nomad::Streams;
use crate::poll::{self, Budget};
use crate::probes::CLUSTER_TASK;

/// The sealer node that runs Raft member `id`.
fn sealer(h: &Harness, id: u32) -> anyhow::Result<String> {
    h.container(&format!("sealer-{id}"))
}

/// A memberId in 0..3 that is not the leader.
fn a_follower(leader: u32) -> u32 {
    (0..3).find(|m| *m != leader).unwrap_or(0)
}

/// Hard-kill the leader's cluster container. The quorum survives the
/// loss and keeps committing. The case checks progress, not a changed
/// memberId: a restarted leader can win the election again, since it
/// holds the most up-to-date log.
pub(crate) async fn leader_kill(h: &mut Harness) -> anyhow::Result<()> {
    let old = h.evidence.cluster_leader(h.knobs.leader_slo).await?;
    let node = sealer(h, old)?;
    crate::log(format!(
        "cluster-leader-kill: current leader memberId={old} on {node}; hard-killing its cluster container"
    ));
    h.inject_hard(&[&node], CLUSTER_TASK).await?;
    h.assert_executor_progress(Duration::from_secs(60)).await?;
    let now = h.evidence.cluster_leader(h.knobs.leader_slo).await.ok();
    crate::log(format!(
        "cluster-leader-kill: pipeline resumed committing after leader kill (now leader memberId={})",
        now.map_or("?".to_string(), |m| m.to_string())
    ));
    h.assert_count(CLUSTER_TASK, 3, h.knobs.restart_slo).await
}

/// Hard-kill a member that is not the leader. The quorum is unaffected,
/// so the pipeline keeps progressing. A snapshot must exist first: an
/// intact-directory restart is where the snapshot restore path runs,
/// and the restarted member must log that restore.
pub(crate) async fn follower_kill(h: &mut Harness) -> anyhow::Result<()> {
    let leader = h.evidence.cluster_leader(h.knobs.leader_slo).await?;
    let follower = a_follower(leader);
    crate::log(format!(
        "cluster-follower-kill: leader=memberId={leader}; waiting for a cluster snapshot"
    ));
    let hs: &Harness = h;
    let outcome = poll::until(Budget::secs(300, 10), |_| async move {
        Ok(hs
            .evidence
            .cluster_logs()
            .await?
            .contains("cluster SNAPSHOT triggered")
            .then_some(()))
    })
    .await?;
    outcome.or_fail(|t| {
        crate::chaos_fail!(
            "cluster-follower-kill: no snapshot within {}s — is the snapshot scheduler running?",
            t.as_secs()
        )
    })?;
    let needle = format!("sealer snapshot RESTORED memberId={follower}");
    let baseline = h
        .evidence
        .count_lines(CLUSTER_TASK, &needle, Streams::StdoutOnly)
        .await?;
    let node = sealer(h, follower)?;
    crate::log(format!(
        "cluster-follower-kill: snapshot present (member {follower} restore count {baseline}); killing FOLLOWER memberId={follower} on {node}"
    ));
    h.inject_hard(&[&node], CLUSTER_TASK).await?;
    h.assert_executor_progress(Duration::from_secs(60)).await?;
    let still = h.evidence.cluster_leader(h.knobs.leader_slo).await?;
    if still == leader {
        crate::log(format!(
            "cluster-follower-kill: leader unchanged (memberId={leader}) — quorum held"
        ));
    } else {
        crate::log(format!(
            "cluster-follower-kill: WARN leader changed ({leader} -> {still}); quorum still held, progress OK"
        ));
    }
    h.assert_count(CLUSTER_TASK, 3, h.knobs.restart_slo).await?;
    h.evidence
        .wait_count_gt(&CountWait {
            job: CLUSTER_TASK,
            needle: &needle,
            baseline,
            timeout: Duration::from_secs(180),
            interval: Duration::from_secs(10),
            streams: Streams::StdoutOnly,
            fail_msg: "cluster-follower-kill: restarted member never logged 'sealer snapshot RESTORED' — the snapshot restore path did not run on an intact-dir restart",
        })
        .await?;
    crate::log(format!(
        "cluster-follower-kill: member {follower} restored from snapshot on restart"
    ));
    Ok(())
}

/// The blank-member catch-up drill: a follower's cluster directory and
/// archive are wiped after its kill, so the restarted member owns
/// nothing and replays the leader's log from position 0. The proof is
/// positional: its latest post-wipe snapshot block must reach the head
/// observed at wipe time. A position that never moves is a join wedge,
/// not slow replay.
pub(crate) async fn member_rejoin(h: &mut Harness) -> anyhow::Result<()> {
    let leader = h.evidence.cluster_leader(h.knobs.leader_slo).await?;
    let follower = a_follower(leader);
    let fresh = format!("sealer state FRESH at genesis memberId={follower}");
    let f0 = h
        .evidence
        .count_lines(CLUSTER_TASK, &fresh, Streams::StdoutOnly)
        .await?;
    let head_at_wipe = h.probes.executor_progress().await.filter(|h| *h > 0).ok_or_else(|| {
        crate::chaos_fail!("cluster-member-rejoin: could not read the executor head before the wipe — refusing to run: the catch-up proof needs a real target position or it proves nothing")
    })?;
    let node = sealer(h, follower)?;
    crate::log(format!(
        "cluster-member-rejoin: leader=memberId={leader}; killing FOLLOWER memberId={follower} and WIPING its cluster + archive dirs"
    ));
    h.inject_hard(&[&node], CLUSTER_TASK).await?;
    h.nodes
        .exec(
            &node,
            "rm -rf /opt/kardamom/cluster/* /opt/kardamom/archive/*",
        )
        .await
        .map_err(|e| {
            crate::chaos_fail!(
                "cluster-member-rejoin: could not wipe memberId={follower} state: {e}"
            )
        })?;
    h.assert_executor_progress(Duration::from_secs(60)).await?;
    h.assert_count(CLUSTER_TASK, 3, h.knobs.restart_slo).await?;
    let track = RefCell::new(Catchup::default());
    let (hs, track_ref, fresh_ref): (&Harness, &RefCell<Catchup>, &str) = (h, &track, &fresh);
    let budget = Budget::new(hs.knobs.rejoin_slo, Duration::from_secs(10));
    let outcome = poll::until(budget, |_| async move {
        let logs = hs.evidence.cluster_logs().await?;
        let f1 = logs.lines().filter(|l| l.contains(fresh_ref)).count();
        let mut track = track_ref.borrow_mut();
        track.observe(catchup_block(&logs, follower), f1);
        Ok((f1 > f0 && track.block >= head_at_wipe).then_some(f1))
    })
    .await?;
    let track = track.into_inner();
    let (f1, elapsed) = outcome.or_fail(|t| track.failure(f0, head_at_wipe, t))?;
    let now = h.evidence.cluster_leader(h.knobs.leader_slo).await.ok();
    crate::log(format!(
        "cluster-member-rejoin: memberId={follower} rejoined blank via full log replay (fresh {f0}->{f1}, replayed to block {} >= head-at-wipe {head_at_wipe}, {}s); leader now memberId={}",
        track.block,
        elapsed.as_secs(),
        now.map_or("?".to_string(), |m| m.to_string())
    ));
    Ok(())
}

/// The replay position of a wiped member: the block of its latest
/// `snapshot TAKEN` line after its most recent `FRESH at genesis` line,
/// so pre-wipe history cannot satisfy the proof.
fn catchup_block(logs: &str, member: u32) -> i64 {
    let fresh = format!("FRESH at genesis memberId={member}");
    let taken = format!("snapshot TAKEN memberId={member}");
    let Some((_, after_fresh)) = logs.rsplit_once(&fresh) else {
        return 0;
    };
    after_fresh
        .lines()
        .filter(|l| l.contains(&taken))
        .filter_map(|l| l.split("block=").nth(1))
        .filter_map(|rest| {
            rest.chars()
                .take_while(char::is_ascii_digit)
                .collect::<String>()
                .parse()
                .ok()
        })
        .next_back()
        .unwrap_or(0)
}

/// The catch-up observations: the first non-zero position and whether
/// it ever moved, which tells a wedge from slow replay.
#[derive(Debug, Default)]
struct Catchup {
    block: i64,
    first: i64,
    moved: bool,
    fresh: usize,
}

impl Catchup {
    fn observe(&mut self, block: i64, fresh: usize) {
        self.block = block;
        self.fresh = fresh;
        if block > 0 && self.first == 0 {
            self.first = block;
        }
        self.moved |= block > self.first;
    }

    fn failure(&self, f0: usize, head: i64, t: Duration) -> anyhow::Error {
        let secs = t.as_secs();
        if self.fresh <= f0 {
            return crate::chaos_fail!(
                "cluster-member-rejoin: restarted member did not start blank (fresh-at-genesis count {f0} -> {}) — the wipe did not take, this run proved nothing about empty-state rejoin",
                self.fresh
            );
        }
        if !self.moved {
            return crate::chaos_fail!(
                "cluster-member-rejoin: blank member FROZE at block {} (head at wipe {head}) — its replay position never advanced once in {secs}s, so this is a JOIN WEDGE, not slow replay: the member stays INACTIVE after a partial replay while reporting healthy to Nomad",
                self.block
            );
        }
        crate::chaos_fail!(
            "cluster-member-rejoin: blank member replayed to block {} of head-at-wipe {head} in {secs}s — the position DID keep advancing, so this is slow catch-up: blank-member replay is O(lifetime log) and the log is never purged",
            self.block
        )
    }
}

/// Kill two whole sealer nodes. Nomad on those nodes is gone too, so
/// only one member is left and the quorum is lost: the pipeline must
/// stall with no false progress. Then one node returns, the quorum is
/// back, and progress resumes with no gaps.
pub(crate) async fn quorum_loss_recover(h: &mut Harness) -> anyhow::Result<()> {
    let victims = [sealer(h, 1)?, sealer(h, 2)?];
    crate::log(format!(
        "cluster-quorum-loss-recover: docker kill TWO sealer nodes ({} {}) → quorum lost (1/3 up)",
        victims[0], victims[1]
    ));
    h.kill_nodes(&[&victims[0], &victims[1]]).await?;
    h.assert_executor_stalled(Duration::from_secs(15)).await?;
    crate::log(format!(
        "cluster-quorum-loss-recover: docker start {} (quorum 2/3 returns)",
        victims[0]
    ));
    h.nodes
        .start(&victims[0])
        .await
        .map_err(|e| crate::chaos_fail!("could not restart node {}: {e}", victims[0]))?;
    h.assert_count(CLUSTER_TASK, 2, h.knobs.reschedule_slo)
        .await?;
    h.assert_executor_progress(Duration::from_secs(180)).await?;
    // Restore the second node too, so later cases see a 3/3 cluster.
    // Best effort, outside this case's SLO.
    let _ = h.nodes.start(&victims[1]).await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_catchup_block_counts_only_lines_after_the_last_fresh_start() {
        let logs = "sealer snapshot TAKEN memberId=1 block=90\n\
            sealer state FRESH at genesis memberId=1\n\
            sealer snapshot TAKEN memberId=1 block=30\n\
            sealer snapshot TAKEN memberId=2 block=120\n\
            sealer snapshot TAKEN memberId=1 block=60\n";
        assert_eq!(catchup_block(logs, 1), 60);
        assert_eq!(catchup_block(logs, 2), 0);
        let mut track = Catchup::default();
        track.observe(0, 1);
        track.observe(30, 2);
        assert!(!track.moved);
        track.observe(60, 2);
        assert!(track.moved);
    }
}
