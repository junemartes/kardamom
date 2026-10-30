//! The clustered-sealer (Raft) cases. Progress is measured at the
//! executor, since the Java cluster node has no Prometheus endpoint:
//! the cluster commits blocks out its egress, and the executor applies
//! them.

use std::cell::RefCell;
use std::collections::HashMap;
use std::time::Duration;

use super::member_storage::MemberStorage;
use crate::evidence::CountWait;
use crate::harness::Harness;
use crate::nomad::Streams;
use crate::nomad::{Nomad, SavedJob};
use crate::poll::{self, Budget};
use crate::probes::CLUSTER_TASK;

/// The sealer node that runs Raft member `id`.
pub(crate) fn sealer(h: &Harness, id: u32) -> anyhow::Result<String> {
    h.container(&format!("sealer-{id}"))
}

/// A memberId in 0..3 that is not the leader.
pub(crate) fn a_follower(leader: u32) -> u32 {
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
    h.assert_executor_progress(Duration::from_mins(1)).await?;
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
    h.assert_executor_progress(Duration::from_mins(1)).await?;
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
            timeout: Duration::from_mins(3),
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

/// How long the rejoin drill waits for the leader's first log purge. The
/// purge needs four snapshots (the three kept and the purge point), one
/// archive segment of log below the purge point, and a posted head past
/// the purge point's block.
const PURGE_WAIT: Duration = Duration::from_mins(6);

/// The blank-member catch-up drill: a follower's cluster directory and
/// Raft archive are wiped after its kill, so the restarted member owns
/// nothing. The drill first waits until the leader has purged its log,
/// so a replay from position 0 is impossible. The member seeds from a
/// peer's latest snapshot and catches up from the leader. The proof is
/// positional: its latest post-wipe snapshot block must reach the head
/// observed at wipe time. A position that never moves is a join wedge,
/// not slow replay.
pub(crate) async fn member_rejoin(h: &mut Harness) -> anyhow::Result<()> {
    let ctx = "cluster-member-rejoin";
    let leader = h.evidence.cluster_leader(h.knobs.leader_slo).await?;
    await_log_purge(h, leader, ctx).await?;
    let follower = a_follower(leader);
    let before = Blank::observe(h, follower, ctx).await?;
    let node = sealer(h, follower)?;
    crate::log(format!(
        "{ctx}: leader=memberId={leader}; killing FOLLOWER memberId={follower} and WIPING its cluster + Raft archive state"
    ));
    h.inject_hard(&[&node], CLUSTER_TASK).await?;
    MemberStorage::wipe(h, &node, ctx).await?;
    h.assert_executor_progress(Duration::from_mins(1)).await?;
    h.assert_count(CLUSTER_TASK, 3, h.knobs.restart_slo).await?;
    before.await_caught_up(h).await
}

/// A full restart keeps only the current leader's persistent Raft data.
/// An offline copy to one blank member restores a quorum. The remaining
/// blank member then seeds from that quorum.
pub(crate) async fn two_blank_recover(h: &mut Harness) -> anyhow::Result<()> {
    let ctx = "cluster-two-blank-recover";
    let leader = h.evidence.cluster_leader(h.knobs.leader_slo).await?;
    await_log_purge(h, leader, ctx).await?;
    let mut blank = Vec::new();
    for member in (0..3).filter(|member| *member != leader) {
        blank.push(Blank::observe(h, member, ctx).await?);
    }
    let copied_member = blank[0].member;
    blank[0] =
        Blank::observe_start(h, copied_member, ctx, BlankStart::copied(copied_member)).await?;
    let job = SavedJob::capture(&h.nomad, CLUSTER_TASK).await?;
    let stopped = job.stop().await;
    let wiped = match stopped {
        Ok(()) => {
            async {
                Blank::wipe_all(h, &blank, ctx).await?;
                MemberStorage::copy_stopped(h, &sealer(h, leader)?, &sealer(h, copied_member)?, ctx)
                    .await
            }
            .await
        }
        Err(error) => Err(error),
    };
    let restored = job.restore().await;
    if let Err(error) = &restored {
        crate::log(format!("{ctx}: restoring the cluster job fails: {error:#}"));
    }
    let report = serde_json::json!({
        "case": ctx, "surviving_data_member": leader,
        "blank_members": blank.iter().map(|proof| proof.member).collect::<Vec<_>>(),
        "offline_copy_member": copied_member,
        "target_blocks": blank.iter().map(|proof| proof.head).collect::<Vec<_>>(),
        "fault": "all member tasks stopped; two member directories and local seeds erased; survivor copied offline to one wiped member with file hashes verified; shared executor archives preserved",
        "wipe_failure": wiped.as_ref().err().map(|error| format!("{error:#}")),
        "restore_failure": restored.as_ref().err().map(|error| format!("{error:#}")),
    });
    let saved = std::fs::write(
        "/tmp/chaos-cluster-two-blank-recover-fault.json",
        serde_json::to_vec_pretty(&report)?,
    );
    wiped?;
    restored?;
    saved?;
    h.assert_count(CLUSTER_TASK, 3, h.knobs.restart_slo).await?;
    h.evidence
        .cluster_elected(super::fleet::FULL_RESTART_ELECTION)
        .await?;
    for member in blank {
        member.await_caught_up(h).await?;
    }
    h.assert_progress().await
}

/// Wait until `leader` logs a purge of its Raft log. The purge lines stay
/// in the logs of the leader's allocation, so an earlier purge counts.
async fn await_log_purge(h: &Harness, leader: u32, ctx: &str) -> anyhow::Result<()> {
    let needle = format!("cluster LOG PURGED memberId={leader} ");
    let (hs, needle_ref) = (h, &needle);
    let outcome = poll::until(
        Budget::new(PURGE_WAIT, Duration::from_secs(3)),
        |_| async move {
            Ok(hs
                .evidence
                .cluster_logs()
                .await?
                .contains(needle_ref.as_str())
                .then_some(()))
        },
    )
    .await?;
    let ((), elapsed) = outcome.or_fail(|t| {
        crate::chaos_fail!(
            "{ctx}: leader memberId={leader} logged no '{}' within {}s — without a purged log the drill cannot prove a rejoin through a peer seed",
            needle.trim_end(),
            t.as_secs()
        )
    })?;
    crate::log(format!(
        "{ctx}: leader memberId={leader} has purged its Raft log ({}s); a replay from position 0 is impossible",
        elapsed.as_secs()
    ));
    Ok(())
}

/// The machine-replacement drill for a Raft member: a follower's sealer
/// node is replaced through the Terraform root, on another address with
/// empty volumes. The other members name it by its Consul node record,
/// so they must reach the new address; the quorum of two keeps
/// committing meanwhile; Nomad places the member on the new node; and
/// the blank member must replay to the head observed before the
/// replacement, the same positional proof as `cluster-member-rejoin`.
pub(crate) async fn node_replace_sealer(h: &mut Harness) -> anyhow::Result<()> {
    let ctx = "node-replace-sealer";
    let leader = h.evidence.cluster_leader(h.knobs.leader_slo).await?;
    let follower = a_follower(leader);
    let before = Blank::observe(h, follower, ctx).await?;
    crate::log(format!(
        "{ctx}: leader=memberId={leader}; replacing the node of FOLLOWER memberId={follower}"
    ));
    h.replace_node(&format!("sealer-{follower}"), ctx).await?;
    h.assert_executor_progress(Duration::from_mins(1)).await?;
    h.assert_count(CLUSTER_TASK, 3, h.knobs.reschedule_slo)
        .await?;
    before.await_caught_up(h).await
}

/// The state a member recovery proof starts from: the member, the count of
/// its expected start lines per allocation, and the executor head,
/// read before the member loses its state. The count is kept per
/// allocation because a member restarted in place logs into its own
/// allocation, while a member on a replaced node logs into a new one and
/// the lost allocation's logs are gone with the node.
struct Blank {
    ctx: &'static str,
    member: u32,
    start: BlankStart,
    before: HashMap<String, usize>,
    head: i64,
}

impl Blank {
    async fn wipe_all(h: &Harness, blank: &[Self], ctx: &str) -> anyhow::Result<()> {
        for member in blank {
            MemberStorage::wipe(h, &sealer(h, member.member)?, ctx).await?;
        }
        crate::log(format!(
            "{ctx}: two members lose all local Raft and seed files; one member keeps its full data"
        ));
        Ok(())
    }

    async fn observe(h: &Harness, member: u32, ctx: &'static str) -> anyhow::Result<Self> {
        Self::observe_start(h, member, ctx, BlankStart::of(member)).await
    }

    async fn observe_start(
        h: &Harness,
        member: u32,
        ctx: &'static str,
        start: BlankStart,
    ) -> anyhow::Result<Self> {
        let before = Self::starts_by_alloc(&h.nomad, member, &start)
            .await?
            .counts();
        let head = h.probes.executor_progress().await.filter(|h| *h > 0).ok_or_else(|| {
            crate::chaos_fail!("{ctx}: could not read the executor head before the member lost its state — refusing to run: the catch-up proof needs a real target position or it proves nothing")
        })?;
        Ok(Self {
            ctx,
            member,
            start,
            before,
            head,
        })
    }

    /// Start and snapshot evidence stays in the same allocation. A removed
    /// node is excluded by its Nomad identity, even while its old
    /// allocation still reports running or complete.
    async fn starts_by_alloc(
        nomad: &Nomad,
        member: u32,
        start: &BlankStart,
    ) -> anyhow::Result<StartLogs> {
        let allocs = nomad.allocations_with_logs(CLUSTER_TASK).await?;
        let mut logs = StartLogs(HashMap::new());
        for alloc in allocs.iter().filter(|a| a.client_status != "lost") {
            let text = nomad.alloc_logs(alloc, Streams::StdoutOnly).await?;
            logs.0.insert(
                alloc.id.clone(),
                StartSample {
                    count: text.lines().filter(|l| start.matches(l)).count(),
                    block: start.catchup_block(&text, member),
                },
            );
        }
        Ok(logs)
    }

    /// The member has a new expected start and its post-start snapshot
    /// reaches the head read before.
    async fn await_caught_up(self, h: &Harness) -> anyhow::Result<()> {
        let track = RefCell::new(Catchup::default());
        let budget = Budget::new(h.knobs.rejoin_slo, Duration::from_secs(10));
        let (track_ref, me) = (&track, &self);
        let outcome = poll::until(budget, |_| async move {
            let logs = Self::starts_by_alloc(&h.nomad, me.member, &me.start).await?;
            let block = logs.new_block(&me.before);
            let mut track = track_ref.borrow_mut();
            track.observe(block.unwrap_or(0), block.is_some());
            Ok((track.blank && track.block >= me.head).then_some(()))
        })
        .await?;
        let track = track.into_inner();
        let ((), elapsed) = outcome.or_fail(|t| track.failure(&self, t))?;
        let now = h.evidence.cluster_leader(h.knobs.leader_slo).await.ok();
        crate::log(format!(
            "{}: memberId={} rejoined after {} (replayed to block {} >= head {}, {}s); leader now memberId={}",
            self.ctx,
            self.member,
            self.start.mode(),
            track.block,
            self.head,
            elapsed.as_secs(),
            now.map_or("?".to_string(), |m| m.to_string())
        ));
        Ok(())
    }
}

struct StartSample {
    count: usize,
    block: i64,
}

struct StartLogs(HashMap<String, StartSample>);

impl StartLogs {
    fn counts(self) -> HashMap<String, usize> {
        self.0
            .into_iter()
            .map(|(id, sample)| (id, sample.count))
            .collect()
    }

    fn new_block(&self, before: &HashMap<String, usize>) -> Option<i64> {
        self.0
            .iter()
            .filter(|(id, sample)| sample.count > before.get(*id).copied().unwrap_or(0))
            .map(|(_, sample)| sample.block)
            .max()
    }
}

/// The start proof of a member: fresh state or a peer seed for an empty
/// start, and a resume from a verified offline copy.
#[derive(Debug)]
enum BlankStart {
    Peer([String; 2]),
    Copied(String),
}

impl BlankStart {
    fn of(member: u32) -> Self {
        Self::Peer([
            format!("sealer state FRESH at genesis memberId={member}"),
            format!("cluster SEED from-peer memberId={member} "),
        ])
    }

    fn copied(member: u32) -> Self {
        Self::Copied(format!("cluster START mode=RESUME memberId={member} "))
    }

    fn mode(&self) -> &str {
        match self {
            Self::Peer(_) => "a blank start",
            Self::Copied(_) => "an offline copy",
        }
    }

    fn needles(&self) -> &[String] {
        match self {
            Self::Peer(needles) => needles,
            Self::Copied(needle) => std::slice::from_ref(needle),
        }
    }

    fn matches(&self, line: &str) -> bool {
        self.needles()
            .iter()
            .any(|needle| line.contains(needle.as_str()))
    }

    /// The logs after the latest blank-start line, or `None` without one.
    fn after_latest<'a>(&self, logs: &'a str) -> Option<&'a str> {
        self.needles()
            .iter()
            .filter_map(|needle| logs.rfind(needle.as_str()).map(|at| at + needle.len()))
            .max()
            .map(|end| &logs[end..])
    }

    /// The latest snapshot after the new start. Pre-wipe history cannot
    /// satisfy the recovery proof.
    fn catchup_block(&self, logs: &str, member: u32) -> i64 {
        let taken = format!("snapshot TAKEN memberId={member}");
        let Some(after_start) = self.after_latest(logs) else {
            return 0;
        };
        after_start
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
}

/// The catch-up observations: the first non-zero position and whether
/// it ever moved, which tells a wedge from slow replay.
#[derive(Debug, Default)]
struct Catchup {
    block: i64,
    first: i64,
    moved: bool,
    blank: bool,
}

impl Catchup {
    fn observe(&mut self, block: i64, blank: bool) {
        self.block = block;
        self.blank = blank;
        if block > 0 && self.first == 0 {
            self.first = block;
        }
        self.moved |= block > self.first;
    }

    fn failure(&self, blank: &Blank, t: Duration) -> anyhow::Error {
        let (ctx, head, secs) = (blank.ctx, blank.head, t.as_secs());
        if !self.blank {
            return crate::chaos_fail!(
                "{ctx}: restarted member lacks a new start proof {:?}; recovery after {} is not proved",
                blank.start.needles(),
                blank.start.mode()
            );
        }
        if !self.moved {
            return crate::chaos_fail!(
                "{ctx}: recovered member FROZE at block {} (head before {head}) — its replay position never advanced once in {secs}s, so this is a JOIN WEDGE, not slow replay: the member stays INACTIVE after a partial replay while reporting healthy to Nomad",
                self.block
            );
        }
        crate::chaos_fail!(
            "{ctx}: recovered member replayed to block {} of head-before {head} in {secs}s — the position DID keep advancing, so recovery advances but does not reach the target within this budget",
            self.block
        )
    }
}

/// Kill two whole sealer nodes. Nomad on those nodes is gone too, so
/// only one member is left and the quorum is lost: the pipeline must
/// stall with no false progress. Then one node returns, the quorum is
/// back, and progress resumes with no gaps. The second node returns
/// last, and the pipeline must still progress with all three members.
/// The load window outlasts both returns, so the verdict covers
/// transactions submitted to the recovered cluster.
pub(crate) async fn quorum_loss_recover(h: &mut Harness) -> anyhow::Result<()> {
    let victims = [sealer(h, 1)?, sealer(h, 2)?];
    crate::log(format!(
        "cluster-quorum-loss-recover: docker kill TWO sealer nodes ({} {}) → quorum lost (1/3 up)",
        victims[0], victims[1]
    ));
    h.kill_nodes(&[&victims[0], &victims[1]]).await?;
    h.assert_executor_stalled(super::fleet::STALL_WINDOW)
        .await?;
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
    h.assert_executor_progress(Duration::from_mins(3)).await?;
    crate::log(format!(
        "cluster-quorum-loss-recover: docker start {} (all three members back)",
        victims[1]
    ));
    h.nodes.start(&victims[1]).await?;
    // The common tail proves that the executors advance with three members.
    h.assert_count(CLUSTER_TASK, 3, h.knobs.reschedule_slo)
        .await
}

#[cfg(test)]
mod tests;
