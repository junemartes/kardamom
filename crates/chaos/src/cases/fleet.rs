//! The whole-fleet cases: every replica of one role goes down at once.
//! No peer is left to lean on, so the sealers must come back from their
//! own logs and snapshots, and the executors from their own disks or
//! their own checkpoints. The load keeps submitting through the
//! outage and after it, so the verdict proves the chain accepts new
//! transactions again and receipts them correctly.

use std::path::PathBuf;
use std::time::Duration;

use crate::cases::cluster::sealer;
use crate::cases::component::{
    FETCHED, RESTORED, executor_containers, wait_peer_checkpoint, wipe_dirs,
};
use crate::evidence::CountWait;
use crate::harness::Harness;
use crate::nomad::{SavedJob, Streams};
use crate::poll::{self, Budget};
use crate::probes::CLUSTER_TASK;
use crate::stages::rebuild::{Output, Rebuild, Rebuilt, Target};

mod exporter_back;
mod sealer_wipe;
mod seed_evidence;
mod seeding;
mod storm;

pub(crate) use exporter_back::await_exporter_back;
pub(crate) use sealer_wipe::sealer_fleet_total_wipe_recover;
pub(crate) use storm::{ROUNDS, executor_restart_storm};

/// How long three restarted members get to elect a leader. Each one
/// restores its snapshot and replays the log tail before it votes, so
/// the election takes longer on a long chain than the 45 s a live
/// cluster needs after one leader kill. The failure model gives the
/// whole recovery 180 s.
pub(crate) const FULL_RESTART_ELECTION: Duration = Duration::from_mins(3);

/// The three sealer nodes, by member id.
pub(crate) fn sealers(h: &Harness) -> anyhow::Result<Vec<String>> {
    (0..3).map(|id| sealer(h, id)).collect()
}

/// Kill all three sealer nodes. No member is left, so the pipeline
/// must stall. Then every node returns with its own log and snapshots,
/// the members elect a leader among themselves, and the backlog
/// drains. The load window outlasts the return, so the verdict covers
/// transactions submitted to the recovered cluster.
pub(crate) async fn cluster_total_loss_recover(h: &mut Harness) -> anyhow::Result<()> {
    let ctx = "cluster-total-loss-recover";
    let nodes = sealers(h)?;
    h.kill_all_nodes(ctx, "sealer", &nodes).await?;
    h.assert_executor_stalled(Duration::from_secs(15)).await?;
    crate::log(format!("{ctx}: docker start all three sealer nodes"));
    h.start_nodes(&nodes).await?;
    h.assert_count(CLUSTER_TASK, 3, h.knobs.reschedule_slo)
        .await?;
    let leader = h.evidence.cluster_leader(FULL_RESTART_ELECTION).await?;
    crate::log(format!("{ctx}: members elected memberId={leader}"));
    h.assert_executor_progress(Duration::from_mins(3)).await
}

/// Kill all three executor nodes. Every exporter goes dark, so the
/// outage is real and not a survivor answering for the fleet. Then the
/// nodes return, each executor resumes from its own state directory,
/// and the fleet catches up on the backlog the sealers kept ordering.
pub(crate) async fn executor_fleet_loss_recover(h: &mut Harness) -> anyhow::Result<()> {
    let ctx = "executor-fleet-loss-recover";
    let nodes = executor_containers(h);
    h.kill_all_nodes(ctx, "executor", &nodes).await?;
    await_exporters_dark(h, ctx).await?;
    crate::log(format!("{ctx}: docker start all three executor nodes"));
    h.start_nodes(&nodes).await?;
    h.assert_count("executor", 3, h.knobs.reschedule_slo)
        .await?;
    await_exporter_back(h, ctx).await?;
    h.assert_executor_progress(Duration::from_mins(3)).await
}

/// Kill all three executor tasks and wipe every state database. The
/// nodes stay up so the wipe can run; each node keeps its own
/// checkpoints, and no peer is live to serve one, so every executor
/// must restore from its local checkpoint and replay the tail. Three
/// restore lines prove that no executor re-synced from genesis or
/// waited for a peer.
///
/// The job stops after the kills and starts again after the wipe.
/// Nomad restarts a killed task in seconds, and a task that runs again
/// before its wipe opens its old state and logs no restore.
pub(crate) async fn executor_fleet_wipe_recover(h: &mut Harness) -> anyhow::Result<()> {
    let ctx = "executor-fleet-wipe-recover";
    let nodes = executor_containers(h);
    for node in &nodes {
        wait_peer_checkpoint(h, node, ctx).await?;
    }
    crate::log(format!(
        "{ctx}: kill ALL executor tasks ({}) and wipe every state DB (checkpoints kept)",
        nodes.join(" ")
    ));
    let tasks: Vec<(String, &str)> = nodes.iter().map(|n| (n.clone(), "executor")).collect();
    let job = h.kill_tasks_and_stop("executor", &tasks).await?;
    // The job starts again even when the wipe fails.
    let wiped = wipe_stopped_fleet(h, ctx, &nodes).await;
    let restored = job.restore().await;
    let baseline = wiped?;
    restored?;
    h.assert_count("executor", nodes.len(), h.knobs.reschedule_slo)
        .await?;
    await_exporter_back(h, ctx).await?;
    h.evidence
        .wait_count_reaches(
            &CountWait {
                job: "executor",
                needle: RESTORED,
                baseline,
                timeout: Duration::from_mins(3),
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
    h.assert_executor_progress(Duration::from_mins(3)).await
}

/// With the executor job stopped, wait until every exporter is dark,
/// read the restore-line count, and wipe the state database of every
/// node. No executor runs, so no restore line can land before the
/// count. Returns the count.
async fn wipe_stopped_fleet(h: &Harness, ctx: &str, nodes: &[String]) -> anyhow::Result<usize> {
    await_exporters_dark(h, ctx).await?;
    let baseline = h
        .evidence
        .count_lines("executor", RESTORED, Streams::Both)
        .await?;
    for node in nodes {
        wipe_dirs(h, node, ctx, "rm -rf /opt/kardamom/state/*").await?;
    }
    Ok(baseline)
}

/// The executor's log line of a resume from its own state cursor.
const RESUMED: &str = "resuming from persisted state cursor via cluster canonical replay";

/// An executor's state directory on its node.
const EXECUTOR_STATE: &str = "/opt/kardamom/state";

/// Stop the executor job and wipe every state database and every
/// checkpoint: no executor holds any state, and no peer can serve a
/// checkpoint. The state is then rebuilt on the host from L1 and the DA
/// store alone, as an executor image, installed on every executor node,
/// and the job starts again. Every executor must resume from the image's
/// cursor with a replay request the sealer accepts, with no checkpoint
/// restore and no fetch from a peer, and the fleet must catch up. The
/// end-of-shard audit then compares the resumed executors with the
/// validator table by table.
///
/// The job is stopped, not killed: Nomad restarts a killed task in
/// seconds, and an executor that starts on an empty directory of a young
/// chain replays from genesis on its own, which would prove nothing.
pub(crate) async fn executor_fleet_total_wipe_recover(h: &mut Harness) -> anyhow::Result<()> {
    let ctx = "executor-fleet-total-wipe-recover";
    let nodes = executor_containers(h);
    let head = h
        .probes
        .executor_progress()
        .await
        .and_then(|head| u64::try_from(head).ok())
        .filter(|head| *head > 0)
        .ok_or_else(|| crate::chaos_fail!("{ctx}: no executor head before the wipe"))?;
    let job = SavedJob::capture(&h.nomad, "executor").await?;
    crate::log(format!(
        "{ctx}: stop the executor job and wipe state AND checkpoints on {} (head {head})",
        nodes.join(" ")
    ));
    job.stop().await?;
    // The job starts again even when the rebuild or the install fails.
    let installed = install_rebuilt_image(h, ctx, &nodes, head, None).await;
    let baseline = ResumeEvidence::read(h).await;
    let restored = job.restore().await;
    installed?;
    restored?;
    h.assert_count("executor", nodes.len(), h.knobs.reschedule_slo)
        .await?;
    await_exporter_back(h, ctx).await?;
    baseline?
        .assert_resumed_from_the_image(h, ctx, nodes.len())
        .await?;
    h.assert_executor_progress(Duration::from_mins(3)).await
}

/// Wipe every executor node in `nodes`, rebuild an executor image
/// through `head` from L1, and install it on every one of them. With
/// `sealer_seed`, the same rebuild writes the seed a wiped sealer cluster
/// starts from. Returns the image, so a caller can install it on another
/// node later.
async fn install_rebuilt_image(
    h: &Harness,
    ctx: &str,
    nodes: &[String],
    head: u64,
    sealer_seed: Option<PathBuf>,
) -> anyhow::Result<Rebuilt> {
    let mut owners = Vec::with_capacity(nodes.len());
    for node in nodes {
        owners.push(wipe_node(h, ctx, node).await?);
    }
    let rebuilt = Rebuild {
        harness: h,
        evidence: evidence_dir(ctx, "chaos-total-wipe-")?,
        target: unchecked_target(head),
        output: Output::ExecutorImage,
        sealer_seed,
    }
    .run()
    .await?;
    let cursor = rebuilt.end_tx_idx().ok_or_else(|| {
        crate::chaos_fail!(
            "{ctx}: the rebuilt image carries no resume cursor: {}",
            rebuilt.report
        )
    })?;
    crate::log(format!(
        "{ctx}: rebuilt an executor image at block {head}, resume cursor {cursor}"
    ));
    for (node, owner) in nodes.iter().zip(&owners) {
        install_image(h, ctx, node, owner, &rebuilt).await?;
    }
    Ok(rebuilt)
}

/// A new directory that keeps a rebuild's evidence after the case.
fn evidence_dir(ctx: &str, prefix: &str) -> anyhow::Result<PathBuf> {
    let evidence = tempfile::Builder::new().prefix(prefix).tempdir()?.keep();
    crate::log(format!("{ctx}: rebuild evidence: {}", evidence.display()));
    Ok(evidence)
}

/// A rebuild target with no root and no cursor to compare. No writer is
/// stopped at a known root in the middle of a case. The end-of-shard
/// audit compares the resumed state with the validator instead.
fn unchecked_target(block: u64) -> Target {
    Target {
        block,
        root: None,
        end_tx_idx: None,
    }
}

/// Record who owns the executor's state database, then wipe its state
/// and its checkpoints.
async fn wipe_node(h: &Harness, ctx: &str, node: &str) -> anyhow::Result<StateOwner> {
    let owner = StateOwner::read(h, ctx, node, EXECUTOR_STATE).await?;
    wipe_dirs(h, node, ctx, &owner.wipe_script()).await?;
    Ok(owner)
}

/// Who owns a consumer's database file, and the mode of its state
/// directory, as `stat` prints them. The directory belongs to the node
/// and is world-writable; the database file belongs to the user the
/// consumer runs as. A copy from the host carries the host's owner and
/// the 0700 mode of a temp directory, and mdbx opens a database it cannot
/// write as read-only, which the consumer refuses. So both are read
/// before the wipe and given to the installed image.
struct StateOwner {
    dir: &'static str,
    file_owner: String,
    dir_mode: String,
}

impl StateOwner {
    /// The node's checkpoint directory. A checkpoint of a wiped state is
    /// a copy of it, so the wipe takes both.
    const CHECKPOINTS: &'static str = "/opt/kardamom/checkpoints";

    async fn read(h: &Harness, ctx: &str, node: &str, dir: &'static str) -> anyhow::Result<Self> {
        let script = format!("stat -c '%u:%g' {dir}/mdbx.dat && stat -c '%a' {dir}");
        let out = h.nodes.exec(node, &script).await.map_err(|e| {
            crate::chaos_fail!("{ctx}: could not stat the state database on {node}: {e}")
        })?;
        let mut fields = out.split_whitespace();
        let (Some(file_owner), Some(dir_mode)) = (fields.next(), fields.next()) else {
            return Err(crate::chaos_fail!(
                "{ctx}: unexpected stat output for the state database on {node}: {out:?}"
            ));
        };
        Ok(Self {
            dir,
            file_owner: file_owner.to_string(),
            dir_mode: dir_mode.to_string(),
        })
    }

    /// The script that empties the state directory and the checkpoints,
    /// hidden files included.
    fn wipe_script(&self) -> String {
        format!(
            "find {dir} {checkpoints} -mindepth 1 -delete",
            dir = self.dir,
            checkpoints = Self::CHECKPOINTS
        )
    }

    /// The script that makes the installed image the consumer's own: the
    /// writer's lock file removed, every file with the recorded owner, the
    /// directory with its recorded mode.
    fn restore_script(&self) -> String {
        format!(
            "rm -f {dir}/mdbx.lck && chown -R {owner} {dir}/. && chmod {mode} {dir} && chmod -R u+rwX {dir}/. && test -s {dir}/mdbx.dat",
            dir = self.dir,
            owner = self.file_owner,
            mode = self.dir_mode
        )
    }
}

/// Copy the image into the node's empty state directory, then make it
/// the consumer's own.
async fn install_image(
    h: &Harness,
    ctx: &str,
    node: &str,
    owner: &StateOwner,
    image: &Rebuilt,
) -> anyhow::Result<()> {
    let source = format!("{}/.", image.state_dir.display());
    h.nodes
        .docker_ok(&["cp", &source, &format!("{node}:{}/", owner.dir)])
        .await
        .map_err(|e| crate::chaos_fail!("{ctx}: could not copy the image to {node}: {e}"))?;
    wipe_dirs(h, node, ctx, &owner.restore_script()).await?;
    crate::log(format!(
        "{ctx}: image installed on {node} (file owner {}, dir mode {})",
        owner.file_owner, owner.dir_mode
    ));
    Ok(())
}

/// The executor job's log counts that tell a resume from the installed
/// image apart from a checkpoint restore.
struct ResumeEvidence {
    resumed: usize,
    restored: usize,
    fetched: usize,
}

impl ResumeEvidence {
    async fn read(h: &Harness) -> anyhow::Result<Self> {
        Ok(Self {
            resumed: h
                .evidence
                .count_lines("executor", RESUMED, Streams::Both)
                .await?,
            restored: h
                .evidence
                .count_lines("executor", RESTORED, Streams::Both)
                .await?,
            fetched: h
                .evidence
                .count_lines("executor", FETCHED, Streams::Both)
                .await?,
        })
    }

    /// Every executor logged a resume from its state cursor since this
    /// baseline, and none restored or fetched a checkpoint: no checkpoint
    /// exists, so such a line would mean the case proved nothing.
    async fn assert_resumed_from_the_image(
        &self,
        h: &Harness,
        ctx: &str,
        executors: usize,
    ) -> anyhow::Result<()> {
        h.evidence
            .wait_count_reaches(
                &CountWait {
                    job: "executor",
                    needle: RESUMED,
                    baseline: self.resumed,
                    timeout: Duration::from_mins(2),
                    interval: Duration::from_secs(5),
                    streams: Streams::Both,
                    fail_msg: "executor-fleet-total-wipe-recover: not every executor resumed from the installed image's cursor",
                },
                self.resumed + executors,
            )
            .await?;
        let now = Self::read(h).await?;
        anyhow::ensure!(
            now.restored == self.restored && now.fetched == self.fetched,
            "{}: {ctx}: an executor restored or fetched a checkpoint (restored {} -> {}, fetched {} -> {}); every checkpoint was wiped, so the image was not what it resumed on",
            crate::FAIL_PREFIX,
            self.restored,
            now.restored,
            self.fetched,
            now.fetched
        );
        crate::log(format!(
            "{ctx}: all {executors} executors resumed from the image's cursor, with no checkpoint restore"
        ));
        Ok(())
    }
}

/// Wait until no executor exporter answers. A node kill that leaves
/// one exporter up proves nothing about the fleet.
pub(crate) async fn await_exporters_dark(h: &Harness, ctx: &str) -> anyhow::Result<()> {
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

async fn all_exporters_dark(h: &Harness) -> bool {
    let mut dark = true;
    for i in 0..h.probes.executors.len() {
        dark &= h.probes.exec_metrics(i).await.is_none();
    }
    dark
}
