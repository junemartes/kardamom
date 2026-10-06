//! The log evidence of a sealer fleet seed: the members' start and
//! snapshot lines, and the sealer's answers to the executors' resumes.
//! Every wait counts lines past a baseline read before the step, since
//! earlier cases log the same lines.

use std::time::Duration;

use super::FULL_RESTART_ELECTION;
use super::sealer_wipe::CTX;
use super::seeding::SeedHead;
use crate::cases::component::{FETCHED, RESTORED};
use crate::evidence::CountWait;
use crate::harness::Harness;
use crate::nomad::Streams;
use crate::probes::CLUSTER_TASK;

/// The sealer's log lines of a start and of the first snapshot. A seeded
/// start logs `sealer state SEEDED ... block=H endTx=E_H senders=`; the
/// case counts it by the part that names the seed's head.
const FRESH: &str = "sealer state FRESH at genesis";
const CONFIRMED: &str = "sealer seed CONFIRMED";
const TAKEN: &str = "sealer snapshot TAKEN";
const SNAPSHOT_RESTORED: &str = "sealer snapshot RESTORED";
/// The sealer's answer to a resume past its head (`REPLAY_AHEAD`).
const AHEAD: &str = " AHEAD head=(";

/// The cluster's log counts before the seeded start.
pub(super) struct SealerCounts {
    seeded: usize,
    fresh: usize,
    confirmed: usize,
    taken: usize,
    restored: usize,
}

impl SealerCounts {
    pub(super) async fn read(h: &Harness, at_head: &str) -> anyhow::Result<Self> {
        let count = |needle: &'static str| {
            h.evidence
                .count_lines(CLUSTER_TASK, needle, Streams::StdoutOnly)
        };
        Ok(Self {
            seeded: h
                .evidence
                .count_lines(CLUSTER_TASK, at_head, Streams::StdoutOnly)
                .await?,
            fresh: count(FRESH).await?,
            confirmed: count(CONFIRMED).await?,
            taken: count(TAKEN).await?,
            restored: count(SNAPSHOT_RESTORED).await?,
        })
    }

    /// Every member logged `SEEDED` at the seed's head, confirmed the
    /// seed and took the snapshot. None started at genesis.
    pub(super) async fn await_seeded(
        &self,
        h: &Harness,
        at_head: &str,
        members: usize,
    ) -> anyhow::Result<()> {
        for (needle, baseline) in [
            (at_head, self.seeded),
            (CONFIRMED, self.confirmed),
            (TAKEN, self.taken),
        ] {
            Self::wait(h, needle, baseline, members).await?;
        }
        self.assert_no_genesis(h).await
    }

    /// Every member restored the snapshot, and none started at genesis.
    pub(super) async fn await_restored(&self, h: &Harness, members: usize) -> anyhow::Result<()> {
        Self::wait(h, SNAPSHOT_RESTORED, self.restored, members).await?;
        self.assert_no_genesis(h).await?;
        crate::log(format!(
            "{CTX}: every member restored the snapshot without the seed"
        ));
        Ok(())
    }

    async fn assert_no_genesis(&self, h: &Harness) -> anyhow::Result<()> {
        let fresh = h
            .evidence
            .count_lines(CLUSTER_TASK, FRESH, Streams::StdoutOnly)
            .await?;
        anyhow::ensure!(
            fresh == self.fresh,
            "{}: {CTX}: a member started at genesis ({FRESH}: {} -> {fresh})",
            crate::FAIL_PREFIX,
            self.fresh
        );
        Ok(())
    }

    async fn wait(
        h: &Harness,
        needle: &str,
        baseline: usize,
        members: usize,
    ) -> anyhow::Result<()> {
        let fail_msg = format!("{CTX}: not every member logged '{needle}'");
        h.evidence
            .wait_count_reaches(
                &CountWait {
                    job: CLUSTER_TASK,
                    needle,
                    baseline,
                    timeout: FULL_RESTART_ELECTION,
                    interval: Duration::from_secs(5),
                    streams: Streams::StdoutOnly,
                    fail_msg: &fail_msg,
                },
                baseline + members,
            )
            .await
    }
}

/// The log counts that tell a resume at the seed's cursor, and a refused
/// stale cursor, from a checkpoint restore.
pub(super) struct ResumeCounts {
    served: usize,
    ahead: usize,
    restored: usize,
    fetched: usize,
}

impl ResumeCounts {
    pub(super) async fn read(h: &Harness, head: &SeedHead) -> anyhow::Result<Self> {
        let cluster = |needle: String| async move {
            h.evidence
                .count_lines(CLUSTER_TASK, &needle, Streams::StdoutOnly)
                .await
        };
        let executor =
            |needle: &'static str| h.evidence.count_lines("executor", needle, Streams::Both);
        Ok(Self {
            served: cluster(format!("from={} served=", head.resume_cursor())).await?,
            ahead: cluster(AHEAD.to_string()).await?,
            restored: executor(RESTORED).await?,
            fetched: executor(FETCHED).await?,
        })
    }

    /// The sealer served `fresh` replays from the seed's cursor.
    pub(super) async fn await_resumed(
        &self,
        h: &Harness,
        head: &SeedHead,
        fresh: usize,
    ) -> anyhow::Result<()> {
        let needle = format!("from={} served=", head.resume_cursor());
        let fail_msg = format!(
            "{CTX}: the fresh executors did not resume at {}",
            head.resume_cursor()
        );
        h.evidence
            .wait_count_reaches(
                &CountWait {
                    job: CLUSTER_TASK,
                    needle: &needle,
                    baseline: self.served,
                    timeout: Duration::from_mins(3),
                    interval: Duration::from_secs(5),
                    streams: Streams::StdoutOnly,
                    fail_msg: &fail_msg,
                },
                self.served + fresh,
            )
            .await
    }

    /// The sealer answered the stale executor's cursor with
    /// `REPLAY_AHEAD`.
    pub(super) async fn await_ahead(&self, h: &Harness) -> anyhow::Result<()> {
        h.evidence
            .wait_count_gt(&CountWait {
                job: CLUSTER_TASK,
                needle: AHEAD,
                baseline: self.ahead,
                timeout: Duration::from_mins(3),
                interval: Duration::from_secs(5),
                streams: Streams::StdoutOnly,
                fail_msg: "sealer-fleet-total-wipe-recover: the executor that kept the old chain got no REPLAY_AHEAD",
            })
            .await
    }

    /// No executor restored or fetched a checkpoint: every checkpoint of
    /// the old chain was wiped, so such a line would mean the case proved
    /// nothing.
    pub(super) async fn assert_no_checkpoint(&self, h: &Harness) -> anyhow::Result<()> {
        let restored = h
            .evidence
            .count_lines("executor", RESTORED, Streams::Both)
            .await?;
        let fetched = h
            .evidence
            .count_lines("executor", FETCHED, Streams::Both)
            .await?;
        anyhow::ensure!(
            restored == self.restored && fetched == self.fetched,
            "{}: {CTX}: an executor restored or fetched a checkpoint (restored {} -> {restored}, fetched {} -> {fetched})",
            crate::FAIL_PREFIX,
            self.restored,
            self.fetched
        );
        Ok(())
    }
}
