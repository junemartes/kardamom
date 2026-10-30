//! The carry sealer evicts old frames. The durable egress keeps the
//! unposted range even when it exceeds the readers' retention window.
//!
//! The batcher freezes first. The roll of the egress to the short reader
//! window and the restore of its own window each run beside a wait of
//! the case: the first beside the sealing past the frozen cursor, the
//! second beside the posts after the thaw.

use std::time::Duration;

use anyhow::Context;
use serde::Serialize;

use super::{ChainView, FrozenBatcher, SNAPSHOT_TAKEN, await_all_running, require_status};
use crate::harness::Harness;
use crate::l1::L1;
use crate::nomad::{SavedJob, Streams};
use crate::poll::{self, Budget};
use crate::probes::CLUSTER_TASK;

mod job;
mod replay;
use job::{SEGMENT_BLOCKS, WindowJob};
use replay::{Copies, Cursor};

const CTX: &str = "prune-floor";
const KEEP_BLOCKS: u64 = 24;

pub(super) struct CarryFloor;

/// The egress job of the fault: as saved, with the short window, and the
/// copies that ran before the roll.
struct Window<'a> {
    saved: &'a SavedJob,
    job: &'a WindowJob,
    old: Copies,
}

#[derive(Debug, Serialize)]
struct Proof {
    cursor: Cursor,
    replay_end: Cursor,
    sealer_floor: i64,
    sealed: i64,
    copies: usize,
    keep_blocks: u64,
    segment_blocks: u64,
}

impl CarryFloor {
    pub(super) async fn run(h: &Harness) -> anyhow::Result<()> {
        let saved = SavedJob::capture(&h.nomad, "egress").await?;
        let configured = WindowJob::new(saved.definition(), KEEP_BLOCKS)?;
        let sealed = Self::configured(h, &saved, &configured).await;
        let (verdict, restored) = tokio::join!(Self::after_thaw(h, sealed), saved.restore());
        if let Err(error) = &restored {
            crate::log(format!("{CTX}: restoring the egress job fails: {error:#}"));
        }
        verdict?;
        restored?;
        h.assert_executor_progress(Duration::from_mins(2)).await
    }

    /// The fault with the short reader window: the sealed head at the
    /// thaw. The batcher thaws on both outcomes.
    async fn configured(h: &Harness, saved: &SavedJob, job: &WindowJob) -> anyhow::Result<u64> {
        let old = Copies::of(h).await?;
        old.head().await?;
        let snapshots = h
            .evidence
            .count_lines(CLUSTER_TASK, SNAPSHOT_TAKEN, Streams::StdoutOnly)
            .await?;
        let frozen = FrozenBatcher::freeze(h, CTX).await?;
        let verdict = Self::frozen(h, Window { saved, job, old }, snapshots).await;
        frozen.thaw(h, CTX).await;
        let proof = verdict?;
        std::fs::write(
            "/tmp/chaos-prune-floor-fault.json",
            serde_json::to_vec_pretty(&proof)?,
        )?;
        Ok(u64::try_from(proof.sealed)?)
    }

    /// The posts through the sealed head of the thaw, when the fault
    /// passed.
    async fn after_thaw(h: &Harness, sealed: anyhow::Result<u64>) -> anyhow::Result<()> {
        Self::posted(h, sealed?).await
    }

    /// Take the frozen cursor from the running copies, roll the egress to
    /// the short window, and wait until the carry floor and the sealed
    /// head pass the cursor and a snapshot lands.
    async fn frozen(h: &Harness, window: Window<'_>, snapshots: usize) -> anyhow::Result<Proof> {
        let cursor = window.old.head().await?;
        crate::log(format!(
            "{CTX}: set the egress reader window to {KEEP_BLOCKS} blocks in {SEGMENT_BLOCKS}-block segments for this fault"
        ));
        window.saved.register(window.job.definition()).await?;
        let copies = Copies::of(h).await?;
        let last = std::cell::Cell::new(None);
        let last_ref = &last;
        let (stretched, _) = poll::until(
            Budget::new(h.knobs.retention_freeze_cap, Duration::from_secs(10)),
            |_| async move {
                let status = require_status(h, CTX).await?;
                last_ref.set(Some(status));
                let floor = u64::try_from(status.floor).unwrap_or(0);
                let sealed = u64::try_from(status.sealed).unwrap_or(0);
                let snapshots_now = h
                    .evidence
                    .count_lines(CLUSTER_TASK, SNAPSHOT_TAKEN, Streams::StdoutOnly)
                    .await?;
                Ok::<_, anyhow::Error>((
                    floor > cursor.block
                        && sealed.saturating_sub(cursor.block) > KEEP_BLOCKS + SEGMENT_BLOCKS
                        && snapshots_now > snapshots
                )
                    .then_some(status))
            },
        )
        .await?
        .or_fail(|elapsed| {
            crate::chaos_fail!(
                "{CTX}: the carry floor did not pass {cursor:?} and the {KEEP_BLOCKS}-block window with a new snapshot in {}s; last {:?}",
                elapsed.as_secs(),
                last.get()
            )
        })?;
        let view = ChainView::read(h).await?;
        anyhow::ensure!(
            u64::try_from(stretched.posted)? < cursor.block,
            "{CTX}: a post covers the retained cursor {cursor:?}: {stretched:?}"
        );
        anyhow::ensure!(view.roots().is_empty(), "{CTX}: unexpected root: {view}");
        anyhow::ensure!(
            view.states_of("batcher")
                .iter()
                .any(|state| state == "gone"),
            "{CTX}: the frozen batcher is not gone: {view}"
        );
        let replay_end = tokio::time::timeout(
            Duration::from_mins(2),
            copies.replay(cursor, KEEP_BLOCKS + SEGMENT_BLOCKS),
        )
        .await
        .context("prune-floor: replay exceeds two minutes")??;
        crate::log(format!(
            "{CTX}: all {} egress copies replay the same bytes from {cursor:?}, below carry floor {} and past the {KEEP_BLOCKS}-block window",
            copies.len(),
            stretched.floor
        ));
        Ok(Proof {
            cursor,
            replay_end,
            sealer_floor: stretched.floor,
            sealed: stretched.sealed,
            copies: copies.len(),
            keep_blocks: KEEP_BLOCKS,
            segment_blocks: SEGMENT_BLOCKS,
        })
    }

    async fn posted(h: &Harness, sealed: u64) -> anyhow::Result<()> {
        let l1 = L1::new(h).await?;
        let l1_ref = &l1;
        poll::until(Budget::secs(300, 5), |_| async move {
            Ok::<_, anyhow::Error>((l1_ref.covered_through().await? >= sealed).then_some(()))
        })
        .await?
        .or_fail(|_| crate::chaos_fail!("{CTX}: the batcher does not post through {sealed}"))?;
        l1.assert_contiguous(CTX).await?;
        await_all_running(h, CTX).await
    }
}
