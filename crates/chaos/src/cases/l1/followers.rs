//! The followers' evidence: the da-watcher's and the indexer's tick
//! outcomes and progress gauges, the resume they prove, and the operator
//! step a poisoned single-source follower needs. The halt judgment lives
//! in `halt`.

use std::time::Duration;

use crate::cases::component::wipe_dirs;
use crate::cases::da_watcher::restart_and_follow_sealer;
use crate::harness::Harness;
use crate::l1::L1;
use crate::nomad::SavedJob;
use crate::poll::{self, Budget};
use crate::probes::{DA_WATCHER_PORT, INDEXER_PORT};

const WATCHER_TICKS: &str = "kardamom_da_watcher_tick_total";
/// The L1 block of the last published epoch: a block number, so it
/// keeps its meaning across a restart, where a counter starts at zero.
const WATCHER_EPOCH_ORIGIN: &str = "kardamom_da_watcher_epoch_origin_block_number";
const INDEXER_TICKS: &str = "kardamom_l1_indexer_tick_total";
const INDEXER_BLOCK: &str = "kardamom_l1_indexer_indexed_block_number";
const INDEXER_LAST_BATCH: &str = "kardamom_l1_indexer_last_batch_index";

/// anvil finalizes two blocks behind its head, and the batcher's posts
/// mine a block every few seconds: both gauges show within a minute.
const READY_BUDGET: Duration = Duration::from_secs(120);
const RESUME_BUDGET: Duration = Duration::from_secs(120);
const ARCHIVE_BUDGET: Duration = Duration::from_secs(240);

/// One sample of both followers. A failed scrape stays `None`: it is
/// evidence, not zero.
#[derive(Debug, Clone, Copy, Default)]
pub(super) struct Followers {
    watcher_breaks: Option<i64>,
    watcher_origin: Option<i64>,
    indexer_errors: Option<i64>,
    indexer_block: Option<i64>,
    indexer_last_batch: Option<i64>,
}

impl Followers {
    pub(super) async fn read(h: &Harness) -> Self {
        let p = &h.probes;
        Self {
            watcher_breaks: p
                .aux_metric_where(DA_WATCHER_PORT, WATCHER_TICKS, "outcome=\"chain_break\"")
                .await,
            watcher_origin: p.aux_metric(DA_WATCHER_PORT, WATCHER_EPOCH_ORIGIN).await,
            indexer_errors: p
                .aux_metric_where(INDEXER_PORT, INDEXER_TICKS, "outcome=\"error\"")
                .await,
            indexer_block: p.aux_metric(INDEXER_PORT, INDEXER_BLOCK).await,
            indexer_last_batch: p.aux_metric(INDEXER_PORT, INDEXER_LAST_BATCH).await,
        }
    }

    /// Both exporters answered with their progress gauges. A gauge is
    /// absent until the follower's first finalized block: a follower
    /// without one has nothing a lie can halt.
    fn is_ready(self) -> bool {
        self.watcher_origin.is_some() && self.indexer_block.is_some()
    }

    /// The first sample where both followers show progress. A case
    /// cannot start before it: its halt and resume judgments compare
    /// against these gauges.
    pub(super) async fn ready(h: &Harness, ctx: &str) -> anyhow::Result<Self> {
        let last = std::cell::Cell::new(Self::default());
        let last_ref = &last;
        let outcome = poll::until(
            Budget::new(READY_BUDGET, Duration::from_secs(2)),
            |_| async move {
                let now = Self::read(h).await;
                last_ref.set(now);
                Ok::<_, anyhow::Error>(now.is_ready().then_some(now))
            },
        )
        .await?;
        let (now, _) = outcome.or_fail(|t| {
            crate::chaos_fail!(
                "{ctx}: a follower shows no finalized block within {}s ({})",
                t.as_secs(),
                last.get().show()
            )
        })?;
        Ok(now)
    }

    /// The followers at the moment the lie stops, then the lie cleared.
    /// A resume must pass this sample: a halted follower holds its
    /// gauges here, and `before` (the sample before the lie) is already
    /// behind it. A follower restarted under the lie has no gauge yet;
    /// its value from `before` stands in.
    pub(super) async fn at_clear(h: &Harness, l1: &L1, before: Self) -> anyhow::Result<Self> {
        let now = Self::read(h).await.or(before);
        l1.clear_faults().await?;
        Ok(now)
    }

    /// Each absent field of `self` taken from `earlier`.
    fn or(self, earlier: Self) -> Self {
        Self {
            watcher_breaks: self.watcher_breaks.or(earlier.watcher_breaks),
            watcher_origin: self.watcher_origin.or(earlier.watcher_origin),
            indexer_errors: self.indexer_errors.or(earlier.indexer_errors),
            indexer_block: self.indexer_block.or(earlier.indexer_block),
            indexer_last_batch: self.indexer_last_batch.or(earlier.indexer_last_batch),
        }
    }

    /// Both followers counted a chain break since `base`.
    pub(super) fn chain_broke_since(self, base: Self) -> bool {
        rose(self.watcher_breaks, base.watcher_breaks)
            && rose(self.indexer_errors, base.indexer_errors)
    }

    /// Both followers moved since `base`: the da-watcher published an
    /// epoch of a later block, the indexer indexed a later block.
    fn advanced_since(self, base: Self) -> bool {
        rose(self.watcher_origin, base.watcher_origin)
            && rose(self.indexer_block, base.indexer_block)
    }

    pub(super) fn show(self) -> String {
        let s = |v: Option<i64>| v.map_or("?".to_string(), |x| x.to_string());
        format!(
            "watcher chain_breaks={} epoch_origin={} indexer errors={} block={} last_batch={}",
            s(self.watcher_breaks),
            s(self.watcher_origin),
            s(self.indexer_errors),
            s(self.indexer_block),
            s(self.indexer_last_batch)
        )
    }
}

fn rose(now: Option<i64>, base: Option<i64>) -> bool {
    matches!((now, base), (Some(n), Some(b)) if n > b)
}

/// Both followers move again past `base`.
pub(super) async fn await_resume(h: &Harness, base: Followers, ctx: &str) -> anyhow::Result<()> {
    let last = std::cell::Cell::new(base);
    let last_ref = &last;
    let outcome = poll::until(
        Budget::new(RESUME_BUDGET, Duration::from_secs(5)),
        |_| async move {
            let now = Followers::read(h).await;
            last_ref.set(now);
            Ok::<_, anyhow::Error>(now.advanced_since(base).then_some(now))
        },
    )
    .await?;
    let (now, elapsed) = outcome.or_fail(|t| {
        crate::chaos_fail!(
            "{ctx}: the followers did not resume within {}s (before: {}; last: {})",
            t.as_secs(),
            base.show(),
            last.get().show()
        )
    })?;
    crate::log(format!(
        "{ctx}: both followers resumed after {}s ({})",
        elapsed.as_secs(),
        now.show()
    ));
    Ok(())
}

/// The archive holds every batch L1 held at the call: the indexer's
/// last batch index reaches the contract's counter as read then.
pub(super) async fn await_archive_complete(h: &Harness, l1: &L1, ctx: &str) -> anyhow::Result<()> {
    // The target is read once: the batcher keeps posting, and the
    // indexer walks finalized blocks only, so it always trails L1's
    // newest batch by the finality depth.
    let on_l1 = i64::try_from(l1.last_batch_index().await?).unwrap_or(i64::MAX);
    let outcome = poll::until(
        Budget::new(ARCHIVE_BUDGET, Duration::from_secs(5)),
        |_| async move {
            let archived = h.probes.aux_metric(INDEXER_PORT, INDEXER_LAST_BATCH).await;
            Ok::<_, anyhow::Error>(archived.filter(|a| *a >= on_l1).map(|a| (a, on_l1)))
        },
    )
    .await?;
    let ((archived, on_l1), elapsed) = outcome.or_fail(|t| {
        crate::chaos_fail!(
            "{ctx}: the archive did not reach L1's last batch within {}s",
            t.as_secs()
        )
    })?;
    crate::log(format!(
        "{ctx}: the archive is complete: batch {archived} archived, L1 at {on_l1} ({}s)",
        elapsed.as_secs()
    ));
    Ok(())
}

/// The step a single-source follower needs after a wrong hash reached
/// its anchor. Both cursors are on disk with the wrong hash. The
/// da-watcher restarts from its registered job: the start resumes after
/// the sealer's L1 origin and reads that block's hash again, so it skips
/// no epoch and needs no resume flag. The indexer's archive is re-indexed
/// from the chain's first block. The two-source followers never store an
/// unagreed hash, which removes this step.
pub(super) async fn heal_single_source_followers(h: &mut Harness, ctx: &str) -> anyhow::Result<()> {
    crate::log(format!(
        "{ctx}: OPERATOR STEP (removed by the two-source followers): restart the da-watcher, re-index the archive"
    ));
    restart_and_follow_sealer(h, ctx).await?;
    let aux = h.probes.validator.container.clone();
    let indexer = SavedJob::capture(&h.nomad, "l1-indexer").await?;
    indexer.stop().await?;
    wipe_dirs(h, &aux, ctx, "rm -rf /opt/kardamom/l1-indexer/*").await?;
    indexer.restore().await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_halt_and_a_resume_need_both_followers() {
        let base = Followers {
            watcher_breaks: Some(0),
            watcher_origin: Some(10),
            indexer_errors: Some(0),
            indexer_block: Some(20),
            indexer_last_batch: Some(3),
        };
        let one = Followers {
            watcher_breaks: Some(1),
            ..base
        };
        assert!(!one.chain_broke_since(base));
        let both = Followers {
            indexer_errors: Some(2),
            ..one
        };
        assert!(both.chain_broke_since(base));
        assert!(!both.advanced_since(base));
        let moved = Followers {
            watcher_origin: Some(11),
            indexer_block: Some(21),
            ..base
        };
        assert!(moved.advanced_since(base));
        let dark = Followers {
            watcher_origin: None,
            ..base
        };
        assert!(!dark.chain_broke_since(base) && !dark.is_ready());
        assert!(base.is_ready());
        let restarted = Followers {
            indexer_block: None,
            ..moved
        };
        assert_eq!(restarted.or(base).indexer_block, Some(20));
        assert_eq!(restarted.or(base).watcher_origin, Some(11));
    }
}
