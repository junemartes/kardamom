//! The followers' evidence: the L1 follower's (the indexer's) tick
//! outcomes and progress gauges, the da-watcher's pause on it and its
//! progress, the resume they prove, and the operator step a poisoned
//! single-source follower needs. The halt judgment lives in `halt`.
//!
//! The L1 follower is the one reader of L1: a lie halts it, and the
//! da-watcher, which reads the follower's stream, pauses with the
//! follower as its root.

use std::time::Duration;

use crate::harness::Harness;
use crate::l1::L1;
use crate::poll::{self, Budget};
use crate::probes::{DA_WATCHER_PORT, INDEXER_PORT};

/// The pause gauge; the da-watcher's pause on the follower carries
/// `root_service="l1-indexer"`.
const PAUSED: &str = "kardamom_paused";
const ON_FOLLOWER: &str = "root_service=\"l1-indexer\"";
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
    watcher_paused: Option<i64>,
    watcher_origin: Option<i64>,
    indexer_errors: Option<i64>,
    indexer_block: Option<i64>,
    indexer_last_batch: Option<i64>,
}

impl Followers {
    pub(super) async fn read(h: &Harness) -> Self {
        let p = &h.probes;
        Self {
            watcher_paused: p
                .aux_metric_where(DA_WATCHER_PORT, PAUSED, ON_FOLLOWER)
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
            watcher_paused: self.watcher_paused.or(earlier.watcher_paused),
            watcher_origin: self.watcher_origin.or(earlier.watcher_origin),
            indexer_errors: self.indexer_errors.or(earlier.indexer_errors),
            indexer_block: self.indexer_block.or(earlier.indexer_block),
            indexer_last_batch: self.indexer_last_batch.or(earlier.indexer_last_batch),
        }
    }

    /// The L1 follower failed ticks since `base`, and the da-watcher is
    /// paused with the follower as its root.
    pub(super) fn halted_since(self, base: Self) -> bool {
        self.watcher_paused == Some(1) && rose(self.indexer_errors, base.indexer_errors)
    }

    /// The da-watcher is paused with the follower as its root.
    pub(super) fn watcher_paused(self) -> bool {
        self.watcher_paused == Some(1)
    }

    /// The da-watcher's last published L1 block, when it exported one.
    pub(super) fn watcher_origin(self) -> Option<i64> {
        self.watcher_origin
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
            "watcher paused_on_follower={} epoch_origin={} indexer errors={} block={} last_batch={}",
            s(self.watcher_paused),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_halt_needs_the_follower_s_errors_and_the_watcher_s_pause() {
        let base = Followers {
            watcher_paused: Some(0),
            watcher_origin: Some(10),
            indexer_errors: Some(0),
            indexer_block: Some(20),
            indexer_last_batch: Some(3),
        };
        let one = Followers {
            watcher_paused: Some(1),
            ..base
        };
        assert!(!one.halted_since(base));
        let both = Followers {
            indexer_errors: Some(2),
            ..one
        };
        assert!(both.halted_since(base));
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
        assert!(!dark.halted_since(base) && !dark.is_ready());
        assert!(base.is_ready());
        let restarted = Followers {
            indexer_block: None,
            ..moved
        };
        assert_eq!(restarted.or(base).indexer_block, Some(20));
        assert_eq!(restarted.or(base).watcher_origin, Some(11));
    }
}
