//! `follower-instance-loss` and `follower-total-loss`: the L1 follower
//! runs as two instances, and the da-watcher reads their `l1_blocks`
//! stream.
//!
//! - One instance frozen during load: the other carries the stream. The
//!   da-watcher publishes an epoch for every block with no gap and no
//!   pause, and the batcher keeps posting.
//! - Both instances stopped for longer than the da-watcher's silence
//!   window: the da-watcher pauses with the follower as its root. The
//!   restart resumes it after the sealer's origin with no gap and no
//!   double epoch: each block's epoch is published once.

use std::time::Duration;

use super::batcher::{assert_posted_through, require_posting};
use super::followers::Followers;
use crate::cases::da_watcher::assert_no_origin_gap;
use crate::harness::Harness;
use crate::metrics::Target;
use crate::nomad::SavedJob;
use crate::poll::{self, Budget};
use crate::probes::{DA_WATCHER_PORT, INDEXER_PORT};

const JOB: &str = "l1-indexer";
/// The follower's task container on its node: `<task>-<alloc-id>`.
const TASK: &str = "l1-indexer";
const PUBLISHED_TOTAL: &str = "kardamom_da_watcher_epochs_published_total";
const WAITING: &str = "kardamom_da_watcher_waiting_for_l1_block";

/// How long the stop of both instances lasts: past the da-watcher's
/// silence window in this shard (30 s), with room for its housekeeping
/// tick and the board's expiry of the instances' heartbeats.
const TOTAL_LOSS_HOLD: Duration = Duration::from_secs(75);
/// How long the da-watcher may take to resume once the follower is back.
const RESUME_BUDGET: Duration = Duration::from_secs(180);

/// The da-watcher's view: its last published block, its published count,
/// its pause on the follower, and whether it waits for a record.
#[derive(Debug, Clone, Copy)]
struct Watcher {
    followers: Followers,
    published: Option<i64>,
    waiting: Option<i64>,
}

impl Watcher {
    async fn read(h: &Harness) -> Self {
        let p = &h.probes;
        Self {
            followers: Followers::read(h).await,
            published: p.aux_metric(DA_WATCHER_PORT, PUBLISHED_TOTAL).await,
            waiting: p.aux_metric(DA_WATCHER_PORT, WAITING).await,
        }
    }

    fn origin(self) -> Option<i64> {
        self.followers.watcher_origin()
    }

    /// The blocks published since `base`, and the publishes: equal when
    /// each block's epoch was published once and none was skipped.
    fn published_once_since(self, base: Self) -> Option<(i64, i64)> {
        let blocks = self.origin()?.checked_sub(base.origin()?)?;
        let publishes = self.published?.checked_sub(base.published?)?;
        Some((blocks, publishes))
    }

    fn show(self) -> String {
        format!(
            "{} published_total={:?} waiting={:?}",
            self.followers.show(),
            self.published,
            self.waiting
        )
    }
}

/// One follower instance frozen for the fault window, during load.
pub(crate) async fn instance_loss(h: &mut Harness) -> anyhow::Result<()> {
    let ctx = "follower-instance-loss";
    Followers::ready(h, ctx).await?;
    let posted0 = require_posting(h, ctx).await?;
    let (node, ip) = h
        .probes
        .follower_nodes()
        .last()
        .map(|n| (n.container.clone(), n.ip))
        .ok_or_else(|| crate::chaos_fail!("{ctx}: no second follower node"))?;
    let inner = h
        .nodes
        .inner_container(&node, TASK)
        .await
        .ok_or_else(|| crate::chaos_fail!("{ctx}: no follower instance runs on {node}"))?;
    let target = Target::bridged(ip, &node, INDEXER_PORT);
    let base = Watcher::read(h).await;
    h.freeze_verified(&node, &inner, &target, ctx).await?;
    crate::log(format!(
        "{ctx}: froze the follower instance {inner} on {node} for {}s ({})",
        h.knobs.l1_fault.as_secs(),
        base.show()
    ));
    let held = hold_without_pause(h, base, h.knobs.l1_fault, ctx).await;
    h.thaw(&node, &inner)
        .await
        .map_err(|e| crate::chaos_fail!("{ctx}: SIGCONT failed: {e}"))?;
    let during = held?;
    assert_one_epoch_per_block(during, base, ctx)?;
    assert_posted_through(h, posted0, 1, ctx).await?;
    assert_no_origin_gap(h, ctx).await
}

/// Through `window`, the da-watcher never pauses and never waits; at the
/// end, it published past `base`. Returns the last sample.
async fn hold_without_pause(
    h: &Harness,
    base: Watcher,
    window: Duration,
    ctx: &str,
) -> anyhow::Result<Watcher> {
    let deadline = tokio::time::Instant::now() + window;
    let mut last = base;
    while tokio::time::Instant::now() < deadline {
        last = sample_without_pause(h, ctx).await?;
    }
    anyhow::ensure!(
        last.origin() > base.origin(),
        "{}: {ctx}: the da-watcher published no epoch with one follower instance down ({})",
        crate::FAIL_PREFIX,
        last.show()
    );
    crate::log(format!(
        "{ctx}: the other instance carried the stream ({})",
        last.show()
    ));
    Ok(last)
}

/// One sample, two seconds apart; a pause or a wait fails the case.
async fn sample_without_pause(h: &Harness, ctx: &str) -> anyhow::Result<Watcher> {
    tokio::time::sleep(Duration::from_secs(2)).await;
    let now = Watcher::read(h).await;
    anyhow::ensure!(
        !now.followers.watcher_paused() && now.waiting.unwrap_or(0) == 0,
        "{}: {ctx}: the da-watcher paused or waits with one follower instance up ({})",
        crate::FAIL_PREFIX,
        now.show()
    );
    Ok(now)
}

/// Each block after `base` was published once: no gap, no double epoch.
fn assert_one_epoch_per_block(now: Watcher, base: Watcher, ctx: &str) -> anyhow::Result<()> {
    let counts = now.published_once_since(base);
    anyhow::ensure!(
        counts.is_some_and(|(blocks, publishes)| blocks == publishes),
        "{}: {ctx}: blocks and publishes since the start differ (blocks, publishes) = {counts:?} ({} -> {})",
        crate::FAIL_PREFIX,
        base.show(),
        now.show()
    );
    crate::log(format!(
        "{ctx}: one epoch per block (blocks, publishes) = {counts:?}"
    ));
    Ok(())
}

/// Both follower instances stopped past the silence window, then started.
pub(crate) async fn total_loss(h: &mut Harness) -> anyhow::Result<()> {
    let ctx = "follower-total-loss";
    Followers::ready(h, ctx).await?;
    require_posting(h, ctx).await?;
    let job = SavedJob::capture(&h.nomad, JOB).await?;
    let base = Watcher::read(h).await;
    crate::log(format!(
        "{ctx}: stop both follower instances for {}s ({})",
        TOTAL_LOSS_HOLD.as_secs(),
        base.show()
    ));
    job.stop().await?;
    let paused = await_watcher(h, TOTAL_LOSS_HOLD, ctx, "pause on the follower", |w| {
        w.followers.watcher_paused()
    })
    .await;
    // The stop lasts the whole hold, whenever the pause showed; the
    // follower comes back even when the case fails.
    let waited = paused.as_ref().map_or(TOTAL_LOSS_HOLD, |(_, took)| *took);
    tokio::time::sleep(TOTAL_LOSS_HOLD.saturating_sub(waited)).await;
    job.restore().await?;
    let (stuck, _) = paused?;
    crate::log(format!(
        "{ctx}: the da-watcher paused on the follower; the follower restarts ({})",
        stuck.show()
    ));
    let (resumed, _) = await_watcher(h, RESUME_BUDGET, ctx, "resume", |w| {
        !w.followers.watcher_paused() && w.origin() > stuck.origin()
    })
    .await?;
    assert_one_epoch_per_block(resumed, base, ctx)?;
    assert_no_origin_gap(h, ctx).await
}

/// The first sample within `budget` that `ok` accepts, and the time it
/// took.
async fn await_watcher(
    h: &Harness,
    budget: Duration,
    ctx: &str,
    what: &str,
    ok: impl Fn(Watcher) -> bool + Copy,
) -> anyhow::Result<(Watcher, Duration)> {
    let last = std::cell::Cell::new(None::<Watcher>);
    let last_ref = &last;
    let outcome = poll::until(
        Budget::new(budget, Duration::from_secs(2)),
        |_| async move {
            let now = Watcher::read(h).await;
            last_ref.set(Some(now));
            Ok::<_, anyhow::Error>(ok(now).then_some(now))
        },
    )
    .await?;
    let (now, elapsed) = outcome.or_fail(|t| {
        crate::chaos_fail!(
            "{ctx}: the da-watcher did not {what} within {}s (last: {})",
            t.as_secs(),
            last.get().map_or_else(|| "none".to_string(), Watcher::show)
        )
    })?;
    crate::log(format!(
        "{ctx}: the da-watcher's {what} after {}s ({})",
        elapsed.as_secs(),
        now.show()
    ));
    Ok((now, elapsed))
}
