//! `batcher-outage-past-retention`: the batcher is frozen until the
//! sealers' egress floor passes its cursor and a snapshot lands, then
//! thawed. The thaw has one valid end. The freeze is longer than the
//! service interval of the Aeron clients of the batcher, so a client
//! times out and the process exits. The orchestrator restarts it, and it
//! posts the group it restores from its spool. The group lands right
//! after the covered block. The batcher then gets the rest of the gap
//! replayed, or rebuilds it from the state databases' block references
//! and the `tx_data` archives, and posts on past the sealers' floor. On a
//! carry-born cluster the batcher reads the egress, which keeps the gap,
//! so the replay of the egress serves it and no rebuild runs.

use std::cell::Cell;
use std::time::Duration;

use super::batcher::{
    AERON_EXIT_LINE, REBUILDING_LINE, REBUILT_LINE, SPOOL_RESTORED_LINE, START_LINE, count,
    field_in_last, require_posting,
};
use crate::evidence::StampedLog;
use crate::harness::Harness;
use crate::l1::{L1, Posted};
use crate::nomad::Streams;
use crate::poll::{self, Budget};
use crate::probes::CLUSTER_TASK;

mod freeze;
use freeze::{Held, freeze_with_spool};

/// The sealer's line for a snapshot, on every member.
const SNAPSHOT_LINE: &str = "snapshot TAKEN";
/// How long the rebuild of the gap may take past the restart: the
/// references of every block, and the bytes of every transaction from
/// the archives.
const REBUILD_BUDGET: Duration = Duration::from_secs(240);
/// The batcher's line for a post that L1 confirmed, with its block
/// range.
const CONFIRMED_LINE: &str = "batch confirmed on L1";
/// How long the frozen group may take to land on L1 once the batcher is
/// back: the spool restore after a restart, and one post.
const SPOOL_POST_BUDGET: Duration = Duration::from_secs(90);

/// The least hold of a floor pass when nothing else bounds it.
pub(super) const MIN_HOLD: Duration = Duration::from_secs(40);

/// Hold until the ingress delta passed twice the retention, `min_hold`
/// passed, and the sealers took a snapshot; the delta and the time.
pub(super) async fn hold_until_floor_passes(
    h: &Harness,
    rx0: i64,
    snapshots0: usize,
    min_hold: Duration,
    ctx: &str,
) -> anyhow::Result<(i64, Duration)> {
    let retention = h.knobs.cluster_retention.ok_or_else(|| {
        crate::chaos_fail!(
            "{ctx}: KARDAMOM_CLUSTER_RETENTION is not set — the floor cannot be known"
        )
    })?;
    let need = i64::try_from(retention.get().saturating_mul(2)).unwrap_or(i64::MAX);
    crate::log(format!(
        "{ctx}: holding until {need} frames flow past the cursor and a snapshot lands (cap {}s)",
        h.knobs.retention_freeze_cap.as_secs()
    ));
    let delta = Cell::new(0_i64);
    let delta_ref = &delta;
    let budget = Budget::new(h.knobs.retention_freeze_cap, Duration::from_secs(5));
    let outcome = poll::after_sleep(budget, |elapsed| async move {
        let d = h
            .probes
            .ingress_received()
            .await
            .unwrap_or(rx0)
            .saturating_sub(rx0);
        delta_ref.set(d);
        let snapshots = h
            .evidence
            .count_lines(CLUSTER_TASK, SNAPSHOT_LINE, Streams::StdoutOnly)
            .await?;
        let passed = d >= need && elapsed >= min_hold && snapshots > snapshots0;
        Ok::<_, anyhow::Error>(passed.then_some(d))
    })
    .await?;
    outcome.or_fail(|t| {
        crate::chaos_fail!(
            "{ctx}: the floor did not pass within {}s — only {} of {need} frames flowed, or no snapshot landed; raise the load rate or lower KARDAMOM_CLUSTER_RETENTION",
            t.as_secs(),
            delta.get()
        )
    })
}

/// The batcher's log counts that tell how it came through the thaw.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Lines {
    /// The start lines: one for each start of the process.
    starts: usize,
    /// The lines of a start that restored its pending group from the
    /// spool.
    restored: usize,
    /// The lines of the Aeron error handler: one for each handler call
    /// before an exit.
    exits: usize,
}

impl Lines {
    async fn read(h: &Harness) -> anyhow::Result<Self> {
        Ok(Self {
            starts: count(h, START_LINE).await?,
            restored: count(h, SPOOL_RESTORED_LINE).await?,
            exits: count(h, AERON_EXIT_LINE).await?,
        })
    }

    /// Fail unless the batcher restarted after `self`. A batcher that
    /// did not restart either logged the handler line and hung in its
    /// exit, or saw no Aeron error at all.
    fn require_restart(&self, now: Lines, ctx: &str) -> anyhow::Result<()> {
        match (now.starts > self.starts, now.exits > self.exits) {
            (true, _) => Ok(()),
            (false, true) => Err(crate::chaos_fail!(
                "{ctx}: the batcher logged '{AERON_EXIT_LINE}' after the thaw but kept running — the process hung in its exit"
            )),
            (false, false) => Err(crate::chaos_fail!(
                "{ctx}: the batcher kept running after the thaw and logged no '{AERON_EXIT_LINE}' — no Aeron client timeout fired"
            )),
        }
    }
}

/// The batcher at the freeze: the block L1 covered, and its log counts.
#[derive(Debug, Clone, Copy)]
struct AtFreeze {
    covered: u64,
    lines: Lines,
}

impl AtFreeze {
    /// The batch that carries the frozen group: the first batch on L1
    /// that ends past the covered block. More batches can land before
    /// the poll reads the record, so the last batch can be a later one.
    fn recovered_batch(&self, posted: &[Posted]) -> Option<Posted> {
        posted
            .iter()
            .find(|p| p.l2_block_end > self.covered)
            .copied()
    }

    /// True when `logs` hold a confirmed post of the block after the
    /// covered block, stamped before the last start line: the old
    /// process posted the frozen group between the thaw and its exit.
    fn old_process_posted(&self, logs: &str) -> bool {
        let Some(next) = self.covered.checked_add(1) else {
            return false;
        };
        let log = StampedLog(logs);
        let start_word = format!("l2_block_start={next}");
        let last_start = log.stamps(START_LINE, "").max();
        let confirmed = log.stamps(CONFIRMED_LINE, &start_word).min();
        matches!((confirmed, last_start), (Some(c), Some(s)) if c < s)
    }

    /// Judge the recovered batch and the log read after it. The batch
    /// must start right after the covered block: no block is lost and
    /// none is posted twice. The batcher must have restarted. The
    /// restarted batcher restored its group from the spool, or the old
    /// process posted the group before its exit.
    fn judge(&self, batch: Posted, after: AfterThaw, ctx: &str) -> anyhow::Result<()> {
        let now = after.lines;
        let next = self.covered.checked_add(1).ok_or_else(|| {
            crate::chaos_fail!("{ctx}: the covered block {} has no successor", self.covered)
        })?;
        anyhow::ensure!(
            batch.l2_block_start == next,
            "{}: {ctx}: the recovered batch {} starts at {}, not at {next}",
            crate::FAIL_PREFIX,
            batch.index,
            batch.l2_block_start
        );
        self.lines.require_restart(now, ctx)?;
        anyhow::ensure!(
            now.restored > self.lines.restored || after.old_posted,
            "{}: {ctx}: the batcher restarted after the thaw but never logged '{SPOOL_RESTORED_LINE}' — its spool was not restored",
            crate::FAIL_PREFIX
        );
        Ok(())
    }

    /// Wait until the batcher logs a start past the baseline, then read
    /// its log. The old process can post the frozen group after the
    /// thaw and before its exit, so the batch can land before the
    /// restart. A wait that runs out returns the last read: the judge
    /// names a missing restart.
    ///
    /// # Errors
    ///
    /// Returns an error if a log read fails.
    async fn await_restart(&self, h: &Harness, budget: Duration) -> anyhow::Result<AfterThaw> {
        let base = self.lines.starts;
        poll::until(
            Budget::new(budget, Duration::from_secs(5)),
            |_| async move {
                Ok::<_, anyhow::Error>((count(h, START_LINE).await? > base).then_some(()))
            },
        )
        .await?;
        let lines = Lines::read(h).await?;
        let logs = h.nomad.job_logs("batcher", Streams::Both).await?;
        Ok(AfterThaw {
            lines,
            old_posted: self.old_process_posted(&logs),
        })
    }
}

/// The batcher's log after the thaw: its counts, and whether the old
/// process confirmed the frozen group before the last start.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct AfterThaw {
    lines: Lines,
    old_posted: bool,
}

/// The frozen group lands on L1 right after the covered block: from the
/// spool of the restarted batcher, or from the old process before its
/// exit. `restart` is the time a restart after the thaw may take.
async fn await_spool_posted(
    h: &Harness,
    l1: &L1,
    at: AtFreeze,
    restart: Duration,
    ctx: &str,
) -> anyhow::Result<()> {
    let at = &at;
    let outcome = poll::until(
        Budget::new(restart + SPOOL_POST_BUDGET, Duration::from_secs(5)),
        |_| async move { Ok::<_, anyhow::Error>(at.recovered_batch(&l1.posted().await?)) },
    )
    .await?;
    let (batch, _) = outcome.or_fail(|t| {
        crate::chaos_fail!(
            "{ctx}: no batch landed past block {} within {}s — the frozen group was not posted",
            at.covered,
            t.as_secs()
        )
    })?;
    let after = at.await_restart(h, restart).await?;
    at.judge(batch, after, ctx)?;
    let by = if after.old_posted {
        "the old process posted the frozen group before its exit"
    } else {
        "the restarted batcher posted the frozen group from its spool"
    };
    crate::log(format!(
        "{ctx}: {by}: batch {} covers {}..={} after the covered block {}",
        batch.index, batch.l2_block_start, batch.l2_block_end, at.covered
    ));
    Ok(())
}

pub(crate) async fn batcher_outage_past_retention(h: &mut Harness) -> anyhow::Result<()> {
    let ctx = "batcher-outage-past-retention";
    let l1 = L1::new(h).await?;
    let aux = h.probes.validator.container.clone();
    require_posting(h, ctx).await?;
    let rx0 = h
        .probes
        .ingress_counts()
        .await
        .complete()
        .ok_or_else(|| crate::chaos_fail!("{ctx}: no complete ingress baseline"))?;
    let snapshots0 = h
        .evidence
        .count_lines(CLUSTER_TASK, SNAPSHOT_LINE, Streams::StdoutOnly)
        .await?;
    let Held { frozen, covered } = freeze_with_spool(h, &l1, &aux, ctx).await?;
    crate::log(format!(
        "{ctx}: batcher frozen with {} spooled blocks through {}, L1 covered through {covered}",
        frozen.blocks, frozen.last_block
    ));
    // The thaw runs on every path: Nomad does not restart a stopped
    // process, so a failed hold must not leave the batcher frozen.
    let held = FrozenHold::hold(h, rx0, snapshots0, covered, ctx).await;
    if h.thaw(&aux, &frozen.inner).await.is_err() {
        crate::log(format!(
            "{ctx}: SIGCONT failed (the task was replaced mid-freeze); the log asserts own the verdict"
        ));
    }
    let FrozenHold { at, lines, sealed } = held?;
    // The time a restart of the batcher after the thaw may take.
    let budget = h.knobs.restart_slo + Duration::from_secs(60);
    await_spool_posted(h, &l1, at, budget, ctx).await?;
    if !await_rebuilt_or_served(h, &l1, lines, sealed, budget + REBUILD_BUDGET, ctx).await? {
        return l1.assert_contiguous(ctx).await;
    }
    anyhow::ensure!(
        !h.knobs.genesis_carry,
        "{}: {ctx}: the batcher reads the egress, which keeps the gap, but it rebuilt the gap from references",
        crate::FAIL_PREFIX
    );
    let logs = h.nomad.job_logs("batcher", Streams::Both).await?;
    let floor = field_in_last(&logs, REBUILDING_LINE, "oldest_block").ok_or_else(|| {
        crate::chaos_fail!("{ctx}: the rebuild line names no oldest_block (the sealers' floor)")
    })?;
    await_covered_through(&l1, floor, ctx).await?;
    l1.assert_contiguous(ctx).await
}

/// What the frozen half of `batcher-outage-past-retention` hands to the
/// thaw: the state at the freeze, the rebuild line counts, and the
/// executor head at the thaw.
struct FrozenHold {
    at: AtFreeze,
    lines: Baselines,
    sealed: u64,
}

impl FrozenHold {
    /// Read the baselines of the frozen batcher, and hold the freeze
    /// until the floor passes. The baselines follow the freeze: a
    /// restart on a retried freeze logs its own lines, which must not
    /// count for the final thaw.
    async fn hold(
        h: &Harness,
        rx0: i64,
        snapshots0: usize,
        covered: u64,
        ctx: &str,
    ) -> anyhow::Result<Self> {
        let lines = Lines::read(h).await?;
        let baselines = Baselines {
            rebuilding: count(h, REBUILDING_LINE).await?,
            rebuilt: count(h, REBUILT_LINE).await?,
        };
        // The freeze must outlast the service interval of the Aeron
        // clients of the batcher, or the thaw does not end the process.
        let min_hold = h.knobs.aeron_stall.evicting_freeze().max(MIN_HOLD);
        let (delta, held) = hold_until_floor_passes(h, rx0, snapshots0, min_hold, ctx).await?;
        crate::log(format!(
            "{ctx}: the floor passed ({delta} frames in {}s); thawing",
            held.as_secs()
        ));
        let sealed = h
            .probes
            .executor_progress()
            .await
            .ok_or_else(|| crate::chaos_fail!("{ctx}: no executor head at the thaw"))?;
        let sealed = u64::try_from(sealed)
            .map_err(|e| crate::chaos_fail!("{ctx}: the executor head is not a block: {e}"))?;
        Ok(Self {
            at: AtFreeze { covered, lines },
            lines: baselines,
            sealed,
        })
    }
}

/// The batcher log counts before the thaw.
#[derive(Clone, Copy)]
struct Baselines {
    rebuilding: usize,
    rebuilt: usize,
}

/// Wait for one of the two ends of the outage. The sealers refused the
/// replay and the batcher rebuilt the gap from references (`true`). Or
/// the sealers, or the egress, kept every frame above the posted head,
/// served the replay, and L1 covers through `sealed`, the head at the
/// thaw, with no refusal on the way (`false`). In reference mode the
/// retention never prunes below the posted head, so the second is the
/// expected end; the first stays valid for a sealer that prunes by the
/// window alone. The exact window of a carry-born sealer prunes by the
/// window alone, but its batcher reads the egress, so the second is the
/// only valid end there.
async fn await_rebuilt_or_served(
    h: &Harness,
    l1: &L1,
    base: Baselines,
    sealed: u64,
    budget: Duration,
    ctx: &str,
) -> anyhow::Result<bool> {
    let outcome = poll::until(
        Budget::new(budget, Duration::from_secs(5)),
        |_| async move {
            if count(h, REBUILT_LINE).await? > base.rebuilt {
                return Ok::<_, anyhow::Error>(Some(true));
            }
            let served = l1.covered_through().await? >= sealed
                && count(h, REBUILDING_LINE).await? == base.rebuilding;
            Ok(served.then_some(false))
        },
    )
    .await?;
    let (rebuilt, elapsed) = outcome.or_fail(|t| {
        crate::chaos_fail!(
            "{ctx}: within {}s the batcher neither rebuilt a refused gap nor posted through block {sealed}, the head at the thaw",
            t.as_secs()
        )
    })?;
    if !rebuilt {
        crate::log(format!(
            "{ctx}: the sealers kept the range above the posted head and served the replay: L1 covers through {sealed} with no refusal ({}s)",
            elapsed.as_secs()
        ));
    }
    Ok(rebuilt)
}

/// L1 covers through `floor`: the rebuilt gap and the sealers' floor
/// block are posted.
async fn await_covered_through(l1: &L1, floor: u64, ctx: &str) -> anyhow::Result<()> {
    let outcome = poll::until(Budget::secs(180, 5), |_| async move {
        let covered = l1.covered_through().await?;
        Ok::<_, anyhow::Error>((covered >= floor).then_some(covered))
    })
    .await?;
    let (covered, elapsed) = outcome.or_fail(|t| {
        crate::chaos_fail!(
            "{ctx}: L1 did not cover the sealers' floor block {floor} within {}s — the rebuilt gap was not posted",
            t.as_secs()
        )
    })?;
    crate::log(format!(
        "{ctx}: the gap was rebuilt and posted: L1 covers through {covered}, past the floor {floor} ({}s)",
        elapsed.as_secs()
    ));
    Ok(())
}

#[cfg(test)]
mod tests;
