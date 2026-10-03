//! The batcher's evidence: its confirmed-post counter, its start line,
//! and the lines a start on the old resume path would print.

use std::time::Duration;

use crate::harness::Harness;
use crate::l1::L1;
use crate::nomad::Streams;
use crate::poll::{self, Budget};
use crate::probes::BATCHER_PORT;

const POSTED: &str = "kardamom_batcher_batches_posted_total";
/// The start line of the live batcher, with its resolved cursor.
pub(super) const START_LINE: &str = "live batcher starting";
/// The lines of a start that waits on the indexer, or scans the logs
/// the endpoint swallowed: the two ways a start dies on a lying L1.
const OLD_RESUME_LINES: [&str; 2] = ["indexer behind L1; waiting", "no BatchPosted event with it"];
/// The line of a restart that found its pending group on disk.
pub(super) const SPOOL_RESTORED_LINE: &str = "pending group restored from the spool";
/// The line of a replay the sealers refused: the cursor is past the
/// retention floor.
pub(super) const REFUSED_LINE: &str = "cluster replay unavailable";

/// The confirmed posts of the running batcher: zero when the exporter
/// answers before its first post, `None` when it does not answer.
pub(super) async fn posted(h: &Harness) -> Option<i64> {
    h.probes.aux_metric_where(BATCHER_PORT, POSTED, "").await
}

/// The post counter moves past `base` within `budget`; the new value.
pub(super) async fn await_posting(
    h: &Harness,
    base: i64,
    budget: Duration,
    ctx: &str,
) -> anyhow::Result<i64> {
    let outcome = poll::until(
        Budget::new(budget, Duration::from_secs(3)),
        |_| async move { Ok::<_, anyhow::Error>(posted(h).await.filter(|p| *p > base)) },
    )
    .await?;
    let (now, elapsed) = outcome.or_fail(|t| {
        crate::chaos_fail!(
            "{ctx}: the batcher confirmed no post within {}s (counter at {base})",
            t.as_secs()
        )
    })?;
    crate::log(format!(
        "{ctx}: the batcher is posting ({base} -> {now} after {}s)",
        elapsed.as_secs()
    ));
    Ok(now)
}

/// The batcher is live: it confirms a post within a minute.
pub(super) async fn require_posting(h: &Harness, ctx: &str) -> anyhow::Result<i64> {
    let base = posted(h).await.unwrap_or(0);
    await_posting(h, base, Duration::from_secs(60), ctx).await
}

/// At least `min` posts confirmed since `base`: the batcher kept posting
/// through a window, instead of landing one post and stalling.
pub(super) async fn assert_posted_through(
    h: &Harness,
    base: i64,
    min: i64,
    ctx: &str,
) -> anyhow::Result<()> {
    let now = posted(h)
        .await
        .ok_or_else(|| crate::chaos_fail!("{ctx}: the batcher exporter does not answer"))?;
    anyhow::ensure!(
        now.saturating_sub(base) >= min,
        "{}: {ctx}: the batcher confirmed {} posts through the window, below {min}",
        crate::FAIL_PREFIX,
        now.saturating_sub(base)
    );
    crate::log(format!(
        "{ctx}: the batcher kept posting through the window ({} posts)",
        now.saturating_sub(base)
    ));
    Ok(())
}

/// How many batcher log lines hold `needle`, across every start.
pub(super) async fn count(h: &Harness, needle: &str) -> anyhow::Result<usize> {
    h.evidence
        .count_lines("batcher", needle, Streams::Both)
        .await
}

/// What a start must be measured against: the start-line count and the
/// old-path line counts before the restart, and the block L1 covered.
#[derive(Debug, Clone, Copy)]
pub(super) struct BeforeRestart {
    starts: usize,
    old_lines: usize,
    covered: u64,
}

impl BeforeRestart {
    pub(super) async fn read(h: &Harness, l1: &L1) -> anyhow::Result<Self> {
        let mut old_lines = 0;
        for needle in OLD_RESUME_LINES {
            old_lines += count(h, needle).await?;
        }
        Ok(Self {
            starts: count(h, START_LINE).await?,
            old_lines,
            covered: l1.covered_through().await?,
        })
    }
}

/// Kill the batcher task; Nomad restarts it in place.
pub(super) async fn restart(h: &mut Harness, ctx: &str) -> anyhow::Result<()> {
    let aux = h.probes.validator.container.clone();
    crate::log(format!("{ctx}: restarting the batcher (docker kill)"));
    h.inject_hard(&[&aux], "batcher").await?;
    h.assert_count("batcher", 1, h.knobs.restart_slo).await
}

/// The restarted batcher resumed from the contract: one new start line
/// within the budget, its covered block between what L1 held before the
/// kill and what it holds now, and no line of the old resume path (the
/// indexer wait, the swallowed-log scan).
pub(super) async fn assert_resumed_from_contract(
    h: &Harness,
    l1: &L1,
    before: BeforeRestart,
    ctx: &str,
) -> anyhow::Result<()> {
    let outcome = poll::until(Budget::secs(90, 3), |_| async move {
        let starts = count(h, START_LINE).await?;
        Ok::<_, anyhow::Error>((starts > before.starts).then_some(starts))
    })
    .await?;
    let (starts, elapsed) = outcome.or_fail(|t| {
        crate::chaos_fail!(
            "{ctx}: the restarted batcher logged no start line within {}s — it did not resume",
            t.as_secs()
        )
    })?;
    let logs = h.nomad.job_logs("batcher", Streams::Both).await?;
    let covered = covered_in_last_start(&logs)
        .ok_or_else(|| crate::chaos_fail!("{ctx}: the start line names no covered block"))?;
    let now = l1.covered_through().await?;
    anyhow::ensure!(
        (before.covered..=now).contains(&covered),
        "{}: {ctx}: the batcher resumed at covered block {covered}, but L1 covered {} before the kill and {now} now — the cursor did not come from the contract",
        crate::FAIL_PREFIX,
        before.covered
    );
    let mut old_lines = 0;
    for needle in OLD_RESUME_LINES {
        old_lines += logs.lines().filter(|l| l.contains(needle)).count();
    }
    anyhow::ensure!(
        old_lines == before.old_lines,
        "{}: {ctx}: the restarted batcher waited on the indexer or scanned the swallowed logs ({} old-path lines)",
        crate::FAIL_PREFIX,
        old_lines - before.old_lines
    );
    crate::log(format!(
        "{ctx}: the batcher resumed from the contract after {}s (start {starts}, covered block {covered}, L1 {}..={now}), with no wait on the indexer",
        elapsed.as_secs(),
        before.covered
    ));
    Ok(())
}

/// The `covered_through_block=` field of the last start line.
fn covered_in_last_start(logs: &str) -> Option<u64> {
    logs.lines()
        .rfind(|l| l.contains(START_LINE))?
        .split_whitespace()
        .find_map(|field| field.strip_prefix("covered_through_block="))
        .and_then(|v| v.parse().ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_last_start_line_names_the_covered_block() {
        let logs = "x live batcher starting covered_through_block=5 replay_from_block=6\n\
            noise\n\
            y live batcher starting last_batch_index=9 covered_through_block=45 chain_id=1\n";
        assert_eq!(covered_in_last_start(logs), Some(45));
        assert_eq!(covered_in_last_start("nothing"), None);
    }
}
