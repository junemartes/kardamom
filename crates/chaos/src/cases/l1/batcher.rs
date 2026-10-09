//! The batcher's evidence: the waits on its confirmed-post counter, its
//! start line, and the lines a start on the old resume path would print.

use std::time::Duration;

use crate::harness::Harness;
use crate::l1::L1;
use crate::nomad::Streams;
use crate::poll::{self, Budget};

/// The start line of the live batcher, with its resolved cursor.
pub(super) const START_LINE: &str = "live batcher starting";
/// The lines of a start that waits on the indexer, or scans the logs
/// the endpoint swallowed: the two ways a start dies on a lying L1.
const OLD_RESUME_LINES: [&str; 2] = ["indexer behind L1; waiting", "no BatchPosted event with it"];
/// The line of the Aeron error handler of every Rust service, just
/// before the process exits.
pub(super) const AERON_EXIT_LINE: &str = "aeron client error; exiting";
/// The line of a restart that found its pending group on disk.
pub(super) const SPOOL_RESTORED_LINE: &str = "pending group restored from the spool";
/// The line of a replay the sealers refused, with the floor block
/// (`oldest_block=`): the rebuild from references starts.
pub(super) const REBUILDING_LINE: &str = "sealer replay refused; rebuilding the gap";
/// The line of a rebuilt gap: the reader resumes at the sealers' floor.
pub(super) const REBUILT_LINE: &str = "gap rebuilt; resuming at the sealer's floor";

/// The line of a batcher start on the executor stream.
const EXEC_SOURCE_LINE: &str = "kardamom-batcher: transaction source exec-stream";
/// The line of a rebuild that reads the executor archives, with the count
/// of wanted records (`wanted=`).
const EXEC_REBUILD_LINE: &str = "rebuild: reading the executor archives";
/// The line of one replay of an executor archive during a rebuild.
const EXEC_REPLAY_LINE: &str = "rebuild: executor archive replay";

/// The executor stream evidence of the batcher, when the deploy runs it on
/// that source: the replay lines before a rebuild.
#[derive(Debug, Clone, Copy)]
pub(super) struct ExecSource {
    replays: usize,
}

impl ExecSource {
    /// `None` when the deploy runs the batcher on `tx_data`. Else fail
    /// unless the batcher logged a start on the executor stream, and keep
    /// the count of replay lines.
    pub(super) async fn read(h: &Harness, ctx: &str) -> anyhow::Result<Option<Self>> {
        if !h.knobs.batcher_exec_stream {
            return Ok(None);
        }
        anyhow::ensure!(
            count(h, EXEC_SOURCE_LINE).await? > 0,
            "{}: {ctx}: the deploy runs the batcher on the executor stream, but it never logged '{EXEC_SOURCE_LINE}'",
            crate::FAIL_PREFIX
        );
        crate::log(format!("{ctx}: the batcher reads the executor stream"));
        Ok(Some(Self {
            replays: count(h, EXEC_REPLAY_LINE).await?,
        }))
    }

    /// Fail unless the rebuild read the executor archives: a replay line
    /// past the baseline, or a rebuild that wanted no record.
    pub(super) async fn require_rebuild_read(&self, h: &Harness, ctx: &str) -> anyhow::Result<()> {
        let logs = h.nomad.job_logs("batcher", Streams::Both).await?;
        let replays = logs
            .lines()
            .filter(|l| l.contains(EXEC_REPLAY_LINE))
            .count();
        let wanted = field_in_last(&logs, EXEC_REBUILD_LINE, "wanted");
        self.judge(replays, wanted, ctx)
    }

    /// The verdict of [`Self::require_rebuild_read`] on the counts read.
    fn judge(self, replays: usize, wanted: Option<u64>, ctx: &str) -> anyhow::Result<()> {
        // Saturating: a log that the job rotated holds fewer lines, which
        // counts as no new line.
        let new = replays.saturating_sub(self.replays);
        match (new, wanted) {
            (0, Some(0)) => {
                crate::log(format!(
                    "{ctx}: the rebuilt gap held no transaction; no executor archive replay was due"
                ));
                Ok(())
            }
            (0, _) => Err(crate::chaos_fail!(
                "{ctx}: the batcher runs on the executor stream, but its rebuild logged no '{EXEC_REPLAY_LINE}' (wanted records: {wanted:?}) — it did not read the executor archives"
            )),
            (new, _) => {
                crate::log(format!(
                    "{ctx}: the rebuild read the executor archives ({new} replays)"
                ));
                Ok(())
            }
        }
    }
}

/// The post counter moves past `base` within `budget`; the new value.
pub(super) async fn await_posting(
    h: &Harness,
    base: i64,
    budget: Duration,
    ctx: &str,
) -> anyhow::Result<i64> {
    let outcome =
        poll::until(
            Budget::new(budget, Duration::from_secs(3)),
            |_| async move {
                Ok::<_, anyhow::Error>(h.probes.batcher_posts().await.filter(|p| *p > base))
            },
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
    let base = h.probes.batcher_posts().await.unwrap_or(0);
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
    let now = h
        .probes
        .batcher_posts()
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
    let covered = field_in_last(&logs, START_LINE, "covered_through_block")
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

/// The numeric `field=` of the last log line that holds `needle`. The
/// batcher's log colors each field name and its `=` with ANSI escapes,
/// so the line is read without them.
pub(super) fn field_in_last(logs: &str, needle: &str, field: &str) -> Option<u64> {
    let prefix = format!("{field}=");
    without_ansi(logs.lines().rfind(|l| l.contains(needle))?)
        .split_whitespace()
        .find_map(|word| word.strip_prefix(prefix.as_str()).map(str::to_string))
        .and_then(|v| v.parse().ok())
}

/// `line` without its ANSI escape sequences (`ESC [` up to the final
/// letter).
fn without_ansi(line: &str) -> String {
    line.split('\x1b')
        .enumerate()
        .map(|(i, part)| match i {
            0 => part,
            _ => part
                .strip_prefix('[')
                .and_then(|rest| {
                    rest.find(|c: char| c.is_ascii_alphabetic())
                        .map(|end| &rest[end + 1..])
                })
                .unwrap_or(part),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_last_start_line_names_the_covered_block() {
        let logs = "x live batcher starting covered_through_block=5 replay_from_block=6\n\
            noise\n\
            y live batcher starting last_batch_index=9 covered_through_block=45 chain_id=1\n";
        assert_eq!(
            field_in_last(logs, START_LINE, "covered_through_block"),
            Some(45)
        );
        assert_eq!(
            field_in_last("nothing", START_LINE, "covered_through_block"),
            None
        );
        let colored = "\x1b[32m INFO\x1b[0m live batcher starting \x1b[3mlast_batch_index\x1b[0m\x1b[2m=\x1b[0m0 \x1b[3mcovered_through_block\x1b[0m\x1b[2m=\x1b[0m7 \x1b[3mchain_id\x1b[0m\x1b[2m=\x1b[0m1\n";
        assert_eq!(
            field_in_last(colored, START_LINE, "covered_through_block"),
            Some(7)
        );
    }

    #[test]
    fn a_rebuild_on_the_executor_stream_must_replay_an_executor_archive() {
        let ctx = "batcher-outage-past-retention";
        let before = ExecSource { replays: 2 };
        before.judge(3, Some(40), ctx).unwrap();
        before.judge(2, Some(0), ctx).unwrap();
        let err = before.judge(2, Some(40), ctx).unwrap_err();
        assert!(
            err.to_string()
                .contains("did not read the executor archives"),
            "{err}"
        );
        assert!(before.judge(2, None, ctx).is_err());
    }
}
