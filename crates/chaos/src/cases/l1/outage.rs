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
use crate::probes::{BATCHER_PORT, CLUSTER_TASK};

/// The sealer's line for a snapshot, on every member.
const SNAPSHOT_LINE: &str = "snapshot TAKEN";
/// How long the rebuild of the gap may take past the restart: the
/// references of every block, and the bytes of every transaction from
/// the archives.
const REBUILD_BUDGET: Duration = Duration::from_secs(240);
/// How many times the freeze is retried to land on an unposted group.
const FREEZE_ATTEMPTS: u32 = 10;
/// How long a freeze attempt holds the batcher before it reads the
/// covered block. A post in flight at the freeze lands on L1 in this
/// time. The attempt then stays under the Aeron client's service
/// interval (10 s), so a thaw for a retry does not restart the batcher.
const SETTLE: Duration = Duration::from_secs(5);
/// The batcher's line for a post that L1 confirmed, with its block
/// range.
const CONFIRMED_LINE: &str = "batch confirmed on L1";
/// How long the frozen group may take to land on L1 once the batcher is
/// back: the spool restore after a restart, and one post.
const SPOOL_POST_BUDGET: Duration = Duration::from_secs(90);

/// The spool root of the batcher on the aux node: its `--spool-dir`.
const SPOOL_ROOT: &str = "/opt/kardamom/batcher/spool";

/// The format registry of this build. The batcher checks its
/// `SPOOL_VERSION` against the `batcher-spool` entry, so the directory of
/// its spool follows from this entry.
const FORMATS: &str = include_str!("../../../../../formats.toml");

/// One freeze attempt on the aux node: find the running batcher
/// container, then in one exec SIGSTOP it, read its process state from
/// `/proc`, list the block files of its spool, and SIGCONT it again
/// unless it is stopped with a non-empty spool. The exec takes about one
/// second. A stopped batcher then waits `SETTLE` for the covered block.
/// A scrape of the frozen exporter waits for its timeout, and does not
/// fit. Each attempt finds the container again: a restart between two
/// attempts can replace it.
struct FreezeAttempt<'a> {
    aux: &'a str,
    /// The directory of the spool that the batcher of this build writes.
    spool: String,
}

impl<'a> FreezeAttempt<'a> {
    /// The attempt on the aux node `aux`, against the spool directory of
    /// this build.
    ///
    /// # Errors
    ///
    /// Returns an error when the registry does not load or lists no
    /// `batcher-spool`.
    fn new(aux: &'a str) -> anyhow::Result<Self> {
        let versions = kardamom_formats::Registry::parse(FORMATS)?
            .versions("batcher-spool")
            .ok_or_else(|| anyhow::anyhow!("the format registry lists no batcher-spool"))?;
        Ok(Self {
            aux,
            spool: format!("{SPOOL_ROOT}/v{}", versions.writes),
        })
    }

    /// The shell command that prints the count of block files in the
    /// spool directory and the number of the highest block, and `0 0`
    /// when the directory does not exist. The file names are zero-padded
    /// block numbers, so `ls` lists them in block order.
    fn count_command(&self) -> String {
        format!(
            "blocks=$(ls {} 2>/dev/null | grep '\\.block$'); last=$(printf '%s\\n' \"$blocks\" | tail -n 1); last=${{last%.block}}; echo \"$(printf '%s' \"$blocks\" | grep -c .) ${{last:-0}}\"",
            self.spool
        )
    }

    /// The shell script of the attempt on the container `inner`. It
    /// prints the process state, the count of spooled blocks and the
    /// highest spooled block, or `gone 0 0` when the container is gone
    /// before the signal.
    fn script(&self, inner: &str) -> String {
        let count = self.count_command();
        format!(
            "docker kill -s STOP {inner} >/dev/null 2>&1 || {{ echo 'gone 0 0'; exit 0; }}
pid=$(docker inspect -f '{{{{.State.Pid}}}}' {inner})
state=?
for _ in $(seq 1 40); do
  state=$(sed 's/.*) //' /proc/$pid/stat | cut -d' ' -f1)
  [ \"$state\" = T ] && break
  sleep 0.05
done
spool=$({count})
if [ \"$state\" != T ] || [ \"${{spool%% *}}\" = 0 ]; then docker kill -s CONT {inner} >/dev/null; fi
echo \"$state $spool\""
        )
    }

    /// Run the attempt. `Some` when the batcher stays frozen with a
    /// non-empty spool; `None` when no batcher container runs, or the
    /// spool was empty and the batcher runs again.
    ///
    /// # Errors
    ///
    /// Returns an error if the exec fails, or if the process is not
    /// stopped after the signal.
    async fn run(&self, h: &Harness, ctx: &str) -> anyhow::Result<Option<Frozen>> {
        let Some(inner) = h.nodes.inner_container(self.aux, "batcher").await else {
            crate::log(format!(
                "{ctx}: no batcher container runs on {}; the freeze waits for it",
                self.aux
            ));
            return Ok(None);
        };
        let out = h.nodes.exec(self.aux, &self.script(&inner)).await?;
        Frozen::parse(&out, inner, ctx)
    }

    /// Hold the stopped batcher for `SETTLE`, so a post in flight at the
    /// freeze lands, then read the block L1 covers. Keep the freeze when
    /// the spool holds a block past it. Else thaw the batcher for a
    /// retry: the restart drops a spool that L1 covers, so the thaw has
    /// no group to recover.
    ///
    /// # Errors
    ///
    /// Returns an error if the L1 read fails.
    async fn settle(
        &self,
        h: &Harness,
        l1: &L1,
        frozen: Frozen,
        ctx: &str,
    ) -> anyhow::Result<Option<Held>> {
        tokio::time::sleep(SETTLE).await;
        let covered = l1.covered_through().await?;
        if frozen.holds_unposted(covered) {
            return Ok(Some(Held { frozen, covered }));
        }
        crate::log(format!(
            "{ctx}: the spool of {} ends at block {}, and L1 covers through {covered}; thawing to retry",
            frozen.inner, frozen.last_block
        ));
        if h.thaw(self.aux, &frozen.inner).await.is_err() {
            crate::log(format!(
                "{ctx}: SIGCONT failed (the task was replaced mid-freeze); the next attempt finds the new container"
            ));
        }
        Ok(None)
    }

    /// One attempt to the end: freeze, then settle. `None` when the
    /// batcher runs again.
    ///
    /// # Errors
    ///
    /// Returns an error if the exec or the L1 read fails, or if the
    /// process is not stopped after the signal.
    async fn hold(&self, h: &Harness, l1: &L1, ctx: &str) -> anyhow::Result<Option<Held>> {
        match self.run(h, ctx).await? {
            Some(frozen) => self.settle(h, l1, frozen, ctx).await,
            None => Ok(None),
        }
    }
}

/// A batcher stopped with a non-empty spool: its container, the count
/// of spooled blocks, and the highest spooled block.
struct Frozen {
    inner: String,
    blocks: usize,
    last_block: u64,
}

impl Frozen {
    /// Parse the attempt's output, `<state> <blocks> <last block>`.
    /// `None` when the container was gone or the spool was empty.
    fn parse(out: &str, inner: String, ctx: &str) -> anyhow::Result<Option<Self>> {
        let mut words = out.split_whitespace();
        let (Some(state), Some(blocks), Some(last), None) =
            (words.next(), words.next(), words.next(), words.next())
        else {
            return Err(crate::chaos_fail!(
                "{ctx}: the freeze attempt printed no state, count and last block: {out:?}"
            ));
        };
        let blocks: usize = blocks
            .parse()
            .map_err(|e| crate::chaos_fail!("{ctx}: spool listing is not a count: {e}"))?;
        let last_block: u64 = last.parse().map_err(|e| {
            crate::chaos_fail!("{ctx}: the last spooled block is not a number: {e}")
        })?;
        match state {
            "gone" => Ok(None),
            "T" => Ok((blocks > 0).then_some(Self {
                inner,
                blocks,
                last_block,
            })),
            _ => Err(crate::chaos_fail!(
                "{ctx}: freeze did NOT take effect (process state {state:?} after SIGSTOP, not T)"
            )),
        }
    }

    /// True when the spool holds a block past `covered`: a group that L1
    /// does not hold. A spool of only the batch in flight at the freeze
    /// holds none once L1 confirms that batch.
    fn holds_unposted(&self, covered: u64) -> bool {
        self.last_block > covered
    }
}

/// A frozen batcher whose spool holds an unposted group, and the block
/// L1 covered while it was frozen.
struct Held {
    frozen: Frozen,
    covered: u64,
}

/// Freeze the batcher with an unposted group in its spool, so the thaw
/// has a group to recover. The spool empties for an instant after every
/// post, and the batch in flight at the freeze can land during it. An
/// attempt that holds no unposted group thaws the batcher and tries
/// again.
async fn freeze_with_spool(h: &Harness, l1: &L1, aux: &str, ctx: &str) -> anyhow::Result<Held> {
    let attempt = FreezeAttempt::new(aux)?;
    let attempt = &attempt;
    let target = h.probes.aux_target(BATCHER_PORT);
    let target = &target;
    let outcome = poll::until(
        Budget::new(
            (Duration::from_secs(4) + SETTLE + h.knobs.restart_slo) * FREEZE_ATTEMPTS,
            Duration::from_secs(1),
        ),
        |_| async move {
            let held = attempt.hold(h, l1, ctx).await?;
            if held.is_none() {
                await_answering(h, target, ctx).await?;
            }
            Ok::<_, anyhow::Error>(held)
        },
    )
    .await?;
    let (held, _) = outcome.or_fail(|_| {
        crate::chaos_fail!(
            "{ctx}: the spool held no block past the covered block, or no batcher ran, on every freeze"
        )
    })?;
    crate::log(format!(
        "{ctx}: freeze verified (process state T, {} spooled blocks through {} in {})",
        held.frozen.blocks, held.frozen.last_block, held.frozen.inner
    ));
    Ok(held)
}

/// Wait until the batcher's metrics endpoint answers: after a thaw, the
/// task can restart, and its container is gone until the new one runs.
async fn await_answering(
    h: &Harness,
    target: &crate::metrics::Target,
    ctx: &str,
) -> anyhow::Result<()> {
    let budget = Budget::new(h.knobs.restart_slo, Duration::from_secs(1));
    poll::until(budget, |_| async move {
        Ok::<_, anyhow::Error>(h.probes.scrape().answers(target).await.then_some(()))
    })
    .await?
    .or_fail(|t| {
        crate::chaos_fail!(
            "{ctx}: the batcher did not answer within {}s after a thaw",
            t.as_secs()
        )
    })
    .map(|_| ())
}

/// The least hold of a floor pass when nothing else bounds it.
pub(super) const MIN_HOLD: Duration = Duration::from_mins(1);

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
    // The baselines follow the freeze: a restart on a retried freeze
    // logs its own lines, which must not count for the final thaw.
    let lines = Lines::read(h).await?;
    let rebuilding0 = count(h, REBUILDING_LINE).await?;
    let rebuilt0 = count(h, REBUILT_LINE).await?;
    let at = AtFreeze { covered, lines };
    crate::log(format!(
        "{ctx}: batcher frozen with {} spooled blocks through {}, L1 covered through {}",
        frozen.blocks, frozen.last_block, at.covered
    ));
    // The freeze must outlast the service interval of the Aeron clients
    // of the batcher, or the thaw does not end the process.
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
    if h.thaw(&aux, &frozen.inner).await.is_err() {
        crate::log(format!(
            "{ctx}: SIGCONT failed (the task was replaced mid-freeze); the log asserts own the verdict"
        ));
    }
    // The time a restart of the batcher after the thaw may take.
    let budget = h.knobs.restart_slo + Duration::from_secs(60);
    await_spool_posted(h, &l1, at, budget, ctx).await?;
    let lines = Baselines {
        rebuilding: rebuilding0,
        rebuilt: rebuilt0,
    };
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
