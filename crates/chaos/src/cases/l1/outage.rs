//! `batcher-outage-past-retention`: the batcher is frozen until the
//! sealers' egress floor passes its cursor and a snapshot lands, then
//! thawed. Its restart posts what its spool held, rebuilds the rest of
//! the gap from the state databases' block references and the `tx_data`
//! archives, and posts on past the sealers' floor.

use std::cell::Cell;
use std::time::Duration;

use super::batcher::{
    REBUILDING_LINE, REBUILT_LINE, SPOOL_RESTORED_LINE, count, field_in_last, require_posting,
};
use crate::harness::Harness;
use crate::l1::L1;
use crate::nomad::Streams;
use crate::poll::{self, Budget};
use crate::probes::{BATCHER_PORT, CLUSTER_TASK};

/// The sealer's line for a snapshot, on every member.
const SNAPSHOT_LINE: &str = "snapshot TAKEN";
/// How long the rebuild of the gap may take past the restart: the
/// references of every block, and the bytes of every transaction from
/// the archives.
const REBUILD_BUDGET: Duration = Duration::from_secs(240);
/// How many times the freeze is retried to land on a non-empty spool.
const FREEZE_ATTEMPTS: u32 = 10;

/// The spool of the batcher on the aux node.
const SPOOL_DIR: &str = "/opt/kardamom/batcher/spool";

/// One freeze attempt on the aux node, in one exec: SIGSTOP the batcher,
/// read its process state from `/proc`, count the spool's block files,
/// and SIGCONT it again unless it is stopped with a non-empty spool. The
/// attempt takes about one second. So a retried attempt stays far under
/// the Aeron client's service interval (10 s), and the thaw does not
/// restart the batcher. A scrape of the frozen exporter waits for its
/// timeout, and does not fit.
struct FreezeAttempt<'a> {
    aux: &'a str,
    inner: &'a str,
}

impl FreezeAttempt<'_> {
    /// The shell script of the attempt. It prints the process state and
    /// the count of spooled blocks.
    fn script(&self) -> String {
        let inner = self.inner;
        format!(
            "docker kill -s STOP {inner} >/dev/null || exit 1
pid=$(docker inspect -f '{{{{.State.Pid}}}}' {inner})
state=?
for _ in $(seq 1 40); do
  state=$(sed 's/.*) //' /proc/$pid/stat | cut -d' ' -f1)
  [ \"$state\" = T ] && break
  sleep 0.05
done
blocks=$(ls {SPOOL_DIR} 2>/dev/null | grep -c '\\.block$' || true)
if [ \"$state\" != T ] || [ \"$blocks\" = 0 ]; then docker kill -s CONT {inner} >/dev/null; fi
echo \"$state $blocks\""
        )
    }

    /// Run the attempt. `Some(blocks)` when the batcher stays frozen with
    /// `blocks` spooled blocks; `None` when the spool was empty and the
    /// batcher runs again.
    ///
    /// # Errors
    ///
    /// Returns an error if the exec fails, or if the process is not
    /// stopped after the signal.
    async fn run(&self, h: &Harness, ctx: &str) -> anyhow::Result<Option<usize>> {
        let out = h.nodes.exec(self.aux, &self.script()).await?;
        let (state, blocks) = out.trim().split_once(' ').ok_or_else(|| {
            crate::chaos_fail!("{ctx}: the freeze attempt printed no state and count: {out:?}")
        })?;
        anyhow::ensure!(
            state == "T",
            "{}: {ctx}: freeze did NOT take effect (process state {state:?} after SIGSTOP, not T)",
            crate::FAIL_PREFIX
        );
        let blocks: usize = blocks
            .parse()
            .map_err(|e| crate::chaos_fail!("{ctx}: spool listing is not a count: {e}"))?;
        Ok((blocks > 0).then_some(blocks))
    }
}

/// Freeze the batcher with a non-empty spool, so the restart has a group
/// to recover. The spool empties for an instant after every post, so an
/// attempt that lands in that instant thaws the batcher and tries again.
async fn freeze_with_spool(
    h: &Harness,
    aux: &str,
    inner: &str,
    ctx: &str,
) -> anyhow::Result<usize> {
    let attempt = FreezeAttempt { aux, inner };
    let attempt = &attempt;
    let target = h.probes.aux_target(BATCHER_PORT);
    let target = &target;
    let outcome = poll::until(
        Budget::new(
            (Duration::from_secs(4) + h.knobs.restart_slo) * FREEZE_ATTEMPTS,
            Duration::from_secs(1),
        ),
        |_| async move {
            let frozen = attempt.run(h, ctx).await?;
            if frozen.is_none() {
                await_answering(h, target, ctx).await?;
            }
            Ok::<_, anyhow::Error>(frozen)
        },
    )
    .await?;
    let (blocks, _) =
        outcome.or_fail(|_| crate::chaos_fail!("{ctx}: the spool was empty on every freeze"))?;
    crate::log(format!(
        "{ctx}: freeze verified (process state T, {blocks} spooled blocks)"
    ));
    Ok(blocks)
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

/// Hold until the ingress delta passed twice the retention, two minutes
/// passed, and the sealers took a snapshot; the delta and the time.
pub(super) async fn hold_until_floor_passes(
    h: &Harness,
    rx0: i64,
    snapshots0: usize,
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
    let budget = Budget::new(h.knobs.retention_freeze_cap, Duration::from_secs(15));
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
        let passed = d >= need && elapsed >= Duration::from_mins(2) && snapshots > snapshots0;
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

/// The count of `needle` in the batcher's logs rises past `base`.
async fn await_line(
    h: &Harness,
    needle: &str,
    base: usize,
    budget: Duration,
    ctx: &str,
) -> anyhow::Result<()> {
    let outcome = poll::until(
        Budget::new(budget, Duration::from_secs(5)),
        |_| async move { Ok::<_, anyhow::Error>((count(h, needle).await? > base).then_some(())) },
    )
    .await?;
    let ((), elapsed) = outcome.or_fail(|t| {
        crate::chaos_fail!(
            "{ctx}: the batcher never logged '{needle}' within {}s",
            t.as_secs()
        )
    })?;
    crate::log(format!(
        "{ctx}: the batcher logged '{needle}' after {}s",
        elapsed.as_secs()
    ));
    Ok(())
}

/// The spooled group lands on L1 right after the covered block the
/// batcher was frozen at.
async fn await_spool_posted(l1: &L1, covered0: u64, ctx: &str) -> anyhow::Result<()> {
    let outcome = poll::until(Budget::secs(90, 5), |_| async move {
        let last = l1.last_batch_index().await?;
        let (start, end) = l1.batch(last).await?;
        Ok::<_, anyhow::Error>((end > covered0).then_some((start, end)))
    })
    .await?;
    let ((start, end), _) = outcome.or_fail(|t| {
        crate::chaos_fail!(
            "{ctx}: no batch landed past block {covered0} within {}s — the spool was not recovered",
            t.as_secs()
        )
    })?;
    anyhow::ensure!(
        start == covered0 + 1,
        "{}: {ctx}: the recovered batch starts at {start}, not at {}",
        crate::FAIL_PREFIX,
        covered0 + 1
    );
    crate::log(format!(
        "{ctx}: the spool was recovered: batch {start}..={end} posted after the covered block {covered0}"
    ));
    Ok(())
}

pub(crate) async fn batcher_outage_past_retention(h: &mut Harness) -> anyhow::Result<()> {
    let ctx = "batcher-outage-past-retention";
    let l1 = L1::new(h).await?;
    let aux = h.probes.validator.container.clone();
    let inner = h
        .nodes
        .inner_container(&aux, "batcher")
        .await
        .ok_or_else(|| crate::chaos_fail!("{ctx}: no inner batcher container on {aux}"))?;
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
    let blocks = freeze_with_spool(h, &aux, &inner, ctx).await?;
    // The baselines follow the freeze: a restart on a retried freeze
    // logs its own lines, which must not count for the final thaw.
    let restored0 = count(h, SPOOL_RESTORED_LINE).await?;
    let rebuilding0 = count(h, REBUILDING_LINE).await?;
    let rebuilt0 = count(h, REBUILT_LINE).await?;
    // A post in flight at the freeze still lands: read the covered
    // block once it has.
    tokio::time::sleep(Duration::from_secs(5)).await;
    let covered0 = l1.covered_through().await?;
    crate::log(format!(
        "{ctx}: batcher frozen with {blocks} spooled blocks, L1 covered through {covered0}"
    ));
    let (delta, held) = hold_until_floor_passes(h, rx0, snapshots0, ctx).await?;
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
    if h.thaw(&aux, &inner).await.is_err() {
        crate::log(format!(
            "{ctx}: SIGCONT failed (the task was replaced mid-freeze); the log asserts own the verdict"
        ));
    }
    let budget = h.knobs.restart_slo + Duration::from_secs(60);
    await_line(h, SPOOL_RESTORED_LINE, restored0, budget, ctx).await?;
    await_spool_posted(&l1, covered0, ctx).await?;
    let lines = Baselines {
        rebuilding: rebuilding0,
        rebuilt: rebuilt0,
    };
    if !await_rebuilt_or_served(h, &l1, lines, sealed, budget + REBUILD_BUDGET, ctx).await? {
        return l1.assert_contiguous(ctx).await;
    }
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
/// the sealers kept every frame above the posted head, served the
/// replay, and L1 covers through `sealed`, the head at the thaw, with no
/// refusal on the way (`false`). The retention never prunes below the
/// posted head, so the second is the expected end; the first stays valid
/// for a sealer that prunes by the window alone.
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
