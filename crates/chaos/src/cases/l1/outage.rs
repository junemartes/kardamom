//! `batcher-outage-past-retention`: the batcher is frozen until the
//! sealers' egress floor passes its cursor and a snapshot lands, then
//! thawed. Its restart recovers what its spool held; the rest of the
//! gap waits on an executor's block refs and the `tx_data` archive.

use std::cell::Cell;
use std::time::Duration;

use super::batcher::{REFUSED_LINE, SPOOL_RESTORED_LINE, count, require_posting};
use super::halt::await_batcher_halted_on_replay;
use crate::harness::Harness;
use crate::l1::L1;
use crate::nomad::Streams;
use crate::poll::{self, Budget};
use crate::probes::{BATCHER_PORT, CLUSTER_TASK};

/// The sealer's line for a snapshot, on every member.
const SNAPSHOT_LINE: &str = "snapshot TAKEN";
/// How many times the freeze is retried to land on a non-empty spool.
const FREEZE_ATTEMPTS: u32 = 10;

/// The spool's block files on the aux node.
async fn spool_blocks(h: &Harness, aux: &str) -> anyhow::Result<usize> {
    h.nodes
        .exec(
            aux,
            "ls /opt/kardamom/batcher/spool 2>/dev/null | grep -c '\\.block$' || true",
        )
        .await?
        .trim()
        .parse()
        .map_err(|e| crate::chaos_fail!("spool listing is not a count: {e}"))
}

/// Freeze the batcher with a non-empty spool, so the restart has a group
/// to recover. The spool empties for an instant after every post, so a
/// freeze that lands in that instant is thawed and tried again.
async fn freeze_with_spool(
    h: &Harness,
    aux: &str,
    inner: &str,
    ctx: &str,
) -> anyhow::Result<usize> {
    let target = h.probes.aux_target(BATCHER_PORT);
    let target = &target;
    let outcome = poll::until(
        Budget::new(
            Duration::from_secs(u64::from(FREEZE_ATTEMPTS) * 4),
            Duration::from_secs(1),
        ),
        |_| async move {
            h.freeze_verified(aux, inner, target, ctx).await?;
            let blocks = spool_blocks(h, aux).await?;
            if blocks > 0 {
                return Ok::<_, anyhow::Error>(Some(blocks));
            }
            h.thaw(aux, inner).await?;
            Ok(None)
        },
    )
    .await?;
    outcome
        .or_fail(|_| crate::chaos_fail!("{ctx}: the spool was empty on every freeze"))
        .map(|(blocks, _)| blocks)
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
    let restored0 = count(h, SPOOL_RESTORED_LINE).await?;
    let refused0 = count(h, REFUSED_LINE).await?;
    let blocks = freeze_with_spool(h, &aux, &inner, ctx).await?;
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
    if h.thaw(&aux, &inner).await.is_err() {
        crate::log(format!(
            "{ctx}: SIGCONT failed (the task was replaced mid-freeze); the log asserts own the verdict"
        ));
    }
    let budget = h.knobs.restart_slo + Duration::from_secs(60);
    await_line(h, SPOOL_RESTORED_LINE, restored0, budget, ctx).await?;
    await_spool_posted(&l1, covered0, ctx).await?;
    await_batcher_halted_on_replay(h, refused0, budget, ctx).await?;
    crate::log(format!(
        "{ctx}: SKIPPED (second half): the sealers refused the replay past the spool; the recovery from an executor's block refs and the tx_data archive waits on kardamom_getBlockRefs"
    ));
    l1.assert_contiguous(ctx).await
}
