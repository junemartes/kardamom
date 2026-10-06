//! `two-day-outage`: the order of a real outage. The liar at T0; a
//! batcher restart at T1; a deploy of the same images at T2; the floor
//! passes the cursor of T1 at T3; the fault clears at T4. No manual step
//! for the batcher: it posts through every step, so the floor never
//! reaches its live cursor.

use std::time::Duration;

use kardamom_l1_fault_proxy::Fault;

use super::batcher::{
    BeforeRestart, assert_resumed_from_contract, await_posting, posted, require_posting, restart,
};
use super::deferred;
use super::followers::{
    Followers, await_archive_complete, await_resume, heal_single_source_followers,
};
use super::halt::await_followers_halted;
use super::outage::hold_until_floor_passes;
use crate::cases::da_watcher::assert_not_past_sealer;
use crate::harness::Harness;
use crate::l1::{L1, STALE_POST_ALERT};
use crate::nomad::{SavedJob, Streams};
use crate::probes::CLUSTER_TASK;

const ALERT_HOLD: Duration = Duration::from_secs(30);

/// T2: every follower job stops and starts again from its registered
/// definition, the same images, as a deploy does.
async fn redeploy_followers(h: &Harness, ctx: &str) -> anyhow::Result<()> {
    crate::log(format!(
        "{ctx}: T2: a deploy of the same images (stop and start every follower job)"
    ));
    for job in ["da-watcher", "l1-indexer", "batcher"] {
        let saved = SavedJob::capture(&h.nomad, job).await?;
        saved.stop().await?;
        saved.restore().await?;
    }
    Ok(())
}

pub(crate) async fn two_day_outage(h: &mut Harness) -> anyhow::Result<()> {
    let ctx = "two-day-outage";
    let l1 = L1::new(h).await?;
    l1.require_rule_loaded(STALE_POST_ALERT, ctx).await?;
    let base = Followers::ready(h, ctx).await?;
    require_posting(h, ctx).await?;
    let window = h.knobs.l1_fault;
    // T0: the lies the followers see, and the batcher's view of its own
    // posts. Null receipts stay with `l1-null-receipts`: these two lies
    // are the ones a start dies on, a receipt is not.
    let faults = [
        Fault::WrongBlockHash {
            from_block: l1.head().await?.saturating_add(1),
        },
        Fault::SwallowLogs {
            address: l1.settlement,
        },
    ];
    crate::log(format!("{ctx}: T0: {faults:?}"));
    l1.set_faults(&faults).await?;
    await_followers_halted(h, base, ctx).await?;
    deferred(
        ctx,
        "kardamom_l1_source_disagreement_total is not exported yet",
    );
    // The alert fires before T1.
    l1.await_alert_held(STALE_POST_ALERT, ALERT_HOLD, window, ctx)
        .await?;
    // T1: the batcher restarts under the lie and resumes from the
    // contract.
    crate::log(format!("{ctx}: T1: batcher restart under the lie"));
    let before = BeforeRestart::read(h, &l1).await?;
    restart(h, ctx).await?;
    assert_resumed_from_contract(h, &l1, before, ctx).await?;
    let after_t1 = await_posting(h, 0, h.knobs.restart_slo, ctx).await?;
    let rx_t1 = h
        .probes
        .ingress_counts()
        .await
        .complete()
        .ok_or_else(|| crate::chaos_fail!("{ctx}: no complete ingress baseline at T1"))?;
    // T2. The da-watcher's restart resumes after the sealer's origin, and
    // halts on the lying anchor again. Its cursor file holds the confirmed
    // origin, so it never stands past the sealer.
    redeploy_followers(h, ctx).await?;
    assert_not_past_sealer(h, ctx).await?;
    await_posting(h, 0, h.knobs.restart_slo + Duration::from_secs(30), ctx).await?;
    // T3: the floor passes the cursor the batcher resumed at in T1. The
    // batcher kept posting, so its live cursor moved with the chain.
    let snapshots0 = h
        .evidence
        .count_lines(CLUSTER_TASK, "snapshot TAKEN", Streams::StdoutOnly)
        .await?;
    crate::log(format!(
        "{ctx}: T3: load until the floor passes the T1 cursor"
    ));
    let (delta, held) = hold_until_floor_passes(h, rx_t1, snapshots0, ctx).await?;
    let at_t3 = posted(h).await.unwrap_or(0);
    anyhow::ensure!(
        at_t3 > after_t1,
        "{}: {ctx}: the batcher confirmed no post between T1 and T3 ({delta} frames in {}s) — the floor passed its live cursor",
        crate::FAIL_PREFIX,
        held.as_secs()
    );
    // T4: the fault clears. The batcher posts within one flush.
    crate::log(format!("{ctx}: T4: the fault clears"));
    let stuck = Followers::at_clear(h, &l1, base).await?;
    await_posting(h, at_t3, Duration::from_secs(30), ctx).await?;
    deferred(
        ctx,
        "the followers' resume by themselves after T4: the single-source followers kept the wrong hash as their anchor",
    );
    heal_single_source_followers(h, ctx).await?;
    await_resume(h, stuck, ctx).await?;
    await_archive_complete(h, &l1, ctx).await?;
    l1.assert_contiguous(ctx).await
}
