//! `l1-null-receipts`: the proxy answers null receipts and empty
//! settlement logs while it serves blocks. Contract reconciliation can
//! confirm a post despite its hidden receipt. Swallowed logs halt the
//! follower's certified view, so its observed-post age grows. A restart
//! inside the fault resumes from the contract without waiting on the indexer.

use std::time::{Duration, Instant};

use kardamom_l1_fault_proxy::Fault;

use super::ALERT_HOLD;
use super::batcher::{
    BeforeRestart, assert_resumed_from_contract, await_posting, require_posting, restart,
};
use super::followers::{Followers, await_archive_complete, await_resume};
use super::halt::await_followers_halted;
use crate::harness::Harness;
use crate::l1::{L1, STALE_POST_ALERT};

pub(crate) async fn null_receipts(h: &mut Harness) -> anyhow::Result<()> {
    let ctx = "l1-null-receipts";
    let l1 = L1::new(h).await?;
    l1.require_rule_loaded(STALE_POST_ALERT, ctx).await?;
    let base = Followers::ready(h, ctx).await?;
    require_posting(h, ctx).await?;
    let window = h.knobs.l1_fault;
    let faults = [
        Fault::NullReceipts {
            from_block: l1.head().await?.saturating_add(1),
        },
        Fault::SwallowLogs {
            address: l1.settlement,
        },
    ];
    crate::log(format!("{ctx}: {faults:?} for {}s", window.as_secs()));
    l1.set_faults(&faults).await?;
    // The faults clear on every path, so no lie leaks into the next case.
    let lied = under_faults(h, &l1, base, ctx).await;
    let cleared = Followers::at_clear(h, &l1, base).await;
    let stalled = lied?;
    let stuck = cleared?;
    // The restored view permits another confirmed post and follower catch-up.
    await_posting(
        h,
        stalled,
        h.knobs.restart_slo + Duration::from_secs(30),
        ctx,
    )
    .await?;
    await_resume(h, stuck, ctx).await?;
    await_archive_complete(h, &l1, ctx).await?;
    l1.assert_contiguous(ctx).await
}

/// Through the armed faults: the halt, the alert, and a restart that
/// resumes from the contract, until the fault window ends. Returns the
/// confirmed posts at the end of the window.
async fn under_faults(h: &mut Harness, l1: &L1, base: Followers, ctx: &str) -> anyhow::Result<i64> {
    let armed = Instant::now();
    let window = h.knobs.l1_fault;
    // The follower's two sources disagree on the swallowed logs: it halts,
    // and the da-watcher pauses on it.
    await_followers_halted(h, base, ctx).await?;
    // The age follows the newest post the follower can certify. It grows
    // through the halted stream even when contract reconciliation succeeds.
    l1.await_alert_held(STALE_POST_ALERT, ALERT_HOLD, window, ctx)
        .await?;
    let before = BeforeRestart::read(h, l1).await?;
    restart(h, ctx).await?;
    assert_resumed_from_contract(h, l1, before, ctx).await?;
    tokio::time::sleep(window.saturating_sub(armed.elapsed())).await;
    h.probes.batcher_posts().await.ok_or_else(|| {
        crate::chaos_fail!("{ctx}: the batcher exporter does not answer the post baseline")
    })
}
