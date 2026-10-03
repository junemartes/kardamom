//! `l1-null-receipts`: the proxy answers null receipts and empty
//! settlement logs while it serves blocks. The batcher cannot confirm a
//! post, the stale-post alert fires, and a restart inside the fault
//! resumes from the contract with no wait on the indexer.

use std::time::{Duration, Instant};

use kardamom_l1_fault_proxy::Fault;

use super::batcher::{
    BeforeRestart, assert_resumed_from_contract, await_posting, posted, require_posting, restart,
};
use super::deferred;
use super::followers::{Followers, await_archive_complete, await_resume};
use crate::harness::Harness;
use crate::l1::{L1, STALE_POST_ALERT};

const ALERT_HOLD: Duration = Duration::from_secs(30);

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
    let armed = Instant::now();
    deferred(
        ctx,
        "the followers' halt within 3 ticks: one source cannot see a null receipt or a missing log; the cross-check of two can",
    );
    // The batcher waits on a receipt that never comes, so its last post
    // ages on L1: the one signal of this fault that pages.
    l1.await_alert_held(STALE_POST_ALERT, ALERT_HOLD, window, ctx)
        .await?;
    let before = BeforeRestart::read(h, &l1).await?;
    restart(h, ctx).await?;
    assert_resumed_from_contract(h, &l1, before, ctx).await?;
    tokio::time::sleep(window.saturating_sub(armed.elapsed())).await;
    let stalled = posted(h).await.unwrap_or(0);
    l1.clear_faults().await?;
    // The receipt of the post in flight arrives; the batcher continues.
    await_posting(
        h,
        stalled,
        h.knobs.restart_slo + Duration::from_secs(30),
        ctx,
    )
    .await?;
    await_resume(h, base, ctx).await?;
    await_archive_complete(h, &l1, ctx).await?;
    l1.assert_contiguous(ctx).await
}
