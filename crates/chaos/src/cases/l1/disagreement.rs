//! `follower-disagreement`: one L1 follower instance reads a fork that is
//! consistent in itself, past its own chain check; the other reads L1.
//!
//! The proxy serves the fork to the calls of the lying instance's node
//! only (`Fault::ForkedChain` with its address). The lying instance is
//! frozen while the honest one publishes past the fork's first block, so
//! the da-watcher takes the true records first and the lie never reaches
//! the chain: the shard's state audit needs a clean chain. Then:
//!
//! - the thawed liar publishes its fork, and the da-watcher halts on
//!   `l1_follower_disagreement`;
//! - the validator's own L1 read verified the epochs the chain holds,
//!   with no fault: its view is independent of the follower.
//!
//! The heal is the runbook: the fault ends, the liar's archive is wiped,
//! the follower restarts, and the operator clears the da-watcher's halt.
//! The da-watcher resumes on the true chain, which the archives' history
//! gives it by the parent chain from its head.

use std::time::Duration;

use kardamom_l1_fault_proxy::Fault;

use super::followers::Followers;
use crate::cases::component::wipe_dirs;
use crate::cases::da_watcher::assert_no_origin_gap;
use crate::harness::Harness;
use crate::l1::L1;
use crate::metrics::Target;
use crate::nomad::SavedJob;
use crate::poll::{self, Budget};
use crate::probes::{DA_WATCHER_PORT, INDEXER_PORT};

const JOB: &str = "l1-indexer";
const TASK: &str = "l1-indexer";
const HALT: &str = "kardamom_halt";
const DISAGREEMENT: &str = "cause=\"l1_follower_disagreement\"";
const PUBLISHED_ORIGIN: &str = "kardamom_da_watcher_epoch_origin_block_number";
const EPOCH_FAULTS: &str = "validator_epoch_faults_total";
const EPOCHS_VERIFIED: &str = "validator_epochs_verified_total";
/// How many true blocks past the fork's start the da-watcher takes
/// before the liar is thawed.
const TRUE_LEAD: u64 = 3;
const BUDGET: Duration = Duration::from_secs(180);

pub(crate) async fn follower_disagreement(h: &mut Harness) -> anyhow::Result<()> {
    let ctx = "follower-disagreement";
    let l1 = L1::new(h).await?;
    Followers::ready(h, ctx).await?;
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
    let faults0 = h.probes.val_metric(EPOCH_FAULTS).await.unwrap_or(0);
    let verified0 = h.probes.val_metric(EPOCHS_VERIFIED).await.unwrap_or(0);
    h.freeze_verified(
        &node,
        &inner,
        &Target::bridged(ip, &node, INDEXER_PORT),
        ctx,
    )
    .await?;
    let from_block = l1.head().await?.saturating_add(1);
    let fault = Fault::ForkedChain {
        from_block,
        client: Some(ip.into()),
    };
    crate::log(format!("{ctx}: {fault:?} for the instance on {node}"));
    l1.set_faults(&[fault]).await?;
    let lied = lie_after_true_lead(h, &l1, &node, &inner, from_block, ctx).await;
    let healed = heal(h, &l1, &node, ctx).await;
    lied?;
    healed?;
    assert_validator_clean(h, faults0, verified0, ctx).await?;
    assert_no_origin_gap(h, ctx).await
}

/// Let the honest instance lead past the fork, thaw the liar, and wait
/// for the da-watcher's halt on the disagreement.
async fn lie_after_true_lead(
    h: &Harness,
    l1: &L1,
    node: &str,
    inner: &str,
    from_block: u64,
    ctx: &str,
) -> anyhow::Result<()> {
    let target = i64::try_from(from_block.saturating_add(TRUE_LEAD)).unwrap_or(i64::MAX);
    let led = await_metric(
        h,
        PUBLISHED_ORIGIN,
        "",
        |v| v >= target,
        ctx,
        "take the true blocks",
    )
    .await;
    let thawed = h.thaw(node, inner).await;
    led?;
    thawed.map_err(|e| crate::chaos_fail!("{ctx}: SIGCONT failed: {e}"))?;
    crate::log(format!(
        "{ctx}: the liar is thawed; L1 head {}",
        l1.head().await.unwrap_or(0)
    ));
    await_metric(
        h,
        HALT,
        DISAGREEMENT,
        |v| v == 1,
        ctx,
        "halt on l1_follower_disagreement",
    )
    .await
}

/// The runbook: end the fault, wipe the liar's archive, restart the
/// follower, clear the da-watcher's halt, and see it publish again.
async fn heal(h: &Harness, l1: &L1, node: &str, ctx: &str) -> anyhow::Result<()> {
    crate::log(format!(
        "{ctx}: OPERATOR STEP: the l1_follower_disagreement runbook"
    ));
    l1.clear_faults().await?;
    let job = SavedJob::capture(&h.nomad, JOB).await?;
    job.stop().await?;
    wipe_dirs(h, node, ctx, "rm -rf /opt/kardamom/l1-indexer/*").await?;
    job.restore().await?;
    let before = h
        .probes
        .aux_metric(DA_WATCHER_PORT, PUBLISHED_ORIGIN)
        .await
        .unwrap_or(0);
    let aux = h.probes.validator.container.clone();
    h.nodes
        .exec(
            &aux,
            &format!("curl -sf -X POST http://127.0.0.1:{DA_WATCHER_PORT}/halt/clear"),
        )
        .await
        .map_err(|e| crate::chaos_fail!("{ctx}: the halt clear failed: {e}"))?;
    await_metric(
        h,
        PUBLISHED_ORIGIN,
        "",
        |v| v > before,
        ctx,
        "publish again after the clear",
    )
    .await?;
    await_metric(h, HALT, DISAGREEMENT, |v| v == 0, ctx, "stay clear").await
}

/// The validator's own L1 read verified epochs during the case, and no
/// epoch failed: no lie reached the chain.
async fn assert_validator_clean(
    h: &Harness,
    faults0: i64,
    verified0: i64,
    ctx: &str,
) -> anyhow::Result<()> {
    let faults = h.probes.val_metric(EPOCH_FAULTS).await.unwrap_or(0);
    anyhow::ensure!(
        faults == faults0,
        "{}: {ctx}: the validator counted {} epoch fault(s): a lie reached the chain",
        crate::FAIL_PREFIX,
        faults - faults0
    );
    await_validator(h, EPOCHS_VERIFIED, |v| v > verified0, ctx, "verify epochs").await
}

/// The da-watcher's metric `name` (series with `label`, or all) passes
/// `ok` within the budget.
async fn await_metric(
    h: &Harness,
    name: &str,
    label: &str,
    ok: impl Fn(i64) -> bool + Copy,
    ctx: &str,
    what: &str,
) -> anyhow::Result<()> {
    let outcome = poll::until(
        Budget::new(BUDGET, Duration::from_secs(2)),
        |_| async move {
            let value = if label.is_empty() {
                h.probes.aux_metric(DA_WATCHER_PORT, name).await
            } else {
                h.probes
                    .aux_metric_where(DA_WATCHER_PORT, name, label)
                    .await
            };
            Ok::<_, anyhow::Error>(value.filter(|v| ok(*v)))
        },
    )
    .await?;
    let (value, elapsed) = outcome.or_fail(|t| {
        crate::chaos_fail!(
            "{ctx}: the da-watcher did not {what} within {}s",
            t.as_secs()
        )
    })?;
    crate::log(format!(
        "{ctx}: the da-watcher did {what} after {}s ({name}={value})",
        elapsed.as_secs()
    ));
    Ok(())
}

/// The validator's metric `name` passes `ok` within the budget.
async fn await_validator(
    h: &Harness,
    name: &str,
    ok: impl Fn(i64) -> bool + Copy,
    ctx: &str,
    what: &str,
) -> anyhow::Result<()> {
    let outcome =
        poll::until(
            Budget::new(BUDGET, Duration::from_secs(3)),
            |_| async move {
                Ok::<_, anyhow::Error>(h.probes.val_metric(name).await.filter(|v| ok(*v)))
            },
        )
        .await?;
    let (value, _) = outcome.or_fail(|t| {
        crate::chaos_fail!(
            "{ctx}: the validator did not {what} within {}s",
            t.as_secs()
        )
    })?;
    crate::log(format!("{ctx}: the validator did {what} ({name}={value})"));
    Ok(())
}
