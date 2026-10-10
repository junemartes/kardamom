//! The data-availability cases. The batcher is frozen with SIGSTOP, so
//! nothing posts to L1 while the chain seals on. `da-lag-halt` runs the
//! sealed head past the DA-lag budget and asserts the chain refuses new
//! transactions with the typed error and the halt record, then resumes
//! by itself once the batcher thaws and posts. `prune-floor` stays under
//! the budget but runs past the sealer's retention window and a Raft
//! snapshot, and asserts the sealer still replays from the batcher's
//! cursor: nothing is pruned below the posted head.
//!
//! Both read the chain status of the service events: `da-lag-halt` sees
//! the sealer halted on `da_lag` as the root and the ingresses paused on
//! it; `prune-floor` sees the frozen batcher gone and no root. Both end
//! with every service running again.

use std::time::Duration;

use super::chain_status::ChainView;
use crate::harness::Harness;
use crate::metrics::{self, Target};
use crate::nomad::Streams;
use crate::poll::{self, Budget};
use crate::probes::{CLUSTER_TASK, EXECUTOR_BLOCK_METRIC};
use crate::rpc::{CHAIN_HALTED_CODE, Rpc};

/// The batcher's exporter: on the aux node, bound to every interface.
const BATCHER_PORT: u16 = 9002;
/// The halt gauge every exporter serves; the ingress raises `da_lag`.
const HALT_METRIC: &str = "kardamom_halt";
const DA_LAG_LABEL: &str = "cause=\"da_lag\"";
/// The cluster status the ingress mirrors from the sealer's egress.
const POSTED_HEAD: &str = "kardamom_ingress_cluster_posted_head";
const SEALED_HEAD: &str = "kardamom_ingress_cluster_sealed_head";
const RETAINED_FRAMES: &str = "kardamom_ingress_cluster_retained_frames";
const FLOOR_BLOCK: &str = "kardamom_ingress_cluster_floor_block";
/// The batcher's own view of its last confirmed post.
const BATCHER_POSTED: &str = "kardamom_batcher_last_posted_block";
/// The sealer's stdout line for a snapshot taken.
const SNAPSHOT_TAKEN: &str = "sealer snapshot TAKEN";
/// The batcher's log line for a refused replay, which must not appear.
const REPLAY_REFUSED: &str = "cluster replay unavailable";
/// The batcher's log line for a confirmed post.
const POST_CONFIRMED: &str = "batch confirmed on L1";

/// The frozen batcher: its node, its inner container, and its exporter.
struct FrozenBatcher {
    node: String,
    inner: String,
    target: Target,
}

impl FrozenBatcher {
    /// Freeze the batcher on the aux node, verified by its dark exporter.
    async fn freeze(h: &Harness, ctx: &str) -> anyhow::Result<Self> {
        let node = h.probes.validator.container.clone();
        let inner = h
            .nodes
            .inner_container(&node, "batcher")
            .await
            .ok_or_else(|| crate::chaos_fail!("{ctx}: no inner batcher container on {node}"))?;
        let target = Target::bridged(h.probes.validator.ip, &node, BATCHER_PORT);
        crate::log(format!("{ctx}: freezing {inner} on {node}"));
        h.freeze_verified(&node, &inner, &target, ctx).await?;
        Ok(Self {
            node,
            inner,
            target,
        })
    }

    async fn thaw(&self, h: &Harness, ctx: &str) {
        if h.thaw(&self.node, &self.inner).await.is_err() {
            crate::log(format!(
                "{ctx}: SIGCONT failed (container may have been replaced mid-freeze)"
            ));
        }
    }

    /// The batcher's last confirmed post, from its exporter.
    async fn posted(&self, h: &Harness) -> Option<i64> {
        let body = h.probes.scrape().fetch(&self.target).await?;
        metrics::first(&body, BATCHER_POSTED)
    }
}

/// The cluster status as the first ingress mirrors it.
#[derive(Debug, Clone, Copy)]
struct Status {
    posted: i64,
    sealed: i64,
    retained: i64,
    floor: i64,
    halted: bool,
}

async fn status(h: &Harness) -> Option<Status> {
    let target = h.probes.ingress_target(&h.probes.ingresses[0]);
    let body = h.probes.scrape().fetch(&target).await?;
    Some(Status {
        posted: metrics::first(&body, POSTED_HEAD)?,
        sealed: metrics::first(&body, SEALED_HEAD)?,
        retained: metrics::first(&body, RETAINED_FRAMES)?,
        floor: metrics::first(&body, FLOOR_BLOCK)?,
        halted: metrics::sum_where(&body, HALT_METRIC, DA_LAG_LABEL) == Some(1),
    })
}

/// The status, or a failure that names the missing gauge.
async fn require_status(h: &Harness, ctx: &str) -> anyhow::Result<Status> {
    status(h).await.ok_or_else(|| {
        crate::chaos_fail!(
            "{ctx}: the ingress exports no cluster status; the batcher never published its cursor"
        )
    })
}

/// The chain halts new transactions when the batcher cannot post past
/// the budget, keeps sealing, and resumes by itself when the batcher
/// posts again.
///
/// # Errors
///
/// Returns the first assertion that fails.
pub(crate) async fn da_lag_halt(h: &mut Harness) -> anyhow::Result<()> {
    let ctx = "da-lag-halt";
    let budget = i64::try_from(
        h.knobs
            .da_lag_budget_blocks
            .ok_or_else(|| crate::chaos_fail!("{ctx}: KARDAMOM_DA_LAG_BUDGET_BLOCKS is not set — this case only means something on a cluster deployed with a small -Dkardamom.cluster.daLagBudgetBlocks"))?
            .get(),
    )
    .unwrap_or(i64::MAX);
    let before = require_status(h, ctx).await?;
    anyhow::ensure!(
        !before.halted,
        "{ctx}: the chain is halted before the injection"
    );
    let frozen = FrozenBatcher::freeze(h, ctx).await?;

    let halted = await_halt(h, ctx, budget, true).await;
    let halted = match halted {
        Ok(s) => s,
        Err(e) => {
            frozen.thaw(h, ctx).await;
            return Err(e);
        }
    };
    crate::log(format!(
        "{ctx}: halted at sealed={} posted={} (budget {budget})",
        halted.sealed, halted.posted
    ));
    let verdict = assert_halted_chain(h, ctx, &halted).await;
    let roots = assert_root_and_pauses(h, ctx).await;
    frozen.thaw(h, ctx).await;
    verdict?;
    roots?;

    let resumed = await_halt(h, ctx, budget, false).await?;
    let posted = frozen.posted(h).await.unwrap_or(0);
    anyhow::ensure!(
        posted > before.posted && resumed.posted > before.posted,
        "{ctx}: the batcher did not post past the frozen head after the thaw (batcher {posted}, status {})",
        resumed.posted
    );
    let rpc = Rpc::new(&h.rpc_url, h.knobs.chain_id)?;
    let gate = h.knobs.gate_account;
    rpc.transfer_hash(gate, rpc.nonce_of(gate).await?, Duration::from_secs(60))
        .await
        .map_err(|e| crate::chaos_fail!("{ctx}: a transfer after the resume failed: {e:#}"))?;
    await_all_running(h, ctx).await?;
    h.assert_executor_progress(Duration::from_mins(2)).await
}

/// While halted, the chain status names the sealer halted on `da_lag` as
/// the root, and the ingresses paused on it: one root, its dependents
/// paused, not halted.
async fn assert_root_and_pauses(h: &Harness, ctx: &str) -> anyhow::Result<()> {
    let view = ChainView::await_until(
        h,
        &format!("{ctx}: the sealer as the da_lag root, the ingresses paused on it"),
        Duration::from_secs(30),
        |v| {
            let paused = v.paused_on("ingress");
            v.roots()
                .contains(&("sealer".to_string(), "da_lag".to_string()))
                && !paused.is_empty()
                && paused.iter().all(|cause| cause == "da_lag")
        },
    )
    .await?;
    anyhow::ensure!(
        view.states_of("ingress").iter().all(|s| s != "halted"),
        "{ctx}: an ingress is halted, not paused: {view}"
    );
    crate::log(format!(
        "{ctx}: chain status: roots {:?}, ingresses paused on {:?}",
        view.roots(),
        view.paused_on("ingress")
    ));
    Ok(())
}

/// After the thaw, no root stands, nothing is paused, and the main
/// services run again.
async fn await_all_running(h: &Harness, ctx: &str) -> anyhow::Result<()> {
    let view = ChainView::await_until(
        h,
        &format!("{ctx}: every service running again"),
        Duration::from_mins(2),
        |v| {
            v.settled()
                && ["ingress", "sequencer", "executor", "batcher"]
                    .iter()
                    .all(|service| v.all_running(service))
        },
    )
    .await?;
    crate::log(format!(
        "{ctx}: chain status settled: ingress {:?}, batcher {:?}",
        view.states_of("ingress"),
        view.states_of("batcher")
    ));
    Ok(())
}

/// Wait until the ingress reports the halt flag `halted`, within the
/// freeze cap.
async fn await_halt(h: &Harness, ctx: &str, budget: i64, halted: bool) -> anyhow::Result<Status> {
    let outcome = poll::until(
        Budget::new(h.knobs.retention_freeze_cap, Duration::from_secs(5)),
        |_| async move { Ok::<_, anyhow::Error>(status(h).await.filter(|s| s.halted == halted)) },
    )
    .await?;
    let (status, _) = outcome.or_fail(|elapsed| {
        crate::chaos_fail!(
            "{ctx}: the ingress did not report da_lag halted={halted} within {}s (budget {budget} blocks)",
            elapsed.as_secs()
        )
    })?;
    Ok(status)
}

/// While the chain is halted: the lag is past the budget, a submit gets
/// the typed error that names `/halt`, and the boundaries keep sealing.
async fn assert_halted_chain(h: &Harness, ctx: &str, halted: &Status) -> anyhow::Result<()> {
    let budget = h
        .knobs
        .da_lag_budget_blocks
        .map_or(0, |b| i64::try_from(b.get()).unwrap_or(i64::MAX));
    anyhow::ensure!(
        halted.sealed - halted.posted > budget,
        "{ctx}: halted with lag {} not past the budget {budget}",
        halted.sealed - halted.posted
    );
    let rpc = Rpc::new(&h.rpc_url, h.knobs.chain_id)?;
    let gate = h.knobs.gate_account;
    let refusal = rpc
        .send_transfer(gate, rpc.nonce_of(gate).await?)
        .await?
        .err()
        .ok_or_else(|| {
            crate::chaos_fail!("{ctx}: a submit was accepted while the chain is halted")
        })?;
    anyhow::ensure!(
        refusal.code == CHAIN_HALTED_CODE && refusal.message.contains("/halt"),
        "{ctx}: the refusal is not the typed DA-lag error: code {} message {}",
        refusal.code,
        refusal.message
    );
    crate::log(format!("{ctx}: submit refused: {}", refusal.message));
    let first = h
        .probes
        .exec_metric(0, EXECUTOR_BLOCK_METRIC)
        .await
        .unwrap_or(0);
    tokio::time::sleep(Duration::from_secs(10)).await;
    let later = h
        .probes
        .exec_metric(0, EXECUTOR_BLOCK_METRIC)
        .await
        .unwrap_or(0);
    anyhow::ensure!(
        later > first,
        "{ctx}: the boundaries stopped while halted (executor block {first} then {later})"
    );
    Ok(())
}

/// The sealer keeps every frame above the posted head: a freeze past the
/// retention window and a Raft snapshot ends with a replay from the
/// batcher's cursor, a contiguous record on L1, and the retention back
/// inside its window.
///
/// # Errors
///
/// Returns the first assertion that fails.
pub(crate) async fn prune_floor(h: &mut Harness) -> anyhow::Result<()> {
    let ctx = "prune-floor";
    let retention = i64::try_from(
        h.knobs
            .cluster_retention
            .ok_or_else(|| crate::chaos_fail!("{ctx}: KARDAMOM_CLUSTER_RETENTION is not set"))?
            .get(),
    )
    .unwrap_or(i64::MAX);
    let before = require_status(h, ctx).await?;
    let refusals_before = h
        .evidence
        .count_lines("batcher", REPLAY_REFUSED, Streams::Both)
        .await?;
    let snapshots_before = h
        .evidence
        .count_lines(CLUSTER_TASK, SNAPSHOT_TAKEN, Streams::StdoutOnly)
        .await?;
    let frozen = FrozenBatcher::freeze(h, ctx).await?;

    let stretched = await_stretched(h, ctx, retention, snapshots_before).await;
    let frozen_view = ChainView::read(h).await;
    frozen.thaw(h, ctx).await;
    let stretched = stretched?;
    let frozen_view = frozen_view?;
    anyhow::ensure!(
        frozen_view.states_of("batcher").iter().any(|s| s == "gone"),
        "{ctx}: the frozen batcher is not gone in the chain status: {frozen_view}"
    );
    anyhow::ensure!(
        frozen_view.roots().is_empty(),
        "{ctx}: a root stands under the budget: {frozen_view}"
    );
    anyhow::ensure!(
        stretched.floor <= before.posted.saturating_add(1),
        "{ctx}: the replay floor {} passed the posted head {}",
        stretched.floor,
        before.posted
    );
    crate::log(format!(
        "{ctx}: retention stretched to {} frames (window {retention}), floor block {}, posted {}",
        stretched.retained, stretched.floor, before.posted
    ));

    let shared: &Harness = h;
    let (back, _) = poll::until(
        Budget::new(Duration::from_mins(5), Duration::from_secs(5)),
        |_| async move {
            Ok::<_, anyhow::Error>(
                status(shared)
                    .await
                    .filter(|s| s.posted > before.posted && s.retained <= retention),
            )
        },
    )
    .await?
    .or_fail(|elapsed| {
        crate::chaos_fail!(
            "{ctx}: the batcher did not post past {} with the retention back inside {retention} within {}s",
            before.posted,
            elapsed.as_secs()
        )
    })?;
    crate::log(format!(
        "{ctx}: posted {} and the retention is back at {} frames",
        back.posted, back.retained
    ));
    let refusals = h
        .evidence
        .count_lines("batcher", REPLAY_REFUSED, Streams::Both)
        .await?;
    anyhow::ensure!(
        refusals == refusals_before,
        "{ctx}: the sealer refused the batcher's replay {} time(s) after the thaw",
        refusals - refusals_before
    );
    assert_contiguous_posts(h, ctx).await?;
    await_all_running(h, ctx).await?;
    h.assert_executor_progress(Duration::from_mins(2)).await
}

/// Hold the freeze until the retention stretched past its window and a
/// snapshot landed, within the freeze cap.
async fn await_stretched(
    h: &Harness,
    ctx: &str,
    retention: i64,
    snapshots_before: usize,
) -> anyhow::Result<Status> {
    let outcome = poll::until(
        Budget::new(h.knobs.retention_freeze_cap, Duration::from_secs(10)),
        |_| async move {
            let snapshots = h
                .evidence
                .count_lines(CLUSTER_TASK, SNAPSHOT_TAKEN, Streams::StdoutOnly)
                .await?;
            Ok::<_, anyhow::Error>(
                status(h)
                    .await
                    .filter(|s| s.retained > retention && snapshots > snapshots_before),
            )
        },
    )
    .await?;
    let (status, _) = outcome.or_fail(|elapsed| {
        crate::chaos_fail!(
            "{ctx}: the retention did not stretch past {retention} frames with a snapshot within {}s",
            elapsed.as_secs()
        )
    })?;
    Ok(status)
}

/// Every confirmed post in the batcher's log starts where the one before
/// it ended.
async fn assert_contiguous_posts(h: &Harness, ctx: &str) -> anyhow::Result<()> {
    let logs = h.nomad.job_logs("batcher", Streams::Both).await?;
    let posts: Vec<(u64, u64)> = logs
        .lines()
        .filter(|l| l.contains(POST_CONFIRMED))
        .filter_map(|l| Some((field(l, "l2_block_start=")?, field(l, "l2_block_end=")?)))
        .collect();
    anyhow::ensure!(
        posts.len() >= 2,
        "{ctx}: fewer than two confirmed posts in the batcher log"
    );
    let gap = posts
        .windows(2)
        .find(|w| w[1].0 != w[0].1.saturating_add(1));
    anyhow::ensure!(
        gap.is_none(),
        "{ctx}: the L1 record is not contiguous: {:?} then {:?}",
        gap.map(|w| w[0]),
        gap.map(|w| w[1])
    );
    crate::log(format!(
        "{ctx}: {} confirmed posts, contiguous",
        posts.len()
    ));
    Ok(())
}

/// The integer after `key=` in a log line.
fn field(line: &str, key: &str) -> Option<u64> {
    let rest = &line[line.find(key)? + key.len()..];
    rest.split(|c: char| !c.is_ascii_digit())
        .next()?
        .parse()
        .ok()
}

#[cfg(test)]
mod tests {
    use super::field;

    #[test]
    fn a_log_field_parses_up_to_the_next_separator() {
        let line =
            "batch confirmed on L1 batch_index=3 l2_block_start=41 l2_block_end=60 payload_bytes=9";
        assert_eq!(field(line, "l2_block_start="), Some(41));
        assert_eq!(field(line, "l2_block_end="), Some(60));
        assert_eq!(field(line, "absent="), None);
    }
}
