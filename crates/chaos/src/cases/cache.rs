//! The Redis layer cases: the primary frozen past the sentinels'
//! down-after, the primary hard-killed, an ingress partitioned from
//! Redis, the mirrors killed with the projection flushed, and the
//! whole Redis job stopped and started empty. Every
//! case holds the fallback rule (the pipeline progresses while Redis is
//! dark, and the readers count a degraded read instead of stalling) and
//! the recovery (the readers use Redis again, the mirror head advances).
//!
//! The readers touch Redis only on a local miss. A cold-address balance
//! read through the ingress RPC is one such miss, so the cases drive the
//! reader counters with a probe read per poll step, and never depend on
//! the load's senders missing the local layer.
//!
//! The primary moves: a promotion leaves it on the replica's node. Every
//! case asks the sentinels where the primary is before it acts.

use std::time::{Duration, Instant};

use crate::harness::Harness;
use crate::nomad::SavedJob;
use crate::poll::{self, Budget};
use crate::probes::Probed;
use crate::rpc::Rpc;

mod backlog;
mod coverage;
mod discovery;
mod interrupted;
#[cfg(test)]
mod test_support;
pub(crate) use backlog::client_restart;
mod readers;
mod rebuilds;
pub(crate) use coverage::CacheRecovery;
pub(crate) use rebuilds::RebuildPhase;

const DEGRADED: &str = "kardamom_cache_degraded_total";
const LOOKUPS: &str = "kardamom_cache_lookups_total";
const REDIS_LAYER: &str = "layer=\"redis\"";
const MIRROR_HEAD: &str = "kardamom_state_mirror_head_tx_idx";
const MIRROR_WRITE_RETRIES: &str = "kardamom_state_mirror_write_retries_total";
/// 1 while a mirror serves the projection, 0 while it rebuilds.
const MIRROR_SERVING: &str = "kardamom_state_mirror_serving";
/// How long the whole Redis job stays stopped. The mirror schedules an
/// outage rebuild for a write that retried past 10 s, so the loss
/// lasts three times that.
const TOTAL_LOSS: Duration = Duration::from_secs(30);
/// How long the sentinels get to promote the replica of a frozen
/// primary: their `down-after-milliseconds` (5 s), the election, and
/// slack. The freeze lasts until the promotion is observed.
const PROMOTION_BUDGET: Duration = Duration::from_mins(1);
/// The redis job: one primary, one replica, three sentinels.
const REDIS_ALLOCS: usize = 5;
/// The mirror job: one mirror per executor node.
const MIRROR_ALLOCS: usize = 3;
const MIRROR_HEAD_KEYS: &str = "head:0 head:1 head:2";
/// The ports ingress-0 drops in the partition case: Redis, then the
/// sentinels. One rule per port; a multiport match needs a kernel
/// module the host may lack.
const REDIS_PORTS: [u16; 2] = [6379, 26379];

/// The reader counters of the ingress pair: degraded reads and Redis
/// lookups of any outcome.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct Readers {
    degraded: i64,
    redis: i64,
}

/// The reader counters of one ingress. `None` when its exporter does
/// not answer.
async fn reader_sample(h: &Harness, node: &Probed) -> Option<Readers> {
    let body = h
        .probes
        .scrape()
        .fetch(&h.probes.ingress_target(node))
        .await?;
    Some(Readers::parse(&body))
}

/// The reader counters summed over the ingress pair.
///
/// # Errors
///
/// Returns an error when no ingress exporter answers.
async fn readers(h: &Harness) -> anyhow::Result<Readers> {
    let mut total: Option<Readers> = None;
    for node in &h.probes.ingresses {
        if let Some(sample) = reader_sample(h, node).await {
            let t = total.unwrap_or_default();
            total = Some(Readers {
                degraded: t.degraded.saturating_add(sample.degraded),
                redis: t.redis.saturating_add(sample.redis),
            });
        }
    }
    total.ok_or_else(|| crate::chaos_fail!("no ingress exporter answers"))
}

/// One cold-address balance read through ingress-0: a local miss, so
/// the reader touches Redis. The address is fresh per call, so the
/// local layer never warms it.
async fn probe_cold_read(h: &Harness, step: u32) -> anyhow::Result<()> {
    Readers::probe(&Rpc::new(&h.rpc_url, h.knobs.chain_id)?, step).await
}

/// Wait until a probe read moves the counter `pick` selects: proof
/// that the reader took the path under test. The counter is the
/// evidence, not the answer: the read itself fails while no executor
/// runs behind the degraded path.
async fn wait_reader_moves(
    h: &Harness,
    ctx: &str,
    what: &str,
    budget: Budget,
    pick: fn(Readers) -> i64,
) -> anyhow::Result<()> {
    let outcome = poll::until(budget, |_| async move {
        let before = pick(readers(h).await?);
        let _ = probe_cold_read(h, 0).await;
        let after = pick(readers(h).await?);
        Ok::<_, anyhow::Error>((after > before).then_some(after))
    })
    .await?;
    let (value, elapsed) = outcome
        .or_fail(|t| crate::chaos_fail!("{ctx}: {what} did not rise within {}s", t.as_secs()))?;
    crate::log(format!(
        "{ctx}: {what} rose to {value} after {}s",
        elapsed.as_secs()
    ));
    Ok(())
}

/// The readers use Redis again: a probe read raises the Redis lookups
/// and leaves the degraded count where it was. A failed probe read or
/// scrape is one reading that shows no recovery: a recovering ingress
/// can refuse a read or drop a scrape, and the next reading tries again.
pub(crate) async fn wait_readers_recovered(h: &Harness, ctx: &str) -> anyhow::Result<()> {
    readers::Recovery::for_harness(h)?.wait(ctx).await
}

/// The readers degrade instead of stalling: a probe read raises the
/// degraded count.
pub(crate) async fn wait_readers_degraded(h: &Harness, ctx: &str) -> anyhow::Result<()> {
    wait_reader_moves(h, ctx, "degraded reads", Budget::secs(60, 2), |r| {
        r.degraded
    })
    .await
}

/// The readers reach Redis at all: the precondition of every case.
async fn wait_readers_connected(h: &Harness, ctx: &str) -> anyhow::Result<()> {
    wait_reader_moves(h, ctx, "redis lookups", Budget::secs(90, 3), |r| r.redis).await
}

/// The `metric` of each mirror exporter on the executor nodes, `None`
/// for one that does not answer.
async fn mirror_metrics(h: &Harness, metric: &str) -> Vec<Option<i64>> {
    let mut values = Vec::with_capacity(h.probes.executors.len());
    for i in 0..h.probes.executors.len() {
        values.push(h.probes.mirror_metric(i, metric).await);
    }
    values
}

/// The highest mirror head across the executor nodes. `None` when no
/// mirror exporter answers.
async fn mirror_head(h: &Harness) -> Option<i64> {
    mirror_metrics(h, MIRROR_HEAD)
        .await
        .into_iter()
        .flatten()
        .max()
}

/// Whether every mirror exporter answers and reports that its mirror
/// serves.
async fn every_mirror_serves(h: &Harness) -> bool {
    mirror_metrics(h, MIRROR_SERVING)
        .await
        .iter()
        .all(|serving| *serving == Some(1))
}

/// The mirror head advances: the projection follows the chain again.
/// The start is a read, not a default: a dark start would let any head
/// pass.
async fn wait_mirror_advances(h: &Harness, ctx: &str) -> anyhow::Result<()> {
    let start = mirror_head(h)
        .await
        .ok_or_else(|| crate::chaos_fail!("{ctx}: no mirror exporter answers the head baseline"))?;
    let outcome = poll::until(Budget::secs(120, 3), |_| async move {
        Ok::<_, anyhow::Error>(mirror_head(h).await.filter(|head| *head > start))
    })
    .await?;
    let (head, elapsed) = outcome.or_fail(|t| {
        crate::chaos_fail!(
            "{ctx}: the mirror head did not advance past {start} within {}s",
            t.as_secs()
        )
    })?;
    crate::log(format!(
        "{ctx}: mirror head {start} -> {head} after {}s",
        elapsed.as_secs()
    ));
    Ok(())
}

/// The aux node, which runs one sentinel: the cases' Redis console.
fn aux(h: &Harness) -> String {
    h.probes.validator.container.clone()
}

/// A `redis-cli` call through the aux node's sentinel container.
fn redis_cli(args: &str) -> String {
    format!("docker exec $(docker ps --filter name=sentinel- -q | head -1) redis-cli {args}")
}

/// A live primary named by any Sentinel, with independent bounded probes.
async fn sentinel_master(h: &Harness) -> anyhow::Result<Option<String>> {
    discovery::Discovery::primary(h).await
}

/// The primary a case acts on. A case that freezes, kills or flushes
/// the primary needs one now.
async fn live_primary(h: &Harness, ctx: &str) -> anyhow::Result<String> {
    sentinel_master(h)
        .await?
        .ok_or_else(|| crate::chaos_fail!("{ctx}: no sentinel named a live primary"))
}

/// The primary's node container and inner container, from the address
/// the sentinels name: a node record or a cluster IP.
async fn primary_on(h: &Harness, ctx: &str, master: &str) -> anyhow::Result<(String, String)> {
    let node = h.contract.node_at(master)?.container.clone();
    let cid = h
        .nodes
        .inner_cid(&node, "redis-")
        .await
        .ok_or_else(|| crate::chaos_fail!("{ctx}: no inner redis container on {node}"))?;
    Ok((node, cid))
}

/// The primary frozen past the sentinels' down-after: the readers
/// degrade and the pipeline progresses meanwhile, the sentinels promote
/// the replica, and after the thaw the readers and the mirror use the
/// promoted primary.
pub(crate) async fn redis_primary_freeze(h: &mut Harness) -> anyhow::Result<()> {
    let ctx = "redis-primary-freeze";
    wait_readers_connected(h, ctx).await?;
    let master0 = live_primary(h, ctx).await?;
    let (node, primary) = primary_on(h, ctx, &master0).await?;
    crate::log(format!(
        "{ctx}: primary {master0} ({primary} on {node}); frozen until the sentinels promote (budget {}s)",
        PROMOTION_BUDGET.as_secs()
    ));
    h.nodes
        .inner_signal(&node, &primary, crate::nodes::Signal::Stop)
        .await
        .map_err(|e| crate::chaos_fail!("{ctx}: SIGSTOP failed: {e}"))?;
    let frozen = frozen_phase(h, ctx, &node, &primary, &master0).await;
    let thawed = h
        .nodes
        .inner_signal(&node, &primary, crate::nodes::Signal::Cont)
        .await
        .map_err(|e| crate::chaos_fail!("{ctx}: SIGCONT of {primary} on {node} failed: {e}"));
    log_cleanup(ctx, &thawed);
    frozen?;
    thawed?;
    wait_readers_recovered(h, ctx).await?;
    wait_mirror_advances(h, ctx).await?;
    h.assert_progress().await
}

/// The frozen half of [`redis_primary_freeze`]: the freeze took, the
/// readers degrade, the pipeline progresses, the sentinels promote.
async fn frozen_phase(
    h: &Harness,
    ctx: &str,
    node: &str,
    primary: &str,
    master0: &str,
) -> anyhow::Result<()> {
    let answers = h
        .nodes
        .exec_status(
            node,
            &format!("timeout 2 docker exec {primary} redis-cli ping"),
        )
        .await?;
    anyhow::ensure!(
        !answers,
        "{ctx}: the freeze did NOT take (the primary still answers PING)"
    );
    wait_readers_degraded(h, ctx).await?;
    h.assert_progress().await?;
    let outcome =
        poll::until(
            Budget::new(PROMOTION_BUDGET, Duration::from_secs(3)),
            |_| async move {
                Ok::<_, anyhow::Error>(sentinel_master(h).await?.filter(|m| m != master0))
            },
        )
        .await?;
    let (master, elapsed) = outcome.or_fail(|t| {
        crate::chaos_fail!(
            "{ctx}: the sentinels did not promote a new primary within {}s",
            t.as_secs()
        )
    })?;
    crate::log(format!(
        "{ctx}: sentinels promoted {master} after {}s",
        elapsed.as_secs()
    ));
    Ok(())
}

/// The primary hard-killed: Nomad restarts it empty, or the sentinels
/// promote the replica first. Either way the readers degrade, the
/// pipeline progresses, and the readers and the mirror recover.
pub(crate) async fn redis_primary_kill(h: &mut Harness) -> anyhow::Result<()> {
    let ctx = "redis-primary-kill";
    wait_readers_connected(h, ctx).await?;
    let master = live_primary(h, ctx).await?;
    let (node, _) = primary_on(h, ctx, &master).await?;
    crate::log(format!("{ctx}: primary {master} on {node}"));
    h.inject_hard(&[&node], "redis-").await?;
    wait_readers_degraded(h, ctx).await?;
    h.assert_progress().await?;
    h.assert_count("redis", REDIS_ALLOCS, h.knobs.restart_slo)
        .await?;
    wait_readers_recovered(h, ctx).await?;
    wait_mirror_advances(h, ctx).await?;
    h.assert_progress().await
}

/// The mirror write retries summed over the executor nodes. A mirror
/// that retries is alive and waits for Redis. `None` when a mirror
/// exporter does not answer: a partial sum is no baseline.
async fn mirror_write_retries(h: &Harness) -> Option<i64> {
    mirror_metrics(h, MIRROR_WRITE_RETRIES)
        .await
        .into_iter()
        .try_fold(0_i64, |total, retries| total.checked_add(retries?))
}

/// The whole Redis job stopped: the primary, the replica and the three
/// sentinels. No reader can resolve a primary, so every cold read
/// degrades, the pipeline progresses, and the mirrors retry their
/// writes. The job then starts again with both instances empty, since
/// Redis keeps nothing on disk. The sentinels name a primary, every
/// mirror rebuilds the projection from its executor's newest
/// checkpoint, and the readers use Redis again. No sentinel restarts on
/// the cold start: each one starts with a config that names the
/// initial primary, also before the primary registers in Consul.
pub(crate) async fn redis_total_loss_recover(h: &mut Harness) -> anyhow::Result<()> {
    let ctx = "redis-total-loss-recover";
    wait_readers_connected(h, ctx).await?;
    let recovery = CacheRecovery::capture(h).await?;
    let retries0 = mirror_write_retries(h).await.ok_or_else(|| {
        crate::chaos_fail!("{ctx}: a mirror exporter does not answer the write-retry baseline")
    })?;
    let job = SavedJob::capture(&h.nomad, "redis").await?;
    crate::log(format!(
        "{ctx}: stop the whole redis job (primary, replica, three sentinels) for {}s",
        TOTAL_LOSS.as_secs()
    ));
    job.stop().await?;
    // The job starts again even when the outage checks fail.
    let outage = total_loss_phase(h, ctx, retries0).await;
    let restored = job.restore().await;
    outage?;
    restored?;
    h.assert_count("redis", REDIS_ALLOCS, h.knobs.reschedule_slo)
        .await?;
    wait_primary_named(h, ctx).await?;
    wait_mirror_advances(h, ctx).await?;
    recovery
        .await_restored(h, ctx, RebuildPhase::Complete)
        .await?;
    wait_readers_recovered(h, ctx).await?;
    h.assert_group_never_restarted("redis", "sentinel", ctx)
        .await?;
    h.assert_progress().await
}

/// The dark half of [`redis_total_loss_recover`]: the readers degrade,
/// the pipeline progresses, the mirrors retry, and the loss lasts its
/// whole window.
async fn total_loss_phase(h: &Harness, ctx: &str, retries0: i64) -> anyhow::Result<()> {
    let dark = Instant::now();
    wait_readers_degraded(h, ctx).await?;
    h.assert_progress().await?;
    let outcome = poll::until(Budget::secs(60, 3), |_| async move {
        Ok::<_, anyhow::Error>(mirror_write_retries(h).await.filter(|now| *now > retries0))
    })
    .await?;
    let (retries, _) = outcome.or_fail(|t| {
        crate::chaos_fail!(
            "{ctx}: no mirror retried a write within {}s of the loss — the mirrors do not wait for Redis",
            t.as_secs()
        )
    })?;
    crate::log(format!(
        "{ctx}: mirror write retries {retries0} -> {retries}"
    ));
    tokio::time::sleep(TOTAL_LOSS.saturating_sub(dark.elapsed())).await;
    Ok(())
}

/// Wait until the restarted sentinels name a primary that answers as
/// one.
async fn wait_primary_named(h: &Harness, ctx: &str) -> anyhow::Result<String> {
    let outcome = poll::until(
        Budget::new(PROMOTION_BUDGET, Duration::from_secs(3)),
        |_| async move { sentinel_master(h).await },
    )
    .await?;
    let (master, elapsed) = outcome.or_fail(|t| {
        crate::chaos_fail!(
            "{ctx}: the restarted sentinels named no live primary within {}s",
            t.as_secs()
        )
    })?;
    crate::log(format!(
        "{ctx}: sentinels name {master} after {}s",
        elapsed.as_secs()
    ));
    Ok(master)
}

/// The iptables rule that drops ingress-0's packets to `port`.
fn drop_rule(port: u16) -> String {
    format!("OUTPUT -p tcp --dport {port} -j DROP")
}

/// Ingress-0 partitioned from Redis: its reads time out and count as
/// degraded, its submits still land, and it recovers when the
/// partition heals.
pub(crate) async fn redis_partition_ingress(h: &mut Harness) -> anyhow::Result<()> {
    let ctx = "redis-partition-ingress";
    wait_readers_connected(h, ctx).await?;
    let node = h.probes.ingresses[0].container.clone();
    let partitioned = partitioned_phase(h, ctx, &node).await;
    let healed = heal_partition(h, ctx, &node).await;
    log_cleanup(ctx, &healed);
    partitioned?;
    healed?;
    crate::log(format!("{ctx}: partition healed on {node}"));
    wait_readers_recovered(h, ctx).await?;
    h.assert_progress().await
}

/// Drop the Redis packets of `node`, then prove the degraded reads and
/// the progress. An error after the first rule leaves the cleanup to
/// [`heal_partition`].
async fn partitioned_phase(h: &Harness, ctx: &str, node: &str) -> anyhow::Result<()> {
    for port in REDIS_PORTS {
        h.nodes
            .exec(node, &format!("iptables -w 5 -I {}", drop_rule(port)))
            .await
            .map_err(|e| crate::chaos_fail!("{ctx}: could not drop port {port} on {node}: {e}"))?;
    }
    crate::log(format!(
        "{ctx}: {node} dropping its Redis and sentinel packets"
    ));
    wait_readers_degraded(h, ctx).await?;
    h.assert_progress().await
}

/// Delete every drop rule of `node`, and fail when a rule stays. A rule
/// that the arm did not insert is no failure.
async fn heal_partition(h: &Harness, ctx: &str, node: &str) -> anyhow::Result<()> {
    let left = REDIS_PORTS.map(drop_rule).join(" | ");
    let script = format!(
        "for rule in {rules}; do while iptables -w 5 -C $rule 2>/dev/null; do iptables -w 5 -D $rule || exit 1; done; done",
        rules = REDIS_PORTS
            .map(|port| format!("'{}'", drop_rule(port)))
            .join(" ")
    );
    h.nodes
        .exec(node, &script)
        .await
        .map(|_| ())
        .map_err(|e| crate::chaos_fail!("{ctx}: a drop rule stays on {node} ({left}): {e}"))
}

/// Log a failed fault cleanup, so a fault that stays has a cause in the
/// log also when the case fails first for another reason.
fn log_cleanup(ctx: &str, cleanup: &anyhow::Result<()>) {
    if let Err(error) = cleanup {
        crate::log(format!("{ctx}: fault cleanup fails: {error:#}"));
    }
}

/// The three mirrors hard-killed and the projection flushed: the
/// restarted mirrors find Redis cold, rebuild from the executors'
/// newest checkpoint, and the head advances again. Exact values for an
/// idle used sender and an untouched funded account prove the rebuild
/// restores checkpoint coverage beyond the new live rows.
pub(crate) async fn mirror_kill_rebuild(h: &mut Harness) -> anyhow::Result<()> {
    interrupted::InterruptedRebuild::run(h).await
}

/// Flush the primary with the mirror job stopped. No mirror writes, so
/// Redis stays cold until the job starts again. A kill of a running
/// mirror before the flush races it: Nomad restarts a mirror after 5 s,
/// and a mirror that starts before the flush finds the live heads and
/// resumes with no rebuild. Returns the flushed primary.
pub(crate) async fn flush_primary(h: &Harness, ctx: &str) -> anyhow::Result<String> {
    let master = live_primary(h, ctx).await?;
    h.nodes
        .exec(
            &aux(h),
            &redis_cli(&format!("-h {master} -p 6379 FLUSHALL")),
        )
        .await
        .map_err(|e| crate::chaos_fail!("{ctx}: FLUSHALL on {master} failed: {e}"))?;
    Ok(master)
}
