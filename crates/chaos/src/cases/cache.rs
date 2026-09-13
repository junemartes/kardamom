//! The Redis layer cases: the primary frozen past the sentinels'
//! down-after, the primary hard-killed, an ingress partitioned from
//! Redis, and the mirrors killed with the projection flushed. Every
//! case holds the fallback rule (the pipeline progresses while Redis is
//! dark, and the readers count a degraded read instead of stalling) and
//! the recovery (the readers use Redis again, the mirror head advances).
//!
//! The readers touch Redis only on a local miss. A cold-address balance
//! read through the ingress RPC is one such miss, so the cases drive the
//! reader counters with a probe read per poll step, and never depend on
//! the load's senders missing the local layer.

use std::time::Duration;

use alloy_primitives::Address;
use kardamom_bench::mnemonic::derive_signers;

use crate::harness::Harness;
use crate::poll::{self, Budget};
use crate::probes::Probed;
use crate::rpc::{ANVIL_MNEMONIC, Rpc};

const DEGRADED: &str = "kardamom_cache_degraded_total";
const LOOKUPS: &str = "kardamom_cache_lookups_total";
const REDIS_LAYER: &str = "layer=\"redis\"";
const MIRROR_HEAD: &str = "kardamom_state_mirror_head_tx_idx";
const MIRROR_REBUILDS: &str = "kardamom_state_mirror_rebuilds_total";
/// Longer than the sentinels' `down-after-milliseconds` (5 s) plus the
/// election, so a freeze forces a promotion.
const PRIMARY_FREEZE: Duration = Duration::from_secs(20);
/// The redis job: one primary, one replica, three sentinels.
const REDIS_ALLOCS: usize = 5;
/// The mirror job: one mirror per executor node.
const MIRROR_ALLOCS: usize = 3;

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
    Some(Readers {
        degraded: crate::metrics::sum(&body, DEGRADED).unwrap_or(0),
        redis: crate::metrics::sum_where(&body, LOOKUPS, REDIS_LAYER).unwrap_or(0),
    })
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
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    let mut bytes = [0u8; 20];
    bytes[..16].copy_from_slice(&nanos.to_le_bytes());
    bytes[16..].copy_from_slice(&step.to_le_bytes());
    Rpc::new(&h.rpc_url, h.knobs.chain_id)?
        .balance_probe(Address::from(bytes))
        .await
}

/// Wait until a probe read moves the counter `pick` selects: proof
/// that the reader took the path under test.
async fn wait_reader_moves(
    h: &Harness,
    ctx: &str,
    what: &str,
    budget: Budget,
    pick: fn(Readers) -> i64,
) -> anyhow::Result<()> {
    let outcome = poll::until(budget, |_| async move {
        let before = pick(readers(h).await?);
        probe_cold_read(h, 0).await?;
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
/// and leaves the degraded count where it was.
async fn wait_readers_recovered(h: &Harness, ctx: &str) -> anyhow::Result<()> {
    let outcome = poll::until(Budget::secs(120, 3), |_| async move {
        let before = readers(h).await?;
        probe_cold_read(h, 1).await?;
        let after = readers(h).await?;
        let recovered = after.redis > before.redis && after.degraded == before.degraded;
        Ok::<_, anyhow::Error>(recovered.then_some(after))
    })
    .await?;
    let (sample, elapsed) = outcome.or_fail(|t| {
        crate::chaos_fail!(
            "{ctx}: the readers did not use Redis again without a degraded read within {}s",
            t.as_secs()
        )
    })?;
    crate::log(format!(
        "{ctx}: readers recovered after {}s (redis lookups {} degraded {})",
        elapsed.as_secs(),
        sample.redis,
        sample.degraded
    ));
    Ok(())
}

/// The readers degrade instead of stalling: a probe read raises the
/// degraded count.
async fn wait_readers_degraded(h: &Harness, ctx: &str) -> anyhow::Result<()> {
    wait_reader_moves(h, ctx, "degraded reads", Budget::secs(60, 2), |r| {
        r.degraded
    })
    .await
}

/// The readers reach Redis at all: the precondition of every case.
async fn wait_readers_connected(h: &Harness, ctx: &str) -> anyhow::Result<()> {
    wait_reader_moves(h, ctx, "redis lookups", Budget::secs(90, 3), |r| r.redis).await
}

/// The highest mirror head across the executor nodes. `None` when no
/// mirror exporter answers.
async fn mirror_head(h: &Harness) -> Option<i64> {
    let mut best = None;
    for i in 0..h.probes.executors.len() {
        best = best.max(h.probes.mirror_metric(i, MIRROR_HEAD).await);
    }
    best
}

/// The rebuild count summed over the mirrors.
async fn mirror_rebuilds(h: &Harness) -> i64 {
    let mut total = 0;
    for i in 0..h.probes.executors.len() {
        total += h
            .probes
            .mirror_metric(i, MIRROR_REBUILDS)
            .await
            .unwrap_or(0);
    }
    total
}

/// The mirror head advances: the projection follows the chain again.
async fn wait_mirror_advances(h: &Harness, ctx: &str) -> anyhow::Result<()> {
    let start = mirror_head(h).await.unwrap_or(0);
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

/// The aux node, which runs the primary and one sentinel.
fn aux(h: &Harness) -> String {
    h.probes.validator.container.clone()
}

/// The primary's inner container on the aux node.
async fn primary_container(h: &Harness, ctx: &str) -> anyhow::Result<String> {
    h.nodes
        .inner_cid(&aux(h), "redis-")
        .await
        .ok_or_else(|| crate::chaos_fail!("{ctx}: no inner redis container on {}", aux(h)))
}

/// Ask the aux node's sentinel who the primary is: the host of
/// `SENTINEL get-master-addr-by-name`.
async fn sentinel_master(h: &Harness) -> anyhow::Result<String> {
    let script = "docker exec $(docker ps --filter name=sentinel- -q | head -1) \
                  redis-cli -p 26379 SENTINEL get-master-addr-by-name kardamom | head -1";
    let host = h.nodes.exec(&aux(h), script).await?;
    anyhow::ensure!(!host.is_empty(), "the sentinel named no primary");
    Ok(host)
}

/// The primary frozen past the sentinels' down-after: the readers
/// degrade and the pipeline progresses meanwhile, the sentinels promote
/// the replica, and after the thaw the readers and the mirror use the
/// promoted primary.
pub(crate) async fn redis_primary_freeze(h: &mut Harness) -> anyhow::Result<()> {
    let ctx = "redis-primary-freeze";
    wait_readers_connected(h, ctx).await?;
    let node = aux(h);
    let primary = primary_container(h, ctx).await?;
    let master0 = sentinel_master(h).await?;
    crate::log(format!(
        "{ctx}: primary {master0} ({primary} on {node}); freezing it for {}s",
        PRIMARY_FREEZE.as_secs()
    ));
    h.nodes
        .inner_signal(&node, &primary, crate::nodes::Signal::Stop)
        .await
        .map_err(|e| crate::chaos_fail!("{ctx}: SIGSTOP failed: {e}"))?;
    let frozen = frozen_phase(h, ctx, &master0).await;
    let _ = h
        .nodes
        .inner_signal(&node, &primary, crate::nodes::Signal::Cont)
        .await;
    frozen?;
    wait_readers_recovered(h, ctx).await?;
    wait_mirror_advances(h, ctx).await?;
    h.assert_progress().await
}

/// The frozen half of [`redis_primary_freeze`]: the freeze took, the
/// readers degrade, the pipeline progresses, the sentinels promote.
async fn frozen_phase(h: &Harness, ctx: &str, master0: &str) -> anyhow::Result<()> {
    let primary = primary_container(h, ctx).await?;
    let answers = h
        .nodes
        .exec_status(
            &aux(h),
            &format!("timeout 2 docker exec {primary} redis-cli ping"),
        )
        .await?;
    anyhow::ensure!(
        !answers,
        "{ctx}: the freeze did NOT take (the primary still answers PING)"
    );
    wait_readers_degraded(h, ctx).await?;
    h.assert_progress().await?;
    let outcome = poll::until(
        Budget::new(PRIMARY_FREEZE * 3, Duration::from_secs(3)),
        |_| async move {
            let master = sentinel_master(h).await?;
            Ok::<_, anyhow::Error>((master != master0).then_some(master))
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
    let node = aux(h);
    h.inject_hard(&[&node], "redis-").await?;
    wait_readers_degraded(h, ctx).await?;
    h.assert_progress().await?;
    h.assert_count("redis", REDIS_ALLOCS, h.knobs.restart_slo)
        .await?;
    wait_readers_recovered(h, ctx).await?;
    wait_mirror_advances(h, ctx).await?;
    h.assert_progress().await
}

/// The iptables rule that drops ingress-0's packets to the primary,
/// the replica, and the sentinels.
const REDIS_DROP: &str = "OUTPUT -p tcp -m multiport --dports 6379,26379 -j DROP";

/// Ingress-0 partitioned from Redis: its reads time out and count as
/// degraded, its submits still land, and it recovers when the
/// partition heals.
pub(crate) async fn redis_partition_ingress(h: &mut Harness) -> anyhow::Result<()> {
    let ctx = "redis-partition-ingress";
    wait_readers_connected(h, ctx).await?;
    let node = h.probes.ingresses[0].container.clone();
    h.nodes
        .exec(&node, &format!("iptables -w 5 -I {REDIS_DROP}"))
        .await
        .map_err(|e| crate::chaos_fail!("{ctx}: could not install the drop rule on {node}: {e}"))?;
    crate::log(format!(
        "{ctx}: {node} dropping its Redis and sentinel packets"
    ));
    let partitioned = partitioned_phase(h, ctx).await;
    let _ = h
        .nodes
        .exec(&node, &format!("iptables -w 5 -D {REDIS_DROP}"))
        .await;
    partitioned?;
    crate::log(format!("{ctx}: partition healed on {node}"));
    wait_readers_recovered(h, ctx).await?;
    h.assert_progress().await
}

async fn partitioned_phase(h: &Harness, ctx: &str) -> anyhow::Result<()> {
    wait_readers_degraded(h, ctx).await?;
    h.assert_progress().await
}

/// The three mirrors hard-killed and the projection flushed: the
/// restarted mirrors find Redis cold, rebuild from the executors'
/// newest checkpoint, and the head advances again. The genesis account
/// of the smoke transfers proves the rebuild wrote the checkpoint's
/// accounts, not only the live rows.
pub(crate) async fn mirror_kill_rebuild(h: &mut Harness) -> anyhow::Result<()> {
    let ctx = "mirror-kill-rebuild";
    wait_readers_connected(h, ctx).await?;
    let rebuilds0 = mirror_rebuilds(h).await;
    let nodes: Vec<String> = h
        .probes
        .executors
        .iter()
        .map(|e| e.container.clone())
        .collect();
    for node in &nodes[1..] {
        if let Some(cid) = h.nodes.inner_cid(node, "state-mirror").await {
            h.nodes.inner_kill(node, &cid).await?;
        }
    }
    h.inject_hard(&[&nodes[0]], "state-mirror").await?;
    let master = sentinel_master(h).await?;
    h.nodes
        .exec(
            &aux(h),
            &format!(
                "docker exec $(docker ps --filter name=sentinel- -q | head -1) \
                 redis-cli -h {master} -p 6379 FLUSHALL"
            ),
        )
        .await
        .map_err(|e| crate::chaos_fail!("{ctx}: FLUSHALL on {master} failed: {e}"))?;
    crate::log(format!("{ctx}: mirrors killed and {master} flushed"));
    h.assert_count("state-mirror", MIRROR_ALLOCS, h.knobs.restart_slo)
        .await?;
    let hs: &Harness = h;
    let outcome = poll::until(Budget::secs(300, 5), |_| async move {
        Ok::<_, anyhow::Error>(Some(mirror_rebuilds(hs).await).filter(|n| *n > rebuilds0))
    })
    .await?;
    let (rebuilds, elapsed) = outcome
        .or_fail(|t| crate::chaos_fail!("{ctx}: no mirror rebuilt within {}s", t.as_secs()))?;
    crate::log(format!(
        "{ctx}: rebuilds {rebuilds0} -> {rebuilds} after {}s",
        elapsed.as_secs()
    ));
    wait_mirror_advances(h, ctx).await?;
    assert_genesis_account_projected(h, ctx, &master).await?;
    wait_readers_recovered(h, ctx).await?;
    h.assert_progress().await
}

/// The first genesis account has a projected row: the rebuild scanned
/// the checkpoint, so an account no live batch touched is present.
async fn assert_genesis_account_projected(
    h: &Harness,
    ctx: &str,
    master: &str,
) -> anyhow::Result<()> {
    let address = derive_signers(ANVIL_MNEMONIC, 1)?
        .pop()
        .ok_or_else(|| crate::chaos_fail!("{ctx}: no genesis signer"))?
        .signer
        .address();
    let script = format!(
        "docker exec $(docker ps --filter name=sentinel- -q | head -1) \
         redis-cli -h {master} -p 6379 HLEN acct:{address}"
    );
    let script = &script;
    let outcome = poll::until(Budget::secs(120, 5), |_| async move {
        let fields: i64 = h
            .nodes
            .exec(&aux(h), script)
            .await?
            .trim()
            .parse()
            .unwrap_or(0);
        Ok::<_, anyhow::Error>((fields > 0).then_some(fields))
    })
    .await?;
    outcome
        .or_fail(|t| {
            crate::chaos_fail!(
                "{ctx}: genesis account {address} has no projected row within {}s of the rebuild",
                t.as_secs()
            )
        })
        .map(|_| ())
}
