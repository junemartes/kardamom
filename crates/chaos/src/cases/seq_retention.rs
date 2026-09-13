//! The sequencer lapse and the retention-overrun cases: a running
//! consumer is frozen with SIGSTOP and must repair itself on thaw.

use std::cell::Cell;
use std::time::Duration;

use crate::harness::Harness;
use crate::nomad::Streams;
use crate::poll::{self, Budget};
use crate::probes::EXECUTOR_BLOCK_METRIC;

const LAG_SUSPECTED: &str = "kardamom_sequencer_resync_lag_suspected_total";
const RESYNC_ENTERED: &str = "kardamom_sequencer_resync_entered_total";
const RESYNC_MODE: &str = "kardamom_sequencer_resync_mode";

/// Forensics for a sequencer-lapse failure: container identity, the
/// resync metrics, and the replica's recent log lines.
async fn seqa_debug(h: &Harness) {
    let node = &h.probes.sequencers[0].container;
    crate::log(format!(
        "sequencer-lapse DEBUG: inner containers on {node}:"
    ));
    println!(
        "{}",
        h.nodes
            .exec(
                node,
                "docker ps -a --format '{{.Names}} {{.Status}}' | head -6"
            )
            .await
            .unwrap_or_default()
    );
    crate::log("sequencer-lapse DEBUG: resync metrics at sequencer-0:9001:");
    let body = h
        .probes
        .scrape()
        .fetch(&h.probes.sequencer_lane0_target(0))
        .await;
    body.unwrap_or_default()
        .lines()
        .filter(|l| l.contains("resync") || l.contains("watermark") || l.contains("floor"))
        .take(12)
        .for_each(|l| println!("{l}"));
    if let Some(inner) = h.nodes.inner_container(node, "sequencer-0").await {
        crate::log("sequencer-lapse DEBUG: current sequencer-0 log tail:");
        println!(
            "{}",
            h.nodes
                .inner_logs(node, &inner, 20)
                .await
                .unwrap_or_default()
        );
    }
}

/// One post-thaw observation of the lapsed replica.
#[derive(Debug, Clone, Copy, Default)]
struct LapseSample {
    lag: Option<i64>,
    entered: Option<i64>,
    mode: Option<i64>,
}

impl LapseSample {
    fn scraped(self) -> bool {
        self.lag.is_some() || self.entered.is_some()
    }

    fn show(self) -> String {
        let s = |v: Option<i64>| v.map_or("?".to_string(), |x| x.to_string());
        format!(
            "lag={} entered={} mode={}",
            s(self.lag),
            s(self.entered),
            s(self.mode)
        )
    }
}

/// Pause lane 0's replica on node 0 for a window under shard-0 load,
/// then resume. The twin keeps ordering; the lapsed replica must detect
/// the lapse and engage resync, on the survivor path (counters move
/// past their baselines or resync mode is active) or on the newborn
/// path (the freeze got its Aeron client evicted, Nomad restarted it,
/// and the fresh process entered startup resync).
pub(crate) async fn sequencer_lapse(h: &mut Harness) -> anyhow::Result<()> {
    let node = h.probes.sequencers[0].container.clone();
    let inner = h
        .nodes
        .inner_container(&node, "sequencer-0")
        .await
        .ok_or_else(|| {
            crate::chaos_fail!("sequencer-lapse: no inner sequencer-0 container on {node}")
        })?;
    let l0 = h
        .probes
        .seq_lane0_metric(0, LAG_SUSPECTED)
        .await
        .unwrap_or(0);
    let r0 = h
        .probes
        .seq_lane0_metric(0, RESYNC_ENTERED)
        .await
        .unwrap_or(0);
    let started0 = h.nodes.started_at(&node, &inner).await;
    crate::log(format!(
        "sequencer-lapse: freezing {inner} (SIGSTOP) for {}s (lag_suspected={l0} resync_entered={r0} started={})",
        h.knobs.seq_lapse.as_secs(),
        started0.as_deref().unwrap_or("?")
    ));
    h.freeze_verified(
        &node,
        &inner,
        &h.probes.sequencer_lane0_target(0),
        "sequencer-lapse",
    )
    .await?;
    tokio::time::sleep(h.knobs.seq_lapse.saturating_sub(Duration::from_secs(3))).await;
    h.thaw(&node, &inner)
        .await
        .map_err(|e| crate::chaos_fail!("sequencer-lapse: SIGCONT failed: {e}"))?;
    crate::log("sequencer-lapse: resumed; twin must have covered (no stall)");
    h.assert_progress().await?;
    let good = Cell::new(0_u32);
    let last = Cell::new(LapseSample::default());
    let (hs, node_ref, started0_ref) = (&*h, node.as_str(), started0.as_deref());
    let (good_ref, last_ref) = (&good, &last);
    let outcome = poll::until(Budget::secs(240, 10), |elapsed| async move {
        let s = lapse_sample(hs).await;
        if !s.scraped() {
            crate::log(format!(
                "sequencer-lapse: sample t={}s SCRAPE FAILED (not counted as zero)",
                elapsed.as_secs()
            ));
            return Ok::<_, anyhow::Error>(None);
        }
        good_ref.set(good_ref.get().saturating_add(1));
        last_ref.set(s);
        crate::log(format!(
            "sequencer-lapse: lapsed-replica sample t={}s {} (scrape ok #{})",
            elapsed.as_secs(),
            s.show(),
            good_ref.get()
        ));
        Ok(engaged(hs, node_ref, started0_ref, s, l0, r0)
            .await
            .then_some(s))
    })
    .await?;
    let (good, last) = (good.get(), last.get());
    let s = match outcome {
        poll::Outcome::Ready { value, .. } => value,
        poll::Outcome::TimedOut { .. } => {
            seqa_debug(h).await;
            if good == 0 {
                return Err(crate::chaos_fail!(
                    "sequencer-lapse: lapsed-replica metrics unreachable for 240s after resume (0 successful scrapes) — cannot judge detection"
                ));
            }
            return Err(crate::chaos_fail!(
                "sequencer-lapse: lapsed replica never engaged resync within 240s of resume (baseline lag {l0} entered {r0}; last {}; {good} good scrapes)",
                last.show()
            ));
        }
    };
    crate::log(format!(
        "sequencer-lapse: lapsed replica engaged resync (baseline lag {l0} entered {r0}; now {})",
        s.show()
    ));
    let target = h.probes.sequencer_lane0_target(0);
    h.assert_replica_healthy(&target, Duration::from_secs(90))
        .await?;
    crate::log(
        "sequencer-lapse PASS: progress held, lag detected, resync engaged, replica healthy",
    );
    Ok(())
}

async fn lapse_sample(h: &Harness) -> LapseSample {
    LapseSample {
        lag: h.probes.seq_lane0_metric(0, LAG_SUSPECTED).await,
        entered: h.probes.seq_lane0_metric(0, RESYNC_ENTERED).await,
        mode: h.probes.seq_lane0_metric(0, RESYNC_MODE).await,
    }
}

/// Whether one sample proves the lapse contract, on either path.
async fn engaged(
    h: &Harness,
    node: &str,
    started0: Option<&str>,
    s: LapseSample,
    l0: i64,
    r0: i64,
) -> bool {
    if s.lag.is_some_and(|l| l > l0)
        || s.entered.is_some_and(|r| r > r0)
        || s.mode.is_some_and(|m| m >= 1)
    {
        return true;
    }
    let restarted = match h.nodes.inner_container(node, "sequencer-0").await {
        Some(cur) => h.nodes.started_at(node, &cur).await,
        None => None,
    }
    .is_some_and(|s1| started0.is_some_and(|s0| s0 != s1));
    if restarted && s.entered.is_some_and(|r| r >= 1) {
        crate::log(format!(
            "sequencer-lapse: replica RESTARTED across the freeze; fresh process entered startup resync (entered={})",
            s.entered.unwrap_or(0)
        ));
        return true;
    }
    if s.entered.is_some_and(|r| r < r0 && r >= 1) {
        crate::log(format!(
            "sequencer-lapse: replica restarted across the freeze (entered {r0} -> {}); startup resync engaged",
            s.entered.unwrap_or(0)
        ));
        return true;
    }
    false
}

/// Which consumer the retention overrun freezes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Victim {
    /// executor-2; nodes 0 and 1 stay untouched as checkpoint donors.
    Executor,
    Validator,
}

impl Victim {
    fn kind(self) -> &'static str {
        match self {
            Self::Executor => "executor",
            Self::Validator => "validator",
        }
    }

    fn restored_needle(self) -> &'static str {
        match self {
            Self::Executor => "restored state from checkpoint",
            Self::Validator => "adopted state from checkpoint",
        }
    }
}

/// Freeze a running consumer until the cluster's bounded egress
/// retention rolls past its cursor, so on thaw its replay is refused
/// and it must repair itself: fetch a peer checkpoint, park the stale
/// state, restart, restore or adopt, and rejoin. The freeze also
/// crosses the cluster session timeout, so the resume goes through a
/// fresh session.
pub(crate) async fn retention_overrun(h: &mut Harness, victim: Victim) -> anyhow::Result<()> {
    let kind = victim.kind();
    let ctx = format!("retention-overrun({kind})");
    let retention = h.knobs.cluster_retention.ok_or_else(|| {
        crate::chaos_fail!("{ctx}: KARDAMOM_CLUSTER_RETENTION is not set — this case only means something on a cluster deployed with a small -Dkardamom.cluster.retention")
    })?;
    let (node, target) = match victim {
        Victim::Executor => (
            h.probes.executors[2].container.clone(),
            h.probes.executor_target(2),
        ),
        Victim::Validator => (
            h.probes.validator.container.clone(),
            h.probes.validator_target(),
        ),
    };
    let inner = h
        .nodes
        .inner_container(&node, kind)
        .await
        .ok_or_else(|| crate::chaos_fail!("{ctx}: no inner {kind} container on {node}"))?;
    let cid0 = h.nodes.inner_cid(&node, &inner).await;
    let donor = h.container("executor-0")?;
    super::component::wait_peer_checkpoint(h, &donor, &ctx).await?;
    require_live_victim(h, victim, &ctx).await?;
    let need = i64::try_from(retention.get().saturating_mul(2)).unwrap_or(i64::MAX);
    let rx_freeze = h.probes.ingress_received().await.unwrap_or(0);
    crate::log(format!(
        "{ctx}: freezing {inner} on {node} until {need} frames flow past it (retention={retention}, cap {}s)",
        h.knobs.retention_freeze_cap.as_secs()
    ));
    h.freeze_verified(&node, &inner, &target, &ctx).await?;
    let (delta, elapsed) = overrun_window(h, rx_freeze, need, &ctx, &node, &inner).await?;
    crate::log(format!(
        "{ctx}: window overrun ({delta} frames in {}s ≈ {}tps); thawing",
        elapsed.as_secs(),
        delta
            .checked_div(i64::try_from(elapsed.as_secs()).unwrap_or(i64::MAX))
            .unwrap_or(0)
    ));
    if h.thaw(&node, &inner).await.is_err() {
        crate::log(format!(
            "{ctx}: SIGCONT failed (container may have been replaced mid-freeze); the log asserts below own the verdict"
        ));
    }
    await_repair_chain(h, victim, &ctx, delta, elapsed).await?;
    let cid_now = h.nodes.inner_cid(&node, &inner).await;
    anyhow::ensure!(
        cid_now.is_some() && cid_now != cid0,
        "{}: {ctx}: victim container was not restarted (cid {} -> {}) — the park/exit/restore loop did not complete",
        crate::FAIL_PREFIX,
        cid0.as_deref().unwrap_or("?"),
        cid_now.as_deref().unwrap_or("gone")
    );
    match victim {
        Victim::Executor => h.assert_executor_progress(Duration::from_secs(180)).await,
        Victim::Validator => await_verifying_resumed(h).await,
    }
}

/// The freeze must hit a live consumer, proven by its own gauge
/// advancing.
async fn require_live_victim(h: &Harness, victim: Victim, ctx: &str) -> anyhow::Result<()> {
    let prev = Cell::new(None);
    let prev_ref = &prev;
    let outcome = poll::until(Budget::secs(120, 6), |_| async move {
        let now = match victim {
            Victim::Executor => h.probes.exec_metric(2, EXECUTOR_BLOCK_METRIC).await,
            Victim::Validator => h.probes.val_metric("validator_committed_block").await,
        }
        .unwrap_or(0);
        let live = prev_ref.get().is_some_and(|p| now > p);
        prev_ref.set(Some(now));
        Ok::<_, anyhow::Error>(live.then_some(now))
    })
    .await?;
    outcome
        .or_fail(|_| crate::chaos_fail!("{ctx}: victim not demonstrably live before the freeze; freezing a dead consumer asserts nothing"))
        .map(|_| ())
}

/// Hold the freeze until enough frames flowed past the frozen cursor
/// and the cluster session lapsed, within the cap. Frames are counted
/// from observed accepted transactions, not the target rate.
async fn overrun_window(
    h: &Harness,
    rx_freeze: i64,
    need: i64,
    ctx: &str,
    node: &str,
    inner: &str,
) -> anyhow::Result<(i64, Duration)> {
    let budget = Budget::new(h.knobs.retention_freeze_cap, Duration::from_secs(15));
    let delta = Cell::new(0_i64);
    let delta_ref = &delta;
    let outcome = poll::after_sleep(budget, |elapsed| async move {
        let rx_now = h.probes.ingress_received().await.unwrap_or(rx_freeze);
        let d = rx_now.saturating_sub(rx_freeze);
        delta_ref.set(d);
        let overrun = d >= need && elapsed >= Duration::from_secs(120);
        Ok::<_, anyhow::Error>(overrun.then_some(d))
    })
    .await?;
    let delta = delta.get();
    match outcome {
        poll::Outcome::Ready { value, elapsed } => {
            Ok((value, elapsed.saturating_add(Duration::from_secs(3))))
        }
        poll::Outcome::TimedOut { elapsed } => {
            let _ = h.thaw(node, inner).await;
            Err(crate::chaos_fail!(
                "{ctx}: load too slow to overrun the retention window — only {delta} of {need} frames flowed in {}s; raise the load rate or lower KARDAMOM_CLUSTER_RETENTION",
                elapsed.as_secs()
            ))
        }
    }
}

/// The recovery evidence splits across container generations, so the
/// only stream holding both halves is the allocation's own Nomad log.
async fn await_repair_chain(
    h: &Harness,
    victim: Victim,
    ctx: &str,
    delta: i64,
    frozen: Duration,
) -> anyhow::Result<()> {
    let kind = victim.kind();
    let restored = victim.restored_needle();
    let seen = Cell::new((false, false, false));
    let seen_ref = &seen;
    let outcome = poll::until(Budget::secs(300, 6), |_| async move {
        let logs = h.nomad.job_logs(kind, Streams::Both).await?;
        let s = (
            logs.contains("cluster replay unavailable"),
            logs.contains("resync prepared: peer checkpoint staged"),
            logs.contains(restored),
        );
        seen_ref.set(s);
        Ok((s.0 && s.1 && s.2).then_some(()))
    })
    .await?;
    let seen = seen.get();
    let elapsed = match outcome {
        poll::Outcome::Ready { elapsed, .. } => elapsed,
        poll::Outcome::TimedOut { .. } => {
            return Err(repair_failure(seen, ctx, delta, frozen, restored));
        }
    };
    crate::log(format!(
        "{ctx}: REPLAY_UNAVAILABLE -> fetch -> park -> restart -> restore observed ({}s after thaw)",
        elapsed.as_secs()
    ));
    Ok(())
}

fn repair_failure(
    seen: (bool, bool, bool),
    ctx: &str,
    delta: i64,
    frozen: Duration,
    restored: &str,
) -> anyhow::Error {
    if !seen.0 {
        return crate::chaos_fail!(
            "{ctx}: consumer never hit REPLAY_UNAVAILABLE after a {}s freeze with {delta} frames flowed — the retention tier was NOT exercised (is the deployed -Dkardamom.cluster.retention what KARDAMOM_CLUSTER_RETENTION says?)",
            frozen.as_secs()
        );
    }
    if !seen.1 {
        return crate::chaos_fail!(
            "{ctx}: REPLAY_UNAVAILABLE hit but the peer-checkpoint resync never completed (no 'resync prepared') — donors dark, or --checkpoint-peers misconfigured"
        );
    }
    crate::chaos_fail!(
        "{ctx}: resync prepared but the restarted consumer never logged '{restored}'"
    )
}

/// The adopted validator must resume verifying, not only commit.
async fn await_verifying_resumed(h: &Harness) -> anyhow::Result<()> {
    let v0 = h
        .probes
        .val_metric("validator_blocks_verified_total")
        .await
        .unwrap_or(0);
    let outcome = poll::after_sleep(Budget::secs(240, 10), |_| async move {
        let v1 = h
            .probes
            .val_metric("validator_blocks_verified_total")
            .await
            .unwrap_or(0);
        Ok::<_, anyhow::Error>((v1 > v0).then_some(v1))
    })
    .await?;
    let (v1, elapsed) = outcome.or_fail(|t| {
        crate::chaos_fail!(
            "retention-overrun(validator): adopted validator never resumed verifying (blocks_verified {v0} -> ? over {}s)",
            t.as_secs()
        )
    })?;
    crate::log(format!(
        "retention-overrun(validator): verifying resumed after adoption (blocks_verified {v0} -> {v1}, {}s)",
        elapsed.as_secs()
    ));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_sample_counts_only_when_a_scrape_succeeded() {
        assert!(!LapseSample::default().scraped());
        let s = LapseSample {
            lag: None,
            entered: Some(1),
            mode: None,
        };
        assert!(s.scraped());
        assert_eq!(s.show(), "lag=? entered=1 mode=?");
        assert_eq!(
            Victim::Executor.restored_needle(),
            "restored state from checkpoint"
        );
    }
}
