//! The whole-stack CPU squeeze: every pipeline node container is
//! throttled at once, in cycles, so sessions lapse and reconnect under
//! starvation. The invariant: the validator may slow down, but must
//! never fork its verdict.

use std::time::Duration;

use super::validator::{DIVERGENCE, wait_verifying_live, warm_line};
use crate::evidence::dump_divergence;
use crate::harness::Harness;

/// Throttle every pipeline node container at once, in cycles, and
/// require the invariant: starvation may slow the validator, but must
/// never fork its verdict.
pub(crate) async fn cpu_squeeze(h: &mut Harness) -> anyhow::Result<()> {
    let s = h.knobs.squeeze.clone();
    let warm = wait_verifying_live(h, Duration::from_secs(150), 15)
        .await
        .map_err(|w| {
            crate::chaos_fail!(
                "cpu-squeeze: validator never verifying live within {}s ({})",
                w.elapsed.as_secs(),
                warm_line(w)
            )
        })?;
    crate::log(format!(
        "cpu-squeeze: warmed up ({}) after {}s",
        warm_line(warm),
        warm.elapsed.as_secs()
    ));
    let nodes = h.nodes.pipeline_nodes().await?;
    anyhow::ensure!(
        !nodes.is_empty(),
        "{}: cpu-squeeze: no kardamom node containers found on the host engine",
        crate::FAIL_PREFIX
    );
    crate::log(format!(
        "cpu-squeeze: {} cycle(s) of {}s at {} CPUs across {} node containers (release {}s between)",
        s.cycles,
        s.window.as_secs(),
        s.cpus_per_node,
        nodes.len(),
        s.release.as_secs()
    ));
    for cycle in 1..=s.cycles.get() {
        squeeze_cycle(h, &nodes, cycle).await?;
    }
    crate::log("cpu-squeeze: restored full CPU; asserting recovery + invariants");
    h.assert_progress().await?;
    let warm = wait_verifying_live(h, s.recover, 15).await.map_err(|w| {
        crate::chaos_fail!(
            "cpu-squeeze: validator not verifying live within {}s of restore ({})",
            s.recover.as_secs(),
            warm_line(w)
        )
    })?;
    let div = h.probes.val_metric(DIVERGENCE).await.unwrap_or(0);
    anyhow::ensure!(
        div == 0,
        "{}: cpu-squeeze: validator counted {div} divergence(s) under starvation",
        crate::FAIL_PREFIX
    );
    if let Some(hit) = h.evidence.divergence_scan().await? {
        dump_divergence(&hit);
        return Err(crate::chaos_fail!(
            "cpu-squeeze: validator diverged under starvation (alloc {}; context above)",
            hit.alloc
        ));
    }
    crate::log(format!(
        "cpu-squeeze PASS: {} nodes starved {}s at {} CPUs, validator recovered (verified={}, lag {}), 0 divergences",
        nodes.len(),
        s.window.as_secs(),
        s.cpus_per_node,
        warm.verified,
        warm.lag()
    ));
    Ok(())
}

/// One squeeze-and-release cycle. The throttle is verified: a silently
/// ignored limit would assert nothing. The restore always runs, with a
/// second pass, since a node left throttled poisons every later case.
async fn squeeze_cycle(h: &Harness, nodes: &[String], cycle: u32) -> anyhow::Result<()> {
    let s = &h.knobs.squeeze;
    for n in nodes {
        h.nodes
            .update_cpus(n, &s.cpus_per_node)
            .await
            .map_err(|e| {
                crate::chaos_fail!("cpu-squeeze: docker update --cpus failed for {n}: {e}")
            })?;
    }
    let nano = h.nodes.nano_cpus(&nodes[0]).await.unwrap_or(0);
    anyhow::ensure!(
        nano > 0,
        "{}: cpu-squeeze: throttle did not take (NanoCpus={nano})",
        crate::FAIL_PREFIX
    );
    crate::log(format!(
        "cpu-squeeze: cycle {cycle}/{} squeezing {}s",
        s.cycles,
        s.window.as_secs()
    ));
    tokio::time::sleep(s.window).await;
    for n in nodes {
        restore_cpus(h, n).await;
    }
    crate::log(format!("cpu-squeeze: cycle {cycle}/{} released", s.cycles));
    if cycle < s.cycles.get() {
        tokio::time::sleep(s.release).await;
    }
    Ok(())
}

async fn restore_cpus(h: &Harness, node: &str) {
    if h.nodes.update_cpus(node, "0").await.is_ok() {
        return;
    }
    tokio::time::sleep(Duration::from_secs(2)).await;
    if h.nodes.update_cpus(node, "0").await.is_err() {
        crate::log(format!(
            "cpu-squeeze: WARNING restore failed for {node} (still throttled)"
        ));
    }
}
