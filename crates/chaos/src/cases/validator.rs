//! The validator cases: the lapse, the fresh join, and the whole-stack
//! CPU squeeze, plus the warm-up gate they share.

use std::cell::{Cell, RefCell};
use std::time::Duration;

use crate::harness::Harness;
use crate::poll::{self, Budget};

const VERIFIED: &str = "validator_blocks_verified_total";
const COMMITTED: &str = "validator_committed_block";
pub(crate) const DIVERGENCE: &str = "validator_divergence_total";
const BAL_MISSING: &str = "validator_bal_missing_total";

/// The last warm-up observations, for the log or the failure line.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct Warm {
    pub(crate) verified: i64,
    pub(crate) block: i64,
    pub(crate) exec: i64,
    pub(crate) elapsed: Duration,
}

impl Warm {
    pub(crate) fn lag(self) -> i64 {
        self.exec.saturating_sub(self.block)
    }
}

/// Wait until the validator verifies live: its verified counter is
/// advancing and its lag to the executors is within `max_lag`. A
/// validator re-executes from genesis, so a case must hit one that
/// reached the live head, not one still catching up.
///
/// # Errors
///
/// Returns the last observations after `timeout`.
pub(crate) async fn wait_verifying_live(
    h: &Harness,
    timeout: Duration,
    max_lag: i64,
) -> Result<Warm, Warm> {
    let prev = Cell::new(-1_i64);
    let last = Cell::new(Warm::default());
    let (prev_ref, last_ref) = (&prev, &last);
    let outcome = poll::until(
        Budget::new(timeout, Duration::from_secs(6)),
        |elapsed| async move {
            let warm = Warm {
                verified: h.probes.val_metric(VERIFIED).await.unwrap_or(0),
                block: h.probes.val_metric(COMMITTED).await.unwrap_or(0),
                exec: h.probes.executor_progress().await.unwrap_or(0),
                elapsed,
            };
            let live = warm.verified > 0
                && warm.verified > prev_ref.get()
                && warm.block > 0
                && warm.lag() <= max_lag;
            prev_ref.set(warm.verified);
            last_ref.set(warm);
            Ok::<_, anyhow::Error>(live.then_some(warm))
        },
    )
    .await;
    match outcome {
        Ok(poll::Outcome::Ready { value, .. }) => Ok(value),
        _ => Err(last.get()),
    }
}

pub(crate) fn warm_line(w: Warm) -> String {
    format!("verified={} block={} exec={}", w.verified, w.block, w.exec)
}

/// The validator's inner container, or the case failure.
async fn validator_inner(h: &Harness, ctx: &str) -> anyhow::Result<String> {
    h.nodes
        .inner_container(&h.probes.validator.container, "validator")
        .await
        .ok_or_else(|| {
            crate::chaos_fail!(
                "{ctx}: no inner validator container on {}",
                h.probes.validator.container
            )
        })
}

/// Forensics for a validator failure: the Nomad view, the node's
/// containers, and the validator's log tail.
async fn val_debug(h: &Harness) {
    let node = &h.probes.validator.container;
    crate::log("validator DEBUG: containers on the validator node:");
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
    if let Some(inner) = h.nodes.inner_container(node, "validator").await {
        crate::log("validator DEBUG: validator container log tail:");
        println!(
            "{}",
            h.nodes
                .inner_logs(node, &inner, 25)
                .await
                .unwrap_or_default()
        );
    }
}

/// Pause the validator for a window under load, then resume. After the
/// thaw, container identity decides the contract: a survivor keeps
/// verifying past its pre-freeze count with bounded coverage loss; a
/// newborn (the freeze exceeded the driver's client liveness, so the
/// process fail-stopped and Nomad restarted it) verifies live from a
/// fresh counter. Both end verifying live, caught up, with zero
/// divergences.
pub(crate) async fn lapse(h: &mut Harness) -> anyhow::Result<()> {
    let node = h.probes.validator.container.clone();
    let inner = validator_inner(h, "validator-lapse").await?;
    let warm = wait_verifying_live(h, Duration::from_secs(150), 15)
        .await
        .map_err(|w| {
            crate::chaos_fail!(
                "validator-lapse: never warmed up within {}s ({}) — not verifying live BEFORE the pause",
                w.elapsed.as_secs(),
                warm_line(w)
            )
        })?;
    crate::log(format!(
        "validator-lapse: warmed up ({}) after {}s",
        warm_line(warm),
        warm.elapsed.as_secs()
    ));
    let m0 = h.probes.val_metric(BAL_MISSING).await.unwrap_or(0);
    let vf0 = h.probes.val_metric(VERIFIED).await.unwrap_or(0);
    let started0 = h.nodes.started_at(&node, &inner).await;
    crate::log(format!(
        "validator-lapse: freezing {inner} (SIGSTOP) for {}s (verified={vf0} bal_missing={m0} started={})",
        h.knobs.validator_lapse.as_secs(),
        started0.as_deref().unwrap_or("?")
    ));
    h.freeze_verified(
        &node,
        &inner,
        &h.probes.validator_target(),
        "validator-lapse",
    )
    .await?;
    tokio::time::sleep(
        h.knobs
            .validator_lapse
            .saturating_sub(Duration::from_secs(3)),
    )
    .await;
    thaw_validator(h, &node, &inner).await;
    confirm_thaw(h, &node, &inner).await;
    let (path, sample) = sample_recovery(h, &node, started0.as_deref(), vf0).await?;
    let d1 = h
        .probes
        .val_metric_required(DIVERGENCE, "validator-lapse divergence==0 assert")
        .await?;
    anyhow::ensure!(
        d1 == 0,
        "{}: validator-lapse: {d1} divergence(s) after recovery",
        crate::FAIL_PREFIX
    );
    if path == Path::Survivor {
        let m1 = h.probes.val_metric(BAL_MISSING).await.unwrap_or(0);
        anyhow::ensure!(
            m1.saturating_sub(m0) <= 5,
            "{}: validator-lapse: coverage REGRESSED on the survivor path — bal_missing grew {m0}->{m1} (lapse window not covered by the live term buffer)",
            crate::FAIL_PREFIX
        );
        crate::log(format!(
            "validator-lapse PASS (survivor): kept verifying {vf0}->{}, bal_missing {m0}->{m1}, 0 divergences",
            sample.verified
        ));
    } else {
        crate::log(format!(
            "validator-lapse PASS (newborn): crash-only recovery verified — fresh process verifying live (verified={}, lag {}), 0 divergences (bal_missing not comparable across restart)",
            sample.verified,
            sample.lag()
        ));
    }
    Ok(())
}

/// SIGCONT, with the replacement path tolerated: the frozen task can be
/// replaced mid-freeze by the supervisor.
async fn thaw_validator(h: &Harness, node: &str, inner: &str) {
    if h.thaw(node, inner).await.is_ok() {
        return;
    }
    let current = h.nodes.inner_container(node, "validator").await;
    if current.as_deref().is_some_and(|c| c != inner) {
        crate::log(format!(
            "validator-lapse: SIGCONT target gone — container replaced during freeze ({inner} -> {}); newborn path",
            current.unwrap_or_default()
        ));
        return;
    }
    tokio::time::sleep(Duration::from_secs(5)).await;
    if h.thaw(node, inner).await.is_err() {
        crate::log(
            "validator-lapse: SIGCONT failed twice (state unknown); relying on supervisor + sampling asserts",
        );
    }
}

/// The verified thaw: within a grace window the endpoint must answer or
/// the container must have been replaced. Otherwise the frozen orphan
/// is killed, so it frees the metrics port for the supervisor's
/// replacement instead of stranding every restart on EADDRINUSE.
async fn confirm_thaw(h: &Harness, node: &str, inner: &str) {
    let target = h.probes.validator_target();
    let target = &target;
    let outcome = poll::after_sleep(Budget::secs(30, 5), |_| async move {
        if h.probes.scrape().answers(target).await {
            return Ok::<_, anyhow::Error>(Some(()));
        }
        let current = h.nodes.inner_container(node, "validator").await;
        Ok(current.filter(|c| c != inner).map(|c| {
            crate::log(format!(
                "validator-lapse: container replaced during/after freeze ({inner} -> {c})"
            ));
        }))
    })
    .await;
    if !matches!(outcome, Ok(poll::Outcome::Ready { .. })) {
        crate::log(
            "validator-lapse: thaw NOT confirmed after 30s — killing the frozen orphan (releases the metrics port for the supervisor's replacement)",
        );
        let _ = h.nodes.inner_kill(node, inner).await;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Path {
    Survivor,
    Newborn,
}

/// Sample until the validator verifies live and caught up on either
/// path. A failed scrape is not counted.
async fn sample_recovery(
    h: &Harness,
    node: &str,
    started0: Option<&str>,
    vf0: i64,
) -> anyhow::Result<(Path, Warm)> {
    let last = Cell::new((Path::Survivor, Warm::default()));
    let last_ref = &last;
    let outcome = poll::after_sleep(Budget::secs(240, 10), |elapsed| async move {
        let Some((path, w)) = recovery_sample(h, node, started0, elapsed).await else {
            crate::log(format!(
                "validator-lapse: sample t={}s SCRAPE FAILED (not counted)",
                elapsed.as_secs()
            ));
            return Ok::<_, anyhow::Error>(None);
        };
        last_ref.set((path, w));
        crate::log(format!(
            "validator-lapse: sample t={}s path={path:?} {}",
            elapsed.as_secs(),
            warm_line(w)
        ));
        let ok = match path {
            Path::Newborn => w.verified > 0 && w.block > 0 && w.lag() <= 25,
            Path::Survivor => w.verified > vf0 && w.lag() <= 25,
        };
        Ok(ok.then_some((path, w)))
    })
    .await?;
    let last = last.get();
    match outcome {
        poll::Outcome::Ready { value, .. } => Ok(value),
        poll::Outcome::TimedOut { elapsed } => {
            val_debug(h).await;
            Err(crate::chaos_fail!(
                "validator-lapse: validator not verifying live + caught up within {}s of thaw (path={:?}, {})",
                elapsed.as_secs(),
                last.0,
                warm_line(last.1)
            ))
        }
    }
}

async fn recovery_sample(
    h: &Harness,
    node: &str,
    started0: Option<&str>,
    elapsed: Duration,
) -> Option<(Path, Warm)> {
    let current = h.nodes.inner_container(node, "validator").await;
    let started1 = match &current {
        Some(c) => h.nodes.started_at(node, c).await,
        None => None,
    };
    let verified = h.probes.val_metric(VERIFIED).await?;
    let block = h.probes.val_metric(COMMITTED).await?;
    let exec = h.probes.executor_progress().await.unwrap_or(0);
    let newborn = matches!((started0, started1.as_deref()), (Some(a), Some(b)) if a != b);
    let path = if newborn {
        Path::Newborn
    } else {
        Path::Survivor
    };
    Some((
        path,
        Warm {
            verified,
            block,
            exec,
            elapsed,
        },
    ))
}

/// A fresh validator joins a running chain: kill the validator, wipe
/// its state inside the restart delay, and require the newborn to adopt
/// a peer checkpoint, bootstrap its trie, catch up, and verify with
/// zero divergences and a live state root.
pub(crate) async fn join(h: &mut Harness) -> anyhow::Result<()> {
    let node = h.probes.validator.container.clone();
    let inner = validator_inner(h, "validator-join").await?;
    let warm = wait_verifying_live(h, Duration::from_secs(150), 15)
        .await
        .map_err(|w| {
            crate::chaos_fail!(
                "validator-join: cluster never warmed up within {}s ({})",
                w.elapsed.as_secs(),
                warm_line(w)
            )
        })?;
    crate::log(format!(
        "validator-join: warmed up ({}); wiping the validator for a fresh join",
        warm_line(warm)
    ));
    let started0 = h.nodes.started_at(&node, &inner).await;
    h.nodes
        .inner_kill(&node, &inner)
        .await
        .map_err(|e| crate::chaos_fail!("validator-join: kill failed: {e}"))?;
    h.nodes
        .exec(
            &node,
            "rm -rf /opt/kardamom/state/validator /opt/kardamom/checkpoints/*",
        )
        .await
        .map_err(|e| crate::chaos_fail!("validator-join: state wipe failed: {e}"))?;
    crate::log("validator-join: validator killed, state + checkpoint staging wiped");
    let newborn = RefCell::new(None::<String>);
    let last = Cell::new(Warm::default());
    let (hs, node_ref, started0_ref) = (&*h, node.as_str(), started0.as_deref());
    let (newborn_ref, last_ref) = (&newborn, &last);
    let outcome = poll::until(Budget::secs(240, 10), |elapsed| async move {
        if newborn_ref.borrow().is_none() {
            *newborn_ref.borrow_mut() = newborn_container(hs, node_ref, started0_ref).await;
        }
        let w = Warm {
            verified: hs.probes.val_metric(VERIFIED).await.unwrap_or(0),
            block: hs.probes.val_metric(COMMITTED).await.unwrap_or(0),
            exec: hs.probes.executor_progress().await.unwrap_or(0),
            elapsed,
        };
        last_ref.set(w);
        let joined =
            newborn_ref.borrow().is_some() && w.verified > 0 && w.block > 0 && w.lag() <= 25;
        Ok::<_, anyhow::Error>(joined.then_some(w))
    })
    .await?;
    let newborn = newborn.into_inner();
    let poll::Outcome::Ready { value: w, .. } = outcome else {
        val_debug(h).await;
        return Err(crate::chaos_fail!(
            "validator-join: fresh validator not verifying + caught up within 240s (newborn={}, {})",
            newborn.as_deref().unwrap_or("none"),
            warm_line(last.get())
        ));
    };
    let newborn = newborn.unwrap_or_default();
    let logs = h
        .nodes
        .inner_logs(&node, &newborn, 400)
        .await
        .unwrap_or_default();
    for (needle, why) in [
        (
            "adopted state from checkpoint",
            "newborn did not adopt a peer checkpoint (genesis replay? peers unreachable?)",
        ),
        (
            "trie bootstrap complete",
            "adopted state but no trie bootstrap ran (trie-off image not detected?)",
        ),
    ] {
        require_line(h, &logs, needle, why).await?;
    }
    let div = h
        .probes
        .val_metric_required(DIVERGENCE, "validator-join divergence==0 assert")
        .await?;
    if div != 0 {
        val_debug(h).await;
        return Err(crate::chaos_fail!(
            "validator-join: {div} divergence(s) after join"
        ));
    }
    let root_blk = h
        .probes
        .val_metric("validator_state_root_block")
        .await
        .unwrap_or(0);
    if root_blk <= 0 {
        val_debug(h).await;
        return Err(crate::chaos_fail!(
            "validator-join: no MPT state-root observation after join (trie dead?)"
        ));
    }
    crate::log(format!(
        "validator-join PASS: fresh validator adopted a peer checkpoint, bootstrapped the trie, caught up (lag {}), verifying live (verified={}), root observed at block {root_blk}, 0 divergences",
        w.lag(),
        w.verified
    ));
    Ok(())
}

async fn require_line(h: &Harness, logs: &str, needle: &str, why: &str) -> anyhow::Result<()> {
    if logs.contains(needle) {
        return Ok(());
    }
    val_debug(h).await;
    Err(crate::chaos_fail!("validator-join: {why}"))
}

/// The newborn validator container, once its `StartedAt` differs from
/// the killed one's.
async fn newborn_container(h: &Harness, node: &str, started0: Option<&str>) -> Option<String> {
    let current = h.nodes.inner_container(node, "validator").await?;
    let started1 = h.nodes.started_at(node, &current).await?;
    if started0 == Some(started1.as_str()) {
        return None;
    }
    crate::log(format!(
        "validator-join: newborn container {current} up (started {started1})"
    ));
    Some(current)
}
