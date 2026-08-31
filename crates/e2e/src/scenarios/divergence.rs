//! `divergence_detection_is_not_vacuous`.
//!
//! The validator-consistency check confirms
//! `validator_divergence_total == 0`. That check means
//! something only if a genuinely divergent stream would trip it. This
//! scenario proves the tripwire: it feeds the validator a corrupt BAL over
//! the real `tx_bal` channel, and requires the documented fail-stop: the
//! halting log line and `std::process::exit(2)`.
//!
//! For determinism, the executor is SIGSTOPped first, so no genuine BAL
//! competes with the injected frame for the target blocks (the sealer
//! keeps stamping boundaries, and the validator keeps re-executing and
//! asking for BALs). The injected blocks stay within the validator's
//! backlog lookbehind, so catch-up mode cannot skip them as unverified.
//!
//! This test runs only on Target-L, by design: it sends process signals
//! and raw Aeron publications, which the Target-C runner cannot reach from
//! outside the cluster. (This closes the `docs/failure-modes.md`
//! "divergence injection" gap.)

use std::time::Duration;

use alloy_primitives::Address;
use anyhow::{Context, Result};

use super::Target;
use crate::harness::metrics::poll_until;
use crate::harness::{LocalStack, inject, l2};

pub async fn corrupt_bal_halts_validator(stack: &mut LocalStack, t: &Target) -> Result<()> {
    // Send a little genuine traffic first. This proves the halt happens on
    // a validator that was verifying happily until the corruption.
    let signers = l2::dev_signers(2)?;
    let to = Address::from([0x77u8; 20]);
    for n in 0..4u64 {
        let tx = l2::sign_transfer(&signers[1], t.chain_id, n, to, 1)?;
        let out = t.rpc.send_raw(&tx.raw).await;
        out.result
            .map_err(|e| anyhow::anyhow!("warmup nonce {n}: {e}"))?;
    }
    poll_until(
        "validator verifying (warmup)",
        // An upper bound, not a pace: a warm run pays actual time. 20-30s
        // was not enough for a cold validator on a contended runner (#250).
        Duration::from_secs(60),
        Duration::from_millis(250),
        || async {
            let v = t
                .validator_metric(super::VALIDATOR_BLOCKS_VERIFIED)
                .await
                .unwrap_or(0.0);
            Ok((v > 0.0).then_some(()))
        },
    )
    .await?;
    let divergence_before = t
        .validator_metric(super::VALIDATOR_DIVERGENCE)
        .await
        .unwrap_or(0.0);
    anyhow::ensure!(divergence_before == 0.0, "diverged before injection");

    // Freeze the executor, then inject corrupt BALs for the validator's next
    // few blocks (within the backlog lookbehind; see the module docs).
    //
    // Use a poll, not a single read. An async snapshot poller sets the
    // committed-block gauge, and it can lag the first verified block by a
    // beat, because verification can run ahead of the durable commit.
    let committed = poll_until(
        "validator committed gauge",
        Duration::from_secs(10),
        Duration::from_millis(200),
        || async {
            Ok(t.validator_metric(super::VALIDATOR_COMMITTED_BLOCK)
                .await
                .ok()
                .filter(|v| *v > 0.0))
        },
    )
    .await? as u64;
    stack.suspend_executor();
    let targets: Vec<u64> = (committed + 1..=committed + 8).collect();
    inject::publish_corrupt_bal(&stack.aeron_dir(), targets)
        .await
        .context("publish corrupt BALs")?;

    // The fail-stop: exit code 2 and the halting log line.
    let code = stack
        .wait_validator_exit(Duration::from_secs(45))
        .context("validator did not exit after a corrupt BAL")?;
    anyhow::ensure!(
        code == Some(2),
        "validator exited with {code:?}, expected the divergence fail-stop's exit 2"
    );
    let log = stack.validator_log().unwrap_or_default();
    anyhow::ensure!(
        log.contains("divergence"),
        "validator log carries no divergence line; tail:\n{}",
        log.lines().rev().take(20).collect::<Vec<_>>().join("\n")
    );

    // Leave the stack coherent for Drop.
    stack.resume_executor();
    Ok(())
}

/// A forged epoch must halt the validator.
///
/// This is the counterpart to the divergence-detection test for the
/// deposit path. The derivation tests prove that an honest producer
/// builds a derivable chain. But the claim
/// "a sequencer cannot drop a deposit without producing a chain that
/// verifiers reject" is only true if verifiers actually reject it. This
/// drill injects an epoch that L1 never produced, and requires the
/// validator to notice.
///
/// This uses a bogus `l1_hash`, not a doctored deposit set. The canonical
/// id is `keccak(l1_hash)`, so cluster dedup cannot swallow the forgery,
/// and the validator's first check against L1 (does this block have this
/// hash) fails. This is the same class of fault as a dropped deposit, but
/// easier to stage on demand.
pub async fn forged_epoch_halts_validator(
    stack: &LocalStack,
    t: &Target,
    l1: &crate::harness::l1::L1,
) -> Result<()> {
    // Warm up. The halt must land on a validator that was verifying happily,
    // not on one that never started.
    poll_until(
        "validator verifying (warmup)",
        // An upper bound, not a pace: a warm run pays actual time. 20-30s
        // was not enough for a cold validator on a contended runner (#250).
        Duration::from_secs(60),
        Duration::from_millis(250),
        || async {
            let v = t
                .validator_metric(super::VALIDATOR_BLOCKS_VERIFIED)
                .await
                .unwrap_or(0.0);
            Ok((v > 0.0).then_some(()))
        },
    )
    .await?;
    anyhow::ensure!(
        t.validator_metric(super::VALIDATOR_DIVERGENCE)
            .await
            .unwrap_or(0.0)
            == 0.0,
        "diverged before injection"
    );

    // Freeze the honest producer, so its epoch for this L1 block cannot race
    // the forgery. Then forge one origin past where the chain has reached
    // (the sealer only accepts an advancing origin).
    anyhow::ensure!(
        stack.suspend_da_watcher(),
        "S11 needs a DA watcher (l1: true)"
    );
    let tip = l1.finalized_block_number().await?;
    crate::harness::inject::publish_forged_epoch(&stack.aeron_dir(), tip + 50).await?;

    // The verdict is deferred by one epoch on purpose, because the L1 read
    // runs off the exec thread. Keep honest epochs coming to carry it
    // through.
    l1.mine(12).await?;

    poll_until(
        "validator divergence on the forged epoch",
        Duration::from_secs(60),
        Duration::from_millis(500),
        || async {
            let d = t
                .validator_metric(super::VALIDATOR_DIVERGENCE)
                .await
                .unwrap_or(0.0);
            Ok((d > 0.0).then_some(()))
        },
    )
    .await
    .context("validator must reject an epoch L1 never produced")?;

    // It must also be recorded as an epoch fault specifically. A
    // divergence from some unrelated check would pass the line above,
    // while proving nothing about epoch verification.
    let faults = t
        .validator_metric(super::VALIDATOR_EPOCH_FAULTS)
        .await
        .unwrap_or(0.0);
    anyhow::ensure!(
        faults > 0.0,
        "validator diverged but recorded no epoch fault — halted for another reason"
    );
    Ok(())
}

/// The validator reads L1 through an interposed endpoint, and
/// rejects it when the endpoint lies.
///
/// Production points `--l1-rpc-url` at a light client, not a raw RPC. The
/// real light client needs a beacon chain, and this test's L1 is anvil, so
/// it cannot run here. This test covers the half that is ours: the
/// contract between the validator and whatever serves it L1 data.
///
/// `fault` selects the lie. `Fault::None` is the baseline: verification
/// must still succeed through an interposed endpoint. Otherwise the lying
/// cases would prove nothing about detection, only that something broke.
pub async fn verified_l1_endpoint(
    stack: &mut LocalStack,
    t: &Target,
    fault: crate::harness::l1_verified::Fault,
) -> Result<()> {
    use crate::harness::l1_verified::Fault;

    // Warm up. The verdict must land on a validator that was verifying
    // happily.
    poll_until(
        "validator verifying (warmup)",
        // An upper bound, not a pace: a warm run pays actual time. 20-30s
        // was not enough for a cold validator on a contended runner (#250).
        Duration::from_secs(60),
        Duration::from_millis(250),
        || async {
            let v = t
                .validator_metric(super::VALIDATOR_BLOCKS_VERIFIED)
                .await
                .unwrap_or(0.0);
            Ok((v > 0.0).then_some(()))
        },
    )
    .await?;
    anyhow::ensure!(
        t.validator_metric(super::VALIDATOR_DIVERGENCE)
            .await
            .unwrap_or(0.0)
            == 0.0,
        "diverged before the fault was armed"
    );

    // Non-vacuity check: the validator must actually read through the
    // mock. Without this check, the whole scenario could pass with the
    // endpoint bypassed.
    anyhow::ensure!(
        stack.verified_l1().context("mock verified L1")?.served() > 0,
        "validator never queried the interposed endpoint — it is not in the L1 path"
    );

    if fault == Fault::None {
        // Baseline: epochs keep verifying through the interposed endpoint.
        let verified_before = t
            .validator_metric(super::VALIDATOR_EPOCHS_VERIFIED)
            .await
            .unwrap_or(0.0);
        stack.l1().context("l1")?.mine(8).await?;
        poll_until(
            "epochs verifying through the interposed endpoint",
            Duration::from_secs(60),
            Duration::from_millis(500),
            || async {
                let v = t
                    .validator_metric(super::VALIDATOR_EPOCHS_VERIFIED)
                    .await
                    .unwrap_or(0.0);
                Ok((v > verified_before).then_some(()))
            },
        )
        .await
        .context("verification must still pass through a faithful endpoint")?;
        anyhow::ensure!(
            t.validator_metric(super::VALIDATOR_DIVERGENCE)
                .await
                .unwrap_or(0.0)
                == 0.0,
            "a FAITHFUL endpoint produced a divergence — the check is over-eager"
        );
        return Ok(());
    }

    // Arm the lie starting at the next L1 block, so already-verified
    // epochs stay verified, and the fault lands only on fresh ones.
    let from = stack.l1().context("l1")?.finalized_block_number().await? + 1;
    let served_at_arm = stack.verified_l1().context("mock verified L1")?.served();
    let verified_at_arm = t
        .validator_metric(super::VALIDATOR_EPOCHS_VERIFIED)
        .await
        .unwrap_or(0.0);
    stack
        .verified_l1()
        .context("mock verified L1")?
        .set_fault(match fault {
            Fault::WrongBlockHash { .. } => Fault::WrongBlockHash { from_block: from },
            Fault::BrokenParentChain { .. } => Fault::BrokenParentChain { from_block: from },
            other => other,
        });
    // SwallowLogs is only a lie if there is a log to swallow. Arm the
    // fault first, then make the deposit. The da-watcher reads anvil
    // directly, so it builds an epoch that carries the deposit, while the
    // validator's interposed view reports none. With the wrong order, the
    // epoch would be empty on both sides, and the case would pass while
    // testing nothing.
    if fault == Fault::SwallowLogs {
        let signers = l2::dev_signers(3)?;
        stack
            .l1()
            .context("l1")?
            .deposit_eth(
                signers[2].address,
                alloy_primitives::U256::from(2_000_000_000_000_000u64),
            )
            .await
            .context("stage a deposit for the swallowed-logs case")?;
    }
    stack.l1().context("l1")?.mine(16).await?;

    // Check the exit code, not a metric. The fail-stop kills the process,
    // so its /metrics endpoint goes with it. Polling a gauge here would
    // read the halt as `unwrap_or(0.0)`, meaning "no divergence", and the
    // scenario would time out while the validator was dead and correct the
    // whole time. This is the same scrape-failure-as-zero trap the
    // lag-resync work hit. Exit code 2 is the divergence fail-stop, and the
    // log line names the reason.
    let served_since_arm = stack
        .verified_l1()
        .context("mock verified L1")?
        .served()
        .saturating_sub(served_at_arm);
    let code = stack
        .wait_validator_exit(Duration::from_secs(90))
        .with_context(|| {
            format!(
                "validator did NOT fail-stop on a lying L1 view ({fault:?}) — armed from L1 \
                 block {from}; endpoint served {} requests since arming; epochs verified \
                 was {verified_at_arm} at arming",
                served_since_arm,
            )
        })?;
    anyhow::ensure!(
        code == Some(2),
        "validator exited with {code:?}, expected the divergence fail-stop's exit 2"
    );

    // It must also have halted on an epoch fault. Exiting with code 2 for
    // some unrelated divergence would pass the check above, while proving
    // nothing about L1 verification.
    let log = stack
        .validator_log()
        .context("read validator log for the halt reason")?;
    anyhow::ensure!(
        log.contains("epoch verification failed"),
        "validator fail-stopped but not on an epoch fault — halted for another reason"
    );
    Ok(())
}
