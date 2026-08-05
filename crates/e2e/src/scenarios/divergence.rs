//! S7 — `divergence_detection_is_not_vacuous`.
//!
//! S6 asserting `validator_divergence_total == 0` only means something if a
//! genuinely divergent stream would have tripped it. This scenario proves
//! the tripwire: feed the validator a corrupt BAL over the real `tx_bal`
//! channel and require the documented fail-stop — the halting log line and
//! `std::process::exit(2)`.
//!
//! Determinism: the executor is SIGSTOPped first, so no genuine BAL competes
//! with the injected frame for the target blocks (the sealer keeps stamping
//! boundaries; the validator keeps re-executing and asking for BALs). The
//! injected blocks stay within the validator's backlog lookbehind so
//! catch-up mode cannot skip them as unverified.
//!
//! Target-L only by design: it drives process signals and raw Aeron
//! publications, which the Target-C runner cannot reach from outside the
//! cluster. (Closes the `docs/failure-modes.md` "divergence injection" gap.)

use std::time::Duration;

use alloy_primitives::Address;
use anyhow::{Context, Result};

use super::Target;
use crate::harness::metrics::poll_until;
use crate::harness::{LocalStack, inject, l2};

pub async fn corrupt_bal_halts_validator(stack: &mut LocalStack, t: &Target) -> Result<()> {
    // A little genuine traffic first, so the halt provably happens on a
    // validator that was verifying happily until the corruption.
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
        Duration::from_secs(20),
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

    // Freeze the executor; inject corrupt BALs for the validator's next few
    // blocks (within the backlog lookbehind — see the module docs).
    //
    // Polled, not single-shot: the committed-block gauge is set by an async
    // snapshot poller and can lag the FIRST verified block by a beat since
    // the #129 pipeline let verification run ahead of the durable commit.
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

/// S11 — a forged epoch must halt the validator.
///
/// The counterpart to S7 for the deposit path: S10a-e prove an HONEST producer
/// builds a derivable chain, but "a sequencer cannot drop a deposit without
/// producing a chain verifiers reject" is only true if someone actually
/// rejects. This drill injects an epoch L1 never produced and requires the
/// validator to notice.
///
/// Uses a bogus `l1_hash` rather than a doctored deposit set: the canonical id
/// is `keccak(l1_hash)`, so the forgery cannot be swallowed by cluster dedup,
/// and the validator's very first check against L1 — does this block have this
/// hash — fails. Same class of fault as a dropped deposit, deterministic to
/// stage.
pub async fn forged_epoch_halts_validator(
    stack: &LocalStack,
    t: &Target,
    l1: &crate::harness::l1::L1,
) -> Result<()> {
    // Warm up: the halt must provably land on a validator that was verifying
    // happily, not on one that never started.
    poll_until(
        "validator verifying (warmup)",
        Duration::from_secs(30),
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

    // Freeze the honest producer so its epoch for this L1 block cannot race
    // the forgery, then forge one origin PAST where the chain has got to
    // (the sealer only accepts an advancing origin).
    anyhow::ensure!(
        stack.suspend_da_watcher(),
        "S11 needs a DA watcher (l1: true)"
    );
    let tip = l1.finalized_block_number().await?;
    crate::harness::inject::publish_forged_epoch(&stack.aeron_dir(), tip + 50).await?;

    // The verdict is deferred by one epoch on purpose (the L1 read runs off
    // the exec thread), so keep honest epochs coming to carry it through.
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

    // And it must be recorded as an epoch fault specifically — a divergence
    // from some unrelated check would pass the line above while proving
    // nothing about epoch verification.
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
