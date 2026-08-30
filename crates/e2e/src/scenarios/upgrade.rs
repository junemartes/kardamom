//! S13 — L1-governed upgrades: feature flags activated from L1.
//!
//! The full chain under test, end to end:
//!
//! ```text
//! factory owner (a Safe in production, impersonated DEV_OWNER here)
//!   → ETHLockbox.initiateUpgrade on L1
//!   → the da-watcher sees the finalized UpgradeInitiated log
//!   → derive_epoch mints a SYSTEM deposit (domain-1 source hash)
//!   → the sealer orders it like any other epoch record
//!   → the executor runs it: KardamomChainState.setFeature
//!   → every subsequent block close records a health beacon
//! ```
//!
//! Three scenarios: [`activates_immediately`] (activation 0 — the flag is live
//! from the block carrying the upgrade), [`activates_at_timestamp`] (scheduled
//! — nothing happens until the chain's own clock reaches T), and
//! [`authority_is_enforced`] (neither an L1 stranger nor an L2 user can reach
//! the flag store).
//!
//! **Assertions are exact, not "eventually".** The beacon count, the head
//! block and the activation word all come from ONE mdbx snapshot
//! ([`read_chain_state`]), and headers + storage commit in a single write
//! transaction, so `beats == head - first_active_block + 1` holds precisely.
//! A greater-than-zero check would pass just as well against a beacon that
//! fired once and stopped — which is the actual failure mode worth catching.

use std::path::Path;
use std::time::Duration;

use alloy_primitives::U256;
use anyhow::{Context, Result};

use super::{
    ChainStateView, Target, VALIDATOR_BLOCKS_VERIFIED, VALIDATOR_DIVERGENCE, assert_receipt_ok,
    await_l2_receipt, read_chain_state, receipt_field, receipt_placement,
};
use crate::harness::l1::{DEPOSITOR_KEY, L1};
use crate::harness::l2::{self, DerivedSigner};
use crate::harness::metrics::poll_until;

/// The health check — feature 1. Matches
/// `kardamom_exec_core::features::FEATURE_HEALTH_CHECK` and
/// `KardamomChainState.FEATURE_HEALTH_CHECK`.
const FEATURE_HEALTH_CHECK: u64 = 1;

/// A feature id that is never scheduled, used by the negative controls.
const FEATURE_UNUSED: u64 = 2;

/// How far ahead of "now" a scheduled activation is set. Blocks close every
/// 250 ms, so this leaves several blocks provably inactive before T while
/// keeping the scenario short.
const SCHEDULE_AHEAD_MS: u64 = 4_000;

pub struct Params {
    /// Dev-mnemonic index used for the L2-side negative control.
    pub intruder: usize,
}

impl Default for Params {
    fn default() -> Self {
        Self { intruder: 8 }
    }
}

/// Wait until `state_dir` has committed at least `block` and return the view.
async fn state_at_or_past(state_dir: &Path, block: u64, what: &str) -> Result<ChainStateView> {
    poll_until(
        &format!("{what} committed through block {block}"),
        Duration::from_secs(60),
        Duration::from_millis(200),
        || async {
            let v = read_chain_state(state_dir)?;
            Ok((v.block_number >= block).then_some(v))
        },
    )
    .await
}

/// Assert the beacon beat in EVERY block from `first_active` through the
/// snapshot's head, and that it carries that head block's own identity.
fn assert_beat_every_block(v: &ChainStateView, first_active: u64, role: &str) -> Result<()> {
    let expected = v
        .block_number
        .checked_sub(first_active)
        .map(|d| d + 1)
        .context("head is behind the first active block")?;
    let (beats, beacon_block, beacon_ts) = v.beacon;
    anyhow::ensure!(
        beats == expected,
        "{role}: beacon beat {beats} times through block {}, expected {expected} \
         (one per block from {first_active}) — a stalled count means the feature \
         stopped firing after activation",
        v.block_number
    );
    // The beacon's own fields pin the packing against real chain data: a
    // mis-shifted field would still count correctly but name the wrong block.
    anyhow::ensure!(
        beacon_block == v.block_number,
        "{role}: beacon names block {beacon_block}, head is {}",
        v.block_number
    );
    anyhow::ensure!(
        beacon_ts != 0,
        "{role}: beacon carries no timestamp: {:?}",
        v.beacon
    );
    Ok(())
}

/// The validator independently reached the same conclusion.
///
/// This is the real proof of activation parity: the validator re-executes every
/// block and compares its own write set against the executor's published one,
/// so identical beacons on both sides mean both roles activated the feature at
/// the same block. A divergence here is a fail-stop, not a flaky assert.
async fn assert_validator_agrees(
    t: &Target,
    validator_state_dir: &Path,
    first_active: u64,
    through_block: u64,
) -> Result<()> {
    let v = state_at_or_past(validator_state_dir, through_block, "validator").await?;
    assert_beat_every_block(&v, first_active, "validator")?;

    poll_until(
        "validator verified past the activation block",
        Duration::from_secs(60),
        Duration::from_millis(250),
        || async {
            let verified = t
                .validator_metric(VALIDATOR_BLOCKS_VERIFIED)
                .await
                .unwrap_or(0.0);
            Ok((verified > 0.0).then_some(verified))
        },
    )
    .await
    .context("validator verifying blocks")?;

    let divergence = t
        .validator_metric(VALIDATOR_DIVERGENCE)
        .await
        .unwrap_or(0.0);
    anyhow::ensure!(
        divergence == 0.0,
        "validator reported {divergence} divergence(s) — executor and validator \
         disagree about the upgrade"
    );
    Ok(())
}

/// Send an upgrade transaction and wait for its system deposit to execute on
/// L2. Returns the block the `setFeature` landed in.
async fn upgrade_and_await(
    t: &Target,
    l1: &L1,
    feature_id: u64,
    activation_timestamp: u64,
) -> Result<u64> {
    let nonce_before = l1.upgrade_nonce().await?;
    let (block_hash, log_index) = l1
        .initiate_upgrade(U256::from(feature_id), activation_timestamp)
        .await
        .context("initiateUpgrade")?;
    anyhow::ensure!(
        l1.upgrade_nonce().await? == nonce_before + 1,
        "the upgrade transaction did not advance the L1 nonce"
    );

    // The watcher only reads FINALIZED logs; --slots-in-an-epoch 1 means a few
    // blocks carry the cursor past the upgrade.
    l1.mine(6).await?;

    // The system deposit is keyed by the DOMAIN-1 source hash. Using the
    // user-deposit hash here would poll forever — that difference is the
    // domain separation doing its job.
    let source_hash = kardamom_da_watcher::source_hash_system(block_hash, log_index);
    let receipt = await_l2_receipt(t, source_hash, "the upgrade transaction").await?;
    assert_receipt_ok(&receipt, "the upgrade transaction")?;
    anyhow::ensure!(
        receipt_field(&receipt, "effectiveGasPrice") == Some("0x0"),
        "a system deposit must execute at gas price 0: {receipt}"
    );

    let (block, _) = receipt_placement(&receipt)?;
    Ok(block)
}

// ---------------------------------------------------------------------------
// S13a — immediate activation
// ---------------------------------------------------------------------------

/// The requested full-flow exercise: multisig-authorized L1 transaction turns
/// a protocol feature on, and it stays on for every subsequent block.
pub async fn activates_immediately(
    t: &Target,
    l1: &L1,
    executor_state_dir: &Path,
    validator_state_dir: &Path,
) -> Result<()> {
    // --- Pre: the feature is dormant and has never fired. ----------------
    let before = read_chain_state(executor_state_dir)?;
    anyhow::ensure!(
        before.activation.is_zero() && before.beats() == 0,
        "the feature must start unscheduled and unfired: {before:?}"
    );

    // --- L1 → L2: schedule with activation 0 (immediately). --------------
    let activation_block = upgrade_and_await(t, l1, FEATURE_HEALTH_CHECK, 0).await?;

    // --- Let a few more blocks close, then take ONE consistent read. -----
    let v = state_at_or_past(executor_state_dir, activation_block + 3, "executor").await?;
    anyhow::ensure!(
        !v.activation.is_zero(),
        "setFeature did not write an activation timestamp: {v:?}"
    );

    // An immediate upgrade resolves to the executing block's timestamp, which
    // is strictly below its own boundary's stamp — so the upgrade's OWN block
    // is the first beating block.
    assert_beat_every_block(&v, activation_block, "executor")?;

    assert_validator_agrees(t, validator_state_dir, activation_block, v.block_number).await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// S13b — scheduled activation
// ---------------------------------------------------------------------------

/// A scheduled upgrade must do nothing until the chain's own clock reaches the
/// activation time, then fire from the first block at or after it.
pub async fn activates_at_timestamp(
    t: &Target,
    l1: &L1,
    executor_state_dir: &Path,
    validator_state_dir: &Path,
) -> Result<()> {
    let before = read_chain_state(executor_state_dir)?;
    anyhow::ensure!(
        before.activation.is_zero() && before.beats() == 0,
        "the feature must start unscheduled and unfired: {before:?}"
    );

    // The chain's clock is the sealer's leader clock in MILLISECONDS, so the
    // schedule is anchored to the chain's own notion of now — reading it off
    // the head block rather than from wall-clock keeps the two in the same
    // frame even if the host clock and the sealer's disagree.
    // Wait rather than read once: the stack's launch barrier guarantees
    // block 1 exists, but a restarted or slow executor can still race this
    // read (issue #250 — the one-shot read failed with "chain has produced
    // no blocks" three times in one CI day).
    let head_ts = crate::harness::metrics::poll_until(
        "a committed head block to anchor the schedule",
        std::time::Duration::from_secs(30),
        std::time::Duration::from_millis(200),
        || async {
            Ok(super::derivation::read_block_origins(executor_state_dir)?
                .last()
                .map(|b| b.l2_timestamp))
        },
    )
    .await?;
    let activation_ts = head_ts + SCHEDULE_AHEAD_MS;

    let scheduled_block = upgrade_and_await(t, l1, FEATURE_HEALTH_CHECK, activation_ts).await?;

    // --- Scheduled is not active. ----------------------------------------
    // Wait for the scheduling block to COMMIT before reading state. A receipt
    // is published when its transaction executes, but the block reaches mdbx
    // only at the next sealer boundary — reading straight off the receipt sees
    // `activation == 0` and reads as a product failure. (This is the same trap
    // `derivation::await_block_origins_through` documents; it survived a
    // single-test run and only surfaced under the full suite's timing.)
    let pending = state_at_or_past(executor_state_dir, scheduled_block, "executor").await?;
    anyhow::ensure!(
        pending.activation == U256::from(activation_ts),
        "activation stored as {:?}, sent {activation_ts} — note the unit is \
         MILLISECONDS",
        pending.activation
    );

    // "Scheduled is not yet active" is only assertable while no committed
    // block has reached T. Headers are read AFTER the state, so they cover at
    // least the blocks that state view saw — the comparison is sound even
    // though the two reads are separate transactions.
    let so_far = super::derivation::read_block_origins(executor_state_dir)?;
    let reached_t = so_far
        .iter()
        .any(|b| b.block_number <= pending.block_number && b.l2_timestamp >= activation_ts);
    anyhow::ensure!(
        reached_t || pending.beats() == 0,
        "the feature beat before its activation time: {pending:?}"
    );

    // --- Wait past T, then find the first block whose header reached it. --
    let v = poll_until(
        "a block closes at or after the activation time",
        Duration::from_secs(60),
        Duration::from_millis(200),
        || async {
            let v = read_chain_state(executor_state_dir)?;
            Ok((v.beats() > 0).then_some(v))
        },
    )
    .await?;

    let headers = super::derivation::read_block_origins(executor_state_dir)?;
    let first_active = headers
        .iter()
        .find(|b| b.l2_timestamp >= activation_ts)
        .map(|b| b.block_number)
        .context("no block reached the activation timestamp")?;
    anyhow::ensure!(
        first_active > scheduled_block,
        "a FUTURE activation must not fire in the block that scheduled it \
         (first active {first_active}, scheduled in {scheduled_block})"
    );
    // The predecessor must be strictly before T — otherwise `first_active` is
    // not actually the first.
    if let Some(prev) = headers.iter().find(|b| b.block_number == first_active - 1) {
        anyhow::ensure!(
            prev.l2_timestamp < activation_ts,
            "block {} already reached the activation time; {first_active} is not the first",
            prev.block_number
        );
    }

    // Re-read so the head and the beacon are consistent with each other.
    let v = state_at_or_past(executor_state_dir, v.block_number, "executor").await?;
    assert_beat_every_block(&v, first_active, "executor")?;

    assert_validator_agrees(t, validator_state_dir, first_active, v.block_number).await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// S13c — authority
// ---------------------------------------------------------------------------

/// Neither side of the flag store is reachable without authority: not the L1
/// entry point (any address but the factory owner), and not the L2 predeploy
/// (any sender but the derivation pipeline's system address).
pub async fn authority_is_enforced(
    t: &Target,
    l1: &L1,
    executor_state_dir: &Path,
    p: Params,
) -> Result<()> {
    // --- L1: a stranger cannot emit an upgrade. --------------------------
    let nonce_before = l1.upgrade_nonce().await?;
    let breach = l1
        .try_initiate_upgrade_unauthorized(DEPOSITOR_KEY, U256::from(FEATURE_UNUSED))
        .await;
    anyhow::ensure!(
        breach.is_err(),
        "an unauthorized L1 account successfully sent an upgrade transaction"
    );
    anyhow::ensure!(
        l1.upgrade_nonce().await? == nonce_before,
        "a rejected upgrade must not consume a nonce"
    );

    // --- L2: a user transaction cannot write the flag store. -------------
    // setFeature(FEATURE_UNUSED, 0) sent as an ordinary L2 transaction. It
    // reaches the predeploy and reverts on the sender check, which is exactly
    // the defence-in-depth the contract exists to provide.
    let signers: Vec<DerivedSigner> = l2::dev_signers(p.intruder as u32 + 1)?;
    let intruder = &signers[p.intruder];
    let calldata = kardamom_types::upgrades::encode_set_feature(U256::from(FEATURE_UNUSED), 0);
    let call = l2::sign_call(
        intruder,
        t.chain_id,
        0,
        kardamom_types::upgrades::CHAIN_STATE,
        U256::ZERO,
        calldata.as_ref(),
    )?;
    let hash = t
        .rpc
        .send_raw(&call.raw)
        .await
        .result
        .map_err(|e| anyhow::anyhow!("submitting the intruder call failed: {e}"))?;
    let receipt = await_l2_receipt(t, hash, "the intruder setFeature call").await?;
    anyhow::ensure!(
        receipt_field(&receipt, "status") == Some("0x0"),
        "a user transaction was allowed to write the feature-flag store: {receipt}"
    );

    // The chain state is untouched: no activation, and — since the health
    // check was never scheduled in this scenario — no beacon either.
    let v = read_chain_state(executor_state_dir)?;
    anyhow::ensure!(
        v.activation.is_zero() && v.beats() == 0,
        "the flag store changed despite both authority checks rejecting: {v:?}"
    );
    Ok(())
}
