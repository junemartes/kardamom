//! The account layer scenarios (`s17`): the ingress's local account
//! layer, fed by the account rows on the `tx_receipts` batch frame, with
//! Redis off. See `docs/specs/2026-09-13-redis-account-cache-design.md`,
//! section 9.2.
//!
//! Every scenario has a discriminator beyond "the submit landed": with
//! Redis off, an unknown, an expired, and a known-good sender all admit.
//! The ingress's `kardamom_cache_lookups_total` counter tells a served
//! read from an admit-by-default, and the executor's own query tells a
//! correct count from a plausible one.
//!
//! - s17a: after `established` transfers, the ingress serves the sender's
//!   count from the layer (a live hit), the executor agrees once the block
//!   commits, a retry of the first transfer answers its hash, and a
//!   different transaction at a landed nonce is invalid params.
//! - s17c: a restarted ingress admits the sender's next nonce on a local
//!   miss, and the miss is counted.
//! - s17e: the validator verifies the rows: its verified-rows counter
//!   rises under traffic. The negative case, a forged row that halts, is
//!   `kardamom_validator::seams` unit coverage: the receipt cross-check
//!   runs first, so an injected frame cannot reach the row check with a
//!   byte-identical receipt.
//! - s17g: A pays a fresh B. On A's receipt the ingress serves B's balance
//!   from the layer, and B spends it at once, before any block boundary.

use std::time::Duration;

use alloy_primitives::{Address, U256};
use alloy_signer_local::PrivateKeySigner;
use anyhow::{Context, Result};

use super::{CODE_INVALID, Target};
use crate::harness::l2::{self, DerivedSigner, RpcError, SignedTransfer};
use crate::harness::metrics::poll_until;

/// The ingress's account-layer lookup counter.
pub const CACHE_LOOKUPS: &str = "kardamom_cache_lookups_total";
/// The validator's verified-rows counter.
pub const VALIDATOR_ROWS_VERIFIED: &str = "validator_rows_verified_total";
/// With Redis off, every hit and every miss is the local layer's.
const HIT: &str = "outcome=\"hit\"";
const MISS: &str = "outcome=\"miss\"";

pub struct Params {
    /// The dev-mnemonic index of the established sender. The payer of
    /// s17g is the next index.
    pub sender: usize,
    /// The number of transfers the sender lands to become established.
    pub established: u64,
}

impl Default for Params {
    fn default() -> Self {
        Self {
            sender: 20,
            established: 3,
        }
    }
}

/// The recipient every transfer of these scenarios pays.
const TO: Address = Address::new([0x59u8; 20]);
/// What A pays B in s17g: enough for B's one transfer at 21,000 gas
/// and 1 gwei, with change.
const PAYMENT_WEI: u64 = 1_000_000_000_000_000;

impl Params {
    fn signer(&self) -> Result<DerivedSigner> {
        let signers = l2::dev_signers_through(self.sender)?;
        Ok(signers[self.sender].clone())
    }

    fn payer(&self) -> Result<DerivedSigner> {
        let index = self.sender.checked_add(1).context("payer index")?;
        let signers = l2::dev_signers_through(index)?;
        Ok(signers[index].clone())
    }
}

/// The hex quantity an account RPC returns, as a number.
fn quantity(hex: &str) -> Result<U256> {
    let digits = hex
        .strip_prefix("0x")
        .with_context(|| format!("not a hex quantity: {hex}"))?;
    U256::from_str_radix(digits, 16).with_context(|| format!("bad hex quantity: {hex}"))
}

/// The ingress's lookup count with `label`, zero before the first.
async fn lookups(t: &Target, label: &str) -> Result<f64> {
    Ok(t.ingress_metric_where(CACHE_LOOKUPS, label)
        .await?
        .unwrap_or(0.0))
}

/// Land `established` transfers from the sender. Returns the first, for
/// the retry check.
///
/// # Errors
/// Returns an error when a transfer fails to send.
pub async fn establish(t: &Target, p: &Params) -> Result<SignedTransfer> {
    let signer = p.signer()?;
    let mut first = None;
    for n in 0..p.established {
        let tx = l2::sign_transfer(&signer, t.chain_id, n, TO, 1)?;
        t.rpc
            .send_raw(&tx.raw)
            .await
            .result
            .map_err(|e| anyhow::anyhow!("established nonce {n}: {e}"))?;
        first.get_or_insert(tx);
    }
    first.context("no transfer landed: established is zero")
}

/// s17a. See the module doc.
///
/// # Errors
/// Returns an error when a step fails or a check does not hold.
pub async fn nonce_matches_executor(t: &Target, p: &Params) -> Result<()> {
    let signer = p.signer()?;
    let hits0 = lookups(t, HIT).await?;
    let first = establish(t, p).await?;

    // The ingress serves the count from the layer.
    let count = t
        .rpc
        .get_transaction_count(signer.address)
        .await
        .result
        .map_err(|e| anyhow::anyhow!("ingress eth_getTransactionCount: {e}"))?;
    anyhow::ensure!(
        quantity(&count)? == U256::from(p.established),
        "ingress count {count} != {} after the transfers landed",
        p.established
    );
    let hits1 = lookups(t, HIT).await?;
    anyhow::ensure!(
        hits1 > hits0,
        "the count was not a live-layer hit (hits {hits0} -> {hits1})"
    );

    // The executor agrees once the block commits.
    let executor = l2::L2Client::new(
        &format!("http://{}", t.executor_query),
        Duration::from_secs(5),
    )?;
    poll_until(
        "the executor's committed count matches",
        Duration::from_secs(30),
        Duration::from_millis(250),
        || async {
            let out = executor.get_transaction_count(signer.address).await;
            let count = out.result.ok().and_then(|c| quantity(&c).ok());
            Ok((count == Some(U256::from(p.established))).then_some(()))
        },
    )
    .await?;

    // The S5 retry contract: the landed transfer answers its hash.
    let retry = t.rpc.send_raw(&first.raw).await;
    let hash = retry
        .result
        .map_err(|e| anyhow::anyhow!("retry of a landed transfer: {e}"))?;
    anyhow::ensure!(
        hash == first.hash,
        "retry answered {hash} != {}",
        first.hash
    );

    // A different transaction at a landed nonce is invalid params.
    let other = l2::sign_transfer(&signer, t.chain_id, 0, TO, 2)?;
    match t.rpc.send_raw(&other.raw).await.result {
        Err(RpcError::Call { code, .. }) if code == CODE_INVALID => Ok(()),
        other => anyhow::bail!("a different tx at a landed nonce: expected -32602, got {other:?}"),
    }
}

/// s17c: the restarted ingress admits the next nonce on a local miss.
///
/// # Errors
/// Returns an error when the submit fails, or when no miss was counted.
pub async fn cold_ingress_admits(t: &Target, p: &Params) -> Result<()> {
    let signer = p.signer()?;
    let misses0 = lookups(t, MISS).await?;
    let tx = l2::sign_transfer(&signer, t.chain_id, p.established, TO, 1)?;
    t.rpc
        .send_raw(&tx.raw)
        .await
        .result
        .map_err(|e| anyhow::anyhow!("nonce {} on the cold ingress: {e}", p.established))?;
    let misses1 = lookups(t, MISS).await?;
    anyhow::ensure!(
        misses1 > misses0,
        "the cold ingress counted no local miss (misses {misses0} -> {misses1})"
    );
    Ok(())
}

/// s17e: the validator's verified-rows counter rises under traffic.
///
/// # Errors
/// Returns an error when the traffic fails to send, or when the counter
/// stays at zero.
pub async fn rows_verified(t: &Target, p: &Params) -> Result<()> {
    establish(t, p).await?;
    t.wait_validator_metric_above(
        VALIDATOR_ROWS_VERIFIED,
        0.0,
        Duration::from_mins(1),
        Duration::from_millis(250),
        "the validator verified the batch rows",
    )
    .await
    .map(|_| ())
}

/// s17g: A pays a fresh B, and B spends it within the batch.
///
/// # Errors
/// Returns an error when a transfer fails, when the ingress does not
/// serve B's balance from the layer, or when B's spend does not land.
pub async fn recipient_spends_within_batch(t: &Target, p: &Params) -> Result<()> {
    let a = p.payer()?;
    let b_key = PrivateKeySigner::random();
    let b = DerivedSigner {
        address: b_key.address(),
        signer: b_key,
    };

    let hits0 = lookups(t, HIT).await?;
    let pay = l2::sign_transfer(&a, t.chain_id, 0, b.address, PAYMENT_WEI)?;
    t.rpc
        .send_raw(&pay.raw)
        .await
        .result
        .map_err(|e| anyhow::anyhow!("A pays B: {e}"))?;

    // The rows applied before A's receipt released the submit: B's
    // balance is served at once, from the layer.
    let balance = t
        .rpc
        .get_balance(b.address)
        .await
        .result
        .map_err(|e| anyhow::anyhow!("eth_getBalance(B): {e}"))?;
    anyhow::ensure!(
        quantity(&balance)? == U256::from(PAYMENT_WEI),
        "B's balance {balance} != the payment right after A's receipt"
    );
    let hits1 = lookups(t, HIT).await?;
    anyhow::ensure!(
        hits1 > hits0,
        "B's balance was not a live-layer hit (hits {hits0} -> {hits1})"
    );

    // B spends at once: the balance check passes on the fresh entry.
    let spend = l2::sign_transfer(&b, t.chain_id, 0, TO, 1)?;
    let out = t.rpc.send_raw(&spend.raw).await;
    let hash = out
        .result
        .map_err(|e| anyhow::anyhow!("B spends within the batch: {e}"))?;
    anyhow::ensure!(
        hash == spend.hash,
        "B's spend answered {hash} != {}",
        spend.hash
    );
    Ok(())
}
