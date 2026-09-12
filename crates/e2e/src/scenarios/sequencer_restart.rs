//! `restarted_sequencer_regains_an_established_sender`.
//!
//! The F02.1 contract, checked end to end. A sequencer that restarts is
//! cold: it holds no nonce state, and it seeds every sender at 0. Before
//! the nonce lookup, it regained an established sender only when a receipt
//! for that sender arrived, which needs the twin to publish. With the
//! lookup (docs/specs/dynamic-sequencer-sizing.md, section 3.4), the first
//! park asks an executor for the committed nonce, and the parked
//! transaction drains.
//!
//! The stack runs one replica per shard, so a stopped twin is the normal
//! shape here. The scenario is two phases around the restart, which the
//! test drives:
//!
//! 1. Before: the sender lands nonces 0..=2.
//! 2. After: the sender submits nonce 3 to the cold replica. It lands
//!    within the park bound, and the replica's lookup counter rose.

use std::time::Duration;

use alloy_primitives::Address;
use anyhow::{Context, Result};

use super::{Target, count_as_f64};
use crate::harness::l2;
use crate::harness::metrics::poll_until;

pub struct Params {
    /// The dev-mnemonic index of the sender.
    pub sender: usize,
    /// The number of transactions the sender lands before the restart.
    pub established: u64,
}

impl Default for Params {
    fn default() -> Self {
        Self {
            sender: 12,
            established: 3,
        }
    }
}

/// The recipient every transfer of this scenario pays.
const TO: Address = Address::new([0x57u8; 20]);

impl Params {
    /// The scenario's sender.
    fn signer(&self) -> Result<l2::DerivedSigner> {
        let signers = l2::dev_signers_through(self.sender)?;
        Ok(signers[self.sender].clone())
    }
}

/// The sender the scenario uses, for the test to pick the sequencer.
///
/// # Errors
/// Returns an error when the dev signers cannot be derived.
pub fn sender_address(p: &Params) -> Result<Address> {
    Ok(p.signer()?.address)
}

/// Phase 1: land `established` transactions. Returns the executor's
/// applied count after them.
///
/// # Errors
/// Returns an error when a transaction fails to send, or when the
/// executor does not apply them all in time.
pub async fn phase_before_restart(t: &Target, p: &Params) -> Result<f64> {
    let sender = p.signer()?;
    let applied_start = t
        .executor_metric_opt(super::EXEC_TX_APPLIED)
        .await?
        .unwrap_or(0.0);
    for n in 0..p.established {
        send_established(t, &sender, n).await?;
    }
    let applied = applied_start + count_as_f64(p.established);
    t.wait_executor_applied(applied, Duration::from_secs(15))
        .await
        .context("established transactions applied")?;
    Ok(applied)
}

/// One transaction of [`phase_before_restart`]'s loop.
async fn send_established(t: &Target, sender: &l2::DerivedSigner, n: u64) -> Result<()> {
    let tx = l2::sign_transfer(sender, t.chain_id, n, TO, 1)?;
    let out = t.rpc.send_raw(&tx.raw).await;
    out.result
        .map_err(|e| anyhow::anyhow!("established nonce {n} failed: {e}"))?;
    Ok(())
}

/// Phase 2: the next nonce lands on the cold replica through the lookup.
///
/// # Errors
/// Returns an error when the transaction fails, takes the whole park
/// bound, is not applied, or when no lookup ran.
pub async fn phase_after_restart(t: &Target, p: &Params, applied_before: f64) -> Result<()> {
    let sender = p.signer()?;
    let lookups_start = t.sequencer_metric_sum(super::SEQ_NONCE_LOOKUPS).await?;

    let tx = l2::sign_transfer(&sender, t.chain_id, p.established, TO, 1)?;
    let out = t.rpc.send_raw(&tx.raw).await;
    let hash = out.result.map_err(|e| {
        anyhow::anyhow!(
            "nonce {} on the restarted replica failed after {:?}: {e}",
            p.established,
            out.elapsed
        )
    })?;
    anyhow::ensure!(hash == tx.hash, "submit returned {hash} != {}", tx.hash);
    anyhow::ensure!(
        out.elapsed < t.pending_receipt_timeout,
        "nonce {} took {:?}, at or past the park bound {:?} — the replica did not \
         regain the sender through the lookup",
        p.established,
        out.elapsed,
        t.pending_receipt_timeout
    );
    t.wait_executor_applied(applied_before + 1.0, Duration::from_secs(15))
        .await
        .context("the post-restart transaction applied")?;

    // The replica asked an executor. The counter sums every outcome, so a
    // rise proves a query ran; the landed transaction proves it answered.
    poll_until(
        "nonce lookup counter rose on the restarted replica",
        Duration::from_secs(5),
        Duration::from_millis(200),
        || async {
            let n = t.sequencer_metric_sum(super::SEQ_NONCE_LOOKUPS).await?;
            Ok((n > lookups_start).then_some(()))
        },
    )
    .await?;
    Ok(())
}
