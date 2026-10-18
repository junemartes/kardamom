//! S13d — the sealer's decision rules switch by an upgrade transaction.
//!
//! The sealer starts on decision version 2: it drops a nonce release, and
//! it sends no rules frame. The upgrade transaction of
//! [`FEATURE_SEALER_V3`] reaches the sealer as a feature activation that
//! the sequencer sends in front of the epoch. From that record on, the
//! sealer runs version 3: it tells each session, and it applies a release.
//!
//! One probe runs before the upgrade and one after it, each on a fresh
//! sender with a small balance:
//!
//! 1. The sender sends transfers at nonces 0 and 1. Each one fits the
//!    balance, and the two together do not, so nonce 1 skips with
//!    insufficient funds. A skip uses no nonce: the chain expects nonce 1
//!    next, and the sealer's guard expects nonce 2.
//! 2. The sender sends a new transfer at nonce 1 until it lands.
//!
//! Before the upgrade, no release reaches the sealer, so the new transfer
//! does not land. After it, the release of the skip brings the guard back
//! to nonce 1, and the new transfer lands.

use std::time::Duration;

use alloy_primitives::{Address, B256};
use alloy_signer_local::PrivateKeySigner;
use anyhow::{Context, Result};
use kardamom_types::upgrades::FEATURE_SEALER_V3;

use super::upgrade::upgrade_and_await;
use super::{Target, assert_receipt_ok, await_l2_receipt, receipt_field};
use crate::harness::l1::L1;
use crate::harness::l2::{self, DerivedSigner, SignedTransfer};
use crate::harness::metrics::poll_until;

/// The balance of a probe sender: one spend fits it, two do not.
const FUNDING_WEI: u64 = 100_000_000_000_000;
/// The value of each of the two spends. With 21 000 gas at 1 gwei, one
/// spend costs 71 000 gwei of the 100 000.
const SPEND_WEI: u64 = 50_000_000_000_000;
/// The recipient of every probe transfer.
const PAYEE: Address = Address::repeat_byte(0x5E);
/// How long the probe before the upgrade tries to land the new transfer.
const REFUSED_FOR: Duration = Duration::from_secs(10);
/// How long the probe after the upgrade may take to land the new transfer.
const LANDS_WITHIN: Duration = Duration::from_mins(1);

const SEALER_VERSION: &str = "kardamom_sequencer_sealer_version";
const NONCE_RELEASES: &str = "kardamom_sequencer_nonce_releases_total";

/// The default funder is account 0: the upgrade genesis funds only it.
#[derive(Default)]
pub struct Params {
    /// The dev-mnemonic index that funds the probe senders.
    pub funder: usize,
}

/// A fresh sender that skips nonce 1, then sends a new transfer at it.
struct ReleaseProbe {
    sender: DerivedSigner,
    retry: SignedTransfer,
}

impl ReleaseProbe {
    /// A probe on the fixed key `seed`, funded by `funder`.
    async fn funded(t: &Target, funder: &DerivedSigner, seed: u8) -> Result<Self> {
        let key = PrivateKeySigner::from_bytes(&B256::repeat_byte(seed)).context("probe key")?;
        let sender = DerivedSigner {
            address: key.address(),
            signer: key,
        };
        let nonce = Self::chain_nonce(t, funder.address).await?;
        let funding = l2::sign_transfer(funder, t.chain_id, nonce, sender.address, FUNDING_WEI)?;
        Self::lands(t, &funding, "the funding of a probe").await?;
        let retry = l2::sign_transfer(&sender, t.chain_id, 1, PAYEE, 1)?;
        Ok(Self { sender, retry })
    }

    /// The nonce that the chain expects next from `who`.
    async fn chain_nonce(t: &Target, who: Address) -> Result<u64> {
        let hex = t
            .rpc
            .get_transaction_count(who)
            .await
            .result
            .map_err(|e| anyhow::anyhow!("eth_getTransactionCount: {e}"))?;
        u64::from_str_radix(hex.trim_start_matches("0x"), 16).context("parse the nonce")
    }

    /// Submit `tx` and require a successful receipt.
    async fn lands(t: &Target, tx: &SignedTransfer, what: &str) -> Result<()> {
        t.rpc
            .send_raw(&tx.raw)
            .await
            .result
            .map_err(|e| anyhow::anyhow!("{what}: {e}"))?;
        assert_receipt_ok(&await_l2_receipt(t, tx.hash, what).await?, what)
    }

    /// Send nonces 0 and 1, and wait for the skip of nonce 1. Nonce 1 goes
    /// first, so the ingress checks it against the whole balance, before
    /// nonce 0 executes. The sequencer holds it until nonce 0 arrives.
    async fn skip_nonce_one(&self, t: &Target) -> Result<()> {
        let first = l2::sign_transfer(&self.sender, t.chain_id, 0, PAYEE, SPEND_WEI)?;
        let second = l2::sign_transfer(&self.sender, t.chain_id, 1, PAYEE, SPEND_WEI)?;
        let (late, early) = tokio::join!(t.rpc.send_raw(&second.raw), async {
            tokio::time::sleep(Duration::from_millis(100)).await;
            Self::lands(t, &first, "the spend at nonce 0").await
        });
        early?;
        late.result
            .map_err(|e| anyhow::anyhow!("the spend at nonce 1: {e}"))?;
        // A skip receipt: status 0, and no gas used.
        let skip = await_l2_receipt(t, second.hash, "the spend at nonce 1").await?;
        anyhow::ensure!(
            receipt_field(&skip, "status") == Some("0x0")
                && receipt_field(&skip, "gasUsed") == Some("0x0"),
            "the spend at nonce 1 must skip with insufficient funds: {skip}"
        );
        anyhow::ensure!(
            Self::chain_nonce(t, self.sender.address).await? == 1,
            "a skip uses no nonce"
        );
        Ok(())
    }

    /// One try of the new transfer at nonce 1: `Some` when it has a
    /// receipt. The sealer refuses the transfer while its guard is ahead
    /// of the chain, so the client sends it again.
    async fn try_retry(&self, t: &Target) -> Result<Option<()>> {
        // The answer does not matter: a refusal and an accepted offer both
        // lead to the next look at the receipt.
        let _answer = t.rpc.send_raw(&self.retry.raw).await;
        Ok(t.rpc
            .receipt(self.retry.hash)
            .await
            .result
            .ok()
            .flatten()
            .map(|_| ()))
    }

    /// The new transfer does not land within `within`.
    async fn stays_refused(&self, t: &Target, within: Duration) -> Result<()> {
        let landed = poll_until(
            "the new transfer at nonce 1 (it must not land)",
            within,
            Duration::from_millis(500),
            || self.try_retry(t),
        )
        .await;
        anyhow::ensure!(
            landed.is_err()
                && t.rpc
                    .receipt(self.retry.hash)
                    .await
                    .result
                    .ok()
                    .flatten()
                    .is_none(),
            "a transfer at the skipped nonce landed under decision version 2"
        );
        Ok(())
    }

    /// The new transfer lands within `within`, with a successful receipt.
    async fn lands_after_release(&self, t: &Target, within: Duration) -> Result<()> {
        poll_until(
            "the new transfer at nonce 1 lands",
            within,
            Duration::from_millis(500),
            || self.try_retry(t),
        )
        .await?;
        let receipt = await_l2_receipt(t, self.retry.hash, "the new transfer at nonce 1").await?;
        assert_receipt_ok(&receipt, "the new transfer at nonce 1")
    }
}

/// The nonce releases that the sequencers sent.
async fn releases_sent(t: &Target) -> Result<f64> {
    t.sequencer_metric_sum_where(NONCE_RELEASES, "outcome=\"sent\"")
        .await
}

/// A nonce release takes effect only after the upgrade transaction of
/// [`FEATURE_SEALER_V3`].
///
/// # Errors
/// Returns an error when the sealer reports version 3 before the upgrade,
/// when a release goes out or takes effect before it, when the upgrade
/// transaction fails, or when no release takes effect after it.
pub async fn release_takes_effect_only_after_the_upgrade(
    t: &Target,
    l1: &L1,
    p: Params,
) -> Result<()> {
    let signers = l2::dev_signers_through(p.funder)?;
    let funder = &signers[p.funder];

    // --- Version 2: the release does not reach the sealer. -----------------
    anyhow::ensure!(
        t.sequencer_metric_sum(SEALER_VERSION).await? == 0.0,
        "the sealer must start on decision version 2, with no rules frame"
    );
    let before = ReleaseProbe::funded(t, funder, 0x71).await?;
    before.skip_nonce_one(t).await?;
    before.stays_refused(t, REFUSED_FOR).await?;
    anyhow::ensure!(
        releases_sent(t).await? == 0.0,
        "a sequencer sent a nonce release to a sealer of version 2"
    );

    // --- The upgrade transaction, active at once. ---------------------------
    upgrade_and_await(t, l1, FEATURE_SEALER_V3, 0).await?;
    poll_until(
        "a sequencer gets the rules frame of version 3",
        Duration::from_secs(30),
        Duration::from_millis(200),
        || async { Ok((t.sequencer_metric_sum(SEALER_VERSION).await? > 0.0).then_some(())) },
    )
    .await?;

    // --- Version 3: the release takes effect. -------------------------------
    let after = ReleaseProbe::funded(t, funder, 0x72).await?;
    after.skip_nonce_one(t).await?;
    after.lands_after_release(t, LANDS_WITHIN).await?;
    anyhow::ensure!(
        releases_sent(t).await? > 0.0,
        "the new transfer landed with no nonce release sent"
    );
    Ok(())
}
