//! S13d — a skipped transaction uses its nonce once [`FEATURE_SEALER_V3`]
//! is on.
//!
//! The sealer's contiguity guard counts every ordered entry, skipped or
//! not. Before the upgrade, a skip uses no nonce on the chain, so the
//! chain and the guard disagree, and the sender is stuck. The sealer runs
//! decision version 3 from its start. From the activation of the upgrade
//! transaction on, a skip at the chain nonce uses that nonce.
//!
//! One probe runs before the upgrade and one after it, each on a fresh
//! sender with a small balance. The sender sends transfers at nonces 0
//! and 1. Each one fits the balance, and the two together do not, so
//! nonce 1 skips with insufficient funds.
//!
//! - Before the upgrade, the chain expects nonce 1 next, and the guard
//!   expects nonce 2. A new transfer at nonce 1 does not land.
//! - After the upgrade, the skip uses nonce 1. The chain expects nonce 2,
//!   a new transfer at nonce 1 is refused, and a transfer at nonce 2
//!   lands. The validator agrees with the executor.

use std::time::Duration;

use alloy_primitives::{Address, B256};
use alloy_signer_local::PrivateKeySigner;
use anyhow::{Context, Result};
use kardamom_types::upgrades::FEATURE_SEALER_V3;

use super::upgrade::upgrade_and_await;
use super::{Target, assert_receipt_ok, await_l2_receipt, metric_u64, receipt_field};
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

const SEALER_VERSION: &str = "kardamom_sequencer_sealer_version";

/// The default funder is account 0: the upgrade genesis funds only it.
#[derive(Default)]
pub struct Params {
    /// The dev-mnemonic index that funds the probe senders.
    pub funder: usize,
}

/// A fresh sender that skips nonce 1.
struct SkipProbe {
    sender: DerivedSigner,
}

impl SkipProbe {
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
        Ok(Self { sender })
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

    /// A transfer of `value` from the probe at `nonce`.
    fn transfer(&self, t: &Target, nonce: u64, value: u64) -> Result<SignedTransfer> {
        l2::sign_transfer(&self.sender, t.chain_id, nonce, PAYEE, value)
    }

    /// Send nonces 0 and 1, wait for the skip of nonce 1, and return the
    /// nonce that the chain expects after it. Nonce 1 goes first, so the
    /// ingress checks it against the whole balance, before nonce 0
    /// executes. The sequencer holds it until nonce 0 arrives.
    async fn skip_nonce_one(&self, t: &Target) -> Result<u64> {
        let first = self.transfer(t, 0, SPEND_WEI)?;
        let second = self.transfer(t, 1, SPEND_WEI)?;
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
        Self::chain_nonce(t, self.sender.address).await
    }

    /// One try of `tx`: `Some` when it has a receipt. The sealer refuses
    /// the transfer while its guard is ahead of the chain, so the client
    /// sends it again.
    async fn try_send(t: &Target, tx: &SignedTransfer) -> Result<Option<()>> {
        // The answer does not matter: a refusal and an accepted offer both
        // lead to the next look at the receipt.
        let _answer = t.rpc.send_raw(&tx.raw).await;
        Ok(t.rpc
            .receipt(tx.hash)
            .await
            .result
            .ok()
            .flatten()
            .map(|_| ()))
    }

    /// A new transfer at nonce 1 does not land within `within`.
    async fn stays_stuck(&self, t: &Target, within: Duration) -> Result<()> {
        let retry = self.transfer(t, 1, 1)?;
        let landed = poll_until(
            "the new transfer at nonce 1 (it must not land)",
            within,
            Duration::from_millis(500),
            || Self::try_send(t, &retry),
        )
        .await;
        anyhow::ensure!(
            landed.is_err()
                && t.rpc
                    .receipt(retry.hash)
                    .await
                    .result
                    .ok()
                    .flatten()
                    .is_none(),
            "a transfer at the skipped nonce landed while the feature was off"
        );
        anyhow::ensure!(
            Self::chain_nonce(t, self.sender.address).await? == 1,
            "the chain still expects the skipped nonce"
        );
        Ok(())
    }

    /// After a skip that used nonce 1: a new transfer at nonce 1 is
    /// refused, and a transfer at nonce 2 lands.
    async fn moves_on(&self, t: &Target) -> Result<()> {
        let reuse = self.transfer(t, 1, 1)?;
        anyhow::ensure!(
            t.rpc.send_raw(&reuse.raw).await.result.is_err(),
            "the ingress must refuse a new transfer at the used nonce 1"
        );
        Self::lands(t, &self.transfer(t, 2, 1)?, "the transfer at nonce 2").await
    }

    /// Wait until every sequencer reports decision version 3: the sealer runs
    /// it from its start.
    async fn await_version_three(t: &Target) -> Result<()> {
        poll_until(
            "every sequencer gets the rules frame of version 3",
            Duration::from_secs(30),
            Duration::from_millis(200),
            || async {
                let versions = t
                    .sequencer_metric_values_where(SEALER_VERSION, "")
                    .await?
                    .into_iter()
                    .map(metric_u64)
                    .collect::<Result<Vec<_>>>()?;
                Ok((!versions.is_empty() && versions.iter().all(|v| *v == 3)).then_some(()))
            },
        )
        .await
    }
}

/// A skipped transaction uses its nonce only after the upgrade
/// transaction of [`FEATURE_SEALER_V3`].
///
/// # Errors
/// Returns an error when a sequencer does not report decision version 3,
/// when a skip uses its nonce before the upgrade, when the upgrade transaction
/// fails, when a skip uses no nonce after it, when the next nonce does not
/// land, or when the validator disagrees.
pub async fn skip_uses_its_nonce_only_after_the_upgrade(
    t: &Target,
    l1: &L1,
    p: Params,
) -> Result<()> {
    let signers = l2::dev_signers_through(p.funder)?;
    let funder = &signers[p.funder];

    // --- Before the upgrade: the skip uses no nonce. ------------------------
    SkipProbe::await_version_three(t).await?;
    let before = SkipProbe::funded(t, funder, 0x71).await?;
    anyhow::ensure!(
        before.skip_nonce_one(t).await? == 1,
        "a skip before the upgrade uses no nonce"
    );
    before.stays_stuck(t, REFUSED_FOR).await?;

    // --- The upgrade transaction, active at once. ---------------------------
    upgrade_and_await(t, l1, FEATURE_SEALER_V3, 0).await?;

    // --- After the upgrade: the skip uses its nonce. ------------------------
    let after = SkipProbe::funded(t, funder, 0x72).await?;
    anyhow::ensure!(
        after.skip_nonce_one(t).await? == 2,
        "a skip after the upgrade uses its nonce"
    );
    after.moves_on(t).await?;
    t.assert_validator_verdict("S13d").await
}
