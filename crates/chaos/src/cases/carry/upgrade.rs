//! One upgrade transaction of the carry window: the release gate, then
//! `ETHLockbox.initiateUpgrade` on the in-cluster L1.

use std::fmt;

use super::check::UpgradeCheck;
use crate::harness::Harness;
use crate::l1::L1;

/// The feature id and the activation time of one upgrade transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Upgrade {
    feature: u64,
    activation_ms: u64,
}

impl fmt::Display for Upgrade {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "initiateUpgrade({}, {})",
            self.feature, self.activation_ms
        )
    }
}

impl Upgrade {
    /// `feature` on at once.
    pub(super) const fn on(feature: u64) -> Self {
        Self {
            feature,
            activation_ms: 0,
        }
    }

    /// The feature id of the transaction.
    pub(super) const fn feature(self) -> u64 {
        self.feature
    }

    /// Whether the transaction turns the feature on at once.
    pub(super) const fn at_once(self) -> bool {
        self.activation_ms == 0
    }

    /// `feature` off: an activation at `2^64 - 1` ms never comes, so the
    /// feature is held and off.
    pub(super) const fn off(feature: u64) -> Self {
        Self {
            feature,
            activation_ms: u64::MAX,
        }
    }

    /// The release check of this transaction: the rules of `just
    /// sealer-upgrade-check`, read straight from the cluster (see
    /// `check`).
    ///
    /// # Errors
    ///
    /// Returns an error when the check refuses it, with every refusal.
    pub(super) async fn check(self, h: &Harness, ctx: &str) -> anyhow::Result<()> {
        let refused = UpgradeCheck::new(h)?.facts(self).await?.refusals(self);
        anyhow::ensure!(
            refused.is_empty(),
            "{}: {ctx}: the upgrade check refuses {self}: {}",
            crate::FAIL_PREFIX,
            refused.join("; ")
        );
        crate::log(format!("{ctx}: the upgrade check passes {self}"));
        Ok(())
    }

    /// Send this transaction to the lockbox of the deploy.
    ///
    /// # Errors
    ///
    /// Returns an error when the deploy names no lockbox, or when L1
    /// refuses the transaction.
    pub(super) async fn send(self, h: &Harness, ctx: &str) -> anyhow::Result<()> {
        let lockbox =
            h.lifecycle.vars().carry.lockbox.clone().ok_or_else(|| {
                crate::chaos_fail!("{ctx}: no lockbox; carry-versions-on deploys it")
            })?;
        crate::log(format!("{ctx}: {self} on the lockbox {lockbox}"));
        L1::new(h)
            .await?
            .initiate_upgrade(&lockbox, self.feature, self.activation_ms)
            .await
    }

    /// Check this transaction, then send it.
    ///
    /// # Errors
    ///
    /// Returns the error of the check or of the send.
    pub(super) async fn apply(self, h: &Harness, ctx: &str) -> anyhow::Result<()> {
        self.check(h, ctx).await?;
        self.send(h, ctx).await
    }
}
