//! A sequencer floor above the decision version of the chain.
//!
//! 1. Feature 3 turns on version 4. Each sequencer restarts with the floor
//!    5, under load. The first report of the sealer lowers the kept
//!    version to 4, and every transfer lands.
//! 2. Feature 4 turns on version 5. A probe sends nonce 1 first, and the
//!    sequencer holds it for nonce 0. Feature 4 turns off, and the chain
//!    returns to version 4. Both nonces then leave in the current format,
//!    without carried bytes. Both receipts land, and the batcher posts them.
//!
//! A parked transaction must use the version at release, rather than the
//! version at admission. The sealer tests cover old full references below
//! version 5 separately.

use anyhow::{Context, Result};
use kardamom_types::upgrades::{FEATURE_SEALER_V4, FEATURE_SEALER_V5};

use super::load::Load;
use super::reads::{self, Gauge};
use super::{OFF, PAYEE, Params, Rollout};
use crate::harness::LocalStack;
use crate::harness::l2;
use crate::scenarios::{SEQ_PENDING_DEPTH, await_l2_receipt, receipt_placement};

/// # Errors
/// Returns an error when a step fails. See the module doc.
pub async fn wrong_floor(stack: &mut LocalStack, p: Params) -> Result<()> {
    let mut r = Rollout::new(stack, p)?;
    r.rounds(2).await?;
    r.activate(FEATURE_SEALER_V4, 0, 4).await?;
    r.await_sequencer_version(4).await?;
    r.floor_above_the_chain().await?;
    r.held_offer_after_downgrade().await?;
    r.rounds(3).await?;
    r.t.assert_validator_verdict("a held offer after a downgrade")
        .await
}

impl Rollout<'_> {
    /// Case 1: the floor 5 on a chain at version 4.
    async fn floor_above_the_chain(&mut self) -> Result<()> {
        let floor = Self::floor_five("-floor");
        self.under_load(async |r| r.restart_sequencers(&floor))
            .await?;
        self.await_sequencer_version(4).await?;
        let below = self.members.values(reads::FULL_REF_BELOW_V5, "").await?;
        eprintln!("wrong floor: full references below version 5 after the restart: {below:?}");
        self.rounds(3).await
    }

    /// Case 2: the held offer uses the version at release.
    async fn held_offer_after_downgrade(&mut self) -> Result<()> {
        self.activate(FEATURE_SEALER_V5, 0, 5).await?;
        self.await_sequencer_version(5).await?;
        let shard = 1;
        let probe = self.probe_on(shard)?;
        let before = self.members.values(reads::FULL_REF_BELOW_V5, "").await?;
        let first = l2::sign_transfer(&probe, self.t.chain_id, 0, PAYEE, 1)?;
        let held = l2::sign_transfer(&probe, self.t.chain_id, 1, PAYEE, 1)?;
        let (rpc, raw) = (self.t.rpc.clone(), held.raw.clone());
        let parked = tokio::spawn(async move { rpc.send_raw(&raw).await });
        self.await_held(shard).await?;
        self.activate(FEATURE_SEALER_V5, OFF, 4).await?;
        self.await_sequencer_version(4).await?;
        Load::land_one(&self.t.rpc, &first).await?;
        let receipt = await_l2_receipt(&self.t, held.hash, "the held nonce 1").await?;
        parked.abort();
        let after = self.members.values(reads::FULL_REF_BELOW_V5, "").await?;
        anyhow::ensure!(
            after == before,
            "the held offer must use the version-4 reference format: {before:?} -> {after:?}"
        );
        self.egress.await_status(first.hash, "executed").await?;
        self.egress.await_status(held.hash, "executed").await?;
        let block = receipt_placement(&receipt)?.block;
        Gauge::of(self.batcher, reads::BATCHER_POSTED)
            .await_above(block)
            .await
            .context("the batcher posts the held transaction")
            .map(|_| ())
    }

    /// Wait until sequencer `shard` holds a transaction for a nonce gap.
    async fn await_held(&self, shard: u32) -> Result<()> {
        Gauge::of(self.stack.sequencer_metrics(shard), SEQ_PENDING_DEPTH)
            .await_above(0)
            .await
            .context("the sequencer holds nonce 1 for nonce 0")
            .map(|_| ())
    }
}
