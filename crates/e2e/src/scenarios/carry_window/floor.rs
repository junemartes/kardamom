//! A sequencer floor above the decision version of the chain.
//!
//! 1. Feature 3 turns on version 4. Each sequencer restarts with the floor
//!    5, under load. The first report of the sealer lowers the kept
//!    version to 4, and every transfer lands.
//! 2. Feature 4 turns on version 5. A probe sends its nonce 1 first: the v2
//!    loop of its sequencer takes it with its bytes, and holds it for nonce
//!    0. Feature 4 turns off with a later activation, so the chain runs
//!    version 4 again, and the sequencer keeps version 4. Then nonce 0 goes
//!    out as a reference without bytes, and the held nonce 1 goes out with
//!    its bytes, below version 5. Every member counts it, the readers run
//!    it, and both receipts land.
//!
//! The first case is what a wrong floor does. Its offers with bytes race
//! the first report of the sealer, which comes a few milliseconds after the
//! session opens, so the case does not count on one. The second case makes
//! the same kind of offer on purpose: the sequencer took the bytes under
//! version 5, and the offer goes out under version 4.

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
    r.full_reference_below_five().await?;
    r.rounds(3).await?;
    r.t.assert_validator_verdict("a full reference below version 5")
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

    /// Case 2: a full reference that the sealer orders below version 5.
    async fn full_reference_below_five(&mut self) -> Result<()> {
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
            after.iter().zip(&before).all(|(a, b)| a > b),
            "every member orders the held offer as a full reference below version 5: \
             {before:?} -> {after:?}"
        );
        self.egress.await_status(first.hash, "executed").await?;
        self.egress.await_status(held.hash, "executed").await?;
        let block = receipt_placement(&receipt)?.block;
        Gauge::of(self.batcher, reads::BATCHER_POSTED)
            .await_above(block)
            .await
            .context("the batcher posts the block of the full reference")
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
