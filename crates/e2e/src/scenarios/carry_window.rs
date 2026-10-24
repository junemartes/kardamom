//! The carry window rollout on a live local chain, in the order of the
//! staging rollout.
//!
//! 1. The stack starts with the member feed on member 0, the egress, the
//!    frames of the executor for the egress, and a live batcher.
//! 2. Features 3 and 4 turn on decision version 5. The sequencers keep
//!    version 5.
//! 3. The sequencers restart with the floor 5. From version 5 on they offer
//!    no reference without bytes, so R, the highest stored reference of
//!    the egress, stops. A restart of the egress keeps R.
//! 4. The validator and the batcher move to the egress once they are past
//!    R. Their cursor gauges pass R.
//! 5. With the batcher stopped, the retained frames grow past `retention`.
//!    The upgrade transaction of feature 5 turns the carry window on. Every
//!    member reports `carryWindow`, and the retained frames fall within
//!    `retention`. The batcher starts again on the egress and posts.
//! 6. The executor moves to carry mode on the feed of member 0, on a
//!    quiet chain. Then the load runs again. The receipts keep landing,
//!    and the validator agrees.
//! 7. A follower restarts from its snapshot under load. It keeps the exact
//!    window.
//! 8. A sequencer on the v1 loop, as an older release, offers a reference
//!    without bytes. Every member refuses it with egress kind 18, the
//!    sequencer publishes a drop, and the egress reports `dropped`. R does
//!    not move. The same transaction, sent again through a sequencer of
//!    this release, gets its receipt.
//! 9. Feature 5 turns off with a later activation: the posted rule comes
//!    back, and the chain keeps going. A new activation turns it on again.
//!
//! Every transfer of the load gets its receipt. The test then compares
//! the persisted state of both executors and the validator.
//!
//! The local stack runs one executor, so "one executor at a time" is one
//! move here, and the egress has one copy of the frames. The members share
//! one host, so only member 0 can feed the shared media driver: the feed
//! uses fixed stream ids on IPC.

mod floor;
mod load;
mod reads;

use std::ops::RangeInclusive;
use std::time::Duration;

use alloy_primitives::Address;
use anyhow::{Context, Result};
use kardamom_types::upgrades::{FEATURE_CARRY_WINDOW, FEATURE_SEALER_V4, FEATURE_SEALER_V5};

pub use floor::wrong_floor;
use load::Load;
use reads::{EgressView, FEATURE_ON, Gauge, Members, Reference};

use super::upgrade::upgrade_and_await;
use super::{EXEC_BLOCK_NUMBER, Target, VALIDATOR_COMMITTED_BLOCK, metric_u64};
use crate::harness::LocalStack;
use crate::harness::l2::{self, DerivedSigner};
use crate::harness::metrics::poll_until;
use crate::harness::services::SequencerOptions;

/// The drop text of a reference that the sealer refused.
const REFUSED_TEXT: &str = "transaction dropped by the sealer, send it again";

/// The compare window of the first carry start of an executor, in blocks:
/// the default of `--rollback-compare-blocks`.
const COMPARE_BLOCKS: u64 = 32;

/// The empty blocks before the executor moves to carry mode.
const QUIET_BLOCKS: u64 = 8;

/// An activation time that keeps a feature off.
const OFF: u64 = u64::MAX;

/// The recipient of the probe transfers.
const PAYEE: Address = Address::repeat_byte(0x7C);

pub struct Params {
    /// The dev accounts of the load. The staging genesis funds #0 to #17.
    pub load: RangeInclusive<usize>,
    /// The dev accounts that the probes pick from.
    pub probes: RangeInclusive<usize>,
}

impl Default for Params {
    fn default() -> Self {
        Self {
            load: 1..=8,
            probes: 10..=17,
        }
    }
}

/// The steps of the rollout, on the stack.
struct Rollout<'a> {
    stack: &'a mut LocalStack,
    t: Target,
    /// The client bound of every [`Target`] of the scenario.
    rpc_timeout: Duration,
    members: Members,
    egress: EgressView,
    batcher: std::net::SocketAddr,
    retention: u64,
    /// The load, while it does not run on its own task.
    load: Option<Load>,
    /// R once the sequencers keep version 5.
    reference: Reference,
    p: Params,
}

/// # Errors
/// Returns an error when a step of the rollout fails. See the module doc.
pub async fn run(stack: &mut LocalStack, p: Params) -> Result<()> {
    let mut r = Rollout::new(stack, p)?;
    r.versions_on().await?;
    r.references_stop().await?;
    r.readers_on_the_egress().await?;
    r.window_on().await?;
    r.executor_in_carry_mode().await?;
    r.member_restarts().await?;
    r.reference_refused().await?;
    r.window_off_and_on().await?;
    r.finish().await
}

impl<'a> Rollout<'a> {
    fn new(stack: &'a mut LocalStack, p: Params) -> Result<Self> {
        let park = stack.cfg.ingress.pending_receipt_timeout.as_duration();
        let rpc_timeout = park
            .saturating_mul(3)
            .saturating_add(Duration::from_secs(5));
        let t = stack.target(rpc_timeout)?;
        let seams = stack
            .carry_seams()
            .context("the carry window needs StackConfig::carry")?;
        let retention = stack
            .cfg
            .carry
            .map(|c| u64::from(c.retention.get()))
            .context("the carry window needs StackConfig::carry")?;
        Ok(Self {
            load: Some(Load::new(t.rpc.clone(), t.chain_id, p.load.clone())?),
            members: Members::new(seams.sealer_admin),
            egress: EgressView::new(&seams.egress_url, seams.egress_metrics)?,
            batcher: seams
                .batcher_metrics
                .context("the carry window needs a batcher (StackConfig::l1)")?,
            stack,
            t,
            rpc_timeout,
            retention,
            reference: Reference { index: 0, block: 0 },
            p,
        })
    }

    /// Step 2: features 3 and 4. The load before them offers references
    /// without bytes, so R is above 0.
    async fn versions_on(&mut self) -> Result<()> {
        self.rounds(3).await?;
        self.activate(FEATURE_SEALER_V4, 0, 4).await?;
        self.activate(FEATURE_SEALER_V5, 0, 5).await?;
        self.await_sequencer_version(5).await?;
        self.rounds(3).await?;
        let r = self.egress.reference().await?;
        anyhow::ensure!(r.index > 0, "the egress stored no reference: R is vacuous");
        Ok(())
    }

    /// Step 3: the floor, and R stops. The egress can trail the chain, so
    /// each read of R waits until the egress stores the head of the
    /// executor.
    async fn references_stop(&mut self) -> Result<()> {
        self.restart_sequencers(&Self::floor_five("-floor"))?;
        self.await_sequencer_version(5).await?;
        self.rounds(3).await?;
        self.await_egress().await?;
        self.reference = self.egress.reference().await?;
        self.rounds(3).await?;
        self.await_egress().await?;
        self.assert_reference("after the floor").await?;
        self.stack.restart_egress()?;
        self.egress.await_up().await?;
        self.assert_reference("after a restart of the egress")
            .await?;
        self.rounds(3).await?;
        self.await_egress().await?;
        self.assert_reference("after the egress restart and more load")
            .await?;
        let below = self.members.values(reads::FULL_REF_BELOW_V5, "").await?;
        anyhow::ensure!(
            below.iter().all(|v| *v == 0),
            "a member ordered a full reference below version 5: {below:?}"
        );
        Ok(())
    }

    /// Step 4: the validator and the batcher move to the egress. Before
    /// the move, both are past the block of R, and past the floor of the
    /// restarted egress: the head of the executor is above both.
    async fn readers_on_the_egress(&mut self) -> Result<()> {
        let validator = self.t.validator_metrics.context("no validator")?;
        let past = self.reference;
        let block = self.block().await?.max(past.block);
        Gauge::of(validator, VALIDATOR_COMMITTED_BLOCK)
            .await_above(block)
            .await?;
        Gauge::of(self.batcher, reads::BATCHER_POSTED)
            .await_above(block)
            .await?;
        self.stack.restart_validator_on_egress()?;
        self.stack.start_batcher_on_egress()?;
        let batcher = self.batcher;
        self.under_load(async |r| {
            Gauge::of(validator, reads::VALIDATOR_CURSOR)
                .await_above(past.index)
                .await?;
            Gauge::of(batcher, reads::BATCHER_CURSOR)
                .await_above(past.index)
                .await?;
            r.t.assert_validator_verdict("the validator on the egress")
                .await
        })
        .await
    }

    /// Step 5: feature 5, under a DA lag.
    async fn window_on(&mut self) -> Result<()> {
        self.stack.stop_batcher()?;
        let retention = self.retention;
        self.under_load(async |r| {
            r.members
                .await_all(reads::RETAINED_FRAMES, "", |v| v > retention)
                .await
                .map(|_| ())
        })
        .await
        .context("the retained frames grow past retention under the DA lag")?;
        self.assert_no_refusal().await?;
        self.window(0, true).await?;
        self.assert_no_refusal().await?;
        let sealed = self.block().await?;
        self.stack.start_batcher_on_egress()?;
        let batcher = self.batcher;
        self.under_load(async |_| {
            Gauge::of(batcher, reads::BATCHER_POSTED)
                .await_above(sealed)
                .await
                .map(|_| ())
        })
        .await
        .context("the batcher posts from the egress below the window")
    }

    /// Step 6: the executor moves to carry mode. The move runs on a quiet
    /// chain: no transfer and no epoch, and the egress holds the head of
    /// the executor. Then the next block is empty, and the egress chains
    /// the new copy of the frames (see
    /// [`LocalStack::restart_executor_in_carry_mode`]). The load then runs
    /// again.
    async fn executor_in_carry_mode(&mut self) -> Result<()> {
        let feed = self.members.values(reads::FEED_STATE, "").await?;
        anyhow::ensure!(feed[0] > 0, "member 0 runs no feed: {feed:?}");
        let floor = self
            .reference
            .block
            .checked_add(COMPARE_BLOCKS)
            .context("block overflows")?;
        let executor = self.t.executor_metrics;
        Gauge::of(executor, EXEC_BLOCK_NUMBER)
            .await_above(floor)
            .await?;
        self.quiet_restart().await?;
        self.under_load(async |r| {
            poll_until(
                "the executor reads the member feed",
                Duration::from_mins(1),
                Duration::from_millis(250),
                || async {
                    Ok(r.stack
                        .carry_executor_log()
                        .filter(|log| log.contains("canonical entries via the member feed"))
                        .map(|_| ()))
                },
            )
            .await?;
            let feed = r.members.admin(0);
            poll_until(
                "the feed of member 0 serves live",
                Duration::from_mins(1),
                Duration::from_millis(250),
                || async {
                    let state = Gauge::of(feed, reads::FEED_STATE).read().await?;
                    Ok((state == reads::FEED_LIVE).then_some(()))
                },
            )
            .await?;
            r.await_progress().await?;
            r.t.assert_validator_verdict("the executor in carry mode")
                .await
        })
        .await
    }

    /// Restart the executor in carry mode on a quiet chain: no epoch, empty
    /// blocks, and a frozen sealer while the egress takes the last frames
    /// and the executor stops. Then the next block is empty, and the egress
    /// chains the new copy of the frames.
    async fn quiet_restart(&mut self) -> Result<()> {
        anyhow::ensure!(
            self.stack.suspend_da_watcher(),
            "the carry window needs a da-watcher (StackConfig::l1)"
        );
        let quiet = self.block().await?.saturating_add(QUIET_BLOCKS);
        Gauge::of(self.t.executor_metrics, EXEC_BLOCK_NUMBER)
            .await_above(quiet)
            .await?;
        self.await_egress().await?;
        self.stack.freeze_sealer();
        let restarted = self.restart_when_settled().await;
        self.stack.thaw_sealer();
        self.stack.resume_da_watcher();
        restarted
    }

    /// On a frozen chain: wait until the egress holds every frame of the
    /// executor, then restart the executor in carry mode.
    async fn restart_when_settled(&mut self) -> Result<()> {
        let block = self.block().await?;
        self.egress.await_settled(block).await?;
        self.stack.restart_executor_in_carry_mode()
    }

    /// A follower restarts from its snapshot under load, and keeps the
    /// exact window. A kill of the leader is a case of the chaos suite.
    async fn member_restarts(&mut self) -> Result<()> {
        let retention = self.retention;
        let follower = self.members.follower().await?;
        self.under_load(async |r| {
            r.stack.restart_sealer_member(follower)?;
            r.members.await_carry_window(true).await?;
            r.members.await_exact_window(retention).await?;
            r.await_progress().await
        })
        .await
    }

    /// Step 7: a reference without bytes after the switch.
    async fn reference_refused(&mut self) -> Result<()> {
        let shard = 0;
        let probe = self.probe_on(shard)?;
        let older = SequencerOptions {
            v1_loop: true,
            ..Self::floor_five("-v1")
        };
        self.restart_sequencer(shard, &older)?;
        let before = self.members.values(reads::REF_REFUSED, "").await?;
        let tx = l2::sign_transfer(&probe, self.t.chain_id, 0, PAYEE, 1)?;
        let (rpc, raw) = (self.t.rpc.clone(), tx.raw.clone());
        let parked = tokio::spawn(async move { rpc.send_raw(&raw).await });
        let dropped = self.egress.await_status(tx.hash, "dropped").await?;
        parked.abort();
        anyhow::ensure!(
            dropped["reason"].as_str() == Some(REFUSED_TEXT) && dropped["nonce"] == "0x0",
            "the drop of the refused reference: {dropped}"
        );
        self.assert_refused_everywhere(shard, &before).await?;
        anyhow::ensure!(
            self.t
                .rpc
                .receipt(tx.hash)
                .await
                .result
                .ok()
                .flatten()
                .is_none(),
            "a refused reference got a receipt"
        );
        self.assert_reference("after the refusal").await?;
        self.restart_sequencer(shard, &Self::floor_five("-resend"))?;
        Load::land_one(&self.t.rpc, &tx).await?;
        self.egress.await_status(tx.hash, "executed").await?;
        self.rounds(3).await
    }

    /// Each member counted one more refusal, and the sequencer of `shard`
    /// counted the kind-18 frame and published the drop.
    async fn assert_refused_everywhere(&self, shard: u32, before: &[u64]) -> Result<()> {
        let after = self.members.values(reads::REF_REFUSED, "").await?;
        anyhow::ensure!(
            after.iter().zip(before).all(|(a, b)| a > b),
            "every member refuses the reference: {before:?} -> {after:?}"
        );
        let seq = self.stack.sequencer_metrics(shard);
        let frames = Gauge::of(seq, reads::SEQ_REF_REFUSED).read().await?;
        let drops = Gauge::of(seq, reads::SEQ_TX_DROPS)
            .with("reason=\"sealer_refused_reference\"")
            .read()
            .await?;
        anyhow::ensure!(
            frames > 0 && drops > 0,
            "sequencer {shard}: {frames} kind-18 frames, {drops} drops"
        );
        Ok(())
    }

    /// Step 8: feature 5 off with a later activation, then on again.
    async fn window_off_and_on(&mut self) -> Result<()> {
        self.under_load(async |r| {
            r.window(OFF, false).await?;
            r.window(0, true).await
        })
        .await
    }

    /// The end: the exact window, and the validator agrees.
    async fn finish(&mut self) -> Result<()> {
        self.rounds(3).await?;
        self.members.await_exact_window(self.retention).await?;
        self.t.assert_validator_verdict("carry window").await?;
        let load = self.load.as_ref().context("the load runs")?;
        eprintln!(
            "carry window: {} transfers landed, {} resends",
            load.landed, load.resends
        );
        Ok(())
    }

    /// Send the upgrade transaction of feature 5 at `activation`, and wait
    /// until every member reports `carryWindow` as `on`, with the window
    /// rule that goes with it.
    async fn window(&mut self, activation: u64, on: bool) -> Result<()> {
        let l1 = self.stack.l1().context("no L1")?;
        upgrade_and_await(&self.t, l1, FEATURE_CARRY_WINDOW, activation).await?;
        self.members.await_carry_window(on).await?;
        let state = if on {
            FEATURE_ON
        } else {
            reads::FEATURE_HELD_OFF
        };
        self.members
            .await_all(reads::FEATURE_STATE, "feature=\"5\"", |v| v == state)
            .await?;
        if on {
            return self.members.await_exact_window(self.retention).await;
        }
        self.members
            .await_all(reads::WINDOW_EXACT, "", |v| v == 0)
            .await
            .map(|_| ())
    }

    /// Send the upgrade transaction of `feature` at `activation`, and wait
    /// until every member runs `version`.
    async fn activate(&mut self, feature: u64, activation: u64, version: u64) -> Result<()> {
        let l1 = self.stack.l1().context("no L1")?;
        upgrade_and_await(&self.t, l1, feature, activation).await?;
        self.members
            .await_all(reads::DECISION_VERSION, "", |v| v == version)
            .await
            .map(|_| ())
    }

    async fn assert_no_refusal(&self) -> Result<()> {
        let refused = self.members.values(reads::REF_REFUSED, "").await?;
        anyhow::ensure!(
            refused.iter().all(|v| *v == 0),
            "a member refused a reference of this release: {refused:?}"
        );
        Ok(())
    }

    /// R is still the value of step 3.
    async fn assert_reference(&self, when: &str) -> Result<()> {
        let now = self.egress.reference().await?;
        anyhow::ensure!(
            now == self.reference,
            "R moved {when}: {:?} -> {now:?}",
            self.reference
        );
        Ok(())
    }

    async fn rounds(&mut self, n: u32) -> Result<()> {
        self.load.as_mut().context("the load runs")?.rounds(n).await
    }

    /// Run `step` while the load runs on its own task. Every transfer of
    /// the load gets its receipt.
    async fn under_load(&mut self, step: impl AsyncFnOnce(&mut Self) -> Result<()>) -> Result<()> {
        let running = self.load.take().context("the load runs")?.start();
        let outcome = step(self).await;
        self.load = Some(running.stop().await?);
        outcome
    }

    /// The block of the executor. A restarted executor shows the gauge
    /// once it commits a block, so this waits for one.
    async fn block(&self) -> Result<u64> {
        Gauge::of(self.t.executor_metrics, EXEC_BLOCK_NUMBER)
            .await_above(0)
            .await
    }

    /// Wait until the egress stores the head of the executor.
    async fn await_egress(&self) -> Result<()> {
        let block = self.block().await?;
        self.egress.await_head_past(block).await
    }

    /// Wait until the executor commits 8 more blocks.
    async fn await_progress(&self) -> Result<()> {
        let now = self.block().await?;
        Gauge::of(self.t.executor_metrics, EXEC_BLOCK_NUMBER)
            .await_above(now.saturating_add(8))
            .await
            .map(|_| ())
    }

    /// Wait until every sequencer keeps `version`.
    async fn await_sequencer_version(&self, version: u64) -> Result<()> {
        poll_until(
            &format!("every sequencer keeps version {version}"),
            Duration::from_mins(1),
            Duration::from_millis(250),
            || async {
                let versions = self
                    .t
                    .sequencer_metric_values_where(reads::SEQ_SEALER_VERSION, "")
                    .await?
                    .into_iter()
                    .map(metric_u64)
                    .collect::<Result<Vec<_>>>()?;
                Ok((!versions.is_empty() && versions.iter().all(|v| *v == version)).then_some(()))
            },
        )
        .await
    }

    /// A sequencer of this release with the floor 5, logging to a file
    /// with `log_tag`.
    fn floor_five(log_tag: &str) -> SequencerOptions {
        SequencerOptions {
            sealer_version_floor: 5,
            log_tag: log_tag.into(),
            ..SequencerOptions::default()
        }
    }

    fn restart_sequencers(&mut self, opts: &SequencerOptions) -> Result<()> {
        (0..self.stack.cfg.shards.get())
            .try_for_each(|i| self.stack.restart_sequencer_with(i, opts))?;
        self.t = self.stack.target(self.rpc_timeout)?;
        Ok(())
    }

    fn restart_sequencer(&mut self, shard: u32, opts: &SequencerOptions) -> Result<()> {
        self.stack.restart_sequencer_with(shard, opts)?;
        self.t = self.stack.target(self.rpc_timeout)?;
        Ok(())
    }

    /// A probe sender that sequencer `shard` serves.
    fn probe_on(&self, shard: u32) -> Result<DerivedSigner> {
        l2::dev_signers_through(*self.p.probes.end())?
            .into_iter()
            .skip(*self.p.probes.start())
            .find(|s| self.stack.sequencer_for(s.address) == shard)
            .with_context(|| format!("no probe sender on shard {shard}"))
    }
}
