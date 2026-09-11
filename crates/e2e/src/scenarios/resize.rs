//! `scripted_resize_moves_senders_with_zero_loss`.
//!
//! The resize protocol of docs/specs/dynamic-sequencer-sizing.md, section
//! 3.5, scripted on the local stack, from 2 shards to 3, under load:
//!
//! 1. Start shard 2 on lane 2 with the moved vslots, subscribed to lanes
//!    0 and 1, in shadow mode.
//! 2. Wait for the warm-up (the shadow gauge reads 0).
//! 3. Switch the ingress to map v1 (a restart on the same RPC port).
//! 4. Wait `tx_ttl`, and read zero parked entries on the old shards.
//! 5. Restart shards 0 and 1 with their new vslot sets.
//! 6. Restart shard 2 with its final config.
//!
//! Load senders submit through every step. Some sit in the moved vslots,
//! some do not. A submit can fail below JSON-RPC while the ingress
//! restarts, or time out while a single-replica shard restarts (the
//! deployed cluster runs two replicas per shard, so the second case does
//! not exist there). Both are retried, and "landed" means a receipt by
//! hash exists. An idle sender in the moved set lands nonces before the
//! resize, sits idle for longer than `tx_ttl`, and submits after the
//! take-over: the new shard is cold for it and heals through the lookup.
//!
//! Zero loss: every submitted transaction lands, no `Expired` and no
//! `Evicted` error happens, and the executor applies exactly the total.
//!
//! Map v1 moves only to lane 2: `lane = 2` when `vslot % 3 == 2`, else
//! `vslot % 2`. A map that also swapped slots between lanes 0 and 1 would
//! turn the runbook into an all-to-all.

use std::num::NonZeroU32;
use std::ops::ControlFlow;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use alloy_primitives::{Address, B256};
use anyhow::{Context, Result};
use kardamom_types::shard_map::{ShardMap, VSLOT_COUNT, VslotSet, vslot_for};

use super::{Target, count_as_f64};
use crate::harness::LocalStack;
use crate::harness::l2::{self, L2Client, RpcError};
use crate::harness::metrics::{self, poll_until};
use crate::harness::services::SequencerOptions;

pub struct Params {
    /// The dev-mnemonic index the senders start at.
    pub sender_base: usize,
    /// Load senders in the moved vslots, and outside them.
    pub moved_senders: usize,
    pub unmoved_senders: usize,
    /// The pause between one sender's transactions.
    pub pacing: Duration,
}

impl Default for Params {
    fn default() -> Self {
        Self {
            sender_base: 20,
            moved_senders: 4,
            unmoved_senders: 4,
            pacing: Duration::from_millis(100),
        }
    }
}

/// The shard count after the resize.
const THREE_SHARDS: NonZeroU32 = NonZeroU32::new(3).unwrap();

/// The recipient every transfer of this scenario pays.
const TO: Address = Address::new([0x58u8; 20]);

/// How many times [`Lander::land`] resubmits before it gives up.
const LAND_ATTEMPTS: u32 = 40;

/// The dev signers the sender pool draws from, past `sender_base`.
const POOL_SIZE: usize = 64;

/// The lane of `vslot` under [`map_v1`].
fn lane_v1(vslot: u8) -> u8 {
    if vslot % 3 == 2 { 2 } else { vslot % 2 }
}

/// The vslots that move to lane 2 under [`map_v1`].
#[must_use]
pub fn moved_vslots() -> VslotSet {
    (0..=u8::MAX)
        .filter(|v| lane_v1(*v) == 2)
        .fold(VslotSet::EMPTY, VslotSet::with)
}

/// Map v1: the identity map over 2 lanes, with every third slot moved to
/// lane 2.
#[must_use]
pub fn map_v1() -> ShardMap {
    let mut table = [0u8; VSLOT_COUNT];
    for (vslot, lane) in (0..=u8::MAX).zip(table.iter_mut()) {
        *lane = lane_v1(vslot);
    }
    ShardMap::from_table(1, table)
}

/// The outcome of one load sender.
#[derive(Debug, Default)]
struct SenderReport {
    landed: u64,
    transport_retries: u64,
    timeout_retries: u64,
}

/// Submits transactions until a receipt exists for each. See the module
/// doc for the retry contract. `park` is the ingress park bound: a
/// receipt poll after a park timeout waits this long.
struct Lander {
    rpc: L2Client,
    park: Duration,
    report: SenderReport,
}

impl Lander {
    fn new(rpc: L2Client, park: Duration) -> Self {
        Self {
            rpc,
            park,
            report: SenderReport::default(),
        }
    }

    /// Submit `tx` until a receipt exists for it.
    async fn land(&mut self, tx: &l2::SignedTransfer) -> Result<()> {
        for _ in 0..LAND_ATTEMPTS {
            if let ControlFlow::Break(()) = self.attempt(tx).await? {
                self.report.landed = self.report.landed.saturating_add(1);
                return Ok(());
            }
        }
        anyhow::bail!(
            "nonce {} of {} did not land after {LAND_ATTEMPTS} attempts",
            tx.nonce,
            tx.sender
        )
    }

    /// One submit of [`Self::land`]'s loop. `Break` means the transaction
    /// landed; `Continue` means retry.
    async fn attempt(&mut self, tx: &l2::SignedTransfer) -> Result<ControlFlow<()>> {
        let out = self.rpc.send_raw(&tx.raw).await;
        match out.result {
            Ok(h) => {
                anyhow::ensure!(h == tx.hash, "submit returned {h} != {}", tx.hash);
                Ok(ControlFlow::Break(()))
            }
            Err(RpcError::Transport(_)) => self.on_transport_error().await,
            Err(RpcError::Call { code, message }) => self.on_call_error(tx, code, &message).await,
        }
    }

    /// The ingress restarted under this submit. The envelope may or may
    /// not have gone out. A resubmit is idempotent.
    async fn on_transport_error(&mut self) -> Result<ControlFlow<()>> {
        self.report.transport_retries = self.report.transport_retries.saturating_add(1);
        tokio::time::sleep(Duration::from_millis(250)).await;
        Ok(ControlFlow::Continue(()))
    }

    /// A park timeout (a single-replica shard restarted under this
    /// envelope), or a duplicate after an ingress restart emptied the
    /// receipt cache. Either way the receipt decides. A sequencer drop
    /// (`Expired`, `Evicted`) is the loss the scenario forbids.
    async fn on_call_error(
        &mut self,
        tx: &l2::SignedTransfer,
        code: i32,
        message: &str,
    ) -> Result<ControlFlow<()>> {
        let lower = message.to_ascii_lowercase();
        anyhow::ensure!(
            !lower.contains("expired") && !lower.contains("evicted"),
            "nonce {} of {}: the sequencer dropped it: {message}",
            tx.nonce,
            tx.sender
        );
        if receipt_exists(&self.rpc, tx.hash, self.park).await {
            return Ok(ControlFlow::Break(()));
        }
        anyhow::ensure!(
            code == super::CODE_TIMEOUT,
            "nonce {} of {}: unexpected error {code}: {message}",
            tx.nonce,
            tx.sender
        );
        self.report.timeout_retries = self.report.timeout_retries.saturating_add(1);
        Ok(ControlFlow::Continue(()))
    }
}

/// True when a receipt for `hash` appears within `within`.
async fn receipt_exists(rpc: &L2Client, hash: B256, within: Duration) -> bool {
    let deadline = Instant::now() + within;
    loop {
        if let ControlFlow::Break(found) = poll_receipt(rpc, hash, deadline).await {
            return found;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// One [`receipt_exists`] poll: `Break(true)` on a receipt, `Break(false)`
/// past the deadline.
async fn poll_receipt(rpc: &L2Client, hash: B256, deadline: Instant) -> ControlFlow<bool> {
    if rpc.receipt(hash).await.result.ok().flatten().is_some() {
        return ControlFlow::Break(true);
    }
    if Instant::now() >= deadline {
        return ControlFlow::Break(false);
    }
    ControlFlow::Continue(())
}

/// Wait for a gauge on one sequencer to read `want`.
async fn wait_sequencer_gauge(
    addr: std::net::SocketAddr,
    name: &str,
    want: f64,
    what: &str,
    timeout: Duration,
) -> Result<()> {
    poll_until(what, timeout, Duration::from_millis(250), || async {
        let s = metrics::scrape(addr).await?;
        #[allow(
            clippy::float_cmp,
            reason = "exact equality is the intended check: a gauge that holds a count scrapes back bit-identical"
        )]
        let at_want = s.value(name).unwrap_or(0.0) == want;
        Ok(at_want.then_some(()))
    })
    .await
}

/// One load sender: submits paced transactions, nonce by nonce, until
/// `stop` is set. The task runs on its own signer and reports its counts.
struct LoadSender {
    signer: l2::DerivedSigner,
    chain_id: u64,
    pacing: Duration,
    stop: Arc<AtomicBool>,
    submitted: Arc<AtomicU64>,
    lander: Lander,
    nonce: u64,
}

impl LoadSender {
    async fn run(mut self) -> Result<SenderReport> {
        while !self.stop.load(Ordering::Relaxed) {
            self.step().await?;
        }
        Ok(self.lander.report)
    }

    /// One transaction of [`Self::run`]'s loop.
    async fn step(&mut self) -> Result<()> {
        let tx = l2::sign_transfer(&self.signer, self.chain_id, self.nonce, TO, 1)?;
        self.submitted.fetch_add(1, Ordering::Relaxed);
        self.lander.land(&tx).await?;
        self.nonce = self.nonce.saturating_add(1);
        tokio::time::sleep(self.pacing).await;
        Ok(())
    }
}

/// The senders the scenario drives: the idle one in the moved set, and
/// the load senders in and outside it.
struct Senders {
    idle: l2::DerivedSigner,
    load: Vec<l2::DerivedSigner>,
}

impl Senders {
    /// Pick the senders by vslot from the dev signers past `sender_base`.
    fn pick(p: &Params, moved: &VslotSet) -> Result<Self> {
        let signers = l2::dev_signers_through(
            p.sender_base
                .checked_add(POOL_SIZE)
                .context("sender pool index overflowed")?,
        )?;
        let pool = &signers[p.sender_base..];
        let (in_moved, outside): (Vec<_>, Vec<_>) = pool
            .iter()
            .partition(|s| moved.contains(vslot_for(s.address)));
        anyhow::ensure!(
            in_moved.len() > p.moved_senders && outside.len() >= p.unmoved_senders,
            "not enough dev signers in the moved ({}) and unmoved ({}) sets",
            in_moved.len(),
            outside.len()
        );
        let load = in_moved[1..=p.moved_senders]
            .iter()
            .chain(outside[..p.unmoved_senders].iter())
            .map(|s| (*s).clone())
            .collect();
        Ok(Self {
            idle: in_moved[0].clone(),
            load,
        })
    }
}

/// The metric baselines sampled before the resize starts.
struct Baselines {
    applied: f64,
    expired: f64,
    evicted: f64,
}

impl Baselines {
    async fn sample(t: &Target) -> Result<Self> {
        Ok(Self {
            applied: t
                .executor_metric_opt(super::EXEC_TX_APPLIED)
                .await?
                .unwrap_or(0.0),
            expired: t.sequencer_metric_sum(super::SEQ_EXPIRED).await?,
            evicted: t.sequencer_metric_sum(super::SEQ_EVICTIONS).await?,
        })
    }
}

/// The load senders' tasks, and the shared stop flag and submit counter.
struct Load {
    stop: Arc<AtomicBool>,
    submitted: Arc<AtomicU64>,
    tasks: tokio::task::JoinSet<Result<SenderReport>>,
}

impl Load {
    /// Start one task per load sender.
    fn start(t: &Target, p: &Params, senders: Vec<l2::DerivedSigner>, park: Duration) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let submitted = Arc::new(AtomicU64::new(0));
        let mut tasks = tokio::task::JoinSet::new();
        for signer in senders {
            let sender = LoadSender {
                signer,
                chain_id: t.chain_id,
                pacing: p.pacing,
                stop: stop.clone(),
                submitted: submitted.clone(),
                lander: Lander::new(t.rpc.clone(), park),
                nonce: 0,
            };
            tasks.spawn(sender.run());
        }
        Self {
            stop,
            submitted,
            tasks,
        }
    }

    /// Stop the load and collect the reports. Returns the landed count.
    async fn stop(mut self) -> Result<u64> {
        self.stop.store(true, Ordering::Relaxed);
        let mut reports = Vec::new();
        while let Some(j) = self.tasks.join_next().await {
            reports.push(j.context("load sender join")??);
        }
        let landed: u64 = reports.iter().map(|r| r.landed).sum();
        let submitted = self.submitted.load(Ordering::Relaxed);
        anyhow::ensure!(
            landed == submitted,
            "landed {landed} != submitted {submitted}"
        );
        let transport_retries: u64 = reports.iter().map(|r| r.transport_retries).sum();
        let timeout_retries: u64 = reports.iter().map(|r| r.timeout_retries).sum();
        eprintln!(
            "resize: {landed} transactions landed across {} senders \
             (transport retries {transport_retries}, park-timeout retries {timeout_retries})",
            reports.len()
        );
        Ok(landed)
    }
}

/// The resize runbook's six steps, on the stack, under load.
struct Runbook<'a> {
    stack: &'a mut LocalStack,
    park: Duration,
    moved: VslotSet,
}

impl Runbook<'_> {
    /// Steps 1 to 6 of the module doc.
    async fn run(&mut self) -> Result<()> {
        let warm = self.park.saturating_add(Duration::from_secs(1));
        let moved_text = self.moved.to_string();
        let identity = ShardMap::identity(2)?;
        let final_0 = identity.vslot_set(0).difference(&self.moved).to_string();
        let final_1 = identity.vslot_set(1).difference(&self.moved).to_string();

        // Step 1: start shard 2 in shadow mode.
        let new_index = self.stack.add_sequencer(&SequencerOptions {
            partition_count: Some(THREE_SHARDS),
            lane: Some(2),
            vslots: Some(moved_text.clone()),
            extra_lanes: vec![0, 1],
            shadow_vslots: Some(moved_text.clone()),
            shadow_warm: Some(warm),
            log_tag: "-shadow".into(),
        })?;
        anyhow::ensure!(new_index == 2, "new shard index {new_index} != 2");

        // Step 2: warm-up.
        wait_sequencer_gauge(
            self.stack.sequencer_metrics(2),
            super::SEQ_SHADOW_VSLOTS,
            0.0,
            "shard 2 leaves shadow mode",
            warm.saturating_add(Duration::from_secs(15)),
        )
        .await?;

        // Step 3: switch the ingress to map v1.
        let map_path = self.stack.write_shard_map(&map_v1())?;
        self.stack.restart_ingress(Some(&map_path))?;

        // Step 4: drain.
        tokio::time::sleep(self.park).await;
        for i in 0..2u32 {
            self.wait_shard_drained(i).await?;
        }

        // Step 5: restart the old shards with their new sets.
        self.restart_final(0, final_0)?;
        self.restart_final(1, final_1)?;

        // Step 6: collapse the new shard to its final config.
        self.restart_final(2, moved_text)
    }

    /// One old shard of step 4: it holds no parked entry.
    async fn wait_shard_drained(&self, index: u32) -> Result<()> {
        wait_sequencer_gauge(
            self.stack.sequencer_metrics(index),
            super::SEQ_PENDING_DEPTH,
            0.0,
            &format!("shard {index} holds no parked entry"),
            Duration::from_secs(15),
        )
        .await
    }

    /// Restart shard `index` on its own lane with `vslots` as its final
    /// set, under the 3-shard count.
    fn restart_final(&mut self, index: u32, vslots: String) -> Result<()> {
        let lane = u8::try_from(index).context("shard index fits a lane")?;
        self.stack.restart_sequencer_with(
            index,
            &SequencerOptions {
                partition_count: Some(THREE_SHARDS),
                lane: Some(lane),
                vslots: Some(vslots),
                log_tag: "-final".into(),
                ..SequencerOptions::default()
            },
        )
    }
}

/// # Errors
/// Returns an error when a runbook step fails, a load sender loses a
/// transaction, the idle sender does not heal through the lookup, or the
/// executor's applied count does not match the total.
pub async fn run(stack: &mut LocalStack, p: Params) -> Result<()> {
    let park = stack.cfg.ingress.pending_receipt_timeout.as_duration();
    let client_timeout = park
        .saturating_mul(3)
        .saturating_add(Duration::from_secs(5));
    let t: Target = stack.target(client_timeout)?;
    let moved = moved_vslots();
    let senders = Senders::pick(&p, &moved)?;
    let base = Baselines::sample(&t).await?;

    // The idle sender lands before the resize.
    let mut idle = Lander::new(t.rpc.clone(), park);
    for n in 0..3u64 {
        idle.land(&l2::sign_transfer(&senders.idle, t.chain_id, n, TO, 1)?)
            .await?;
    }

    // Load through every step. Let it reach a steady state first.
    let load = Load::start(&t, &p, senders.load, park);
    tokio::time::sleep(Duration::from_secs(2)).await;
    Runbook { stack, park, moved }.run().await?;
    // Settle, then stop the load.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let landed = load.stop().await?;

    // The idle sender submits after the take-over. The new shard never
    // saw this sender. The submit parks, the lookup answers, and the
    // parked entry drains.
    let t = stack.target(client_timeout)?;
    let tx = l2::sign_transfer(&senders.idle, t.chain_id, 3, TO, 1)?;
    let out = t.rpc.send_raw(&tx.raw).await;
    out.result
        .map_err(|e| anyhow::anyhow!("idle sender nonce 3 after the resize failed: {e}"))?;
    anyhow::ensure!(
        out.elapsed < park,
        "idle sender nonce 3 took {:?}, at or past the park bound {:?}",
        out.elapsed,
        park
    );

    assert_zero_loss(&t, &base, landed.saturating_add(4)).await
}

/// Zero loss: the executor applied exactly `total` more transactions
/// (the load, plus the idle sender's three before and one after), and no
/// sequencer expired or evicted anything.
async fn assert_zero_loss(t: &Target, base: &Baselines, total: u64) -> Result<()> {
    let want = base.applied + count_as_f64(total);
    t.wait_executor_applied(want, Duration::from_secs(30))
        .await
        .context("every transaction applied exactly once")?;
    let applied = t.executor_metric(super::EXEC_TX_APPLIED).await?;
    let expired = t.sequencer_metric_sum(super::SEQ_EXPIRED).await?;
    let evicted = t.sequencer_metric_sum(super::SEQ_EVICTIONS).await?;
    #[allow(
        clippy::float_cmp,
        reason = "exact equality is the intended check: counters scrape back bit-identical"
    )]
    let (applied_exact, no_drops) = (
        applied == want,
        expired == base.expired && evicted == base.evicted,
    );
    anyhow::ensure!(
        applied_exact,
        "executor applied {applied}, expected {} + {total}",
        base.applied
    );
    anyhow::ensure!(
        no_drops,
        "the resize dropped transactions: expired {} -> {expired}, evicted {} -> {evicted}",
        base.expired,
        base.evicted
    );
    Ok(())
}
