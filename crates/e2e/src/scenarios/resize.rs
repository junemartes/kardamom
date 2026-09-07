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

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use alloy_primitives::{Address, B256};
use anyhow::{Context, Result};
use kardamom_types::shard_map::{ShardMap, VSLOT_COUNT, VslotSet, vslot_for};

use super::Target;
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

/// The vslots that move to lane 2 under [`map_v1`].
pub fn moved_vslots() -> VslotSet {
    let mut set = VslotSet::EMPTY;
    for v in 0..=255u8 {
        if v % 3 == 2 {
            set.insert(v);
        }
    }
    set
}

/// Map v1: the identity map over 2 lanes, with every third slot moved to
/// lane 2.
pub fn map_v1() -> ShardMap {
    let mut table = [0u8; VSLOT_COUNT];
    for (v, lane) in table.iter_mut().enumerate() {
        *lane = if v % 3 == 2 { 2 } else { (v % 2) as u8 };
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

/// Submit `tx` until a receipt exists for it. See the module doc for the
/// retry contract.
async fn land(
    rpc: &L2Client,
    tx: &l2::SignedTransfer,
    park: Duration,
    report: &mut SenderReport,
) -> Result<()> {
    for attempt in 0..40 {
        let out = rpc.send_raw(&tx.raw).await;
        match out.result {
            Ok(h) => {
                anyhow::ensure!(h == tx.hash, "submit returned {h} != {}", tx.hash);
                return Ok(());
            }
            Err(RpcError::Transport(_)) => {
                // The ingress restarted under this submit. The envelope
                // may or may not have gone out. A resubmit is idempotent.
                report.transport_retries += 1;
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
            Err(RpcError::Call { code, message }) => {
                let lower = message.to_ascii_lowercase();
                anyhow::ensure!(
                    !lower.contains("expired") && !lower.contains("evicted"),
                    "nonce {} of {}: the sequencer dropped it: {message}",
                    tx.nonce,
                    tx.sender
                );
                // A park timeout (a single-replica shard restarted under
                // this envelope), or a duplicate after an ingress restart
                // emptied the receipt cache. Either way the receipt
                // decides.
                if receipt_exists(rpc, tx.hash, park).await? {
                    return Ok(());
                }
                anyhow::ensure!(
                    code == super::CODE_TIMEOUT,
                    "nonce {} of {}: unexpected error {code}: {message}",
                    tx.nonce,
                    tx.sender
                );
                report.timeout_retries += 1;
            }
        }
        let _ = attempt;
    }
    anyhow::bail!(
        "nonce {} of {} did not land after 40 attempts",
        tx.nonce,
        tx.sender
    )
}

async fn receipt_exists(rpc: &L2Client, hash: B256, within: Duration) -> Result<bool> {
    let deadline = std::time::Instant::now() + within;
    loop {
        if rpc.receipt(hash).await.result.ok().flatten().is_some() {
            return Ok(true);
        }
        if std::time::Instant::now() >= deadline {
            return Ok(false);
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
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
        Ok((s.value(name).unwrap_or(0.0) == want).then_some(()))
    })
    .await
}

pub async fn run(stack: &mut LocalStack, p: Params) -> Result<()> {
    let park = stack.cfg.ingress.pending_receipt_timeout;
    let client_timeout = park * 3 + Duration::from_secs(5);
    let t: Target = stack.target(client_timeout)?;
    let to = Address::from([0x58u8; 20]);
    let moved = moved_vslots();

    // --- Pick the senders by vslot. --------------------------------------
    let signers = l2::dev_signers((p.sender_base + 64) as u32)?;
    let pool = &signers[p.sender_base..];
    let in_moved: Vec<&l2::DerivedSigner> = pool
        .iter()
        .filter(|s| moved.contains(vslot_for(s.address)))
        .collect();
    let outside: Vec<&l2::DerivedSigner> = pool
        .iter()
        .filter(|s| !moved.contains(vslot_for(s.address)))
        .collect();
    anyhow::ensure!(
        in_moved.len() > p.moved_senders && outside.len() >= p.unmoved_senders,
        "not enough dev signers in the moved ({}) and unmoved ({}) sets",
        in_moved.len(),
        outside.len()
    );
    let idle = in_moved[0];
    let load: Vec<l2::DerivedSigner> = in_moved[1..=p.moved_senders]
        .iter()
        .chain(outside[..p.unmoved_senders].iter())
        .map(|s| (*s).clone())
        .collect();

    let applied_start = t
        .executor_metric(super::EXEC_TX_APPLIED)
        .await
        .unwrap_or(0.0);
    let expired_start = t.sequencer_metric_sum(super::SEQ_EXPIRED).await?;
    let evicted_start = t.sequencer_metric_sum(super::SEQ_EVICTIONS).await?;

    // --- The idle sender lands before the resize. ------------------------
    let mut idle_report = SenderReport::default();
    for n in 0..3u64 {
        let tx = l2::sign_transfer(idle, t.chain_id, n, to, 1)?;
        land(&t.rpc, &tx, park, &mut idle_report).await?;
        idle_report.landed += 1;
    }

    // --- Load through every step. ----------------------------------------
    let stop = Arc::new(AtomicBool::new(false));
    let submitted = Arc::new(AtomicU64::new(0));
    let mut tasks = tokio::task::JoinSet::new();
    for signer in load {
        let rpc = t.rpc.clone();
        let stop = stop.clone();
        let submitted = submitted.clone();
        let chain_id = t.chain_id;
        let pacing = p.pacing;
        tasks.spawn(async move {
            let mut report = SenderReport::default();
            let mut nonce = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let tx = l2::sign_transfer(&signer, chain_id, nonce, to, 1)?;
                submitted.fetch_add(1, Ordering::Relaxed);
                land(&rpc, &tx, park, &mut report).await?;
                report.landed += 1;
                nonce += 1;
                tokio::time::sleep(pacing).await;
            }
            Ok::<_, anyhow::Error>(report)
        });
    }
    // Let the load reach a steady state before the first step.
    tokio::time::sleep(Duration::from_secs(2)).await;

    let warm = park + Duration::from_secs(1);
    let moved_text = moved.to_string();
    let identity = ShardMap::identity(2)?;
    let final_0 = identity.vslot_set(0).difference(&moved).to_string();
    let final_1 = identity.vslot_set(1).difference(&moved).to_string();

    // --- Step 1: start shard 2 in shadow mode. -----------------------------
    let new_index = stack.add_sequencer(&SequencerOptions {
        partition_count: Some(3),
        lane: Some(2),
        vslots: Some(moved_text.clone()),
        extra_lanes: vec![0, 1],
        shadow_vslots: Some(moved_text.clone()),
        shadow_warm: Some(warm),
        log_tag: "-shadow".into(),
    })?;
    anyhow::ensure!(new_index == 2, "new shard index {new_index} != 2");

    // --- Step 2: warm-up. -----------------------------------------------
    wait_sequencer_gauge(
        stack.sequencer_metrics(2),
        super::SEQ_SHADOW_VSLOTS,
        0.0,
        "shard 2 leaves shadow mode",
        warm + Duration::from_secs(15),
    )
    .await?;

    // --- Step 3: switch the ingress to map v1. -----------------------------
    let map_path = stack.write_shard_map(&map_v1())?;
    stack.restart_ingress(Some(&map_path))?;

    // --- Step 4: drain. ------------------------------------------------------
    tokio::time::sleep(park).await;
    for i in 0..2u32 {
        wait_sequencer_gauge(
            stack.sequencer_metrics(i),
            super::SEQ_PENDING_DEPTH,
            0.0,
            &format!("shard {i} holds no parked entry"),
            Duration::from_secs(15),
        )
        .await?;
    }

    // --- Step 5: restart the old shards with their new sets. ---------------
    stack.restart_sequencer_with(
        0,
        &SequencerOptions {
            partition_count: Some(3),
            lane: Some(0),
            vslots: Some(final_0),
            log_tag: "-final".into(),
            ..SequencerOptions::default()
        },
    )?;
    stack.restart_sequencer_with(
        1,
        &SequencerOptions {
            partition_count: Some(3),
            lane: Some(1),
            vslots: Some(final_1),
            log_tag: "-final".into(),
            ..SequencerOptions::default()
        },
    )?;

    // --- Step 6: collapse the new shard to its final config. ---------------
    stack.restart_sequencer_with(
        2,
        &SequencerOptions {
            partition_count: Some(3),
            lane: Some(2),
            vslots: Some(moved_text),
            log_tag: "-final".into(),
            ..SequencerOptions::default()
        },
    )?;

    // --- Settle, then stop the load. ---------------------------------------
    tokio::time::sleep(Duration::from_secs(2)).await;
    stop.store(true, Ordering::Relaxed);
    let mut reports = Vec::new();
    while let Some(j) = tasks.join_next().await {
        reports.push(j.context("load sender join")??);
    }
    let landed: u64 = reports.iter().map(|r| r.landed).sum();
    let submitted = submitted.load(Ordering::Relaxed);
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

    // --- The idle sender submits after the take-over. ----------------------
    // The new shard never saw this sender. The submit parks, the lookup
    // answers, and the parked entry drains.
    let t = stack.target(client_timeout)?;
    let tx = l2::sign_transfer(idle, t.chain_id, 3, to, 1)?;
    let out = t.rpc.send_raw(&tx.raw).await;
    out.result
        .map_err(|e| anyhow::anyhow!("idle sender nonce 3 after the resize failed: {e}"))?;
    anyhow::ensure!(
        out.elapsed < park,
        "idle sender nonce 3 took {:?}, at or past the park bound {:?}",
        out.elapsed,
        park
    );

    // --- Zero loss. ---------------------------------------------------------
    // The idle sender: three before, one after.
    let total = landed + 4;
    t.wait_executor_applied(applied_start + total as f64, Duration::from_secs(30))
        .await
        .context("every transaction applied exactly once")?;
    let applied = t.executor_metric(super::EXEC_TX_APPLIED).await?;
    anyhow::ensure!(
        applied == applied_start + total as f64,
        "executor applied {applied}, expected {} + {total}",
        applied_start
    );
    let expired = t.sequencer_metric_sum(super::SEQ_EXPIRED).await?;
    let evicted = t.sequencer_metric_sum(super::SEQ_EVICTIONS).await?;
    anyhow::ensure!(
        expired == expired_start && evicted == evicted_start,
        "the resize dropped transactions: expired {expired_start} -> {expired}, \
         evicted {evicted_start} -> {evicted}"
    );
    Ok(())
}
