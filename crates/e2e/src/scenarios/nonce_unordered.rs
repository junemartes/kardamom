//! S3 — `nonces_unordered_all_land`.
//!
//! K senders × N nonces each, submitted over real JSON-RPC in a per-sender
//! shuffled order, all in flight concurrently. The chain must accept every
//! one of them: the sequencer's per-sender reorder buffer absorbs arbitrary
//! disorder within its window, the pipeline executes each sender densely
//! ascending from 0, and no health counter (past-nonce drops, buffer
//! evictions) moves. Every existing nonce-order test feeds the sequencer
//! in-process; this is the same guarantee observed through
//! `eth_sendRawTransaction`.

use std::time::Duration;

use alloy_primitives::Address;
use anyhow::{Context, Result};

use super::{SeqCounters, Target};
use crate::harness::l2::{self, SignedTransfer};

pub struct Params {
    pub senders: usize,
    pub txs_per_sender: usize,
    /// First dev-mnemonic account index to use (senders occupy
    /// `sender_base..sender_base+senders`).
    pub sender_base: usize,
    pub shuffle_seed: u64,
}

impl Default for Params {
    fn default() -> Self {
        Self {
            senders: 8,
            txs_per_sender: 64,
            sender_base: 1,
            shuffle_seed: 0xC0FF_EED0_0D42,
        }
    }
}

pub async fn run(t: &Target, p: Params) -> Result<()> {
    let signers = l2::dev_signers((p.sender_base + p.senders) as u32)?;
    let to = Address::from([0x51u8; 20]);
    let baseline = SeqCounters::snapshot(t).await?;
    let applied_before = t
        .executor_metric(super::EXEC_TX_APPLIED)
        .await
        .unwrap_or(0.0);

    // Sign per-sender dense nonce runs, then shuffle each run independently
    // (seed offset per sender so orderings differ).
    let mut planned: Vec<SignedTransfer> = Vec::with_capacity(p.senders * p.txs_per_sender);
    for (i, signer) in signers[p.sender_base..].iter().enumerate() {
        let mut run: Vec<SignedTransfer> = (0..p.txs_per_sender)
            .map(|n| l2::sign_transfer(signer, t.chain_id, n as u64, to, 1))
            .collect::<Result<_>>()?;
        l2::seeded_shuffle(&mut run, p.shuffle_seed + i as u64 + 1);
        planned.extend(run);
    }

    // Fire everything concurrently. Each submit parks server-side until its
    // receipt lands, so the whole batch resolves only when every sender's
    // run has been reassembled and executed.
    let mut set = tokio::task::JoinSet::new();
    for tx in planned {
        let rpc = t.rpc.clone();
        set.spawn(async move {
            let out = rpc.send_raw(&tx.raw).await;
            (tx, out)
        });
    }

    let total = p.senders * p.txs_per_sender;
    let mut landed = 0usize;
    while let Some(joined) = set.join_next().await {
        let (tx, out) = joined.context("submit task join")?;
        let hash = out.result.map_err(|e| {
            anyhow::anyhow!(
                "sender {} nonce {} failed after {:?}: {e}",
                tx.sender,
                tx.nonce,
                out.elapsed
            )
        })?;
        anyhow::ensure!(
            hash == tx.hash,
            "sender {} nonce {}: returned hash {hash} != locally computed {}",
            tx.sender,
            tx.nonce,
            tx.hash
        );
        landed += 1;
    }
    anyhow::ensure!(landed == total, "landed {landed}/{total}");

    // Executor applied exactly the batch (bounded wait for the counter to
    // catch up with the last acks).
    t.wait_executor_applied(applied_before + total as f64, Duration::from_secs(30))
        .await?;
    let applied_after = t.executor_metric(super::EXEC_TX_APPLIED).await?;
    anyhow::ensure!(
        applied_after == applied_before + total as f64,
        "executor applied {applied_after} != {} + {total}",
        applied_before
    );

    // No past-nonce drops, no reorder-buffer sheds: disorder was absorbed,
    // not worked around.
    baseline.assert_flat(t, "unordered batch").await?;

    // Receipts are individually queryable (spot-check one per sender).
    for i in 0..p.senders {
        let signer = &signers[p.sender_base + i];
        let probe = l2::sign_transfer(signer, t.chain_id, 0, to, 1)?;
        let r = t.rpc.receipt(probe.hash).await;
        let body = r
            .result
            .map_err(|e| anyhow::anyhow!("receipt lookup failed: {e}"))?;
        anyhow::ensure!(
            body.is_some(),
            "sender {} nonce 0 receipt missing from cache",
            signer.address
        );
    }
    Ok(())
}
