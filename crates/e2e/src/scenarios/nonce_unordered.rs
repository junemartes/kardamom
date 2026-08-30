//! `nonces_unordered_all_land`.
//!
//! K senders each submit N nonces over real JSON-RPC, in a shuffled order
//! per sender, all in flight at the same time. The chain must accept every
//! transaction: the sequencer's per-sender reorder buffer absorbs any
//! disorder within its window, the pipeline executes each sender in dense
//! ascending order from 0, and no health counter moves (no past-nonce
//! drops, no buffer evictions). Every other nonce-order test feeds the
//! sequencer in-process. This test proves the same guarantee through
//! `eth_sendRawTransaction`.

use std::time::Duration;

use alloy_primitives::Address;
use anyhow::{Context, Result};

use super::{SeqCounters, Target};
use crate::harness::l2::{self, SignedTransfer};

pub struct Params {
    pub senders: usize,
    pub txs_per_sender: usize,
    /// First dev-mnemonic account index to use. Senders occupy the range
    /// `sender_base..sender_base+senders`.
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

    // Sign a dense nonce run for each sender, then shuffle each run on its
    // own (a per-sender seed offset makes the orders differ).
    let mut planned: Vec<SignedTransfer> = Vec::with_capacity(p.senders * p.txs_per_sender);
    for (i, signer) in signers[p.sender_base..].iter().enumerate() {
        let mut run: Vec<SignedTransfer> = (0..p.txs_per_sender)
            .map(|n| l2::sign_transfer(signer, t.chain_id, n as u64, to, 1))
            .collect::<Result<_>>()?;
        l2::seeded_shuffle(&mut run, p.shuffle_seed + i as u64 + 1);
        planned.extend(run);
    }

    // Send everything at the same time. Each submit call waits on the
    // server until its receipt lands. So the whole batch finishes only
    // after the pipeline reassembles and executes every sender's run.
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

    // The executor applied exactly the batch. Wait, with a time limit, for
    // the counter to catch up with the last acks.
    t.wait_executor_applied(applied_before + total as f64, Duration::from_secs(30))
        .await?;
    let applied_after = t.executor_metric(super::EXEC_TX_APPLIED).await?;
    anyhow::ensure!(
        applied_after == applied_before + total as f64,
        "executor applied {applied_after} != {} + {total}",
        applied_before
    );

    // No past-nonce drops and no reorder-buffer sheds: the pipeline
    // absorbed the disorder, it did not work around it.
    baseline.assert_flat(t, "unordered batch").await?;

    // Each receipt is queryable on its own. Spot-check one receipt per
    // sender.
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
