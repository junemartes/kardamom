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

use std::num::NonZeroUsize;
use std::time::Duration;

use alloy_primitives::Address;
use anyhow::{Context, Result};

use super::{SeqCounters, Target};
use crate::harness::l2::{self, SignedTransfer};

pub struct Params {
    pub senders: NonZeroUsize,
    pub txs_per_sender: NonZeroUsize,
    /// First dev-mnemonic account index to use. Senders occupy the range
    /// `sender_base..sender_base+senders`.
    pub sender_base: usize,
    pub shuffle_seed: u64,
}

const DEFAULT_SENDERS: NonZeroUsize = NonZeroUsize::new(8).unwrap();
const DEFAULT_TXS_PER_SENDER: NonZeroUsize = NonZeroUsize::new(64).unwrap();

impl Default for Params {
    fn default() -> Self {
        Self {
            senders: DEFAULT_SENDERS,
            txs_per_sender: DEFAULT_TXS_PER_SENDER,
            sender_base: 1,
            shuffle_seed: 0xC0FF_EED0_0D42,
        }
    }
}

/// # Errors
/// Returns an error when a transfer fails to sign or send, when fewer
/// transactions land than were submitted, when the executor's applied
/// count does not match, when the sequencer's health counters moved, or
/// when a spot-checked receipt is missing.
pub async fn run(t: &Target, p: Params) -> Result<()> {
    // This is a total signer count already (not a highest index), so it
    // takes no `+ 1`.
    let signers = l2::dev_signers_total(
        p.sender_base
            .checked_add(p.senders.get())
            .context("sender_base + senders overflows")?,
    )?;
    let to = Address::from([0x51u8; 20]);
    let baseline = SeqCounters::snapshot(t).await?;
    let applied_before = t
        .executor_metric_opt(super::EXEC_TX_APPLIED)
        .await?
        .unwrap_or(0.0);

    // `senders * txs_per_sender` sizes the whole run; a Params product this
    // large must fail loudly rather than silently wrap the allocation.
    let total = p
        .senders
        .get()
        .checked_mul(p.txs_per_sender.get())
        .context("senders * txs_per_sender overflows")?;

    // Sign a dense nonce run for each sender, then shuffle each run on its
    // own (a per-sender seed offset makes the orders differ).
    let mut planned: Vec<SignedTransfer> = Vec::with_capacity(total);
    for (i, signer) in signers[p.sender_base..].iter().enumerate() {
        let mut run: Vec<SignedTransfer> = (0..p.txs_per_sender.get())
            .map(|n| l2::sign_transfer(signer, t.chain_id, n as u64, to, 1))
            .collect::<Result<_>>()?;
        // A wrapped-to-zero sum (only reachable with a shuffle_seed within
        // `senders` of u64::MAX) falls back to the minimum seed, instead of
        // panicking.
        let seed = std::num::NonZeroU64::new(p.shuffle_seed.wrapping_add(i as u64).wrapping_add(1))
            .unwrap_or(std::num::NonZeroU64::MIN);
        l2::seeded_shuffle(&mut run, seed);
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

    #[allow(
        clippy::cast_precision_loss,
        reason = "total is a test-parameter-sized transaction count, always small enough for \
                   f64 to represent exactly"
    )]
    let total_f64 = total as f64;
    // The executor applied exactly the batch. Wait, with a time limit, for
    // the counter to catch up with the last acks.
    t.wait_executor_applied(applied_before + total_f64, Duration::from_secs(30))
        .await?;
    let applied_after = t.executor_metric(super::EXEC_TX_APPLIED).await?;
    let expected_applied = applied_before + total_f64;
    #[allow(
        clippy::float_cmp,
        reason = "exact equality is the intended check: an executor that applied exactly the \
                   batch scrapes back bit-identical to the sum computed here"
    )]
    let applied_matches = applied_after == expected_applied;
    anyhow::ensure!(
        applied_matches,
        "executor applied {applied_after} != {expected_applied}"
    );

    // No past-nonce drops and no reorder-buffer sheds: the pipeline
    // absorbed the disorder, it did not work around it.
    baseline.assert_flat(t, "unordered batch").await?;

    // Each receipt is queryable on its own. Spot-check one receipt per
    // sender.
    for i in 0..p.senders.get() {
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
