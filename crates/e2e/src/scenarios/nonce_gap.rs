//! `nonce_gap_is_never_processed`.
//!
//! The nonce-gap contract, checked end to end:
//!
//! 1. A gap parks its later transactions and never executes them. Nonces
//!    {0,1,2} land. Nonces {4,5} (the gap is at 3) park on the server and
//!    fail with the documented `-32000` timeout. The wait has a bound, the
//!    server does not hang, and the executor's applied counter proves that
//!    4 and 5 never ran.
//! 2. Gap isolation: while one sender is gapped, other senders keep landing
//!    transactions at normal latency (the no-wedge property).
//! 3. Late fill: submitting nonce 3 drains the sequencer-buffered {4,5}.
//!    Receipts appear for all three, and resubmitting the timed-out raw
//!    transactions returns instantly from the receipt cache with the same
//!    hashes.
//! 4. Disorder variant: a fresh sender submits {5,3,1,0,2,4} in that wire
//!    order, staggered, and all six land (a reorder window, with no gap).
//!
//! Run this scenario on a stack with a short pending-receipt timeout. The
//! Target-L default of 30 s would stretch step 1 with no benefit. Use a
//! client timeout well above the server timeout.

use std::time::Duration;

use alloy_primitives::Address;
use anyhow::{Context, Result};

use super::{CODE_TIMEOUT, Target};
use crate::harness::l2::{self, RpcError};
use crate::harness::metrics::poll_until;

pub struct Params {
    /// Dev-mnemonic indices for the three senders (gapped, bystander,
    /// disorder).
    pub gapped: usize,
    pub bystander: usize,
    pub disorder: usize,
}

impl Default for Params {
    fn default() -> Self {
        Self {
            gapped: 9,
            bystander: 10,
            disorder: 11,
        }
    }
}

pub async fn run(t: &Target, p: Params) -> Result<()> {
    let max_idx = p.gapped.max(p.bystander).max(p.disorder);
    let signers = l2::dev_signers(max_idx as u32 + 1)?;
    let to = Address::from([0x54u8; 20]);
    let park = t.pending_receipt_timeout;
    let applied_start = t
        .executor_metric(super::EXEC_TX_APPLIED)
        .await
        .unwrap_or(0.0);

    // --- Step 1a: the contiguous prefix {0,1,2} lands normally. -------------
    let gapped = &signers[p.gapped];
    let prefix: Vec<_> = (0..3)
        .map(|n| l2::sign_transfer(gapped, t.chain_id, n, to, 1))
        .collect::<Result<_>>()?;
    let mut set = tokio::task::JoinSet::new();
    for tx in prefix {
        let rpc = t.rpc.clone();
        set.spawn(async move { (tx.nonce, rpc.send_raw(&tx.raw).await) });
    }
    while let Some(j) = set.join_next().await {
        let (nonce, out) = j.context("prefix join")?;
        out.result
            .map_err(|e| anyhow::anyhow!("prefix nonce {nonce} failed: {e}"))?;
    }

    // --- Step 1b: {4,5} park behind the missing 3. ---------------------------
    let tx4 = l2::sign_transfer(gapped, t.chain_id, 4, to, 1)?;
    let tx5 = l2::sign_transfer(gapped, t.chain_id, 5, to, 1)?;
    let mut parked = tokio::task::JoinSet::new();
    for tx in [tx4.clone(), tx5.clone()] {
        let rpc = t.rpc.clone();
        parked.spawn(async move { (tx.nonce, rpc.send_raw(&tx.raw).await) });
    }

    // --- Step 2: gap isolation. A bystander lands txs while {4,5} park. ----
    let bystander = &signers[p.bystander];
    for n in 0..3u64 {
        let tx = l2::sign_transfer(bystander, t.chain_id, n, to, 1)?;
        let out = t.rpc.send_raw(&tx.raw).await;
        out.result
            .map_err(|e| anyhow::anyhow!("bystander nonce {n} failed during gap: {e}"))?;
        anyhow::ensure!(
            out.elapsed < park / 2,
            "bystander nonce {n} took {:?} — gapped sender is wedging others",
            out.elapsed
        );
    }

    // --- Step 1c: the parked pair times out with -32000, bounded. -----------
    // The lower bound uses a wide margin (park/2). This only proves that the
    // error came from the server park, not from an eager local rejection.
    while let Some(j) = parked.join_next().await {
        let (nonce, out) = j.context("parked join")?;
        match out.result {
            Err(RpcError::Call { code, message }) => {
                anyhow::ensure!(
                    code == CODE_TIMEOUT,
                    "gap nonce {nonce}: expected {CODE_TIMEOUT}, got {code} ({message})"
                );
            }
            Err(RpcError::Transport(m)) => {
                anyhow::bail!(
                    "gap nonce {nonce}: transport-level failure ({m}) — client \
                               aborted before the server's bounded timeout"
                )
            }
            Ok(h) => anyhow::bail!("gap nonce {nonce} unexpectedly landed as {h}"),
        }
        anyhow::ensure!(
            out.elapsed >= park / 2 && out.elapsed < park * 3,
            "gap nonce {nonce}: timeout latency {:?} outside [{:?}, {:?})",
            out.elapsed,
            park / 2,
            park * 3
        );
    }

    // --- Step 1d: the gap txs were never processed. --------------------------
    let applied = t.executor_metric(super::EXEC_TX_APPLIED).await?;
    anyhow::ensure!(
        applied == applied_start + 6.0,
        "executor applied {applied}, expected {} + 6 (gap txs must not execute)",
        applied_start
    );
    for (n, tx) in [(4u64, &tx4), (5u64, &tx5)] {
        let r = t.rpc.receipt(tx.hash).await;
        let body = r.result.map_err(|e| anyhow::anyhow!("receipt({n}): {e}"))?;
        anyhow::ensure!(body.is_none(), "gap nonce {n} has a receipt — it executed");
    }

    // --- Step 3: late fill. Nonce 3 arrives; the sequencer drains {3,4,5}. ---
    let tx3 = l2::sign_transfer(gapped, t.chain_id, 3, to, 1)?;
    let out = t.rpc.send_raw(&tx3.raw).await;
    out.result
        .map_err(|e| anyhow::anyhow!("late fill nonce 3 failed: {e}"))?;
    // The pipeline executed 4 and 5 as part of the drained run. Their
    // receipts appear with no resubmission.
    poll_until(
        "drained gap receipts (4,5)",
        Duration::from_secs(10),
        Duration::from_millis(200),
        || async {
            let r4 = t.rpc.receipt(tx4.hash).await.result.ok().flatten();
            let r5 = t.rpc.receipt(tx5.hash).await.result.ok().flatten();
            Ok((r4.is_some() && r5.is_some()).then_some(()))
        },
    )
    .await
    .context("nonces 4/5 must execute once the gap fills")?;
    // An idempotent resubmit of the original raw transactions returns
    // immediately from the receipt cache, with the same hashes.
    for tx in [&tx4, &tx5] {
        let out = t.rpc.send_raw(&tx.raw).await;
        let h = out
            .result
            .map_err(|e| anyhow::anyhow!("cached resubmit nonce {}: {e}", tx.nonce))?;
        anyhow::ensure!(h == tx.hash, "cached resubmit returned {h} != {}", tx.hash);
        anyhow::ensure!(
            out.elapsed < Duration::from_secs(2),
            "cached resubmit nonce {} took {:?} — not served from cache",
            tx.nonce,
            out.elapsed
        );
    }
    t.wait_executor_applied(applied_start + 9.0, Duration::from_secs(15))
        .await
        .context("all six gapped-sender txs + three bystander txs applied")?;

    // --- Step 4: disorder variant: {5,3,1,0,2,4} in that wire order. --------
    let disorder = &signers[p.disorder];
    let order = [5u64, 3, 1, 0, 2, 4];
    let mut set = tokio::task::JoinSet::new();
    for n in order {
        let tx = l2::sign_transfer(disorder, t.chain_id, n, to, 1)?;
        let rpc = t.rpc.clone();
        set.spawn(async move { (n, rpc.send_raw(&tx.raw).await) });
        // Stagger submissions so the wire arrival order roughly follows the
        // planned order. Submission is concurrent, and the pipeline must
        // not care about the order either way.
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    while let Some(j) = set.join_next().await {
        let (nonce, out) = j.context("disorder join")?;
        out.result
            .map_err(|e| anyhow::anyhow!("disorder nonce {nonce} failed: {e}"))?;
    }
    t.wait_executor_applied(applied_start + 15.0, Duration::from_secs(15))
        .await
        .context("disorder-sender txs applied")?;
    Ok(())
}
