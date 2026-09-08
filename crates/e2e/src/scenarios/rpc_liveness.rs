//! `rpc_endpoints_never_hang`.
//!
//! Every endpoint answers within its contract bound, with the documented
//! error code, under bad input and while submits are parked:
//!
//! - malformed RLP or an invalid signature: `-32602`, immediate.
//! - a different transaction that reuses a landed (sender, nonce) slot: an
//!   immediate `-32602` nonce-conflict rejection, not a silent alias of
//!   another transaction's hash.
//! - an idempotent raw resubmit: immediate success, same hash.
//! - an unknown-hash receipt lookup: `null`, fast.
//! - `eth_chainId` (with the correct value) and `eth_blockNumber` stay
//!   fast while a nonce-gap submit is parked on the same server.
//! - the deferred read endpoints (`eth_getBalance`,
//!   `eth_getTransactionCount`) fail cleanly with `-32603`, and never hang.
//!
//! Two probes need dedicated stacks, so they live as separate drivers
//! below: [`connection_cap_refusal`] (a small `--rpc-max-connections`
//! value) and [`queue_depth_canary`] (a regression guard for a
//! pending-registry leak on cancelled RPC futures, fixed by the
//! Weak-indexed registry).

use std::num::NonZeroUsize;
use std::time::Duration;

use alloy_consensus::{SignableTransaction, TxEnvelope};
use alloy_eips::eip2718::Encodable2718;
use alloy_primitives::{Address, Bytes, Signature, U256};
use anyhow::{Context, Result};

use super::{CODE_INTERNAL, CODE_INVALID, Target};
use crate::harness::l2::{self, L2Client, RpcError, RpcOutcome};
use crate::harness::metrics::poll_until;

/// A call that must answer promptly, whatever the answer is.
fn assert_fast<T: std::fmt::Debug>(out: &RpcOutcome<T>, bound: Duration, what: &str) -> Result<()> {
    anyhow::ensure!(
        out.elapsed < bound,
        "{what} took {:?} (bound {bound:?}) — result {:?}",
        out.elapsed,
        out.result
    );
    Ok(())
}

fn expect_call_error<T: std::fmt::Debug>(out: &RpcOutcome<T>, code: i32, what: &str) -> Result<()> {
    match &out.result {
        Err(RpcError::Call { code: c, message }) => {
            anyhow::ensure!(*c == code, "{what}: expected {code}, got {c} ({message})");
            Ok(())
        }
        other => anyhow::bail!("{what}: expected rpc error {code}, got {other:?}"),
    }
}

/// A call that must answer promptly with a specific RPC error code:
/// [`assert_fast`] then [`expect_call_error`], under the same `what`.
fn expect_fast_error<T: std::fmt::Debug>(
    out: &RpcOutcome<T>,
    code: i32,
    bound: Duration,
    what: &str,
) -> Result<()> {
    assert_fast(out, bound, what)?;
    expect_call_error(out, code, what)
}

/// A structurally valid legacy transaction whose signature cannot recover
/// (s is far beyond the curve order). This exercises the signature-verify
/// rejection path, not the RLP decoder.
fn unrecoverable_tx(chain_id: u64, to: Address) -> Bytes {
    let tx = l2::legacy_tx(chain_id, 0, 21_000, to, U256::from(1u64));
    let sig = Signature::new(U256::from(1u64), U256::MAX, false);
    let envelope: TxEnvelope = tx.into_signed(sig).into();
    let mut bytes = Vec::with_capacity(110);
    envelope.encode_2718(&mut bytes);
    Bytes::from(bytes)
}

pub struct Params {
    pub sender: usize,
    /// A second account. Its nonce-gap submit gives the read-endpoint
    /// probes a background of parked load.
    pub parked_sender: usize,
}

impl Default for Params {
    fn default() -> Self {
        Self {
            sender: 12,
            parked_sender: 15,
        }
    }
}

/// # Errors
/// Returns an error when any endpoint in the module docs' matrix answers
/// slowly, with the wrong error code, or with the wrong result.
pub async fn run(t: &Target, p: Params) -> Result<()> {
    let signers = l2::dev_signers_through(p.sender.max(p.parked_sender))?;
    let sender = &signers[p.sender];
    let to = Address::from([0x55u8; 20]);
    let fast = Duration::from_secs(2);

    // --- Land one tx to seed the (sender, nonce 0) slot. ---------------------
    let tx0 = l2::sign_transfer(sender, t.chain_id, 0, to, 1)?;
    let out = t.rpc.send_raw(&tx0.raw).await;
    out.result
        .map_err(|e| anyhow::anyhow!("seed tx failed: {e}"))?;

    // --- Park a nonce-gap submit in the background (load backdrop). ----------
    let parked_sender = &signers[p.parked_sender];
    let gap_tx = l2::sign_transfer(parked_sender, t.chain_id, 7, to, 1)?;
    let parked = {
        let rpc = t.rpc.clone();
        let raw = gap_tx.raw.clone();
        tokio::spawn(async move { rpc.send_raw(&raw).await })
    };

    // --- Bad-input matrix, all bounded. -------------------------------------
    // Malformed RLP.
    let out = t
        .rpc
        .send_raw(&Bytes::from_static(b"\xde\xad\xbe\xef"))
        .await;
    expect_fast_error(&out, CODE_INVALID, fast, "malformed RLP submit")?;

    // Unrecoverable signature.
    let out = t.rpc.send_raw(&unrecoverable_tx(t.chain_id, to)).await;
    expect_fast_error(&out, CODE_INVALID, fast, "unrecoverable-sig submit")?;

    // A different transaction at the landed (sender, nonce 0) slot. This
    // checks identity-honest semantics: the contract gives a prompt, named
    // nonce-conflict rejection (like "nonce too low"), never the slot's
    // canonical hash (which would carry another transaction's identity and
    // silently poison a client's view of what landed). The liveness
    // property under test, a bounded and non-hanging answer, still holds
    // through this error path.
    let tx0_alt = l2::sign_transfer(sender, t.chain_id, 0, to, 2)?;
    anyhow::ensure!(tx0_alt.hash != tx0.hash, "alt tx must differ");
    let out = t.rpc.send_raw(&tx0_alt.raw).await;
    expect_fast_error(
        &out,
        CODE_INVALID,
        fast,
        "different-tx-at-landed-slot submit",
    )?;

    // Idempotent raw resubmit.
    let out = t.rpc.send_raw(&tx0.raw).await;
    assert_fast(&out, fast, "idempotent resubmit")?;
    anyhow::ensure!(
        out.result.as_ref().ok() == Some(&tx0.hash),
        "idempotent resubmit result {:?}",
        out.result
    );

    // Unknown-hash receipt lookup.
    let out = t
        .rpc
        .receipt(alloy_primitives::B256::from([0xEEu8; 32]))
        .await;
    assert_fast(&out, fast, "unknown-hash receipt")?;
    anyhow::ensure!(
        matches!(out.result, Ok(None)),
        "unknown-hash receipt: {:?}",
        out.result
    );

    // Reads stay fast and correct while the gap submit is parked.
    let expected_chain_id = format!("0x{:x}", t.chain_id);
    for i in 0..20 {
        let out = t.rpc.chain_id().await;
        assert_fast(&out, fast, &format!("eth_chainId #{i} under parked load"))?;
        let v = out
            .result
            .map_err(|e| anyhow::anyhow!("eth_chainId #{i}: {e}"))?;
        anyhow::ensure!(
            v == expected_chain_id,
            "eth_chainId returned {v}, expected {expected_chain_id}"
        );
        let out = t.rpc.block_number().await;
        assert_fast(
            &out,
            fast,
            &format!("eth_blockNumber #{i} under parked load"),
        )?;
        out.result
            .map_err(|e| anyhow::anyhow!("eth_blockNumber #{i}: {e}"))?;
    }

    // Deferred read endpoints: clean error, never a hang.
    let out = t.rpc.get_balance(sender.address).await;
    expect_fast_error(&out, CODE_INTERNAL, fast, "eth_getBalance (deferred)")?;
    let out = t.rpc.get_transaction_count(sender.address).await;
    expect_fast_error(
        &out,
        CODE_INTERNAL,
        fast,
        "eth_getTransactionCount (deferred)",
    )?;

    // The parked submit resolves with the server timeout, which is bounded.
    let out = parked.await.context("parked submit join")?;
    match out.result {
        Err(RpcError::Call { code, .. }) if code == super::CODE_TIMEOUT => {}
        other => anyhow::bail!("parked gap submit: expected -32000 timeout, got {other:?}"),
    }
    Ok(())
}

/// The connection cap refuses the (cap+1)-th concurrent connection
/// promptly, with a transport-level refusal, never an indefinite park.
/// Run this on a stack with a small `--rpc-max-connections` value (`cap`)
/// and a short pending-receipt timeout.
///
/// # Errors
/// Returns an error when a parked submit or the over-cap probe fails to
/// build a client, when the over-cap probe is slow or not a transport
/// refusal, when a parked submit does not time out with `-32000`, or when
/// capacity does not recover within 10s of the parked submits timing out.
pub async fn connection_cap_refusal(
    rpc_url: &str,
    chain_id: u64,
    cap: NonZeroUsize,
    park: Duration,
    sender_base: usize,
) -> Result<()> {
    let signers = l2::dev_signers_through(
        sender_base
            .checked_add(cap.get())
            .context("sender_base + cap overflows")?,
    )?;
    let to = Address::from([0x56u8; 20]);

    // Occupy exactly `cap` connections with parked nonce-gap submits. Each
    // client opens one HTTP connection.
    let mut parked = tokio::task::JoinSet::new();
    for i in 0..cap.get() {
        // `park` is a Target-supplied Duration; `Mul` panics on overflow, so
        // a patient client timeout saturates instead.
        let client = L2Client::new(rpc_url, park.saturating_mul(3))?;
        let tx = l2::sign_transfer(&signers[sender_base + i], chain_id, 5, to, 1)?;
        parked.spawn(async move { client.send_raw(&tx.raw).await });
    }
    // Give the parked submits a moment to occupy their connections. There
    // is no per-connection counter to poll, so this just waits a short,
    // fixed time instead.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // The next connection must be refused promptly.
    let extra = L2Client::new(rpc_url, Duration::from_secs(3))?;
    let out = extra.chain_id().await;
    anyhow::ensure!(
        out.elapsed < Duration::from_secs(3),
        "over-cap probe took {:?} — connection-table overflow must fail fast",
        out.elapsed
    );
    anyhow::ensure!(
        matches!(out.result, Err(RpcError::Transport(_))),
        "over-cap probe expected a transport refusal, got {:?}",
        out.result
    );

    // Once the parked submits time out on the server, capacity frees up,
    // and the same endpoint serves requests again.
    while let Some(j) = parked.join_next().await {
        let out = j.context("parked join")?;
        anyhow::ensure!(
            matches!(&out.result, Err(RpcError::Call { code, .. }) if *code == super::CODE_TIMEOUT),
            "parked cap submit: expected -32000, got {:?}",
            out.result
        );
    }
    poll_until(
        "post-timeout capacity recovery",
        Duration::from_secs(10),
        Duration::from_millis(250),
        || async {
            let probe = L2Client::new(rpc_url, Duration::from_secs(2))?;
            Ok(probe.chain_id().await.result.ok().map(|_| ()))
        },
    )
    .await?;
    Ok(())
}

/// Regression guard for a pending-registry leak. This cancels in-flight
/// gap submits at the client, by dropping the connection mid-park, and
/// requires `kardamom_ingress_queue_depth` to return to baseline once the
/// server park bound passes. With the Weak-indexed registry, the dropped
/// handler future kills its entry, and its `Drop` implementation reaps
/// the slot. This test checks that property end to end.
///
/// # Errors
/// Returns an error when a canary client fails to build, when a canary
/// submit is not a client-side transport abort, or when the ingress
/// queue depth does not return to baseline within its budget.
pub async fn queue_depth_canary(t: &Target, sender_base: usize, n: NonZeroUsize) -> Result<()> {
    // This is a total signer count already (not a highest index), so it
    // takes no `+ 1`.
    let signers = l2::dev_signers_total(
        sender_base
            .checked_add(n.get())
            .context("sender_base + n overflows")?,
    )?;
    let to = Address::from([0x57u8; 20]);
    let depth_before = t
        .ingress_metric_opt(super::INGRESS_QUEUE_DEPTH)
        .await?
        .unwrap_or(0.0);

    // These clients give up long before the server's park bound. Each drop
    // abandons its parked submit_raw future on the server.
    let mut aborted = tokio::task::JoinSet::new();
    for i in 0..n.get() {
        let client = L2Client::new(&t.rpc.url, Duration::from_millis(500))?;
        let tx = l2::sign_transfer(&signers[sender_base + i], t.chain_id, 9, to, 1)?;
        aborted.spawn(async move { client.send_raw(&tx.raw).await });
    }
    while let Some(j) = aborted.join_next().await {
        let out = j.context("aborted join")?;
        anyhow::ensure!(
            matches!(out.result, Err(RpcError::Transport(_))),
            "canary submit should have been client-aborted, got {:?}",
            out.result
        );
    }

    // After the server bound, plus a margin, every abandoned entry should
    // be cleaned up.
    poll_until(
        "ingress queue depth back to baseline after client aborts",
        // `pending_receipt_timeout` is a Target config Duration; `Add`
        // panics on overflow, so the poll budget saturates instead.
        t.pending_receipt_timeout
            .saturating_add(Duration::from_secs(10)),
        Duration::from_millis(500),
        || async {
            let d = t.ingress_metric(super::INGRESS_QUEUE_DEPTH).await?;
            Ok((d <= depth_before).then_some(()))
        },
    )
    .await
    .context(
        "leak regression: a cancelled RPC future left a (sender, nonce) entry in the \
         pending registry — the Weak-indexed registry should reap it on drop",
    )?;
    Ok(())
}
