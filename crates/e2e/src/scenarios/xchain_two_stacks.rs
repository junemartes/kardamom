//! S14 — TWO real LocalStacks talking cross-chain, no mock anywhere: the
//! egress-E1 acceptance.
//!
//! Chain A (the repo-default 412346) and chain B (412347, a patched-genesis
//! copy — same predeploy blobs) each run the full stack INCLUDING a
//! validator in the deployed configuration (`--parallel-validation`) with
//! the E1 serving role (`--serve-feed`). The pipeline under test end to end:
//!
//! ```text
//!  user tx on A ─► A ingress ─► A chain (Outbox.sendMessage)
//!     A validator: re-executes, extracts MessageSent (BAL cross-checked),
//!                  serves kardamom_subscribeOutbox            ─┐ real WS
//!  B's interop watcher (the real binary) subscribes to A ◄────┘
//!     derives the remote epoch ─► B sealer ─► 0x7D delivery on B
//!     (Inbox.deliver → receiver contract; callback response enqueued
//!      through B's OWN Outbox)
//!     B validator: same role, serving B's outbox lanes
//!  A's second watcher subscribes to B ─► the callback completes the
//!     round trip: onXChainResult delivered ON A (0x7D from aliased B)
//! ```
//!
//! Both validators' verdicts are load-bearing
//! ([`Target::assert_validator_verdict`]): every interop block went through
//! the whole-block parallel path, was BAL/receipt cross-checked, and the
//! extraction cross-checked each send against the claimed `sentMessages`
//! slot — a fail-stop anywhere kills the feed and the scenario with it.
//!
//! ## Origin-block closing
//!
//! A batch is one origin block. The origin validator sends a `head` event
//! after the last message once a later block closes, and the sealer stamps
//! blocks on a timer, so a lane with one message delivers by itself. No
//! synthetic closer message is needed on either leg.

use std::path::Path;
use std::time::Duration;

use alloy_primitives::{Address, B256, U256, keccak256};
use anyhow::{Context, Result};
use kardamom_types::xchain::{Callback, INBOX, OUTBOX, remote_source_hash, xchain_tx_sender};

use super::xchain::{
    RECEIVER_INIT_CODE, inbox_delivered_slot, inbox_next_seq_slot, log_address, log_topic,
    log_topic0, message_delivered_topic0, message_sent_topic0, outbox_nonces_slot, read_slot,
    u64_word,
};
use super::{Target, assert_receipt_ok, await_l2_receipt, receipt_field, receipt_placement};
use crate::harness::l2::{self, DerivedSigner};

/// Chain B's id — anything ≠ A's 412346; the harness materialises a
/// patched-genesis copy for it.
pub const CHAIN_B_ID: u64 = 412_347;

/// ABI-encode `Outbox.sendMessage(destChainId, target, gasLimit, data, cb)`.
/// Head: 4 static params + the callback tuple inlined (3 words) = 7 words,
/// so `data`'s offset is 0xE0; `cb = None` encodes the zeroed tuple
/// (`XChain.isNone`).
pub fn send_message_calldata(
    dest_chain_id: u64,
    target: Address,
    gas_limit: u64,
    data: &[u8],
    cb: Option<Callback>,
) -> Vec<u8> {
    let selector =
        &keccak256("sendMessage(uint64,address,uint64,bytes,(address,uint64,bytes32))")[..4];
    let cb = cb.unwrap_or_default();
    let mut out = Vec::with_capacity(4 + 8 * 32 + data.len().div_ceil(32) * 32);
    out.extend_from_slice(selector);
    out.extend_from_slice(u64_word(dest_chain_id).as_slice());
    out.extend_from_slice(super::xchain::address_word(target).as_slice());
    out.extend_from_slice(u64_word(gas_limit).as_slice());
    out.extend_from_slice(u64_word(7 * 32).as_slice()); // offset of `data`
    out.extend_from_slice(super::xchain::address_word(cb.target).as_slice());
    out.extend_from_slice(u64_word(cb.gas_limit).as_slice());
    out.extend_from_slice(cb.context.as_slice());
    out.extend_from_slice(u64_word(data.len() as u64).as_slice());
    out.extend_from_slice(data);
    out.resize(out.len() + (data.len().div_ceil(32) * 32 - data.len()), 0);
    out
}

/// Per-chain sender state: dev signer #0 plus its running nonce.
pub struct ChainSender {
    signer: DerivedSigner,
    payee: Address,
    nonce: u64,
}

impl ChainSender {
    pub fn new() -> Result<Self> {
        let signers = l2::dev_signers(2)?;
        Ok(Self {
            signer: signers[0].clone(),
            payee: signers[1].address,
            nonce: 0,
        })
    }
}

/// Submit one `sendMessage` on `t` and return the (asserted-successful)
/// receipt's L2 block number plus the seq the Outbox assigned (from the
/// `MessageSent` log's topic 2).
async fn send_message(
    t: &Target,
    s: &mut ChainSender,
    dest_chain_id: u64,
    target: Address,
    data: &[u8],
    cb: Option<Callback>,
    what: &str,
) -> Result<(u64, u64)> {
    let calldata = send_message_calldata(dest_chain_id, target, 150_000, data, cb);
    let tx = l2::sign_call(
        &s.signer,
        t.chain_id,
        s.nonce,
        OUTBOX,
        U256::ZERO,
        &calldata,
    )?;
    s.nonce += 1;
    t.rpc
        .send_raw(&tx.raw)
        .await
        .result
        .map_err(|e| anyhow::anyhow!("{what}: submit sendMessage: {e}"))?;
    let receipt = await_l2_receipt(t, tx.hash, what).await?;
    assert_receipt_ok(&receipt, what)?;
    let logs = receipt
        .get("logs")
        .and_then(|l| l.as_array())
        .with_context(|| format!("{what}: receipt has no logs"))?;
    let sent = logs
        .iter()
        .find(|l| {
            log_topic0(l) == Some(message_sent_topic0())
                && log_address(l).is_some_and(|a| a.eq_ignore_ascii_case(&OUTBOX.to_string()))
        })
        .with_context(|| format!("{what}: no MessageSent log from the Outbox: {receipt}"))?;
    anyhow::ensure!(
        log_topic(sent, 1) == Some(u64_word(dest_chain_id)),
        "{what}: MessageSent destChainId topic mismatch: {sent}"
    );
    let seq_word = log_topic(sent, 2).with_context(|| format!("{what}: no seq topic"))?;
    let seq = u64::from_be_bytes(seq_word.as_slice()[24..32].try_into().unwrap());
    let (block, _) = receipt_placement(&receipt)?;
    Ok((block, seq))
}

/// What the A→B leg proved; the callback leg builds on it.
pub struct ForwardOutcome {
    /// The receiver contract deployed on B.
    pub receiver_on_b: Address,
    /// The calldata word the A-side message carried.
    pub payload_word: B256,
    /// The callback requested from B back to A.
    pub callback: Callback,
    /// A-side sender (its nonce continues into the callback leg's nudges).
    pub sender_a: ChainSender,
    /// B-side sender.
    pub sender_b: ChainSender,
}

/// Leg 1 — A → B: a user tx on A sends through A's REAL Outbox; A's
/// validator extracts and serves; B's watcher (already subscribed to A's
/// validator feed) derives; B delivers 0x7D through its Inbox into the
/// receiver contract.
pub async fn forward_leg(
    a: &Target,
    b: &Target,
    a_chain_id: u64,
    b_exec_dir: &Path,
    b_cursor_file: &Path,
) -> Result<ForwardOutcome> {
    let mut sender_a = ChainSender::new()?;
    let mut sender_b = ChainSender::new()?;

    // Receiver contract on B (ordinary CREATE through B's ingress).
    let deploy = l2::sign_create(
        &sender_b.signer,
        b.chain_id,
        sender_b.nonce,
        &RECEIVER_INIT_CODE,
    )?;
    sender_b.nonce += 1;
    b.rpc
        .send_raw(&deploy.raw)
        .await
        .result
        .map_err(|e| anyhow::anyhow!("deploy receiver on B: {e}"))?;
    let receipt = await_l2_receipt(b, deploy.hash, "the receiver deploy on B").await?;
    assert_receipt_ok(&receipt, "the receiver deploy on B")?;
    let receiver_on_b = sender_b.signer.address.create(0);

    // The REAL send on A: dest = B, target = the receiver, with a callback
    // addressed back to A.
    let payload_word = B256::repeat_byte(0xA5);
    let callback = Callback {
        target: sender_a.payee, // an EOA on A — delivery trivially succeeds
        gas_limit: 90_000,
        context: B256::repeat_byte(0x42),
    };
    let (_send_block, seq) = send_message(
        a,
        &mut sender_a,
        b.chain_id,
        receiver_on_b,
        payload_word.as_slice(),
        Some(callback),
        "A->B sendMessage",
    )
    .await?;
    anyhow::ensure!(
        seq == 0,
        "first message on the A->B lane must be seq 0, got {seq}"
    );
    // No closer: A's validator sends a `head` event once the next block
    // closes, and B's watcher derives the batch from it.

    // The delivery on B: a 0x7D receipt keyed by the position-derived id.
    let source_hash = remote_source_hash(a_chain_id, 0);
    let r = await_l2_receipt(b, source_hash, "A->B delivery on B").await?;
    assert_receipt_ok(&r, "A->B delivery on B")?;
    anyhow::ensure!(
        receipt_field(&r, "effectiveGasPrice") == Some("0x0"),
        "delivery must execute fee-free: {r}"
    );
    let to = receipt_field(&r, "to").context("delivery receipt has no `to`")?;
    anyhow::ensure!(
        to.eq_ignore_ascii_case(&INBOX.to_string()),
        "delivery must call the Inbox predeploy, got {to}"
    );
    let from = receipt_field(&r, "from").context("delivery receipt has no `from`")?;
    anyhow::ensure!(
        from.eq_ignore_ascii_case(&xchain_tx_sender(a_chain_id).to_string()),
        "delivery sender must be the aliased A Outbox, got {from}"
    );
    let logs = r
        .get("logs")
        .and_then(|l| l.as_array())
        .context("no logs")?;
    anyhow::ensure!(
        logs.iter().any(|l| {
            log_topic0(l) == Some(message_delivered_topic0())
                && log_topic(l, 1) == Some(u64_word(a_chain_id))
                && log_topic(l, 2) == Some(u64_word(0))
        }),
        "no MessageDelivered(origin=A, seq=0) log: {r}"
    );
    // The callback: B's Inbox enqueued the response through B's OWN Outbox.
    anyhow::ensure!(
        logs.iter().any(|l| {
            log_topic0(l) == Some(message_sent_topic0())
                && log_address(l).is_some_and(|x| x.eq_ignore_ascii_case(&OUTBOX.to_string()))
                && log_topic(l, 1) == Some(u64_word(a_chain_id))
        }),
        "the callback response must be enqueued through B's Outbox toward A: {r}"
    );
    // Contract + Inbox state on B, from B's executor DB. Commits are
    // pipelined, so nudge B with transfers until the delivery is durable.
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        let next_seq = read_slot(b_exec_dir, INBOX, inbox_next_seq_slot(a_chain_id))?;
        if next_seq >= U256::ONE {
            break;
        }
        anyhow::ensure!(
            std::time::Instant::now() < deadline,
            "B's Inbox.nextSeq[A] never settled >= 1 (got {next_seq})"
        );
        let payee = sender_b.payee;
        let nudge = l2::sign_transfer(&sender_b.signer, b.chain_id, sender_b.nonce, payee, 1)?;
        if b.rpc.send_raw(&nudge.raw).await.result.is_ok() {
            sender_b.nonce += 1;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    let delivered = read_slot(b_exec_dir, INBOX, inbox_delivered_slot(a_chain_id, 0))?;
    anyhow::ensure!(
        delivered == U256::ONE,
        "B's Inbox.delivered[A][0] = {delivered}, expected 1"
    );
    let stored = read_slot(b_exec_dir, receiver_on_b, B256::ZERO)?;
    anyhow::ensure!(
        B256::from(stored.to_be_bytes::<32>()) == payload_word,
        "the receiver on B must hold A's calldata word: got {stored:#x}"
    );
    // The response occupies seq 0 of B's return lane to A.
    let lane_nonce = read_slot(b_exec_dir, OUTBOX, outbox_nonces_slot(a_chain_id))?;
    anyhow::ensure!(
        lane_nonce >= U256::ONE,
        "B's Outbox.nonces[A] = {lane_nonce}, expected >= 1 (the callback response)"
    );

    // B's durable lane cursor advanced past the delivered seq.
    let cursor = std::fs::read_to_string(b_cursor_file)
        .with_context(|| format!("read B cursor {}", b_cursor_file.display()))?;
    let cursor_seq: u64 = cursor
        .trim()
        .parse()
        .with_context(|| format!("parse B cursor {cursor:?}"))?;
    anyhow::ensure!(
        cursor_seq >= 1,
        "B's A-lane cursor must be >= 1, got {cursor_seq}"
    );

    Ok(ForwardOutcome {
        receiver_on_b,
        payload_word,
        callback,
        sender_a,
        sender_b,
    })
}

/// Leg 2 — the callback comes home: B's validator serves its outbox lanes,
/// A's second watcher (subscribed to B's validator feed) derives the
/// response, and `onXChainResult` is delivered ON A as a 0x7D from the
/// aliased B Outbox — the round trip A→B→A, no mock anywhere.
pub async fn callback_leg(
    a: &Target,
    b: &Target,
    a_exec_dir: &Path,
    a_cursor_file: &Path,
    mut outcome: ForwardOutcome,
) -> Result<()> {
    // No closer on B either: B's validator sends a `head` event once the
    // block after the delivery closes, and A's watcher derives the response.

    // The response delivery on A: seq 0 of B's lane to A.
    let source_hash = remote_source_hash(b.chain_id, 0);
    let r = await_l2_receipt(a, source_hash, "B->A callback delivery on A").await?;
    assert_receipt_ok(&r, "B->A callback delivery on A")?;
    let from = receipt_field(&r, "from").context("no `from`")?;
    anyhow::ensure!(
        from.eq_ignore_ascii_case(&xchain_tx_sender(b.chain_id).to_string()),
        "the response must arrive from the aliased B Outbox, got {from}"
    );
    let to = receipt_field(&r, "to").context("no `to`")?;
    anyhow::ensure!(
        to.eq_ignore_ascii_case(&INBOX.to_string()),
        "the response must be delivered through A's Inbox, got {to}"
    );
    let logs = r
        .get("logs")
        .and_then(|l| l.as_array())
        .context("no logs")?;
    anyhow::ensure!(
        logs.iter().any(|l| {
            log_topic0(l) == Some(message_delivered_topic0())
                && log_topic(l, 1) == Some(u64_word(b.chain_id))
                && log_topic(l, 2) == Some(u64_word(0))
        }),
        "no MessageDelivered(origin=B, seq=0) on A: {r}"
    );

    // A's Inbox marks the response delivered — nudge A until durable.
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        let delivered = read_slot(a_exec_dir, INBOX, inbox_delivered_slot(b.chain_id, 0))?;
        if delivered == U256::ONE {
            break;
        }
        anyhow::ensure!(
            std::time::Instant::now() < deadline,
            "A's Inbox.delivered[B][0] never settled at 1 (got {delivered})"
        );
        let payee = outcome.sender_a.payee;
        let nudge = l2::sign_transfer(
            &outcome.sender_a.signer,
            a.chain_id,
            outcome.sender_a.nonce,
            payee,
            1,
        )?;
        if a.rpc.send_raw(&nudge.raw).await.result.is_ok() {
            outcome.sender_a.nonce += 1;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }

    // A's durable cursor for the B lane advanced past the response.
    let cursor = std::fs::read_to_string(a_cursor_file)
        .with_context(|| format!("read A cursor {}", a_cursor_file.display()))?;
    let cursor_seq: u64 = cursor
        .trim()
        .parse()
        .with_context(|| format!("parse A cursor {cursor:?}"))?;
    anyhow::ensure!(
        cursor_seq >= 1,
        "A's B-lane cursor must be >= 1, got {cursor_seq}"
    );
    Ok(())
}
