//! S14 — TWO real `LocalStack`s talking cross-chain, no mock anywhere: the
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
//! A batch is one origin block, and an origin block is only known complete
//! when a LATER message appears on the same lane (the watcher's grouping
//! rule). Each leg therefore sends a CLOSER — a second `sendMessage` on the
//! same lane, forced into a later origin block — whose own delivery is never
//! awaited (its block stays open, exactly like S12's seq-3 sentinel).

use std::path::Path;
use std::time::Duration;

use alloy_primitives::{Address, B256, U256, keccak256};
use anyhow::{Context, Result};
use kardamom_types::xchain::{Callback, INBOX, OUTBOX, remote_source_hash, xchain_tx_sender};

use super::xchain::{
    RECEIVER_INIT_CODE, assert_cursor_at_least, assert_delivery_receipt, inbox_delivered_slot,
    inbox_next_seq_slot, log_address, log_topic, log_topic0, message_delivered_topic0,
    message_sent_topic0, outbox_nonces_slot, read_slot, u64_word,
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
#[must_use]
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
    /// # Errors
    /// Returns an error when signer derivation fails.
    pub fn new() -> Result<Self> {
        let signers = l2::dev_signers_total(2)?;
        Ok(Self {
            signer: signers[0].clone(),
            payee: signers[1].address,
            nonce: 0,
        })
    }

    /// The current nonce, advancing it by one.
    ///
    /// # Errors
    /// Returns an error when the nonce overflows.
    fn next_nonce(&mut self) -> Result<u64> {
        let n = self.nonce;
        self.nonce = self
            .nonce
            .checked_add(1)
            .context("sender nonce overflows")?;
        Ok(n)
    }

    /// Submit one `sendMessage` on `t` and return the (asserted-successful)
    /// receipt's L2 block number plus the seq the Outbox assigned (from
    /// the `MessageSent` log's topic 2).
    async fn send_message(
        &mut self,
        t: &Target,
        dest_chain_id: u64,
        target: Address,
        data: &[u8],
        cb: Option<Callback>,
        what: &str,
    ) -> Result<(u64, u64)> {
        let calldata = send_message_calldata(dest_chain_id, target, 150_000, data, cb);
        let nonce = self.next_nonce()?;
        let tx = l2::sign_call(
            &self.signer,
            t.chain_id,
            nonce,
            OUTBOX,
            U256::ZERO,
            &calldata,
        )?;
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
        let block = receipt_placement(&receipt)?.block;
        Ok((block, seq))
    }

    /// Send a CLOSER on the same lane, retrying until it lands in a block
    /// STRICTLY AFTER `after_block` — what lets the watcher's grouping
    /// rule close the previous origin block. Its own delivery is never
    /// awaited.
    async fn send_closer(
        &mut self,
        t: &Target,
        dest_chain_id: u64,
        after_block: u64,
        what: &str,
    ) -> Result<()> {
        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        loop {
            let payee = self.payee;
            let (block, _seq) = self
                .send_message(t, dest_chain_id, payee, &[0xC1, 0x05, 0xE2], None, what)
                .await?;
            if block > after_block {
                return Ok(());
            }
            anyhow::ensure!(
                std::time::Instant::now() < deadline,
                "{what}: closer never landed after block {after_block}"
            );
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
    }

    /// Nudge the chain with 1-wei transfers from this sender, retrying
    /// every 300ms, until `cond` reports settled (`Ok(None)`) or 60s
    /// elapse. `cond` returns `Some(detail)` describing the unmet value
    /// while waiting, which becomes part of the timeout message
    /// (`"{what} ({detail})"`).
    ///
    /// Commits are pipelined and settle at later tx-carrying boundaries,
    /// so state a caller is polling for needs real traffic to become
    /// durable — bounded, never a fixed sleep.
    async fn nudge_until<F>(&mut self, t: &Target, what: &str, mut cond: F) -> Result<()>
    where
        F: FnMut() -> Result<Option<String>>,
    {
        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        loop {
            let Some(detail) = cond()? else {
                return Ok(());
            };
            anyhow::ensure!(std::time::Instant::now() < deadline, "{what} ({detail})");
            let payee = self.payee;
            let nudge = l2::sign_transfer(&self.signer, t.chain_id, self.nonce, payee, 1)?;
            if t.rpc.send_raw(&nudge.raw).await.result.is_ok() {
                self.next_nonce()?;
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
    }
}

/// What the A→B leg proved; the callback leg builds on it.
pub struct ForwardOutcome {
    /// The receiver contract deployed on B.
    pub receiver_on_b: Address,
    /// The calldata word the A-side message carried.
    pub payload_word: B256,
    /// The callback requested from B back to A.
    pub callback: Callback,
    /// A-side sender (its nonce continues into the callback leg's closers).
    pub sender_a: ChainSender,
    /// B-side sender.
    pub sender_b: ChainSender,
    /// L2 block on B that delivered seq 0 (the callback's origin block).
    pub delivery_block_on_b: u64,
}

/// Leg 1's state: both chains, A's origin id, B's executor state dir, the
/// two per-chain senders, the receiver deployed on B, and the payload
/// word it carries. The steps below read and update this as state
/// instead of taking and threading it through loose parameters.
struct ForwardLeg<'a> {
    a: &'a Target,
    b: &'a Target,
    a_chain_id: u64,
    b_exec_dir: &'a Path,
    b_cursor_file: &'a Path,
    sender_a: ChainSender,
    sender_b: ChainSender,
    receiver_on_b: Address,
    payload_word: B256,
}

impl<'a> ForwardLeg<'a> {
    /// Derive both senders and deploy the receiver contract on B
    /// (ordinary CREATE through B's ingress).
    async fn new(
        a: &'a Target,
        b: &'a Target,
        a_chain_id: u64,
        b_exec_dir: &'a Path,
        b_cursor_file: &'a Path,
    ) -> Result<Self> {
        let sender_a = ChainSender::new()?;
        let mut sender_b = ChainSender::new()?;
        let receiver_on_b = Self::deploy_receiver_on_b(b, &mut sender_b).await?;
        Ok(Self {
            a,
            b,
            a_chain_id,
            b_exec_dir,
            b_cursor_file,
            sender_a,
            sender_b,
            receiver_on_b,
            payload_word: B256::repeat_byte(0xA5),
        })
    }

    async fn deploy_receiver_on_b(b: &Target, sender_b: &mut ChainSender) -> Result<Address> {
        let nonce = sender_b.next_nonce()?;
        let deploy = l2::sign_create(&sender_b.signer, b.chain_id, nonce, &RECEIVER_INIT_CODE)?;
        b.rpc
            .send_raw(&deploy.raw)
            .await
            .result
            .map_err(|e| anyhow::anyhow!("deploy receiver on B: {e}"))?;
        let receipt = await_l2_receipt(b, deploy.hash, "the receiver deploy on B").await?;
        assert_receipt_ok(&receipt, "the receiver deploy on B")?;
        Ok(sender_b.signer.address.create(0))
    }

    /// The REAL send on A: dest = B, target = the receiver, with
    /// `callback` addressed back to A. Sends, then closes the origin
    /// block so the batch derives.
    async fn send_and_close_forward(&mut self, callback: Callback) -> Result<()> {
        let (send_block, seq) = self
            .sender_a
            .send_message(
                self.a,
                self.b.chain_id,
                self.receiver_on_b,
                self.payload_word.as_slice(),
                Some(callback),
                "A->B sendMessage",
            )
            .await?;
        anyhow::ensure!(
            seq == 0,
            "first message on the A->B lane must be seq 0, got {seq}"
        );
        self.sender_a
            .send_closer(self.a, self.b.chain_id, send_block, "A->B closer")
            .await
    }

    /// The delivery on B: a 0x7D receipt keyed by the position-derived
    /// id, with the `MessageDelivered` and callback `MessageSent` logs
    /// checked. Returns the L2 block on B that delivered it.
    async fn await_forward_delivery(&self) -> Result<u64> {
        let r = assert_delivery_receipt(self.b, self.a_chain_id, 0, "A->B delivery on B").await?;
        let logs = r
            .get("logs")
            .and_then(|l| l.as_array())
            .context("no logs")?;
        anyhow::ensure!(
            logs.iter().any(|l| {
                log_topic0(l) == Some(message_delivered_topic0())
                    && log_topic(l, 1) == Some(u64_word(self.a_chain_id))
                    && log_topic(l, 2) == Some(u64_word(0))
            }),
            "no MessageDelivered(origin=A, seq=0) log: {r}"
        );
        // The callback: B's Inbox enqueued the response through B's OWN
        // Outbox.
        anyhow::ensure!(
            logs.iter().any(|l| {
                log_topic0(l) == Some(message_sent_topic0())
                    && log_address(l).is_some_and(|x| x.eq_ignore_ascii_case(&OUTBOX.to_string()))
                    && log_topic(l, 1) == Some(u64_word(self.a_chain_id))
            }),
            "the callback response must be enqueued through B's Outbox toward A: {r}"
        );
        let delivery_block_on_b = receipt_placement(&r)?.block;
        Ok(delivery_block_on_b)
    }

    /// Contract + Inbox state on B, from B's executor DB. Commits are
    /// pipelined, so nudge B with transfers until the delivery is
    /// durable. `>= 1`, not `== 1`: a retried closer can legitimately
    /// close its predecessor's block and deliver an extra lane seq.
    async fn settle_forward_state(&mut self) -> Result<()> {
        let b_exec_dir = self.b_exec_dir;
        let a_chain_id = self.a_chain_id;
        self.sender_b
            .nudge_until(self.b, "B's Inbox.nextSeq[A] never settled >= 1", || {
                let next_seq = read_slot(b_exec_dir, INBOX, inbox_next_seq_slot(a_chain_id))?;
                Ok((next_seq < U256::ONE).then(|| format!("got {next_seq}")))
            })
            .await?;
        let delivered = read_slot(
            self.b_exec_dir,
            INBOX,
            inbox_delivered_slot(self.a_chain_id, 0),
        )?;
        anyhow::ensure!(
            delivered == U256::ONE,
            "B's Inbox.delivered[A][0] = {delivered}, expected 1"
        );
        let stored = read_slot(self.b_exec_dir, self.receiver_on_b, B256::ZERO)?;
        anyhow::ensure!(
            B256::from(stored.to_be_bytes::<32>()) == self.payload_word,
            "the receiver on B must hold A's calldata word: got {stored:#x}"
        );
        // The response occupies seq 0 of B's return lane to A.
        let lane_nonce = read_slot(self.b_exec_dir, OUTBOX, outbox_nonces_slot(self.a_chain_id))?;
        anyhow::ensure!(
            lane_nonce >= U256::ONE,
            "B's Outbox.nonces[A] = {lane_nonce}, expected >= 1 (the callback response)"
        );
        Ok(())
    }

    /// B's durable lane cursor advanced past the delivered seq (>= 1: a
    /// retried closer can push it further).
    fn assert_forward_cursor(&self) -> Result<()> {
        assert_cursor_at_least(self.b_cursor_file, 1, "B's A-lane")
    }
}

/// Leg 1 — A → B: a user tx on A sends through A's REAL Outbox; A's
/// validator extracts and serves; B's watcher (already subscribed to A's
/// validator feed) derives; B delivers 0x7D through its Inbox into the
/// receiver contract.
///
/// # Errors
/// Returns an error at any of the checks the module docs describe: the
/// receiver deploy, the send and its closer, the delivery receipt and its
/// logs, the settled Inbox/Outbox/receiver state on B, or B's durable
/// cursor.
pub async fn forward_leg(
    a: &Target,
    b: &Target,
    a_chain_id: u64,
    b_exec_dir: &Path,
    b_cursor_file: &Path,
) -> Result<ForwardOutcome> {
    let mut leg = ForwardLeg::new(a, b, a_chain_id, b_exec_dir, b_cursor_file).await?;

    let callback = Callback {
        target: leg.sender_a.payee, // an EOA on A — delivery trivially succeeds
        gas_limit: 90_000,
        context: B256::repeat_byte(0x42),
    };
    leg.send_and_close_forward(callback).await?;

    let delivery_block_on_b = leg.await_forward_delivery().await?;
    leg.settle_forward_state().await?;
    leg.assert_forward_cursor()?;

    Ok(ForwardOutcome {
        receiver_on_b: leg.receiver_on_b,
        payload_word: leg.payload_word,
        callback,
        sender_a: leg.sender_a,
        sender_b: leg.sender_b,
        delivery_block_on_b,
    })
}

/// Leg 2 — the callback comes home: B's validator serves its outbox lanes,
/// A's second watcher (subscribed to B's validator feed) derives the
/// response, and `onXChainResult` is delivered ON A as a 0x7D from the
/// aliased B Outbox — the round trip A→B→A, no mock anywhere.
///
/// # Errors
/// Returns an error when the closer fails, when the response delivery
/// receipt or its `MessageDelivered` log is missing or wrong, when A's
/// Inbox never settles the delivery, or when A's durable cursor does not
/// advance.
pub async fn callback_leg(
    a: &Target,
    b: &Target,
    a_chain_id: u64,
    a_exec_dir: &Path,
    a_cursor_file: &Path,
    mut outcome: ForwardOutcome,
) -> Result<()> {
    // Close the response's origin block on B: a user send on the SAME B→A
    // lane, in a later B block than the delivery that enqueued the response.
    outcome
        .sender_b
        .send_closer(b, a_chain_id, outcome.delivery_block_on_b, "B->A closer")
        .await?;

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
    outcome
        .sender_a
        .nudge_until(a, "A's Inbox.delivered[B][0] never settled at 1", || {
            let delivered = read_slot(a_exec_dir, INBOX, inbox_delivered_slot(b.chain_id, 0))?;
            Ok((delivered != U256::ONE).then(|| format!("got {delivered}")))
        })
        .await?;

    // A's durable cursor for the B lane advanced past the response.
    assert_cursor_at_least(a_cursor_file, 1, "A's B-lane")
}
