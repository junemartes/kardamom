//! S12 — cross-chain message delivery, end to end through the REAL stack.
//!
//! The destination pipeline under test is the production one: the
//! `kardamom-da-watcher` binary in interop mode (WS source → shared
//! derivation rule → durable cursor → Aeron `tx_remote_epochs`), the real
//! sequencers relaying onto the cluster, the Java sealer's per-peer origin
//! advance, and the executor delivering 0x7D txs through the genesis-seeded
//! `Inbox` predeploy. The only simulated piece is the ORIGIN CHAIN itself:
//! a [`MockInteropFeed`] — a real jsonrpsee WS server speaking the real feed
//! protocol — plays the external validator, exactly the "simulate an external
//! validator's responses" seam. [`super::xchain_two_stacks`] runs the true
//! two-stack e2e, with a real egress node in place of the mock feed.
//!
//! Two arms:
//!
//! * [`delivery`] — 3 messages across 2 origin blocks: a Receiver-style
//!   contract call (calldata proven via the receiver's storage), a plain EOA
//!   call, and a message with a callback (the response enqueued through the
//!   destination's OWN Outbox, asserted via both the `MessageSent` receipt
//!   log and the Outbox's storage). Plus: 0x7D receipts keyed by
//!   `remote_source_hash`, `Inbox.delivered`/`nextSeq` via executor state,
//!   the durable lane cursor file, and the sequencer relay metrics.
//! * [`gap_halts_pair_not_chain`] — the adversarial arm: the feed swallows a
//!   seq, the watcher process fail-stops (exits nonzero, never skips), and
//!   the chain demonstrably keeps sealing blocks — the pair-scoped fault
//!   domain, proven at process level.

use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use alloy_primitives::{Address, B256, Bytes as AlloyBytes, U256, keccak256};
use anyhow::{Context, Result};
use kardamom_da_watcher::interop::mock::MockInteropFeed;
use kardamom_types::StateDatabase;
use kardamom_types::xchain::{
    Anchor, Callback, INBOX, Inbox, OUTBOX, Outbox, OutboxMessage, XChainMessage,
    remote_source_hash, xchain_tx_sender,
};

use super::{
    Target, assert_receipt_ok, await_l2_receipt, open_state_ro, receipt_field, receipt_placement,
};
use crate::harness::l2::{self, DerivedSigner};
use crate::harness::metrics;

pub use super::xchain_skipped_seq::{gap_halts_pair_not_chain, sealer_rejects_a_skipped_seq};

/// The simulated origin chain's id — anything ≠ the stack's 412346.
pub const ORIGIN_CHAIN_ID: u64 = 412_399;

/// `keccak256("MessageDelivered(uint64,uint64,bool)")` — pinned by
/// `forge inspect Inbox events`.
pub(crate) fn message_delivered_topic0() -> B256 {
    keccak256("MessageDelivered(uint64,uint64,bool)")
}

// The Outbox / Inbox storage slots and the `MessageSent` topic have one
// definition, on `kardamom_types::xchain::{Outbox, Inbox}`. The tests
// there pin each value against `forge inspect` and `cast index`.
pub(crate) use super::u64_word;

/// Minimal "Receiver-style" runtime, deployed via an ordinary CREATE tx
/// through the ingress: copies the first 32 bytes of calldata into storage
/// slot 0. 12 bytes of init returning 14 bytes of runtime — no compiler in
/// the loop, and the assertion (slot 0 == the payload word we sent from the
/// "origin chain") proves the delivered calldata reached the target intact.
pub(crate) const RECEIVER_INIT_CODE: [u8; 26] = [
    // init: CODECOPY(0, 0x0c, 0x0e); RETURN(0, 0x0e)
    0x60, 0x0e, 0x60, 0x0c, 0x60, 0x00, 0x39, 0x60, 0x0e, 0x60, 0x00, 0xf3,
    // runtime: CALLDATACOPY(0, 0, 0x20); SSTORE(0, MLOAD(0)); STOP
    0x60, 0x20, 0x60, 0x00, 0x60, 0x00, 0x37, 0x60, 0x00, 0x51, 0x60, 0x00, 0x55, 0x00,
];

/// An address as a 32-byte topic word (left-padded), the indexed-address
/// encoding.
pub(crate) fn address_word(a: Address) -> B256 {
    let mut w = [0u8; 32];
    w[12..].copy_from_slice(a.as_slice());
    B256::from(w)
}

/// One storage slot of `address`, read from the executor's live state DB
/// (MVCC read-only snapshot — the `read_validator_state_root` seam).
pub(crate) fn read_slot(state_dir: &Path, address: Address, slot: B256) -> Result<U256> {
    let env = open_state_ro(state_dir)?;
    let snap = kardamom_state::StateSnapshot::open(&env).context("snapshot executor state")?;
    snap.storage(address, slot).context("read storage slot")
}

pub(crate) fn feed_msg(
    seq: u64,
    origin_block: u64,
    target: Address,
    data: &[u8],
    callback: Option<Callback>,
) -> OutboxMessage {
    OutboxMessage {
        origin_block_number: origin_block,
        // The watcher recomputes the anchor and rejects a feed that
        // chooses its own, so the scripted feed must serve the real one.
        origin_block_hash: Anchor {
            origin_chain_id: ORIGIN_CHAIN_ID,
            block_number: origin_block,
        }
        .hash(),
        dest_chain_id: crate::harness::DEV_CHAIN_ID.get(),
        seq,
        sender: Address::repeat_byte(0xA1),
        target,
        value: 0,
        gas_limit: 150_000,
        data: AlloyBytes::copy_from_slice(data),
        callback,
    }
}

/// Everything [`delivery`] proved that [`gap_halts_pair_not_chain`] and the
/// DA-parity scenario ([`super::xchain_da_parity`]) build on.
pub struct DeliveryOutcome {
    /// The dev-mnemonic #0 signer's next unused nonce.
    pub next_nonce: u64,
    /// Every user tx the flow submitted (the receiver deploy + the settle
    /// nudges), in nonce order — the DA-parity scenario rebuilds the
    /// canonical blocks from these plus the 0x7D receipts.
    pub user_txs: Vec<l2::SignedTransfer>,
    /// The outbox messages scripted into the feed, in seq order. The first
    /// three were delivered; seq 3 stays pending (nothing closes its origin
    /// block during the happy path).
    pub messages: Vec<kardamom_types::xchain::OutboxMessage>,
    /// The deployed receiver contract.
    pub receiver: Address,
    /// The calldata word seq 0 stored into the receiver's slot 0.
    pub payload_word: B256,
}

/// The sequencer relay counters, sampled before a delivery run so
/// [`DeliveryRun::assert_watcher_and_relay_metrics`] can check the delta
/// the run itself produced.
struct RelayBaseline {
    epochs: f64,
    msgs: f64,
}

/// One `delivery` run's state: the target, the executor's state dir, the
/// cursor file and watcher metrics address, the sender and payee, and
/// the running nonce and submitted-tx log. The steps below read and
/// update this as state instead of taking and threading it through loose
/// parameters.
struct DeliveryRun<'a> {
    t: &'a Target,
    executor_state_dir: &'a Path,
    cursor_file: &'a Path,
    watcher_metrics: SocketAddr,
    nudge: l2::NudgeSender,
    user_txs: Vec<l2::SignedTransfer>,
}

impl<'a> DeliveryRun<'a> {
    fn new(
        t: &'a Target,
        executor_state_dir: &'a Path,
        cursor_file: &'a Path,
        watcher_metrics: SocketAddr,
        sender: &DerivedSigner,
        payee: Address,
    ) -> Self {
        Self {
            t,
            executor_state_dir,
            cursor_file,
            watcher_metrics,
            nudge: l2::NudgeSender::new(sender.clone(), payee, 0),
            user_txs: Vec::new(),
        }
    }

    /// Sample the sequencer relay counters, for a later delta check.
    ///
    /// # Errors
    /// Returns an error when a scrape fails.
    async fn relay_baseline(&self) -> Result<RelayBaseline> {
        Ok(RelayBaseline {
            epochs: self
                .t
                .sequencer_metric_sum(super::SEQ_REMOTE_EPOCHS_RELAYED)
                .await?,
            msgs: self
                .t
                .sequencer_metric_sum(super::SEQ_REMOTE_MESSAGES_RELAYED)
                .await?,
        })
    }

    /// Deploy the receiver via the harness (an ordinary CREATE tx) at
    /// nonce 0, and record it. Returns the receiver's address.
    async fn deploy_receiver(&mut self) -> Result<Address> {
        let nonce = self.nudge.next_nonce()?;
        let deploy = l2::sign_create(
            self.nudge.signer(),
            self.t.chain_id,
            nonce,
            &RECEIVER_INIT_CODE,
        )?;
        self.t
            .rpc
            .send_raw(&deploy.raw)
            .await
            .result
            .map_err(|e| anyhow::anyhow!("deploy receiver: {e}"))?;
        let receipt = await_l2_receipt(self.t, deploy.hash, "the receiver deploy").await?;
        assert_receipt_ok(&receipt, "the receiver deploy")?;
        let receiver = self.nudge.signer().address.create(nonce);
        if let Some(created) = receipt_field(&receipt, "contractAddress") {
            anyhow::ensure!(
                created.eq_ignore_ascii_case(&receiver.to_string()),
                "receiver deployed at {created}, expected {receiver}"
            );
        }
        self.user_txs.push(deploy);
        Ok(receiver)
    }

    /// Commits are pipelined (depth 4) and settle at LATER tx-carrying
    /// boundaries, so nudge the chain with real transfers until the
    /// deliveries' block is durable — bounded, never a fixed sleep.
    /// Extends `user_txs` with every nudge that landed, and advances
    /// `nonce` past them.
    async fn nudge_until_settled(&mut self) -> Result<()> {
        let last_seq = std::cell::Cell::new(U256::ZERO);
        metrics::poll_until(
            "executor state to settle on Inbox.nextSeq == 3",
            Duration::from_secs(60),
            Duration::from_millis(300),
            async || self.nudge_once_if_unsettled(&last_seq).await,
        )
        .await
        .with_context(|| format!("got {}", last_seq.get()))
    }

    /// One [`Self::nudge_until_settled`] tick: `Some(())` once the
    /// executor's Inbox cursor reaches 3 (delivery settled); otherwise
    /// send one more nudge transfer (extending `user_txs` and advancing
    /// `nonce` when it lands) and ask for another tick. `last_seq` records
    /// the cursor read, for the caller's timeout message.
    async fn nudge_once_if_unsettled(
        &mut self,
        last_seq: &std::cell::Cell<U256>,
    ) -> Result<Option<()>> {
        let next_seq = read_slot(
            self.executor_state_dir,
            INBOX,
            Inbox::next_seq_slot(ORIGIN_CHAIN_ID),
        )?;
        last_seq.set(next_seq);
        if next_seq == U256::from(3) {
            return Ok(Some(()));
        }
        if let Some(tx) = self.nudge.send(&self.t.rpc, self.t.chain_id).await? {
            self.user_txs.push(tx);
        }
        Ok(None)
    }

    /// The delivered flags in `Inbox` storage, and the receiver
    /// contract's stored calldata word, via the executor's own DB.
    fn assert_contract_state(&self, receiver: Address, payload_word: B256) -> Result<()> {
        for seq in 0..3u64 {
            let status = read_slot(
                self.executor_state_dir,
                INBOX,
                Inbox::delivered_slot(ORIGIN_CHAIN_ID, seq),
            )?;
            anyhow::ensure!(
                status == U256::from(1),
                "Inbox.delivered[{ORIGIN_CHAIN_ID}][{seq}] = {status}, expected 1 (success)"
            );
        }
        let stored = read_slot(self.executor_state_dir, receiver, B256::ZERO)?;
        anyhow::ensure!(
            B256::from(stored.to_be_bytes::<32>()) == payload_word,
            "the receiver contract must hold the delivered calldata word: got {stored:#x}"
        );
        Ok(())
    }

    /// The callback's Outbox bookkeeping: lane nonce advanced, and the
    /// exact response commitment recorded. Recomputing the leaf here
    /// re-derives the response payload (selector + status + return-data
    /// hash + context) from first principles — nothing is read back from
    /// the event.
    fn assert_callback_response(&self, cb: &Callback) -> Result<()> {
        let lane_nonce = read_slot(
            self.executor_state_dir,
            OUTBOX,
            Outbox::nonces_slot(ORIGIN_CHAIN_ID),
        )?;
        anyhow::ensure!(
            lane_nonce == U256::from(1),
            "Outbox.nonces[origin] = {lane_nonce}, expected 1 (the callback response)"
        );
        let mut response_data = keccak256("onXChainResult(bool,bytes32,bytes32)")[..4].to_vec();
        let mut word = [0u8; 32];
        word[31] = 1; // success = true (an EOA call cannot revert)
        response_data.extend_from_slice(&word);
        response_data.extend_from_slice(keccak256([0u8; 0]).as_slice()); // empty return data
        response_data.extend_from_slice(cb.context.as_slice());
        // Responses never carry a callback — depth is capped at 1.
        let response_leaf = XChainMessage {
            seq: 0,
            origin_sender: INBOX,
            target: cb.target,
            value: 0,
            gas_limit: cb.gas_limit,
            input: bytes::Bytes::from(response_data),
            callback: None,
            ..Default::default()
        }
        .leaf(
            crate::harness::DEV_CHAIN_ID.get(), // the response's ORIGIN is this chain
            ORIGIN_CHAIN_ID,
        );
        let sent_slot = read_slot(
            self.executor_state_dir,
            OUTBOX,
            Outbox::sent_messages_slot(response_leaf),
        )?;
        anyhow::ensure!(
            sent_slot == U256::from(1),
            "Outbox.sentMessages[{response_leaf}] must be true — the recomputed response \
             commitment is not in the destination's Outbox"
        );
        Ok(())
    }

    /// The durable lane cursor advanced to 3 (seq 3 is still pending: its
    /// origin block never closed).
    fn assert_cursor_holds_3(&self) -> Result<()> {
        let cursor = std::fs::read_to_string(self.cursor_file)
            .with_context(|| format!("read cursor file {}", self.cursor_file.display()))?;
        anyhow::ensure!(
            cursor.trim() == "3",
            "lane cursor file must hold 3 (one past the last published seq), got {cursor:?}"
        );
        Ok(())
    }

    /// The watcher's published-record count, and the sequencer relay
    /// counters, moved by the expected amount since `baseline`.
    async fn assert_watcher_and_relay_metrics(&self, baseline: &RelayBaseline) -> Result<()> {
        let scrape = metrics::scrape(self.watcher_metrics).await?;
        let published = scrape
            .value(kardamom_da_watcher::metrics::REMOTE_EPOCHS_PUBLISHED_TOTAL)
            .context("watcher remote_epochs_published_total absent")?;
        #[allow(
            clippy::float_cmp,
            reason = "exact equality is the intended check: a counter records an exact integer \
                       count, so it scrapes back bit-identical to the literal 2"
        )]
        let published_two = published == 2.0;
        anyhow::ensure!(
            published_two,
            "watcher published {published} records, expected 2"
        );
        let relayed_epochs = self
            .t
            .sequencer_metric_sum(super::SEQ_REMOTE_EPOCHS_RELAYED)
            .await?;
        let relayed_msgs = self
            .t
            .sequencer_metric_sum(super::SEQ_REMOTE_MESSAGES_RELAYED)
            .await?;
        anyhow::ensure!(
            relayed_epochs >= baseline.epochs + 2.0,
            "sequencer relay must have moved ≥2 remote epochs (was {}, now {relayed_epochs})",
            baseline.epochs
        );
        anyhow::ensure!(
            relayed_msgs >= baseline.msgs + 3.0,
            "sequencer relay must have moved ≥3 remote messages (was {}, now {relayed_msgs})",
            baseline.msgs
        );
        Ok(())
    }
}

/// What [`script_origin_messages`] scripted: the payload word the
/// receiver-bound message carries, the callback requested on the third
/// message, and every message pushed to the feed.
struct OriginScript {
    payload_word: B256,
    cb: Callback,
    messages: Vec<OutboxMessage>,
}

/// Script the origin chain: 3 messages across 2 origin blocks, plus a
/// sentinel in a third block (see the field comment on the returned
/// message list).
fn script_origin_messages(
    feed: &MockInteropFeed,
    receiver: Address,
    payee: Address,
) -> OriginScript {
    let payload_word = B256::repeat_byte(0xA5);
    let cb = Callback {
        target: Address::repeat_byte(0xCB), // a contract on the ORIGIN chain
        gas_limit: 90_000,
        context: B256::repeat_byte(0x42),
    };
    let messages = vec![
        feed_msg(0, 100, receiver, payload_word.as_slice(), None),
        feed_msg(1, 100, payee, &[0xDE, 0xAD], None),
        feed_msg(2, 101, payee, &[0xBE, 0xEF], Some(cb)),
        // Sentinel in a THIRD origin block: an origin block is only known
        // complete when a later one appears, so this is what closes block
        // 101. It stays pending itself (nothing closes 102), and is
        // delivered by the gap arm.
        feed_msg(3, 102, payee, &[0x03], None),
    ];
    messages.iter().cloned().for_each(|m| feed.push_message(m));
    OriginScript {
        payload_word,
        cb,
        messages,
    }
}

/// Await and check the 0x7D delivery receipt for `origin`'s message
/// `seq`: status ok, fee-free execution, called through the Inbox
/// predeploy, sent from the aliased origin Outbox. `what` names the
/// delivery, for every check's error message.
///
/// # Errors
/// Returns an error when the receipt never lands, when it reverted, or
/// when it does not match the fee, `to`, or `from` a delivery must carry.
pub(crate) async fn assert_delivery_receipt(
    t: &Target,
    origin: u64,
    seq: u64,
    what: &str,
) -> Result<serde_json::Value> {
    let source_hash = remote_source_hash(origin, seq);
    let r = await_l2_receipt(t, source_hash, what).await?;
    assert_receipt_ok(&r, what)?;
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
        from.eq_ignore_ascii_case(&xchain_tx_sender(origin).to_string()),
        "delivery sender must be the aliased origin Outbox, got {from}"
    );
    Ok(r)
}

/// The three 0x7D receipts, keyed by `remote_source_hash`, and the
/// grouping check: one origin block = one atomic record = one
/// destination block, in seq order — the deposit-epoch grouping
/// property, on the interop path.
async fn await_delivery_receipts(t: &Target) -> Result<Vec<serde_json::Value>> {
    let mut receipts = Vec::new();
    for seq in 0..3u64 {
        let r = assert_delivery_receipt(
            t,
            ORIGIN_CHAIN_ID,
            seq,
            &format!("xchain delivery seq {seq}"),
        )
        .await?;
        receipts.push(r);
    }

    let p0 = receipt_placement(&receipts[0])?;
    let p1 = receipt_placement(&receipts[1])?;
    let p2 = receipt_placement(&receipts[2])?;
    let (b0, i0) = (p0.block, p0.index);
    let (b1, i1) = (p1.block, p1.index);
    let b2 = p2.block;
    anyhow::ensure!(
        b0 == b1 && i1.checked_sub(i0) == Some(1),
        "origin block 100's two messages must land contiguously in one L2 block \
         (got block {b0} idx {i0} and block {b1} idx {i1})"
    );
    anyhow::ensure!(
        b2 > b0,
        "origin block 101's message must open a LATER L2 block (got {b2} vs {b0})"
    );
    Ok(receipts)
}

/// The `MessageDelivered` logs on every receipt, and the callback's
/// `MessageSent`.
fn assert_delivery_logs(receipts: &[serde_json::Value]) -> Result<()> {
    for (seq, r) in receipts.iter().enumerate() {
        let logs = r
            .get("logs")
            .and_then(|l| l.as_array())
            .context("no logs")?;
        let delivered_log = logs
            .iter()
            .find(|l| log_topic0(l) == Some(message_delivered_topic0()))
            .with_context(|| format!("seq {seq}: no MessageDelivered log: {r}"))?;
        anyhow::ensure!(
            log_address(delivered_log).is_some_and(|a| a.eq_ignore_ascii_case(&INBOX.to_string())),
            "MessageDelivered must come from the Inbox predeploy"
        );
        anyhow::ensure!(
            log_topic(delivered_log, 1) == Some(u64_word(ORIGIN_CHAIN_ID))
                && log_topic(delivered_log, 2) == Some(u64_word(seq as u64)),
            "MessageDelivered topics must carry (origin, seq): {delivered_log}"
        );
    }
    let logs2 = receipts[2]
        .get("logs")
        .and_then(|l| l.as_array())
        .context("no logs")?;
    let sent = logs2
        .iter()
        .find(|l| log_topic0(l) == Some(Outbox::message_sent_topic0()))
        .with_context(|| format!("callback: no MessageSent log: {}", receipts[2]))?;
    anyhow::ensure!(
        log_address(sent).is_some_and(|a| a.eq_ignore_ascii_case(&OUTBOX.to_string())),
        "the callback response must be enqueued through the destination's OWN Outbox"
    );
    anyhow::ensure!(
        log_topic(sent, 1) == Some(u64_word(ORIGIN_CHAIN_ID)) // destChainId of the response
            && log_topic(sent, 2) == Some(u64_word(0)) // seq 0 of the return lane
            && log_topic(sent, 3) == Some(address_word(INBOX)),
        "MessageSent topics must be (dest=origin, seq=0, sender=Inbox): {sent}"
    );
    Ok(())
}

/// Read a durable lane cursor file, and require it be at least `min`.
/// `what` names what the cursor tracks, for the error message.
///
/// # Errors
/// Returns an error when the file cannot be read or parsed, or when the
/// cursor is below `min`.
pub(crate) fn assert_cursor_at_least(path: &Path, min: u64, what: &str) -> Result<()> {
    let cursor = std::fs::read_to_string(path)
        .with_context(|| format!("read {what} cursor {}", path.display()))?;
    let cursor_seq: u64 = cursor
        .trim()
        .parse()
        .with_context(|| format!("parse {what} cursor {cursor:?}"))?;
    anyhow::ensure!(
        cursor_seq >= min,
        "{what} cursor must be >= {min}, got {cursor_seq}"
    );
    Ok(())
}

/// The happy path: 3 messages across 2 origin blocks, delivered through the
/// full stack, with every layer's evidence asserted.
///
/// # Errors
/// Returns an error at any of the checks the module docs describe: the
/// receiver deploy, the delivery receipts and their logs, the executor's
/// contract state, the callback's Outbox bookkeeping, the durable
/// cursor, or the watcher and sequencer relay metrics.
pub async fn delivery(
    t: &Target,
    feed: &MockInteropFeed,
    executor_state_dir: &Path,
    cursor_file: &Path,
    watcher_metrics: SocketAddr,
) -> Result<DeliveryOutcome> {
    let signers = l2::dev_signers_total(2)?;
    let sender = &signers[0];
    let payee = signers[1].address;
    let mut run = DeliveryRun::new(
        t,
        executor_state_dir,
        cursor_file,
        watcher_metrics,
        sender,
        payee,
    );

    let receiver = run.deploy_receiver().await?;

    let baseline = run.relay_baseline().await?;

    let script = script_origin_messages(feed, receiver, payee);

    let receipts = await_delivery_receipts(t).await?;
    assert_delivery_logs(&receipts)?;

    run.nudge_until_settled().await?;
    run.assert_contract_state(receiver, script.payload_word)?;
    run.assert_callback_response(&script.cb)?;
    run.assert_cursor_holds_3()?;
    run.assert_watcher_and_relay_metrics(&baseline).await?;

    Ok(DeliveryOutcome {
        next_nonce: run.nudge.nonce(),
        user_txs: run.user_txs,
        messages: script.messages,
        receiver,
        payload_word: script.payload_word,
    })
}

pub(crate) fn log_address(log: &serde_json::Value) -> Option<&str> {
    log.get("address").and_then(|a| a.as_str())
}

pub(crate) fn log_topic0(log: &serde_json::Value) -> Option<B256> {
    log_topic(log, 0)
}

pub(crate) fn log_topic(log: &serde_json::Value, i: usize) -> Option<B256> {
    log.get("topics")?
        .as_array()?
        .get(i)?
        .as_str()?
        .parse()
        .ok()
}
