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
//! validator's responses" seam. The true two-stack e2e lands with the egress
//! node (spec §5); until then this is the strongest destination-side proof
//! available.
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
    Callback, INBOX, OUTBOX, OutboxMessage, msg_leaf, remote_source_hash, xchain_tx_sender,
};

use super::{
    Target, assert_receipt_ok, await_l2_receipt, open_state_ro, receipt_field, receipt_placement,
};
use crate::harness::l2::{self, DerivedSigner};
use crate::harness::metrics::{self, poll_until};

/// The simulated origin chain's id — anything ≠ the stack's 412346.
pub const ORIGIN_CHAIN_ID: u64 = 412_399;

/// `keccak256("MessageDelivered(uint64,uint64,bool)")` — pinned by
/// `forge inspect Inbox events`.
fn message_delivered_topic0() -> B256 {
    keccak256("MessageDelivered(uint64,uint64,bool)")
}

/// `keccak256` of the MessageSent signature (the callback struct flattens to
/// its tuple type) — pinned by `forge inspect Outbox events`.
fn message_sent_topic0() -> B256 {
    keccak256(
        "MessageSent(uint64,uint64,address,address,uint256,uint64,bytes,bytes32,(address,uint64,bytes32))",
    )
}

/// Minimal "Receiver-style" runtime, deployed via an ordinary CREATE tx
/// through the ingress: copies the first 32 bytes of calldata into storage
/// slot 0. 12 bytes of init returning 14 bytes of runtime — no compiler in
/// the loop, and the assertion (slot 0 == the payload word we sent from the
/// "origin chain") proves the delivered calldata reached the target intact.
const RECEIVER_INIT_CODE: [u8; 26] = [
    // init: CODECOPY(0, 0x0c, 0x0e); RETURN(0, 0x0e)
    0x60, 0x0e, 0x60, 0x0c, 0x60, 0x00, 0x39, 0x60, 0x0e, 0x60, 0x00, 0xf3,
    // runtime: CALLDATACOPY(0, 0, 0x20); SSTORE(0, MLOAD(0)); STOP
    0x60, 0x20, 0x60, 0x00, 0x60, 0x00, 0x37, 0x60, 0x00, 0x51, 0x60, 0x00, 0x55, 0x00,
];

/// Storage slot of `mapping(uint64 => uint64) Inbox.nextSeq` (slot 1; slot 0
/// is `delivered`).
fn inbox_next_seq_slot(origin: u64) -> B256 {
    map_slot(u64_word(origin), 1)
}

/// Storage slot of `Inbox.delivered[origin][seq]` (double mapping at slot 0).
fn inbox_delivered_slot(origin: u64, seq: u64) -> B256 {
    let inner = map_slot(u64_word(origin), 0);
    keccak256([u64_word(seq).as_slice(), inner.as_slice()].concat())
}

/// Storage slot of `mapping(uint64 => uint64) Outbox.nonces` (slot 0).
fn outbox_nonces_slot(dest: u64) -> B256 {
    map_slot(u64_word(dest), 0)
}

/// Storage slot of `mapping(bytes32 => bool) Outbox.sentMessages` (slot 1).
fn outbox_sent_messages_slot(msg_hash: B256) -> B256 {
    map_slot(msg_hash, 1)
}

fn u64_word(v: u64) -> B256 {
    let mut w = [0u8; 32];
    w[24..].copy_from_slice(&v.to_be_bytes());
    B256::from(w)
}

/// An address as a 32-byte topic word (left-padded), the indexed-address
/// encoding.
fn address_word(a: Address) -> B256 {
    let mut w = [0u8; 32];
    w[12..].copy_from_slice(a.as_slice());
    B256::from(w)
}

fn map_slot(key: B256, base_slot: u64) -> B256 {
    keccak256([key.as_slice(), u64_word(base_slot).as_slice()].concat())
}

/// One storage slot of `address`, read from the executor's live state DB
/// (MVCC read-only snapshot — the `read_validator_state_root` seam).
fn read_slot(state_dir: &Path, address: Address, slot: B256) -> Result<U256> {
    let env = open_state_ro(state_dir)?;
    let snap = kardamom_state::StateSnapshot::open(&env).context("snapshot executor state")?;
    snap.storage(address, slot).context("read storage slot")
}

fn feed_msg(
    seq: u64,
    origin_block: u64,
    target: Address,
    data: &[u8],
    callback: Option<Callback>,
) -> OutboxMessage {
    OutboxMessage {
        origin_block_number: origin_block,
        origin_block_hash: B256::repeat_byte(origin_block as u8),
        dest_chain_id: crate::harness::DEV_CHAIN_ID,
        seq,
        sender: Address::repeat_byte(0xA1),
        target,
        value: 0,
        gas_limit: 150_000,
        data: AlloyBytes::copy_from_slice(data),
        callback,
    }
}

/// Everything [`delivery`] proved that [`gap_halts_pair_not_chain`] builds on.
pub struct DeliveryOutcome {
    /// The dev-mnemonic #0 signer's next unused nonce.
    pub next_nonce: u64,
}

/// The happy path: 3 messages across 2 origin blocks, delivered through the
/// full stack, with every layer's evidence asserted.
pub async fn delivery(
    t: &Target,
    feed: &MockInteropFeed,
    executor_state_dir: &Path,
    cursor_file: &Path,
    watcher_metrics: SocketAddr,
) -> Result<DeliveryOutcome> {
    let signers = l2::dev_signers(2)?;
    let sender = &signers[0];
    let mut nonce = 0u64;

    // --- Deploy the receiver via the harness (an ordinary CREATE tx). -----
    let deploy = l2::sign_create(sender, t.chain_id, nonce, &RECEIVER_INIT_CODE)?;
    nonce += 1;
    t.rpc
        .send_raw(&deploy.raw)
        .await
        .result
        .map_err(|e| anyhow::anyhow!("deploy receiver: {e}"))?;
    let receipt = await_l2_receipt(t, deploy.hash, "the receiver deploy").await?;
    assert_receipt_ok(&receipt, "the receiver deploy")?;
    let receiver = sender.address.create(0);
    if let Some(created) = receipt_field(&receipt, "contractAddress") {
        anyhow::ensure!(
            created.eq_ignore_ascii_case(&receiver.to_string()),
            "receiver deployed at {created}, expected {receiver}"
        );
    }

    let relayed_epochs_before = t
        .sequencer_metric_sum(super::SEQ_REMOTE_EPOCHS_RELAYED)
        .await?;
    let relayed_msgs_before = t
        .sequencer_metric_sum(super::SEQ_REMOTE_MESSAGES_RELAYED)
        .await?;

    // --- Script the origin chain: 3 messages across 2 origin blocks. ------
    let payload_word = B256::repeat_byte(0xA5);
    let cb = Callback {
        target: Address::repeat_byte(0xCB), // a contract on the ORIGIN chain
        gas_limit: 90_000,
        context: B256::repeat_byte(0x42),
    };
    let payee = signers[1].address;
    feed.push_message(feed_msg(0, 100, receiver, payload_word.as_slice(), None));
    feed.push_message(feed_msg(1, 100, payee, &[0xDE, 0xAD], None));
    feed.push_message(feed_msg(2, 101, payee, &[0xBE, 0xEF], Some(cb)));
    // Sentinel in a THIRD origin block: an origin block is only known
    // complete when a later one appears, so this is what closes block 101.
    // It stays pending itself (nothing closes 102), and is delivered by the
    // gap arm.
    feed.push_message(feed_msg(3, 102, payee, &[0x03], None));

    // --- The three 0x7D receipts, keyed by remote_source_hash. ------------
    let mut receipts = Vec::new();
    for seq in 0..3u64 {
        let source_hash = remote_source_hash(ORIGIN_CHAIN_ID, seq);
        let r = await_l2_receipt(t, source_hash, &format!("xchain delivery seq {seq}")).await?;
        assert_receipt_ok(&r, &format!("xchain delivery seq {seq}"))?;
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
            from.eq_ignore_ascii_case(&xchain_tx_sender(ORIGIN_CHAIN_ID).to_string()),
            "delivery sender must be the aliased origin Outbox, got {from}"
        );
        receipts.push(r);
    }

    // One origin block = one atomic record = one destination block, in seq
    // order — the deposit-epoch grouping property, on the interop path.
    let (b0, i0) = receipt_placement(&receipts[0])?;
    let (b1, i1) = receipt_placement(&receipts[1])?;
    let (b2, _) = receipt_placement(&receipts[2])?;
    anyhow::ensure!(
        b0 == b1 && i1 == i0 + 1,
        "origin block 100's two messages must land contiguously in one L2 block \
         (got block {b0} idx {i0} and block {b1} idx {i1})"
    );
    anyhow::ensure!(
        b2 > b0,
        "origin block 101's message must open a LATER L2 block (got {b2} vs {b0})"
    );

    // --- MessageDelivered logs, and the callback's MessageSent. -----------
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
        .find(|l| log_topic0(l) == Some(message_sent_topic0()))
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

    // --- Contract state, via the executor's own DB. -----------------------
    // Commits are pipelined (depth 4) and settle at LATER tx-carrying
    // boundaries, so nudge the chain with real transfers until the
    // deliveries' block is durable — bounded, never a fixed sleep.
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        let next_seq = read_slot(
            executor_state_dir,
            INBOX,
            inbox_next_seq_slot(ORIGIN_CHAIN_ID),
        )?;
        if next_seq == U256::from(3) {
            break;
        }
        anyhow::ensure!(
            std::time::Instant::now() < deadline,
            "executor state never settled on Inbox.nextSeq == 3 (got {next_seq})"
        );
        let nudge = l2::sign_transfer(sender, t.chain_id, nonce, payee, 1)?;
        if t.rpc.send_raw(&nudge.raw).await.result.is_ok() {
            nonce += 1;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    for seq in 0..3u64 {
        let status = read_slot(
            executor_state_dir,
            INBOX,
            inbox_delivered_slot(ORIGIN_CHAIN_ID, seq),
        )?;
        anyhow::ensure!(
            status == U256::from(1),
            "Inbox.delivered[{ORIGIN_CHAIN_ID}][{seq}] = {status}, expected 1 (success)"
        );
    }
    let stored = read_slot(executor_state_dir, receiver, B256::ZERO)?;
    anyhow::ensure!(
        B256::from(stored.to_be_bytes::<32>()) == payload_word,
        "the receiver contract must hold the delivered calldata word: got {stored:#x}"
    );

    // The callback's Outbox bookkeeping: lane nonce advanced, and the exact
    // response commitment recorded. Recomputing the leaf here re-derives the
    // response payload (selector + status + return-data hash + context) from
    // first principles — nothing is read back from the event.
    let lane_nonce = read_slot(
        executor_state_dir,
        OUTBOX,
        outbox_nonces_slot(ORIGIN_CHAIN_ID),
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
    let response_leaf = msg_leaf(
        crate::harness::DEV_CHAIN_ID, // the response's ORIGIN is this chain
        ORIGIN_CHAIN_ID,
        0,
        INBOX,
        cb.target,
        0,
        cb.gas_limit,
        keccak256(&response_data),
        B256::ZERO, // responses never carry a callback — depth is capped at 1
    );
    let sent_slot = read_slot(
        executor_state_dir,
        OUTBOX,
        outbox_sent_messages_slot(response_leaf),
    )?;
    anyhow::ensure!(
        sent_slot == U256::from(1),
        "Outbox.sentMessages[{response_leaf}] must be true — the recomputed response \
         commitment is not in the destination's Outbox"
    );

    // --- The durable lane cursor advanced to 3 (seq 3 is still pending: ---
    // its origin block never closed).
    let cursor = std::fs::read_to_string(cursor_file)
        .with_context(|| format!("read cursor file {}", cursor_file.display()))?;
    anyhow::ensure!(
        cursor.trim() == "3",
        "lane cursor file must hold 3 (one past the last published seq), got {cursor:?}"
    );

    // --- Watcher + sequencer relay metrics moved. -------------------------
    let scrape = metrics::scrape(watcher_metrics).await?;
    let published = scrape
        .value(kardamom_da_watcher::metrics::REMOTE_EPOCHS_PUBLISHED_TOTAL)
        .context("watcher remote_epochs_published_total absent")?;
    anyhow::ensure!(
        published == 2.0,
        "watcher published {published} records, expected 2"
    );
    let relayed_epochs = t
        .sequencer_metric_sum(super::SEQ_REMOTE_EPOCHS_RELAYED)
        .await?;
    let relayed_msgs = t
        .sequencer_metric_sum(super::SEQ_REMOTE_MESSAGES_RELAYED)
        .await?;
    anyhow::ensure!(
        relayed_epochs >= relayed_epochs_before + 2.0,
        "sequencer relay must have moved ≥2 remote epochs (was {relayed_epochs_before}, now {relayed_epochs})"
    );
    anyhow::ensure!(
        relayed_msgs >= relayed_msgs_before + 3.0,
        "sequencer relay must have moved ≥3 remote messages (was {relayed_msgs_before}, now {relayed_msgs})"
    );

    Ok(DeliveryOutcome { next_nonce: nonce })
}

/// The adversarial arm: the feed swallows one seq. The watcher must
/// fail-stop — the PROCESS exits nonzero, nothing is skipped — while the
/// chain itself keeps sealing blocks. Pair-scoped fault domain, proven.
pub async fn gap_halts_pair_not_chain(
    t: &Target,
    feed: &MockInteropFeed,
    watcher: &mut crate::harness::services::Spawned,
    outcome: DeliveryOutcome,
) -> Result<()> {
    let signers = l2::dev_signers(2)?;
    let sender: &DerivedSigner = &signers[0];
    let payee = signers[1].address;
    let mut nonce = outcome.next_nonce;

    // seq 4 is swallowed by the feed — the hole. seq 5 (block 104) closes
    // block 102, delivering the pending seq 3; seq 6 (block 105) closes
    // block 104, whose batch starts at 5 while the lane cursor says 4:
    // SeqSkipped, the terminal pair fault.
    feed.gap_next(1);
    feed.push_message(feed_msg(4, 103, payee, &[0x04], None));
    feed.push_message(feed_msg(5, 104, payee, &[0x05], None));
    feed.push_message(feed_msg(6, 105, payee, &[0x06], None));

    // The interop path runs ALONE in this process, so the pair fault is a
    // process exit — and it must be NONZERO: a fail-stop that looks like a
    // clean shutdown would hide the halt from the orchestrator.
    let exit = watcher
        .proc
        .wait_exit(Duration::from_secs(60))
        .context("watcher did not exit after the seq gap — it skipped or stalled")?;
    anyhow::ensure!(
        exit.is_some_and(|code| code != 0),
        "watcher must exit NONZERO on a derivation fault, got {exit:?}"
    );
    let log = std::fs::read_to_string(&watcher.proc.log_path).unwrap_or_default();
    anyhow::ensure!(
        log.contains("remote epoch derivation fault"),
        "watcher log must name the derivation fault; tail:\n{}",
        watcher.proc.log_tail(15)
    );

    // seq 3 (pending from the happy path) was delivered on the way to the
    // fault; the swallowed seq 4 and everything after it must NEVER execute.
    await_l2_receipt(
        t,
        remote_source_hash(ORIGIN_CHAIN_ID, 3),
        "xchain delivery seq 3",
    )
    .await?;

    // THE CHAIN KEEPS SEALING. New ordinary transactions land in new blocks
    // after the watcher's death — the fault stopped one pair, not the chain.
    let head_at_halt = t.executor_metric(super::EXEC_BLOCK_NUMBER).await?;
    for _ in 0..2 {
        let tx = l2::sign_transfer(sender, t.chain_id, nonce, payee, 1)?;
        nonce += 1;
        t.rpc
            .send_raw(&tx.raw)
            .await
            .result
            .map_err(|e| anyhow::anyhow!("post-halt transfer rejected: {e}"))?;
        let r = await_l2_receipt(t, tx.hash, "a post-halt transfer").await?;
        assert_receipt_ok(&r, "a post-halt transfer")?;
    }
    poll_until(
        "the head to advance past the halt",
        Duration::from_secs(30),
        Duration::from_millis(250),
        || async {
            let head = t.executor_metric(super::EXEC_BLOCK_NUMBER).await?;
            Ok((head > head_at_halt).then_some(head))
        },
    )
    .await?;

    // And the hole was never stepped over: the swallowed seq has no receipt.
    let gone = t
        .rpc
        .receipt(remote_source_hash(ORIGIN_CHAIN_ID, 4))
        .await
        .result
        .map_err(|e| anyhow::anyhow!("receipt probe for the swallowed seq: {e}"))?;
    anyhow::ensure!(
        gone.is_none(),
        "seq 4 was swallowed by the feed and must never execute: {gone:?}"
    );
    Ok(())
}

fn log_address(log: &serde_json::Value) -> Option<&str> {
    log.get("address").and_then(|a| a.as_str())
}

fn log_topic0(log: &serde_json::Value) -> Option<B256> {
    log_topic(log, 0)
}

fn log_topic(log: &serde_json::Value, i: usize) -> Option<B256> {
    log.get("topics")?
        .as_array()?
        .get(i)?
        .as_str()?
        .parse()
        .ok()
}
