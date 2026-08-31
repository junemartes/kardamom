//! Origin-side outbox extraction (spec §5): decode `MessageSent` logs from
//! the Outbox predeploy out of this validator's RE-EXECUTED receipts,
//! recompute-and-reject the event-carried commitment, and cross-check each
//! send against the executor's BAL claim for the `sentMessages` slot.
//!
//! The recompute discipline mirrors `decode_message_passed`
//! (`kardamom-types::withdrawals`): the leaf is rebuilt from the DECODED
//! fields via the shared [`msg_leaf`] rule and compared against the
//! event-carried `msgHash` — event data is never trusted, so predeploy/
//! bytecode drift (the runtime bytecode is duplicated by hand in
//! `chains/dev-interop.toml`) is caught at extraction instead of shipped to
//! peers. Unlike the withdrawal decoder, a mismatch here is a FAULT rather
//! than a skip: silently dropping a send would punch a hole in the pair's
//! dense seq and strand every message behind it, so the loud outcome is a
//! divergence halt.
//!
//! The BAL cross-check ties the event to STATE: `sentMessages[msgHash]`
//! (slot 1 of the Outbox layout) must be claimed `true` by the executor at
//! exactly this tx's access index. A send the executor did not claim — or a
//! claim without a matching event — means the two views of the block
//! diverged: halt, the `write_set_eq` posture.

use alloy_primitives::{Address, B256, U256, keccak256};
use alloy_sol_types::{SolEvent, sol};

use kardamom_types::xchain::{Callback, OUTBOX, OutboxMessage, msg_leaf};
use kardamom_types::{Receipt, WireLog};

use crate::parallel::ClaimIndex;

sol! {
    /// `XChain.Callback` as it appears in the event ABI.
    struct SolCallback {
        address target;
        uint64 gasLimit;
        bytes32 context;
    }

    /// `Outbox.MessageSent` — must stay signature-identical to
    /// `contracts/src/L2/Outbox.sol` (pinned by a topic0 test below).
    event MessageSent(
        uint64 indexed destChainId,
        uint64 indexed seq,
        address indexed sender,
        address target,
        uint256 value,
        uint64 gasLimit,
        bytes data,
        bytes32 msgHash,
        SolCallback callback
    );
}

/// Storage slot index of `mapping(bytes32 => bool) sentMessages` in the
/// Outbox predeploy — the SECOND declared field (`nonces` is slot 0).
pub const SENT_MESSAGES_SLOT_INDEX: u64 = 1;

/// The storage slot holding `sentMessages[msg_hash]`:
/// `keccak256(msgHash ‖ uint256(SENT_MESSAGES_SLOT_INDEX))` — the Solidity
/// mapping rule. Pinned against forge-computed vectors (`cast index`) below.
pub fn sent_messages_slot(msg_hash: B256) -> B256 {
    let mut buf = [0u8; 64];
    buf[0..32].copy_from_slice(msg_hash.as_slice());
    buf[32..64].copy_from_slice(&U256::from(SENT_MESSAGES_SLOT_INDEX).to_be_bytes::<32>());
    keccak256(buf)
}

/// Deterministic anchor for one origin block, served as the feed's
/// `originBlockHash`.
///
/// Kardamom blocks carry NO canonical hash in v0 — the sealed
/// `BlockBoundary` is slim (no state commitment) and the RPC returns
/// `blockHash: null` — so this is a position commitment, not a content one.
/// What matters is DETERMINISM: every validator of the same chain serves the
/// identical anchor for a block, so racing relayers derive byte-identical
/// `RemoteEpochRecord`s and `canonical_id` dedup collapses them. Content
/// authenticity is §10's job (re-derivation / attestation quorum), never
/// this field's; the destination treats it as opaque.
pub fn xchain_anchor_hash(origin_chain_id: u64, block_number: u64) -> B256 {
    let mut buf = Vec::with_capacity(25 + 16);
    buf.extend_from_slice(b"KARDAMOM_XCHAIN_ANCHOR_V0");
    buf.extend_from_slice(&origin_chain_id.to_be_bytes());
    buf.extend_from_slice(&block_number.to_be_bytes());
    keccak256(&buf)
}

/// Why extraction failed. Every variant is a chain-level fault: the receipts
/// are this validator's OWN re-execution (already cross-checked against the
/// published receipts), so a malformed or unclaimed send means the predeploy
/// or the executor's claims diverged from them — halt, never skip.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum OutboxExtractError {
    /// A log from the Outbox address carries the MessageSent topic but does
    /// not ABI-decode.
    #[error("block {block} tx {tx_index}: MessageSent log does not decode: {detail}")]
    Undecodable {
        block: u64,
        tx_index: u64,
        detail: String,
    },
    /// The recomputed message leaf differs from the event-carried `msgHash` —
    /// predeploy/bytecode drift, or a contract at the predeploy address that
    /// is not the Outbox.
    #[error(
        "block {block} tx {tx_index} (dest {dest}, seq {seq}): recomputed msg leaf {computed} \
         != event-carried {carried} — event data drifted from the shared hashing rule"
    )]
    LeafMismatch {
        block: u64,
        tx_index: u64,
        dest: u64,
        seq: u64,
        computed: B256,
        carried: B256,
    },
    /// The BAL claim for the `sentMessages` slot is absent or wrong at this
    /// tx's access index — the executor's claimed state does not contain the
    /// send this validator executed.
    #[error(
        "block {block} tx {tx_index} (dest {dest}, seq {seq}): sentMessages slot {slot} \
         not claimed true at claim index {claim_index}: {detail}"
    )]
    ClaimMismatch {
        block: u64,
        tx_index: u64,
        dest: u64,
        seq: u64,
        slot: B256,
        claim_index: u64,
        detail: String,
    },
}

/// Extract the outbox messages sent in one block from its (re-executed)
/// receipts, cross-checking each against the executor's BAL claims when
/// available.
///
/// - `origin_chain_id` — THIS chain (an input to the leaf recompute).
/// - `receipts` — the block's receipts in block order. Only successful
///   receipts emit logs, so failed sends are naturally absent.
/// - `claims` — the block's decoded BAL claim index with its wire
///   granularity, when the frame arrived. `None` skips the state cross-check
///   (the caller counts it — the `bal_missing` posture; the messages are
///   still this validator's own re-execution).
///
/// Returns the messages in block order (== per-destination seq order, since
/// the Outbox's per-destination counter is dense and monotone within a
/// block).
pub fn collect_outbox_messages(
    origin_chain_id: u64,
    block_number: u64,
    receipts: &[Receipt],
    claims: Option<(u16, &ClaimIndex)>,
) -> Result<Vec<OutboxMessage>, OutboxExtractError> {
    let mut out = Vec::new();
    for receipt in receipts {
        for log in &receipt.logs {
            if let Some(msg) = decode_message_sent(
                origin_chain_id,
                block_number,
                receipt.transaction_index,
                log,
            )? {
                if let Some((granularity, idx)) = claims {
                    cross_check_claim(
                        block_number,
                        receipt.transaction_index,
                        &msg,
                        granularity,
                        idx,
                    )?;
                }
                out.push(msg.message);
            }
        }
    }
    Ok(out)
}

/// One decoded send plus the fields the claim cross-check needs.
struct DecodedSend {
    message: OutboxMessage,
    msg_hash: B256,
}

/// Decode one log if it is a `MessageSent` from the Outbox predeploy.
/// `Ok(None)` = not ours (foreign address or topic — skip); `Err` = it IS a
/// MessageSent-shaped Outbox log but malformed or drifted (fault).
fn decode_message_sent(
    origin_chain_id: u64,
    block: u64,
    tx_index: u64,
    log: &WireLog,
) -> Result<Option<DecodedSend>, OutboxExtractError> {
    if log.address != OUTBOX {
        return Ok(None);
    }
    if log.topics.first() != Some(&MessageSent::SIGNATURE_HASH) {
        return Ok(None);
    }
    let decoded = MessageSent::decode_raw_log(log.topics.iter().copied(), log.data.as_ref())
        .map_err(|e| OutboxExtractError::Undecodable {
            block,
            tx_index,
            detail: e.to_string(),
        })?;

    let value = u128::try_from(decoded.value).map_err(|_| OutboxExtractError::Undecodable {
        block,
        tx_index,
        detail: format!("value {} exceeds the protocol's u128", decoded.value),
    })?;
    let callback = {
        let cb = &decoded.callback;
        if cb.target == Address::ZERO && cb.gasLimit == 0 && cb.context == B256::ZERO {
            None
        } else {
            Some(Callback {
                target: cb.target,
                gas_limit: cb.gasLimit,
                context: cb.context,
            })
        }
    };

    // Recompute-and-reject: the commitment is rebuilt from the decoded
    // fields through the shared rule; the event-carried hash is only ever
    // COMPARED, never propagated.
    let computed = msg_leaf(
        origin_chain_id,
        decoded.destChainId,
        decoded.seq,
        decoded.sender,
        decoded.target,
        value,
        decoded.gasLimit,
        keccak256(&decoded.data),
        callback
            .as_ref()
            .map(Callback::commitment)
            .unwrap_or_else(kardamom_types::xchain::no_callback_hash),
    );
    if computed != decoded.msgHash {
        return Err(OutboxExtractError::LeafMismatch {
            block,
            tx_index,
            dest: decoded.destChainId,
            seq: decoded.seq,
            computed,
            carried: decoded.msgHash,
        });
    }

    Ok(Some(DecodedSend {
        message: OutboxMessage {
            origin_block_number: block,
            origin_block_hash: xchain_anchor_hash(origin_chain_id, block),
            dest_chain_id: decoded.destChainId,
            seq: decoded.seq,
            sender: decoded.sender,
            target: decoded.target,
            value,
            gas_limit: decoded.gasLimit,
            data: decoded.data.clone(),
            callback,
        },
        msg_hash: computed,
    }))
}

/// The state tie: `sentMessages[msgHash]` must be claimed `true` by the
/// executor at exactly this tx's access index (chunk ordinal at wire
/// granularity K > 1 — the validator's ladder view always follows the wire).
fn cross_check_claim(
    block: u64,
    tx_index: u64,
    send: &DecodedSend,
    granularity: u16,
    claims: &ClaimIndex,
) -> Result<(), OutboxExtractError> {
    let slot = sent_messages_slot(send.msg_hash);
    let bal_index = tx_index + 1;
    let claim_index = if granularity > 1 {
        kardamom_engine::bal_ladder::chunk_of(bal_index, u64::from(granularity))
    } else {
        bal_index
    };
    let mismatch = |detail: String| OutboxExtractError::ClaimMismatch {
        block,
        tx_index,
        dest: send.message.dest_chain_id,
        seq: send.message.seq,
        slot,
        claim_index,
        detail,
    };
    let Some(writes) = claims.storage.get(&(OUTBOX, slot)) else {
        return Err(mismatch("no claim for the slot at all".into()));
    };
    let Some((_, claimed)) = writes.iter().find(|(i, _)| *i == claim_index) else {
        return Err(mismatch(format!(
            "slot claimed only at indices {:?}",
            writes.iter().map(|(i, _)| *i).collect::<Vec<_>>()
        )));
    };
    if *claimed != U256::ONE {
        return Err(mismatch(format!(
            "claimed post-value {claimed}, expected 1"
        )));
    }
    Ok(())
}

/// Test fixtures shared with the sink tests: honest `MessageSent` logs built
/// through alloy's ABI encoder (byte-parity with Solidity).
#[cfg(test)]
pub(crate) mod tests_support {
    use super::*;
    use alloy_primitives::Bytes as AlloyBytes;

    /// Build a MessageSent WireLog exactly as the predeploy emits it: the
    /// carried msgHash computed through the SAME shared rule (an honest
    /// contract).
    pub(crate) fn honest_sent_log_full(
        origin: u64,
        dest: u64,
        seq: u64,
        data: &[u8],
        callback: Option<Callback>,
    ) -> WireLog {
        let sender = Address::repeat_byte(0xA1);
        let target = Address::repeat_byte(0xB2);
        let cb_hash = callback
            .as_ref()
            .map(Callback::commitment)
            .unwrap_or(B256::ZERO);
        let msg_hash = msg_leaf(
            origin,
            dest,
            seq,
            sender,
            target,
            0,
            200_000,
            keccak256(data),
            cb_hash,
        );
        let ev = MessageSent {
            destChainId: dest,
            seq,
            sender,
            target,
            value: U256::ZERO,
            gasLimit: 200_000,
            data: AlloyBytes::copy_from_slice(data),
            msgHash: msg_hash,
            callback: match callback {
                Some(cb) => SolCallback {
                    target: cb.target,
                    gasLimit: cb.gas_limit,
                    context: cb.context,
                },
                None => SolCallback {
                    target: Address::ZERO,
                    gasLimit: 0,
                    context: B256::ZERO,
                },
            },
        };
        let log_data = ev.encode_log_data();
        WireLog {
            address: OUTBOX,
            topics: log_data.topics().to_vec(),
            data: log_data.data.to_vec().into(),
        }
    }

    pub(crate) fn honest_sent_log(origin: u64, dest: u64, seq: u64, data: &[u8]) -> WireLog {
        honest_sent_log_full(origin, dest, seq, data, None)
    }

    /// Decode the carried msgHash back out of a raw log (claim fixtures).
    pub(crate) fn log_msg_hash(log: &WireLog) -> B256 {
        MessageSent::decode_raw_log(log.topics.iter().copied(), log.data.as_ref())
            .unwrap()
            .msgHash
    }
}

#[cfg(test)]
mod tests {
    use super::tests_support::{honest_sent_log_full, log_msg_hash};
    use super::*;

    const SELF_CHAIN: u64 = 412_346;
    const DEST_CHAIN: u64 = 412_347;

    fn sent_log(seq: u64, data: &[u8], callback: Option<Callback>) -> WireLog {
        honest_sent_log_full(SELF_CHAIN, DEST_CHAIN, seq, data, callback)
    }

    fn receipt_with(tx_index: u64, logs: Vec<WireLog>) -> Receipt {
        Receipt {
            transaction_index: tx_index,
            logs,
            ..Default::default()
        }
    }

    /// The sol! event signature must equal the Solidity contract's — the
    /// pinned string is `Outbox.sol`'s event with the callback struct
    /// flattened to its tuple type.
    #[test]
    fn topic0_matches_the_predeploy_signature() {
        assert_eq!(
            MessageSent::SIGNATURE_HASH,
            keccak256(
                "MessageSent(uint64,uint64,address,address,uint256,uint64,bytes,bytes32,\
                 (address,uint64,bytes32))"
            )
        );
    }

    /// Solidity mapping-slot derivation pinned against forge-computed
    /// vectors (`cast index bytes32 <key> 1`).
    #[test]
    fn sent_messages_slot_matches_forge_vectors() {
        // cast index bytes32 0x1111..11 1
        assert_eq!(
            sent_messages_slot(B256::repeat_byte(0x11)),
            "0x7deb3b60ec0f1bf56dbdd0ffedbadafddeaa08947884ff0f215ce93ee1826102"
                .parse::<B256>()
                .unwrap()
        );
        // cast index bytes32 0x0df14340..4d3c 1 (the cross-language msg_leaf
        // vector from kardamom-types/Outbox.t.sol as the mapping key).
        assert_eq!(
            sent_messages_slot(
                "0x0df14340efd8c8b32f4c333c3dca8470b0bae319a3dfe32adb213df2b8834d3c"
                    .parse()
                    .unwrap()
            ),
            "0xd4b78be0c1de834d6a6db01a7ae3f433776afbc98b26f4e51412b94effd6d438"
                .parse::<B256>()
                .unwrap()
        );
    }

    #[test]
    fn collects_and_recomputes_honest_sends() {
        let cb = Callback {
            target: Address::repeat_byte(0x0C),
            gas_limit: 90_000,
            context: B256::repeat_byte(0x1D),
        };
        let receipts = vec![
            receipt_with(0, vec![sent_log(0, &[0xCA, 0xFE], None)]),
            // A foreign log among ours is skipped, not a fault.
            receipt_with(
                1,
                vec![
                    WireLog {
                        address: Address::repeat_byte(0x99),
                        topics: vec![B256::repeat_byte(0x77)],
                        data: Default::default(),
                    },
                    sent_log(1, &[], Some(cb)),
                ],
            ),
        ];
        let msgs = collect_outbox_messages(SELF_CHAIN, 42, &receipts, None).unwrap();
        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0].seq, 0);
        assert_eq!(msgs[0].dest_chain_id, DEST_CHAIN);
        assert_eq!(msgs[0].data.as_ref(), &[0xCA, 0xFE]);
        assert_eq!(msgs[0].origin_block_number, 42);
        assert_eq!(
            msgs[0].origin_block_hash,
            xchain_anchor_hash(SELF_CHAIN, 42),
            "anchor is the deterministic position commitment"
        );
        assert_eq!(msgs[1].seq, 1);
        assert_eq!(msgs[1].callback, Some(cb));
    }

    /// Recompute-and-reject: a tampered field (the event's data no longer
    /// hashing to the carried msgHash) is a FAULT, not a skip — a dropped
    /// send would hole the pair's dense seq.
    #[test]
    fn event_carried_drift_is_a_fault() {
        // Head layout: target, value, gasLimit, data-offset, msgHash,
        // callback tuple (3 words), then the dynamic data tail.
        // Both tamper directions must be caught (the decode_message_passed
        // discipline): a corrupted CARRIED hash, and a corrupted FIELD.
        let log = sent_log(0, &[0xCA, 0xFE], None);
        let mut carried = log.clone();
        let mut data = carried.data.to_vec();
        data[4 * 32] ^= 0x01; // first byte of the msgHash word
        carried.data = data.into();
        let err = collect_outbox_messages(SELF_CHAIN, 42, &[receipt_with(0, vec![carried])], None)
            .unwrap_err();
        assert!(
            matches!(err, OutboxExtractError::LeafMismatch { seq: 0, .. }),
            "{err:?}"
        );

        let mut field = log;
        let mut data = field.data.to_vec();
        let n = data.len();
        data[n - 32] ^= 0x01; // first payload byte of `data`
        field.data = data.into();
        let err = collect_outbox_messages(SELF_CHAIN, 42, &[receipt_with(0, vec![field])], None)
            .unwrap_err();
        assert!(
            matches!(err, OutboxExtractError::LeafMismatch { seq: 0, .. }),
            "{err:?}"
        );
    }

    fn claims_for(sends: &[(u64, B256)]) -> ClaimIndex {
        // (bal_index, msg_hash) → the executor's sentMessages claim.
        let mut idx = ClaimIndex::default();
        for (bal_index, msg_hash) in sends {
            idx.storage
                .entry((OUTBOX, sent_messages_slot(*msg_hash)))
                .or_default()
                .push((*bal_index, U256::ONE));
        }
        idx
    }

    fn hash_of(log: &WireLog) -> B256 {
        // The carried msgHash equals the recomputed leaf for honest logs.
        log_msg_hash(log)
    }

    #[test]
    fn claim_cross_check_passes_and_catches_mismatches() {
        let log0 = sent_log(0, &[0x01], None);
        let log1 = sent_log(1, &[0x02], None);
        let h0 = hash_of(&log0);
        let h1 = hash_of(&log1);
        let receipts = vec![
            receipt_with(0, vec![log0]),
            receipt_with(1, vec![log1.clone()]),
        ];

        // Honest claims: tx 0 -> bal index 1, tx 1 -> bal index 2.
        let claims = claims_for(&[(1, h0), (2, h1)]);
        let msgs = collect_outbox_messages(SELF_CHAIN, 7, &receipts, Some((1, &claims))).unwrap();
        assert_eq!(msgs.len(), 2);

        // Missing claim for the second send.
        let claims = claims_for(&[(1, h0)]);
        let err =
            collect_outbox_messages(SELF_CHAIN, 7, &receipts, Some((1, &claims))).unwrap_err();
        assert!(
            matches!(err, OutboxExtractError::ClaimMismatch { seq: 1, .. }),
            "{err:?}"
        );

        // Claim at the WRONG access index (attribution drift).
        let claims = claims_for(&[(1, h0), (3, h1)]);
        assert!(collect_outbox_messages(SELF_CHAIN, 7, &receipts, Some((1, &claims))).is_err());

        // Claimed false (post-value 0).
        let mut claims = claims_for(&[(1, h0), (2, h1)]);
        claims
            .storage
            .get_mut(&(OUTBOX, sent_messages_slot(h1)))
            .unwrap()[0]
            .1 = U256::ZERO;
        assert!(collect_outbox_messages(SELF_CHAIN, 7, &receipts, Some((1, &claims))).is_err());
    }

    /// At wire granularity K > 1 the claims are chunk-collapsed: the send's
    /// access index coarsens to its chunk ordinal.
    #[test]
    fn quantized_claims_check_at_the_chunk_ordinal() {
        let log = sent_log(0, &[0x01], None);
        let h = hash_of(&log);
        // tx index 25 -> bal index 26 -> chunk 2 at K=20.
        let receipts = vec![receipt_with(25, vec![log])];
        let claims = claims_for(&[(2, h)]);
        assert!(collect_outbox_messages(SELF_CHAIN, 7, &receipts, Some((20, &claims))).is_ok());
        // The per-tx index must NOT be accepted at K=20.
        let claims = claims_for(&[(26, h)]);
        assert!(collect_outbox_messages(SELF_CHAIN, 7, &receipts, Some((20, &claims))).is_err());
    }
}
