//! Storage layout and ABI pins of the Outbox and Inbox predeploys.
//!
//! The validator's outbox extraction, the e2e scenarios, and the contracts
//! share these values. This is the one Rust copy. The tests pin each value
//! against `forge inspect` and `cast index` output. The Solidity fields are
//! append-only: a field inserted before `sentMessages` shifts the slot
//! index, and every validator halts with `ClaimMismatch` on the first send.

use alloy_primitives::{B256, keccak256};

/// Storage slot index of `mapping(uint64 => uint64) nonces` in the Outbox
/// predeploy, the FIRST declared field.
pub const OUTBOX_NONCES_SLOT_INDEX: u64 = 0;

/// Storage slot index of `mapping(bytes32 => bool) sentMessages` in the
/// Outbox predeploy, the SECOND declared field.
pub const SENT_MESSAGES_SLOT_INDEX: u64 = 1;

/// Storage slot index of `mapping(uint64 => mapping(uint64 => uint8))
/// delivered` in the Inbox predeploy, the FIRST declared field.
pub const INBOX_DELIVERED_SLOT_INDEX: u64 = 0;

/// Storage slot index of `mapping(uint64 => uint64) nextSeq` in the Inbox
/// predeploy, the SECOND declared field.
pub const INBOX_NEXT_SEQ_SLOT_INDEX: u64 = 1;

/// Solidity signature of `Outbox.sendMessage`. The callback struct flattens
/// to its tuple type, as in [`super::INBOX_DELIVER_SIGNATURE`].
pub const OUTBOX_SEND_MESSAGE_SIGNATURE: &str =
    "sendMessage(uint64,address,uint64,bytes,(address,uint64,bytes32))";

/// Solidity signature of the `Outbox.MessageSent` event.
pub const MESSAGE_SENT_SIGNATURE: &str = "MessageSent(uint64,uint64,address,address,uint256,\
                                          uint64,bytes,bytes32,(address,uint64,bytes32))";

/// 4-byte function selector of [`OUTBOX_SEND_MESSAGE_SIGNATURE`].
#[must_use]
pub fn outbox_send_message_selector() -> [u8; 4] {
    let h = keccak256(OUTBOX_SEND_MESSAGE_SIGNATURE.as_bytes());
    [h[0], h[1], h[2], h[3]]
}

/// `topic0` of the `Outbox.MessageSent` event.
#[must_use]
pub fn message_sent_topic0() -> B256 {
    keccak256(MESSAGE_SENT_SIGNATURE.as_bytes())
}

/// A `u64` as one left-padded 32-byte word: the `abi.encode` of a `uint64`,
/// and the key form of a `uint64` mapping key.
#[must_use]
pub fn u64_word(v: u64) -> B256 {
    let mut w = [0u8; 32];
    w[24..].copy_from_slice(&v.to_be_bytes());
    B256::from(w)
}

/// The Solidity mapping rule: the slot of `m[key]` for a mapping declared at
/// `slot_index` is `keccak256(key ‖ uint256(slot_index))`.
#[must_use]
pub fn mapping_slot(key: B256, slot_index: u64) -> B256 {
    let mut buf = [0u8; 64];
    buf[0..32].copy_from_slice(key.as_slice());
    buf[32..64].copy_from_slice(u64_word(slot_index).as_slice());
    keccak256(buf)
}

/// The storage slot of `Outbox.nonces[dest_chain_id]`.
#[must_use]
pub fn outbox_nonces_slot(dest_chain_id: u64) -> B256 {
    mapping_slot(u64_word(dest_chain_id), OUTBOX_NONCES_SLOT_INDEX)
}

/// The storage slot of `Outbox.sentMessages[msg_hash]`. The executor must
/// claim this slot `true` for every honest send; the validator's outbox
/// extraction checks the claim.
#[must_use]
pub fn sent_messages_slot(msg_hash: B256) -> B256 {
    mapping_slot(msg_hash, SENT_MESSAGES_SLOT_INDEX)
}

/// The storage slot of `Inbox.nextSeq[origin_chain_id]`.
#[must_use]
pub fn inbox_next_seq_slot(origin_chain_id: u64) -> B256 {
    mapping_slot(u64_word(origin_chain_id), INBOX_NEXT_SEQ_SLOT_INDEX)
}

/// The storage slot of `Inbox.delivered[origin_chain_id][seq]`. The outer
/// mapping's value slot is the inner mapping's slot index.
#[must_use]
pub fn inbox_delivered_slot(origin_chain_id: u64, seq: u64) -> B256 {
    let inner = mapping_slot(u64_word(origin_chain_id), INBOX_DELIVERED_SLOT_INDEX);
    let mut buf = [0u8; 64];
    buf[0..32].copy_from_slice(u64_word(seq).as_slice());
    buf[32..64].copy_from_slice(inner.as_slice());
    keccak256(buf)
}
