//! Address aliasing and canonical tx id derivation for cross-chain messages.

use alloc::vec::Vec;

use alloy_primitives::{Address, B256, keccak256};

use super::{OUTBOX, XCHAIN_ALIAS_TAG, XCHAIN_SOURCE_DOMAIN};

/// The EVM sender of a derived cross-chain tx from `origin_chain_id`: the
/// origin's Outbox predeploy, aliased into the destination's address space.
/// `Inbox.deliver` authenticates against exactly this address (the OP
/// aliased-counterpart pattern), so only the derivation pipeline can deliver.
#[must_use]
pub fn xchain_tx_sender(origin_chain_id: u64) -> Address {
    alias_remote_address(origin_chain_id, OUTBOX)
}

/// Canonical id of a cross-chain tx on the destination:
///
/// ```text
///   msg_id_hash        = keccak256(rlp([origin_chain_id, seq]))
///   remote_source_hash = keccak256(rlp([domain = 2u64, msg_id_hash]))
/// ```
///
/// Deliberately position-based — see the module docs. Also deliberately
/// independent of the trust mode's anchor identity: the same message resolves
/// to the same tx hash whether it arrived via an anchored feed or a sovereign
/// validator, so switching a pair's trust mode never re-identifies its txs.
#[must_use]
pub fn remote_source_hash(origin_chain_id: u64, seq: u64) -> B256 {
    let inner = crate::rlp::encode_list_two(&origin_chain_id, &seq);
    let msg_id_hash = keccak256(&inner);
    let outer = crate::rlp::encode_list_two(&XCHAIN_SOURCE_DOMAIN, &msg_id_hash);
    keccak256(&outer)
}

/// Alias an origin-chain sender into the destination's address space:
/// `address(keccak256(TAG ‖ be64(origin_chain_id) ‖ sender)[12..])`.
///
/// Hash-based rather than offset-based so that senders from *different*
/// origins can never collide with each other (or with local contracts) —
/// the price is irreversibility, which is why [`super::XChainMessage`] carries
/// the un-aliased `origin_sender` on the wire and aliases only at the
/// execution edge.
#[must_use]
pub fn alias_remote_address(origin_chain_id: u64, sender: Address) -> Address {
    let mut buf = Vec::with_capacity(XCHAIN_ALIAS_TAG.len() + 8 + 20);
    buf.extend_from_slice(XCHAIN_ALIAS_TAG.as_bytes());
    buf.extend_from_slice(&origin_chain_id.to_be_bytes());
    buf.extend_from_slice(sender.as_slice());
    let h = keccak256(&buf);
    Address::from_slice(&h.as_slice()[12..])
}
