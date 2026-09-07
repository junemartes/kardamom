//! The Outbox commitment leaf: the hash the predeploy stores per message.

use alloy_primitives::{Address, B256, keccak256};

/// First `abi.encode` word of [`MsgLeaf::hash`]. Must equal
/// `Outbox.LEAF_DOMAIN_XCHAIN`. Domain-separates message leaves from every
/// other keccak commitment in the system (withdrawal leaves, tree nodes).
#[must_use]
pub fn xchain_leaf_domain() -> B256 {
    keccak256("KARDAMOM_XCHAIN_MESSAGE_V0")
}

/// `cbHash` for a message without a callback.
#[must_use]
pub fn no_callback_hash() -> B256 {
    B256::ZERO
}

/// The ten static fields the Outbox predeploy commits to per message. An
/// argument-group struct, so the hash rule takes one receiver instead of
/// nine positional parameters.
#[derive(Debug, Clone, Copy)]
pub struct MsgLeaf {
    pub origin_chain_id: u64,
    pub dest_chain_id: u64,
    pub seq: u64,
    pub sender: Address,
    pub target: Address,
    pub value: u128,
    pub gas_limit: u64,
    pub data_hash: B256,
    pub cb_hash: B256,
}

impl MsgLeaf {
    /// The commitment the Outbox predeploy stores per message:
    /// `keccak256(abi.encode(LEAF_DOMAIN_XCHAIN, originChainId, destChainId,
    /// seq, sender, target, value, gasLimit, keccak256(data), cbHash))` — ten
    /// static 32-byte words. Must stay byte-identical to
    /// `Outbox.hashMessage`. Origin AND destination chain ids inside the
    /// leaf make replay across pairs impossible.
    #[must_use]
    pub fn hash(&self) -> B256 {
        let mut buf = [0u8; 320];
        buf[0..32].copy_from_slice(xchain_leaf_domain().as_slice());
        buf[32..64].copy_from_slice(&crate::abi::word_u64(self.origin_chain_id));
        buf[64..96].copy_from_slice(&crate::abi::word_u64(self.dest_chain_id));
        buf[96..128].copy_from_slice(&crate::abi::word_u64(self.seq));
        buf[128..160].copy_from_slice(&crate::abi::word_address(self.sender));
        buf[160..192].copy_from_slice(&crate::abi::word_address(self.target));
        buf[192..224].copy_from_slice(&crate::abi::word_u128(self.value));
        buf[224..256].copy_from_slice(&crate::abi::word_u64(self.gas_limit));
        buf[256..288].copy_from_slice(self.data_hash.as_slice());
        buf[288..320].copy_from_slice(self.cb_hash.as_slice());
        keccak256(buf)
    }
}

/// Compatibility wrapper over [`MsgLeaf::hash`], kept only for the two
/// external callers this group cannot change directly:
/// `crates/e2e/src/scenarios/xchain.rs` and
/// `crates/validator/src/interop/extract.rs`. **Phase B**: move both onto
/// `MsgLeaf::hash` at merge, then delete this function.
#[allow(
    clippy::too_many_arguments,
    reason = "compatibility wrapper for the two Phase-B external callers named above; \
              MsgLeaf::hash is the real, argument-struct-based rule"
)]
#[must_use]
pub fn msg_leaf(
    origin_chain_id: u64,
    dest_chain_id: u64,
    seq: u64,
    sender: Address,
    target: Address,
    value: u128,
    gas_limit: u64,
    data_hash: B256,
    cb_hash: B256,
) -> B256 {
    MsgLeaf {
        origin_chain_id,
        dest_chain_id,
        seq,
        sender,
        target,
        value,
        gas_limit,
        data_hash,
        cb_hash,
    }
    .hash()
}
