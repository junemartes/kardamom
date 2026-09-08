//! `Inbox.deliver` calldata encoding — the derived 0x7D tx's call.

use alloc::vec::Vec;

use alloy_primitives::{Address, keccak256};

use super::message::XChainMessage;

/// Canonical signature of `Inbox.deliver` — the call every derived 0x7D tx
/// makes. The callback parameter is the static tuple `(address,uint64,bytes32)`
/// (`XChain.Callback`), which Solidity resolves into the signature exactly as
/// written here.
pub const INBOX_DELIVER_SIGNATURE: &str =
    "deliver(uint64,uint64,address,address,uint256,uint64,bytes,(address,uint64,bytes32))";

/// 4-byte function selector of [`INBOX_DELIVER_SIGNATURE`].
#[must_use]
pub fn inbox_deliver_selector() -> [u8; 4] {
    let h = keccak256(INBOX_DELIVER_SIGNATURE.as_bytes());
    [h[0], h[1], h[2], h[3]]
}

/// ABI-encode `Inbox.deliver(originChainId, seq, originSender, target, value,
/// gasLimit, data, cb)` for one message — the calldata of the derived 0x7D tx.
///
/// Hand-rolled: the execution edge is `no_std` and must not grow an ABI
/// codegen dependency for one fixed call; byte-parity with `alloy-sol-types`
/// is pinned in tests. Layout: a 10-word head — the six static params, the
/// offset word for `data` (0x140, the tail begins right after the head), and
/// the static callback tuple inlined as three words — then `data`'s length
/// word and its right-padded bytes. `callback: None` encodes as the zeroed
/// tuple, which is exactly what `XChain.isNone` tests for.
pub fn deliver_calldata(origin_chain_id: u64, msg: &XChainMessage) -> Vec<u8> {
    const HEAD_WORDS: usize = 10;
    // `HEAD_WORDS * 32`: the byte offset where `data`'s tail begins, as the
    // `u64` word `deliver_calldata` writes into the head. Kept as its own
    // constant instead of a runtime `u64::try_from(HEAD_WORDS * 32)`, since
    // both sides are already known at compile time.
    const HEAD_BYTES: u64 = 320;
    const _: () = assert!(HEAD_BYTES == HEAD_WORDS as u64 * 32);

    let data = msg.input.as_ref();
    let padded_len = data.len().div_ceil(32) * 32;
    let cb = msg.callback.unwrap_or_default();

    let mut out = AbiWords::with_capacity(4 + (HEAD_WORDS + 1) * 32 + padded_len)
        .bytes(&inbox_deliver_selector())
        .u64(origin_chain_id)
        .u64(msg.seq)
        .address(msg.origin_sender)
        .address(msg.target)
        .u128(msg.value)
        .u64(msg.gas_limit)
        .u64(HEAD_BYTES)
        .address(cb.target)
        .u64(cb.gas_limit)
        .bytes(cb.context.as_slice())
        .u64(crate::num::usize_to_u64(data.len()))
        .into_vec();
    out.extend_from_slice(data);
    out.resize(out.len() + (padded_len - data.len()), 0);
    out
}

/// A right-aligned, 32-byte-word ABI head builder. Each method appends one
/// static word (or raw bytes) and returns `self`, so a call's head encodes
/// as one chained expression instead of a run of separate `push_word_*`
/// calls threading the same output buffer.
struct AbiWords(Vec<u8>);

impl AbiWords {
    fn with_capacity(cap: usize) -> Self {
        Self(Vec::with_capacity(cap))
    }

    /// Append raw bytes, unpadded. Used for the selector and for a
    /// static tuple's already-word-shaped fields, such the callback
    /// context's 32 bytes.
    fn bytes(mut self, b: &[u8]) -> Self {
        self.0.extend_from_slice(b);
        self
    }

    fn u64(mut self, v: u64) -> Self {
        crate::abi::push_word_u64(&mut self.0, v);
        self
    }

    fn u128(mut self, v: u128) -> Self {
        crate::abi::push_word_u128(&mut self.0, v);
        self
    }

    fn address(mut self, a: Address) -> Self {
        crate::abi::push_word_address(&mut self.0, a);
        self
    }

    fn into_vec(self) -> Vec<u8> {
        self.0
    }
}
