//! Right-aligned, 32-byte Solidity ABI word encoding.
//!
//! Several hand-rolled encoders across this crate — withdrawal and
//! cross-chain message hashing, upgrade calldata, the zk prover's public
//! inputs — each write one value right-aligned into a fixed offset of a
//! byte buffer. These functions are the one place that alignment lives,
//! so a `[N..32]` off-by-one shows up once instead of independently in
//! every encoder.

use alloc::vec::Vec;

use alloy_primitives::{Address, U256};

/// `v`, right-aligned into a 32-byte word (Solidity's `uint64` ABI shape).
#[must_use]
pub(crate) fn word_u64(v: u64) -> [u8; 32] {
    let mut w = [0u8; 32];
    w[24..].copy_from_slice(&v.to_be_bytes());
    w
}

/// `v`, right-aligned into a 32-byte word (Solidity's `uint128` ABI shape).
#[must_use]
pub(crate) fn word_u128(v: u128) -> [u8; 32] {
    let mut w = [0u8; 32];
    w[16..].copy_from_slice(&v.to_be_bytes());
    w
}

/// `v` as a full 32-byte word (Solidity's `uint256` ABI shape).
#[must_use]
pub(crate) fn word_u256(v: U256) -> [u8; 32] {
    v.to_be_bytes()
}

/// `a`, right-aligned into a 32-byte word (Solidity's `address` ABI shape).
#[must_use]
pub(crate) fn word_address(a: Address) -> [u8; 32] {
    let mut w = [0u8; 32];
    w[12..].copy_from_slice(a.as_slice());
    w
}

/// Append one `uint64` word to `out`.
pub(crate) fn push_word_u64(out: &mut Vec<u8>, v: u64) {
    out.extend_from_slice(&word_u64(v));
}

/// Append one `uint128` word to `out`.
pub(crate) fn push_word_u128(out: &mut Vec<u8>, v: u128) {
    out.extend_from_slice(&word_u128(v));
}

/// Append one `address` word to `out`.
pub(crate) fn push_word_address(out: &mut Vec<u8>, a: Address) {
    out.extend_from_slice(&word_address(a));
}
