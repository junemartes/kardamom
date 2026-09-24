//! The two spellings of "no code" that meet at every state boundary this
//! crate crosses: kardamom's state DB and delta/write-set wire forms use
//! `B256::ZERO`, while revm's `CacheDB` and trie leaves use
//! `KECCAK_EMPTY` (`keccak256(&[])`). Execution treats the two
//! identically, so every read from one side into the other must
//! normalize, or an account's `code_hash` (and so its receipt or trie
//! leaf) could depend on which side last touched it. This module names
//! the two directions once, instead of writing the `if h == X { Y } else
//! { h }` out at every crossing.
//!
//! `no_std`: the guest crosses these same boundaries (anchoring against a
//! witness trie built with `KECCAK_EMPTY` leaves, from a delta built with
//! `B256::ZERO`).

use alloy_primitives::{B256, U256};
use revm::primitives::KECCAK_EMPTY;

/// Normalize a code hash into revm's convention: `KECCAK_EMPTY` for "no
/// code" instead of kardamom's `B256::ZERO`. Every other hash passes
/// through unchanged.
#[must_use]
pub(crate) fn to_revm_code_hash(h: B256) -> B256 {
    if h == B256::ZERO { KECCAK_EMPTY } else { h }
}

/// Normalize a code hash into kardamom's wire convention: `B256::ZERO`
/// for "no code" instead of revm's `KECCAK_EMPTY`. Every other hash
/// passes through unchanged.
#[must_use]
pub(crate) fn to_wire_code_hash(h: B256) -> B256 {
    if h == KECCAK_EMPTY { B256::ZERO } else { h }
}

/// The EIP-161 empty-account test: no nonce, no balance, no code, under
/// either code-hash convention. An empty account does not exist in the
/// state trie.
#[must_use]
pub(crate) fn is_empty_account(nonce: u64, balance: U256, code_hash: B256) -> bool {
    nonce == 0 && balance.is_zero() && to_revm_code_hash(code_hash) == KECCAK_EMPTY
}
