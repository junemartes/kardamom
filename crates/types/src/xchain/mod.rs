//! Cross-chain (Kardamom ↔ Kardamom) **remote epoch** derivation — the single
//! definition of "which outbox messages does origin chain O contribute next,
//! and in what order".
//!
//! Like [`crate::epoch`], this lives in `kardamom-types` on purpose: the same
//! rule is run by the **producer** (the interop watcher, deriving from a peer
//! validator's feed or a sovereign local validator) and the **verifier** (the
//! destination validator, re-deriving and rejecting a chain that disagrees).
//! One copy, or the verification verifies nothing.
//!
//! Contents:
//!
//!   * [`remote_source_hash`] — the cross-chain tx's canonical id, domain 2 of
//!     the [`crate::epoch::source_hash`] scheme. **Position-based** (origin
//!     chain id + per-pair seq), NOT content-based: dedup must key on the
//!     message's slot so an equivocating origin (two payloads at one seq)
//!     collapses to first-seen and is then caught by content verification as a
//!     fault — content-keyed ids would let both execute.
//!   * [`alias_remote_address`] — hash-based aliasing. The L1 constant-offset
//!     trick ([`crate::epoch::alias_l1_address`]) cannot serve here: two
//!     origins' contracts at the same address would collide on the
//!     destination.
//!   * [`OutboxMessage`] — the decoded shape of one `MessageSent` outbox
//!     event, transport-agnostic (peer feed, sovereign validator, test
//!     fixture), which is what lets producer and verifier share
//!     [`derive_remote_epoch`].
//!   * [`XChainMessage`] / [`Callback`] — the wire shapes on the canonical
//!     stream.
//!   * [`RemoteEpochRecord`] — one origin's contiguous message batch as it
//!     travels on the canonical stream.
//!   * [`MsgLeaf`] — the commitment the Outbox predeploy stores; must stay
//!     byte-identical to `Outbox.hashMessage` (see `contracts/src/Outbox.sol`).
//!
//! The module splits into: [`ids`] (address and hash derivation), [`message`]
//! (the wire and record types), [`leaf`] (the Outbox commitment), [`abi`]
//! (the `Inbox.deliver` calldata encoder), and [`derive`] (the derivation
//! rule and its error type).

mod abi;
mod derive;
mod ids;
mod layout;
mod leaf;
mod message;

#[cfg(test)]
mod abi_tests;
#[cfg(test)]
mod layout_tests;
#[cfg(test)]
mod tests;

pub use abi::{INBOX_DELIVER_SIGNATURE, deliver_calldata, inbox_deliver_selector};
pub use derive::{
    MAX_REMOTE_EPOCH_WIRE_BYTES, REMOTE_EPOCH_FIXED_WIRE_BYTES, XCHAIN_MSG_FIXED_WIRE_BYTES,
    XChainError, derive_remote_epoch, remote_epoch_wire_bytes,
};
pub use ids::{Anchor, alias_remote_address, remote_source_hash, xchain_tx_sender};
pub use layout::{Inbox, Outbox, u64_word, word_u64};
pub use leaf::{MsgLeaf, no_callback_hash, xchain_leaf_domain};
pub use message::{
    BoundsFault, Callback, NonEmptyVec, OutboxMessage, RemoteEpochRecord, XChainMessage,
};

use alloy_primitives::{Address, B256, address, keccak256};

/// `keccak256` over concatenated byte slices, for the several fixed-shape
/// preimages in this module (an anchor, an alias, a canonical id, a mapping
/// slot) that would otherwise each hand-build the same kind of buffer.
fn keccak_concat(parts: &[&[u8]]) -> B256 {
    keccak256(parts.concat())
}

/// Domain of [`remote_source_hash`] in the source-hash scheme shared with
/// deposits (`crate::epoch`: 0 = user deposit, 1 = reserved system tx).
pub const XCHAIN_SOURCE_DOMAIN: u64 = 2;

/// Tag string hashed into [`alias_remote_address`]. Versioned: changing the
/// alias scheme is a chain-splitting change and must bump this.
pub const XCHAIN_ALIAS_TAG: &str = "KARDAMOM_XCHAIN_ALIAS_V0";

/// Tag string hashed into [`Anchor::hash`]. Versioned like
/// [`XCHAIN_ALIAS_TAG`]: a change here changes every record id.
pub const XCHAIN_ANCHOR_TAG: &str = "KARDAMOM_XCHAIN_ANCHOR_V0";

/// Canonical predeploy address of the `Outbox` (cross-chain send entry point)
/// on every Kardamom chain. Seeded into genesis like
/// [`crate::withdrawals::MESSAGE_PASSER`].
pub const OUTBOX: Address = address!("0x42000000000000000000000000000000000000E0");

/// Canonical predeploy address of the `Inbox` (cross-chain delivery entry
/// point). The derived 0x7D tx always calls this address, with the aliased
/// origin OUTBOX as the EVM sender — an address no key exists for, which is
/// what makes delivery injection unforgeable by user txs.
pub const INBOX: Address = address!("0x42000000000000000000000000000000000000E1");

/// Largest `gasLimit` the origin `Outbox` accepts for one message. This
/// mirrors `Outbox.MAX_MESSAGE_GAS` (`contracts/src/L2/Outbox.sol`).
/// [`derive_remote_epoch`] rejects a larger value, so an honest origin can
/// never produce a message that the destination cannot budget.
pub const MAX_MESSAGE_GAS: u64 = 10_000_000;

/// Largest `data` length the origin `Outbox` accepts for one message. This
/// mirrors `Outbox.MAX_DATA_BYTES` (`contracts/src/L2/Outbox.sol`).
pub const MAX_DATA_BYTES: usize = 65_536;
