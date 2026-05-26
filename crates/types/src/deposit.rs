//! L1 deposit wire types.
//!
//! Two records flow on the deposit path:
//!
//! - [`Deposit`] — the full per-deposit payload. Published by the DA watcher
//!   onto the dedicated `tx_deposits` Aeron channel. Carries the OP-aligned
//!   deposit envelope (`source_hash` for dedup, sender/recipient/value/mint,
//!   plus inner-call data).
//! - [`DepositRef`] — a tiny reference (~36 B rkyv-packed) sequencers
//!   republish onto the canonical `tx_ordering` channel for each observed
//!   deposit. Carries `(source_hash, deposit_position)`; downstream consumers
//!   (executor) dedup on `source_hash` and resolve the full `Deposit` from
//!   the `tx_deposits` archive at `deposit_position`. This mirrors the
//!   `TxData → TxRef on tx_ordering → executor` flow for regular L2 txs.
//!
//! `source_hash` is the deposit's canonical id (computed by the DA watcher
//! from the L1 block hash + log index per the OP source-hash domain rules)
//! and serves the same dedup role `tx_hash` does for regular L2 txs.

use alloy_primitives::{Address, B256, U256};
use bytes::Bytes;
use rkyv::with::Map;
use rkyv::{Archive, Deserialize, Serialize};

use crate::position::BPosition;
use crate::wire;

/// L1 deposit envelope republished on the `tx_deposits` channel.
///
/// `to: None` denotes a CREATE deposit (rare in practice — most deposits
/// target an existing L2 contract). `mint` is the amount minted on L2 to
/// the (aliased) `from` account *before* the inner EVM call runs; `value`
/// is what the inner call carries.
#[derive(Clone, Debug, Default, Eq, PartialEq, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug))]
pub struct Deposit {
    /// OP-style deposit source hash (`keccak256(domain || l1_block_hash ||
    /// log_index)`). Canonical id of this deposit; downstream consumers
    /// dedup on this field.
    #[rkyv(with = wire::B256Bytes)]
    pub source_hash: B256,
    /// L1 sender, already address-aliased to L2 space by the DA watcher
    /// (`L1addr + 0x1111...1111`). The L2 executor never observes the bare
    /// L1 address.
    #[rkyv(with = wire::AddressBytes)]
    pub from: Address,
    /// L2 recipient of the inner call. `None` denotes a CREATE deposit.
    #[rkyv(with = Map<wire::AddressBytes>)]
    pub to: Option<Address>,
    /// Amount minted on L2 to `from` before the inner EVM call.
    pub mint: u128,
    /// Value forwarded as the EVM `value` field of the inner call (typically
    /// equal to `mint` for ETH-bridge deposits, but the two are independent
    /// on the wire so a deposit can mint without forwarding value).
    #[rkyv(with = wire::U256Bytes)]
    pub value: U256,
    /// Gas limit for the inner EVM call. The deposit itself pays no fee.
    pub gas_limit: u64,
    /// OP system transaction flag. v0 only canonicalises user deposits
    /// (always `false`); the L1-attributes / system tx (`true`) is reserved
    /// for a follow-up.
    pub is_system_transaction: bool,
    /// Calldata for the inner EVM call.
    #[rkyv(with = wire::BytesVec)]
    pub input: Bytes,
}

/// Reference to a deposit republished on `tx_ordering`. Carries the
/// minimum needed to dedup and to resolve the deposit from the
/// `tx_deposits` archive.
///
/// On the wire: ~36 B rkyv-packed. The canonical L2 ordering is determined
/// by the position of this record on `tx_ordering` itself; consumers look
/// up the full [`Deposit`] at `deposit_position` on the `tx_deposits`
/// archive.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug), compare(PartialEq))]
pub struct DepositRef {
    /// OP source hash — used for O(1) dedup of duplicate refs from racing
    /// sequencers.
    #[rkyv(with = wire::B256Bytes)]
    pub source_hash: B256,
    /// Aeron position on `tx_deposits` where the [`Deposit`] envelope
    /// starts.
    pub deposit_position: BPosition,
}

impl DepositRef {
    pub fn new(source_hash: B256, deposit_position: BPosition) -> Self {
        Self {
            source_hash,
            deposit_position,
        }
    }
}
