//! L1 deposit wire types.
//!
//! Two records flow on the deposit path:
//!
//! - [`Deposit`] — the full payload for one deposit. The DA watcher publishes
//!   it on the dedicated `tx_deposits` Aeron channel. It carries the
//!   OP-aligned deposit envelope: `source_hash` for dedup, sender, recipient,
//!   value, mint, and inner-call data.
//! - [`DepositRef`] — a small reference (about 36 bytes, rkyv-packed).
//!   Sequencers republish one on the canonical `tx_ordering` channel for each
//!   observed deposit. It carries `(source_hash, deposit_position)`.
//!   Downstream consumers, such as the executor, dedup on `source_hash` and
//!   read the full `Deposit` from the `tx_deposits` archive at
//!   `deposit_position`. This mirrors the `TxData → TxRef on tx_ordering →
//!   executor` flow used for regular L2 transactions.
//!
//! `source_hash` is the deposit's canonical id. The DA watcher computes it
//! from the L1 block hash and log index, using the OP source-hash domain
//! rules. It plays the same dedup role that `tx_hash` plays for regular L2
//! transactions.

use alloy_primitives::{Address, B256, U256};
use bytes::Bytes;
use rkyv::with::Map;
use rkyv::{Archive, Deserialize, Serialize};

use crate::position::BPosition;
use crate::wire;

/// L1 deposit envelope republished on the `tx_deposits` channel.
///
/// `to: None` marks a CREATE deposit. This is rare; most deposits target an
/// existing L2 contract. `mint` is the amount minted on L2 to the aliased
/// `from` account before the inner EVM call runs. `value` is the amount the
/// inner call carries.
#[derive(Clone, Debug, Default, Eq, PartialEq, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug))]
pub struct Deposit {
    /// OP-style deposit source hash (`keccak256(domain || l1_block_hash ||
    /// log_index)`). This is the canonical id of the deposit. Downstream
    /// consumers dedup on this field.
    #[rkyv(with = wire::B256Bytes)]
    pub source_hash: B256,
    /// L1 sender. The DA watcher already aliases this to L2 space
    /// (`L1addr + 0x1111...1111`). The L2 executor never sees the bare L1
    /// address.
    #[rkyv(with = wire::AddressBytes)]
    pub from: Address,
    /// L2 recipient of the inner call. `None` marks a CREATE deposit.
    #[rkyv(with = Map<wire::AddressBytes>)]
    pub to: Option<Address>,
    /// Amount minted on L2 to `from` before the inner EVM call.
    pub mint: u128,
    /// Value forwarded as the EVM `value` field of the inner call. For
    /// ETH-bridge deposits this typically equals `mint`. The two fields are
    /// independent on the wire, so a deposit can mint without forwarding value.
    #[rkyv(with = wire::U256Bytes)]
    pub value: U256,
    /// Gas limit for the inner EVM call. The deposit itself pays no fee.
    pub gas_limit: u64,
    /// OP system-transaction flag. v0 only canonicalizes user deposits, so
    /// this is always `false`. The L1-attributes system transaction
    /// (`true`) is reserved for later work.
    pub is_system_transaction: bool,
    /// Calldata for the inner EVM call.
    #[rkyv(with = wire::BytesVec)]
    pub input: Bytes,
}

/// Reference to a deposit, republished on `tx_ordering`. It carries only
/// what a consumer needs to dedup the deposit and find it in the
/// `tx_deposits` archive.
///
/// On the wire this is about 36 bytes, rkyv-packed. The position of this
/// record on `tx_ordering` sets the canonical L2 order. A consumer looks up
/// the full [`Deposit`] at `deposit_position` in the `tx_deposits` archive.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug), compare(PartialEq))]
pub struct DepositRef {
    /// OP source hash. Used for O(1) dedup of duplicate references from
    /// racing sequencers.
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
