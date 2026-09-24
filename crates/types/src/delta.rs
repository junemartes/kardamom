//! Block-write payload from executor to state writer.
//!
//! Carries all account, storage, and code mutations, plus receipts, from a
//! sealed block. The state writer commits them atomically.

use alloc::vec::Vec;
use core::num::NonZeroU16;

use alloy_primitives::{Address, B256, U256};
use bytes::Bytes;
use rkyv::{Archive, Deserialize, Serialize};

use crate::receipt::Receipt;
use crate::wire;

#[derive(Clone, Debug, Default, Eq, PartialEq, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug))]
pub struct AccountChange {
    #[rkyv(with = wire::AddressBytes)]
    pub address: Address,
    pub nonce: u64,
    #[rkyv(with = wire::U256Bytes)]
    pub balance: U256,
    #[rkyv(with = wire::B256Bytes)]
    pub code_hash: B256,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug))]
pub struct StorageChange {
    #[rkyv(with = wire::AddressBytes)]
    pub address: Address,
    #[rkyv(with = wire::B256Bytes)]
    pub key: B256,
    #[rkyv(with = wire::U256Bytes)]
    pub value: U256,
}

/// A single code-hash-to-bytecode mapping in a block delta. A struct, not a
/// `(B256, Bytes)` tuple, lets the rkyv `with` adapters apply cleanly.
#[derive(Clone, Debug, Default, Eq, PartialEq, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug))]
pub struct CodeEntry {
    #[rkyv(with = wire::B256Bytes)]
    pub code_hash: B256,
    #[rkyv(with = wire::BytesVec)]
    pub code: Bytes,
}

/// `tx_bal` wire frame. It carries the merged final-value write set, plus
/// the EIP-7928 Block Access List (canonical alloy RLP). The list carries
/// per-slot `(tx_index, value)` write lists and per-account storage reads.
///
/// This type has no version, by choice: the wire format may still change
/// while the chain is at v0. Add versioning back when there is a second
/// live shape to carry.
#[derive(Clone, Debug, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug))]
pub struct BalFrame {
    /// The merged final-value write set for the block. Receipts are
    /// stripped: they dominate frame size, and large frames collapsed the
    /// validator's lapse window.
    pub delta: BlockDelta,
    /// RLP-encoded EIP-7928 block access list (attribution), quantized at
    /// `granularity`. This is empty when capture is disabled.
    pub bal_rlp: Vec<u8>,
    /// Attribution granularity. `1` means per-transaction. `K > 1` collapses
    /// chunks of `K` transactions. Never zero: the archived form rejects a
    /// zero byte pattern at decode time, so a corrupt or malicious producer
    /// loses the whole frame (the `bal_missing` posture) instead of handing
    /// every reader a value they must re-check.
    pub granularity: NonZeroU16,
}

impl BalFrame {
    /// The merged final-value section.
    #[must_use]
    pub fn delta(&self) -> &BlockDelta {
        &self.delta
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug))]
pub struct BlockDelta {
    pub block_number: u64,
    pub accounts: Vec<AccountChange>,
    pub storage: Vec<StorageChange>,
    pub code: Vec<CodeEntry>,
    pub receipts: Vec<Receipt>,
}

#[cfg(test)]
mod tests {
    use super::{BalFrame, BlockDelta, NonZeroU16, Vec};

    fn frame(granularity: u16) -> BalFrame {
        BalFrame {
            delta: BlockDelta::default(),
            bal_rlp: Vec::new(),
            granularity: NonZeroU16::new(granularity).expect("fixture granularity"),
        }
    }

    /// Defect: a `BalFrame` with `granularity` zeroed on the wire must fail
    /// to decode, not silently hand every reader an invalid value. The
    /// archived form has the same layout as `u16`, so this locates the
    /// granularity bytes by diffing two otherwise-identical encodings, then
    /// zeroes exactly those bytes in a third and checks decode rejects it.
    #[test]
    fn a_zeroed_granularity_byte_pattern_fails_to_decode() {
        let one = rkyv::to_bytes::<rkyv::rancor::Error>(&frame(1)).unwrap();
        let two = rkyv::to_bytes::<rkyv::rancor::Error>(&frame(2)).unwrap();
        assert_eq!(one.len(), two.len(), "only the granularity value differs");
        let diff_positions: Vec<usize> = one
            .iter()
            .zip(two.iter())
            .enumerate()
            .filter(|(_, (a, b))| a != b)
            .map(|(i, _)| i)
            .collect();
        assert!(
            !diff_positions.is_empty(),
            "granularity must appear somewhere in the archive"
        );
        let mut corrupted = one.to_vec();
        for i in diff_positions {
            corrupted[i] = 0;
        }
        assert!(
            rkyv::from_bytes::<BalFrame, rkyv::rancor::Error>(&corrupted).is_err(),
            "a zero granularity must not decode"
        );
    }
}
