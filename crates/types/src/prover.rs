//! The prover input. This holds everything that one block's anchored
//! stateless execution needs, as a single rkyv frame.
//!
//! On the host side, the validator's capture and anchoring assemble this
//! from the output of `capture_block_witness` and `anchor_block_witness`.
//! On the guest side, the zkVM program deserializes it, rebuilds the
//! exec-core record list, and runs `execute_block_anchored`. The proof
//! reveals only that function's public outputs ([`PublicOutputs`]).
//!
//! The BAL travels as its canonical RLP (`bal_rlp`). These are the exact
//! bytes the executor published in the frame. So
//! `bal_commitment = keccak256(bal_rlp)` binds the proof to the posted
//! artifact, with no re-encoding ambiguity.

use alloc::vec::Vec;
use core::num::NonZeroU16;

use alloy_primitives::{B256, Keccak256};
use bytes::Bytes;
use rkyv::{Archive, Deserialize, Serialize};

use crate::witness::{ExecutionWitness, WitnessProofs};
use crate::{BPosition, BlockBoundaryStart, Deposit, TxEnvelope, wire};

/// One canonical record on the prover wire. This mirrors the exec core's
/// `BufferedRecord`. That type is not itself a wire type; the guest
/// rebuilds it from this record.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
pub enum ProverRecord {
    Tx {
        tx_idx: u64,
        envelope: TxEnvelope,
        position: BPosition,
    },
    Deposit {
        tx_idx: u64,
        deposit: Deposit,
        position: BPosition,
    },
}

/// The complete input for proving one block.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
pub struct ProverInput {
    pub chain_id: u64,
    /// The boundary that opened this block. Block N's transactions execute
    /// under boundary N-1's timestamp. The guest rebuilds `ExecEnv` exactly
    /// as the live exec thread does.
    pub boundary: BlockBoundaryStart,
    pub witness: ExecutionWitness,
    pub proofs: WitnessProofs,
    pub records: Vec<ProverRecord>,
    /// The published frame's canonical BAL RLP. This is the proof input;
    /// the guest re-derives it and compares.
    #[rkyv(with = wire::BytesVec)]
    pub bal_rlp: Bytes,
    /// The BAL attribution granularity this frame was quantized at.
    /// Never zero — a wire frame with granularity 0 does not decode.
    pub granularity: NonZeroU16,
}

/// A 160-byte, five-word Solidity ABI frame. [`PublicOutputs`] and
/// [`BatchPublicOutputs`] both encode and decode this exact shape, so
/// this is the one place their layout lives — an offset drift between
/// the block oracle and the batch oracle would otherwise be silent.
struct Words160([u8; 160]);

impl Words160 {
    const LEN: usize = 160;

    fn new() -> Self {
        Self([0u8; Self::LEN])
    }

    /// `None` if `bytes` is not exactly [`Self::LEN`] bytes.
    fn from_bytes(bytes: &[u8]) -> Option<Self> {
        Some(Self(bytes.try_into().ok()?))
    }

    fn put_b256(&mut self, i: usize, v: B256) {
        self.0[i * 32..i * 32 + 32].copy_from_slice(v.as_slice());
    }

    fn put_u64(&mut self, i: usize, v: u64) {
        self.0[i * 32..i * 32 + 32].copy_from_slice(&crate::abi::word_u64(v));
    }

    fn b256(&self, i: usize) -> B256 {
        B256::from_slice(&self.0[i * 32..i * 32 + 32])
    }

    /// `None` if word `i` does not fit in a `u64`: its top 24 bytes are
    /// not all zero.
    fn u64_checked(&self, i: usize) -> Option<u64> {
        let word = &self.0[i * 32..i * 32 + 32];
        if word[..24].iter().any(|&b| b != 0) {
            return None;
        }
        Some(u64::from_be_bytes(*word.last_chunk::<8>()?))
    }

    fn into_bytes(self) -> [u8; Self::LEN] {
        self.0
    }
}

/// The single-block proof's public outputs. This is a dispute-ready,
/// 160-byte abi shape. A Solidity call to
/// `abi.decode(publicValues, (bytes32, bytes32, uint256, bytes32, bytes32))`
/// reads it directly: `pre_state_root || post_state_root ||
/// block_number(u256) || records_digest || bal_commitment`. `records_digest`
/// is the block's [`BlockRecordsDigest`], covering L2 transactions only.
/// The optimistic oracle's `challengeBlock` compares this field against the
/// claim's per-block digests. `bal_commitment` binds the L2-published BAL
/// artifact for off-chain accountability. L1 does not store it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublicOutputs {
    pub pre_state_root: B256,
    pub post_state_root: B256,
    pub block_number: u64,
    pub records_digest: B256,
    pub bal_commitment: B256,
}

impl PublicOutputs {
    pub const ENCODED_LEN: usize = 160;

    #[must_use]
    pub fn encode(&self) -> [u8; Self::ENCODED_LEN] {
        let mut w = Words160::new();
        w.put_b256(0, self.pre_state_root);
        w.put_b256(1, self.post_state_root);
        w.put_u64(2, self.block_number);
        w.put_b256(3, self.records_digest);
        w.put_b256(4, self.bal_commitment);
        w.into_bytes()
    }

    #[must_use]
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        let w = Words160::from_bytes(bytes)?;
        Some(Self {
            pre_state_root: w.b256(0),
            post_state_root: w.b256(1),
            block_number: w.u64_checked(2)?,
            records_digest: w.b256(3),
            bal_commitment: w.b256(4),
        })
    }
}

/// Per-block digest of the batch-attested record identities.
///
/// Covers L2-originated transactions only. Deposits are L1-originated. The
/// batcher deliberately excludes them from DA batches, because L1 already
/// holds them. So the batch commitment binds exactly what the batch posts.
/// Both the batcher, at batch close, and the batch guest, over its input
/// records, compute this digest. The settlement contract stores the batch
/// fold, and the proof oracle requires the two to match.
pub struct BlockRecordsDigest {
    h: Keccak256,
}

impl BlockRecordsDigest {
    #[must_use]
    pub fn new(block_number: u64) -> Self {
        let mut h = Keccak256::new();
        h.update(b"KREC");
        h.update(block_number.to_le_bytes());
        Self { h }
    }

    /// # Panics
    ///
    /// Panics if `raw_tx` is 4 GiB or larger: the digest format is a
    /// fixed-width `u32` length prefix (see the wire layout above), and a
    /// silently truncated length would let two different transactions
    /// share one digest. No transaction reaches this size in practice —
    /// the ingress and gas limits reject it long before that — so this
    /// turns an impossible-in-practice case into a loud failure instead
    /// of a silent one.
    pub fn add_tx(&mut self, raw_tx: &[u8]) {
        self.h.update([0x01]);
        let len = u32::try_from(raw_tx.len())
            .unwrap_or_else(|_| panic!("raw_tx is {} bytes, over u32::MAX", raw_tx.len()));
        self.h.update(len.to_le_bytes());
        self.h.update(raw_tx);
    }

    #[must_use]
    pub fn finish(self) -> B256 {
        self.h.finalize()
    }
}

/// Fold per-block digests into the batch commitment the L1 stores.
pub fn batch_records_commitment(block_digests: impl IntoIterator<Item = B256>) -> B256 {
    let mut h = Keccak256::new();
    h.update(b"KBAT");
    for d in block_digests {
        h.update(d.as_slice());
    }
    h.finalize()
}

/// A batch proof's input: contiguous per-block frames. The
/// guest chains the roots internally: block i's `pre_state_root` must equal
/// block i-1's recomputed post root. So one proof attests the whole posted
/// range.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
pub struct BatchProverInput {
    pub blocks: Vec<ProverInput>,
}

/// The batch proof's public outputs: 160 bytes, five 32-byte words. This
/// matches Solidity's `abi.decode(publicValues, (bytes32, bytes32, uint256,
/// uint256, bytes32))`: `pre_state_root || post_state_root || first_block ||
/// last_block || records_commitment`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BatchPublicOutputs {
    pub pre_state_root: B256,
    pub post_state_root: B256,
    pub first_block: u64,
    pub last_block: u64,
    pub records_commitment: B256,
}

impl BatchPublicOutputs {
    pub const ENCODED_LEN: usize = 160;

    #[must_use]
    pub fn encode(&self) -> [u8; Self::ENCODED_LEN] {
        let mut w = Words160::new();
        w.put_b256(0, self.pre_state_root);
        w.put_b256(1, self.post_state_root);
        w.put_u64(2, self.first_block);
        w.put_u64(3, self.last_block);
        w.put_b256(4, self.records_commitment);
        w.into_bytes()
    }

    #[must_use]
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        let w = Words160::from_bytes(bytes)?;
        Some(Self {
            pre_state_root: w.b256(0),
            post_state_root: w.b256(1),
            first_block: w.u64_checked(2)?,
            last_block: w.u64_checked(3)?,
            records_commitment: w.b256(4),
        })
    }
}
