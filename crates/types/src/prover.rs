//! The prover input. This holds everything that one block's anchored
//! stateless execution needs, as a single rkyv frame. See
//! docs/agents/no-std-exec-core-spec.md.
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

use alloy_primitives::{B256, Keccak256, U256};
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
    pub granularity: u16,
}

/// The single-block proof's public outputs (v2, spec PR 5 slice 0). This is
/// a dispute-ready, 160-byte abi shape. A Solidity call to
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

    pub fn encode(&self) -> [u8; Self::ENCODED_LEN] {
        let mut out = [0u8; Self::ENCODED_LEN];
        out[0..32].copy_from_slice(self.pre_state_root.as_slice());
        out[32..64].copy_from_slice(self.post_state_root.as_slice());
        out[64..96].copy_from_slice(&U256::from(self.block_number).to_be_bytes::<32>());
        out[96..128].copy_from_slice(self.records_digest.as_slice());
        out[128..160].copy_from_slice(self.bal_commitment.as_slice());
        out
    }

    pub fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != Self::ENCODED_LEN {
            return None;
        }
        let n = U256::from_be_slice(&bytes[64..96]);
        if n > U256::from(u64::MAX) {
            return None;
        }
        Some(Self {
            pre_state_root: B256::from_slice(&bytes[0..32]),
            post_state_root: B256::from_slice(&bytes[32..64]),
            block_number: n.to::<u64>(),
            records_digest: B256::from_slice(&bytes[96..128]),
            bal_commitment: B256::from_slice(&bytes[128..160]),
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
    pub fn new(block_number: u64) -> Self {
        let mut h = Keccak256::new();
        h.update(b"KREC");
        h.update(block_number.to_le_bytes());
        Self { h }
    }

    pub fn add_tx(&mut self, raw_tx: &[u8]) {
        self.h.update([0x01]);
        self.h.update((raw_tx.len() as u32).to_le_bytes());
        self.h.update(raw_tx);
    }

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

    pub fn encode(&self) -> [u8; Self::ENCODED_LEN] {
        let mut out = [0u8; Self::ENCODED_LEN];
        out[0..32].copy_from_slice(self.pre_state_root.as_slice());
        out[32..64].copy_from_slice(self.post_state_root.as_slice());
        out[64..96].copy_from_slice(&U256::from(self.first_block).to_be_bytes::<32>());
        out[96..128].copy_from_slice(&U256::from(self.last_block).to_be_bytes::<32>());
        out[128..160].copy_from_slice(self.records_commitment.as_slice());
        out
    }

    pub fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != Self::ENCODED_LEN {
            return None;
        }
        let word = |i: usize| U256::from_be_slice(&bytes[i..i + 32]);
        let first = word(64);
        let last = word(96);
        if first > U256::from(u64::MAX) || last > U256::from(u64::MAX) {
            return None;
        }
        Some(Self {
            pre_state_root: B256::from_slice(&bytes[0..32]),
            post_state_root: B256::from_slice(&bytes[32..64]),
            first_block: first.to::<u64>(),
            last_block: last.to::<u64>(),
            records_commitment: B256::from_slice(&bytes[128..160]),
        })
    }
}
