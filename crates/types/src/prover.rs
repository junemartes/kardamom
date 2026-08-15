//! The prover input: everything one block's anchored stateless execution
//! consumes, as a single rkyv frame (spec: no-std-exec-core, phase 3c).
//!
//! Host side: the validator's capture + anchoring assemble this from
//! `capture_block_witness` / `anchor_block_witness` output. Guest side: the
//! zkVM program deserializes it, rebuilds the exec-core record list, and
//! runs `execute_block_anchored` — whose public outputs are the ONLY thing
//! the proof reveals ([`PublicOutputs`]).
//!
//! The BAL travels as its canonical RLP (`bal_rlp`) — the exact bytes the
//! executor published in the frame — so `bal_commitment = keccak256(bal_rlp)`
//! binds the proof to the posted artifact without re-encoding ambiguity.

use alloc::vec::Vec;

use alloy_primitives::B256;
use bytes::Bytes;
use rkyv::{Archive, Deserialize, Serialize};

use crate::witness::{ExecutionWitness, WitnessProofs};
use crate::{BPosition, BlockBoundaryStart, Deposit, TxEnvelope, wire};

/// One canonical record on the prover wire — mirrors the exec core's
/// `BufferedRecord` (which is not itself a wire type; the guest rebuilds
/// it from this).
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
    /// The boundary that OPENED this block (block N's txs execute under
    /// boundary N-1's timestamp — the guest rebuilds `ExecEnv` exactly as
    /// the live exec thread does).
    pub boundary: BlockBoundaryStart,
    pub witness: ExecutionWitness,
    pub proofs: WitnessProofs,
    pub records: Vec<ProverRecord>,
    /// The published frame's canonical BAL RLP (the proof input; the guest
    /// re-derives and compares).
    #[rkyv(with = wire::BytesVec)]
    pub bal_rlp: Bytes,
    pub granularity: u16,
}

/// The proof's public outputs, in their committed byte layout:
/// `pre_state_root(32) || post_state_root(32) || bal_commitment(32) ||
/// block_number(8 LE)` — 104 bytes, the tuple an L1 verifier reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublicOutputs {
    pub pre_state_root: B256,
    pub post_state_root: B256,
    pub bal_commitment: B256,
    pub block_number: u64,
}

impl PublicOutputs {
    pub const ENCODED_LEN: usize = 104;

    pub fn encode(&self) -> [u8; Self::ENCODED_LEN] {
        let mut out = [0u8; Self::ENCODED_LEN];
        out[0..32].copy_from_slice(self.pre_state_root.as_slice());
        out[32..64].copy_from_slice(self.post_state_root.as_slice());
        out[64..96].copy_from_slice(self.bal_commitment.as_slice());
        out[96..104].copy_from_slice(&self.block_number.to_le_bytes());
        out
    }

    pub fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != Self::ENCODED_LEN {
            return None;
        }
        Some(Self {
            pre_state_root: B256::from_slice(&bytes[0..32]),
            post_state_root: B256::from_slice(&bytes[32..64]),
            bal_commitment: B256::from_slice(&bytes[64..96]),
            block_number: u64::from_le_bytes(bytes[96..104].try_into().ok()?),
        })
    }
}
