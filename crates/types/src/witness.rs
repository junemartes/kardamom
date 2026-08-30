//! Execution witness: the exact pre-state slice a block's re-execution reads.
//!
//! The validator's witness collector produces this. This is
//! `kardamom-exec-core`'s `WitnessRecorder`, wrapped around its state
//! snapshot. A stateless re-execution (`WitnessDb`) consumes it. This is
//! the zk-guest input shape.
//!
//! Absence is explicit. An account the execution finds non-existent is
//! recorded with `exists = false`. A storage slot the execution finds zero
//! is recorded with `value = 0`. A stateless re-execution treats a key
//! missing from the witness entirely as an error, an incomplete witness,
//! never as empty. This is the fail-closed rule a prover needs. Phase 2
//! witnesses are trusted, produced and consumed inside the validator.
//! Phase 3 anchors `accounts` and `storage` to the pre-state root with MPT
//! proofs.

use alloc::vec::Vec;

use alloy_primitives::{Address, B256, U256};
use bytes::Bytes;
use rkyv::with::Map;
use rkyv::{Archive, Deserialize, Serialize};

use crate::delta::CodeEntry;
use crate::wire;

/// One account's observed pre-state. `exists = false` records a proven
/// absence: the execution read the account and found nothing. In that
/// case, the value fields are zero-filled and meaningless.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
pub struct WitnessAccount {
    #[rkyv(with = wire::AddressBytes)]
    pub address: Address,
    pub exists: bool,
    pub nonce: u64,
    #[rkyv(with = wire::U256Bytes)]
    pub balance: U256,
    #[rkyv(with = wire::B256Bytes)]
    pub code_hash: B256,
}

/// One storage slot's observed pre-state value. A zero value is recorded
/// explicitly. Absence from the witness is an error, not a zero.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
pub struct WitnessSlot {
    #[rkyv(with = wire::AddressBytes)]
    pub address: Address,
    #[rkyv(with = wire::B256Bytes)]
    pub key: B256,
    #[rkyv(with = wire::U256Bytes)]
    pub value: U256,
}

/// The complete pre-state slice that one block's execution reads, in
/// canonical (sorted) order. The pipelined-commit parent layer is not part
/// of the witness. Capture runs at the snapshot seam, below the parent and
/// seed layers, so a parent-layer read surfaces here as an ordinary
/// pre-state entry.
#[derive(Debug, Clone, PartialEq, Eq, Default, Archive, Serialize, Deserialize)]
pub struct ExecutionWitness {
    pub block_number: u64,
    /// Sorted by address; unique.
    pub accounts: Vec<WitnessAccount>,
    /// Sorted by (address, key); unique.
    pub storage: Vec<WitnessSlot>,
    /// Bytecode for every non-empty code hash the execution loaded. Sorted by
    /// code_hash; unique.
    pub code: Vec<CodeEntry>,
    /// Effective gas price context is carried by the records themselves.
    /// The witness covers only state.
    #[rkyv(with = Map<wire::B256Bytes>)]
    pub pre_state_root: Option<B256>,
}

/// MPT proof material anchoring an [`ExecutionWitness`] to its
/// `pre_state_root`.
///
/// This is one flat, content-addressed node set, not per-entry proof
/// paths. It holds every RLP-encoded trie node needed to: prove each
/// witness account or slot present or absent under `pre_state_root`, and
/// recompute the post-state root after the block's delta, including the
/// deletion-collapse siblings the capture fixed point adds. MPT proof
/// paths share prefixes heavily, so this set is far smaller than the sum
/// of the paths. A consumer walks the trie from the root, looking up
/// nodes by `keccak256(node)`. Reaching a leaf is the inclusion proof.
/// Reaching a divergence is the exclusion proof.
///
/// A node whose RLP is shorter than 32 bytes never appears here. The MPT
/// embeds such nodes in their parent, so the walk finds them inline.
///
/// This is not part of [`ExecutionWitness::digest`]. Proof nodes are
/// recomputable commitments over state the digest already covers. Every
/// node is verified by hash against `pre_state_root` on use, so a
/// tampered node set can only fail verification. It can never smuggle
/// state.
#[derive(Debug, Clone, PartialEq, Eq, Default, Archive, Serialize, Deserialize)]
pub struct WitnessProofs {
    /// RLP-encoded MPT nodes. The account trie and storage tries mix
    /// together; content addressing needs no namespacing. Sorted by
    /// `keccak256(node)`, ascending, and unique. A consumer must reject an
    /// unsorted or duplicate set. This is the canonical wire form: one
    /// valid encoding per set.
    #[rkyv(with = Map<wire::BytesVec>)]
    pub nodes: Vec<Bytes>,
}

impl ExecutionWitness {
    /// Deterministic keccak256 digest of the witness. The layout mirrors
    /// `WriteSet::hash`'s explicit width and endianness rules, so two
    /// replicas produce identical bytes:
    ///
    /// ```text
    /// "WACC" || count_be_u32
    ///   || per account (addr asc): addr(20) || exists(1) || nonce(8 LE)
    ///        || balance(32 BE) || code_hash(32)
    /// "WSTO" || count_be_u32
    ///   || per slot ((addr,key) asc): addr(20) || key(32) || value(32 BE)
    /// "WCOD" || count_be_u32
    ///   || per entry (hash asc): code_hash(32) || len(8 LE) || bytes
    /// "WHDR" || block_number(8 LE) || has_root(1) || root(32 or absent)
    /// ```
    pub fn digest(&self) -> B256 {
        let mut h = alloy_primitives::Keccak256::new();
        h.update(b"WACC");
        h.update((self.accounts.len() as u32).to_be_bytes());
        for a in &self.accounts {
            h.update(a.address.as_slice());
            h.update([u8::from(a.exists)]);
            h.update(a.nonce.to_le_bytes());
            h.update(a.balance.to_be_bytes::<32>());
            h.update(a.code_hash.as_slice());
        }
        h.update(b"WSTO");
        h.update((self.storage.len() as u32).to_be_bytes());
        for s in &self.storage {
            h.update(s.address.as_slice());
            h.update(s.key.as_slice());
            h.update(s.value.to_be_bytes::<32>());
        }
        h.update(b"WCOD");
        h.update((self.code.len() as u32).to_be_bytes());
        for c in &self.code {
            h.update(c.code_hash.as_slice());
            h.update((c.code.len() as u64).to_le_bytes());
            h.update(c.code.as_ref());
        }
        h.update(b"WHDR");
        h.update(self.block_number.to_le_bytes());
        match &self.pre_state_root {
            Some(r) => {
                h.update([1u8]);
                h.update(r.as_slice());
            }
            None => h.update([0u8]),
        }
        h.finalize()
    }
}
