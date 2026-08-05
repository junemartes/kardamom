//! Execution witness: the exact pre-state slice a block's re-execution reads.
//!
//! Produced by the validator's witness collector (`kardamom-exec-core`'s
//! `WitnessRecorder` wrapped around its state snapshot) and consumed by a
//! stateless re-execution (`WitnessDb`) — the zk-guest input shape (spec:
//! no-std-exec-core, phase 2).
//!
//! Absence is EXPLICIT: an account the execution observed as non-existent is
//! recorded with `exists = false`, and a storage slot observed as zero is
//! recorded with `value = 0`. A stateless re-execution treats a key that is
//! not in the witness at all as an error (incomplete witness), never as
//! empty — the fail-closed semantic a prover needs. Phase 2 witnesses are
//! TRUSTED (produced and consumed inside the validator); phase 3 anchors
//! `accounts`/`storage` to the pre-state root with MPT proofs.

use alloc::vec::Vec;

use alloy_primitives::{Address, B256, U256};
use rkyv::with::Map;
use rkyv::{Archive, Deserialize, Serialize};

use crate::delta::CodeEntry;
use crate::wire;

/// One account's observed pre-state. `exists = false` records a PROVEN
/// absence (the execution read the account and found nothing); the value
/// fields are zero-filled and meaningless in that case.
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

/// One storage slot's observed pre-state value (zero values are recorded
/// explicitly — absence from the witness is an error, not a zero).
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
pub struct WitnessSlot {
    #[rkyv(with = wire::AddressBytes)]
    pub address: Address,
    #[rkyv(with = wire::B256Bytes)]
    pub key: B256,
    #[rkyv(with = wire::U256Bytes)]
    pub value: U256,
}

/// The complete pre-state slice one block's execution reads, in canonical
/// (sorted) order. The pipelined-commit parent layer is NOT part of the
/// witness: capture runs at the snapshot seam, below the parent/seed layers,
/// so parent-layer reads surface here as ordinary pre-state entries.
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
    /// Effective gas price context is carried by the records themselves; the
    /// witness only covers STATE.
    #[rkyv(with = Map<wire::B256Bytes>)]
    pub pre_state_root: Option<B256>,
}

impl ExecutionWitness {
    /// Deterministic keccak256 digest of the witness. Layout mirrors
    /// `WriteSet::hash`'s explicit width + endianness discipline so two
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
