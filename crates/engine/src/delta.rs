//! Per-tx write-set accumulation and deterministic hashing.
//!
//! `WriteSet` is the per-tx unit. `kardamom_types::BlockDelta` is the
//! per-block accumulator the state writer eventually consumes. The crucial
//! invariant is that `WriteSet::hash()` is identical across all executor
//! replicas for any given tx — see (divergence panic).
//!
//! **No state-root computation here** (S0). The executor never emits
//! a state-root commitment. Per-tx `write_set_hash` is the sole determinism
//! witness; block-level attestation is a future validator concern.
//!
//! Selfdestruct semantics are out of scope for v0 — the EVM hardforks the
//! executor targets do not produce selfdestruct effects on existing chains
//! (EIP-6780 reduced its observable effect to balance transfer within the
//! same tx). Add a `destroyed` flag when the runtime starts requiring it.

use std::collections::BTreeMap;

use alloy_primitives::{Address, B256, U256, keccak256};
use bytes::Bytes;
use kardamom_types::delta::CodeEntry;
use kardamom_types::{AccountChange, BlockDelta, StorageChange};

/// One transaction's write effects. `BTreeMap` so iteration is canonical.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct WriteSet {
    /// address → (nonce, balance, code_hash).
    pub accounts: BTreeMap<Address, (u64, U256, B256)>,
    /// (address, slot_key) → value.
    pub storage: BTreeMap<(Address, B256), U256>,
    /// code_hash → bytecode.
    pub code: BTreeMap<B256, Bytes>,
}

impl WriteSet {
    /// Deterministic keccak256 hash of the write set.
    ///
    /// Layout (concatenated, then hashed):
    ///   "ACC" || count_be_u32
    ///     || for each (addr asc): addr(20) || nonce(8 LE) || balance(32 BE) || code_hash(32)
    ///   "STO" || count_be_u32
    ///     || for each ((addr, key) asc): addr(20) || key(32 BE) || value(32 BE)
    ///   "COD" || count_be_u32
    ///     || for each (code_hash asc): code_hash(32) || bytes_len(8 LE) || bytes
    ///
    /// Stable ordering comes from `BTreeMap`. Numeric encodings are explicit
    /// width + endianness so two replicas on different architectures produce
    /// identical bytes.
    pub fn hash(&self) -> B256 {
        let mut buf: Vec<u8> = Vec::with_capacity(
            3 + 4
                + self.accounts.len() * (20 + 8 + 32 + 32)
                + 3
                + 4
                + self.storage.len() * (20 + 32 + 32)
                + 3
                + 4
                + self.code.len() * (32 + 8),
        );

        buf.extend_from_slice(b"ACC");
        buf.extend_from_slice(&(self.accounts.len() as u32).to_be_bytes());
        for (addr, (nonce, balance, code_hash)) in &self.accounts {
            buf.extend_from_slice(addr.as_slice());
            buf.extend_from_slice(&nonce.to_le_bytes());
            buf.extend_from_slice(&balance.to_be_bytes::<32>());
            buf.extend_from_slice(code_hash.as_slice());
        }

        buf.extend_from_slice(b"STO");
        buf.extend_from_slice(&(self.storage.len() as u32).to_be_bytes());
        for ((addr, key), value) in &self.storage {
            buf.extend_from_slice(addr.as_slice());
            buf.extend_from_slice(key.as_slice());
            buf.extend_from_slice(&value.to_be_bytes::<32>());
        }

        buf.extend_from_slice(b"COD");
        buf.extend_from_slice(&(self.code.len() as u32).to_be_bytes());
        for (code_hash, bytes) in &self.code {
            buf.extend_from_slice(code_hash.as_slice());
            buf.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
            buf.extend_from_slice(bytes);
        }

        keccak256(&buf)
    }
}

/// Mutable per-block accumulator the exec thread carries between txs. Holds
/// the same canonical maps as `WriteSet` so we can both layer running writes
/// over the snapshot for the next tx and serialize to `kardamom_types::
/// BlockDelta` at block close.
#[derive(Debug, Default, Clone)]
pub struct PendingDelta {
    pub accounts: BTreeMap<Address, (u64, U256, B256)>,
    pub storage: BTreeMap<(Address, B256), U256>,
    pub code: BTreeMap<B256, Bytes>,
}

impl PendingDelta {
    pub fn new() -> Self {
        Self::default()
    }

    /// Merge a per-tx WriteSet into the running delta. Later writes overwrite
    /// earlier ones; this matches the sequential execution model — the last
    /// tx to touch a slot wins for the block.
    pub fn apply(&mut self, ws: WriteSet) {
        for (addr, v) in ws.accounts {
            self.accounts.insert(addr, v);
        }
        for (k, v) in ws.storage {
            self.storage.insert(k, v);
        }
        for (h, b) in ws.code {
            self.code.insert(h, b);
        }
    }

    /// Finalize: produce a wire-shape `BlockDelta` ready for the state
    /// writer. `receipts` are the block's per-tx receipts in arrival order —
    /// supplied by the caller (the actor / replayer holds them), persisted by
    /// the writer into the `receipts` + `tx_hash_index` tables so
    /// `eth_getTransactionReceipt` answers from durable state after a restart
    /// (#109: this was hardcoded empty and the read path was dead).
    pub fn finalize(self, block_number: u64, receipts: Vec<kardamom_types::Receipt>) -> BlockDelta {
        let accounts = self
            .accounts
            .into_iter()
            .map(|(address, (nonce, balance, code_hash))| AccountChange {
                address,
                nonce,
                balance,
                code_hash,
            })
            .collect();
        let storage = self
            .storage
            .into_iter()
            .map(|((address, key), value)| StorageChange {
                address,
                key,
                value,
            })
            .collect();
        let code = self
            .code
            .into_iter()
            .map(|(code_hash, code)| CodeEntry { code_hash, code })
            .collect();
        BlockDelta {
            block_number,
            accounts,
            storage,
            code,
            receipts,
        }
    }
}

//NOTE: No `block_delta_root` / state-root function here. the
// executor does not compute or publish any state-root commitment. The sealed
// BlockBoundary on tx_receipts is slim; the state writer flushes the delta to
// libmdbx, and that's the end of the executor's role in block closure.

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{Address, U256};

    fn sample_account(b: u64, n: u64) -> (u64, U256, B256) {
        (n, U256::from(b), B256::repeat_byte(0xCC))
    }

    #[test]
    fn empty_write_set_has_stable_hash() {
        let h1 = WriteSet::default().hash();
        let h2 = WriteSet::default().hash();
        assert_eq!(h1, h2);
    }

    #[test]
    fn hash_is_independent_of_insertion_order() {
        // BTreeMap canonicalizes order, so even if a buggy revm wrapper inserted
        // in different orders, the hash is the same.
        let a1 = Address::from([0x11u8; 20]);
        let a2 = Address::from([0x22u8; 20]);

        let mut ws_a = WriteSet::default();
        ws_a.accounts.insert(a1, sample_account(10, 1));
        ws_a.accounts.insert(a2, sample_account(20, 2));
        ws_a.storage
            .insert((a1, B256::from(U256::from(1u64))), U256::from(100u64));
        ws_a.storage
            .insert((a2, B256::from(U256::from(2u64))), U256::from(200u64));

        let mut ws_b = WriteSet::default();
        ws_b.accounts.insert(a2, sample_account(20, 2));
        ws_b.accounts.insert(a1, sample_account(10, 1));
        ws_b.storage
            .insert((a2, B256::from(U256::from(2u64))), U256::from(200u64));
        ws_b.storage
            .insert((a1, B256::from(U256::from(1u64))), U256::from(100u64));

        assert_eq!(ws_a.hash(), ws_b.hash());
    }

    #[test]
    fn hash_differs_on_value_change() {
        let addr = Address::from([0x11u8; 20]);

        let mut ws_a = WriteSet::default();
        ws_a.storage
            .insert((addr, B256::from(U256::from(1u64))), U256::from(100u64));

        let mut ws_b = WriteSet::default();
        ws_b.storage
            .insert((addr, B256::from(U256::from(1u64))), U256::from(101u64));

        assert_ne!(ws_a.hash(), ws_b.hash());
    }

    #[test]
    fn hash_differs_on_nonce_change() {
        let addr = Address::from([0x11u8; 20]);
        let mut ws_a = WriteSet::default();
        ws_a.accounts.insert(addr, sample_account(0, 0));
        let mut ws_b = WriteSet::default();
        ws_b.accounts.insert(addr, sample_account(0, 1));
        assert_ne!(ws_a.hash(), ws_b.hash());
    }

    #[test]
    fn hash_covers_code_bytes() {
        let h = B256::repeat_byte(0xAA);
        let mut ws_a = WriteSet::default();
        ws_a.code.insert(h, Bytes::from_static(&[0x60, 0x00]));
        let mut ws_b = WriteSet::default();
        ws_b.code.insert(h, Bytes::from_static(&[0x60, 0x01]));
        assert_ne!(ws_a.hash(), ws_b.hash());
    }

    #[test]
    fn apply_write_set_merges_and_overwrites() {
        let addr = Address::from([0x11u8; 20]);
        let mut delta = PendingDelta::default();

        let mut ws1 = WriteSet::default();
        ws1.accounts.insert(addr, sample_account(10, 1));
        ws1.storage
            .insert((addr, B256::from(U256::from(1u64))), U256::from(100u64));
        delta.apply(ws1);

        let mut ws2 = WriteSet::default();
        ws2.accounts.insert(addr, sample_account(15, 2));
        ws2.storage
            .insert((addr, B256::from(U256::from(1u64))), U256::from(200u64));
        delta.apply(ws2);

        let (nonce, balance, _ch) = delta.accounts[&addr];
        assert_eq!(balance, U256::from(15u64));
        assert_eq!(nonce, 2);
        assert_eq!(
            delta.storage[&(addr, B256::from(U256::from(1u64)))],
            U256::from(200u64)
        );
    }

    #[test]
    fn finalize_produces_canonical_block_delta() {
        let a1 = Address::from([0x11u8; 20]);
        let a2 = Address::from([0x22u8; 20]);
        let mut delta = PendingDelta::default();
        delta.accounts.insert(a2, sample_account(20, 0));
        delta.accounts.insert(a1, sample_account(10, 1));

        let receipts = vec![kardamom_types::Receipt {
            block_number: 7,
            ..Default::default()
        }];
        let bd = delta.finalize(7, receipts);
        assert_eq!(bd.block_number, 7);
        assert_eq!(bd.accounts.len(), 2);
        // BTreeMap iteration order = ascending; verify deterministic order.
        assert_eq!(bd.accounts[0].address, a1);
        assert_eq!(bd.accounts[1].address, a2);
        // #109: the block's receipts ride INSIDE the delta so the writer
        // persists them (receipts + tx_hash_index tables) — no longer empty.
        assert_eq!(bd.receipts.len(), 1);
    }
}
