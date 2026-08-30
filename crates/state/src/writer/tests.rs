use super::*;
use crate::env::{Durability, StateEnvBuilder};
use crate::trie;
use alloy_primitives::{Address, B256, U256};
use kardamom_types::{AccountChange, BPosition, StorageChange};
use std::collections::BTreeMap;

fn acct(addr: u8, nonce: u64, balance: u64) -> AccountChange {
    AccountChange {
        address: Address::from([addr; 20]),
        nonce,
        balance: U256::from(balance),
        code_hash: B256::ZERO,
    }
}

fn slot(addr: u8, key: u8, value: u64) -> StorageChange {
    StorageChange {
        address: Address::from([addr; 20]),
        key: B256::from(U256::from(key)),
        value: U256::from(value),
    }
}

fn boundary(block: u64) -> BlockBoundary {
    BlockBoundary {
        block_number: block,
        end_tx_idx: BPosition::from_index(block),
        l2_timestamp: 1_700_000_000 + block,
        l1_origin: 0,
    }
}

/// An independent oracle. Compute the canonical state root from a pure
/// in-memory model of the accounts and slots, recomputing storage roots
/// from scratch.
fn model_root(
    accounts: &BTreeMap<Address, (u64, U256, B256)>,
    storage: &BTreeMap<Address, BTreeMap<B256, U256>>,
) -> B256 {
    trie::state_root(accounts.iter().map(|(addr, &(nonce, balance, code_hash))| {
        let sroot = storage
            .get(addr)
            .map(|slots| trie::storage_root(slots.iter().map(|(k, v)| (*k, *v))))
            .unwrap_or_else(trie::empty_root);
        (
            *addr,
            trie::AccountTrieParts {
                nonce,
                balance,
                code_hash,
                storage_root: sroot,
            },
        )
    }))
}

#[test]
fn trie_writer_root_matches_model_and_persists() {
    let dir = tempfile::tempdir().unwrap();

    // Build a 3-block scenario and a parallel model of the final state.
    let blocks = vec![
        BlockDelta {
            block_number: 1,
            accounts: vec![acct(0x11, 1, 1000), acct(0x22, 0, 500)],
            storage: vec![slot(0x11, 1, 7), slot(0x11, 2, 9)],
            code: vec![],
            receipts: vec![],
        },
        BlockDelta {
            block_number: 2,
            // 0x11 changes a slot; this is storage-only, not in accounts.
            // 0x22 gets funds. 0x33 is created as a real, non-empty
            // account, so its slot below is reachable. An empty
            // account's storage is pruned.
            accounts: vec![acct(0x22, 1, 800), acct(0x33, 1, 1)],
            storage: vec![slot(0x11, 1, 0), slot(0x33, 5, 42)], // Zero one slot, add 0x33's slot.
            code: vec![],
            receipts: vec![],
        },
        BlockDelta {
            block_number: 3,
            accounts: vec![acct(0x33, 2, 0), acct(0x11, 2, 1000)],
            storage: vec![slot(0x11, 2, 99)],
            code: vec![],
            receipts: vec![],
        },
    ];

    let mut model_accts: BTreeMap<Address, (u64, U256, B256)> = BTreeMap::new();
    let mut model_stor: BTreeMap<Address, BTreeMap<B256, U256>> = BTreeMap::new();

    // Submit all blocks, then shut down. `shutdown()` drains every queued
    // batch and joins, so block 3 is durably committed when it returns.
    {
        let env = StateEnvBuilder::new(dir.path())
            .durability(Durability::SafeNoSync)
            .open()
            .unwrap();
        let mut handle = StateWriter::spawn_with_trie(env, TrieMode::Incremental).unwrap();
        for delta in &blocks {
            for s in &delta.storage {
                model_stor
                    .entry(s.address)
                    .or_default()
                    .insert(s.key, s.value);
            }
            for a in &delta.accounts {
                model_accts.insert(a.address, (a.nonce, a.balance, a.code_hash));
            }
            handle
                .delta_tx
                .send(WriteBatch::new(boundary(delta.block_number), delta.clone()))
                .unwrap();
        }
        handle.shutdown().unwrap();
    }

    // Reopen and verify. The persisted root equals an independent rebuild
    // of the model, is non-empty, and survived the restart.
    let env = StateEnvBuilder::new(dir.path())
        .durability(Durability::SafeNoSync)
        .open()
        .unwrap();
    let snap = StateSnapshot::open(&env).unwrap();
    assert_eq!(snap.block_number(), 3);
    let persisted = snap.state_root().unwrap().expect("trie writer set a root");
    assert_eq!(persisted, model_root(&model_accts, &model_stor));
    assert_ne!(persisted, trie::empty_root());
}

#[test]
fn shadow_check_agrees_every_block() {
    // In ShadowCheck mode, the writer rebuilds the root from scratch each
    // block, and stops on a mismatch. The independent rebuild must agree
    // with the incremental walker every block, so the writer shuts down
    // cleanly.
    let dir = tempfile::tempdir().unwrap();
    let env = StateEnvBuilder::new(dir.path())
        .durability(Durability::SafeNoSync)
        .open()
        .unwrap();
    let mut handle =
        StateWriter::spawn_with_trie(env, TrieMode::ShadowCheck { every_n: 1 }).unwrap();
    let blocks = vec![
        BlockDelta {
            block_number: 1,
            accounts: vec![acct(0x11, 1, 1000), acct(0x22, 1, 500)],
            storage: vec![slot(0x11, 1, 7)],
            code: vec![],
            receipts: vec![],
        },
        BlockDelta {
            block_number: 2,
            accounts: vec![acct(0x33, 1, 9)],
            storage: vec![slot(0x11, 2, 8), slot(0x33, 1, 1)],
            code: vec![],
            receipts: vec![],
        },
        BlockDelta {
            block_number: 3,
            accounts: vec![acct(0x11, 2, 1001)],
            storage: vec![slot(0x11, 1, 0)],
            code: vec![],
            receipts: vec![],
        },
    ];
    for d in &blocks {
        handle
            .delta_tx
            .send(WriteBatch::new(boundary(d.block_number), d.clone()))
            .unwrap();
    }
    // A shadow-check mismatch would surface here as the writer's Err result.
    handle.shutdown().expect("shadow-check agreed every block");
}
