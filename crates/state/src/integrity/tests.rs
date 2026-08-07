use super::*;
use alloy_primitives::{Address, U256};
use kardamom_types::{BPosition, BlockBoundary, BlockDelta, Receipt};
use signet_libmdbx::WriteFlags;

use crate::env::{Durability, StateEnvBuilder};
use crate::schema::{TABLE_ACCOUNTS, TABLE_HEADERS, TABLE_RECEIPTS, decode_account_value};
use crate::writer::{StateWriter, TrieMode, WriteBatch};

/// Build a small 2-block chain (with receipts) through the trie-aware
/// writer into `dir`, seeded with a genesis account.
fn build_db(dir: &std::path::Path) {
    let env = StateEnvBuilder::new(dir)
        .durability(Durability::SafeNoSync)
        .open()
        .unwrap();
    let genesis_accounts = vec![kardamom_types::AccountChange {
        address: Address::from([0xAA; 20]),
        nonce: 0,
        balance: U256::from(1_000_000u64),
        code_hash: B256::ZERO,
    }];
    crate::genesis::seed_genesis(&env, &genesis_accounts, &[]).unwrap();
    let handle = StateWriter::spawn_with_trie(env, TrieMode::Incremental).unwrap();
    for block in 1..=2u64 {
        let receipt = Receipt {
            tx_idx: BPosition::from_index(block),
            tx_hash: B256::from(U256::from(0xBEEF00 + block)),
            status: true,
            gas_used: 21_000,
            write_set_hash: B256::from(U256::from(7u64)),
            ..Default::default()
        };
        let delta = BlockDelta {
            block_number: block,
            accounts: vec![kardamom_types::AccountChange {
                address: Address::from([0xAA; 20]),
                nonce: block,
                balance: U256::from(1_000_000 - block * 10),
                code_hash: B256::ZERO,
            }],
            storage: vec![],
            code: vec![],
            receipts: vec![receipt],
        };
        let boundary = BlockBoundary {
            block_number: block,
            end_tx_idx: BPosition::from_index(block),
            l2_timestamp: 1_700_000_000 + block,
            l1_origin: 0,
        };
        handle
            .delta_tx
            .send(WriteBatch::new(boundary, delta))
            .unwrap();
    }
    handle.shutdown().unwrap();
}

fn open(dir: &std::path::Path) -> StateEnv {
    StateEnvBuilder::new(dir)
        .durability(Durability::SafeNoSync)
        .open()
        .unwrap()
}

#[test]
fn sweep_is_clean_on_a_healthy_db() {
    let dir = tempfile::tempdir().unwrap();
    build_db(dir.path());
    let r = sweep(&open(dir.path())).unwrap();
    assert!(r.is_clean(), "problems: {:?}", r.problems);
    assert_eq!(r.last_committed_block, 2);
    assert_eq!(r.receipts, 2);
    assert!(r.state_root.is_some(), "trie writer persists a root");
    assert_eq!(r.state_root, r.rebuilt_root);
}

#[test]
fn sweep_detects_a_flipped_receipt_byte() {
    let dir = tempfile::tempdir().unwrap();
    build_db(dir.path());
    // Corrupt one receipt value in place (what disk rot / a torn write
    // would look like at the row level).
    {
        let env = open(dir.path());
        let txn = env.raw().begin_rw_sync().unwrap();
        let db = txn.open_db(Some(TABLE_RECEIPTS)).unwrap();
        let (k, mut v) = {
            let mut cur = txn.cursor(db).unwrap();
            cur.first::<Vec<u8>, Vec<u8>>().unwrap().unwrap()
        };
        let last = v.len() - 1;
        v[last] ^= 0xFF;
        v.truncate(v.len() - 3); // also shear the tail so rkyv must reject
        txn.put(db, &k, &v, WriteFlags::UPSERT).unwrap();
        txn.commit().unwrap();
    }
    let r = sweep(&open(dir.path())).unwrap();
    assert!(!r.is_clean(), "sweep must flag the corrupted receipt");
    assert!(
        r.problems.iter().any(|p| p.contains("receipts")),
        "problems: {:?}",
        r.problems
    );
}

#[test]
fn sweep_detects_a_headers_gap() {
    let dir = tempfile::tempdir().unwrap();
    build_db(dir.path());
    {
        let env = open(dir.path());
        let txn = env.raw().begin_rw_sync().unwrap();
        let db = txn.open_db(Some(TABLE_HEADERS)).unwrap();
        txn.del(db, crate::schema::encode_block_key(1), None)
            .unwrap();
        txn.commit().unwrap();
    }
    let r = sweep(&open(dir.path())).unwrap();
    assert!(
        r.problems
            .iter()
            .any(|p| p.contains("gap") || p.contains("start at")),
        "problems: {:?}",
        r.problems
    );
}

#[test]
fn deep_compare_identical_dbs_is_empty_and_divergent_is_not() {
    let a = tempfile::tempdir().unwrap();
    let b = tempfile::tempdir().unwrap();
    build_db(a.path());
    build_db(b.path());
    let ea = open(a.path());
    let eb = open(b.path());
    assert!(deep_compare(&ea, &eb).unwrap().is_empty());
    drop(eb);
    // Perturb one account balance in b.
    {
        let eb = open(b.path());
        let txn = eb.raw().begin_rw_sync().unwrap();
        let db = txn.open_db(Some(TABLE_ACCOUNTS)).unwrap();
        let (k, v) = {
            let mut cur = txn.cursor(db).unwrap();
            cur.first::<Vec<u8>, Vec<u8>>().unwrap().unwrap()
        };
        let mut acct = decode_account_value(&v).unwrap();
        acct.balance += U256::from(1u64);
        txn.put(
            db,
            &k,
            crate::schema::encode_account_value(&acct),
            WriteFlags::UPSERT,
        )
        .unwrap();
        txn.commit().unwrap();
    }
    let eb = open(b.path());
    let diffs = deep_compare(&ea, &eb).unwrap();
    assert!(
        diffs.iter().any(|d| d.contains(TABLE_ACCOUNTS)),
        "diffs: {diffs:?}"
    );
}
