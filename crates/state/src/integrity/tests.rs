use super::*;
use alloy_primitives::{Address, U256};
use kardamom_types::{BPosition, BlockBoundary, BlockDelta, Receipt};
use signet_libmdbx::WriteFlags;

use crate::env::{Durability, StateEnvBuilder};
use crate::schema::{TABLE_ACCOUNTS, TABLE_HEADERS, TABLE_RECEIPTS, decode_account_value};
use crate::writer::{StateWriter, TrieMode, WriteBatch};

/// Build a small 2-block chain, with receipts, through the trie-aware
/// writer into `dir`. Seed it with a genesis account.
fn build_db(dir: &std::path::Path) {
    build_db_with(dir, [WriteBatch::new, WriteBatch::new]);
}

/// How a fixture writes one block: the receipt's hash and position go to
/// a reference when the writer keeps one.
type BlockWrite = fn(BlockBoundary, BlockDelta) -> WriteBatch;

/// The block written with its reference on an archive.
fn with_ref(boundary: BlockBoundary, delta: BlockDelta) -> WriteBatch {
    let refs = delta
        .receipts
        .iter()
        .map(|r| kardamom_types::TxRef::new(r.tx_hash, 0, r.tx_idx, -9))
        .collect();
    WriteBatch::with_refs(boundary, delta, refs)
}

/// [`build_db`] with block `n` written by `writes[n - 1]`.
fn build_db_with(dir: &std::path::Path, writes: [BlockWrite; 2]) {
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
    let mut handle = StateWriter::spawn_with_trie(env, TrieMode::Incremental).unwrap();
    for (block, write) in (1..=2u64).zip(writes) {
        let receipt = Receipt {
            tx_idx: BPosition::from_index(block),
            tx_hash: B256::from(U256::from(0x00BE_EF00 + block)),
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
            base_fee: 0,
            gas_used: 0,
        };
        handle.delta_tx.send(write(boundary, delta)).unwrap();
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
    // Corrupt one receipt value in place. This is what disk rot or a
    // torn write looks like at the row level.
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
        v.truncate(v.len() - 3); // Also cut the tail, so rkyv must reject this value.
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
    // Change one account balance in b.
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

/// Append block 3 to `dir`: empty when `receipts` is false, else one
/// receipt with an account change.
fn append_block_3(dir: &std::path::Path, receipts: bool) {
    let env = StateEnvBuilder::new(dir)
        .durability(Durability::SafeNoSync)
        .open()
        .unwrap();
    let mut handle = StateWriter::spawn_with_trie(env, TrieMode::Incremental).unwrap();
    let end = if receipts {
        BPosition::from_index(3)
    } else {
        BPosition::from_index(2)
    };
    let delta = BlockDelta {
        block_number: 3,
        accounts: if receipts {
            vec![kardamom_types::AccountChange {
                address: Address::from([0xAA; 20]),
                nonce: 3,
                balance: U256::from(1_000_000 - 30u64),
                code_hash: B256::ZERO,
            }]
        } else {
            vec![]
        },
        storage: vec![],
        code: vec![],
        receipts: if receipts {
            vec![Receipt {
                tx_idx: BPosition::from_index(3),
                tx_hash: B256::from(U256::from(0x00BE_EF03u64)),
                status: true,
                gas_used: 21_000,
                write_set_hash: B256::from(U256::from(7u64)),
                ..Default::default()
            }]
        } else {
            vec![]
        },
    };
    let boundary = BlockBoundary {
        block_number: 3,
        end_tx_idx: end,
        l2_timestamp: 1_700_000_003,
        l1_origin: 0,
        base_fee: 0,
        gas_used: 0,
    };
    handle
        .delta_tx
        .send(WriteBatch::new(boundary, delta))
        .unwrap();
    handle.shutdown().unwrap();
}

#[test]
fn a_bounded_compare_tolerates_one_empty_tail_block_only() {
    let a = tempfile::tempdir().unwrap();
    let b = tempfile::tempdir().unwrap();
    build_db(a.path());
    build_db(b.path());
    append_block_3(b.path(), false);
    let ea = open(a.path());
    let eb = open(b.path());
    assert!(!deep_compare(&ea, &eb).unwrap().is_empty());
    assert!(deep_compare_to(&ea, &eb, 2).unwrap().is_empty());
    drop(eb);
    let c = tempfile::tempdir().unwrap();
    build_db(c.path());
    append_block_3(c.path(), true);
    let ec = open(c.path());
    let diffs = deep_compare_to(&ea, &ec, 2).unwrap();
    assert!(
        diffs
            .iter()
            .any(|d| d.starts_with("receipts: extra key in b")),
        "{diffs:?}"
    );
}

/// A node that rebuilt a block from L1 keeps no archive reference for its
/// transactions. The deep compare accepts that row against a peer's row
/// with a reference at the same position. A row without a reference past
/// the rebuilt mark is still a difference.
#[test]
fn a_rebuilt_row_matches_a_referenced_row_only_up_to_the_mark() {
    let live = tempfile::tempdir().unwrap();
    let rebuilt = tempfile::tempdir().unwrap();
    let unmarked = tempfile::tempdir().unwrap();
    build_db_with(live.path(), [with_ref, with_ref]);
    build_db_with(rebuilt.path(), [WriteBatch::rebuilt_from_l1, with_ref]);
    build_db_with(
        unmarked.path(),
        [WriteBatch::rebuilt_from_l1, WriteBatch::new],
    );
    let live = open(live.path());
    assert!(
        deep_compare(&open(rebuilt.path()), &live)
            .unwrap()
            .is_empty()
    );
    assert!(
        deep_compare(&live, &open(rebuilt.path()))
            .unwrap()
            .is_empty()
    );
    let diffs = deep_compare(&open(unmarked.path()), &live).unwrap();
    assert_eq!(diffs.len(), 1, "{diffs:?}");
    assert!(
        diffs[0].starts_with("tx_hash_index[") && diffs[0].contains("(8 vs 21 bytes)"),
        "{diffs:?}"
    );
}
