//! Regression test for #159: a write set's `code_hash` must not depend on
//! WHERE the account was read from.
//!
//! Genesis-seeded EOAs carry `code_hash = B256::ZERO` in the state DB. revm's
//! CacheDB normalizes zero code hashes to KECCAK_EMPTY for accounts that pass
//! through an execution scope, but a fresh scope reads the DB's value
//! verbatim through `SnapshotRef`. The executor executes a block's txs in ONE
//! scope while the validator batches per BAL chunk — so before the
//! normalization fix, a fresh account's second-ever tx hashed its sender row
//! with KECCAK_EMPTY on the executor (same-scope read) but B256::ZERO on the
//! validator (fresh-scope read) whenever the two txs straddled a validator
//! batch boundary: a wsh-only receipt "divergence" and a validator fail-stop
//! over two spellings of "no code".
//!
//! The test drives the SAME two transfers through both batching shapes and
//! asserts identical write-set hashes.

use alloy_consensus::{SignableTransaction, TxLegacy};
use alloy_eips::eip2718::Encodable2718;
use alloy_network::TxSignerSync;
use alloy_primitives::{Address, B256, TxKind, U256, address};
use alloy_signer_local::PrivateKeySigner;
use kardamom_exec_core::block_env::ExecEnv;
use kardamom_exec_core::delta::{PendingDelta, WriteSet};
use kardamom_exec_core::exec_types::TxIndex;
use kardamom_exec_core::executor::{ExecScope, execute_tx};
use kardamom_exec_core::state::MockStateDatabase;
use kardamom_types::{AccountChange, BPosition, BlockDelta, StorageChange, TxEnvelope};

const CHAIN_ID: u64 = 412346;
const RECIPIENT: Address = address!("000000000000000000000000000000000000dEaD");

fn signer() -> PrivateKeySigner {
    // Anvil dev key #0 — public, dev only.
    "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
        .parse()
        .unwrap()
}

fn transfer(nonce: u64) -> TxEnvelope {
    let s = signer();
    let mut tx = TxLegacy {
        chain_id: Some(CHAIN_ID),
        nonce,
        gas_price: 1_000_000_000,
        gas_limit: 21_000,
        to: TxKind::Call(RECIPIENT),
        value: U256::from(1u64),
        input: Default::default(),
    };
    let sig = s.sign_transaction_sync(&mut tx).unwrap();
    let env = alloy_consensus::TxEnvelope::Legacy(tx.into_signed(sig));
    let mut raw = Vec::new();
    env.encode_2718(&mut raw);
    TxEnvelope {
        correlation_id: 0,
        raw_tx: bytes::Bytes::from(raw),
        sender: s.address(),
        tx_hash: *env.tx_hash(),
    }
}

/// Genesis-shaped mock: the sender exactly as `bin_support` seeds no-code
/// alloc entries — `code_hash = B256::ZERO`, NOT keccak-empty.
fn genesis_db() -> MockStateDatabase {
    MockStateDatabase::builder()
        .account(
            signer().address(),
            U256::from(1000u64) * U256::from(10u64).pow(U256::from(18)),
            0,
            B256::ZERO,
        )
        .build()
}

fn env() -> ExecEnv {
    ExecEnv {
        chain_id: CHAIN_ID,
        block_number: 1,
        l2_timestamp: 0,
    }
}

fn pos(off: i32) -> BPosition {
    BPosition {
        term_id: 0,
        term_offset: off,
    }
}

fn delta_from(ws: &WriteSet, block_number: u64) -> BlockDelta {
    BlockDelta {
        block_number,
        accounts: ws
            .accounts
            .iter()
            .map(|(address, (nonce, balance, code_hash))| AccountChange {
                address: *address,
                nonce: *nonce,
                balance: *balance,
                code_hash: *code_hash,
            })
            .collect(),
        storage: ws
            .storage
            .iter()
            .map(|((address, key), value)| StorageChange {
                address: *address,
                key: *key,
                value: *value,
            })
            .collect(),
        code: Vec::new(),
        receipts: Vec::new(),
    }
}

#[test]
fn second_tx_write_set_is_scope_invariant() {
    let tx0 = transfer(0);
    let tx1 = transfer(1);

    // Executor shape: both txs in ONE scope (per-block CacheDB).
    let db_a = genesis_db();
    let mut scope = ExecScope::new(&db_a, None, env()).unwrap();
    let (r0_same, _ws0_same) = scope
        .execute_tx(TxIndex(0), pos(0), &tx0, 0, 0, None)
        .unwrap();
    let (r1_same, _ws1_same) = scope
        .execute_tx(TxIndex(1), pos(1), &tx1, 1, r0_same.gas_used, None)
        .unwrap();
    assert!(r0_same.status && r1_same.status, "setup: txs must succeed");

    // Validator shape: tx0 in one scope, its delta committed to the DB, then
    // tx1 in a FRESH scope reading the committed state (batch straddle).
    let db_b = genesis_db();
    let (r0_split, ws0_split) = execute_tx(
        &db_b,
        None,
        &PendingDelta::new(),
        env(),
        TxIndex(0),
        pos(0),
        &tx0,
        0,
        0,
        None,
    )
    .unwrap();
    assert!(r0_split.status);
    db_b.apply_block_delta(&delta_from(&ws0_split, 1));
    let (r1_split, _ws1_split) = execute_tx(
        &db_b,
        None,
        &PendingDelta::new(),
        env(),
        TxIndex(1),
        pos(1),
        &tx1,
        1,
        r0_split.gas_used,
        None,
    )
    .unwrap();
    assert!(r1_split.status);

    // The first tx must already hash identically (both scopes read genesis).
    assert_eq!(
        r0_same.write_set_hash, r0_split.write_set_hash,
        "first-tx write sets diverged between batching shapes"
    );
    // #159: before normalization the same-scope row carried KECCAK_EMPTY and
    // the fresh-scope row carried the DB's B256::ZERO — a false divergence.
    assert_eq!(
        r1_same.write_set_hash, r1_split.write_set_hash,
        "second-tx write sets diverged between batching shapes (#159)"
    );
}
