//! Regression test: a write set's `code_hash` must not depend on where
//! the account was read from.
//!
//! Genesis-seeded EOAs carry `code_hash = B256::ZERO` in the state DB.
//! Revm's `CacheDB` normalizes zero code hashes to `KECCAK_EMPTY` for
//! accounts that pass through an execution scope, but a fresh scope reads
//! the DB's value verbatim through `SnapshotRef`. The executor executes a
//! block's txs in one scope, while the validator batches per BAL chunk.
//! Before the normalization fix, a fresh account's second-ever tx hashed
//! its sender row with `KECCAK_EMPTY` on the executor (a same-scope read)
//! but `B256::ZERO` on the validator (a fresh-scope read), whenever the two
//! txs straddled a validator batch boundary. That gave a false receipt
//! divergence and a validator fail-stop, over two spellings of "no code".
//!
//! This test drives the same two transfers through both batching shapes,
//! and asserts identical write-set hashes.

use alloy_primitives::{Address, B256, U256, address};
use kardamom_exec_core::block_env::ExecEnv;
use kardamom_exec_core::delta::{PendingDelta, WriteSet};
use kardamom_exec_core::executor::Executor;
use kardamom_exec_core::state::MockStateDatabase;
use kardamom_test_support::{LegacyTx, anvil_signer_0};
use kardamom_types::{BlockDelta, TxEnvelope};

mod common;
use common::slot;

const CHAIN_ID: u64 = 412_346;
const RECIPIENT: Address = address!("000000000000000000000000000000000000dEaD");

fn transfer(nonce: u64) -> TxEnvelope {
    LegacyTx {
        chain_id: CHAIN_ID,
        to: RECIPIENT,
        nonce,
        value: 1,
        gas_price: 1_000_000_000,
        ..Default::default()
    }
    .sign(&anvil_signer_0())
}

/// A genesis-shaped mock. The sender is set up exactly as `bin_support`
/// seeds no-code alloc entries: `code_hash = B256::ZERO`, not
/// keccak-empty.
fn genesis_db() -> MockStateDatabase {
    MockStateDatabase::builder()
        .account(
            anvil_signer_0().address(),
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

/// `PendingDelta::apply` plus `finalize` already builds a `BlockDelta`
/// from a `WriteSet` exactly this way; this is just that path with an
/// empty receipt list, for a test that only cares about accounts and
/// storage.
fn delta_from(ws: &WriteSet, block_number: u64) -> BlockDelta {
    let mut d = PendingDelta::new();
    d.apply(ws.clone());
    d.finalize(block_number, Vec::new())
}

#[test]
fn second_tx_write_set_is_scope_invariant() {
    let tx0 = transfer(0);
    let tx1 = transfer(1);

    // Executor shape: both txs in one scope (per-block CacheDB).
    let db_a = genesis_db();
    let mut scope = Executor::new(&db_a, None, env()).unwrap();
    let (r0_same, _ws0_same) = scope
        .execute_tx(slot(0, 0, 0, 0), &tx0, None, None)
        .unwrap();
    let (r1_same, _ws1_same) = scope
        .execute_tx(slot(1, 1, 1, r0_same.gas_used), &tx1, None, None)
        .unwrap();
    assert!(r0_same.status && r1_same.status, "setup: txs must succeed");

    // Validator shape: tx0 runs in one scope, its delta is committed to
    // the DB, then tx1 runs in a fresh scope that reads the committed
    // state (a batch straddle).
    let db_b = genesis_db();
    let (r0_split, ws0_split) = Executor::execute_once(
        &db_b,
        None,
        &PendingDelta::new(),
        env(),
        slot(0, 0, 0, 0),
        &tx0,
        None,
    )
    .unwrap();
    assert!(r0_split.status);
    db_b.apply_block_delta(&delta_from(&ws0_split, 1));
    let (r1_split, _ws1_split) = Executor::execute_once(
        &db_b,
        None,
        &PendingDelta::new(),
        env(),
        slot(1, 1, 1, r0_split.gas_used),
        &tx1,
        None,
    )
    .unwrap();
    assert!(r1_split.status);

    // The first tx must already hash identically, since both scopes read genesis.
    assert_eq!(
        r0_same.write_set_hash, r0_split.write_set_hash,
        "first-tx write sets diverged between batching shapes"
    );
    // Before normalization, the same-scope row carried KECCAK_EMPTY and
    // the fresh-scope row carried the DB's B256::ZERO: a false divergence.
    assert_eq!(
        r1_same.write_set_hash, r1_split.write_set_hash,
        "second-tx write sets diverged between batching shapes"
    );
}
