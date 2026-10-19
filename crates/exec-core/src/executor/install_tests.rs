//! Tests for `executor::install`: the install of the chain state code by
//! a `setFeature` system deposit, on the fresh-cache deposit path and on
//! the block-scope deposit path.

use alloy_primitives::{B256, U256};
use alloy_rlp::Encodable;
use kardamom_types::epoch::{UpgradeLog, upgrade_from_log};
use kardamom_types::upgrades::{
    CHAIN_STATE, CHAIN_STATE_CODE, CHAIN_STATE_CODE_HASH, FEATURE_SEALER_V3,
};
use kardamom_types::{Deposit, Receipt};

use crate::block_env::ExecEnv;
use crate::delta::{AccountFields, PendingDelta, WriteSet};
use crate::executor::test_support::{boundary, slot};
use crate::executor::{Executor, execute_deposit_tx};
use crate::features::{BlockFeatures, activation_slot};
use crate::state::MockStateDatabase;

/// The time the test block executes with, in ms.
const NOW_MS: u64 = 7_000;

/// The system deposit of an upgrade transaction that schedules
/// `FEATURE_SEALER_V3` at once.
fn upgrade() -> Deposit {
    upgrade_from_log(&UpgradeLog {
        block_number: 40,
        block_hash: B256::repeat_byte(0x40),
        log_index: 0,
        feature_id: U256::from(FEATURE_SEALER_V3),
        activation_timestamp: 0,
    })
}

fn env() -> ExecEnv {
    let mut start = boundary(1);
    start.l2_timestamp = NOW_MS;
    ExecEnv::new(1, &start, BlockFeatures::NONE)
}

/// A chain with the canonical code at [`CHAIN_STATE`]. The account has
/// nonce 1, as the predeploy of `chains/dev-withdrawals.toml` (a contract
/// account starts at nonce 1). An installed account keeps its nonce, so it
/// has nonce 0 ([`assert_installed`]).
fn with_contract() -> MockStateDatabase {
    let hash = CHAIN_STATE_CODE_HASH;
    MockStateDatabase::builder()
        .account(CHAIN_STATE, U256::ZERO, 1, hash)
        .code(hash, bytes::Bytes::from_static(CHAIN_STATE_CODE))
        .build()
}

/// Run `deposit` on both deposit paths over `snap`, require the same
/// receipt, write set and BAL claims, and return the receipt and the
/// write set.
fn both_paths(snap: &MockStateDatabase, deposit: &Deposit) -> (Receipt, WriteSet) {
    let delta = PendingDelta::new();
    let mut fresh_bal = revm::state::bal::Bal::new();
    let (fresh_r, fresh_ws) = execute_deposit_tx(
        snap,
        None,
        &delta,
        env(),
        slot(0, 0, 0, 0),
        deposit,
        Some((&mut fresh_bal, 1)),
    )
    .expect("fresh path");
    let mut scope = Executor::new(snap, None, env()).expect("scope");
    let mut scope_bal = revm::state::bal::Bal::new();
    let (scope_r, scope_ws) = scope
        .execute_deposit(slot(0, 0, 0, 0), deposit, Some((&mut scope_bal, 1)))
        .expect("scope path");
    assert_eq!(scope_ws, fresh_ws, "write sets");
    assert_eq!(scope_r, fresh_r, "receipts");
    let (mut f, mut s) = (Vec::new(), Vec::new());
    fresh_bal.into_alloy_bal().encode(&mut f);
    scope_bal.into_alloy_bal().encode(&mut s);
    assert_eq!(s, f, "BAL claims");
    (fresh_r, fresh_ws)
}

/// The code entry and the account that an install puts in a write set.
/// The account keeps its nonce (0) and its `balance`.
fn assert_installed(ws: &WriteSet, balance: U256) {
    let hash = CHAIN_STATE_CODE_HASH;
    assert_eq!(
        ws.account(&CHAIN_STATE),
        Some(&AccountFields {
            nonce: 0,
            balance,
            code_hash: hash,
        })
    );
    assert!(
        ws.code
            .iter()
            .any(|(h, code)| *h == hash && code.as_ref() == CHAIN_STATE_CODE),
        "the write set holds the code: {ws:?}"
    );
}

#[test]
fn the_first_upgrade_installs_the_code_and_writes_the_word() {
    let snap = MockStateDatabase::builder().build();
    let (receipt, ws) = both_paths(&snap, &upgrade());
    assert!(receipt.status, "{receipt:?}");
    assert_eq!(receipt.logs.len(), 1, "FeatureScheduled");
    assert_installed(&ws, U256::ZERO);
    assert_eq!(
        ws.storage.as_slice(),
        [(
            (CHAIN_STATE, activation_slot(FEATURE_SEALER_V3)),
            U256::from(NOW_MS)
        )]
        .as_slice()
    );
}

#[test]
fn a_chain_with_the_code_installs_nothing() {
    let (receipt, ws) = both_paths(&with_contract(), &upgrade());
    assert!(receipt.status, "{receipt:?}");
    assert_eq!(
        ws.account(&CHAIN_STATE),
        None,
        "the account does not change"
    );
    assert_eq!(
        ws.storage.as_slice(),
        [(
            (CHAIN_STATE, activation_slot(FEATURE_SEALER_V3)),
            U256::from(NOW_MS)
        )]
        .as_slice()
    );
}

#[test]
fn only_a_system_deposit_of_the_upgrader_installs() {
    let snap = MockStateDatabase::builder().build();
    let user = Deposit {
        is_system_transaction: false,
        ..upgrade()
    };
    let stranger = Deposit {
        from: alloy_primitives::Address::repeat_byte(0x77),
        ..upgrade()
    };
    for deposit in [user, stranger] {
        let (receipt, ws) = both_paths(&snap, &deposit);
        assert!(receipt.status, "a call to an empty account: {receipt:?}");
        assert_eq!(ws.account(&CHAIN_STATE), None);
        assert_eq!(ws.code.len(), 0);
        assert_eq!(ws.storage.len(), 0);
    }
}

/// A deposit that revm rejects at validation keeps its install, as it
/// keeps its mint.
#[test]
fn a_rejected_upgrade_keeps_the_install() {
    let snap = MockStateDatabase::builder().build();
    let deposit = Deposit {
        gas_limit: 0,
        ..upgrade()
    };
    let (receipt, ws) = both_paths(&snap, &deposit);
    assert!(receipt.is_invalid_skip(), "{receipt:?}");
    assert_installed(&ws, U256::ZERO);
    assert_eq!(ws.storage.len(), 0);
}

/// An account at [`CHAIN_STATE`] with a balance and no code (someone sent
/// ETH there) gets the code, and keeps its balance.
#[test]
fn an_account_with_a_balance_and_no_code_gets_the_code() {
    let balance = U256::from(5u64) * U256::from(10u64).pow(U256::from(18u64));
    let snap = MockStateDatabase::builder()
        .account(CHAIN_STATE, balance, 0, revm::primitives::KECCAK_EMPTY)
        .build();
    let (receipt, ws) = both_paths(&snap, &upgrade());
    assert!(receipt.status, "{receipt:?}");
    assert_installed(&ws, balance);
    assert_eq!(
        ws.storage.as_slice(),
        [(
            (CHAIN_STATE, activation_slot(FEATURE_SEALER_V3)),
            U256::from(NOW_MS)
        )]
        .as_slice()
    );
}

/// A later record of the same block sees the installed code: a second
/// upgrade in the block installs nothing and writes its word. The block
/// scope sees the first deposit in its cache; the fresh-cache path sees
/// it in the block delta. Both give the same artifact.
#[test]
fn a_second_upgrade_in_the_block_sees_the_code() {
    let snap = MockStateDatabase::builder().build();
    let mut scope = Executor::new(&snap, None, env()).expect("scope");
    let (_, first) = scope
        .execute_deposit(slot(0, 0, 0, 0), &upgrade(), None)
        .expect("first");
    assert_installed(&first, U256::ZERO);
    let mut delta = PendingDelta::new();
    delta.apply(first);
    let second = Deposit {
        source_hash: B256::repeat_byte(0x41),
        ..upgrade()
    };
    let (receipt, ws) = scope
        .execute_deposit(slot(1, 1, 1, 0), &second, None)
        .expect("second, block scope");
    let (fresh_receipt, fresh_ws) =
        execute_deposit_tx(&snap, None, &delta, env(), slot(1, 1, 1, 0), &second, None)
            .expect("second, fresh cache");
    assert_eq!(fresh_ws, ws, "write sets");
    assert_eq!(fresh_receipt, receipt, "receipts");
    assert!(receipt.status, "{receipt:?}");
    assert_eq!(receipt.logs.len(), 1);
    assert_eq!(ws.account(&CHAIN_STATE), None);
}
