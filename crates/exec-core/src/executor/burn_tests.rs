//! Tests for `executor::burn`: the nonce burn of a skipped transaction,
//! on the sequential executor.

use alloy_primitives::{Address, B256, U256, address};
use alloy_signer_local::PrivateKeySigner;
use kardamom_test_support::LegacyTx;
use kardamom_types::upgrades::{CHAIN_STATE, FEATURE_SEALER_V3};
use kardamom_types::{BlockBoundaryStart, EMPTY_WRITE_SET_HASH, SkipReason, TxEnvelope};
use revm::primitives::KECCAK_EMPTY;

use crate::block_env::ExecEnv;
use crate::delta::{PendingDelta, WriteSet};
use crate::executor::Executor;
use crate::executor::test_support::{boundary, slot};
use crate::features::activation_slot;
use crate::state::MockStateDatabase;

const PAYEE: Address = address!("0000000000000000000000000000000000001234");
/// The time the test blocks execute with, in ms.
const NOW_MS: u64 = 10_000;
/// A balance that pays one transfer of [`SPEND`] at [`GAS_PRICE`], not two.
const FUNDS: u64 = 1_000_000;
const SPEND: u64 = 600_000;
const GAS_PRICE: u128 = 10;

/// A sender at chain nonce `nonce` with [`FUNDS`], and the feature
/// scheduled at `activation_ms` (0: never).
struct Chain {
    signer: PrivateKeySigner,
    snap: MockStateDatabase,
}

impl Chain {
    fn new(nonce: u64, activation_ms: u64) -> Self {
        let signer = PrivateKeySigner::random();
        let snap = MockStateDatabase::builder()
            .account(signer.address(), U256::from(FUNDS), nonce, KECCAK_EMPTY)
            // The predeploy exists from genesis. The block cache reads no
            // slot of an account that does not exist.
            .account(CHAIN_STATE, U256::ZERO, 1, B256::repeat_byte(0xC0))
            .storage(
                CHAIN_STATE,
                activation_slot(FEATURE_SEALER_V3),
                U256::from(activation_ms),
            )
            .build();
        Self { signer, snap }
    }

    fn transfer(&self, nonce: u64, value: u64) -> TxEnvelope {
        LegacyTx {
            to: PAYEE,
            nonce,
            value,
            gas_price: GAS_PRICE,
            ..LegacyTx::default()
        }
        .sign(&self.signer)
    }

    /// One block scope that executes at [`NOW_MS`].
    fn scope(&self) -> Executor<&MockStateDatabase> {
        let start = BlockBoundaryStart {
            l2_timestamp: NOW_MS,
            ..boundary(1)
        };
        Executor::new(&self.snap, None, ExecEnv::new(1, &start)).expect("scope")
    }

    fn sender_nonce(&self, ws: &WriteSet) -> Option<u64> {
        ws.account(&self.signer.address()).map(|a| a.nonce)
    }
}

#[test]
fn the_empty_write_set_hash_matches_the_receipt_constant() {
    assert_eq!(WriteSet::default().hash(), EMPTY_WRITE_SET_HASH);
}

/// With the feature active, an insufficient-funds skip at the chain nonce
/// sets the nonce one higher, and writes nothing else. The next nonce of
/// the sender then executes in the same block.
#[test]
fn an_active_flag_burns_the_nonce_of_a_skip() {
    let chain = Chain::new(3, 1);
    let mut scope = chain.scope();
    let (skip, ws) = scope
        .execute_tx(slot(0, 0, 0, 0), &chain.transfer(3, FUNDS), None, None)
        .unwrap();
    assert_eq!(skip.skip_reason, Some(SkipReason::InsufficientFunds));
    assert!(skip.is_invalid_skip() && skip.burns_nonce() && skip.uses_nonce());
    assert_eq!(skip.nonce, 3);
    assert_eq!(skip.write_set_hash, ws.hash());
    assert_eq!(ws.accounts.len(), 1, "only the sender: {ws:?}");
    assert!(ws.storage.is_empty() && ws.code.is_empty());
    let sender = ws.account(&chain.signer.address()).unwrap();
    assert_eq!(
        (sender.nonce, sender.balance, sender.code_hash),
        (4, U256::from(FUNDS), KECCAK_EMPTY),
        "the nonce moves, and no fee is charged"
    );

    let (next, ws) = scope
        .execute_tx(slot(1, 64, 1, 0), &chain.transfer(4, SPEND), None, None)
        .unwrap();
    assert!(next.status, "the next nonce lands: {next:?}");
    assert_eq!(chain.sender_nonce(&ws), Some(5));
}

#[test]
fn an_inactive_flag_burns_nothing() {
    for activation_ms in [0, NOW_MS + 1] {
        let chain = Chain::new(3, activation_ms);
        let (skip, ws) = chain
            .scope()
            .execute_tx(slot(0, 0, 0, 0), &chain.transfer(3, FUNDS), None, None)
            .unwrap();
        assert_eq!(skip.skip_reason, Some(SkipReason::InsufficientFunds));
        assert_eq!(skip.write_set_hash, EMPTY_WRITE_SET_HASH, "{activation_ms}");
        assert!(!skip.burns_nonce() && ws.accounts.is_empty());
    }
}

/// A nonce skip never burns. A skip whose nonce is not the chain nonce
/// burns nothing either, whatever its reason.
#[test]
fn a_skip_off_the_chain_nonce_burns_nothing() {
    let chain = Chain::new(3, 1);
    let mut scope = chain.scope();
    let high = scope
        .execute_tx(slot(0, 0, 0, 0), &chain.transfer(5, 1), None, None)
        .unwrap();
    let low = scope
        .execute_tx(slot(1, 64, 1, 0), &chain.transfer(2, 1), None, None)
        .unwrap();
    let gas = LegacyTx {
        nonce: 7,
        gas_limit: 1,
        ..LegacyTx::default()
    }
    .sign(&chain.signer);
    let gas = scope
        .execute_tx(slot(2, 128, 2, 0), &gas, None, None)
        .unwrap();
    for ((receipt, ws), reason) in [
        (high, SkipReason::NonceTooHigh),
        (low, SkipReason::NonceTooLow),
        (gas, SkipReason::GasLimit),
    ] {
        assert_eq!(receipt.skip_reason, Some(reason));
        assert!(
            !receipt.burns_nonce() && ws.accounts.is_empty(),
            "{reason:?}"
        );
    }
}

/// A burn claims the nonce write in the block BAL, so a validator that
/// seeds from the BAL sees the new nonce.
#[test]
fn a_burn_claims_its_nonce_in_the_block_bal() {
    let chain = Chain::new(0, 1);
    let mut bal = revm::state::bal::Bal::new();
    let (skip, _) = chain
        .scope()
        .execute_tx(
            slot(0, 0, 0, 0),
            &chain.transfer(0, FUNDS),
            Some((&mut bal, 1)),
            None,
        )
        .unwrap();
    assert!(skip.burns_nonce());
    let alloy = bal.into_alloy_bal();
    let sender = alloy
        .iter()
        .find(|a| a.address == chain.signer.address())
        .expect("the sender is claimed");
    assert_eq!(sender.nonce_changes.len(), 1, "{sender:?}");
    assert!(sender.balance_changes.is_empty(), "{sender:?}");
}

/// A burn of an account that does not exist yet creates it at nonce 1.
#[test]
fn a_burn_of_a_new_account_creates_it() {
    let chain = Chain::new(0, 1);
    let stranger = PrivateKeySigner::random();
    let tx = LegacyTx {
        to: PAYEE,
        value: 1,
        gas_price: GAS_PRICE,
        ..LegacyTx::default()
    }
    .sign(&stranger);
    let (skip, ws) = chain
        .scope()
        .execute_tx(slot(0, 0, 0, 0), &tx, None, None)
        .unwrap();
    assert!(skip.burns_nonce());
    assert_eq!(
        ws.account(&stranger.address())
            .map(|a| (a.nonce, a.balance)),
        Some((1, U256::ZERO))
    );
    assert_ne!(ws.hash(), B256::ZERO);
}

/// The one-shot executor that replay uses burns alike.
#[test]
fn execute_once_burns_alike() {
    let chain = Chain::new(3, 1);
    let start = BlockBoundaryStart {
        l2_timestamp: NOW_MS,
        ..boundary(1)
    };
    let (skip, ws) = Executor::execute_once(
        &chain.snap,
        None,
        &PendingDelta::new(),
        ExecEnv::new(1, &start),
        slot(0, 0, 0, 0),
        &chain.transfer(3, FUNDS),
        None,
    )
    .unwrap();
    assert!(skip.burns_nonce());
    assert_eq!(chain.sender_nonce(&ws), Some(4));
}
