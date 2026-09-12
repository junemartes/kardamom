//! Tests for `executor::scope`.

use super::*;
use crate::executor::test_support::{boundary, run_once, slot};
use crate::state::MockStateDatabase;
use alloy_consensus::{SignableTransaction, TxLegacy};
use alloy_eips::eip2718::Encodable2718;
use alloy_network::TxSignerSync;
use alloy_primitives::Bytes as AlloyBytes;
use alloy_primitives::{B256, TxKind as APTxKind, U256, address, keccak256};
use alloy_signer_local::PrivateKeySigner;
use bytes::Bytes;
use kardamom_types::TxEnvelope as KtTxEnvelope;
use revm::primitives::KECCAK_EMPTY;

fn signed_transfer(from: &PrivateKeySigner, to: Address, value: u64, nonce: u64) -> KtTxEnvelope {
    let mut tx = TxLegacy {
        chain_id: Some(1),
        nonce,
        gas_price: 0,
        gas_limit: 21_000,
        to: APTxKind::Call(to),
        value: U256::from(value),
        input: AlloyBytes::new(),
    };
    let sig = from.sign_transaction_sync(&mut tx).expect("sign");
    let alloy_env: alloy_consensus::TxEnvelope = tx.into_signed(sig).into();
    let raw_tx = Bytes::from(alloy_env.encoded_2718());
    let tx_hash = keccak256(&raw_tx);
    KtTxEnvelope {
        correlation_id: 0,
        raw_tx,
        sender: from.address(),
        tx_hash,
    }
}

// -- Deterministically invalid canonical txs skip, never halt ---------

#[test]
fn nonce_too_low_skips_with_marker_receipt_and_chain_continues() {
    let signer = PrivateKeySigner::random();
    let from = signer.address();
    let to = address!("0000000000000000000000000000000000001234");
    // The sender's canonical nonce is 5. A nonce-3 tx (a duplicate
    // past every dedup layer) is deterministically invalid.
    let snap = MockStateDatabase::builder()
        .account(from, U256::from(10u128.pow(18)), 5, KECCAK_EMPTY)
        .build();
    let delta = PendingDelta::new();
    let env = ExecEnv::new(1, &boundary(1));

    let stale = signed_transfer(&signer, to, 1_000, 3);
    let (receipt, ws) = run_once(
        &snap,
        &delta,
        env,
        slot(0, 0, 0, 77),
        &stale,
        None,
        "invalid tx must SKIP, not error",
    );
    assert!(receipt.is_invalid_skip(), "status=false, gas_used=0 marker");
    assert_eq!(
        receipt.skip_reason,
        Some(SkipReason::NonceTooLow),
        "typed cause on the wire"
    );
    assert_eq!(receipt.tx_hash, stale.tx_hash);
    assert_eq!(receipt.nonce, 3);
    assert_eq!(
        receipt.cumulative_gas_used, 77,
        "gas accounting unchanged by a skip"
    );
    assert_eq!(
        receipt.write_set_hash,
        WriteSet::default().hash(),
        "a skip writes NOTHING (empty-set hash on every re-exec path)"
    );
    assert!(ws.accounts.is_empty() && ws.storage.is_empty() && ws.code.is_empty());

    // The chain continues. The sender's real next tx, nonce 5, executes.
    let env2 = ExecEnv::new(1, &boundary(1));
    let live = signed_transfer(&signer, to, 1_000, 5);
    let (r2, _) = run_once(
        &snap,
        &delta,
        env2,
        slot(1, 64, 1, 77),
        &live,
        None,
        "valid tx after a skip",
    );
    assert!(r2.status);
    assert!(!r2.is_invalid_skip());
    assert_eq!(r2.skip_reason, None, "an executed tx carries no reason");
}

#[test]
fn undecodable_raw_tx_skips_with_marker_receipt() {
    let signer = PrivateKeySigner::random();
    let snap = MockStateDatabase::builder().build();
    let delta = PendingDelta::new();
    let env = ExecEnv::new(1, &boundary(1));
    let garbage = KtTxEnvelope {
        correlation_id: 9,
        raw_tx: Bytes::from(vec![0xde, 0xad, 0xbe, 0xef]),
        sender: signer.address(),
        tx_hash: keccak256([0xde, 0xad, 0xbe, 0xef]),
    };
    let (receipt, ws) = run_once(
        &snap,
        &delta,
        env,
        slot(0, 0, 0, 0),
        &garbage,
        None,
        "undecodable bytes must SKIP, not error",
    );
    assert!(receipt.is_invalid_skip());
    assert_eq!(receipt.skip_reason, Some(SkipReason::Undecodable));
    assert_eq!(receipt.nonce, 0, "nonce unknowable from undecodable bytes");
    assert_eq!(receipt.write_set_hash, WriteSet::default().hash());
    assert!(ws.accounts.is_empty());
}

#[test]
fn simple_transfer_produces_write_set_and_success_receipt() {
    let signer = PrivateKeySigner::random();
    let from = signer.address();
    let to = address!("0000000000000000000000000000000000001234");

    let snap = MockStateDatabase::builder()
        .account(from, U256::from(10u128.pow(18)), 0, KECCAK_EMPTY)
        .build();
    let delta = PendingDelta::new();
    let env = ExecEnv::new(1, &boundary(1));

    let env_tx = signed_transfer(&signer, to, 1_000, 0);
    let (receipt, ws) = run_once(
        &snap,
        &delta,
        env,
        slot(0, 0, 0, 0),
        &env_tx,
        None,
        "execute",
    );

    // The receipt's tx_hash must equal the inbound envelope's
    // tx_hash, byte for byte. The executor never recomputes it.
    assert_eq!(receipt.tx_hash, env_tx.tx_hash);
    assert!(receipt.status); // success is bool true (kardamom_types::Receipt)
    assert!(receipt.gas_used >= 21_000);
    // Both accounts are touched: the sender (balance and nonce) and
    // the recipient (balance).
    assert!(ws.account(&from).is_some());
    assert!(ws.account(&to).is_some());
    assert_eq!(ws.account(&to).unwrap().balance, U256::from(1_000u64));
    // No storage or code writes for a plain transfer.
    assert!(ws.storage.is_empty());
    assert!(ws.code.is_empty());

    // RPC enrichment populated by execute_tx.
    assert_eq!(receipt.from, from);
    assert_eq!(receipt.to, Some(to));
    assert_eq!(receipt.contract_address, None);
    assert_eq!(receipt.nonce, 0);
    assert_eq!(receipt.effective_gas_price, 0); // the tx was built with gas_price = 0
    assert_eq!(receipt.block_number, 1);
    assert_eq!(receipt.transaction_index, 0);
    assert_eq!(receipt.cumulative_gas_used, receipt.gas_used);
}

#[test]
fn second_tx_sees_first_tx_balance_via_delta() {
    let signer = PrivateKeySigner::random();
    let from = signer.address();
    let to = address!("00000000000000000000000000000000000ABCDE");

    let snap = MockStateDatabase::builder()
        .account(from, U256::from(10u128.pow(18)), 0, KECCAK_EMPTY)
        .build();
    let env = ExecEnv::new(1, &boundary(1));

    let mut delta = PendingDelta::new();
    // First transfer.
    let tx1 = signed_transfer(&signer, to, 100, 0);
    let (r1, ws1) = run_once(
        &snap,
        &delta,
        env,
        slot(0, 0, 0, 0),
        &tx1,
        None,
        "execute 1",
    );
    assert!(r1.status);
    assert_eq!(r1.tx_hash, tx1.tx_hash); // copied, not recomputed
    assert_eq!(r1.nonce, 0);
    assert_eq!(r1.transaction_index, 0);
    let gas_after_tx1 = r1.cumulative_gas_used;
    delta.apply(ws1);

    // Second transfer from the same sender. The nonce must be 1, and
    // the sender balance must already show a debit of 100.
    let tx2 = signed_transfer(&signer, to, 50, 1);
    let (r2, ws2) = run_once(
        &snap,
        &delta,
        env,
        slot(1, 1, 1, gas_after_tx1),
        &tx2,
        None,
        "execute 2",
    );
    assert!(r2.status);
    assert_eq!(r2.tx_hash, tx2.tx_hash);
    assert_eq!(ws2.account(&to).unwrap().balance, U256::from(150u64));
    assert_eq!(ws2.account(&from).unwrap().nonce, 2); // nonce
    // RPC enrichment: tx2 sees a higher nonce and transaction_index.
    // cumulative_gas_used adds up across both txs in the block.
    assert_eq!(r2.nonce, 1);
    assert_eq!(r2.transaction_index, 1);
    assert_eq!(r2.cumulative_gas_used, gas_after_tx1 + r2.gas_used);
}

/// Regression: EIP-7928 capture must actually populate the block BAL
/// when a BAL handle is supplied to `execute_tx`. An
/// empty BAL means the validator has nothing to verify or seed from.
#[test]
fn execute_tx_captures_into_the_block_bal() {
    let signer = PrivateKeySigner::random();
    let from = signer.address();
    let to = address!("0000000000000000000000000000000000005678");
    let snap = MockStateDatabase::builder()
        .account(from, U256::from(10u128.pow(18)), 0, KECCAK_EMPTY)
        .build();
    let delta = PendingDelta::new();
    let env = ExecEnv::new(1, &boundary(1));
    let tx = signed_transfer(&signer, to, 1_000, 0);

    let mut bal = revm::state::bal::Bal::new();
    let (_receipt, ws) = run_once(
        &snap,
        &delta,
        env,
        slot(0, 0, 0, 0),
        &tx,
        Some((&mut bal, 1)),
        "execute",
    );
    assert!(!ws.accounts.is_empty(), "the tx wrote accounts");

    let alloy = bal.into_alloy_bal();
    assert!(
        !alloy.is_empty(),
        "capture produced an EMPTY BAL for a tx that wrote {} accounts",
        ws.accounts.len()
    );
    // The sender and recipient must both appear, with balance or
    // nonce claims.
    let has_sender = alloy.iter().any(|a| {
        a.address == from && (!a.balance_changes.is_empty() || !a.nonce_changes.is_empty())
    });
    assert!(
        has_sender,
        "sender's balance/nonce change must be claimed: {alloy:?}"
    );
}

/// Production shape: the second tx in a block executes against a
/// non-empty `delta`, seeded into the `CacheDB`. Capture must still
/// record it. An earlier bug produced empty BALs once deltas grew to
/// about 76KB/block, even though empty-delta tests passed.
#[test]
fn execute_tx_captures_with_a_seeded_delta() {
    let signer = PrivateKeySigner::random();
    let from = signer.address();
    let to = address!("0000000000000000000000000000000000009999");
    let snap = MockStateDatabase::builder()
        .account(from, U256::from(10u128.pow(18)), 0, KECCAK_EMPTY)
        .build();
    let mut delta = PendingDelta::new();
    let env = ExecEnv::new(1, &boundary(1));

    // tx1 populates the delta (as in a real block).
    let tx1 = signed_transfer(&signer, to, 1_000, 0);
    let mut bal = revm::state::bal::Bal::new();
    let (_r1, ws1) = run_once(
        &snap,
        &delta,
        env,
        slot(0, 0, 0, 0),
        &tx1,
        Some((&mut bal, 1)),
        "execute 1",
    );
    delta.apply(ws1);
    let after_tx1 = bal.clone().into_alloy_bal().len();

    // tx2 runs with the seeded delta. This is the production path.
    let tx2 = signed_transfer(&signer, to, 500, 1);
    let (_r2, ws2) = run_once(
        &snap,
        &delta,
        env,
        slot(1, 64, 1, 21_000),
        &tx2,
        Some((&mut bal, 2)),
        "execute 2",
    );
    assert!(!ws2.accounts.is_empty(), "tx2 wrote accounts");

    let alloy = bal.into_alloy_bal();
    assert!(after_tx1 > 0, "tx1 must be captured");
    assert!(!alloy.is_empty(), "capture must survive a seeded delta");
    // tx2's claims must be present: some account carries a
    // bal_index 2 change.
    let has_tx2 = alloy.iter().any(|a| {
        a.balance_changes.iter().any(|c| c.block_access_index == 2)
            || a.nonce_changes.iter().any(|c| c.block_access_index == 2)
    });
    assert!(has_tx2, "tx2's claims missing from BAL: {alloy:?}");
}

/// A deposit that revm rejects at validation returns a failed receipt,
/// not an error, on the block scope. The scope keeps running: a normal
/// tx right after it still executes.
#[test]
fn deposit_validation_failure_on_the_scope_does_not_poison_the_block() {
    let signer = PrivateKeySigner::random();
    let from = signer.address();
    let to = address!("0000000000000000000000000000000000004321");
    let snap = MockStateDatabase::builder()
        .account(from, U256::from(10u128.pow(18)), 0, KECCAK_EMPTY)
        .build();
    let env = ExecEnv::new(1, &boundary(1));
    let mut scope = Executor::new(&snap, None, env).expect("scope");

    // `gas_limit: 0` is below the intrinsic cost: revm rejects it at
    // validation.
    let deposit = kardamom_types::Deposit {
        source_hash: B256::repeat_byte(0x11),
        from,
        to: Some(to),
        mint: 1_000,
        value: U256::ZERO,
        gas_limit: 0,
        is_system_transaction: false,
        input: Bytes::new(),
    };
    let (receipt, ws) = scope
        .execute_deposit(slot(0, 0, 0, 0), &deposit, None)
        .expect("a failed receipt, not an error");
    assert!(receipt.is_invalid_skip());
    assert_eq!(receipt.skip_reason, Some(SkipReason::GasLimit));
    // The mint stays even though the inner call never ran.
    assert_eq!(
        ws.account(&from).unwrap().balance,
        U256::from(10u128.pow(18) + 1_000)
    );

    // The scope keeps running: the sender's real next tx still executes.
    let tx = signed_transfer(&signer, to, 1_000, 0);
    let (r2, _) = scope
        .execute_tx(slot(1, 1, 1, receipt.cumulative_gas_used), &tx, None, None)
        .expect("execute");
    assert!(r2.status);
    assert!(!r2.is_invalid_skip());
}
