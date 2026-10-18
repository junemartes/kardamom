//! The nonce burn of a skipped transaction is byte-identical across the
//! sequential driver and the parallel engine: the burn's write set, its
//! receipt, and the next nonce of the sender in the same block.

mod common;

use alloy_primitives::{B256, TxKind, U256};
use alloy_signer_local::PrivateKeySigner;
use common::*;
use kardamom_exec_core::features::activation_slot;
use kardamom_exec_core::state::MockStateDatabase;
use kardamom_footprint::classifier::Stats;
use kardamom_stm::execute::{execute_block_sequential, execute_block_stm};
use kardamom_test_support::seeded_signer;
use kardamom_types::upgrades::{CHAIN_STATE, FEATURE_SEALER_V3};
use kardamom_types::{SkipReason, TxEnvelope};

/// One transfer at the fixture gas costs at most 5e14 wei. The poor
/// sender pays its first transfer, and then has too little for a second
/// one of the same value, but enough for one with no value.
const POOR_FUNDS: u64 = 600_000_000_000_000;
const SPEND: u64 = 50_000_000_000_000;

/// The fixture chain, with a poor sender, and the feature scheduled at
/// `activation_ms` (0: never).
fn chain(
    rich: &[PrivateKeySigner],
    poor: &PrivateKeySigner,
    activation_ms: u64,
) -> MockStateDatabase {
    let mut b = MockStateDatabase::builder()
        .account(poor.address(), U256::from(POOR_FUNDS), 0, B256::ZERO)
        .account(CHAIN_STATE, U256::ZERO, 1, B256::repeat_byte(0xC5))
        .storage(
            CHAIN_STATE,
            activation_slot(FEATURE_SEALER_V3),
            U256::from(activation_ms),
        );
    for s in rich {
        b = b.account(s.address(), U256::from(10u128.pow(18)), 0, B256::ZERO);
    }
    let counter_hash = alloy_primitives::keccak256(COUNTER_CODE);
    b = b
        .account(COUNTER, U256::ZERO, 1, counter_hash)
        .code(counter_hash, COUNTER_CODE.to_vec().into());
    b.build()
}

/// The poor sender spends at nonce 0, skips at nonce 1, and sends a
/// transfer with no value at nonce 2, among the transfers of the rich
/// senders.
fn block(rich: &[PrivateKeySigner], poor: &PrivateKeySigner) -> Vec<TxEnvelope> {
    let payee = TxKind::Call(rich[0].address());
    vec![
        tx(poor, 0, payee, SPEND, &[]),
        tx(&rich[0], 0, TxKind::Call(rich[1].address()), 500, &[]),
        tx(poor, 1, payee, POOR_FUNDS, &[]),
        tx(&rich[1], 0, TxKind::Call(COUNTER), 0, &COUNTER_SEL),
        tx(poor, 2, payee, 0, &[]),
        tx(&rich[0], 1, TxKind::Call(COUNTER), 0, &COUNTER_SEL),
    ]
}

fn run(activation_ms: u64) -> Vec<kardamom_types::Receipt> {
    let rich = signers(2);
    let poor = seeded_signer(77);
    let database = chain(&rich, &poor, activation_ms);
    let recs = records(block(&rich, &poor));
    let seq = execute_block_sequential(&database, None, env_of(&database), &recs).unwrap();
    for workers in [1, 2, 4, 8] {
        let out = execute_block_stm(
            &database,
            None,
            env_of(&database),
            &recs,
            &Stats::default(),
            nz(workers),
        )
        .unwrap();
        assert!(!out.fallback, "the workers run the block: w={workers}");
        assert_identical(
            &seq,
            &out.receipts,
            &out.delta,
            &format!("activation={activation_ms} w={workers}"),
        );
    }
    let nonce = seq.1.accounts.get(&poor.address()).map(|a| a.nonce);
    let landed = seq.0[4].status;
    assert_eq!(
        (nonce, landed),
        if activation_ms == 0 {
            (Some(1), false)
        } else {
            (Some(3), true)
        },
        "activation={activation_ms}"
    );
    seq.0
}

#[test]
fn a_burn_is_byte_identical_and_the_next_nonce_lands() {
    let receipts = run(1);
    let skip = &receipts[2];
    assert_eq!(skip.skip_reason, Some(SkipReason::InsufficientFunds));
    assert!(skip.burns_nonce(), "{skip:?}");
}

#[test]
fn with_the_flag_off_a_skip_burns_nothing_and_the_next_nonce_skips() {
    let receipts = run(0);
    assert!(!receipts[2].burns_nonce());
    assert_eq!(receipts[4].skip_reason, Some(SkipReason::NonceTooHigh));
}

/// An earlier transaction of the block funds the poor sender, and its
/// spend comes later. A worker that runs the spend first reads too little
/// and skips. The skip result carries its reads, so the validation finds
/// the stale balance and the spend runs again: every worker count gives
/// the sequential bytes, with the spend executed.
#[test]
fn a_skip_that_an_earlier_transaction_undoes_runs_again() {
    let rich = signers(2);
    let poor = seeded_signer(78);
    let database = chain(&rich, &poor, 1);
    let recs = records(vec![
        tx(
            &rich[0],
            0,
            TxKind::Call(poor.address()),
            1_000_000_000_000_000,
            &[],
        ),
        tx(&poor, 0, TxKind::Call(rich[1].address()), POOR_FUNDS, &[]),
    ]);
    let seq = execute_block_sequential(&database, None, env_of(&database), &recs).unwrap();
    assert!(seq.0[1].status, "the funded spend executes: {:?}", seq.0[1]);
    for (workers, rep) in shard_worker_pairs([1, 2, 4, 8], [0, 1, 2, 3]) {
        let out = execute_block_stm(
            &database,
            None,
            env_of(&database),
            &recs,
            &Stats::default(),
            nz(workers),
        )
        .unwrap();
        assert_identical(
            &seq,
            &out.receipts,
            &out.delta,
            &format!("w={workers} rep={rep}"),
        );
    }
}
