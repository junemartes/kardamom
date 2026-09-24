//! Fail-stop tests: a forged BAL claim (transaction, cross-chain
//! delivery, or deposit) must stop the process at the batch that
//! produces it. This is what makes seeding from unverified claims sound.

use alloy_primitives::{U256, address};
use alloy_signer_local::PrivateKeySigner;
use kardamom_engine::actor::BufferedRecord;
use kardamom_engine::error::ExecutorError;
use kardamom_engine::state::MockStateDatabase;

use crate::parallel::{BlockInputs, execute_block_parallel};

use super::fixtures::{dep, env, funded, honest_claims, nz, nz16, test_pool, tx, xchain};

#[test]
fn a_forged_claim_fails_stop_at_its_producing_batch() {
    let signer = PrivateKeySigner::random();
    let to = address!("00000000000000000000000000000000000000BB");
    let snap = funded(&signer);
    let txs: Vec<BufferedRecord> = (0..8).map(|i| tx(&signer, to, i, 500, i)).collect();

    let mut claims = honest_claims(&snap, &txs);
    // Tamper with the claims: inflate the recipient's claimed balance at
    // tx 6 (batch 2 of 5-tx batches), so the executor claims a state it
    // did not compute.
    let bogus = claims.balance.get_mut(&to).expect("recipient claims");
    if let Some(entry) = bogus.iter_mut().find(|(i, _)| *i == 6) {
        entry.1 += U256::from(1_000_000u64);
    }

    let err = execute_block_parallel(
        &test_pool(),
        &BlockInputs {
            snapshot: &snap,
            parent: None,
            txs: &txs,
            claims: &claims,
            env: env(),
            granularity: nz16(1),
        },
        nz(5),
    )
    .expect_err("a forged claim must be caught");
    match err {
        ExecutorError::Divergence(msg) => {
            assert!(msg.contains("tx 6"), "must name the producing tx: {msg}");
            assert!(
                msg.contains("balance"),
                "must name the mismatching item: {msg}"
            );
        }
        other => panic!("expected Divergence, got {other:?}"),
    }
}

/// A forged claim about a cross-chain delivery fails closed like any
/// other — 0x7D records get no special trust in the induction.
#[test]
fn a_forged_xchain_claim_fails_stop() {
    let signer = PrivateKeySigner::random();
    let to = address!("00000000000000000000000000000000000000F5");
    let snap = funded(&signer);
    let origin = 424_242u64;
    let records: Vec<BufferedRecord> = vec![xchain(origin, 0, to, 0), tx(&signer, to, 0, 1_000, 1)];
    let mut claims = honest_claims(&snap, &records);
    // Tamper the delivery's claimed nonce write (the aliased sender's
    // nonce bump at bal index 1).
    let aliased = kardamom_types::xchain::xchain_tx_sender(origin);
    let w = claims
        .nonce
        .get_mut(&aliased)
        .expect("delivery nonce claim");
    let entry = w.iter_mut().find(|(i, _)| *i == 1).expect("index 1");
    entry.1 += 7;

    let err = execute_block_parallel(
        &test_pool(),
        &BlockInputs {
            snapshot: &snap,
            parent: None,
            txs: &records,
            claims: &claims,
            env: env(),
            granularity: nz16(1),
        },
        nz(1),
    )
    .expect_err("forged xchain claim must be caught");
    match err {
        ExecutorError::Divergence(msg) => {
            assert!(msg.contains("tx 1"), "must name the producing unit: {msg}");
        }
        other => panic!("expected Divergence, got {other:?}"),
    }
}

/// A forged claim about a deposit stops the process like any other:
/// deposits get no special trust.
#[test]
fn a_forged_deposit_claim_fails_stop() {
    let signer = PrivateKeySigner::random();
    let d = signer.address();
    let to = address!("00000000000000000000000000000000000000F3");
    let snap = MockStateDatabase::builder()
        .account(d, U256::ZERO, 0, alloy_primitives::KECCAK256_EMPTY)
        .build();
    let records: Vec<BufferedRecord> = vec![
        dep(d, Some(to), 10u128.pow(18), Vec::new(), 0),
        tx(&signer, to, 1, 1_000, 1),
    ];
    let mut claims = honest_claims(&snap, &records);
    // Tamper the deposit's claimed mint (bal index 1).
    let w = claims.balance.get_mut(&d).expect("mint claim");
    let entry = w.iter_mut().find(|(i, _)| *i == 1).expect("index 1");
    entry.1 += U256::from(7u64);

    let err = execute_block_parallel(
        &test_pool(),
        &BlockInputs {
            snapshot: &snap,
            parent: None,
            txs: &records,
            claims: &claims,
            env: env(),
            granularity: nz16(1),
        },
        nz(1),
    )
    .expect_err("forged deposit claim must be caught");
    match err {
        ExecutorError::Divergence(msg) => {
            assert!(msg.contains("tx 1"), "must name the producing unit: {msg}");
            assert!(msg.contains("balance"), "must name the field: {msg}");
        }
        other => panic!("expected Divergence, got {other:?}"),
    }
}
