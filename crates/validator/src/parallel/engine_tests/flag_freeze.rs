//! The validator's drivers switch a feature flag at the same block as the
//! sequential driver: the seeded parallel batches, and the stateless
//! re-execution over a captured witness (the zk guest shape). The chain
//! turns `FEATURE_SEALER_V3` on, off and on again with `setFeature`
//! deposits in the middle of blocks.

use alloy_primitives::{B256, U256};
use alloy_signer_local::PrivateKeySigner;
use kardamom_engine::actor::fixtures::LegacyTx;
use kardamom_engine::actor::flag_switches::FlagSwitches;
use kardamom_engine::actor::{BlockExecOutput, BufferedRecord};
use kardamom_engine::bal_ladder::quantize;
use kardamom_engine::block_env::ExecEnv;
use kardamom_engine::delta::PendingDelta;
use kardamom_engine::exec_types::TxIndex;
use kardamom_engine::stateless::execute_block_with_bal;
use kardamom_exec_core::features::activation_slot;
use kardamom_types::upgrades::{CHAIN_STATE, FEATURE_SEALER_V3};
use kardamom_types::{BPosition, Receipt, StateDatabase};

use crate::parallel::{BlockInputs, ClaimIndex, execute_block_parallel};
use crate::witness::{capture_block_witness, reexecute_stateless};

use super::fixtures::{env_of, fund, nz, nz16, test_pool};

/// One block through the seeded parallel batches at `batch_size`, with
/// the claims of the sequential capture.
fn parallel<S: StateDatabase + Sync>(
    snapshot: &S,
    parent: Option<&PendingDelta>,
    records: &[BufferedRecord],
    env: ExecEnv,
    batch_size: usize,
) -> BlockExecOutput {
    let (_, bal) = execute_block_with_bal(snapshot, parent, records, env).expect("capture");
    let claims = ClaimIndex::from_alloy(&bal);
    let out = execute_block_parallel(
        &test_pool(),
        &BlockInputs {
            snapshot,
            parent,
            txs: records,
            claims: &claims,
            env,
            granularity: nz16(1),
        },
        nz(batch_size),
    )
    .expect("parallel");
    BlockExecOutput {
        receipts: out.receipts,
        delta: out.delta,
        bal: None,
    }
}

/// One block through the witness capture, then through the stateless
/// re-execution over that witness alone.
fn stateless<S: StateDatabase>(
    snapshot: &S,
    parent: Option<&PendingDelta>,
    records: &[BufferedRecord],
    env: ExecEnv,
) -> BlockExecOutput {
    let (captured, witness, bal) =
        capture_block_witness(snapshot, parent, records, env).expect("capture");
    let out = reexecute_stateless(&witness, parent, records, env, &quantize(bal, 1), nz16(1))
        .expect("stateless");
    assert_eq!(out.receipts, captured.receipts);
    out
}

/// The chain through the seeded parallel batches at `batch_size`.
fn run_parallel(chain: &FlagSwitches, batch_size: usize) -> Vec<Vec<Receipt>> {
    chain.run(|parent, records, env| {
        parallel(chain.snapshot(), Some(parent), records, env, batch_size)
    })
}

#[test]
fn the_parallel_batches_switch_flags_at_the_same_block() {
    let chain = FlagSwitches::new();
    for batch_size in [1, 2, 4] {
        FlagSwitches::assert_burns(&run_parallel(&chain, batch_size));
    }
}

#[test]
fn the_stateless_run_switches_flags_at_the_same_block() {
    let chain = FlagSwitches::new();
    FlagSwitches::assert_burns(
        &chain.run(|parent, records, env| stateless(chain.snapshot(), Some(parent), records, env)),
    );
}

/// The guest shape: no parent layer, and the flag word in the snapshot.
/// The witness holds the flag word and its account, and the stateless
/// run reads the flag from the witness alone, whatever `env` says.
#[test]
fn the_witness_carries_the_flags_of_the_block() {
    let signer = PrivateKeySigner::random();
    let snap = fund(
        kardamom_exec_core::state::MockStateDatabase::builder(),
        signer.address(),
    )
    .account(CHAIN_STATE, U256::ZERO, 1, B256::repeat_byte(0xC5))
    .storage(
        CHAIN_STATE,
        activation_slot(FEATURE_SEALER_V3),
        U256::from(1u64),
    )
    .build();
    // An honest envelope: the stateless run checks the hash and the
    // sender of every transaction.
    let records = vec![BufferedRecord::Tx {
        tx_idx: TxIndex(0),
        envelope: LegacyTx {
            chain_id: 1,
            to: alloy_primitives::Address::repeat_byte(0xBB),
            value: u64::MAX,
            gas_limit: 100_000,
            gas_price: 1_000_000_000,
            ..Default::default()
        }
        .sign(&signer),
        position: BPosition::from_index(0),
    }];
    let env = env_of(&snap, None);
    let (captured, witness, bal) =
        capture_block_witness(&snap, None, &records, env).expect("capture");
    let slot = activation_slot(FEATURE_SEALER_V3);
    assert!(
        witness
            .storage
            .iter()
            .any(|s| s.address == CHAIN_STATE && s.key == slot),
        "the witness holds the flag word"
    );
    assert!(
        witness.accounts.iter().any(|a| a.address == CHAIN_STATE),
        "the witness holds the account of the flag word"
    );
    let unflagged = ExecEnv {
        features: kardamom_exec_core::features::BlockFeatures::NONE,
        ..env
    };
    let out = reexecute_stateless(
        &witness,
        None,
        &records,
        unflagged,
        &quantize(bal, 1),
        nz16(1),
    )
    .expect("stateless");
    assert!(out.receipts[0].burns_nonce(), "{:?}", out.receipts[0]);
    assert_eq!(out.receipts, captured.receipts);
    assert_eq!(
        out.delta.accounts.get(&signer.address()).map(|a| a.nonce),
        Some(1)
    );
}
