//! Full pipeline contract: capture, anchor,
//! and guest, end to end, against the production stored trie.
//!
//! One block runs through real EVM execution with witness capture (phase
//! 2). The witness is anchored against the validator's committed libmdbx
//! trie through the recompute-guided fixed point
//! (`anchor_block_witness`), and the guest entry
//! (`execute_block_anchored`) re-executes from the witness and proofs
//! alone. The closing assertion is the point of the whole test: the
//! guest's recomputed post-state root equals the root the live
//! incremental writer (`update_for_block`) produces for the same block.
//! Proof and chain agree on state, byte for byte.
//!
//! The block mixes the shapes that stress the anchor: a plain transfer
//! (account updates and fresh-account creation), a contract call that
//! zeroes a storage slot (a storage-trie deletion collapse, where the
//! fixed point must pull the off-path sibling), and another that writes
//! a fresh slot.
//!
use alloy_primitives::{Address, B256, U256, address, keccak256};
use alloy_signer_local::PrivateKeySigner;
use kardamom_engine::actor::{BlockExecOutput, BufferedRecord};
use kardamom_engine::stateless::AnchoredBlockOutput;
use kardamom_engine::{ExecEnv, MockStateDatabase};
use kardamom_state::trie::{TrieTables, update_for_block};
use kardamom_state::{Durability, StateEnv, StateEnvBuilder};
use kardamom_types::{
    AccountChange, BPosition, BlockBoundaryStart, BlockDelta, BlockRecordsDigest, ExecutionWitness,
    ProverInput, ProverRecord, PublicOutputs, StorageChange, WitnessProofs,
};
use kardamom_validator::witness::{anchor_block_witness, capture_block_witness};

mod common;
use common::{CHAIN_ID, RECIPIENT, S0, S1, ZEROER, ZEROER_CODE, tx};

/// SSTORE(3, 0x2a); STOP. Writes a fresh slot: the storage-insert shape.
const WRITER: Address = address!("00000000000000000000000000000000000000Bb");
const WRITER_CODE: [u8; 6] = [0x60, 0x2a, 0x60, 0x03, 0x55, 0x00];
/// `execute_block_anchored`'s BAL granularity for an unquantized (K=1)
/// frame.
const GRANULARITY_1: std::num::NonZeroU16 = std::num::NonZeroU16::new(1).unwrap();

fn exec_env() -> ExecEnv {
    ExecEnv::new(
        CHAIN_ID,
        &BlockBoundaryStart {
            block_number: 1,
            end_tx_idx: BPosition::from_index(0),
            l2_timestamp: 1_700_000_000,
            l1_origin: 0,
        },
    )
}

/// Seed identically into the mock (execution) and the libmdbx trie
/// (anchoring): the sender, both contracts (`ZEROER` holds two live
/// slots, so zeroing one collapses a branch), and background noise.
fn seed_delta(sender: Address, zeroer_hash: B256, writer_hash: B256) -> BlockDelta {
    let mut seed = BlockDelta {
        block_number: 0,
        accounts: vec![
            AccountChange {
                address: sender,
                nonce: 0,
                balance: U256::from(10u128.pow(18)),
                code_hash: B256::ZERO, // The live writer maps ZERO to KECCAK_EMPTY.
            },
            AccountChange {
                address: ZEROER,
                nonce: 1,
                balance: U256::ZERO,
                code_hash: zeroer_hash,
            },
            AccountChange {
                address: WRITER,
                nonce: 1,
                balance: U256::ZERO,
                code_hash: writer_hash,
            },
        ],
        storage: vec![
            StorageChange {
                address: ZEROER,
                key: S0,
                value: U256::from(5),
            },
            StorageChange {
                address: ZEROER,
                key: S1,
                value: U256::from(7),
            },
        ],
        code: Vec::new(),
        receipts: Vec::new(),
    };
    for i in 0u8..16 {
        seed.accounts.push(AccountChange {
            address: Address::repeat_byte(0xC0u8.wrapping_add(i)),
            nonce: 1,
            balance: U256::from(u64::from(i) + 1),
            code_hash: B256::ZERO,
        });
    }
    seed
}

/// The same pre-state as [`seed_delta`], as the mock execution database.
fn seed_mock(sender: Address, zeroer_hash: B256, writer_hash: B256) -> MockStateDatabase {
    let mut mock = MockStateDatabase::builder()
        .account(
            sender,
            U256::from(10u128.pow(18)),
            0,
            alloy_primitives::KECCAK256_EMPTY,
        )
        .account(ZEROER, U256::ZERO, 1, zeroer_hash)
        .account(WRITER, U256::ZERO, 1, writer_hash)
        .storage(ZEROER, S0, U256::from(5))
        .storage(ZEROER, S1, U256::from(7))
        .code(zeroer_hash, ZEROER_CODE.as_slice().into())
        .code(writer_hash, WRITER_CODE.as_slice().into());
    for i in 0u8..16 {
        mock = mock.account(
            Address::repeat_byte(0xC0u8.wrapping_add(i)),
            U256::from(u64::from(i) + 1),
            1,
            alloy_primitives::KECCAK256_EMPTY,
        );
    }
    mock.build()
}

/// Commit `seed` into the trie-backed `env`, and return the resulting root.
fn commit_seed(env: &StateEnv, seed: &BlockDelta) -> B256 {
    let txn = env.raw().begin_rw_sync().unwrap();
    let tables = TrieTables::open(&txn).unwrap();
    let root = update_for_block(&txn, &tables, seed).unwrap();
    txn.commit().unwrap();
    root
}

/// Capture the block's witness against `snap`, then anchor it against the
/// committed trie in `env`: the capture fixed point. Asserts the zeroing
/// write landed in the delta, and that a real trie yields real proofs.
fn capture_and_anchor(
    env: &StateEnv,
    snap: &MockStateDatabase,
    pre_root: B256,
    records: &[BufferedRecord],
) -> (
    BlockExecOutput,
    ExecutionWitness,
    alloy_eip7928::BlockAccessList,
    WitnessProofs,
    B256,
) {
    let (out, mut witness, bal) =
        capture_block_witness(snap, None, records, exec_env()).expect("capture");
    assert!(
        out.delta
            .storage
            .get(&(ZEROER, S0))
            .is_some_and(U256::is_zero),
        "the zeroing write must be in the delta"
    );

    let ro = env.raw().begin_ro_sync().unwrap();
    let tables = TrieTables::open(&ro).unwrap();
    let (proofs, post_root) =
        anchor_block_witness(&ro, &tables, pre_root, &mut witness, &out.delta).expect("anchor");
    assert!(!proofs.nodes.is_empty(), "a real trie yields real proofs");
    (out, witness, bal, proofs, post_root)
}

/// Re-execute in the guest shape, one shot over the witness and proofs
/// alone, and check it reproduces the capture side's roots and receipts.
fn guest_reverify(
    witness: &ExecutionWitness,
    proofs: &WitnessProofs,
    records: &[BufferedRecord],
    bal: &alloy_eip7928::BlockAccessList,
    pre_root: B256,
    post_root: B256,
    out: &BlockExecOutput,
) -> AnchoredBlockOutput {
    let anchored = kardamom_engine::stateless::execute_block_anchored(
        witness,
        proofs,
        None,
        records,
        exec_env(),
        bal,
        GRANULARITY_1,
    )
    .expect("guest execution");
    assert_eq!(anchored.pre_state_root, pre_root);
    assert_eq!(anchored.post_state_root, post_root);
    assert_eq!(anchored.block_number, 1);
    assert_eq!(anchored.out.receipts, out.receipts, "receipts identical");
    anchored
}

/// When `KARDAMOM_EMIT_PROVER_FIXTURE=dir` is set, serialize the exact
/// `ProverInput` this test just validated, plus the expected 104-byte
/// `PublicOutputs`. The SP1 host runner (`guest/kardamom-zk-host`) runs
/// the real guest ELF against these and checks byte equality: the
/// guest/host round-trip contract.
fn export_prover_fixture_if_requested(
    witness: &ExecutionWitness,
    proofs: &WitnessProofs,
    records: &[BufferedRecord],
    bal: &alloy_eip7928::BlockAccessList,
    anchored: &AnchoredBlockOutput,
) {
    let Ok(dir) = std::env::var("KARDAMOM_EMIT_PROVER_FIXTURE") else {
        return;
    };
    let mut bal_rlp = Vec::new();
    alloy_rlp::Encodable::encode(bal, &mut bal_rlp);
    let input = ProverInput {
        chain_id: CHAIN_ID,
        boundary: BlockBoundaryStart {
            block_number: 1,
            end_tx_idx: BPosition::from_index(0),
            l2_timestamp: 1_700_000_000,
            l1_origin: 0,
        },
        witness: witness.clone(),
        proofs: proofs.clone(),
        records: records
            .iter()
            .map(|r| match r {
                BufferedRecord::Tx {
                    tx_idx,
                    envelope,
                    position,
                } => ProverRecord::Tx {
                    tx_idx: tx_idx.0,
                    envelope: envelope.clone(),
                    position: *position,
                },
                BufferedRecord::Deposit {
                    tx_idx,
                    deposit,
                    position,
                } => ProverRecord::Deposit {
                    tx_idx: tx_idx.0,
                    deposit: deposit.clone(),
                    position: *position,
                },
                // Cross-chain (0x7D) deliveries have no prover-wire
                // shape yet (see `wire_records` in `prover.rs`). This
                // fixture's block never carries one.
                BufferedRecord::XChain { .. } => {
                    panic!("prover fixture export does not support xchain records yet")
                }
            })
            .collect(),
        bal_rlp: bal_rlp.into(),
        granularity: std::num::NonZeroU16::MIN,
    };
    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&input).expect("serialize input");
    let mut digest = BlockRecordsDigest::new(1);
    records
        .iter()
        .filter_map(|r| match r {
            BufferedRecord::Tx { envelope, .. } => Some(envelope),
            _ => None,
        })
        .for_each(|envelope| digest.add_tx(&envelope.raw_tx));
    let expected = PublicOutputs {
        pre_state_root: anchored.pre_state_root,
        post_state_root: anchored.post_state_root,
        block_number: anchored.block_number,
        records_digest: digest.finish(),
        bal_commitment: anchored.bal_commitment,
    };
    std::fs::write(format!("{dir}/prover-input.rkyv"), &bytes).unwrap();
    std::fs::write(format!("{dir}/expected-outputs.bin"), expected.encode()).unwrap();
}

/// Commit `out`'s delta to the live incremental writer as block 1, and
/// return the resulting root: the closing assertion checks this equals
/// the guest's recomputed root for the same block.
fn commit_live_and_get_root(env: &StateEnv, out: &BlockExecOutput) -> B256 {
    let txn = env.raw().begin_rw_sync().unwrap();
    let tables = TrieTables::open(&txn).unwrap();
    // `finalize` sorts accounts and storage (and would emit `code`, but
    // this block's records are all `Call`s to already-deployed
    // contracts, so `out.delta.code` is empty here — not a deliberate
    // omission, just this test's actual data). Receipts are not needed
    // for the trie-commit step, so `Vec::new()`.
    let delta = out.delta.clone().finalize(1, Vec::new());
    let root = update_for_block(&txn, &tables, &delta).unwrap();
    txn.commit().unwrap();
    root
}

/// Corrupt one proof node byte, and check the guest rejects it: the
/// anchor's tamper-evidence contract.
fn assert_tamper_rejected(
    witness: &ExecutionWitness,
    proofs: &WitnessProofs,
    records: &[BufferedRecord],
    bal: &alloy_eip7928::BlockAccessList,
) {
    let mut bad = proofs.clone();
    let mut b0 = bad.nodes[0].to_vec();
    b0[0] ^= 0x01;
    bad.nodes[0] = b0.into();
    let err = kardamom_engine::stateless::execute_block_anchored(
        witness,
        &bad,
        None,
        records,
        exec_env(),
        bal,
        GRANULARITY_1,
    )
    .expect_err("tampered proofs must fail");
    assert!(
        matches!(
            err,
            kardamom_engine::error::ExecutorError::WitnessUnanchored(_)
        ),
        "got: {err:?}"
    );
}

#[test]
// This test is deterministic, so it also doubles as the prover-fixture
// generator (see `export_prover_fixture_if_requested`).
fn capture_anchor_guest_and_live_trie_agree() {
    let signer = PrivateKeySigner::from_bytes(&alloy_primitives::B256::repeat_byte(0x5A)).unwrap();
    let sender = signer.address();
    let zeroer_hash = keccak256(ZEROER_CODE);
    let writer_hash = keccak256(WRITER_CODE);

    let seed = seed_delta(sender, zeroer_hash, writer_hash);
    let snap = seed_mock(sender, zeroer_hash, writer_hash);

    let dir = tempfile::tempdir().unwrap();
    let env = StateEnvBuilder::new(dir.path())
        .durability(Durability::SafeNoSync)
        .open()
        .unwrap();
    let pre_root = commit_seed(&env, &seed);

    // --- The block: transfer to a fresh account, zero a slot, and write a slot.
    let records = vec![
        tx(&signer, RECIPIENT, 0, 250_000, 0),
        tx(&signer, ZEROER, 1, 0, 1),
        tx(&signer, WRITER, 2, 0, 2),
    ];
    let (out, witness, bal, proofs, post_root) =
        capture_and_anchor(&env, &snap, pre_root, &records);

    let anchored = guest_reverify(&witness, &proofs, &records, &bal, pre_root, post_root, &out);
    export_prover_fixture_if_requested(&witness, &proofs, &records, &bal, &anchored);

    let live_root = commit_live_and_get_root(&env, &out);
    assert_eq!(
        live_root, post_root,
        "guest recompute and live incremental trie must agree"
    );

    assert_tamper_rejected(&witness, &proofs, &records, &bal);
}
