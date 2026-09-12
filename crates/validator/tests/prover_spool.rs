//! The prover-spool contract: a block
//! captured, anchored, and spooled against a real writer-committed
//! pre-state snapshot must produce a frame that re-verifies one-shot in
//! the guest shape, and that names the exact post root the live
//! trie-aware writer then commits for the same block.
//!
//! This drives `spool_block`, the per-block body of the async spool
//! task, against a production `StateWriter` (`TrieMode::Incremental`) and
//! the MVCC `StateSnapshot` pin: the live wiring, minus the tokio loop.
//!
use std::time::Duration;

use alloy_primitives::{Address, B256, U256, keccak256};
use alloy_signer_local::PrivateKeySigner;
use kardamom_engine::actor::BufferedRecord;
use kardamom_engine::exec_types::TxIndex;
use kardamom_engine::stateless::AnchoredBlockOutput;
use kardamom_engine::{ExecEnv, error::ExecutorError};
use kardamom_state::writer::{StateWriter, WriteBatch, WriterHandle};
use kardamom_state::{Durability, StateEnvBuilder, StateSnapshot, TrieMode};
use kardamom_types::{
    AccountChange, BPosition, BlockBoundary, BlockBoundaryStart, BlockDelta, CodeEntry,
    ProverInput, PublicOutputs, StorageChange,
};
use kardamom_validator::prover::{PinnedPreState, spool_block};

mod common;
use common::{CHAIN_ID, RECIPIENT, S0, S1, ZEROER, ZEROER_CODE, tx};

fn boundary(block_number: u64, ts: u64) -> BlockBoundary {
    BlockBoundary {
        block_number,
        end_tx_idx: BPosition::from_index(0),
        l2_timestamp: ts,
        l1_origin: 0,
    }
}

/// Wait (up to 10s) for the writer's committed snapshot to reach `block`,
/// and return it. Used for both the pre-state pin and the post-commit
/// root check, so one poll loop serves every wait in this test.
fn wait_for_snapshot(writer: &WriterHandle, block: u64) -> StateSnapshot {
    kardamom_obs::testkit::poll_sync(
        &format!("writer committed block {block}"),
        Duration::from_secs(10),
        Duration::from_millis(20),
        || Ok(snapshot_at(writer, block)),
    )
    .unwrap_or_else(|e| panic!("{e}"))
}

/// The writer's committed snapshot, if it has reached `block`.
fn snapshot_at(writer: &WriterHandle, block: u64) -> Option<StateSnapshot> {
    let s = writer.snapshot_rx.current()?;
    (s.block_number() == block).then_some(s)
}

/// Set up a production `StateWriter` and commit the seed block (block 1)
/// through it: accounts, code, storage, trie, and meta all land as live
/// commits do. Returns the writer, the spool dir, and the committed
/// snapshot at block 1 (the spool's pre-state pin for block 2).
fn setup_seeded_writer(sender: Address) -> (tempfile::TempDir, WriterHandle, StateSnapshot) {
    let zeroer_hash = keccak256(ZEROER_CODE);
    let dir = tempfile::tempdir().unwrap();
    let env = StateEnvBuilder::new(dir.path().join("state"))
        .durability(Durability::SafeNoSync)
        .open()
        .unwrap();
    let writer = StateWriter::spawn_with_trie(env, TrieMode::Incremental).unwrap();

    let seed = BlockDelta {
        block_number: 1,
        accounts: vec![
            AccountChange {
                address: sender,
                nonce: 0,
                balance: U256::from(10u128.pow(18)),
                code_hash: B256::ZERO,
            },
            AccountChange {
                address: ZEROER,
                nonce: 1,
                balance: U256::ZERO,
                code_hash: zeroer_hash,
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
        code: vec![CodeEntry {
            code_hash: zeroer_hash,
            code: ZEROER_CODE.as_slice().into(),
        }],
        receipts: Vec::new(),
    };
    writer
        .delta_tx
        .send(WriteBatch::new(boundary(1, 1_700_000_000), seed))
        .unwrap();

    let snap1 = wait_for_snapshot(&writer, 1);
    (dir, writer, snap1)
}

/// Spool block 2 (a transfer to a fresh account, and a storage-deletion
/// collapse — the anchoring shape that needs the fixed point) against
/// `snap1`, then re-verify the spooled frame one-shot in the guest
/// shape. Returns the spooled outputs and the guest's anchored
/// re-execution, whose delta the caller commits to the live writer next.
fn spool_and_guest_reverify(
    spool: &std::path::Path,
    signer: &PrivateKeySigner,
    snap1: &StateSnapshot,
) -> (PublicOutputs, AnchoredBlockOutput) {
    let records = vec![
        tx(signer, RECIPIENT, 0, 250_000, 0),
        tx(signer, ZEROER, 1, 0, 1),
    ];
    let env2 = ExecEnv::new(
        CHAIN_ID,
        &BlockBoundaryStart {
            block_number: 2,
            end_tx_idx: BPosition::from_index(0),
            l2_timestamp: 1_700_000_002,
            l1_origin: 0,
        },
    );
    let pinned2 = PinnedPreState::new(snap1.clone(), 2).expect("snap1 pinned at block 1");
    let outputs = spool_block(spool, CHAIN_ID, &pinned2, env2, &records).expect("spool block 2");

    // The spooled frame re-verifies one-shot in the guest shape.
    let bytes = std::fs::read(spool.join("block-2/prover-input.rkyv")).unwrap();
    let expected = std::fs::read(spool.join("block-2/expected-outputs.bin")).unwrap();
    assert_eq!(expected, outputs.encode());
    let input: ProverInput =
        rkyv::from_bytes::<ProverInput, rkyv::rancor::Error>(&bytes).expect("frame decodes");
    let guest_records: Vec<BufferedRecord> = input
        .records
        .iter()
        .map(|r| match r {
            kardamom_types::ProverRecord::Tx {
                tx_idx,
                envelope,
                position,
            } => BufferedRecord::Tx {
                tx_idx: TxIndex(*tx_idx),
                envelope: envelope.clone(),
                position: *position,
            },
            kardamom_types::ProverRecord::Deposit {
                tx_idx,
                deposit,
                position,
            } => BufferedRecord::Deposit {
                tx_idx: TxIndex(*tx_idx),
                deposit: deposit.clone(),
                position: *position,
            },
        })
        .collect();
    let genv = ExecEnv::new(input.chain_id, &input.boundary);
    let mut bal_slice: &[u8] = &input.bal_rlp;
    let expected_bal =
        <alloy_eip7928::BlockAccessList as alloy_rlp::Decodable>::decode(&mut bal_slice).unwrap();
    let anchored = kardamom_engine::stateless::execute_block_anchored(
        &input.witness,
        &input.proofs,
        None,
        &guest_records,
        genv,
        &expected_bal,
        input.granularity,
    )
    .expect("guest-shape re-verification");
    assert_eq!(anchored.pre_state_root, outputs.pre_state_root);
    assert_eq!(anchored.post_state_root, outputs.post_state_root);
    assert_eq!(anchored.bal_commitment, outputs.bal_commitment);

    (outputs, anchored)
}

/// Commit `anchored`'s delta to the live writer as block 2, wait for it
/// to land, and return the committed trie root: the proof queue and the
/// chain must agree before any proving happens.
fn commit_block_2_and_wait_root(writer: &mut WriterHandle, anchored: &AnchoredBlockOutput) -> B256 {
    // `finalize` sorts accounts and storage (and would emit `code`, but
    // this block's records are all `Call`s to an already-deployed
    // contract, so `anchored.out.delta.code` is empty here — not a
    // deliberate omission, just this test's actual data). Receipts are
    // not needed for the trie-commit step, so `Vec::new()`.
    let delta2 = anchored.out.delta.clone().finalize(2, Vec::new());
    writer
        .delta_tx
        .send(WriteBatch::new(boundary(2, 1_700_000_002), delta2))
        .unwrap();
    wait_for_snapshot(writer, 2)
        .state_root()
        .unwrap()
        .expect("trie root at block 2")
}

/// Spool block 3 (one more transfer) against the pinned snapshot at
/// block 2, the second half of a real contiguous batch. The batch guest
/// requires the root chain to link 2 to 3.
fn spool_block_3(
    spool: &std::path::Path,
    signer: &PrivateKeySigner,
    snap2: StateSnapshot,
) -> PublicOutputs {
    let records3 = vec![tx(signer, RECIPIENT, 2, 111, 0)];
    let env3 = ExecEnv::new(
        CHAIN_ID,
        &BlockBoundaryStart {
            block_number: 3,
            end_tx_idx: BPosition::from_index(0),
            l2_timestamp: 1_700_000_003,
            l1_origin: 0,
        },
    );
    let pinned3 = PinnedPreState::new(snap2, 3).expect("snap2 pinned at block 2");
    spool_block(spool, CHAIN_ID, &pinned3, env3, &records3).expect("spool block 3")
}

#[test]
fn spooled_frame_reverifies_and_matches_the_live_writer_root() {
    let signer = PrivateKeySigner::from_bytes(&B256::repeat_byte(0x77)).unwrap();

    // When KARDAMOM_EMIT_BATCH_SPOOL=dir is set, the spool lands there
    // (blocks 2 and 3, a real contiguous batch) for the zk-host batch
    // round trip.
    let export = std::env::var("KARDAMOM_EMIT_BATCH_SPOOL").ok();
    let (dir, mut writer, snap1) = setup_seeded_writer(signer.address());
    let spool = export.map_or_else(|| dir.path().join("spool"), std::path::PathBuf::from);

    let (outputs, anchored) = spool_and_guest_reverify(&spool, &signer, &snap1);

    let live_root = commit_block_2_and_wait_root(&mut writer, &anchored);
    assert_eq!(
        live_root, outputs.post_state_root,
        "spooled post root must equal the live writer's root"
    );

    let snap2 = wait_for_snapshot(&writer, 2);
    let outputs3 = spool_block_3(&spool, &signer, snap2);
    assert_eq!(
        outputs3.pre_state_root, outputs.post_state_root,
        "the spooled chain must link 2 -> 3"
    );

    // A pin against the wrong pre-state snapshot must be rejected:
    // `snap1` is still pinned at block 1, but this claims block 3
    // (pre-state 2).
    let stale = PinnedPreState::new(snap1, 3);
    assert!(
        matches!(stale, Err(ExecutorError::WitnessUnanchored(_))),
        "wrong-window pin must fail closed: {stale:?}"
    );

    writer.shutdown().unwrap();
}
