use super::*;
use crate::env::{Durability, StateEnvBuilder};
use crate::snapshot::StateSnapshot;
use crate::writer::{StateWriter, WriteBatch};
use alloy_primitives::{Address, B256, U256};
use kardamom_types::{AccountChange, BPosition, BlockBoundary, BlockDelta, StateDatabase};

fn commit_blocks(env: &StateEnv, addr: Address, upto: u64) {
    let handle = StateWriter::spawn(env.clone()).unwrap();
    for b in 1..=upto {
        let boundary = BlockBoundary {
            block_number: b,
            end_tx_idx: BPosition::from_index(b),
            l2_timestamp: 1_700_000_000 + b,
            l1_origin: 0,
        };
        let delta = BlockDelta {
            block_number: b,
            accounts: vec![AccountChange {
                address: addr,
                nonce: b,
                balance: U256::from(b * 100),
                code_hash: B256::ZERO,
            }],
            storage: Vec::new(),
            code: Vec::new(),
            receipts: Vec::new(),
        };
        handle
            .delta_tx
            .send(WriteBatch::new(boundary, delta))
            .unwrap();
    }
    // Wait for the last block to commit, then shut down.
    while let Some(s) = handle.snapshot_rx.recv() {
        if s.block_number() >= upto {
            break;
        }
    }
    handle.shutdown().unwrap();
}

/// Every real env is genesis-seeded at startup; checkpoints inherit that
/// digest as their chain identity, so test envs must be seeded too.
fn seed_test_genesis(env: &StateEnv) {
    let accounts = [kardamom_types::AccountChange {
        address: Address::from([0x01; 20]),
        nonce: 0,
        balance: alloy_primitives::U256::from(1u64),
        code_hash: B256::ZERO,
    }];
    crate::genesis::seed_genesis(env, &accounts, &[]).unwrap();
}

#[test]
fn checkpoint_restore_roundtrips_state() {
    let src_dir = tempfile::tempdir().unwrap();
    let ckpt_dir = tempfile::tempdir().unwrap();
    let restore_dir = tempfile::tempdir().unwrap();
    let addr = Address::from([0x42; 20]);

    // Build a DB with 5 committed blocks.
    {
        let env = StateEnvBuilder::new(src_dir.path())
            .durability(Durability::SafeNoSync)
            .open()
            .unwrap();
        seed_test_genesis(&env);
        commit_blocks(&env, addr, 5);

        let info = create_checkpoint(&env, ckpt_dir.path()).unwrap();
        assert_eq!(info.block, 5);
        assert!(info.path.exists());
    }

    // The latest checkpoint is block 5.
    let latest = latest_checkpoint(ckpt_dir.path()).unwrap().unwrap();
    assert_eq!(latest.block, 5);

    // Restore into a fresh dir and confirm the state matches exactly.
    let restored_block = restore_checkpoint(&latest.path, restore_dir.path(), None).unwrap();
    assert_eq!(restored_block, 5);

    let snap =
        StateSnapshot::open(&StateEnvBuilder::new(restore_dir.path()).open().unwrap()).unwrap();
    assert_eq!(snap.block_number(), 5);
    let (nonce, balance, _) = snap.basic(addr).unwrap().unwrap();
    assert_eq!(nonce, 5);
    assert_eq!(balance, U256::from(500u64));
}

/// A checkpoint whose bytes changed under its manifest must be refused,
/// not adopted. This is the at-rest half of the DA-blob lesson: an image
/// is only as trustworthy as something that pins its content.
#[test]
fn restore_refuses_a_tampered_image() {
    let src = tempfile::tempdir().unwrap();
    let ckpt = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();
    let addr = Address::from([0x21; 20]);
    let env = StateEnvBuilder::new(src.path())
        .durability(Durability::SafeNoSync)
        .open()
        .unwrap();
    seed_test_genesis(&env);
    commit_blocks(&env, addr, 3);
    let c = create_checkpoint(&env, ckpt.path()).unwrap();
    drop(env);

    // Verifies while honest.
    verify_checkpoint(&c.path, None).expect("untouched checkpoint must verify");

    // Flip one byte deep inside the image, preserving length.
    let data = checkpoint_data_file(&c.path).unwrap();
    let mut bytes = std::fs::read(&data).unwrap();
    let mid = bytes.len() / 2;
    bytes[mid] ^= 0x01;
    std::fs::write(&data, &bytes).unwrap();

    let err = restore_checkpoint(&c.path, dst.path(), None).unwrap_err();
    assert!(
        format!("{err}").contains("CORRUPT"),
        "expected a corruption refusal, got: {err}"
    );
    // And nothing was staged into the destination.
    assert!(!has_state_db(dst.path()).unwrap());
}

/// A checkpoint from ANOTHER CHAIN must be refused. This is the failure
/// that actually happened: a stale checkpoint leaked across a chaos reset,
/// a fresh node adopted it, and it then asked the cluster for a canonical
/// index that chain had never produced — looping forever.
#[test]
fn restore_refuses_a_foreign_chain() {
    let src = tempfile::tempdir().unwrap();
    let ckpt = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();
    let addr = Address::from([0x22; 20]);
    let env = StateEnvBuilder::new(src.path())
        .durability(Durability::SafeNoSync)
        .open()
        .unwrap();
    seed_test_genesis(&env);
    commit_blocks(&env, addr, 2);
    let c = create_checkpoint(&env, ckpt.path()).unwrap();
    let ours = stored_genesis_digest(&env).unwrap();
    drop(env);

    // Same bytes, but this node belongs to a different chain.
    let theirs = B256::repeat_byte(0xAB);
    assert_ne!(ours, theirs);
    let err = restore_checkpoint(&c.path, dst.path(), Some(theirs)).unwrap_err();
    assert!(
        format!("{err}").contains("DIFFERENT CHAIN"),
        "expected a chain-identity refusal, got: {err}"
    );
    assert!(!has_state_db(dst.path()).unwrap());

    // Our own chain still restores.
    restore_checkpoint(&c.path, dst.path(), Some(ours)).expect("same chain must restore");
}

/// An image with no manifest at all is unverifiable and must be refused —
/// otherwise the check is trivially bypassed by deleting a file.
#[test]
fn restore_refuses_an_unmanifested_image() {
    let src = tempfile::tempdir().unwrap();
    let ckpt = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();
    let addr = Address::from([0x23; 20]);
    let env = StateEnvBuilder::new(src.path())
        .durability(Durability::SafeNoSync)
        .open()
        .unwrap();
    seed_test_genesis(&env);
    commit_blocks(&env, addr, 1);
    let c = create_checkpoint(&env, ckpt.path()).unwrap();
    drop(env);

    std::fs::remove_file(manifest_path(&c.path)).unwrap();
    let err = restore_checkpoint(&c.path, dst.path(), None).unwrap_err();
    assert!(
        format!("{err}").contains("manifest"),
        "expected an unverifiable-image refusal, got: {err}"
    );
}

/// A bad newest checkpoint must cost one rung of the ladder, not wedge
/// the node: restore_best quarantines it and restores the next-newest.
#[test]
fn restore_best_quarantines_bad_and_falls_back() {
    let src = tempfile::tempdir().unwrap();
    let ckpt = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();
    let addr = Address::from([0x24; 20]);
    let env = StateEnvBuilder::new(src.path())
        .durability(Durability::SafeNoSync)
        .open()
        .unwrap();
    seed_test_genesis(&env);
    commit_blocks(&env, addr, 2);
    let good = create_checkpoint(&env, ckpt.path()).unwrap();
    commit_blocks(&env, addr, 5);
    let torn = create_checkpoint(&env, ckpt.path()).unwrap();
    drop(env);

    // Make the newest look like the observed CI failure: an image whose
    // MANIFEST never arrived (tar raced the writer's prune).
    std::fs::remove_file(manifest_path(&torn.path)).unwrap();

    let (block, path) = restore_best_checkpoint(ckpt.path(), dst.path(), None)
        .expect("restore_best must not error on a quarantinable checkpoint")
        .expect("the older good checkpoint must restore");
    assert_eq!(block, good.block);
    assert_eq!(path, good.path);
    // The torn one is quarantined under a hidden name, not retried forever.
    assert!(!torn.path.exists());
    let rejected: Vec<_> = std::fs::read_dir(ckpt.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.starts_with(".rejected-"))
        .collect();
    assert_eq!(
        rejected.len(),
        1,
        "expected one quarantined checkpoint: {rejected:?}"
    );

    // Nothing restorable left -> Ok(None), never a crash loop.
    let dst2 = tempfile::tempdir().unwrap();
    std::fs::remove_file(manifest_path(&good.path)).unwrap();
    assert!(
        restore_best_checkpoint(ckpt.path(), dst2.path(), None)
            .unwrap()
            .is_none()
    );
}

#[test]
fn latest_picks_highest_block_and_prune_trims() {
    let src_dir = tempfile::tempdir().unwrap();
    let ckpt_dir = tempfile::tempdir().unwrap();
    let addr = Address::from([0x7; 20]);

    let env = StateEnvBuilder::new(src_dir.path())
        .durability(Durability::SafeNoSync)
        .open()
        .unwrap();
    seed_test_genesis(&env);
    commit_blocks(&env, addr, 3);
    let c3 = create_checkpoint(&env, ckpt_dir.path()).unwrap();
    assert_eq!(c3.block, 3);
    // Advance and checkpoint again.
    commit_blocks(&env, addr, 7);
    let c7 = create_checkpoint(&env, ckpt_dir.path()).unwrap();
    assert_eq!(c7.block, 7);

    assert_eq!(
        latest_checkpoint(ckpt_dir.path()).unwrap().unwrap().block,
        7
    );

    // Prune everything before block 7 → removes the block-3 checkpoint.
    assert_eq!(prune_checkpoints(ckpt_dir.path(), 7).unwrap(), 1);
    assert_eq!(
        latest_checkpoint(ckpt_dir.path()).unwrap().unwrap().block,
        7
    );
}

#[test]
fn create_sweeps_stale_tmp_and_leaves_no_residue() {
    let src_dir = tempfile::tempdir().unwrap();
    let ckpt_dir = tempfile::tempdir().unwrap();
    let addr = Address::from([0x5; 20]);
    let env = StateEnvBuilder::new(src_dir.path())
        .durability(Durability::SafeNoSync)
        .open()
        .unwrap();
    seed_test_genesis(&env);
    commit_blocks(&env, addr, 2);

    // Plant a stale tmp dir from a "crashed" earlier writer.
    let stale = ckpt_dir.path().join(".checkpoint-000000000000000001.tmp");
    std::fs::create_dir_all(&stale).unwrap();

    let c = create_checkpoint(&env, ckpt_dir.path()).unwrap();
    assert!(c.path.exists());
    assert!(!stale.exists());
    let tmp_residue = std::fs::read_dir(ckpt_dir.path())
        .unwrap()
        .filter(|e| {
            e.as_ref()
                .unwrap()
                .file_name()
                .to_string_lossy()
                .ends_with(".tmp")
        })
        .count();
    assert_eq!(tmp_residue, 0);
}

#[test]
fn restore_refuses_to_clobber_populated_dir() {
    let src_dir = tempfile::tempdir().unwrap();
    let ckpt_dir = tempfile::tempdir().unwrap();
    let addr = Address::from([0x9; 20]);
    let env = StateEnvBuilder::new(src_dir.path())
        .durability(Durability::SafeNoSync)
        .open()
        .unwrap();
    seed_test_genesis(&env);
    commit_blocks(&env, addr, 2);
    let c = create_checkpoint(&env, ckpt_dir.path()).unwrap();

    // Restoring over the live (populated) src dir must be refused.
    let err = restore_checkpoint(&c.path, src_dir.path(), None).unwrap_err();
    assert!(matches!(err, StateError::Recovery(_)));
}
