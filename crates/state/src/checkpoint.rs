//! State checkpoints: consistent point-in-time snapshots of the libMDBX state
//! DB for **fast cold-start recovery**.
//!
//! A cold-started executor/validator with an empty state DB otherwise re-syncs
//! from genesis — replaying the *entire* canonical stream — which grows without
//! bound as the chain ages. A checkpoint lets a wiped node restore a recent
//! consistent state and replay only the short tail from the checkpoint's block
//! to the head. Because the executor replicas are deterministic state machines
//! at the same block, one replica's checkpoint is a valid restore source for
//! another (peer-to-peer restore, mirroring the archive re-replication path).
//!
//! Creation reuses [`compact_to`] (`mdbx_env_copy` + `MDBX_CP_COMPACT`) against
//! an online RO snapshot, so the live env keeps serving reads/writes throughout.
//! The copy lands under a hidden tmp name and is renamed into place when
//! complete, so anything visible as `checkpoint-*` is a full, consistent image
//! that will never change again — copying one concurrently is always safe.
//! Restore is a plain file copy of the checkpoint env into a fresh state dir;
//! the normal startup path then sees a non-empty DB and resumes from its
//! `last_committed_block` cursor.

use std::path::{Path, PathBuf};

use tracing::info;

use crate::compaction::compact_to;
use crate::env::StateEnv;
use crate::error::StateError;
use crate::recovery::read_recovery_point;

/// A checkpoint on disk: the block it captures and its directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointInfo {
    /// `last_committed_block` captured by the checkpoint.
    pub block: u64,
    /// Directory holding the compacted mdbx env.
    pub path: PathBuf,
}

/// Checkpoint directory name for `block`, zero-padded so lexical order matches
/// numeric order (`latest_checkpoint` relies on it).
pub(crate) fn checkpoint_name(block: u64) -> String {
    format!("checkpoint-{block:018}")
}

pub(crate) fn parse_checkpoint_block(name: &str) -> Option<u64> {
    name.strip_prefix("checkpoint-")
        .and_then(|s| s.parse().ok())
}

/// Create a checkpoint of `env` under `checkpoints_dir`, named by its committed
/// block. Returns the checkpoint's block + path. If a checkpoint for the same
/// block already exists it is treated as a no-op success (idempotent). The live
/// env stays online throughout.
pub fn create_checkpoint(
    env: &StateEnv,
    checkpoints_dir: &Path,
) -> Result<CheckpointInfo, StateError> {
    let block = read_recovery_point(env)?.last_committed_block;
    std::fs::create_dir_all(checkpoints_dir)?;
    let dest = checkpoints_dir.join(checkpoint_name(block));
    if dest.exists() {
        info!(block, path = %dest.display(), "checkpoint for block already present; skipping");
        return Ok(CheckpointInfo { block, path: dest });
    }
    // Compact under a hidden tmp name and rename into place only when the copy
    // is complete: peers copy checkpoints while we write (chaos re-replication,
    // peer restore), and a half-written env under its final name would hand
    // them a torn mdbx image. Hidden names don't parse as `checkpoint-*`, so
    // `latest_checkpoint`/`prune_checkpoints` never see in-progress copies.
    sweep_stale_tmp(checkpoints_dir)?;
    let tmp = checkpoints_dir.join(format!(".{}.tmp", checkpoint_name(block)));
    compact_to(env, &tmp)?;
    std::fs::rename(&tmp, &dest)?;
    info!(block, path = %dest.display(), "created state checkpoint");
    Ok(CheckpointInfo { block, path: dest })
}

/// Remove leftover `.checkpoint-*.tmp` entries (a writer crashed mid-compact).
/// Safe to run before compacting because there is a single checkpoint writer
/// per directory.
fn sweep_stale_tmp(checkpoints_dir: &Path) -> Result<(), StateError> {
    for entry in std::fs::read_dir(checkpoints_dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let s = name.to_string_lossy();
        if s.starts_with(".checkpoint-") && s.ends_with(".tmp") {
            let path = entry.path();
            if path.is_dir() {
                std::fs::remove_dir_all(&path)?;
            } else {
                std::fs::remove_file(&path)?;
            }
        }
    }
    Ok(())
}

/// Return the highest-block checkpoint under `checkpoints_dir`, or `None` if
/// there are none (or the directory does not exist). A checkpoint is either a
/// directory (`mdbx.dat` inside) or a single file, depending on the mdbx build.
pub fn latest_checkpoint(checkpoints_dir: &Path) -> Result<Option<CheckpointInfo>, StateError> {
    let rd = match std::fs::read_dir(checkpoints_dir) {
        Ok(rd) => rd,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    let mut best: Option<CheckpointInfo> = None;
    for entry in rd {
        let entry = entry?;
        let name = entry.file_name();
        if let Some(block) = parse_checkpoint_block(&name.to_string_lossy())
            && best.as_ref().is_none_or(|b| block > b.block)
        {
            best = Some(CheckpointInfo {
                block,
                path: entry.path(),
            });
        }
    }
    Ok(best)
}

/// Delete every checkpoint older than `keep_from` block, keeping newer ones.
/// Returns how many were removed. Used to bound checkpoint disk use.
pub fn prune_checkpoints(checkpoints_dir: &Path, keep_from: u64) -> Result<usize, StateError> {
    let rd = match std::fs::read_dir(checkpoints_dir) {
        Ok(rd) => rd,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(e.into()),
    };
    let mut removed = 0;
    for entry in rd {
        let entry = entry?;
        let path = entry.path();
        if let Some(block) = parse_checkpoint_block(&entry.file_name().to_string_lossy())
            && block < keep_from
        {
            if path.is_dir() {
                std::fs::remove_dir_all(&path)?;
            } else {
                std::fs::remove_file(&path)?;
            }
            removed += 1;
        }
    }
    Ok(removed)
}

/// Restore `checkpoint` into `state_dir` (which must be empty or absent):
/// copies the compacted mdbx env files across. Returns the restored
/// `last_committed_block`. The caller then opens `state_dir` normally and the
/// standard resume path replays the tail.
pub fn restore_checkpoint(checkpoint: &Path, state_dir: &Path) -> Result<u64, StateError> {
    std::fs::create_dir_all(state_dir)?;
    // Refuse to clobber an existing populated state DB — restore is for a fresh
    // (wiped) node only.
    if has_state_db(state_dir)? {
        return Err(StateError::Recovery(format!(
            "state dir {} already holds a DB; restore is for a fresh dir only",
            state_dir.display()
        )));
    }

    let data_src = checkpoint_data_file(checkpoint)?;
    std::fs::copy(&data_src, state_dir.join("mdbx.dat"))?;
    let env = crate::env::StateEnvBuilder::new(state_dir).open()?;
    let block = read_recovery_point(&env)?.last_committed_block;
    info!(block, src = %data_src.display(), "restored state DB from checkpoint");
    Ok(block)
}

/// Resolve a checkpoint's mdbx data file: either `<checkpoint>/mdbx.dat` (the
/// env was copied in subdir mode) or the `<checkpoint>` file itself
/// (single-file mode). Either way it is a complete mdbx image; dir-mode open
/// reads it as `<state_dir>/mdbx.dat`.
pub(crate) fn checkpoint_data_file(checkpoint: &Path) -> Result<PathBuf, StateError> {
    if checkpoint.is_dir() {
        find_mdbx_data(checkpoint)?.ok_or_else(|| {
            StateError::Recovery(format!(
                "checkpoint dir {} holds no mdbx data file",
                checkpoint.display()
            ))
        })
    } else if checkpoint.is_file() {
        Ok(checkpoint.to_path_buf())
    } else {
        Err(StateError::Recovery(format!(
            "checkpoint {} not found",
            checkpoint.display()
        )))
    }
}

/// Find the mdbx data file inside a subdir-mode checkpoint directory.
fn find_mdbx_data(dir: &Path) -> Result<Option<PathBuf>, StateError> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let s = name.to_string_lossy();
        if entry.path().is_file() && (s.ends_with(".dat") || s == "mdbx.dat" || s == "data.mdbx") {
            return Ok(Some(entry.path()));
        }
    }
    Ok(None)
}

/// Move a stale state DB aside into `<state_dir>/stale/`, so the next startup
/// sees a fresh dir (and restores from a checkpoint) while an operator keeps
/// one generation of the old DB for inspection. A previous parked copy is
/// dropped. Returns the parked path, or `None` if there was nothing to park.
///
/// Used by the resync fallback: a cursor below the cluster's retention floor
/// can never catch up, so the DB it lives in is unusable as-is — but deleting
/// state outright on an automated path is not acceptable.
pub fn park_state_db(state_dir: &Path) -> Result<Option<PathBuf>, StateError> {
    if !has_state_db(state_dir)? {
        return Ok(None);
    }
    let parked = state_dir.join("stale");
    if parked.exists() {
        std::fs::remove_dir_all(&parked)?;
    }
    std::fs::create_dir_all(&parked)?;
    for entry in std::fs::read_dir(state_dir)? {
        let entry = entry?;
        if entry.path() == parked {
            continue;
        }
        std::fs::rename(entry.path(), parked.join(entry.file_name()))?;
    }
    info!(parked = %parked.display(), "parked stale state DB");
    Ok(Some(parked))
}

/// True if `dir` already contains an mdbx data file (a populated state DB).
/// A caller can use this to decide whether a cold-start should restore a
/// checkpoint (dir empty) or resume the existing DB — without opening the env
/// (which would itself create the data file).
pub fn has_state_db(dir: &Path) -> Result<bool, StateError> {
    let rd = match std::fs::read_dir(dir) {
        Ok(rd) => rd,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(e) => return Err(e.into()),
    };
    for entry in rd {
        let name = entry?.file_name();
        let s = name.to_string_lossy();
        if s.ends_with(".dat") || s == "mdbx.dat" || s == "data.mdbx" {
            return Ok(true);
        }
    }
    Ok(false)
}

#[cfg(test)]
mod tests {
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
            commit_blocks(&env, addr, 5);

            let info = create_checkpoint(&env, ckpt_dir.path()).unwrap();
            assert_eq!(info.block, 5);
            assert!(info.path.exists());
        }

        // The latest checkpoint is block 5.
        let latest = latest_checkpoint(ckpt_dir.path()).unwrap().unwrap();
        assert_eq!(latest.block, 5);

        // Restore into a fresh dir and confirm the state matches exactly.
        let restored_block = restore_checkpoint(&latest.path, restore_dir.path()).unwrap();
        assert_eq!(restored_block, 5);

        let snap =
            StateSnapshot::open(&StateEnvBuilder::new(restore_dir.path()).open().unwrap()).unwrap();
        assert_eq!(snap.block_number(), 5);
        let (nonce, balance, _) = snap.basic(addr).unwrap().unwrap();
        assert_eq!(nonce, 5);
        assert_eq!(balance, U256::from(500u64));
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
        commit_blocks(&env, addr, 2);
        let c = create_checkpoint(&env, ckpt_dir.path()).unwrap();

        // Restoring over the live (populated) src dir must be refused.
        let err = restore_checkpoint(&c.path, src_dir.path()).unwrap_err();
        assert!(matches!(err, StateError::Recovery(_)));
    }
}
