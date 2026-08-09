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
//!
//! The manifest type and the shared verify/publish helpers (also consumed by
//! the peer-fetch path in [`crate::checkpoint_transfer`]) live in [`manifest`].

mod manifest;

#[cfg(test)]
mod tests;

pub use manifest::{CheckpointManifest, manifest_path, read_manifest, verify_checkpoint};

use manifest::stored_genesis_digest;
pub(crate) use manifest::{check_image_identity, file_keccak, publish_checkpoint};

use std::path::{Path, PathBuf};

use alloy_primitives::B256;

use tracing::{info, warn};

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

/// True if `name` names an mdbx data file, in either convention the mdbx
/// build may use (subdir-mode `mdbx.dat` / `data.mdbx`, or any `.dat`).
fn is_mdbx_data_name(name: &str) -> bool {
    name.ends_with(".dat") || name == "mdbx.dat" || name == "data.mdbx"
}

/// Delete `path` whether it is a directory or a single file (checkpoints are
/// either, depending on the mdbx build).
fn remove_dir_or_file(path: &Path) -> Result<(), StateError> {
    if path.is_dir() {
        std::fs::remove_dir_all(path)?;
    } else {
        std::fs::remove_file(path)?;
    }
    Ok(())
}

/// `read_dir` that treats a missing directory as empty (`Ok(None)`) — the
/// scanners must not fail before the first checkpoint is ever created.
fn read_dir_or_absent(dir: &Path) -> Result<Option<std::fs::ReadDir>, StateError> {
    match std::fs::read_dir(dir) {
        Ok(rd) => Ok(Some(rd)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
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
    let tmp_data = tmp.join("mdbx.dat");
    compact_to(env, &tmp_data)?;
    let manifest = CheckpointManifest {
        block,
        image_keccak: file_keccak(&tmp_data)?,
        genesis_digest: stored_genesis_digest(env)?,
    };
    publish_checkpoint(&tmp, &dest, &manifest)?;
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
            remove_dir_or_file(&entry.path())?;
        }
    }
    Ok(())
}

/// Return the highest-block checkpoint under `checkpoints_dir`, or `None` if
/// there are none (or the directory does not exist). A checkpoint is either a
/// directory (`mdbx.dat` inside) or a single file, depending on the mdbx build.
pub fn latest_checkpoint(checkpoints_dir: &Path) -> Result<Option<CheckpointInfo>, StateError> {
    let Some(rd) = read_dir_or_absent(checkpoints_dir)? else {
        return Ok(None);
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
    let Some(rd) = read_dir_or_absent(checkpoints_dir)? else {
        return Ok(0);
    };
    let mut removed = 0;
    for entry in rd {
        let entry = entry?;
        if let Some(block) = parse_checkpoint_block(&entry.file_name().to_string_lossy())
            && block < keep_from
        {
            remove_dir_or_file(&entry.path())?;
            removed += 1;
        }
    }
    Ok(removed)
}

/// Restore `checkpoint` into `state_dir` (which must be empty or absent):
/// copies the compacted mdbx env files across. Returns the restored
/// `last_committed_block`. The caller then opens `state_dir` normally and the
/// standard resume path replays the tail.
pub fn restore_checkpoint(
    checkpoint: &Path,
    state_dir: &Path,
    expected_genesis: Option<B256>,
) -> Result<u64, StateError> {
    // Verify BEFORE copying: an unverifiable or foreign image must never
    // become this node's state.
    let manifest = verify_checkpoint(checkpoint, expected_genesis)?;
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
    info!(
        block,
        manifest_block = manifest.block,
        genesis_digest = %manifest.genesis_digest,
        src = %data_src.display(),
        "restored state DB from checkpoint (image + chain identity verified)"
    );
    Ok(block)
}

/// Restore the newest VERIFIABLE checkpoint under `checkpoints_dir` into
/// `state_dir`, returning `(restored_block, checkpoint_path)`. A checkpoint
/// that fails verification is QUARANTINED — renamed to a hidden
/// `.rejected-<name>` the scanners never pick up — and the next-newest is
/// tried. A bad image must cost the node one rung of its fallback ladder
/// (older checkpoint -> peer fetch -> genesis), never wedge it: observed in
/// CI, a copy that raced the source's prune delivered an image without its
/// MANIFEST, and the restart refused it, exited, restarted into the same
/// refusal — a crash loop that held the fleet at 2/3 until the case timed
/// out. `Ok(None)` when no restorable checkpoint remains.
pub fn restore_best_checkpoint(
    checkpoints_dir: &Path,
    state_dir: &Path,
    expected_genesis: Option<B256>,
) -> Result<Option<(u64, PathBuf)>, StateError> {
    loop {
        let Some(ckpt) = latest_checkpoint(checkpoints_dir)? else {
            return Ok(None);
        };
        match restore_checkpoint(&ckpt.path, state_dir, expected_genesis) {
            Ok(block) => return Ok(Some((block, ckpt.path))),
            Err(e) => {
                // A failure after verification (I/O mid-copy) may have staged
                // a partial data file; a leftover would make the next attempt
                // refuse "state dir already holds a DB".
                let _ = std::fs::remove_file(state_dir.join("mdbx.dat"));
                let name = ckpt
                    .path
                    .file_name()
                    .map(|n| n.to_string_lossy().into_owned())
                    .unwrap_or_else(|| "checkpoint".into());
                let rejected = ckpt.path.with_file_name(format!(".rejected-{name}"));
                warn!(
                    checkpoint = %ckpt.path.display(),
                    error = %e,
                    quarantined_as = %rejected.display(),
                    "checkpoint failed verification; quarantining and trying the next-newest"
                );
                // If even the rename fails the loop cannot make progress —
                // surface the original refusal rather than spinning.
                std::fs::rename(&ckpt.path, &rejected).map_err(|re| {
                    StateError::Recovery(format!(
                        "checkpoint {} failed verification ({e}) and could not be                          quarantined: {re}",
                        ckpt.path.display()
                    ))
                })?;
            }
        }
    }
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
        if entry.path().is_file() && is_mdbx_data_name(&s) {
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
    let Some(rd) = read_dir_or_absent(dir)? else {
        return Ok(false);
    };
    for entry in rd {
        let name = entry?.file_name();
        if is_mdbx_data_name(&name.to_string_lossy()) {
            return Ok(true);
        }
    }
    Ok(false)
}
