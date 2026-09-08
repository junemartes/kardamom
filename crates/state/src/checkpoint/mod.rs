//! State checkpoints: consistent point-in-time snapshots of the libmdbx
//! state DB, for fast cold-start recovery.
//!
//! A cold-started executor or validator with an empty state DB must
//! otherwise re-sync from genesis, replaying the entire canonical
//! stream. This replay grows without bound as the chain ages. A
//! checkpoint lets a wiped node restore a recent consistent state and
//! replay only the short tail from the checkpoint's block to the head.
//!
//! Executor replicas are deterministic state machines at the same block,
//! so one replica's checkpoint is a valid restore source for another.
//! This supports peer-to-peer restore, which mirrors the archive
//! re-replication path.
//!
//! Creation reuses [`compact_to`] (`mdbx_env_copy` with
//! `MDBX_CP_COMPACT`) against an online read-only snapshot, so the live
//! env keeps serving reads and writes throughout. The copy lands under a
//! hidden temp name and is renamed into place when complete. So anything
//! visible as `checkpoint-*` is a full, consistent image that will never
//! change again, and copying one concurrently is always safe.
//!
//! Restore is a plain file copy of the checkpoint env into a fresh state
//! directory. The normal startup path then sees a non-empty DB and
//! resumes from its `last_committed_block` cursor.
//!
//! The manifest type and the shared verify and publish helpers live in
//! [`manifest`]. The peer-fetch path in [`crate::checkpoint_transfer`]
//! also uses these helpers.

mod manifest;

#[cfg(test)]
mod tests;

#[cfg(test)]
pub(crate) use manifest::manifest_path;
pub(crate) use manifest::{CheckpointManifest, read_manifest, verify_checkpoint};

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

/// The checkpoint directory name for `block`. It is zero-padded, so
/// lexical order matches numeric order. [`latest_checkpoint`] depends on
/// this.
pub(crate) fn checkpoint_name(block: u64) -> String {
    format!("checkpoint-{block:018}")
}

/// What a directory entry name means, in any of the places this module
/// scans a directory: a checkpoints directory (`Checkpoint`, `Tmp`), or a
/// single checkpoint's, or the state directory's, own listing
/// (`MdbxData`). Parsed once per entry name; each scanner matches on the
/// variant it cares about and ignores the rest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CheckpointEntry {
    /// A `checkpoint-<block>` entry. [`checkpoint_name`] is the encoder.
    Checkpoint(u64),
    /// A hidden `.checkpoint-*.tmp` entry, left by a writer that crashed
    /// mid-compact.
    Tmp,
    /// An mdbx data file, using either naming convention the mdbx build
    /// may use: subdir-mode `mdbx.dat` or `data.mdbx`, or any name
    /// ending in `.dat`.
    MdbxData,
    /// Anything else: a `.rejected-*` quarantined image, an operator
    /// file, or an unrelated entry.
    Other,
}

impl CheckpointEntry {
    fn parse(name: &str) -> Self {
        if let Some(block) = name
            .strip_prefix("checkpoint-")
            .and_then(|s| s.parse().ok())
        {
            return Self::Checkpoint(block);
        }
        #[allow(
            clippy::case_sensitive_file_extension_comparisons,
            reason = "this crate is the only writer of the .tmp suffix (see create_checkpoint), \
                      always lowercase; a case-insensitive match would accept names this crate \
                      itself never produces"
        )]
        let is_tmp = name.starts_with(".checkpoint-") && name.ends_with(".tmp");
        if is_tmp {
            return Self::Tmp;
        }
        #[allow(
            clippy::case_sensitive_file_extension_comparisons,
            reason = "mdbx always emits a lowercase extension; a case-insensitive match would \
                      accept names mdbx itself never produces"
        )]
        let is_mdbx_data = name.ends_with(".dat") || name == "mdbx.dat" || name == "data.mdbx";
        if is_mdbx_data {
            return Self::MdbxData;
        }
        Self::Other
    }
}

/// Delete `path`, whether it is a directory or a single file. A
/// checkpoint can be either, depending on the mdbx build.
fn remove_dir_or_file(path: &Path) -> Result<(), StateError> {
    if path.is_dir() {
        std::fs::remove_dir_all(path)?;
    } else {
        std::fs::remove_file(path)?;
    }
    Ok(())
}

/// Like `read_dir`, but treats a missing directory as empty (`Ok(None)`).
/// The scanners must not fail before the first checkpoint is ever created.
fn read_dir_or_absent(dir: &Path) -> Result<Option<std::fs::ReadDir>, StateError> {
    match std::fs::read_dir(dir) {
        Ok(rd) => Ok(Some(rd)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// List `dir`'s entries as `(name, path)` pairs, or an empty list if
/// `dir` does not exist. Every scanner in this module reads a directory
/// this way, then matches [`CheckpointEntry::parse`] on the name.
fn entry_names(dir: &Path) -> Result<Vec<(String, PathBuf)>, StateError> {
    let Some(rd) = read_dir_or_absent(dir)? else {
        return Ok(Vec::new());
    };
    rd.map(|e| {
        let e = e?;
        Ok((e.file_name().to_string_lossy().into_owned(), e.path()))
    })
    .collect()
}

/// Create a checkpoint of `env` under `checkpoints_dir`, named by its
/// committed block. Returns the checkpoint's block and path.
///
/// If a checkpoint for the same block already exists, this function
/// treats it as a no-op success. It is idempotent. The live env stays
/// online throughout.
///
/// # Errors
///
/// Returns [`StateError`] if the checkpoints directory, the compaction,
/// or the manifest write fails.
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
    // Compact under a hidden temp name, and rename into place only when
    // the copy is complete. Peers may copy checkpoints while we write, for
    // re-replication or peer restore. A half-written env under its final
    // name would hand them a torn mdbx image. Hidden names do not parse as
    // `checkpoint-*`, so `latest_checkpoint` and `prune_checkpoints` never
    // see an in-progress copy.
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

/// Remove leftover `.checkpoint-*.tmp` entries, left when a writer
/// crashed mid-compact. This is safe to run before compacting, because
/// each directory has a single checkpoint writer.
fn sweep_stale_tmp(checkpoints_dir: &Path) -> Result<(), StateError> {
    entry_names(checkpoints_dir)?
        .into_iter()
        .filter(|(name, _)| CheckpointEntry::parse(name) == CheckpointEntry::Tmp)
        .try_for_each(|(_, path)| remove_dir_or_file(&path))
}

/// Return the highest-block checkpoint under `checkpoints_dir`. Returns
/// `None` if there are none, or the directory does not exist. A
/// checkpoint is either a directory holding `mdbx.dat`, or a single
/// file, depending on the mdbx build.
///
/// # Errors
///
/// Returns [`StateError`] if the directory listing fails partway through.
pub(crate) fn latest_checkpoint(
    checkpoints_dir: &Path,
) -> Result<Option<CheckpointInfo>, StateError> {
    let best = entry_names(checkpoints_dir)?
        .into_iter()
        .filter_map(|(name, path)| match CheckpointEntry::parse(&name) {
            CheckpointEntry::Checkpoint(block) => Some(CheckpointInfo { block, path }),
            CheckpointEntry::Tmp | CheckpointEntry::MdbxData | CheckpointEntry::Other => None,
        })
        // `min_by_key` keeps the first of equal keys, unlike `max_by_key`
        // (which keeps the last); this matches the original loop's
        // `block > best.block` strict comparison.
        .min_by_key(|c| std::cmp::Reverse(c.block));
    Ok(best)
}

/// Delete every checkpoint older than the `keep_from` block, and keep
/// newer ones. Returns how many were removed. This bounds checkpoint
/// disk use.
///
/// # Errors
///
/// Returns [`StateError`] if the directory listing or a removal fails.
pub fn prune_checkpoints(checkpoints_dir: &Path, keep_from: u64) -> Result<usize, StateError> {
    entry_names(checkpoints_dir)?
        .into_iter()
        .filter_map(|(name, path)| match CheckpointEntry::parse(&name) {
            CheckpointEntry::Checkpoint(block) if block < keep_from => Some(path),
            _ => None,
        })
        .try_fold(0usize, |removed, path| {
            remove_dir_or_file(&path).map(|()| removed + 1)
        })
}

/// Restore `checkpoint` into `state_dir`. `state_dir` must be empty or
/// absent. This copies the compacted mdbx env files across, and returns
/// the restored `last_committed_block`. The caller then opens
/// `state_dir` normally, and the standard resume path replays the tail.
///
/// # Errors
///
/// Returns [`StateError`] if the checkpoint fails verification, if
/// `state_dir` already holds a populated state DB, or if the file copy
/// fails.
pub(crate) fn restore_checkpoint(
    checkpoint: &Path,
    state_dir: &Path,
    expected_genesis: Option<B256>,
) -> Result<u64, StateError> {
    // Verify before copying. An unverifiable or foreign image must never
    // become this node's state.
    let manifest = verify_checkpoint(checkpoint, expected_genesis)?;
    std::fs::create_dir_all(state_dir)?;
    // Refuse to overwrite an existing, populated state DB. Restore is
    // only for a fresh, wiped node.
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

/// Restore the newest verifiable checkpoint under `checkpoints_dir`
/// into `state_dir`. Returns `(restored_block, checkpoint_path)`.
///
/// A checkpoint that fails verification is quarantined: renamed to a
/// hidden `.rejected-<name>` that the scanners never pick up. The
/// function then tries the next-newest checkpoint.
///
/// A bad image must cost the node only one rung of its fallback ladder
/// (older checkpoint, then peer fetch, then genesis). It must never
/// wedge the node in a restart loop.
///
/// Returns `Ok(None)` when no restorable checkpoint remains.
///
/// # Errors
///
/// Returns [`StateError`] if the checkpoint directory cannot be listed, or
/// if a checkpoint fails verification and then cannot even be quarantined.
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
            Err(e) => quarantine_checkpoint(&ckpt.path, state_dir, &e)?,
        }
    }
}

/// Quarantine a checkpoint that failed verification: clean up any
/// partial restore, rename it out of the scan path, and log why. The
/// caller retries with the next-newest checkpoint.
fn quarantine_checkpoint(path: &Path, state_dir: &Path, e: &StateError) -> Result<(), StateError> {
    // A failure after verification, such as I/O failing mid-copy, may
    // have staged a partial data file. A leftover file would make the
    // next attempt refuse with "state dir already holds a DB".
    let _ = std::fs::remove_file(state_dir.join("mdbx.dat"));
    let name = path
        .file_name()
        .map_or_else(|| "checkpoint".into(), |n| n.to_string_lossy().into_owned());
    let rejected = path.with_file_name(format!(".rejected-{name}"));
    warn!(
        checkpoint = %path.display(),
        error = %e,
        quarantined_as = %rejected.display(),
        "checkpoint failed verification; quarantining and trying the next-newest"
    );
    // If even the rename fails, the loop cannot make progress. Report
    // the original refusal instead of spinning.
    std::fs::rename(path, &rejected).map_err(|re| quarantine_failed_message(path, e, &re))
}

/// Builds the error for a checkpoint that failed verification and could
/// not be quarantined. Its text has a unit test.
fn quarantine_failed_message(
    path: &Path,
    verify_err: &StateError,
    rename_err: &std::io::Error,
) -> StateError {
    StateError::Recovery(format!(
        "checkpoint {} failed verification ({verify_err}) and could not be quarantined: {rename_err}",
        path.display()
    ))
}

/// Resolve a checkpoint's mdbx data file. This is either
/// `<checkpoint>/mdbx.dat`, when the env was copied in subdir mode, or
/// the `<checkpoint>` file itself, in single-file mode. Either way, it
/// is a complete mdbx image. Dir-mode open reads it as
/// `<state_dir>/mdbx.dat`.
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
    Ok(entry_names(dir)?
        .into_iter()
        .find(|(name, path)| {
            path.is_file() && CheckpointEntry::parse(name) == CheckpointEntry::MdbxData
        })
        .map(|(_, path)| path))
}

/// Move a stale state DB aside into `<state_dir>/stale/`. The next
/// startup then sees a fresh directory and restores from a checkpoint,
/// while an operator keeps one generation of the old DB for inspection.
/// This drops any previous parked copy. Returns the parked path, or
/// `None` if there was nothing to park.
///
/// The resync fallback uses this. A cursor below the cluster's
/// retention floor can never catch up, so its DB is unusable as-is. But
/// an automated path must not delete state outright.
///
/// # Errors
///
/// Returns [`StateError`] if the directory listing, or a remove or
/// rename, fails.
pub fn park_state_db(state_dir: &Path) -> Result<Option<PathBuf>, StateError> {
    if !has_state_db(state_dir)? {
        return Ok(None);
    }
    let parked = state_dir.join("stale");
    if parked.exists() {
        std::fs::remove_dir_all(&parked)?;
    }
    std::fs::create_dir_all(&parked)?;
    std::fs::read_dir(state_dir)?.try_for_each(|entry| park_one(&entry?, &parked))?;
    info!(parked = %parked.display(), "parked stale state DB");
    Ok(Some(parked))
}

/// Move one `read_dir` entry into `parked`, unless it already is
/// `parked` itself (the directory this same call just created).
fn park_one(entry: &std::fs::DirEntry, parked: &Path) -> Result<(), StateError> {
    if entry.path() != parked {
        std::fs::rename(entry.path(), parked.join(entry.file_name()))?;
    }
    Ok(())
}

/// Returns true if `dir` already contains an mdbx data file, meaning a
/// populated state DB.
///
/// A caller can use this to decide whether a cold start should restore
/// a checkpoint, when the directory is empty, or resume the existing
/// DB. This check does not open the env, which would itself create the
/// data file.
///
/// # Errors
///
/// Returns [`StateError`] if the directory listing fails partway through.
pub fn has_state_db(dir: &Path) -> Result<bool, StateError> {
    Ok(entry_names(dir)?
        .iter()
        .any(|(name, _)| CheckpointEntry::parse(name) == CheckpointEntry::MdbxData))
}
