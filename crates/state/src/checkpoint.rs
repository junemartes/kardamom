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

/// Sidecar written next to every checkpoint: what these bytes ARE.
///
/// A checkpoint is a bare mdbx image. Renaming it into place makes it atomic
/// against torn writes, but the file itself is entirely self-UNdescribing:
/// nothing binds it to a chain, and the peer transfer is plain HTTP with no
/// checksum. Three failures were therefore silent — bytes corrupted at rest
/// or in flight, a truncated fetch, and (observed in the recovery-C campaign)
/// a checkpoint from a PREVIOUS chain adopted by a fresh node, which then
/// requested a canonical index that chain never had and looped forever.
///
/// Since #143 the VALIDATOR adopts peer checkpoints and bootstraps its trie
/// from them, so a bad adoption becomes its state — and its own shadow-check
/// cannot object, because that rebuilds from the same adopted mirror.
///
/// Format is flat `key=value` lines: no serde dependency, trivially readable
/// by an operator, and forward-compatible (unknown keys are ignored).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointManifest {
    /// Committed block the image was taken at (matches the file name).
    pub block: u64,
    /// keccak256 of the mdbx image bytes.
    pub image_keccak: B256,
    /// `KEY_GENESIS_DIGEST` from the image — the chain-identity binding.
    pub genesis_digest: B256,
}

impl CheckpointManifest {
    pub fn encode(&self) -> String {
        format!(
            "version=1\nblock={}\nimage_keccak={:#x}\ngenesis_digest={:#x}\n",
            self.block, self.image_keccak, self.genesis_digest
        )
    }

    pub fn parse(text: &str) -> Result<Self, StateError> {
        let mut block = None;
        let mut image_keccak = None;
        let mut genesis_digest = None;
        for line in text.lines() {
            let Some((k, v)) = line.split_once('=') else {
                continue;
            };
            match k.trim() {
                "block" => block = v.trim().parse::<u64>().ok(),
                "image_keccak" => image_keccak = v.trim().parse::<B256>().ok(),
                "genesis_digest" => genesis_digest = v.trim().parse::<B256>().ok(),
                _ => {}
            }
        }
        match (block, image_keccak, genesis_digest) {
            (Some(block), Some(image_keccak), Some(genesis_digest)) => Ok(Self {
                block,
                image_keccak,
                genesis_digest,
            }),
            _ => Err(StateError::Recovery(
                "checkpoint manifest is malformed (need block, image_keccak, genesis_digest)"
                    .into(),
            )),
        }
    }
}

/// Manifest path for a checkpoint: `<checkpoint>/MANIFEST`, INSIDE the
/// checkpoint directory. Living inside means the single directory rename
/// publishes image and manifest atomically, and any copy of the checkpoint —
/// peer fetch, chaos re-replication, an operator's rsync — carries its
/// manifest by construction. A sibling file would be silently dropped by
/// exactly the copy paths the manifest exists to protect.
pub fn manifest_path(checkpoint: &Path) -> PathBuf {
    checkpoint.join("MANIFEST")
}

/// keccak256 of a file's bytes, read in chunks (images are hundreds of MB).
pub(crate) fn file_keccak(path: &Path) -> Result<B256, StateError> {
    use std::io::Read;
    let mut f = std::fs::File::open(path)?;
    let mut hasher = alloy_primitives::Keccak256::new();
    let mut buf = vec![0u8; 1 << 20];
    loop {
        let n = f.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hasher.finalize())
}

/// Read the genesis digest an env was seeded with (the chain-identity key).
pub(crate) fn stored_genesis_digest(env: &StateEnv) -> Result<B256, StateError> {
    let txn = env.raw().begin_ro_sync()?;
    let meta = txn.open_db(Some(crate::schema::TABLE_META))?;
    match txn.get::<Vec<u8>>(meta.dbi(), crate::meta::KEY_GENESIS_DIGEST)? {
        Some(b) if b.len() == 32 => Ok(B256::from_slice(&b)),
        _ => Err(StateError::Recovery(
            "env has no genesis digest; refusing to checkpoint an unidentifiable image".into(),
        )),
    }
}

/// Read a checkpoint's manifest without hashing the image (the serve path
/// only needs to describe what it is about to send).
pub fn read_manifest(checkpoint: &Path) -> Result<CheckpointManifest, StateError> {
    let mpath = manifest_path(checkpoint);
    let text = std::fs::read_to_string(&mpath)
        .map_err(|e| StateError::Recovery(format!("no manifest at {}: {e}", mpath.display())))?;
    CheckpointManifest::parse(&text)
}

/// Verify a checkpoint image against its manifest, and (when supplied)
/// against the chain the caller expects. Returns the manifest.
pub fn verify_checkpoint(
    checkpoint: &Path,
    expected_genesis: Option<B256>,
) -> Result<CheckpointManifest, StateError> {
    let mpath = manifest_path(checkpoint);
    let text = std::fs::read_to_string(&mpath).map_err(|e| {
        StateError::Recovery(format!(
            "checkpoint {} has no readable manifest at {}: {e} — refusing to \
             adopt an unverifiable image",
            checkpoint.display(),
            mpath.display()
        ))
    })?;
    let manifest = CheckpointManifest::parse(&text)?;
    let data = checkpoint_data_file(checkpoint)?;
    let got = file_keccak(&data)?;
    if got != manifest.image_keccak {
        return Err(StateError::Recovery(format!(
            "checkpoint {} is CORRUPT: image hashes to {got:#x}, manifest says {:#x}",
            checkpoint.display(),
            manifest.image_keccak
        )));
    }
    if let Some(want) = expected_genesis
        && want != manifest.genesis_digest
    {
        return Err(StateError::Recovery(format!(
            "checkpoint {} belongs to a DIFFERENT CHAIN: its genesis digest is \
             {:#x}, this node's is {want:#x}",
            checkpoint.display(),
            manifest.genesis_digest
        )));
    }
    Ok(manifest)
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
    // Manifest INSIDE the tmp dir, then one rename: image and manifest become
    // visible atomically, so an observable checkpoint is always verifiable
    // and self-contained under any copy mechanism. A crash before the rename
    // leaves only the hidden tmp dir, swept by `sweep_stale_tmp`.
    let manifest = CheckpointManifest {
        block,
        image_keccak: file_keccak(&tmp_data)?,
        genesis_digest: stored_genesis_digest(env)?,
    };
    std::fs::write(tmp.join("MANIFEST"), manifest.encode())?;
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
}
