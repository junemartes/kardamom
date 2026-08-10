//! Checkpoint manifests: what a checkpoint's bytes ARE, and the shared
//! verify/publish helpers every path that adopts or emits an image goes
//! through (the recovery-C hardening; see [`CheckpointManifest`]).

use std::path::{Path, PathBuf};

use alloy_primitives::B256;

use crate::env::StateEnv;
use crate::error::StateError;

use super::checkpoint_data_file;

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

/// The two refusal checks every checkpoint image must pass before it can
/// become this node's state — integrity (the bytes hash to what the source
/// claims) and chain identity (the image belongs to this node's chain).
/// Shared by the disk-restore path ([`verify_checkpoint`], hash claimed by
/// the MANIFEST) and the peer-fetch path (hash claimed by the peer's
/// headers): this is the recovery-C hardening, and a one-sided change here
/// silently weakens whichever path stops getting it.
///
/// `image` names the image and `claimant` the source of the expected hash,
/// for the refusal messages (tests assert on the CORRUPT / DIFFERENT CHAIN
/// markers).
pub(crate) fn check_image_identity(
    image: &str,
    claimant: &str,
    got_keccak: B256,
    want_keccak: B256,
    image_genesis: B256,
    expected_genesis: Option<B256>,
) -> Result<(), StateError> {
    if got_keccak != want_keccak {
        return Err(StateError::CorruptCheckpointImage {
            image: image.into(),
            claimant: claimant.into(),
            got: got_keccak,
            want: want_keccak,
        });
    }
    if let Some(want) = expected_genesis
        && want != image_genesis
    {
        return Err(StateError::ForeignChainCheckpoint {
            image: image.into(),
            image_genesis,
            expected: want,
        });
    }
    Ok(())
}

/// Publish a staged checkpoint: manifest INSIDE the tmp entry, then one
/// rename — image and manifest become visible atomically, so an observable
/// checkpoint is always verifiable and self-contained under any copy
/// mechanism. A crash before the rename leaves only the hidden tmp entry
/// (swept by `sweep_stale_tmp` / re-fetched next time).
pub(crate) fn publish_checkpoint(
    tmp: &Path,
    dest: &Path,
    manifest: &CheckpointManifest,
) -> Result<(), StateError> {
    std::fs::write(tmp.join("MANIFEST"), manifest.encode())?;
    std::fs::rename(tmp, dest)?;
    Ok(())
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
    check_image_identity(
        &checkpoint.display().to_string(),
        "manifest",
        got,
        manifest.image_keccak,
        manifest.genesis_digest,
        expected_genesis,
    )?;
    Ok(manifest)
}
