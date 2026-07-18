//! Data-availability blob store: the *bytes* behind the on-chain commitments.
//!
//! EIP-4844 blob bytes are not retained on the execution layer and only ~18
//! days on the consensus layer, so a rollup fetches historical blob bytes from
//! a DA layer / blob archive keyed by versioned hash, while L1 retains only the
//! commitments (versioned hashes) + ordering. This models that split: the
//! batcher writes every posted blob here keyed by its KZG versioned hash; the
//! reconstructor fetches by the hashes it reads from `BatchPosted` events, so it
//! only ever consumes blob bytes that L1 has committed to.
//!
//! [`FsBlobStore`] is a filesystem implementation suitable for the local
//! cluster and tests (a shared volume plays the DA layer). A production
//! deployment swaps in a [`BlobSource`] backed by a beacon-chain sidecar RPC or
//! a dedicated DA network — the reconstruction path is generic over the trait.

use std::path::{Path, PathBuf};

use alloy_eips::eip4844::{BYTES_PER_BLOB, Blob};
use alloy_primitives::B256;

use crate::error::BatcherError;

/// Read side: fetch a blob's bytes by its versioned hash. Implemented by the
/// filesystem store here and (in production) by a beacon/DA-network client.
pub trait BlobSource {
    /// Fetch the blob whose KZG versioned hash is `versioned_hash`.
    fn fetch_blob(&self, versioned_hash: B256) -> Result<Blob, BatcherError>;
}

/// Filesystem-backed DA store. One file per blob, named by its versioned hash.
#[derive(Clone, Debug)]
pub struct FsBlobStore {
    dir: PathBuf,
}

impl FsBlobStore {
    /// Open (creating if needed) a blob store rooted at `dir`.
    pub fn open(dir: impl AsRef<Path>) -> Result<Self, BatcherError> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;
        Ok(Self { dir })
    }

    fn path_for(&self, versioned_hash: B256) -> PathBuf {
        // `{:x}` renders the 32-byte hash as 64 lowercase hex chars, no `0x`.
        self.dir.join(format!("{versioned_hash:x}.blob"))
    }

    /// Persist `blob` under its `versioned_hash`. Idempotent: re-storing the
    /// same blob overwrites with identical bytes.
    pub fn put(&self, versioned_hash: B256, blob: &Blob) -> Result<(), BatcherError> {
        std::fs::write(self.path_for(versioned_hash), blob.as_slice())?;
        Ok(())
    }

    /// Number of blobs currently held (counts `*.blob` files).
    pub fn len(&self) -> usize {
        std::fs::read_dir(&self.dir)
            .map(|rd| {
                rd.filter_map(Result::ok)
                    .filter(|e| e.path().extension().is_some_and(|x| x == "blob"))
                    .count()
            })
            .unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl BlobSource for FsBlobStore {
    fn fetch_blob(&self, versioned_hash: B256) -> Result<Blob, BatcherError> {
        let path = self.path_for(versioned_hash);
        let bytes = std::fs::read(&path).map_err(|e| {
            BatcherError::Reconstruct(format!("blob {versioned_hash:x} not in DA store: {e}"))
        })?;
        if bytes.len() != BYTES_PER_BLOB {
            return Err(BatcherError::Reconstruct(format!(
                "blob {versioned_hash:x} is {} bytes, expected {BYTES_PER_BLOB}",
                bytes.len()
            )));
        }
        let mut arr = [0u8; BYTES_PER_BLOB];
        arr.copy_from_slice(&bytes);
        Ok(Blob::new(arr))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blob::pack_to_blobs;

    #[test]
    fn put_then_fetch_roundtrips_blob_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let store = FsBlobStore::open(dir.path()).unwrap();
        assert!(store.is_empty());

        let blobs = pack_to_blobs(b"kardamom da store round trip payload").unwrap();
        let vh = B256::repeat_byte(0x11);
        store.put(vh, &blobs[0]).unwrap();
        assert_eq!(store.len(), 1);

        let got = store.fetch_blob(vh).unwrap();
        assert_eq!(got.as_slice(), blobs[0].as_slice());
    }

    #[test]
    fn fetch_missing_blob_errors() {
        let dir = tempfile::tempdir().unwrap();
        let store = FsBlobStore::open(dir.path()).unwrap();
        let err = store.fetch_blob(B256::repeat_byte(0xAB)).unwrap_err();
        assert!(matches!(err, BatcherError::Reconstruct(_)));
    }
}
