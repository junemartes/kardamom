//! The archive on disk: one directory, plain files, one write per item.
//!
//! - `batches/<index>.json`: a [`BatchEntry`].
//! - `epochs/<l1_block>.rkyv`: the epoch record of that block, the bytes
//!   the epoch stream carries, and the bytes of the `epoch` field of the
//!   block's record on `l1_blocks`.
//! - `blocks/<l1_block>.rkyv`: the block's `l1_blocks` record.
//! - `payloads/<keccak(cert)>.bin`: the payload bytes, named by the
//!   certificate's hash (a certificate is a few hundred bytes).
//! - `cursor.json`: the [`Cursor`], written last, after the items of a
//!   block, so a crash between the two re-indexes the block and never
//!   skips it.
//!
//! Files, not a database: the archive is re-derivable, an operator can
//! read it with `ls`, and a volume snapshot is a backup.

use std::fs;
use std::path::{Path, PathBuf};

use alloy_primitives::{Bytes, keccak256};
use kardamom_types::epoch::EpochRecord;

use crate::{BatchEntry, Cursor, IndexerError, L1Block};

/// The archive rooted at one directory.
#[derive(Clone, Debug)]
pub struct Store {
    root: PathBuf,
}

impl Store {
    /// Open the archive at `root`; create its directories.
    ///
    /// # Errors
    /// Returns an error when a directory cannot be created.
    pub fn open(root: impl AsRef<Path>) -> Result<Self, IndexerError> {
        let root = root.as_ref().to_path_buf();
        for sub in ["batches", "blocks", "epochs", "payloads"] {
            fs::create_dir_all(root.join(sub))?;
        }
        Ok(Self { root })
    }

    fn write_atomic(path: &Path, bytes: &[u8]) -> Result<(), IndexerError> {
        let tmp = path.with_extension("tmp");
        fs::write(&tmp, bytes)?;
        fs::rename(&tmp, path)?;
        Ok(())
    }

    /// The persisted cursor; the default before the first block.
    ///
    /// # Errors
    /// Returns an error when the file exists and does not parse.
    pub fn cursor(&self) -> Result<Cursor, IndexerError> {
        let path = self.root.join("cursor.json");
        if !path.exists() {
            return Ok(Cursor::default());
        }
        let bytes = fs::read(path)?;
        serde_json::from_slice(&bytes).map_err(|e| IndexerError::Store(format!("cursor: {e}")))
    }

    /// Persist the cursor. Written last for a block.
    ///
    /// # Errors
    /// Returns an error when the write fails.
    pub fn set_cursor(&self, cursor: &Cursor) -> Result<(), IndexerError> {
        let bytes = serde_json::to_vec_pretty(cursor)
            .map_err(|e| IndexerError::Store(format!("cursor: {e}")))?;
        Self::write_atomic(&self.root.join("cursor.json"), &bytes)
    }

    /// Store a batch's descriptor. Its payload goes through [`Self::put_payload`].
    ///
    /// # Errors
    /// Returns an error when the write fails.
    pub fn put_batch(&self, entry: &BatchEntry) -> Result<(), IndexerError> {
        let bytes = serde_json::to_vec_pretty(entry)
            .map_err(|e| IndexerError::Store(format!("batch {}: {e}", entry.index)))?;
        Self::write_atomic(
            &self
                .root
                .join("batches")
                .join(format!("{}.json", entry.index)),
            &bytes,
        )
    }

    /// A stored batch, or `None`.
    ///
    /// # Errors
    /// Returns an error when the file exists and does not parse.
    pub fn batch(&self, index: u64) -> Result<Option<BatchEntry>, IndexerError> {
        let path = self.root.join("batches").join(format!("{index}.json"));
        if !path.exists() {
            return Ok(None);
        }
        let bytes = fs::read(path)?;
        serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|e| IndexerError::Store(format!("batch {index}: {e}")))
    }

    fn payload_path(&self, da_cert: &Bytes) -> PathBuf {
        self.root
            .join("payloads")
            .join(format!("{:x}.bin", keccak256(da_cert)))
    }

    /// Store a payload under its certificate.
    ///
    /// # Errors
    /// Returns an error when the write fails.
    pub fn put_payload(&self, da_cert: &Bytes, payload: &[u8]) -> Result<(), IndexerError> {
        Self::write_atomic(&self.payload_path(da_cert), payload)
    }

    /// A stored payload, or `None`.
    ///
    /// # Errors
    /// Returns an error when the read fails.
    pub fn payload(&self, da_cert: &Bytes) -> Result<Option<Vec<u8>>, IndexerError> {
        let path = self.payload_path(da_cert);
        if !path.exists() {
            return Ok(None);
        }
        Ok(Some(fs::read(path)?))
    }

    /// Store an epoch record as the bytes the epoch stream carries.
    ///
    /// # Errors
    /// Returns an error when the record does not serialize or the write fails.
    pub fn put_epoch(&self, epoch: &EpochRecord) -> Result<(), IndexerError> {
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(epoch)
            .map_err(|e| IndexerError::Store(format!("epoch {}: {e}", epoch.l1_number)))?;
        Self::write_atomic(
            &self
                .root
                .join("epochs")
                .join(format!("{}.rkyv", epoch.l1_number)),
            &bytes,
        )
    }

    /// Store a block's `l1_blocks` record.
    ///
    /// # Errors
    /// Returns an error when the record does not serialize or the write fails.
    pub fn put_block(&self, block: &L1Block) -> Result<(), IndexerError> {
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(block)
            .map_err(|e| IndexerError::Store(format!("block {}: {e}", block.number)))?;
        Self::write_atomic(
            &self
                .root
                .join("blocks")
                .join(format!("{}.rkyv", block.number)),
            &bytes,
        )
    }

    /// The stored bytes of a block's `l1_blocks` record, or `None`.
    ///
    /// # Errors
    /// Returns an error when the read fails.
    pub fn block_bytes(&self, l1_block: u64) -> Result<Option<Vec<u8>>, IndexerError> {
        let path = self.root.join("blocks").join(format!("{l1_block}.rkyv"));
        if !path.exists() {
            return Ok(None);
        }
        Ok(Some(fs::read(path)?))
    }

    /// The stored bytes of an epoch record, or `None`.
    ///
    /// # Errors
    /// Returns an error when the read fails.
    pub fn epoch_bytes(&self, l1_block: u64) -> Result<Option<Vec<u8>>, IndexerError> {
        let path = self.root.join("epochs").join(format!("{l1_block}.rkyv"));
        if !path.exists() {
            return Ok(None);
        }
        Ok(Some(fs::read(path)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::BlockId;
    use alloy_primitives::B256;

    #[test]
    fn cursor_batch_and_epoch_round_trip() {
        let dir = tempfile_dir();
        let store = Store::open(&dir).unwrap();
        assert_eq!(store.cursor().unwrap(), Cursor::default());
        let entry = BatchEntry {
            index: 7,
            da_cert: Bytes::from(vec![0x03, 0xAA]),
            l2_block_start: 10,
            l2_block_end: 12,
            records_commitment: B256::repeat_byte(2),
            l1_block: 100,
            l1_tx: B256::repeat_byte(3),
        };
        store.put_batch(&entry).unwrap();
        assert_eq!(store.batch(7).unwrap(), Some(entry.clone()));
        assert_eq!(store.batch(8).unwrap(), None);
        store.put_payload(&entry.da_cert, b"payload").unwrap();
        assert_eq!(
            store.payload(&entry.da_cert).unwrap(),
            Some(b"payload".to_vec())
        );
        assert_eq!(store.payload(&Bytes::from(vec![0x03])).unwrap(), None);
        let epoch = EpochRecord {
            l1_number: 100,
            l1_hash: B256::repeat_byte(4),
            deposits: vec![],
        };
        store.put_epoch(&epoch).unwrap();
        assert!(store.epoch_bytes(100).unwrap().is_some());
        assert!(store.epoch_bytes(101).unwrap().is_none());
        let cursor = Cursor {
            l1_block: Some(BlockId {
                number: 100,
                hash: B256::repeat_byte(4),
            }),
            last_batch: Some(7),
        };
        store.set_cursor(&cursor).unwrap();
        assert_eq!(store.cursor().unwrap(), cursor);
        fs::remove_dir_all(dir).unwrap();
    }

    fn tempfile_dir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "kardamom-l1-indexer-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&dir).unwrap();
        dir
    }
}
