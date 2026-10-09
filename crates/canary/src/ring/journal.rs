//! The journal of one ring account: its in-flight transaction, written
//! to disk before the transaction leaves the canary. A restart reads it
//! back, so a transaction whose answer was lost is resolved before its
//! nonce takes other work.

use std::path::PathBuf;

use alloy_primitives::{Address, B256, Bytes};
use serde::{Deserialize, Serialize};

/// A signed transaction the chain may hold.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InFlight {
    pub nonce: u64,
    pub hash: B256,
    pub raw: Bytes,
    /// Whether the transaction may have reached the chain: a submit of
    /// it was accepted, or its answer was lost. A refusal of a
    /// transaction that never reached the chain frees its nonce; a
    /// refusal of one that may have reached it does not.
    pub published: bool,
}

/// The journal file of one account.
#[derive(Debug, Clone)]
pub struct Journal {
    path: PathBuf,
}

impl Journal {
    /// The journal of `address` under `dir`.
    #[must_use]
    pub fn new(dir: &std::path::Path, address: Address) -> Self {
        Self {
            path: dir.join(format!("{address:#x}.json")),
        }
    }

    /// The in-flight transaction the journal holds, if any.
    ///
    /// # Errors
    ///
    /// Returns an error when the file exists but does not read or parse.
    pub async fn load(&self) -> anyhow::Result<Option<InFlight>> {
        match tokio::fs::read(&self.path).await {
            Ok(bytes) => {
                Ok(Some(serde_json::from_slice(&bytes).map_err(|e| {
                    anyhow::anyhow!("journal {}: {e}", self.path.display())
                })?))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(anyhow::anyhow!("journal {}: {e}", self.path.display())),
        }
    }

    /// Store `entry` durably: a temporary file, synced, then renamed over
    /// the journal.
    ///
    /// # Errors
    ///
    /// Returns an error when a write, the sync or the rename fails.
    pub async fn store(&self, entry: &InFlight) -> std::io::Result<()> {
        let tmp = self.path.with_extension("json.tmp");
        let bytes = serde_json::to_vec(entry).map_err(std::io::Error::other)?;
        let mut file = tokio::fs::File::create(&tmp).await?;
        tokio::io::AsyncWriteExt::write_all(&mut file, &bytes).await?;
        file.sync_all().await?;
        tokio::fs::rename(&tmp, &self.path).await
    }

    /// Empty the journal: the account holds no in-flight transaction.
    ///
    /// # Errors
    ///
    /// Returns an error when the file exists and does not delete.
    pub async fn clear(&self) -> std::io::Result<()> {
        match tokio::fs::remove_file(&self.path).await {
            Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(e),
            _ => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn an_entry_survives_a_reopen_and_clears() {
        let dir = tempfile::tempdir().unwrap();
        let journal = Journal::new(dir.path(), Address::repeat_byte(7));
        assert_eq!(journal.load().await.unwrap(), None);
        let entry = InFlight {
            nonce: 9,
            hash: B256::repeat_byte(1),
            raw: Bytes::from_static(&[1, 2, 3]),
            published: false,
        };
        journal.store(&entry).await.unwrap();
        let reopened = Journal::new(dir.path(), Address::repeat_byte(7));
        assert_eq!(reopened.load().await.unwrap(), Some(entry));
        reopened.clear().await.unwrap();
        reopened.clear().await.unwrap();
        assert_eq!(journal.load().await.unwrap(), None);
    }
}
