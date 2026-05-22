//! Thin MDBX env wrapper. Owns the `Database` handle, creates all named tables
//! on first open, and enforces the schema-version + chain-id guards.

use std::path::Path;

use libmdbx::{
    Database, DatabaseOptions, Mode, NoWriteMap, ReadWriteOptions, SyncMode, TableFlags,
    Transaction, WriteFlags, RO, RW,
};

use crate::error::StateError;
use crate::schema::{self, tables};

/// Soft upper bound on the number of named tables we ever expect. We have 7
/// today; headroom is cheap and avoids reopening the env to grow later.
const MAX_TABLES: u64 = 16;

#[derive(Debug)]
pub struct Store {
    env: Database<NoWriteMap>,
}

impl Store {
    /// Open (creating if absent) the MDBX env at `path` and ensure all
    /// schema tables exist. Validates / initializes the schema-version and
    /// chain-id meta entries: a fresh env is stamped with the caller's
    /// `chain_id` + the current `SCHEMA_VERSION`; an existing env must
    /// match both.
    pub fn open(path: &Path, chain_id: u64) -> Result<Self, StateError> {
        std::fs::create_dir_all(path)?;

        let options = DatabaseOptions {
            max_tables: Some(MAX_TABLES),
            mode: Mode::ReadWrite(ReadWriteOptions {
                sync_mode: SyncMode::Durable,
                ..Default::default()
            }),
            ..Default::default()
        };
        let env = Database::<NoWriteMap>::open_with_options(path, options)?;

        // Create every table up-front so subsequent reads don't have to.
        {
            let txn = env.begin_rw_txn()?;
            for name in tables::ALL {
                txn.create_table(Some(name), TableFlags::default())?;
            }
            txn.commit()?;
        }

        // Verify / initialize meta.
        let store = Self { env };
        store.check_or_init_meta(chain_id)?;
        Ok(store)
    }

    fn check_or_init_meta(&self, chain_id: u64) -> Result<(), StateError> {
        let txn = self.env.begin_rw_txn()?;
        let meta = txn.open_table(Some(tables::META))?;

        let stored_version = txn.get::<Vec<u8>>(&meta, schema::meta::SCHEMA_VERSION)?;
        let stored_chain = txn.get::<Vec<u8>>(&meta, schema::meta::CHAIN_ID)?;

        match (stored_version, stored_chain) {
            (None, None) => {
                // Fresh env. Stamp it.
                txn.put(
                    &meta,
                    schema::meta::SCHEMA_VERSION,
                    schema::SCHEMA_VERSION.to_be_bytes(),
                    WriteFlags::default(),
                )?;
                txn.put(
                    &meta,
                    schema::meta::CHAIN_ID,
                    chain_id.to_be_bytes(),
                    WriteFlags::default(),
                )?;
                txn.commit()?;
                Ok(())
            }
            (Some(version_bytes), Some(chain_bytes)) => {
                let found_version = decode_u32(&version_bytes)?;
                if found_version != schema::SCHEMA_VERSION {
                    return Err(StateError::SchemaVersionMismatch {
                        found: found_version,
                        expected: schema::SCHEMA_VERSION,
                    });
                }
                let found_chain = decode_u64(&chain_bytes)?;
                if found_chain != chain_id {
                    return Err(StateError::ChainIdMismatch {
                        found: found_chain,
                        expected: chain_id,
                    });
                }
                // Read-only check; drop the txn without committing.
                Ok(())
            }
            // Half-initialized envs only happen if someone hand-edited the
            // file. Refuse to load rather than guess.
            (Some(_), None) => Err(StateError::MissingMeta("chain_id")),
            (None, Some(_)) => Err(StateError::MissingMeta("schema_version")),
        }
    }

    pub fn begin_ro(&self) -> Result<Transaction<'_, RO, NoWriteMap>, StateError> {
        Ok(self.env.begin_ro_txn()?)
    }

    pub fn begin_rw(&self) -> Result<Transaction<'_, RW, NoWriteMap>, StateError> {
        Ok(self.env.begin_rw_txn()?)
    }
}

pub(crate) fn decode_u32(bytes: &[u8]) -> Result<u32, StateError> {
    if bytes.len() != 4 {
        return Err(StateError::codec(format!(
            "u32 expects 4 bytes, got {}",
            bytes.len()
        )));
    }
    let mut buf = [0u8; 4];
    buf.copy_from_slice(bytes);
    Ok(u32::from_be_bytes(buf))
}

pub(crate) fn decode_u64(bytes: &[u8]) -> Result<u64, StateError> {
    if bytes.len() != 8 {
        return Err(StateError::codec(format!(
            "u64 expects 8 bytes, got {}",
            bytes.len()
        )));
    }
    let mut buf = [0u8; 8];
    buf.copy_from_slice(bytes);
    Ok(u64::from_be_bytes(buf))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn open_creates_fresh_env() {
        let dir = TempDir::new().unwrap();
        let store = Store::open(dir.path(), 412346).expect("open");
        // Reading any table on a fresh env returns None for any key.
        let txn = store.begin_ro().unwrap();
        let accounts = txn.open_table(Some(tables::ACCOUNTS)).unwrap();
        let val = txn.get::<Vec<u8>>(&accounts, &[0u8; 20]).unwrap();
        assert!(val.is_none());
    }

    #[test]
    fn open_reopens_existing_env_with_same_chain_id() {
        let dir = TempDir::new().unwrap();
        {
            let _store = Store::open(dir.path(), 412346).unwrap();
        }
        // Reopen — meta already stamped.
        let _store = Store::open(dir.path(), 412346).expect("reopen");
    }

    #[test]
    fn open_rejects_wrong_chain_id() {
        let dir = TempDir::new().unwrap();
        let _store = Store::open(dir.path(), 412346).unwrap();
        drop(_store);
        let err = Store::open(dir.path(), 1).unwrap_err();
        assert!(
            matches!(
                err,
                StateError::ChainIdMismatch {
                    found: 412346,
                    expected: 1
                }
            ),
            "got {err:?}"
        );
    }

    #[test]
    fn open_recovers_from_tables_created_but_meta_unstamped() {
        // Simulate a crash between `Store::open`'s two MDBX commits: the
        // first creates the tables, the second stamps meta. If we crash
        // in between we end up with tables but empty meta. The next open
        // must succeed and stamp meta.
        let dir = TempDir::new().unwrap();
        {
            let store = Store::open(dir.path(), 412346).unwrap();
            // Wipe both meta rows to mimic a half-initialized env.
            let txn = store.begin_rw().unwrap();
            let meta = txn.open_table(Some(tables::META)).unwrap();
            txn.del(&meta, schema::meta::SCHEMA_VERSION, None).unwrap();
            txn.del(&meta, schema::meta::CHAIN_ID, None).unwrap();
            txn.commit().unwrap();
        }
        // Reopen against an env whose tables exist but meta is empty —
        // must stamp afresh and succeed.
        let store = Store::open(dir.path(), 412346).expect("recovered");
        let txn = store.begin_ro().unwrap();
        let meta = txn.open_table(Some(tables::META)).unwrap();
        assert!(
            txn.get::<Vec<u8>>(&meta, schema::meta::SCHEMA_VERSION)
                .unwrap()
                .is_some()
        );
        assert!(
            txn.get::<Vec<u8>>(&meta, schema::meta::CHAIN_ID)
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn open_rejects_wrong_schema_version() {
        let dir = TempDir::new().unwrap();
        {
            let store = Store::open(dir.path(), 412346).unwrap();
            // Manually overwrite schema_version with a bogus value.
            let txn = store.begin_rw().unwrap();
            let meta = txn.open_table(Some(tables::META)).unwrap();
            txn.put(
                &meta,
                schema::meta::SCHEMA_VERSION,
                999u32.to_be_bytes(),
                WriteFlags::default(),
            )
            .unwrap();
            txn.commit().unwrap();
        }
        let err = Store::open(dir.path(), 412346).unwrap_err();
        assert!(
            matches!(
                err,
                StateError::SchemaVersionMismatch {
                    found: 999,
                    expected: schema::SCHEMA_VERSION,
                }
            ),
            "got {err:?}"
        );
    }
}
