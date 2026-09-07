//! mdbx Environment opener and table-handle cache.
//!
//! Every other module in this crate uses `StateEnv` for the env handle.
//! Geometry is set once, at open time, as described in `geometry.rs`.
//!
//! This crate uses the `signet-libmdbx` 0.8 binding (MIT/Apache). The
//! environment runs in synchronized transaction mode (`begin_ro_sync` and
//! `begin_rw_sync`). This lets the writer thread and the executor's
//! read-only snapshot thread share handles safely.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use signet_libmdbx::sys::{EnvironmentKind, PageSize};
use signet_libmdbx::{DatabaseFlags, Environment, EnvironmentFlags, Geometry, Mode, SyncMode};

use crate::error::StateError;
use crate::geometry::{
    GROWTH_STEP, MAX_DBS, MAX_READERS, PAGE_SIZE, SHRINK_STEP, SIZE_LOWER, SIZE_UPPER,
};
use crate::schema::ALL_TABLES;

/// Durability mode passed through to libmdbx.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Durability {
    /// `Durable` mode. It calls fdatasync on each commit. Use it in production.
    Durable,
    /// `SafeNoSync` mode. Commit returns after the page-table flush, but
    /// skips fdatasync. Use it only in tests. This mode is unsafe on real
    /// hosts, even with power-loss-protected (PLP) `NVMe`.
    SafeNoSync,
}

impl Durability {
    fn into_sync_mode(self) -> SyncMode {
        match self {
            Durability::Durable => SyncMode::Durable,
            Durability::SafeNoSync => SyncMode::SafeNoSync,
        }
    }
}

#[derive(Debug, Clone)]
pub struct StateEnvBuilder {
    path: PathBuf,
    durability: Durability,
    max_readers: u64,
    read_only: bool,
    write_map: bool,
}

impl StateEnvBuilder {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            durability: Durability::Durable,
            max_readers: MAX_READERS,
            write_map: false,
            read_only: false,
        }
    }

    #[must_use]
    pub fn durability(mut self, d: Durability) -> Self {
        self.durability = d;
        self
    }

    /// mdbx WRITEMAP mode. Dirty pages mutate the map directly, and commit
    /// skips the per-page pwrite pass. In one measurement, this cut commit
    /// time from 30-45 ms to about 4 ms for a 16k-value block under
    /// `SafeNoSync`.
    ///
    /// The trade-off: a stray write in this process can corrupt the map,
    /// because there is no copy-on-write isolation. This mode is opt-in,
    /// for benchmarking and for deployments that accept that risk. It is
    /// off by default.
    #[must_use]
    pub fn write_map(mut self, yes: bool) -> Self {
        self.write_map = yes;
        self
    }

    /// Open the environment as read-only. This mode has no writes, no table
    /// creation, and no directory creation.
    ///
    /// This is safe even if another process is actively writing to the same
    /// state directory. mdbx supports multiple processes: a reader takes an
    /// MVCC snapshot and never blocks the writer. Tooling uses this to
    /// inspect a live node. Both `kardamom-statecheck` and the end-to-end
    /// test suite rely on it.
    ///
    /// The env must already exist and be initialized. `Durability` has no
    /// effect in this mode, because nothing syncs.
    #[must_use]
    pub fn read_only(mut self, yes: bool) -> Self {
        self.read_only = yes;
        self
    }

    /// # Errors
    ///
    /// Returns [`StateError`] if the state directory cannot be created, or
    /// if the underlying mdbx environment fails to open.
    pub fn open(self) -> Result<StateEnv, StateError> {
        if !self.read_only {
            std::fs::create_dir_all(&self.path)?;
        }

        let flags = EnvironmentFlags {
            mode: if self.read_only {
                Mode::ReadOnly
            } else {
                Mode::ReadWrite {
                    sync_mode: self.durability.into_sync_mode(),
                }
            },
            no_rdahead: true,
            coalesce: true,
            liforeclaim: true,
            ..Default::default()
        };

        let mut builder = Environment::builder();
        if self.write_map {
            builder.write_map();
        }
        builder
            .set_max_dbs(MAX_DBS)
            .set_max_readers(self.max_readers)
            .set_geometry(Geometry {
                size: Some(SIZE_LOWER..SIZE_UPPER),
                growth_step: Some(GROWTH_STEP),
                shrink_threshold: Some(SHRINK_STEP),
                page_size: Some(PageSize::Set(PAGE_SIZE)),
            })
            .set_flags(flags);

        let env = builder.open(&self.path)?;

        // This crate uses the no-write-map mode by default, the safest
        // choice for arbitrary kernels and signet-libmdbx's own default.
        // Check it here, once, at open time: a bad env kind then fails
        // startup cleanly, instead of panicking the writer thread later
        // and stranding every consumer of the snapshot channel.
        if !matches!(
            env.env_kind(),
            EnvironmentKind::Default | EnvironmentKind::WriteMap
        ) {
            return Err(StateError::Recovery(format!(
                "state env at {} opened with unexpected env kind {:?}",
                self.path.display(),
                env.env_kind()
            )));
        }

        // Create every named DB once, so handles are cached in the
        // environment. A downstream read-only transaction then does not
        // need to call `create_db`. Skip this step in read-only mode: the
        // tables already exist, and a read-write transaction is not
        // possible in that mode.
        if !self.read_only {
            let txn = env.begin_rw_sync()?;
            for name in ALL_TABLES {
                txn.create_db(Some(name), DatabaseFlags::empty())?;
            }
            txn.commit()?;
        }

        Ok(StateEnv {
            env: Arc::new(env),
            path: self.path,
        })
    }
}

/// A shared handle to an open mdbx environment. It is cheap to clone.
#[derive(Debug, Clone)]
pub struct StateEnv {
    pub(crate) env: Arc<Environment>,
    pub(crate) path: PathBuf,
}

impl StateEnv {
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    #[must_use]
    pub fn raw(&self) -> &Environment {
        &self.env
    }

    /// Read the on-disk schema version, if the `meta` table has one. Opens
    /// and ends its own transaction, so the borrow never outlives this call.
    pub(crate) fn read_schema_version(&self) -> Result<Option<u32>, StateError> {
        let txn = self.raw().begin_rw_sync()?;
        let meta = txn.open_db(Some(crate::schema::TABLE_META))?;
        crate::meta::read_meta_u32(&txn, meta, crate::meta::KEY_SCHEMA_VERSION)
    }

    /// Write the schema-version meta key on first start, or check it on a
    /// later one. A version other than [`crate::meta::SCHEMA_VERSION`]
    /// refuses to start: the compare happens outside any transaction scope,
    /// so the check itself never blocks a concurrent read.
    pub(crate) fn ensure_schema_version(&self) -> Result<(), StateError> {
        use crate::meta::{KEY_SCHEMA_VERSION, SCHEMA_VERSION, encode_u32};
        use signet_libmdbx::WriteFlags;

        match self.read_schema_version()? {
            None => {
                let txn = self.raw().begin_rw_sync()?;
                let meta = txn.open_db(Some(crate::schema::TABLE_META))?;
                txn.put(
                    meta,
                    KEY_SCHEMA_VERSION,
                    encode_u32(SCHEMA_VERSION),
                    WriteFlags::UPSERT,
                )?;
                txn.commit()?;
            }
            Some(on_disk) if on_disk != SCHEMA_VERSION => {
                return Err(StateError::Recovery(format!(
                    "schema version mismatch: on-disk={on_disk}, code={SCHEMA_VERSION}"
                )));
            }
            Some(_) => {}
        }
        Ok(())
    }
}
