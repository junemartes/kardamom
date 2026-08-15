//! mdbx Environment opener and table-handle cache.
//!
//! Every other module in this crate goes through `StateEnv` for the env
//! handle. Geometry is set once at open time per `geometry.rs`.
//!
//! We use the `signet-libmdbx` 0.8 binding (MIT/Apache). The
//! environment runs in the synchronized transaction mode (`begin_ro_sync` /
//! `begin_rw_sync`) so the writer thread + executor's RO snapshot thread can
//! share handles safely.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use signet_libmdbx::sys::PageSize;
use signet_libmdbx::{DatabaseFlags, Environment, EnvironmentFlags, Geometry, Mode, SyncMode};

use crate::error::StateError;
use crate::geometry::{
    GROWTH_STEP, MAX_DBS, MAX_READERS, PAGE_SIZE, SHRINK_STEP, SIZE_LOWER, SIZE_UPPER,
};
use crate::schema::ALL_TABLES;

/// Durability mode passed through to libmdbx.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Durability {
    /// `Durable` mode (fdatasync on commit). Use in production.
    Durable,
    /// `SafeNoSync` — commit returns after page-table flush but skips fdatasync.
    /// Use only in tests; combined with PLP NVMe this is unsafe on real hosts.
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

    pub fn durability(mut self, d: Durability) -> Self {
        self.durability = d;
        self
    }

    /// Open the environment READ-ONLY: no writes, no table creation, and no
    /// directory creation. Safe to point at a state dir another process is
    /// actively writing (mdbx is multi-process; a reader takes an MVCC
    /// snapshot and never blocks the writer), which is how tooling inspects a
    /// LIVE node — `kardamom-statecheck` and the e2e suite both rely on it.
    ///
    /// The env must already exist and be initialised; `Durability` is
    /// irrelevant in this mode (nothing syncs).
    /// mdbx WRITEMAP: dirty pages mutate the map directly and commit
    /// skips the per-page pwrite pass — measured 30-45ms -> ~4ms for a
    /// 16k-value block commit under SafeNoSync. The trade: any wild
    /// write in THIS process can corrupt the map (no copy-on-write
    /// isolation), so this is opt-in — benchmarking and deployments
    /// that accept the blast radius. OFF by default.
    pub fn write_map(mut self, yes: bool) -> Self {
        self.write_map = yes;
        self
    }

    pub fn read_only(mut self, yes: bool) -> Self {
        self.read_only = yes;
        self
    }

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

        // Create every named DB once so handles are cached in the environment
        // and a downstream RO txn does not have to call `create_db`. Skipped
        // read-only: the tables already exist (the writing process made them)
        // and an RW txn is impossible in that mode.
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

/// Shared handle to an open mdbx environment. Cheap to clone.
#[derive(Debug, Clone)]
pub struct StateEnv {
    pub(crate) env: Arc<Environment>,
    pub(crate) path: PathBuf,
}

impl StateEnv {
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn raw(&self) -> &Environment {
        &self.env
    }
}
