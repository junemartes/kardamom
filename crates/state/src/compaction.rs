//! Scheduled libmdbx compaction (spec section 5).
//!
//! libmdbx is copy-on-write. Long-running databases fragment. The plan is:
//!
//! 1. Open a hot mirror directory next to the live env.
//! 2. Call `mdbx_env_copy` with `MDBX_CP_COMPACT`. This walks the live env's
//!    pages and writes a compacted copy into the mirror.
//! 3. Atomically swap the mirror into the live path (rename and reopen).
//!
//! In v0, this is a one-shot [`compact_to`] function. An external scheduler,
//! such as a systemd timer or cron, calls it once per day.
//!
//! `signet-libmdbx` 0.8 does not expose `mdbx_env_copy` in its safe API. We
//! call the underlying FFI directly, through `Environment::with_raw_env_ptr`.

use std::ffi::CString;
use std::path::Path;

use signet_libmdbx::ffi;
use tracing::{info, warn};

use crate::env::StateEnv;
use crate::error::StateError;

/// Copy the env to `dest`, with compaction. The caller must swap `dest`
/// into place to make the compacted copy the live env.
///
/// The live env stays online for reads and writes throughout compaction.
/// Compaction runs against a read-only snapshot of the env.
pub fn compact_to(env: &StateEnv, dest: &Path) -> Result<(), StateError> {
    info!(src = %env.path().display(), dst = %dest.display(), "starting compaction");
    if dest.exists() {
        warn!("destination already exists; refusing to overwrite");
        return Err(StateError::Recovery(format!(
            "compaction destination {} already exists",
            dest.display()
        )));
    }
    // Make sure the parent directory exists. mdbx_env_copy creates the
    // destination subdirectory itself.
    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let c_path = CString::new(dest.as_os_str().as_encoded_bytes()).map_err(|_| {
        StateError::Recovery(format!(
            "compaction dest path contains NUL: {}",
            dest.display()
        ))
    })?;

    let rc = env.raw().with_raw_env_ptr(|env_ptr| unsafe {
        ffi::mdbx_env_copy(env_ptr, c_path.as_ptr(), ffi::MDBX_CP_COMPACT)
    });
    if rc != 0 {
        return Err(StateError::Recovery(format!(
            "mdbx_env_copy returned non-zero: {rc}"
        )));
    }

    info!("compaction complete");
    Ok(())
}
