//! The wait of a starting client for its media driver.
//!
//! A service and the media driver of its node start as separate tasks.
//! After a node returns, the service can start before the driver has
//! created its `CnC` file, or before the driver's conductor runs. The
//! Aeron C client then fails its start after the driver timeout, or it
//! starts against a driver that does not answer yet, and the first add of
//! a publication times out. Each failure ends the process, and each
//! restart costs the service its image pull and start again.
//!
//! So every client start of this crate first waits until the driver of its
//! directory is active: the `CnC` file exists, and the driver heartbeat is
//! younger than the driver timeout. The wait has a bound: the driver wait
//! of the client's driver budget (`DriverBudget::driver_wait`), or the
//! connect timeout of an archive session when that is shorter. Past the
//! bound the start fails as before, so the supervisor still restarts a
//! client whose driver never comes.

use std::ffi::CString;
use std::time::{Duration, Instant};

use tracing::{info, warn};

use crate::error::LogError;

/// The pause between two probes of the driver.
const PROBE_EVERY: Duration = Duration::from_millis(100);
/// The period of the progress line while the wait lasts.
const LOG_EVERY: Duration = Duration::from_secs(5);

/// An Aeron context whose driver a start can wait for.
pub(crate) trait DriverDir {
    /// The `aeron.dir` of the context.
    fn driver_dir(&self) -> &str;
    /// The driver timeout of the context, in ms.
    fn driver_timeout_ms(&self) -> u64;
}

macro_rules! driver_dir_for {
    ($krate:ident) => {
        impl DriverDir for $krate::AeronContext {
            fn driver_dir(&self) -> &str {
                self.get_dir()
            }

            fn driver_timeout_ms(&self) -> u64 {
                self.get_driver_timeout_ms()
            }
        }
    };
}

driver_dir_for!(rusteron_client);
driver_dir_for!(rusteron_archive);

/// One wait for the driver of one directory.
pub(crate) struct DriverWait {
    dir: CString,
    timeout_ms: i64,
    limit: Duration,
}

/// The progress of one wait.
struct Progress {
    started: Instant,
    logged: Instant,
}

impl DriverWait {
    /// The wait for the driver of `ctx`, bounded by `limit`.
    ///
    /// # Errors
    ///
    /// Returns an error if the directory holds a NUL byte, or the driver
    /// timeout does not fit an `i64`.
    pub(crate) fn of<C: DriverDir>(ctx: &C, limit: Duration) -> Result<Self, LogError> {
        let dir = CString::new(ctx.driver_dir()).map_err(|_| {
            LogError::Aeron(format!(
                "aeron.dir contains a NUL byte: {}",
                ctx.driver_dir()
            ))
        })?;
        let timeout_ms = i64::try_from(ctx.driver_timeout_ms()).map_err(|_| {
            LogError::Aeron(format!(
                "driver timeout {} ms does not fit",
                ctx.driver_timeout_ms()
            ))
        })?;
        Ok(Self {
            dir,
            timeout_ms,
            limit,
        })
    }

    /// Wait until the driver is active, and log the wait while it lasts.
    ///
    /// # Errors
    ///
    /// Returns an error when no active driver appears within the bound.
    pub(crate) fn wait(&self) -> Result<(), LogError> {
        let now = Instant::now();
        let mut progress = Progress {
            started: now,
            logged: now,
        };
        std::iter::repeat_with(|| self.probe(&mut progress))
            .find_map(std::convert::identity)
            .expect("an endless probe sequence ends only on an answer")
    }

    /// One probe. `Some` ends the wait: the driver is active, or the bound
    /// passed.
    fn probe(&self, progress: &mut Progress) -> Option<Result<(), LogError>> {
        let waited = progress.started.elapsed();
        if self.active() {
            if waited >= PROBE_EVERY {
                info!(dir = ?self.dir, waited_ms = waited.as_millis(), "media driver is active");
            }
            return Some(Ok(()));
        }
        if waited >= self.limit {
            return Some(Err(LogError::Aeron(format!(
                "no active media driver at {:?} within {:?}",
                self.dir, self.limit
            ))));
        }
        if progress.logged.elapsed() >= LOG_EVERY || waited.is_zero() {
            warn!(
                dir = ?self.dir,
                waited_s = waited.as_secs(),
                limit_s = self.limit.as_secs(),
                "waiting for the media driver to start"
            );
            progress.logged = Instant::now();
        }
        std::thread::sleep(PROBE_EVERY);
        None
    }

    /// Whether a driver uses the directory: its `CnC` file maps with a
    /// version the client reads, and the driver's heartbeat on its command
    /// buffer is younger than the driver timeout.
    fn active(&self) -> bool {
        use rusteron_client::bindings::{
            aeron_cnc_close, aeron_cnc_init, aeron_cnc_t, aeron_cnc_to_driver_heartbeat,
        };
        let mut cnc: *mut aeron_cnc_t = std::ptr::null_mut();
        // SAFETY: `dir` is a valid C string and `cnc` a valid out pointer.
        // A timeout of 0 makes the call return at once. On success the
        // call owns a mapping that `aeron_cnc_close` releases below.
        if unsafe { aeron_cnc_init(&raw mut cnc, self.dir.as_ptr(), 0) } != 0 {
            return false;
        }
        // SAFETY: `cnc` is the mapping that the successful init gave, and
        // nothing else holds it. It is read once, then closed once.
        let heartbeat_ms = unsafe { aeron_cnc_to_driver_heartbeat(cnc) };
        // SAFETY: as above; `cnc` is not used after the close.
        unsafe { aeron_cnc_close(cnc) };
        Self::now_ms().saturating_sub(heartbeat_ms) < self.timeout_ms
    }

    /// The wall clock in ms since the epoch, the clock of the heartbeat.
    fn now_ms() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .ok()
            .and_then(|d| i64::try_from(d.as_millis()).ok())
            .unwrap_or(i64::MAX)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Missing(String);

    impl DriverDir for Missing {
        fn driver_dir(&self) -> &str {
            &self.0
        }

        fn driver_timeout_ms(&self) -> u64 {
            1_000
        }
    }

    #[test]
    fn a_directory_with_no_driver_fails_at_the_bound() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = Missing(dir.path().to_str().unwrap().to_string());
        let wait = DriverWait::of(&ctx, Duration::from_millis(300)).unwrap();
        let started = Instant::now();
        let error = wait.wait().unwrap_err().to_string();
        assert!(error.contains("no active media driver"), "{error}");
        assert!(started.elapsed() >= Duration::from_millis(300));
    }
}
