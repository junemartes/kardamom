//! The error log of a media driver, printed when a test fails.

use std::io::Write;
use std::path::PathBuf;

use rusteron_client::{AeronCError, AeronCnc};

/// The error log of the media driver at one `aeron.dir`, read from its
/// `cnc.dat`. A fixture holds one for each driver. When the test panics,
/// the drop prints the log: a driver reports a failed channel bind only
/// there, and the test then only times out. The log stays in the file
/// after the driver stops.
#[derive(Debug)]
pub struct DriverErrorLog {
    aeron_dir: PathBuf,
}

impl DriverErrorLog {
    /// The error log of the driver at `aeron_dir`. The driver can start
    /// later: the read happens at each call.
    #[must_use]
    pub fn new(aeron_dir: impl Into<PathBuf>) -> Self {
        Self {
            aeron_dir: aeron_dir.into(),
        }
    }

    /// Each distinct error of the log, in log order, after the count of
    /// its observations.
    ///
    /// # Errors
    ///
    /// Returns an error if the `cnc.dat` of the driver does not load.
    pub fn errors(&self) -> Result<Vec<String>, AeronCError> {
        let cnc = AeronCnc::new_on_heap(&self.aeron_dir.to_string_lossy())?;
        let mut errors = Vec::new();
        cnc.error_log_read_once(
            |count, _, _, error: &str| errors.push(format!("{count} x {error}")),
            0,
        );
        Ok(errors)
    }

    /// The text that the drop prints in a panic: a heading line, then
    /// each distinct error, or the reason that the log does not load.
    #[must_use]
    pub fn report(&self) -> String {
        let dir = self.aeron_dir.display();
        match self.errors() {
            Ok(errors) => format!(
                "==> driver error log of {dir}: {} distinct errors\n{}",
                errors.len(),
                errors.join("\n")
            ),
            Err(error) => {
                format!("==> driver error log of {dir}: the cnc.dat does not load: {error}")
            }
        }
    }
}

impl Drop for DriverErrorLog {
    fn drop(&mut self) {
        if std::thread::panicking() {
            // A failed write is ignored: a panic in this unwind aborts the
            // process.
            let _ = writeln!(std::io::stderr(), "{}", self.report());
        }
    }
}
