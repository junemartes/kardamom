//! The persisted divergence verdict. A proven divergence is a state of the
//! chain, not a property of one process. The validator writes the reason
//! to a file beside its state. A later start finds the file and runs
//! halted, so a restart never absorbs a divergence. An operator clears
//! the file after the investigation.

use std::io;
use std::path::{Path, PathBuf};

/// The file name beside the state directory.
const FILE_NAME: &str = "verdict";
/// The name of the temporary file a write goes through.
const TEMP_NAME: &str = "verdict.tmp";

/// The verdict file of one state directory.
#[derive(Debug, Clone)]
pub struct VerdictFile {
    path: PathBuf,
}

impl VerdictFile {
    /// The verdict file beside the state in `state_dir`.
    #[must_use]
    pub fn beside(state_dir: &Path) -> Self {
        Self {
            path: state_dir.join(FILE_NAME),
        }
    }

    /// The path of the file.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Record `reason` as the standing verdict. The write is atomic: a
    /// temporary file in the same directory, then a rename. A crash
    /// between the two leaves no partial verdict.
    ///
    /// # Errors
    ///
    /// Returns the I/O error of the directory creation, the write, or
    /// the rename.
    pub fn record(&self, reason: &str) -> io::Result<()> {
        let dir = self.path.parent().unwrap_or(Path::new("."));
        std::fs::create_dir_all(dir)?;
        let temp = dir.join(TEMP_NAME);
        std::fs::write(&temp, format!("{reason}\n"))?;
        std::fs::rename(temp, &self.path)
    }

    /// The standing reason, or `None` when no verdict stands.
    ///
    /// # Errors
    ///
    /// Returns the I/O error of a read that fails for a reason other than
    /// a missing file.
    pub fn standing(&self) -> io::Result<Option<String>> {
        match std::fs::read_to_string(&self.path) {
            Ok(reason) => Ok(Some(reason.trim().to_string())),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Remove the verdict. `Ok(false)` when none stood.
    ///
    /// # Errors
    ///
    /// Returns the I/O error of a removal that fails for a reason other
    /// than a missing file.
    pub fn clear(&self) -> io::Result<bool> {
        match std::fs::remove_file(&self.path) {
            Ok(()) => Ok(true),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(e),
        }
    }
}

#[cfg(test)]
#[path = "verdict_tests.rs"]
mod tests;
