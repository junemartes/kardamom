//! The local head file: the highest batch end position this mirror has
//! applied, persisted so a restart can tell a Redis regression from a
//! resume. See the failover rule in the spec.

use std::path::PathBuf;

use anyhow::{Context, Result};

/// The head file. Written through a temporary file and a rename, so a
/// crash mid-write leaves the old value.
pub(crate) struct HeadFile {
    path: PathBuf,
}

impl HeadFile {
    pub(crate) fn new(path: PathBuf) -> Self {
        Self { path }
    }

    /// The persisted head, as a canonical index. `None` when no head was
    /// ever written or the file does not parse.
    pub(crate) fn read(&self) -> Option<u64> {
        std::fs::read_to_string(&self.path)
            .ok()
            .and_then(|s| s.trim().parse().ok())
    }

    /// Persist `head`.
    pub(crate) fn write(&self, head: u64) -> Result<()> {
        let dir = self.path.parent().context("head file has no parent")?;
        std::fs::create_dir_all(dir).context("create the mirror dir")?;
        let tmp = self.path.with_extension("tmp");
        std::fs::write(&tmp, format!("{head}\n")).context("write the head file")?;
        std::fs::rename(&tmp, &self.path).context("rename the head file")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_and_reads_none_when_absent() {
        let dir = std::env::temp_dir().join(format!("kardamom-head-{}", std::process::id()));
        let file = HeadFile::new(dir.join("head"));
        assert_eq!(file.read(), None);
        file.write(42).unwrap();
        assert_eq!(file.read(), Some(42));
        file.write(43).unwrap();
        assert_eq!(file.read(), Some(43));
        std::fs::remove_dir_all(dir).unwrap();
    }
}
