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

    /// A partial projection write or scan requires a full rebuild after restart.
    pub(crate) fn rebuild_pending(&self) -> Result<bool> {
        self.path
            .with_extension("rebuild")
            .try_exists()
            .context("read the mirror rebuild marker")
    }

    /// Persist the recovery obligation before changing the projection.
    pub(crate) fn begin_rebuild(&self) -> Result<()> {
        let dir = self.path.parent().context("head file has no parent")?;
        std::fs::create_dir_all(dir).context("create the mirror dir")?;
        std::fs::File::create(self.path.with_extension("rebuild"))
            .context("create the mirror rebuild marker")?
            .sync_all()
            .context("sync the mirror rebuild marker")?;
        std::fs::File::open(dir)?.sync_all()?;
        Ok(())
    }

    /// Only a completed checkpoint scan discharges the recovery obligation.
    pub(crate) fn finish_rebuild(&self) -> Result<()> {
        std::fs::remove_file(self.path.with_extension("rebuild"))
            .context("remove the mirror rebuild marker")?;
        let dir = self.path.parent().context("head file has no parent")?;
        std::fs::File::open(dir)?.sync_all()?;
        Ok(())
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

    #[test]
    fn an_unfinished_rebuild_survives_reopen_and_later_head_writes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("head");
        let head = HeadFile::new(path.clone());
        head.write(10).unwrap();
        head.begin_rebuild().unwrap();
        assert!(
            std::fs::read(dir.path().join("head.rebuild"))
                .unwrap()
                .is_empty()
        );
        let restarted = HeadFile::new(path);
        assert!(restarted.rebuild_pending().unwrap());
        assert_eq!(restarted.read(), Some(10));
        restarted.write(30).unwrap();
        assert!(restarted.rebuild_pending().unwrap());
        restarted.finish_rebuild().unwrap();
        assert!(!restarted.rebuild_pending().unwrap());
        assert_eq!(restarted.read(), Some(30));
    }

    #[test]
    fn an_unwritable_marker_refuses_the_recovery_transition() {
        let dir = tempfile::tempdir().unwrap();
        let parent = dir.path().join("file");
        std::fs::write(&parent, b"not a directory").unwrap();
        assert!(HeadFile::new(parent.join("head")).begin_rebuild().is_err());
    }
}
