//! Archive re-replication: restore a wiped Aeron Archive from a peer's mirror.
//!
//! `tx_data` is recorded on **both** ingress replicas (each joins the shard's
//! UDP multicast group and records it), so there are two node-independent mirror
//! copies of every shard's archive. That survives a single node loss — but only
//! once: after one copy is gone there is no path back to two, so the *next* loss
//! is fatal, and on a volume wipe the executor's `resolve_recording` simply
//! hangs. This tool closes that gap by restoring a lost archive from the
//! surviving peer, returning the cluster to full 2-copy redundancy.
//!
//! rusteron-archive does not expose Aeron's network `replicate()`, so
//! re-replication is a **file-level segment mirror**: copy every `.rec` segment
//! plus the `archive.catalog` and `archive-mark.dat` (both ingress archives
//! share identical recording ids, so the catalog transplants cleanly). The copy
//! is format-agnostic — it does not parse the raw Aeron segment framing; a
//! deeper integrity pass is `aeron-archive verify` on the restored node.
//!
//! Operational note: the destination archive daemon **must be stopped** during
//! the copy (it holds the catalog open). The chaos case / runbook stops the
//! `aeron` job, runs this, then restarts it.

use std::path::Path;

use crate::error::BatcherError;

/// Aeron archive files the mirror transplants besides the `.rec` segments.
const CATALOG_FILE: &str = "archive.catalog";
const MARK_FILE: &str = "archive-mark.dat";

/// What a [`mirror_archive`] run copied.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct MirrorReport {
    pub segments_copied: usize,
    pub bytes_copied: u64,
    pub catalog_copied: bool,
    pub mark_copied: bool,
}

/// Mirror the Aeron archive directory `source_dir` into `dest_dir`: every
/// `*.rec` segment plus `archive.catalog` / `archive-mark.dat`. Existing files
/// in `dest_dir` are overwritten. `source_dir` / `dest_dir` are the archive's
/// `dir/` directory (where the `.rec` files live).
///
/// The destination archive daemon MUST be stopped first. Errors if the source
/// holds no `.rec` segments (nothing to replicate — likely a wrong path).
pub fn mirror_archive(source_dir: &Path, dest_dir: &Path) -> Result<MirrorReport, BatcherError> {
    std::fs::create_dir_all(dest_dir)?;
    let mut report = MirrorReport::default();

    for entry in std::fs::read_dir(source_dir)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        let is_rec = path.extension().is_some_and(|x| x == "rec");
        let is_catalog = name_str == CATALOG_FILE;
        let is_mark = name_str == MARK_FILE;
        if !(is_rec || is_catalog || is_mark) {
            continue;
        }
        let bytes = std::fs::copy(&path, dest_dir.join(&name))?;
        if is_rec {
            report.segments_copied += 1;
            report.bytes_copied += bytes;
        }
        report.catalog_copied |= is_catalog;
        report.mark_copied |= is_mark;
    }

    if report.segments_copied == 0 {
        return Err(BatcherError::Reconstruct(format!(
            "no .rec segments in source archive {} — nothing to re-replicate",
            source_dir.display()
        )));
    }
    Ok(report)
}

/// Verify `dest_dir` mirrors `source_dir`: every source `.rec` exists in dest
/// with an identical byte length. Returns the number of segments verified.
/// A fast post-copy gate; `aeron-archive verify` is the deeper (frame-level)
/// check run on the restored node before the daemon rejoins.
pub fn verify_mirror(source_dir: &Path, dest_dir: &Path) -> Result<usize, BatcherError> {
    let mut verified = 0usize;
    for entry in std::fs::read_dir(source_dir)? {
        let entry = entry?;
        let path = entry.path();
        if !path.extension().is_some_and(|x| x == "rec") {
            continue;
        }
        let src_len = entry.metadata()?.len();
        let dest = dest_dir.join(entry.file_name());
        let dest_len = std::fs::metadata(&dest)
            .map_err(|e| {
                BatcherError::Reconstruct(format!(
                    "mirrored segment {} missing: {e}",
                    entry.file_name().to_string_lossy()
                ))
            })?
            .len();
        if src_len != dest_len {
            return Err(BatcherError::Reconstruct(format!(
                "segment {} size mismatch: source {src_len} != dest {dest_len}",
                entry.file_name().to_string_lossy()
            )));
        }
        verified += 1;
    }
    Ok(verified)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(dir: &Path, name: &str, bytes: &[u8]) {
        std::fs::write(dir.join(name), bytes).unwrap();
    }

    #[test]
    fn mirrors_segments_catalog_and_mark() {
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        write(src.path(), "0-0.rec", &[1u8; 4096]);
        write(src.path(), "1-0.rec", &[2u8; 8192]);
        write(src.path(), CATALOG_FILE, &[9u8; 1024]);
        write(src.path(), MARK_FILE, &[7u8; 512]);
        // A non-archive file must be ignored.
        write(src.path(), "notes.txt", b"ignore me");

        let report = mirror_archive(src.path(), dst.path()).unwrap();
        assert_eq!(report.segments_copied, 2);
        assert_eq!(report.bytes_copied, 4096 + 8192);
        assert!(report.catalog_copied);
        assert!(report.mark_copied);

        // Bytes are identical and the stray file was skipped.
        assert_eq!(
            std::fs::read(dst.path().join("0-0.rec")).unwrap(),
            vec![1u8; 4096]
        );
        assert_eq!(
            std::fs::read(dst.path().join(CATALOG_FILE)).unwrap(),
            vec![9u8; 1024]
        );
        assert!(!dst.path().join("notes.txt").exists());

        assert_eq!(verify_mirror(src.path(), dst.path()).unwrap(), 2);
    }

    #[test]
    fn empty_source_is_an_error() {
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        write(src.path(), "archive.catalog", &[0u8; 16]); // catalog but no segments
        let err = mirror_archive(src.path(), dst.path()).unwrap_err();
        assert!(matches!(err, BatcherError::Reconstruct(_)));
    }

    #[test]
    fn verify_detects_truncated_segment() {
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        write(src.path(), "0-0.rec", &[1u8; 4096]);
        mirror_archive(src.path(), dst.path()).unwrap();
        // Corrupt the destination copy (simulate a partial transfer).
        write(dst.path(), "0-0.rec", &[1u8; 100]);
        let err = verify_mirror(src.path(), dst.path()).unwrap_err();
        assert!(matches!(err, BatcherError::Reconstruct(_)));
    }
}
