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
//! plus the `archive.catalog` (both ingress archives share identical recording
//! ids, so the catalog transplants cleanly). The copy is format-agnostic — it
//! does not parse the raw Aeron segment framing; a deeper integrity pass is
//! `aeron-archive verify` on the restored node.
//!
//! `archive-mark.dat` is **never** copied: a live source daemon heartbeats its
//! mark file, so a transplanted copy looks "active" to the destination's
//! restarting Archive, which then crash-loops on `active Mark file detected`
//! until the copied heartbeat ages out (observed blowing the chaos restart SLO).
//! The destination daemon recreates its own mark on start; the mark carries no
//! recording data.
//!
//! Operational note: the destination archive daemon **must be stopped** during
//! the copy (it holds the catalog open). The chaos case / runbook stops the
//! `aeron` job, runs this, then restarts it.

use std::path::Path;

use crate::error::BatcherError;

/// The one non-`.rec` file the mirror transplants. The mark file
/// (`archive-mark.dat`) is deliberately NOT copied — see the module doc.
const CATALOG_FILE: &str = "archive.catalog";

/// What a [`mirror_archive`] run copied.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct MirrorReport {
    pub segments_copied: usize,
    pub bytes_copied: u64,
    pub catalog_copied: bool,
}

/// Mirror the Aeron archive directory `source_dir` into `dest_dir`: every
/// `*.rec` segment plus `archive.catalog` (never the mark file). Existing files
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
        if !(is_rec || is_catalog) {
            continue;
        }
        let bytes = std::fs::copy(&path, dest_dir.join(&name))?;
        if is_rec {
            report.segments_copied += 1;
            report.bytes_copied += bytes;
        }
        report.catalog_copied |= is_catalog;
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
/// with **identical content** (chunked byte compare — a length-preserving
/// corruption is exactly the case a size check cannot see). Returns the number
/// of segments verified; a divergence is `BatcherError::Corruption` naming
/// every differing segment. `aeron-archive verify` (CRC-armed) remains the
/// arbiter of WHICH side is corrupt — mirror inequality alone only proves that
/// one of them is.
pub fn verify_mirror(source_dir: &Path, dest_dir: &Path) -> Result<usize, BatcherError> {
    let diverged = diff_mirror(source_dir, dest_dir)?;
    if !diverged.is_empty() {
        return Err(BatcherError::Corruption(format!(
            "segments diverge from mirror: {}",
            diverged.join(", ")
        )));
    }
    let mut verified = 0usize;
    for entry in std::fs::read_dir(source_dir)? {
        if entry?.path().extension().is_some_and(|x| x == "rec") {
            verified += 1;
        }
    }
    Ok(verified)
}

/// Compare every source `.rec` against its mirror copy in `dest_dir` and
/// return the names of segments that are missing or whose bytes differ. The
/// heal path copies exactly this set (instead of the whole archive).
pub fn diff_mirror(source_dir: &Path, dest_dir: &Path) -> Result<Vec<String>, BatcherError> {
    let mut diverged = Vec::new();
    for entry in std::fs::read_dir(source_dir)? {
        let entry = entry?;
        let path = entry.path();
        if !path.extension().is_some_and(|x| x == "rec") {
            continue;
        }
        let name = entry.file_name().to_string_lossy().into_owned();
        let dest = dest_dir.join(entry.file_name());
        if !dest.is_file() || files_differ(&path, &dest)? {
            diverged.push(name);
        }
    }
    diverged.sort();
    Ok(diverged)
}

/// Chunked byte compare — segments can be large, so never slurp whole files.
fn files_differ(a: &Path, b: &Path) -> Result<bool, BatcherError> {
    use std::io::Read;
    if std::fs::metadata(a)?.len() != std::fs::metadata(b)?.len() {
        return Ok(true);
    }
    let mut fa = std::io::BufReader::new(std::fs::File::open(a)?);
    let mut fb = std::io::BufReader::new(std::fs::File::open(b)?);
    let mut ba = vec![0u8; 64 * 1024];
    let mut bb = vec![0u8; 64 * 1024];
    loop {
        let n = fa.read(&mut ba)?;
        if n == 0 {
            return Ok(false);
        }
        fb.read_exact(&mut bb[..n])?;
        if ba[..n] != bb[..n] {
            return Ok(true);
        }
    }
}

/// What a [`heal_from_mirror`] run repaired.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct HealReport {
    pub segments_healed: usize,
    pub bytes_copied: u64,
}

/// Targeted repair: copy ONLY the named `.rec` segments from the surviving
/// mirror into `dest_dir`, leaving everything else untouched (a whole-archive
/// re-copy is `mirror_archive`). Names must be bare segment file names — the
/// output of [`diff_mirror`] or of a CRC-armed `ArchiveTool verify`.
///
/// Same operational constraint as the full mirror: the destination archive
/// daemon must be stopped during the copy.
pub fn heal_from_mirror(
    source_dir: &Path,
    dest_dir: &Path,
    segments: &[String],
) -> Result<HealReport, BatcherError> {
    if segments.is_empty() {
        return Err(BatcherError::Reconstruct(
            "no segments named — nothing to heal".into(),
        ));
    }
    let mut report = HealReport::default();
    for name in segments {
        // Bare `.rec` file names only — reject anything path-like.
        if name.contains('/') || name.contains('\\') || !name.ends_with(".rec") {
            return Err(BatcherError::Reconstruct(format!(
                "invalid segment name {name:?} — expected a bare *.rec file name"
            )));
        }
        let src = source_dir.join(name);
        if !src.is_file() {
            return Err(BatcherError::Reconstruct(format!(
                "segment {name} missing from source mirror {}",
                source_dir.display()
            )));
        }
        report.bytes_copied += std::fs::copy(&src, dest_dir.join(name))?;
        report.segments_healed += 1;
    }
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(dir: &Path, name: &str, bytes: &[u8]) {
        std::fs::write(dir.join(name), bytes).unwrap();
    }

    #[test]
    fn mirrors_segments_and_catalog_but_never_the_mark() {
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        write(src.path(), "0-0.rec", &[1u8; 4096]);
        write(src.path(), "1-0.rec", &[2u8; 8192]);
        write(src.path(), CATALOG_FILE, &[9u8; 1024]);
        // A live source's mark file must NOT be transplanted (the destination
        // daemon would see it as active and crash-loop) — and non-archive
        // files must be ignored.
        write(src.path(), "archive-mark.dat", &[7u8; 512]);
        write(src.path(), "notes.txt", b"ignore me");

        let report = mirror_archive(src.path(), dst.path()).unwrap();
        assert_eq!(report.segments_copied, 2);
        assert_eq!(report.bytes_copied, 4096 + 8192);
        assert!(report.catalog_copied);
        assert!(!dst.path().join("archive-mark.dat").exists());

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
        assert!(matches!(err, BatcherError::Corruption(_)));
    }

    #[test]
    fn verify_detects_length_preserving_byte_flip() {
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        write(src.path(), "0-0.rec", &[1u8; 4096]);
        mirror_archive(src.path(), dst.path()).unwrap();
        // Flip one byte mid-file, length unchanged — invisible to a size
        // check, which is exactly the corruption this verify must catch.
        let mut corrupt = vec![1u8; 4096];
        corrupt[2048] ^= 0xFF;
        write(dst.path(), "0-0.rec", &corrupt);
        let err = verify_mirror(src.path(), dst.path()).unwrap_err();
        assert!(matches!(err, BatcherError::Corruption(_)));
    }

    #[test]
    fn diff_names_exactly_the_diverged_segments() {
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        write(src.path(), "0-0.rec", &[1u8; 1024]);
        write(src.path(), "1-0.rec", &[2u8; 1024]);
        write(src.path(), "2-0.rec", &[3u8; 1024]);
        mirror_archive(src.path(), dst.path()).unwrap();
        let mut corrupt = vec![2u8; 1024];
        corrupt[7] ^= 0x01;
        write(dst.path(), "1-0.rec", &corrupt);
        std::fs::remove_file(dst.path().join("2-0.rec")).unwrap();

        let diverged = diff_mirror(src.path(), dst.path()).unwrap();
        assert_eq!(diverged, vec!["1-0.rec".to_string(), "2-0.rec".to_string()]);
    }

    #[test]
    fn heal_copies_only_named_segments() {
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        write(src.path(), "0-0.rec", &[1u8; 1024]);
        write(src.path(), "1-0.rec", &[2u8; 1024]);
        mirror_archive(src.path(), dst.path()).unwrap();
        let mut corrupt = vec![2u8; 1024];
        corrupt[100] ^= 0xFF;
        write(dst.path(), "1-0.rec", &corrupt);
        // Also locally modify the healthy one to prove heal doesn't touch it.
        write(dst.path(), "0-0.rec", &[9u8; 1024]);

        let report = heal_from_mirror(src.path(), dst.path(), &["1-0.rec".to_string()]).unwrap();
        assert_eq!(report.segments_healed, 1);
        assert_eq!(
            std::fs::read(dst.path().join("1-0.rec")).unwrap(),
            vec![2u8; 1024]
        );
        assert_eq!(
            std::fs::read(dst.path().join("0-0.rec")).unwrap(),
            vec![9u8; 1024]
        );
    }

    #[test]
    fn heal_rejects_path_like_and_missing_names() {
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        write(src.path(), "0-0.rec", &[1u8; 64]);
        for bad in ["../evil.rec", "sub/0-0.rec", "0-0.dat", "missing.rec"] {
            let err = heal_from_mirror(src.path(), dst.path(), &[bad.to_string()]).unwrap_err();
            assert!(matches!(err, BatcherError::Reconstruct(_)), "{bad}");
        }
    }
}
