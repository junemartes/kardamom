//! Archive re-replication: restore a wiped Aeron Archive from a peer's mirror.
//!
//! `tx_data` is recorded on both ingress replicas. Each one joins the shard's
//! UDP multicast group and records it. So there are two node-independent
//! mirror copies of every shard's archive. That survives one node loss, but
//! only once: once one copy is gone, there is no path back to two, so the
//! next loss is fatal. On a volume wipe, the executor's `resolve_recording`
//! simply hangs. This tool closes that gap. It restores a lost archive from
//! the surviving peer, and returns the cluster to full 2-copy redundancy.
//!
//! rusteron-archive does not expose Aeron's network `replicate()`. So
//! re-replication is a file-level segment mirror: it copies every `.rec`
//! segment plus the `archive.catalog`. Both ingress archives share identical
//! recording ids, so the catalog transplants cleanly. The copy is
//! format-agnostic: it does not parse the raw Aeron segment framing. A
//! deeper integrity pass is `aeron-archive verify` on the restored node.
//!
//! `archive-mark.dat` is never copied. A live source daemon heartbeats its
//! mark file. A transplanted copy would look "active" to the destination's
//! restarting Archive, which then crash-loops on `active Mark file
//! detected` until the copied heartbeat ages out. The destination daemon
//! recreates its own mark on start; the mark carries no recording data.
//!
//! Operational note: the destination archive daemon must be stopped during
//! the copy, because it holds the catalog open. The chaos case, or the
//! runbook, stops the `aeron` job, runs this tool, then restarts it.

use std::path::Path;

use crate::error::BatcherError;

/// The one non-`.rec` file the mirror transplants. The mark file
/// (`archive-mark.dat`) is deliberately not copied. See the module doc.
const CATALOG_FILE: &str = "archive.catalog";

/// What a [`mirror_archive`] run copied.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct MirrorReport {
    pub segments_copied: usize,
    pub bytes_copied: u64,
    pub catalog_copied: bool,
}

/// Mirror the Aeron archive directory `source_dir` into `dest_dir`. Copy
/// the `archive.catalog` first (using a stable read), then every `*.rec`
/// segment (never the mark file). This overwrites existing files in
/// `dest_dir`. `source_dir` and `dest_dir` are the archive's `dir/`
/// directory, where the `.rec` files live.
///
/// The catalog is the file most prone to tearing. A live source daemon
/// rewrites catalog entries on recording lifecycle events (start, stop,
/// extend). The very incident that motivates a restore, a peer's
/// publishers dying, makes the mirror stop those recordings and rewrite
/// exactly those entries seconds later. A plain copy that races such a
/// write captures a torn entry whose stored checksum no longer matches its
/// descriptor, which fails a CRC-armed `ArchiveTool verify`. The stable
/// read (two consecutive identical snapshots) guarantees a consistent
/// catalog image. Copying it before the segments guarantees the segment
/// data covers every position the catalog references.
///
/// The destination archive daemon must be stopped first. This returns an
/// error if the source holds no `.rec` segments. That likely means the path
/// is wrong.
pub fn mirror_archive(source_dir: &Path, dest_dir: &Path) -> Result<MirrorReport, BatcherError> {
    std::fs::create_dir_all(dest_dir)?;
    let mut report = MirrorReport::default();

    let catalog_src = source_dir.join(CATALOG_FILE);
    if catalog_src.is_file() {
        let catalog = read_stable(&catalog_src, STABLE_READ_ATTEMPTS)?;
        std::fs::write(dest_dir.join(CATALOG_FILE), catalog)?;
        report.catalog_copied = true;
    }

    for entry in std::fs::read_dir(source_dir)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() || !path.extension().is_some_and(|x| x == "rec") {
            continue;
        }
        let bytes = std::fs::copy(&path, dest_dir.join(entry.file_name()))?;
        report.segments_copied += 1;
        report.bytes_copied += bytes;
    }

    if report.segments_copied == 0 {
        return Err(BatcherError::Reconstruct(format!(
            "no .rec segments in source archive {} — nothing to re-replicate",
            source_dir.display()
        )));
    }
    Ok(report)
}

/// The number of attempts before giving up on a stable catalog snapshot.
/// Catalog writes come in event-driven bursts (recording start or stop),
/// not a steady stream. Two identical consecutive reads normally happen on
/// the first try.
const STABLE_READ_ATTEMPTS: usize = 10;

/// Read `path` until two consecutive snapshots are byte-identical. Returns
/// the stable bytes. Returns a `Corruption` error if the file never
/// stabilizes: the caller is copying from something busier than a catalog
/// should ever be.
fn read_stable(path: &Path, attempts: usize) -> Result<Vec<u8>, BatcherError> {
    read_stable_with(attempts, || std::fs::read(path).map_err(BatcherError::from)).map_err(|e| {
        match e {
            BatcherError::Corruption(_) => BatcherError::Corruption(format!(
                "{} kept changing across {attempts} reads — is the source daemon quiesced?",
                path.display()
            )),
            other => other,
        }
    })
}

/// Core of [`read_stable`], parameterized over the reader for testability.
fn read_stable_with<F>(attempts: usize, mut read: F) -> Result<Vec<u8>, BatcherError>
where
    F: FnMut() -> Result<Vec<u8>, BatcherError>,
{
    let mut prev = read()?;
    for _ in 1..attempts.max(2) {
        let next = read()?;
        if next == prev {
            return Ok(next);
        }
        prev = next;
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    Err(BatcherError::Corruption("unstable read".into()))
}

/// Verify that `dest_dir` mirrors `source_dir`: every source `.rec` must
/// exist in `dest_dir` with identical content. This uses a chunked byte
/// compare, because a length-preserving corruption is exactly the case a
/// size check cannot see. Returns the number of segments verified. A
/// divergence returns `BatcherError::Corruption`, naming every differing
/// segment. `aeron-archive verify` (CRC-armed) still decides which side is
/// corrupt; a mirror mismatch only proves that one side is.
pub fn verify_mirror(source_dir: &Path, dest_dir: &Path) -> Result<usize, BatcherError> {
    let diff = diff_mirror(source_dir, dest_dir)?;
    if !diff.is_clean() {
        return Err(BatcherError::Corruption(format!(
            "segments diverge from mirror: {}",
            diff.diverged
                .iter()
                .cloned()
                .chain(diff.dest_only.iter().map(|n| format!("{n} (dest-only)")))
                .collect::<Vec<_>>()
                .join(", ")
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

/// [`diff_mirror`] compares every source `.rec` against its mirror copy in
/// `dest_dir`. It returns the names of segments that are missing or whose
/// bytes differ. The heal path copies exactly this set, instead of the
/// whole archive.
///
/// What [`diff_mirror`] found. `diverged` are segments the mirror can vouch
/// for, and [`heal_from_mirror`] can repair them. `dest_only` are segments
/// present only on the destination. Recording ids are per-archive counters,
/// so a daemon restart, or a post-restore session, opens ids the mirror
/// never has. Those segments cannot be healed from this mirror, and must be
/// surfaced, not silently skipped. A diff that iterates only the source
/// would claim "no divergence" for bytes the mirror cannot vouch for.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct MirrorDiff {
    pub diverged: Vec<String>,
    pub dest_only: Vec<String>,
}

impl MirrorDiff {
    pub fn is_clean(&self) -> bool {
        self.diverged.is_empty() && self.dest_only.is_empty()
    }
}

pub fn diff_mirror(source_dir: &Path, dest_dir: &Path) -> Result<MirrorDiff, BatcherError> {
    let mut diff = MirrorDiff::default();
    let mut source_names = std::collections::HashSet::new();
    for entry in std::fs::read_dir(source_dir)? {
        let entry = entry?;
        let path = entry.path();
        if !path.extension().is_some_and(|x| x == "rec") {
            continue;
        }
        let name = entry.file_name().to_string_lossy().into_owned();
        let dest = dest_dir.join(entry.file_name());
        if !dest.is_file() || files_differ(&path, &dest)? {
            diff.diverged.push(name.clone());
        }
        source_names.insert(name);
    }
    for entry in std::fs::read_dir(dest_dir)? {
        let entry = entry?;
        if !entry.path().extension().is_some_and(|x| x == "rec") {
            continue;
        }
        let name = entry.file_name().to_string_lossy().into_owned();
        if !source_names.contains(&name) {
            diff.dest_only.push(name);
        }
    }
    diff.diverged.sort();
    diff.dest_only.sort();
    Ok(diff)
}

/// A chunked byte compare. Segments can be large, so this never reads a
/// whole file into memory.
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

/// A targeted repair. Copy only the named `.rec` segments from the
/// surviving mirror into `dest_dir`, leaving everything else untouched. For
/// a whole-archive re-copy, use `mirror_archive`. Names must be bare
/// segment file names, such as the output of [`diff_mirror`] or of a
/// CRC-armed `ArchiveTool verify`.
///
/// This has the same operational constraint as the full mirror: the
/// destination archive daemon must be stopped during the copy.
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
        // Accept only bare `.rec` file names. Reject anything path-like.
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
        // A live source's mark file must not be transplanted. The
        // destination daemon would see it as active and crash-loop. Also,
        // non-archive files must be ignored.
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
    fn stable_read_returns_first_repeated_snapshot() {
        let mut reads = vec![b"v1".to_vec(), b"v2".to_vec(), b"v2".to_vec()].into_iter();
        let got = read_stable_with(10, || Ok(reads.next().unwrap())).unwrap();
        assert_eq!(got, b"v2");
    }

    #[test]
    fn stable_read_gives_up_on_a_churning_file() {
        let mut n = 0u8;
        let err = read_stable_with(4, || {
            n += 1;
            Ok(vec![n])
        })
        .unwrap_err();
        assert!(matches!(err, BatcherError::Corruption(_)));
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
        // Flip one byte in the middle of the file. The length stays the
        // same, so a size check cannot see it. This is the corruption this
        // verify must catch.
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

        let diff = diff_mirror(src.path(), dst.path()).unwrap();
        assert_eq!(
            diff.diverged,
            vec!["1-0.rec".to_string(), "2-0.rec".to_string()]
        );
        assert!(diff.dest_only.is_empty());
    }

    /// Recording ids are per-archive counters, so a restarted or
    /// post-restore destination owns segment names the mirror never has. A
    /// diff that iterates only the source would silently skip them. They
    /// must surface as dest-only: this cannot be healed from this mirror,
    /// but it is still a divergence.
    #[test]
    fn diff_reports_dest_only_segments() {
        let src = tempfile::tempdir().unwrap();
        let dst = tempfile::tempdir().unwrap();
        write(src.path(), "0-0.rec", &[1u8; 1024]);
        mirror_archive(src.path(), dst.path()).unwrap();
        write(dst.path(), "11-0.rec", &[7u8; 1024]);

        let diff = diff_mirror(src.path(), dst.path()).unwrap();
        assert!(diff.diverged.is_empty());
        assert_eq!(diff.dest_only, vec!["11-0.rec".to_string()]);
        assert!(!diff.is_clean());
        // And the mirror-equality gate refuses to call this archive clean.
        let err = verify_mirror(src.path(), dst.path())
            .unwrap_err()
            .to_string();
        assert!(err.contains("11-0.rec (dest-only)"), "got: {err}");
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
        // Also locally modify the healthy one, to prove heal does not touch it.
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
