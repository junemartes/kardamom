//! Unit-level test for the FsyncSidecar's write+fdatasync loop, decoupled from
//! Aeron. We feed it a fake source (a temp file we append bytes to) and a fake
//! position-counter (an atomic), and assert that after each "burst" the mirror
//! file contents match the source up to the watermark.
//!
//! NOTE: O_DIRECT requires a backing filesystem that supports it. tmpfs
//! returns EINVAL on O_DIRECT opens. The test below first probes whether
//! O_DIRECT can be opened on the system tempdir; if not, it skips with a
//! diagnostic message so default `cargo test` runs on hosts whose `/tmp` is
//! tmpfs don't fail spuriously.

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use kardamom_log::fsync_sidecar::{FsyncSidecar, PositionSource};
use kardamom_types::BPosition;

struct FakePosition(Arc<AtomicI64>);
impl PositionSource for FakePosition {
    fn current(&self) -> i64 {
        self.0.load(Ordering::Acquire)
    }
}

/// Returns `true` if the system tempdir supports `O_DIRECT` opens.
fn o_direct_supported() -> bool {
    use std::os::unix::fs::OpenOptionsExt;
    let dir = match tempfile::tempdir() {
        Ok(d) => d,
        Err(_) => return false,
    };
    let path = dir.path().join("probe.bin");
    std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .custom_flags(libc::O_DIRECT)
        .open(&path)
        .is_ok()
}

#[test]
fn sidecar_mirrors_and_fsyncs_appended_bytes() {
    if !o_direct_supported() {
        eprintln!("skipping: tempdir filesystem does not support O_DIRECT");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source.bin");
    let mirror = dir.path().join("mirror.bin");

    // Pre-write 4096 bytes to source (chunk size must be a multiple of 4096
    // to satisfy O_DIRECT length alignment).
    let buf = vec![0xAB; 4096];
    std::fs::write(&source, &buf).unwrap();

    let pos = Arc::new(AtomicI64::new(4096));
    let mut sidecar =
        FsyncSidecar::open(&source, &mirror, Box::new(FakePosition(pos)), 256).unwrap();

    let wm = sidecar.tick().unwrap();
    assert_eq!(
        wm,
        Some(BPosition {
            term_id: 0,
            term_offset: 4096
        })
    );

    let on_disk = std::fs::read(&mirror).unwrap();
    assert_eq!(on_disk.len(), 4096);
    assert_eq!(&on_disk[..], &buf[..]);
}

#[test]
fn sidecar_returns_none_when_no_new_bytes() {
    if !o_direct_supported() {
        eprintln!("skipping: tempdir filesystem does not support O_DIRECT");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source.bin");
    let mirror = dir.path().join("mirror.bin");
    std::fs::write(&source, b"").unwrap();

    let pos = Arc::new(AtomicI64::new(0));
    let mut sidecar =
        FsyncSidecar::open(&source, &mirror, Box::new(FakePosition(pos)), 256).unwrap();

    assert_eq!(sidecar.tick().unwrap(), None);
}
