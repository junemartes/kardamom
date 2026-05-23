//! Standalone fsync sidecar bench. Does not require Aeron binaries — feeds
//! the sidecar a fake position counter and measures per-tick (4 KiB write +
//! fdatasync) latency through io_uring + O_DIRECT.

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use kardamom_log::fsync_sidecar::{FsyncSidecar, PositionSource};

struct FakePos(Arc<AtomicI64>);
impl PositionSource for FakePos {
    fn current(&self) -> i64 {
        self.0.load(Ordering::Acquire)
    }
}

fn o_direct_supported(dir: &std::path::Path) -> bool {
    use std::os::unix::fs::OpenOptionsExt;
    let path = dir.join("probe.bin");
    std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .custom_flags(libc::O_DIRECT)
        .open(&path)
        .is_ok()
}

fn bench(c: &mut Criterion) {
    let tmp = tempfile::tempdir().unwrap();
    if !o_direct_supported(tmp.path()) {
        eprintln!("skipping bench: tempdir does not support O_DIRECT");
        return;
    }
    let source = tmp.path().join("source.bin");
    let mirror = tmp.path().join("mirror.bin");
    // Pre-populate 16 MiB of source data.
    std::fs::write(&source, vec![0u8; 16 * 1024 * 1024]).unwrap();

    let pos = Arc::new(AtomicI64::new(0));
    let mut sidecar =
        FsyncSidecar::open(&source, &mirror, Box::new(FakePos(pos.clone())), 256).unwrap();

    let mut group = c.benchmark_group("fsync_watermark");
    group.throughput(Throughput::Bytes(4096));
    group.measurement_time(Duration::from_secs(10));
    group.bench_function("4KiB_write_plus_fdatasync", |b| {
        let mut off = 0i64;
        b.iter(|| {
            off += 4096;
            pos.store(off, Ordering::Release);
            let _ = sidecar.tick().unwrap();
        });
    });
    group.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
