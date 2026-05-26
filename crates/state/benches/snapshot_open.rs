//! Measure RO snapshot open latency. Target: <100 µs on a quiet host.

use std::time::Duration;

use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use state::StateSnapshot;
use state::env::{Durability, StateEnvBuilder};

fn bench_snapshot_open(c: &mut Criterion) {
    let dir = tempfile::tempdir().unwrap();
    let env = StateEnvBuilder::new(dir.path())
        .durability(Durability::SafeNoSync)
        .open()
        .unwrap();

    let mut group = c.benchmark_group("state_snapshot");
    group.measurement_time(Duration::from_secs(5));
    group.bench_function("open_ro_txn", |b| {
        b.iter(|| {
            let snap = StateSnapshot::open(&env).unwrap();
            black_box(snap);
        });
    });
    group.finish();
}

criterion_group!(benches, bench_snapshot_open);
criterion_main!(benches);
