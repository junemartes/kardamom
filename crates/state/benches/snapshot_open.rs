//! Measure read-only snapshot open latency. Target: under 100
//! microseconds on a quiet host.

use std::time::Duration;

use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use kardamom_state::StateSnapshot;

#[path = "../tests/common/mod.rs"]
mod common;

fn bench_snapshot_open(c: &mut Criterion) {
    let (_dir, env) = common::temp_env();

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
