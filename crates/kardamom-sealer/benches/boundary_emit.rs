//! Per-tick overhead microbench for the boundary emitter.
//!
//! Drives [`kardamom_sealer::emitter::BoundaryEmitter::run_one_tick`] against
//! the in-memory FakeBus adapter. Measures the sealer's CPU work only — not
//! Aeron transit. Target: sub-microsecond per tick on commodity hardware.

use criterion::{Criterion, criterion_group, criterion_main};
use kardamom_log::testing::FakeBus;
use kardamom_sealer::clock::MockClock;
use kardamom_sealer::emitter::BoundaryEmitter;
use kardamom_sealer::emitter::fakes::FakeBoundaryPublisher;

fn bench_emit(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    c.bench_function("boundary_emit_one_tick", |b| {
        // Build a fresh emitter per measurement batch. iter_batched avoids
        // both the FnMut closure-captures-async-state lifetime issue and the
        // RefCell-held-across-await clippy lint.
        b.to_async(&rt).iter_batched(
            || {
                let bus = FakeBus::new();
                let pubh = FakeBoundaryPublisher::new(bus, "ch", 2);
                let clock = MockClock::new(1_000);
                (BoundaryEmitter::new(pubh, clock, 1, 250, 1),)
            },
            |(mut emitter,)| async move {
                emitter.run_one_tick().await.unwrap();
            },
            criterion::BatchSize::SmallInput,
        );
    });
}

criterion_group!(benches, bench_emit);
criterion_main!(benches);
