//! Criterion throughput bench. Real benches land in Task 19; this stub keeps
//! `cargo build --all-targets` happy until then.

use criterion::{Criterion, criterion_group, criterion_main};

fn placeholder(c: &mut Criterion) {
    c.bench_function("placeholder", |b| b.iter(|| 1u64 + 1));
}

criterion_group!(benches, placeholder);
criterion_main!(benches);
