//! The cost of the `tx_events` records on the publish loop
//! (`docs/specs/2026-10-25-client-events.md`, 4.8): the append of one
//! `sent` record into the frame, and the send of a full frame of 1,024
//! records to an offer that does nothing. The live offer adds one copy of
//! each frame into an aligned buffer, and one command to the Aeron thread.

use std::hint::black_box;

use alloy_primitives::B256;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use kardamom_log::aeron_live::{FrameOffer, TxEventsSink};
use kardamom_types::tx_events::{MAX_RECORDS, TxEventKind};

/// An offer that only reads the frame.
struct NoOffer;

impl FrameOffer for NoOffer {
    fn offer_frame(&self, frame: &[u8]) {
        black_box(frame);
    }
}

fn sent_records(c: &mut Criterion) {
    let mut group = c.benchmark_group("tx_events");
    let hashes: Vec<B256> = (0..MAX_RECORDS as u64)
        .map(|i| B256::left_padding_from(&i.to_be_bytes()))
        .collect();
    let mut sink = TxEventsSink::new(NoOffer, 0, &[TxEventKind::Sent]);
    group.throughput(Throughput::Elements(MAX_RECORDS as u64));
    group.bench_function("sent_full_frame", |b| {
        b.iter(|| {
            for hash in &hashes {
                sink.sent(black_box(hash), 3);
            }
        });
    });
    group.throughput(Throughput::Elements(1));
    group.bench_function("flush_due_empty", |b| b.iter(|| sink.flush_due()));
    group.finish();
}

criterion_group!(benches, sent_records);
criterion_main!(benches);
