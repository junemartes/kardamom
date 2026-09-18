//! Per-sequencer throughput on one core.
//!
//! The proxy supplies the sender, so this hot path does no secp256k1 work.
//! The spec target is more than 100k tx/s per core for simple signatures.
//! This bench measures `run_once` loop throughput on one thread.

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use kardamom_types::{BPosition, TxDataLoc, TxEnvelope};

use kardamom_sequencer::config::SequencerConfig;
use kardamom_sequencer::sequencer::Sequencer;
use kardamom_sequencer::testkit::{Rig, one_partition_cfg, signed_envelope, signer};

fn bench_in_order(c: &mut Criterion) {
    let signers: Vec<_> = (1..=64u64).map(signer).collect();
    // The cartesian product of sender and nonce, in the same row-major
    // order the original nested loop walked: `correlation` there was
    // just `i * 16 + n`.
    let batch: Vec<(TxDataLoc, TxEnvelope)> = signers
        .iter()
        .enumerate()
        .flat_map(|(i, s)| (0u64..16).map(move |n| (i, s, n)))
        .map(|(i, s, n)| {
            let correlation = u64::try_from(i).unwrap() * 16 + n;
            let position = BPosition {
                term_id: 0,
                term_offset: i32::try_from(correlation).unwrap() * 64,
            };
            (
                TxDataLoc::new(0, position),
                signed_envelope(s, n, correlation),
            )
        })
        .collect();
    c.bench_function("sequencer_run_once_1024_proxy_sender", |b| {
        b.iter_batched(
            || {
                let mut rig = Rig::default();
                rig.tx_data.queue = batch.clone().into_iter().collect();
                let seq = Sequencer::new(SequencerConfig {
                    max_pending_per_sender: 16,
                    ..one_partition_cfg()
                })
                .unwrap();
                (seq, rig)
            },
            run_to_completion,
            BatchSize::SmallInput,
        );
    });
}

/// Drain `rig` against `seq` until the queued batch is exhausted. This is
/// the measured routine for [`bench_in_order`]'s `iter_batched` call.
fn run_to_completion((mut seq, mut rig): (Sequencer, Rig)) {
    while rig.step(&mut seq).unwrap() {}
}

criterion_group!(benches, bench_in_order);
criterion_main!(benches);
