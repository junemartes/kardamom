//! Measure end-to-end pack throughput: build, encode, compress, and pack,
//! across blob counts.

use std::hint::black_box;
use std::time::Duration;

use alloy_primitives::{Address, B256};
use bytes::Bytes;
use criterion::{Criterion, criterion_group, criterion_main};
use kardamom_batcher::batch::{ClosedBlock, RecordedTx};
use kardamom_batcher::batcher::{BatcherConfig, pack_blocks};
use kardamom_types::{BPosition, TxEnvelope};

fn make_block(n_txs: usize, raw_len: usize) -> ClosedBlock {
    let txs = (0..n_txs as u64)
        .map(|i| RecordedTx {
            position: BPosition {
                term_id: 0,
                term_offset: (i * 64) as i32,
            },
            envelope: TxEnvelope {
                correlation_id: i,
                raw_tx: Bytes::from(vec![0xAB; raw_len]),
                sender: Address::repeat_byte((i & 0xFF) as u8),
                tx_hash: B256::repeat_byte(((i ^ 0x55) & 0xFF) as u8),
            },
        })
        .collect();
    ClosedBlock {
        block_number: 1,
        l2_timestamp: 1_700_000_000,
        end_tx_idx: BPosition {
            term_id: 0,
            term_offset: 0,
        },
        txs,
    }
}

fn bench_pack(c: &mut Criterion) {
    let mut group = c.benchmark_group("batcher_pack");
    group.measurement_time(Duration::from_secs(5));
    group.sample_size(20);

    // About 500 KiB of raw txs. This fits in 6 blobs uncompressed (6 times
    // 126_976 bytes is about a 745 KiB ceiling, minus framing overhead).
    let block_600k = vec![make_block(2_400, 200)];
    group.bench_function("pack_600k_compressed", |b| {
        let cfg = BatcherConfig {
            compress: true,
            ..Default::default()
        };
        b.iter(|| {
            let out = pack_blocks(&cfg, black_box(&block_600k)).unwrap();
            black_box(out.blobs.len());
        });
    });

    group.bench_function("pack_600k_uncompressed", |b| {
        let cfg = BatcherConfig {
            compress: false,
            ..Default::default()
        };
        b.iter(|| {
            let out = pack_blocks(&cfg, black_box(&block_600k)).unwrap();
            black_box(out.blobs.len());
        });
    });
    group.finish();
}

criterion_group!(benches, bench_pack);
criterion_main!(benches);
