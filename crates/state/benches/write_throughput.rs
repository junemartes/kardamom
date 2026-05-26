//! Measure block-delta write throughput at the spec's target size.
//!
//! Target: 25 MB per block at 4 Hz = 100 MB/s sustained. We do not pace at
//! 4 Hz here — we measure raw apply latency per block.

use std::time::Duration;

use alloy_primitives::{Address, B256, U256};
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use state::env::{Durability, StateEnvBuilder};
use state::{StateWriter, WriteBatch};
use types::{AccountChange, BPosition, BlockBoundary, BlockDelta, Receipt};

fn big_batch(block: u64) -> WriteBatch {
    // ~25 MB target: 100 B per account × 250k accounts. Real workload mixes
    // accounts + storage; we approximate with accounts only for the bench.
    let n = 250_000usize;
    let accounts: Vec<AccountChange> = (0..n)
        .map(|i| {
            let mut bytes = [0u8; 20];
            bytes[..8].copy_from_slice(&(i as u64).to_be_bytes());
            AccountChange {
                address: Address::from(bytes),
                nonce: block,
                balance: U256::from(i as u64),
                code_hash: B256::ZERO,
            }
        })
        .collect();
    let pos = BPosition {
        term_id: 0,
        term_offset: (block * 1024) as i32,
    };
    let mut hash_bytes = [0u8; 32];
    hash_bytes[24..].copy_from_slice(&block.to_be_bytes());
    let tx_hash = B256::from(hash_bytes);
    WriteBatch::new(
        BlockBoundary {
            block_number: block,
            end_tx_idx: pos,
            l2_timestamp: 1_700_000_000 + block,
        },
        BlockDelta {
            block_number: block,
            accounts,
            storage: vec![],
            code: vec![],
            receipts: vec![Receipt {
                tx_idx: pos,
                tx_hash,
                status: true,
                gas_used: 21_000,
                logs: vec![],
                write_set_hash: B256::ZERO,
            }],
        },
    )
}

fn bench_apply(c: &mut Criterion) {
    let mut group = c.benchmark_group("state_writer");
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(10);

    group.bench_function("apply_25mb_block", |b| {
        b.iter_batched(
            || {
                let dir = tempfile::tempdir().unwrap();
                let env = StateEnvBuilder::new(dir.path())
                    .durability(Durability::SafeNoSync)
                    .open()
                    .unwrap();
                let writer = StateWriter::spawn(env).unwrap();
                // Drain the initial snapshot.
                writer.snapshot_rx.recv().unwrap();
                (dir, writer)
            },
            |(_dir, writer)| {
                writer.delta_tx.send(big_batch(1)).unwrap();
                writer.snapshot_rx.recv().unwrap();
                writer.shutdown().unwrap();
            },
            BatchSize::LargeInput,
        );
    });

    group.finish();
}

criterion_group!(benches, bench_apply);
criterion_main!(benches);
