//! Criterion: sequential executor throughput.
//!
//! Scenarios:
//!   - `transfer_step`    : `execute_tx` for plain transfers (per-tx CPU).
//!   - `actor_throughput` : the full actor, end-to-end, over mock channels.
//!   - `sstore_step`      : `execute_tx` against an SSTORE-heavy contract.
//!
//! This bench does not assert throughput floors, because CI variance is
//! real. It prints numbers for a human to compare; run `cargo bench`
//! locally to compare hardware-relative numbers.
//!
#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_precision_loss,
    reason = "indices and counters here are bounded by small, fixed bench parameters, never near a truncation boundary"
)]

use std::num::NonZeroU64;
use std::num::NonZeroUsize;

use alloy_primitives::{Address, Bytes as AlloyBytes, U256, address};
use alloy_signer_local::PrivateKeySigner;
use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use revm::primitives::KECCAK_EMPTY;
use revm::state::Bytecode;

use kardamom_engine::actor::fixtures::{ChannelHarness, HarnessInput, HarnessSetup, LegacyTx};
use kardamom_engine::block_env::ExecEnv;
use kardamom_engine::executor::Executor as ExecCore;
use kardamom_engine::{
    BPosition, BlockBoundaryStart, CMessage, ExecutorConfig, MockStateDatabase, PendingDelta,
    ResumePoint, TxEnvelope, TxIndex, TxOrderingMessage, TxRef,
};

/// `ExecutorConfig::receipt_queue_depth` for [`bench_actor_throughput`]:
/// at least `BATCH` slots, plus headroom.
const QUEUE_DEPTH_512: NonZeroUsize = NonZeroUsize::new(512).unwrap();

const SSTORE_42_AT_VAR_KEY: [u8; 8] = [
    0x60, 0x42, // PUSH1 0x42 (value)
    0x60, 0x00, // PUSH1 0x00 (key)
    0x55, // SSTORE
    0x60, 0x00, // PUSH1 0x00
    0x00, // STOP
];

fn signed_transfer(signer: &PrivateKeySigner, to: Address, nonce: u64) -> TxEnvelope {
    LegacyTx {
        to,
        nonce,
        value: 1,
        ..Default::default()
    }
    .sign(signer)
}

fn signed_sstore_call(signer: &PrivateKeySigner, contract: Address, nonce: u64) -> TxEnvelope {
    LegacyTx {
        to: contract,
        nonce,
        gas_limit: 100_000,
        ..Default::default()
    }
    .sign(signer)
}

fn pos(off: i32) -> BPosition {
    BPosition {
        term_id: 0,
        term_offset: off,
    }
}

/// One repeated single-transaction bench: build a fresh
/// [`PendingDelta`] and a fresh transaction each iteration, so no state
/// accumulates across iterations, and run it through
/// [`ExecCore::execute_once`] at nonce 0. `snap` and `env` stay fixed
/// for the whole group; `mk` builds the transaction.
struct TxBench<'a, F: Fn() -> TxEnvelope> {
    snap: &'a MockStateDatabase,
    env: ExecEnv,
    mk: F,
}

impl<F: Fn() -> TxEnvelope> TxBench<'_, F> {
    fn run(&self, c: &mut Criterion, group: &str, name: &str) {
        let mut group = c.benchmark_group(group);
        group.throughput(Throughput::Elements(1));
        group.bench_function(name, |b| {
            b.iter(|| {
                let delta = PendingDelta::new();
                let env_tx = (self.mk)();
                let slot = kardamom_engine::exec_types::TxSlot {
                    tx_idx: TxIndex(0),
                    tx_position: pos(0),
                    tx_index_in_block: 0,
                    cumulative_gas_used_before: 0,
                };
                let _ =
                    ExecCore::execute_once(self.snap, None, &delta, self.env, slot, &env_tx, None)
                        .unwrap();
            });
        });
        group.finish();
    }
}

fn bench_transfer_step(c: &mut Criterion) {
    let signer = PrivateKeySigner::random();
    let from = signer.address();
    let to = address!("00000000000000000000000000000000000ABCDE");
    let snap = MockStateDatabase::builder()
        .account(from, U256::MAX, 0, KECCAK_EMPTY)
        .build();
    let env = ExecEnv {
        chain_id: 1,
        block_number: 1,
        l2_timestamp: 0,
    };

    TxBench {
        snap: &snap,
        env,
        mk: || signed_transfer(&signer, to, 0),
    }
    .run(c, "transfer_step", "plain_transfer");
}

fn bench_sstore_step(c: &mut Criterion) {
    let signer = PrivateKeySigner::random();
    let from = signer.address();
    let contract = address!("00000000000000000000000000000000000ABC55");
    let code = AlloyBytes::from_static(&SSTORE_42_AT_VAR_KEY);
    let code_hash = Bytecode::new_raw(code.clone()).hash_slow();
    let snap = MockStateDatabase::builder()
        .account(from, U256::MAX, 0, KECCAK_EMPTY)
        .account(contract, U256::ZERO, 1, code_hash)
        .code(code_hash, Bytes::copy_from_slice(code.as_ref()))
        .build();
    let env = ExecEnv {
        chain_id: 1,
        block_number: 1,
        l2_timestamp: 0,
    };

    TxBench {
        snap: &snap,
        env,
        mk: || signed_sstore_call(&signer, contract, 0),
    }
    .run(c, "sstore_step", "sstore_one_slot");
}

// Actor end-to-end: `BATCH` txs per iteration; reports throughput in tx/s.

/// Transfer `i`'s two records: its envelope on `tx_data`, and its `TxRef`
/// on `tx_ordering`.
fn transfer_records(
    signer: &PrivateKeySigner,
    to: Address,
    i: u64,
) -> ((BPosition, TxEnvelope), (BPosition, TxOrderingMessage)) {
    let tx_data_position = pos((i as i32) * 200);
    let env = signed_transfer(signer, to, i);
    let tx_ref = TxRef::new(env.tx_hash, 0, tx_data_position, 0);
    (
        (tx_data_position, env),
        (pos(i as i32), TxOrderingMessage::TxRef(tx_ref)),
    )
}

/// One `actor_throughput` iteration: runs `batch` transfers through an
/// isolated channel-backed executor, and checks it delivered them all.
fn run_one_batch(batch: u64) {
    let signer = PrivateKeySigner::random();
    let from = signer.address();
    let to = address!("00000000000000000000000000000000DEAD0001");
    let snap = MockStateDatabase::builder()
        .account(from, U256::MAX, 0, KECCAK_EMPTY)
        .build();

    let (tx_data, refs): (Vec<_>, Vec<_>) =
        (0..batch).map(|i| transfer_records(&signer, to, i)).unzip();
    let boundary = (
        pos(batch as i32),
        TxOrderingMessage::BoundaryStart(BlockBoundaryStart {
            block_number: 1,
            // end_tx_idx is the cumulative count of canonical records
            // through this block (encoded through `BPosition::from_index`).
            // The executor compares it against its applied-record count:
            // here, all `batch` txs. It is not the last tx's index.
            end_tx_idx: BPosition::from_index(batch),
            l2_timestamp: 0,
            l1_origin: 0,
        }),
    );
    let outcome = ChannelHarness::run(
        HarnessSetup {
            cfg: ExecutorConfig {
                chain_id: NonZeroU64::MIN,
                receipt_queue_depth: QUEUE_DEPTH_512,
                ..Default::default()
            },
            start: ResumePoint::GENESIS,
            snap,
        },
        HarnessInput {
            tx_data: vec![tx_data],
            tx_ordering: refs.into_iter().chain(std::iter::once(boundary)).collect(),
        },
    );
    outcome.result.expect("ok");
    let got = outcome
        .receipts
        .iter()
        .filter(|m| matches!(m, CMessage::Receipt(_)))
        .count();
    assert_eq!(got as u64, batch);
}

fn bench_actor_throughput(c: &mut Criterion) {
    const BATCH: u64 = 256;
    let mut group = c.benchmark_group("actor_throughput");
    group.throughput(Throughput::Elements(BATCH));

    group.bench_function(BenchmarkId::from_parameter("transfers_256"), |b| {
        b.iter(|| run_one_batch(BATCH));
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_transfer_step,
    bench_sstore_step,
    bench_actor_throughput
);
criterion_main!(benches);
