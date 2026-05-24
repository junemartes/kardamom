//! Criterion: sequential executor throughput.
//!
//! Scenarios:
//!   - `transfer_step`         : just `execute_tx` for plain transfers (per-tx CPU).
//!   - `actor_throughput`      : full actor end-to-end via mock channels.
//!   - `sstore_step`           : `execute_tx` against an SSTORE-heavy contract.
//!
//! Throughput floors are not asserted (CI variance is real). Run `cargo
//! bench` locally to compare hardware-relative numbers; the spec target
//! is >50k tx/s on plain transfers on one core.

use std::thread;
use std::time::Duration;

use alloy_consensus::{SignableTransaction, TxLegacy};
use alloy_eips::eip2718::Encodable2718;
use alloy_network::TxSignerSync;
use alloy_primitives::{
    Address, Bytes as AlloyBytes, TxKind as APTxKind, U256, address, keccak256,
};
use alloy_signer_local::PrivateKeySigner;
use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use crossbeam_channel::{Receiver, Sender, bounded};
use revm::primitives::KECCAK_EMPTY;
use revm::state::Bytecode;

use kardamom_executor::block_env::ExecEnv;
use kardamom_executor::executor::execute_tx;
use kardamom_executor::{
    BMessage, BPosition, BlockBoundaryStart, CMessage, ChannelBSubscription, ChannelCPublication,
    Executor, ExecutorConfig, ExecutorError, MockStateDatabase, MutatingSnapshotSource,
    PendingDelta, StateWriterSignal, TxEnvelope as KtTxEnvelope, TxIndex, WriterApplyingQueue,
};

const SSTORE_42_AT_VAR_KEY: [u8; 8] = [
    0x60, 0x42, // PUSH1 0x42 (value)
    0x60, 0x00, // PUSH1 0x00 (key)
    0x55, // SSTORE
    0x60, 0x00, // PUSH1 0x00
    0x00, // STOP
];

fn wrap_envelope(
    signer: &PrivateKeySigner,
    alloy_env: alloy_consensus::TxEnvelope,
) -> KtTxEnvelope {
    let raw_tx = Bytes::from(alloy_env.encoded_2718());
    let tx_hash = keccak256(&raw_tx);
    KtTxEnvelope {
        correlation_id: 0,
        raw_tx,
        sender: signer.address(),
        tx_hash,
    }
}

fn signed_transfer(signer: &PrivateKeySigner, to: Address, nonce: u64) -> KtTxEnvelope {
    let mut tx = TxLegacy {
        chain_id: Some(1),
        nonce,
        gas_price: 0,
        gas_limit: 21_000,
        to: APTxKind::Call(to),
        value: U256::from(1u64),
        input: AlloyBytes::new(),
    };
    let sig = signer.sign_transaction_sync(&mut tx).unwrap();
    wrap_envelope(signer, tx.into_signed(sig).into())
}

fn signed_sstore_call(signer: &PrivateKeySigner, contract: Address, nonce: u64) -> KtTxEnvelope {
    let mut tx = TxLegacy {
        chain_id: Some(1),
        nonce,
        gas_price: 0,
        gas_limit: 100_000,
        to: APTxKind::Call(contract),
        value: U256::ZERO,
        input: AlloyBytes::new(),
    };
    let sig = signer.sign_transaction_sync(&mut tx).unwrap();
    wrap_envelope(signer, tx.into_signed(sig).into())
}

fn pos(off: i32) -> BPosition {
    BPosition {
        term_id: 0,
        term_offset: off,
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

    let mut group = c.benchmark_group("transfer_step");
    group.throughput(Throughput::Elements(1));
    // Bench the per-tx CPU cost of a single transfer at nonce 0; the
    // snapshot is rebuilt each iteration so we don't accumulate state.
    group.bench_function("plain_transfer", |b| {
        b.iter(|| {
            let delta = PendingDelta::new();
            let env_tx = signed_transfer(&signer, to, 0);
            let _ = execute_tx(&snap, &delta, env, TxIndex(0), pos(0), &env_tx).unwrap();
        })
    });
    group.finish();
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

    let mut group = c.benchmark_group("sstore_step");
    group.throughput(Throughput::Elements(1));
    group.bench_function("sstore_one_slot", |b| {
        b.iter(|| {
            let delta = PendingDelta::new();
            let env_tx = signed_sstore_call(&signer, contract, 0);
            let (_r, _ws) = execute_tx(&snap, &delta, env, TxIndex(0), pos(0), &env_tx).unwrap();
        })
    });
    group.finish();
}

// Actor end-to-end: BATCH txs per iter; reports throughput in tx/s.
struct ChanBSub(Receiver<BMessage>);
impl ChannelBSubscription for ChanBSub {
    fn next(&mut self) -> Result<BMessage, ExecutorError> {
        self.0.recv().map_err(|_| ExecutorError::ChannelBClosed)
    }
}
struct ChanCPub(Sender<CMessage>);
impl ChannelCPublication for ChanCPub {
    fn publish(&mut self, m: CMessage) -> Result<(), ExecutorError> {
        self.0.send(m).map_err(|_| ExecutorError::ChannelCClosed)
    }
}
struct Imm;
impl StateWriterSignal for Imm {
    fn wait_committed(&mut self, b: u64) -> Result<u64, ExecutorError> {
        Ok(b)
    }
}

fn bench_actor_throughput(c: &mut Criterion) {
    const BATCH: u64 = 256;
    let mut group = c.benchmark_group("actor_throughput");
    group.throughput(Throughput::Elements(BATCH));

    group.bench_function(BenchmarkId::from_parameter("transfers_256"), |b| {
        b.iter(|| {
            let signer = PrivateKeySigner::random();
            let from = signer.address();
            let to = address!("00000000000000000000000000000000DEAD0001");
            let snap = MockStateDatabase::builder()
                .account(from, U256::MAX, 0, KECCAK_EMPTY)
                .build();
            let writer_q = WriterApplyingQueue::new(snap.clone());
            let snapshots = MutatingSnapshotSource(snap);

            let (b_tx, b_rx) = bounded::<BMessage>((BATCH as usize) + 8);
            let (c_tx, c_rx) = bounded::<CMessage>((BATCH as usize) + 8);

            for i in 0..BATCH {
                b_tx.send(BMessage::Tx {
                    position: pos(i as i32),
                    tx_idx: TxIndex(i),
                    envelope: signed_transfer(&signer, to, i),
                })
                .unwrap();
            }
            b_tx.send(BMessage::BlockBoundaryStart(BlockBoundaryStart {
                block_number: 1,
                end_tx_idx: pos((BATCH as i32) - 1),
                l2_timestamp: 0,
            }))
            .unwrap();
            drop(b_tx);

            let h = thread::spawn(move || {
                Executor::run(
                    ExecutorConfig {
                        chain_id: 1,
                        receipt_queue_depth: 512,
                    },
                    ChanBSub(b_rx),
                    ChanCPub(c_tx),
                    snapshots,
                    Imm,
                    writer_q,
                    0,
                )
            });

            let mut got = 0u64;
            while let Ok(m) = c_rx.recv_timeout(Duration::from_secs(10)) {
                if matches!(m, CMessage::Receipt(_)) {
                    got += 1;
                }
            }
            assert_eq!(got, BATCH);
            h.join().expect("no panic").expect("ok");
        });
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
