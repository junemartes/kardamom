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

use kardamom_engine::block_env::ExecEnv;
// The exec-core Executor (the per-block EVM scope) is a different type
// from the actor `kardamom_engine::Executor` imported below.
use kardamom_engine::executor::Executor as ExecCore;
use kardamom_engine::{
    BPosition, BlockBoundaryStart, CMessage, EngineWiring, Executor, ExecutorConfig, ExecutorError,
    Inbound, MockStateDatabase, MutatingSnapshotSource, NoEpochCheck, Outbound, PendingDelta,
    ResumePoint, RoleHooks, StateWriterSignal, TxDataSubscription, TxEnvelope as KtTxEnvelope,
    TxIndex, TxOrderingMessage, TxOrderingSubscription, TxReceiptsPublication, TxRef,
    WriterApplyingQueue,
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
            let _ = ExecCore::execute_once(
                &snap,
                None,
                &delta,
                env,
                TxIndex(0),
                pos(0),
                &env_tx,
                0,
                0,
                None,
            )
            .unwrap();
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
            let (_r, _ws) = ExecCore::execute_once(
                &snap,
                None,
                &delta,
                env,
                TxIndex(0),
                pos(0),
                &env_tx,
                0,
                0,
                None,
            )
            .unwrap();
        })
    });
    group.finish();
}

// Actor end-to-end: BATCH txs per iter; reports throughput in tx/s.
struct ChanTxDataSub {
    sequencer_id: u8,
    rx: Receiver<(BPosition, KtTxEnvelope)>,
}
impl TxDataSubscription for ChanTxDataSub {
    fn sequencer_id(&self) -> u8 {
        self.sequencer_id
    }
    fn next(&mut self) -> Result<(kardamom_types::TxDataLoc, KtTxEnvelope), ExecutorError> {
        self.rx
            .recv()
            .map(|(pos, env)| (kardamom_types::TxDataLoc::new(0, pos), env))
            .map_err(|_| ExecutorError::TxDataClosed {
                sequencer_id: self.sequencer_id,
            })
    }
}
struct ChanTxOrderingSub(Receiver<(BPosition, TxOrderingMessage)>);
impl TxOrderingSubscription for ChanTxOrderingSub {
    fn next(&mut self) -> Result<(BPosition, TxOrderingMessage), ExecutorError> {
        self.0.recv().map_err(|_| ExecutorError::TxOrderingClosed)
    }
}
struct ChanReceiptsPub(Sender<CMessage>);
impl TxReceiptsPublication for ChanReceiptsPub {
    fn publish(&mut self, m: CMessage) -> Result<(), ExecutorError> {
        self.0.send(m).map_err(|_| ExecutorError::TxReceiptsClosed)
    }
}
struct Imm;
impl StateWriterSignal for Imm {
    fn committed(&mut self) -> Result<u64, ExecutorError> {
        Ok(u64::MAX)
    }
    fn wait_committed(&mut self, b: u64) -> Result<u64, ExecutorError> {
        Ok(b)
    }
}

/// Port types for the bench's channel-backed fakes.
struct BenchWiring;
impl EngineWiring for BenchWiring {
    type TxData = ChanTxDataSub;
    type TxOrdering = ChanTxOrderingSub;
    type TxReceipts = ChanReceiptsPub;
    type Snapshots = MutatingSnapshotSource;
    type WriterSignal = Imm;
    type WriterQueue = WriterApplyingQueue;
    type Epoch = NoEpochCheck;
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

            // Post-S4-arch-update wiring: pre-load all envelopes onto a
            // single tx_data and all TxRefs onto tx_ordering before the
            // executor starts. The bench measures end-to-end actor
            // throughput; the demux split itself adds one extra crossbeam
            // hop per tx, which should be negligible vs. revm time.
            let (a_tx, a_rx) = bounded::<(BPosition, KtTxEnvelope)>((BATCH as usize) + 8);
            let (b_tx, b_rx) = bounded::<(BPosition, TxOrderingMessage)>((BATCH as usize) + 8);
            let (c_tx, c_rx) = bounded::<CMessage>((BATCH as usize) + 8);

            for i in 0..BATCH {
                let tx_data_position = pos((i as i32) * 200);
                let env = signed_transfer(&signer, to, i);
                let tx_hash = env.tx_hash;
                a_tx.send((tx_data_position, env)).unwrap();
                b_tx.send((
                    pos(i as i32),
                    TxOrderingMessage::TxRef(TxRef::new(tx_hash, 0, tx_data_position, 0)),
                ))
                .unwrap();
            }
            b_tx.send((
                pos(BATCH as i32),
                TxOrderingMessage::BoundaryStart(BlockBoundaryStart {
                    block_number: 1,
                    // end_tx_idx is the cumulative COUNT of canonical records
                    // through this block (encoded via BPosition::from_index),
                    // which the executor compares against its applied-record
                    // count — here all BATCH txs. (Not the last tx's index.)
                    end_tx_idx: BPosition::from_index(BATCH),
                    l2_timestamp: 0,
                    l1_origin: 0,
                }),
            ))
            .unwrap();
            drop(a_tx);
            drop(b_tx);

            let tx_data_subs = vec![ChanTxDataSub {
                sequencer_id: 0,
                rx: a_rx,
            }];
            let h = thread::spawn(move || {
                Executor::run::<BenchWiring>(
                    ExecutorConfig {
                        chain_id: 1,
                        receipt_queue_depth: 512,
                        ..Default::default()
                    },
                    Inbound {
                        tx_data: tx_data_subs,
                        tx_ordering: ChanTxOrderingSub(b_rx),
                        join_recovery: None,
                    },
                    Outbound {
                        tx_receipts: ChanReceiptsPub(c_tx),
                        snapshots,
                        writer_signal: Imm,
                        writer_queue: writer_q,
                    },
                    ResumePoint::GENESIS,
                    RoleHooks::none(),
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
