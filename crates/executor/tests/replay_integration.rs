//! Integration test: feed a synthetic stream of (txs + boundaries) into an
//! `Executor` and assert the tx_receipts output matches expectation.
//!
//! Post-S4-arch-update wiring: single-sequencer
//! (M=1) topology. Envelopes are pushed onto a fake tx_data; tiny
//! `TxRef` records + a `BlockBoundaryStart` are pushed onto fake channel
//! B in the same canonical order. The executor's M+1 readers join the
//! two streams via the in-process `JoinBuffer`. The expected receipts
//! and slim boundaries on tx_receipts are unchanged from pre-split.

use std::thread;
use std::time::Duration;

use alloy_consensus::{SignableTransaction, TxLegacy};
use alloy_eips::eip2718::Encodable2718;
use alloy_network::TxSignerSync;
use alloy_primitives::{
    Address, B256, Bytes as AlloyBytes, TxKind as APTxKind, U256, address, keccak256,
};
use alloy_signer_local::PrivateKeySigner;
use bytes::Bytes;
use crossbeam_channel::{Receiver, Sender, bounded};
use revm::primitives::KECCAK_EMPTY;

use executor::{
    BPosition, BlockBoundary, BlockBoundaryStart, CMessage, ChannelCPublication, Executor,
    ExecutorConfig, ExecutorError, MockStateDatabase, MutatingSnapshotSource, StateWriterSignal,
    TxDataSubscription, TxEnvelope as KtTxEnvelope, TxOrderingMessage, TxOrderingSubscription,
    TxRef, WriterApplyingQueue,
};

/// Bridge a crossbeam receiver of `(BPosition, TxEnvelope)` into a
/// `TxDataSubscription`. The `BPosition` we emit is the tx_data
/// position (the value sequencers publish in `TxRef`); we make it equal
/// to a synthetic offset for the test.
struct ChanASub {
    sequencer_id: u8,
    rx: Receiver<(BPosition, KtTxEnvelope)>,
}
impl TxDataSubscription for ChanASub {
    fn sequencer_id(&self) -> u8 {
        self.sequencer_id
    }
    fn next(&mut self) -> Result<(BPosition, KtTxEnvelope), ExecutorError> {
        self.rx.recv().map_err(|_| ExecutorError::ChannelAClosed {
            sequencer_id: self.sequencer_id,
        })
    }
}

struct ChanBSub(Receiver<(BPosition, TxOrderingMessage)>);
impl TxOrderingSubscription for ChanBSub {
    fn next(&mut self) -> Result<(BPosition, TxOrderingMessage), ExecutorError> {
        self.0.recv().map_err(|_| ExecutorError::ChannelBClosed)
    }
}

struct ChanCPub(Sender<CMessage>);
impl ChannelCPublication for ChanCPub {
    fn publish(&mut self, msg: CMessage) -> Result<(), ExecutorError> {
        self.0.send(msg).map_err(|_| ExecutorError::ChannelCClosed)
    }
}

struct Imm;
impl StateWriterSignal for Imm {
    fn wait_committed(&mut self, b: u64) -> Result<u64, ExecutorError> {
        Ok(b)
    }
}

/// Proxy-style envelope builder: sign, encode raw_tx, populate sender + tx_hash.
fn transfer(signer: &PrivateKeySigner, nonce: u64, to: Address, val: u64) -> KtTxEnvelope {
    let mut tx = TxLegacy {
        chain_id: Some(1),
        nonce,
        gas_price: 0,
        gas_limit: 21_000,
        to: APTxKind::Call(to),
        value: U256::from(val),
        input: AlloyBytes::new(),
    };
    let sig = signer.sign_transaction_sync(&mut tx).unwrap();
    let alloy_env: alloy_consensus::TxEnvelope = tx.into_signed(sig).into();
    let raw_tx = Bytes::from(alloy_env.encoded_2718());
    let tx_hash = keccak256(&raw_tx);
    KtTxEnvelope {
        correlation_id: 0,
        raw_tx,
        sender: signer.address(),
        tx_hash,
    }
}

fn bpos(off: i32) -> BPosition {
    BPosition {
        term_id: 0,
        term_offset: off,
    }
}

#[test]
fn replay_10_txs_across_3_blocks_yields_expected_c_stream() {
    let signer = PrivateKeySigner::random();
    let from = signer.address();
    let to = address!("00000000000000000000000000000000000ABCDE");

    // Shared MockStateDatabase — the writer applies each block's delta back
    // into it so the next block's snapshot reflects the previous block's
    // writes (matching the production libmdbx semantics).
    let snap = MockStateDatabase::builder()
        .account(from, U256::from(10u128.pow(18)), 0, KECCAK_EMPTY)
        .build();

    // Single-sequencer topology: one tx_data, one tx_ordering.
    let (a_tx, a_rx) = bounded::<(BPosition, KtTxEnvelope)>(64);
    let (b_tx, b_rx) = bounded::<(BPosition, TxOrderingMessage)>(64);
    let (c_tx, c_rx) = bounded::<CMessage>(64);

    // 4 txs → boundary block 1 → 3 txs → boundary block 2 → 3 txs → boundary block 3.
    let mut nonce: u64 = 0;
    let mut bpos_off: i32 = 0;
    let mut a_pos: i32 = 0;
    let mut expected_hashes: Vec<B256> = Vec::new();
    let plan = [(4u64, 1u64), (3, 2), (3, 3)];
    for (n_txs, blk) in plan {
        for _ in 0..n_txs {
            let env = transfer(&signer, nonce, to, 1);
            let tx_hash = env.tx_hash;
            expected_hashes.push(tx_hash);
            // Publish to tx_data[0] at `a_pos`.
            let tx_data_position = bpos(a_pos);
            a_tx.send((tx_data_position, env)).unwrap();
            // Then publish a TxRef onto tx_ordering at canonical position
            // `bpos_off`. The executor uses the B position as the tx's
            // canonical id (Receipt.tx_idx).
            b_tx.send((
                bpos(bpos_off),
                TxOrderingMessage::TxRef(TxRef::new(tx_hash, 0, tx_data_position)),
            ))
            .unwrap();
            nonce += 1;
            bpos_off += 1;
            a_pos += 200;
        }
        b_tx.send((
            bpos(bpos_off),
            TxOrderingMessage::BoundaryStart(BlockBoundaryStart {
                block_number: blk,
                end_tx_idx: bpos(bpos_off - 1),
                l2_timestamp: 1_700_000_000 + blk,
            }),
        ))
        .unwrap();
    }
    drop(a_tx);
    drop(b_tx);

    let cfg = ExecutorConfig {
        chain_id: 1,
        receipt_queue_depth: 64,
        ..Default::default()
    };
    let writer_q = WriterApplyingQueue::new(snap.clone());
    let snapshots = MutatingSnapshotSource(snap);
    let a_subs: Vec<Box<dyn TxDataSubscription>> = vec![Box::new(ChanASub {
        sequencer_id: 0,
        rx: a_rx,
    })];
    let b_sub: Box<dyn TxOrderingSubscription> = Box::new(ChanBSub(b_rx));
    let join = thread::spawn(move || {
        Executor::run(
            cfg,
            a_subs,
            b_sub,
            ChanCPub(c_tx),
            snapshots,
            Imm,
            writer_q,
            0,
        )
    });

    let mut receipts = 0usize;
    let mut boundaries = 0usize;
    let mut got_hashes: Vec<B256> = Vec::new();
    while let Ok(msg) = c_rx.recv_timeout(Duration::from_secs(5)) {
        match msg {
            CMessage::Receipt(r) => {
                assert!(r.status, "tx {receipts} should succeed");
                assert_ne!(r.write_set_hash, B256::ZERO);
                got_hashes.push(r.tx_hash);
                receipts += 1;
            }
            CMessage::BlockBoundary(b) => {
                // S0: BlockBoundary has no state_root_commitment field.
                // We assert only the slim three-field shape via destructure.
                let BlockBoundary {
                    block_number,
                    end_tx_idx: _,
                    l2_timestamp: _,
                } = b;
                assert!((1..=3).contains(&block_number));
                boundaries += 1;
            }
        }
    }
    assert_eq!(receipts, 10);
    assert_eq!(boundaries, 3);
    // CRITICAL (S0): every receipt's tx_hash must equal the inbound
    // envelope's tx_hash, byte-for-byte, in the same order. The executor
    // never recomputes — it propagates.
    assert_eq!(got_hashes, expected_hashes);

    join.join().expect("no panic").expect("exec ok");
}
