//! Integration test: feed a synthetic stream of (txs + boundaries) into an
//! `Executor` and assert the channel-C output matches expectation.
//!
//! No real Aeron, no real libmdbx — mock channels and `MockStateDatabase`.

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

use kardamom_executor::{
    BMessage, BPosition, BlockBoundary, BlockBoundaryStart, CMessage, ChannelBSubscription,
    ChannelCPublication, Executor, ExecutorConfig, ExecutorError, MockStateDatabase,
    MutatingSnapshotSource, StateWriterSignal, TxEnvelope as KtTxEnvelope, TxIndex,
    WriterApplyingQueue,
};

struct ChanBSub(Receiver<BMessage>);
impl ChannelBSubscription for ChanBSub {
    fn next(&mut self) -> Result<BMessage, ExecutorError> {
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

    let (b_tx, b_rx) = bounded::<BMessage>(64);
    let (c_tx, c_rx) = bounded::<CMessage>(64);

    // 4 txs → boundary block 1 → 3 txs → boundary block 2 → 3 txs → boundary block 3.
    let mut nonce: u64 = 0;
    let mut tx_idx: u64 = 0;
    let mut expected_hashes: Vec<B256> = Vec::new();
    let plan = [(4u64, 1u64), (3, 2), (3, 3)];
    for (n_txs, blk) in plan {
        for _ in 0..n_txs {
            let env = transfer(&signer, nonce, to, 1);
            expected_hashes.push(env.tx_hash);
            b_tx.send(BMessage::Tx {
                position: BPosition {
                    term_id: 0,
                    term_offset: tx_idx as i32,
                },
                tx_idx: TxIndex(tx_idx),
                envelope: env,
            })
            .unwrap();
            nonce += 1;
            tx_idx += 1;
        }
        b_tx.send(BMessage::BlockBoundaryStart(BlockBoundaryStart {
            block_number: blk,
            end_tx_idx: BPosition {
                term_id: 0,
                term_offset: (tx_idx as i32) - 1,
            },
            l2_timestamp: 1_700_000_000 + blk,
        }))
        .unwrap();
    }
    drop(b_tx);

    let cfg = ExecutorConfig {
        chain_id: 1,
        receipt_queue_depth: 64,
    };
    let writer_q = WriterApplyingQueue::new(snap.clone());
    let snapshots = MutatingSnapshotSource(snap);
    let join = thread::spawn(move || {
        Executor::run(
            cfg,
            ChanBSub(b_rx),
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
                // S0 D-Sh11: BlockBoundary has no state_root_commitment field.
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
    // CRITICAL (S0 D-Sh4): every receipt's tx_hash must equal the inbound
    // envelope's tx_hash, byte-for-byte, in the same order. The executor
    // never recomputes — it propagates.
    assert_eq!(got_hashes, expected_hashes);

    join.join().expect("no panic").expect("exec ok");
}
