//! Determinism conformance: two executor instances driven by the same input
//! must produce byte-identical channel-C output (every `tx_hash` and every
//! `write_set_hash` matches). No state-root assertion: the executor does not
//! emit a state-root commitment (S0 D-Sh11).

use std::thread;
use std::time::Duration;

use alloy_consensus::{SignableTransaction, TxLegacy};
use alloy_eips::eip2718::Encodable2718;
use alloy_network::TxSignerSync;
use alloy_primitives::{Bytes as AlloyBytes, TxKind as APTxKind, U256, address, keccak256};
use alloy_signer_local::PrivateKeySigner;
use bytes::Bytes;
use crossbeam_channel::{Receiver, Sender, bounded};
use revm::primitives::KECCAK_EMPTY;

use kardamom_executor::{
    BMessage, BPosition, BlockBoundaryStart, CMessage, ChannelBSubscription, ChannelCPublication,
    Executor, ExecutorConfig, ExecutorError, MockStateDatabase, MutatingSnapshotSource,
    StateWriterSignal, TxEnvelope as KtTxEnvelope, TxIndex, WriterApplyingQueue,
};

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

fn populate_b(b_tx: &Sender<BMessage>, signer: &PrivateKeySigner) {
    let to = address!("00000000000000000000000000000000DEAD0001");
    let mut tx_idx: u64 = 0;
    let mut nonce: u64 = 0;
    for blk in 1..=3u64 {
        for _ in 0..5 {
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
            let alloy_env: alloy_consensus::TxEnvelope = tx.into_signed(sig).into();
            let raw_tx = Bytes::from(alloy_env.encoded_2718());
            let tx_hash = keccak256(&raw_tx);
            let env = KtTxEnvelope {
                correlation_id: 0,
                raw_tx,
                sender: signer.address(),
                tx_hash,
            };
            b_tx.send(BMessage::Tx {
                position: BPosition {
                    term_id: 0,
                    term_offset: tx_idx as i32,
                },
                tx_idx: TxIndex(tx_idx),
                envelope: env,
            })
            .unwrap();
            tx_idx += 1;
            nonce += 1;
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
}

fn run_one(signer: PrivateKeySigner) -> Vec<CMessage> {
    let from = signer.address();
    let snap = MockStateDatabase::builder()
        .account(from, U256::from(10u128.pow(18)), 0, KECCAK_EMPTY)
        .build();
    let writer_q = WriterApplyingQueue::new(snap.clone());
    let snapshots = MutatingSnapshotSource(snap);

    let (b_tx, b_rx) = bounded::<BMessage>(128);
    let (c_tx, c_rx) = bounded::<CMessage>(128);

    populate_b(&b_tx, &signer);
    drop(b_tx);

    let cfg = ExecutorConfig {
        chain_id: 1,
        receipt_queue_depth: 128,
    };
    let h = thread::spawn(move || {
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

    let mut out = Vec::new();
    while let Ok(m) = c_rx.recv_timeout(Duration::from_secs(5)) {
        out.push(m);
    }
    h.join().expect("no panic").expect("ok");
    out
}

#[test]
fn two_replicas_produce_byte_identical_c_stream() {
    let signer_a =
        PrivateKeySigner::from_bytes(&alloy_primitives::B256::repeat_byte(0xCD)).unwrap();
    let signer_b =
        PrivateKeySigner::from_bytes(&alloy_primitives::B256::repeat_byte(0xCD)).unwrap();
    assert_eq!(signer_a.address(), signer_b.address());

    let a = run_one(signer_a);
    let b = run_one(signer_b);

    assert_eq!(a.len(), b.len());
    for (i, (x, y)) in a.iter().zip(b.iter()).enumerate() {
        match (x, y) {
            (CMessage::Receipt(rx), CMessage::Receipt(ry)) => {
                assert_eq!(rx, ry, "receipt mismatch at idx {i}");
            }
            (CMessage::BlockBoundary(bx), CMessage::BlockBoundary(by)) => {
                assert_eq!(bx, by, "boundary mismatch at idx {i}");
            }
            _ => panic!("type mismatch at idx {i}"),
        }
    }
}
