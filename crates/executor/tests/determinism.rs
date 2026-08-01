//! Determinism conformance: two executor instances driven by the same input
//! must produce byte-identical tx_receipts output (every `tx_hash` and every
//! `write_set_hash` matches). No state-root assertion: the executor does not
//! emit a state-root commitment (S0).
//!
//! Post-S4-arch-update wiring: M=1 tx_data + 1 tx_ordering, refs join via
//! the executor's `JoinBuffer`. Determinism doesn't depend on the demux
//! shape — it depends on canonical ordering, which tx_ordering preserves.

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
    BPosition, BlockBoundaryStart, CMessage, Executor, ExecutorConfig, ExecutorError,
    MockStateDatabase, MutatingSnapshotSource, StateWriterSignal, TxDataSubscription,
    TxEnvelope as KtTxEnvelope, TxOrderingMessage, TxOrderingSubscription, TxReceiptsPublication,
    TxRef, WriterApplyingQueue,
};

struct ChanASub {
    sequencer_id: u8,
    rx: Receiver<(BPosition, KtTxEnvelope)>,
}
impl TxDataSubscription for ChanASub {
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
struct ChanBSub(Receiver<(BPosition, TxOrderingMessage)>);
impl TxOrderingSubscription for ChanBSub {
    fn next(&mut self) -> Result<(BPosition, TxOrderingMessage), ExecutorError> {
        self.0.recv().map_err(|_| ExecutorError::TxOrderingClosed)
    }
}
struct ChanCPub(Sender<CMessage>);
impl TxReceiptsPublication for ChanCPub {
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

fn bpos(off: i32) -> BPosition {
    BPosition {
        term_id: 0,
        term_offset: off,
    }
}

fn populate(
    a_tx: &Sender<(BPosition, KtTxEnvelope)>,
    b_tx: &Sender<(BPosition, TxOrderingMessage)>,
    signer: &PrivateKeySigner,
) {
    let to = address!("00000000000000000000000000000000DEAD0001");
    let mut bpos_off: i32 = 0;
    let mut a_pos: i32 = 0;
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
            let tx_data_position = bpos(a_pos);
            a_tx.send((tx_data_position, env)).unwrap();
            b_tx.send((
                bpos(bpos_off),
                TxOrderingMessage::TxRef(TxRef::new(tx_hash, 0, tx_data_position, 0)),
            ))
            .unwrap();
            bpos_off += 1;
            a_pos += 200;
            nonce += 1;
        }
        // end_tx_idx is the cumulative COUNT of canonical records through this
        // block (alignment key). bpos_off has advanced once per TxRef, so it IS
        // that count; encode it via bpos() (== BPosition::from_index for these
        // small term-0 values).
        b_tx.send((
            bpos(bpos_off),
            TxOrderingMessage::BoundaryStart(BlockBoundaryStart {
                block_number: blk,
                end_tx_idx: bpos(bpos_off),
                l2_timestamp: 1_700_000_000 + blk,
            }),
        ))
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

    let (a_tx, a_rx) = bounded::<(BPosition, KtTxEnvelope)>(128);
    let (b_tx, b_rx) = bounded::<(BPosition, TxOrderingMessage)>(128);
    let (c_tx, c_rx) = bounded::<CMessage>(128);

    populate(&a_tx, &b_tx, &signer);
    drop(a_tx);
    drop(b_tx);

    let cfg = ExecutorConfig {
        chain_id: 1,
        receipt_queue_depth: 128,
        ..Default::default()
    };
    let a_subs: Vec<Box<dyn TxDataSubscription>> = vec![Box::new(ChanASub {
        sequencer_id: 0,
        rx: a_rx,
    })];
    let b_sub: Box<dyn TxOrderingSubscription> = Box::new(ChanBSub(b_rx));
    let h = thread::spawn(move || {
        Executor::run(
            cfg,
            a_subs,
            b_sub,
            None,
            ChanCPub(c_tx),
            snapshots,
            Imm,
            writer_q,
            0,
            None,
            None,
            // Whole-block exec strategy (validator parallel path).
            None,
            None,
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
