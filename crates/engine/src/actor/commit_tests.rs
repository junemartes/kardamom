//! Commit-thread tests: stream-order preservation, must-deliver retry,
//! adaptive batching, suffix resume, and the divergence fail-stop.

use std::sync::{Arc, Mutex};

use alloy_primitives::B256;
use crossbeam_channel::bounded;
use kardamom_types::{BPosition, BlockBoundary, Receipt};

use crate::error::ExecutorError;
use crate::exec_types::CMessage;

use super::{ExecToCommit, TxReceiptsPublication, spawn_commit};

struct RecordPub(Arc<Mutex<Vec<CMessage>>>);
impl TxReceiptsPublication for RecordPub {
    fn publish(&mut self, msg: CMessage) -> Result<(), ExecutorError> {
        self.0.lock().unwrap().push(msg);
        Ok(())
    }
}

#[test]
fn commit_thread_preserves_order() {
    let (tx, rx) = bounded::<ExecToCommit>(8);
    let log = Arc::new(Mutex::new(Vec::new()));
    let pos0 = BPosition {
        term_id: 0,
        term_offset: 0,
    };

    tx.send(ExecToCommit::Receipt(Receipt {
        tx_idx: pos0,
        tx_hash: B256::repeat_byte(0xAA),
        status: true,
        gas_used: 21_000,
        logs: Vec::new(),
        write_set_hash: B256::ZERO,
        ..Default::default()
    }))
    .unwrap();
    tx.send(ExecToCommit::Boundary(BlockBoundary {
        block_number: 1,
        end_tx_idx: pos0,
        l2_timestamp: 100,
        l1_origin: 0,
    }))
    .unwrap();
    drop(tx);

    let h = spawn_commit(RecordPub(log.clone()), rx);
    h.join().expect("no panic").expect("ok");

    let l = log.lock().unwrap();
    assert_eq!(l.len(), 2);
    assert!(matches!(&l[0], CMessage::Receipt(r) if r.tx_idx == pos0));
    assert!(matches!(&l[1], CMessage::BlockBoundary(b) if b.block_number == 1));
}

/// Rejects the first `fails_left` publish attempts (a transient
/// NOT_CONNECTED while the ingress subscription is forming), then records.
struct FlakyPub {
    fails_left: u32,
    log: Arc<Mutex<Vec<CMessage>>>,
}
impl TxReceiptsPublication for FlakyPub {
    fn publish(&mut self, msg: CMessage) -> Result<(), ExecutorError> {
        if self.fails_left > 0 {
            self.fails_left -= 1;
            return Err(ExecutorError::TxReceiptsClosed);
        }
        self.log.lock().unwrap().push(msg);
        Ok(())
    }
}

// tx_receipts is must-deliver: a transient publish failure (subscriber not
// yet connected during multi-host bring-up) must neither drop the receipt
// nor kill the commit thread — it must retry until the receipt lands.
#[test]
fn commit_thread_retries_until_delivered() {
    let (tx, rx) = bounded::<ExecToCommit>(8);
    let log = Arc::new(Mutex::new(Vec::new()));
    let pos0 = BPosition {
        term_id: 0,
        term_offset: 0,
    };
    tx.send(ExecToCommit::Receipt(Receipt {
        tx_idx: pos0,
        tx_hash: B256::repeat_byte(0xAB),
        status: true,
        gas_used: 21_000,
        logs: Vec::new(),
        write_set_hash: B256::ZERO,
        ..Default::default()
    }))
    .unwrap();
    drop(tx);

    // The publisher rejects the first 3 attempts, then accepts.
    let h = spawn_commit(
        FlakyPub {
            fails_left: 3,
            log: log.clone(),
        },
        rx,
    );
    // Must return Ok — the thread survived the transient failures.
    h.join()
        .expect("no panic")
        .expect("commit thread must not die on a transient publish failure");

    let l = log.lock().unwrap();
    assert_eq!(l.len(), 1, "the receipt must be delivered, not dropped");
    assert!(matches!(&l[0], CMessage::Receipt(r) if r.tx_idx == pos0));
}

fn receipt(tag: u8, offset: i32) -> Receipt {
    Receipt {
        tx_idx: BPosition {
            term_id: 0,
            term_offset: offset,
        },
        tx_hash: B256::repeat_byte(tag),
        status: true,
        gas_used: 21_000,
        logs: Vec::new(),
        write_set_hash: B256::ZERO,
        ..Default::default()
    }
}

/// Records each `publish_receipts` call's batch (the live transport's
/// one-frame-per-batch shape) plus boundaries via `publish`.
struct BatchRecordPub {
    batches: Arc<Mutex<Vec<Vec<Receipt>>>>,
    boundaries: Arc<Mutex<Vec<BlockBoundary>>>,
}
impl TxReceiptsPublication for BatchRecordPub {
    fn publish(&mut self, msg: CMessage) -> Result<(), ExecutorError> {
        match msg {
            CMessage::Receipt(r) => self.batches.lock().unwrap().push(vec![r]),
            CMessage::BlockBoundary(b) => self.boundaries.lock().unwrap().push(b),
        }
        Ok(())
    }
    fn publish_receipts(&mut self, receipts: &[Receipt]) -> (usize, Option<ExecutorError>) {
        self.batches.lock().unwrap().push(receipts.to_vec());
        (receipts.len(), None)
    }
}

// Queued receipts drain into ONE batch publish (adaptive batching), and a
// boundary flushes the receipts gathered before it — order preserved.
#[test]
fn commit_thread_batches_queued_receipts_and_flushes_on_boundary() {
    let (tx, rx) = bounded::<ExecToCommit>(16);
    for i in 0..5 {
        tx.send(ExecToCommit::Receipt(receipt(i as u8, i * 64)))
            .unwrap();
    }
    tx.send(ExecToCommit::Boundary(BlockBoundary {
        block_number: 1,
        end_tx_idx: BPosition {
            term_id: 0,
            term_offset: 4 * 64,
        },
        l2_timestamp: 100,
        l1_origin: 0,
    }))
    .unwrap();
    drop(tx);

    let batches = Arc::new(Mutex::new(Vec::new()));
    let boundaries = Arc::new(Mutex::new(Vec::new()));
    let h = spawn_commit(
        BatchRecordPub {
            batches: batches.clone(),
            boundaries: boundaries.clone(),
        },
        rx,
    );
    h.join().expect("no panic").expect("ok");

    let b = batches.lock().unwrap();
    assert_eq!(b.len(), 1, "already-queued receipts ride one batch");
    assert_eq!(b[0].len(), 5);
    let hashes: Vec<u8> = b[0].iter().map(|r| r.tx_hash.0[0]).collect();
    assert_eq!(hashes, vec![0, 1, 2, 3, 4], "in-batch order preserved");
    assert_eq!(
        boundaries.lock().unwrap().len(),
        1,
        "boundary after the flush"
    );
}

/// Publishes `accept` receipts of each batch then fails transiently once,
/// recording everything accepted — exercises the suffix resume.
struct PartialPub {
    accept: usize,
    fail_once: bool,
    delivered: Arc<Mutex<Vec<Receipt>>>,
}
impl TxReceiptsPublication for PartialPub {
    fn publish(&mut self, _msg: CMessage) -> Result<(), ExecutorError> {
        Ok(())
    }
    fn publish_receipts(&mut self, receipts: &[Receipt]) -> (usize, Option<ExecutorError>) {
        if self.fail_once {
            self.fail_once = false;
            let n = self.accept.min(receipts.len());
            self.delivered
                .lock()
                .unwrap()
                .extend_from_slice(&receipts[..n]);
            return (n, Some(ExecutorError::TxReceiptsClosed));
        }
        self.delivered.lock().unwrap().extend_from_slice(receipts);
        (receipts.len(), None)
    }
}

// A partial batch failure resumes at the unpublished SUFFIX: every receipt
// is delivered exactly once, in order.
#[test]
fn commit_thread_resumes_batch_at_failed_suffix() {
    let (tx, rx) = bounded::<ExecToCommit>(16);
    for i in 0..6 {
        tx.send(ExecToCommit::Receipt(receipt(i as u8, i * 64)))
            .unwrap();
    }
    drop(tx);

    let delivered = Arc::new(Mutex::new(Vec::new()));
    let h = spawn_commit(
        PartialPub {
            accept: 2,
            fail_once: true,
            delivered: delivered.clone(),
        },
        rx,
    );
    h.join().expect("no panic").expect("ok");

    let d = delivered.lock().unwrap();
    let hashes: Vec<u8> = d.iter().map(|r| r.tx_hash.0[0]).collect();
    assert_eq!(
        hashes,
        vec![0, 1, 2, 3, 4, 5],
        "each receipt delivered exactly once, in order, across the resume"
    );
}

/// A sink that reports a PROVEN divergence (the validator's receipt
/// cross-check) on every publish.
struct DivergingPub;
impl TxReceiptsPublication for DivergingPub {
    fn publish(&mut self, _msg: CMessage) -> Result<(), ExecutorError> {
        Err(ExecutorError::Divergence("receipt mismatch at tx 0".into()))
    }
}

// F10.1 regression: the must-deliver retry must NOT spin on a proven
// divergence — that would consume the fail-stop (retry → empty buffer →
// "unverified" → pipeline keeps committing). A Divergence error has to
// propagate out of the commit thread immediately.
#[test]
fn commit_thread_fail_stops_on_divergence() {
    let (tx, rx) = bounded::<ExecToCommit>(8);
    tx.send(ExecToCommit::Receipt(Receipt {
        tx_idx: BPosition {
            term_id: 0,
            term_offset: 0,
        },
        ..Default::default()
    }))
    .unwrap();
    drop(tx);

    let h = spawn_commit(DivergingPub, rx);
    let res = h.join().expect("no panic");
    assert!(
        matches!(res, Err(ExecutorError::Divergence(_))),
        "divergence must propagate, not be retried: {res:?}"
    );
}
