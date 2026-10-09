//! Tests for the commit thread: stream order, must-deliver retry, adaptive
//! batching, suffix resume, and the divergence fail-stop.

use std::sync::{Arc, Mutex};

use alloy_primitives::B256;
use kardamom_types::{BlockBoundary, Receipt, ReceiptRows};

use crate::error::ExecutorError;
use crate::exec_types::CMessage;

use super::test_support::{feed_commits, pos};
use super::{CommitLoop, Escalation, ExecToCommit, TxReceiptsPublication};

struct RecordPub(Arc<Mutex<Vec<CMessage>>>);
impl TxReceiptsPublication for RecordPub {
    fn publish(&mut self, msg: CMessage) -> Result<(), ExecutorError> {
        self.0.lock().unwrap().push(msg);
        Ok(())
    }
}

fn receipt(tag: u8, offset: i32) -> Box<ReceiptRows> {
    Box::new(ReceiptRows::bare(Receipt {
        tx_idx: pos(offset),
        tx_hash: B256::repeat_byte(tag),
        status: true,
        gas_used: 21_000,
        logs: Vec::new(),
        write_set_hash: B256::ZERO,
        ..Default::default()
    }))
}

#[test]
fn commit_thread_preserves_order() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let pos0 = pos(0);

    let rx = feed_commits(vec![
        ExecToCommit::Receipt(Box::new(ReceiptRows::bare(Receipt {
            tx_idx: pos0,
            tx_hash: B256::repeat_byte(0xAA),
            status: true,
            gas_used: 21_000,
            logs: Vec::new(),
            write_set_hash: B256::ZERO,
            ..Default::default()
        }))),
        ExecToCommit::Boundary(BlockBoundary {
            block_number: 1,
            end_tx_idx: pos0,
            l2_timestamp: 100,
            l1_origin: 0,
            base_fee: 0,
            gas_used: 0,
        }),
    ]);

    let h = CommitLoop::new(RecordPub(log.clone()), rx).spawn();
    h.join().expect("no panic").expect("ok");

    let l = log.lock().unwrap();
    assert_eq!(l.len(), 2);
    assert!(matches!(&l[0], CMessage::Receipt(r) if r.tx_idx == pos0));
    assert!(matches!(&l[1], CMessage::BlockBoundary(b) if b.block_number == 1));
}

/// Rejects the first `fails_left` publish attempts, then records each one.
/// This simulates a transient `NOT_CONNECTED` error while the ingress
/// subscription is forming.
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

// tx_receipts is must-deliver. A transient publish failure (the subscriber
// is not yet connected during multi-host startup) must not drop the
// receipt or stop the commit thread. The thread must retry until the
// receipt lands.
#[test]
fn commit_thread_retries_until_delivered() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let pos0 = pos(0);
    let rx = feed_commits(vec![ExecToCommit::Receipt(Box::new(ReceiptRows::bare(
        Receipt {
            tx_idx: pos0,
            tx_hash: B256::repeat_byte(0xAB),
            status: true,
            gas_used: 21_000,
            logs: Vec::new(),
            write_set_hash: B256::ZERO,
            ..Default::default()
        },
    )))]);

    // The publisher rejects the first 3 attempts, then accepts.
    let h = CommitLoop::new(
        FlakyPub {
            fails_left: 3,
            log: log.clone(),
        },
        rx,
    )
    .spawn();
    // This must return Ok. The thread survived the transient failures.
    h.join()
        .expect("no panic")
        .expect("commit thread must not die on a transient publish failure");

    let l = log.lock().unwrap();
    assert_eq!(l.len(), 1, "the receipt must be delivered, not dropped");
    assert!(matches!(&l[0], CMessage::Receipt(r) if r.tx_idx == pos0));
}

/// Records the batch from each `publish_receipts` call. This matches the
/// live transport's one-frame-per-batch shape. It also records boundaries
/// through `publish`.
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
    fn publish_receipts(&mut self, items: &[ReceiptRows]) -> (usize, Option<ExecutorError>) {
        let receipts = items.iter().map(|i| i.receipt.clone()).collect();
        self.batches.lock().unwrap().push(receipts);
        (items.len(), None)
    }
}

// Queued receipts drain into one batch publish (adaptive batching). A
// boundary flushes the receipts gathered before it, and order is preserved.
#[test]
fn commit_thread_batches_queued_receipts_and_flushes_on_boundary() {
    let mut messages: Vec<ExecToCommit> = (0..5u8)
        .map(|i| ExecToCommit::Receipt(receipt(i, i32::from(i) * 64)))
        .collect();
    messages.push(ExecToCommit::Boundary(BlockBoundary {
        block_number: 1,
        end_tx_idx: pos(4 * 64),
        l2_timestamp: 100,
        l1_origin: 0,
        base_fee: 0,
        gas_used: 0,
    }));
    let rx = feed_commits(messages);

    let batches = Arc::new(Mutex::new(Vec::new()));
    let boundaries = Arc::new(Mutex::new(Vec::new()));
    let h = CommitLoop::new(
        BatchRecordPub {
            batches: batches.clone(),
            boundaries: boundaries.clone(),
        },
        rx,
    )
    .spawn();
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

/// Publishes `accept` receipts from each batch, then fails once with a
/// transient error. Records everything it accepts. This tests the suffix
/// resume.
struct PartialPub {
    accept: usize,
    fail_once: bool,
    delivered: Arc<Mutex<Vec<Receipt>>>,
}
impl TxReceiptsPublication for PartialPub {
    fn publish(&mut self, _msg: CMessage) -> Result<(), ExecutorError> {
        Ok(())
    }
    fn publish_receipts(&mut self, items: &[ReceiptRows]) -> (usize, Option<ExecutorError>) {
        let receipts: Vec<Receipt> = items.iter().map(|i| i.receipt.clone()).collect();
        if self.fail_once {
            self.fail_once = false;
            let n = self.accept.min(receipts.len());
            self.delivered
                .lock()
                .unwrap()
                .extend_from_slice(&receipts[..n]);
            return (n, Some(ExecutorError::TxReceiptsClosed));
        }
        self.delivered.lock().unwrap().extend_from_slice(&receipts);
        (receipts.len(), None)
    }
}

// A partial batch failure resumes at the unpublished suffix. Every receipt
// is delivered exactly once, in order.
#[test]
fn commit_thread_resumes_batch_at_failed_suffix() {
    let rx = feed_commits(
        (0..6u8)
            .map(|i| ExecToCommit::Receipt(receipt(i, i32::from(i) * 64)))
            .collect(),
    );

    let delivered = Arc::new(Mutex::new(Vec::new()));
    let h = CommitLoop::new(
        PartialPub {
            accept: 2,
            fail_once: true,
            delivered: delivered.clone(),
        },
        rx,
    )
    .spawn();
    h.join().expect("no panic").expect("ok");

    let d = delivered.lock().unwrap();
    let hashes: Vec<u8> = d.iter().map(|r| r.tx_hash.0[0]).collect();
    assert_eq!(
        hashes,
        vec![0, 1, 2, 3, 4, 5],
        "each receipt delivered exactly once, in order, across the resume"
    );
}

/// A sink that reports a proven divergence on every publish. This
/// simulates the validator's receipt cross-check.
struct DivergingPub;
impl TxReceiptsPublication for DivergingPub {
    fn publish(&mut self, _msg: CMessage) -> Result<(), ExecutorError> {
        Err(ExecutorError::Divergence("receipt mismatch at tx 0".into()))
    }
}

// Regression test: the must-deliver retry must not spin on a proven
// divergence. Spinning would defeat the fail-stop, because the retry finds
// an empty buffer, lands in the "unverified" arm, and the pipeline keeps
// committing. A Divergence error must propagate out of the commit thread
// immediately.
#[test]
fn commit_thread_fail_stops_on_divergence() {
    let rx = feed_commits(vec![ExecToCommit::Receipt(Box::new(ReceiptRows::bare(
        Receipt {
            tx_idx: pos(0),
            ..Default::default()
        },
    )))]);

    let h = CommitLoop::new(DivergingPub, rx).spawn();
    let res = h.join().expect("no panic");
    assert!(
        matches!(res, Err(ExecutorError::Divergence(_))),
        "divergence must propagate, not be retried: {res:?}"
    );
}

/// A publication with no connected subscriber: every publish fails with
/// `NotConnected` until `reopen` runs `connects_after_reopen` times,
/// then every publish lands. A subscriber is listed from attempt
/// `listed_from_attempt` on. Counts the attempts and the reopens.
struct UnconnectedPub {
    reopens: Arc<Mutex<u32>>,
    connects_after_reopen: u32,
    listed_from_attempt: u32,
    attempts: Arc<Mutex<u32>>,
    log: Arc<Mutex<Vec<CMessage>>>,
}

impl UnconnectedPub {
    fn new(connects_after_reopen: u32, listed_from_attempt: u32) -> Self {
        Self {
            reopens: Arc::new(Mutex::new(0)),
            connects_after_reopen,
            listed_from_attempt,
            attempts: Arc::new(Mutex::new(0)),
            log: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn connected(&self) -> bool {
        *self.reopens.lock().unwrap() >= self.connects_after_reopen
    }
}

impl TxReceiptsPublication for UnconnectedPub {
    fn publish(&mut self, msg: CMessage) -> Result<(), ExecutorError> {
        *self.attempts.lock().unwrap() += 1;
        if !self.connected() {
            return Err(ExecutorError::NotConnected {
                topic: "tx_receipts".into(),
                detail: "aeron offer failed: NOT_CONNECTED (-1)".into(),
            });
        }
        self.log.lock().unwrap().push(msg);
        Ok(())
    }

    fn reopen(&mut self) -> Result<(), ExecutorError> {
        *self.reopens.lock().unwrap() += 1;
        Ok(())
    }

    fn subscribers_listed(&mut self) -> bool {
        *self.attempts.lock().unwrap() >= self.listed_from_attempt
    }
}

/// A tiny budget, so a test runs its whole escalation in under a second:
/// the reopen after 100 ms, the exit after 500 ms.
const TEST_BUDGET: std::time::Duration = std::time::Duration::from_millis(100);

fn one_receipt() -> crossbeam_channel::Receiver<ExecToCommit> {
    feed_commits(vec![ExecToCommit::Receipt(receipt(0xAC, 0))])
}

// A publication that connects after its reopen delivers the receipt: the
// escalation reopens once, and the retry carries the receipt across.
#[test]
fn commit_thread_reopens_an_unconnected_publication_once_and_delivers() {
    let publication = UnconnectedPub::new(1, 0);
    let (reopens, log) = (publication.reopens.clone(), publication.log.clone());
    let h = CommitLoop::new(publication, one_receipt())
        .escalating(Escalation::from_stall_budget(TEST_BUDGET))
        .spawn();
    h.join()
        .expect("no panic")
        .expect("delivered after the reopen");
    assert_eq!(
        *reopens.lock().unwrap(),
        1,
        "one reopen per unconnected period"
    );
    assert_eq!(log.lock().unwrap().len(), 1, "the receipt was not dropped");
}

// While no subscriber is listed, the clock does not run: no reopen, no
// exit, however long the publication stays unconnected. Once a subscriber
// is listed, the clock starts, and the reopen follows after one budget.
#[test]
fn commit_thread_escalates_only_while_a_subscriber_is_listed() {
    // About 10 attempts of 50 ms pass unlisted: five budgets, which
    // would have exited with a listed subscriber.
    let publication = UnconnectedPub::new(1, 10);
    let (reopens, attempts) = (publication.reopens.clone(), publication.attempts.clone());
    let started = std::time::Instant::now();
    let h = CommitLoop::new(publication, one_receipt())
        .escalating(Escalation::from_stall_budget(TEST_BUDGET))
        .spawn();
    h.join()
        .expect("no panic")
        .expect("delivered after the reopen, with no exit");
    assert_eq!(*reopens.lock().unwrap(), 1);
    assert!(
        *attempts.lock().unwrap() >= 12,
        "the reopen waits one budget after the listing: {} attempts",
        *attempts.lock().unwrap()
    );
    assert!(started.elapsed() >= TEST_BUDGET * 5);
}

// A publication that stays unconnected after its reopen ends the thread
// with `PublicationDead` after the total budget, with one reopen only.
#[test]
fn commit_thread_exits_after_the_total_budget_without_a_subscriber() {
    let publication = UnconnectedPub::new(u32::MAX, 0);
    let reopens = publication.reopens.clone();
    let started = std::time::Instant::now();
    let h = CommitLoop::new(publication, one_receipt())
        .escalating(Escalation::from_stall_budget(TEST_BUDGET))
        .spawn();
    let res = h.join().expect("no panic");
    assert!(
        matches!(
            &res,
            Err(ExecutorError::PublicationDead { topic, reopen_after_s: 0, .. }) if topic == "tx_receipts"
        ),
        "the thread must end with PublicationDead: {res:?}"
    );
    assert!(
        started.elapsed() >= TEST_BUDGET * 5,
        "the exit waits the whole budget"
    );
    assert_eq!(*reopens.lock().unwrap(), 1, "one reopen, then the exit");
}
