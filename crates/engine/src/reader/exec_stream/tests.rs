//! Tests for the executor stream source: the dedup of the copies, the
//! check against the canonical `TxRef`, the refetch of a gap from an
//! executor archive, the wait that never votes, the catch-up after a
//! restart, and the stop when every archive holds a mismatched record.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use alloy_primitives::{Address, B256};
use alloy_signer_local::PrivateKeySigner;
use bytes::Bytes;
use crossbeam_channel::{Receiver, Sender, unbounded};
use kardamom_types::{
    BPosition, BlockBoundaryStart, ExecTxRecord, TxEnvelope, TxOrderingMessage, TxRef, VoidRecord,
};

use super::super::tests_void::VotingSub;
use super::*;
use crate::reader::{NoExecStream, ReaderConfig, ReaderToExec, TxOrderingInputs, TxOrderingReader};

/// The `exec_txs` feed of a test: a channel. Its close ends the feed.
struct ChanFeed(Receiver<ExecTxRecord>);

impl ExecRecordSubscription for ChanFeed {
    fn next(&mut self) -> Option<ExecTxRecord> {
        self.0.recv().ok()
    }
}

/// One executor of the fake archive: its locator answer, and the records
/// its recording holds from the locator on.
#[derive(Clone)]
struct FakeExecutor {
    answer: LocatorAnswer,
    records: Vec<ExecTxRecord>,
}

/// Executor archives with no network behind them. `replays` counts the
/// replays across every executor.
#[derive(Clone, Default)]
struct FakeArchive {
    executors: Vec<FakeExecutor>,
    replays: Arc<AtomicUsize>,
}

impl ExecArchiveSeed for FakeArchive {
    type Archive = Self;

    fn build(self) -> Self {
        self
    }
}

impl ExecArchive for FakeArchive {
    fn executors(&self) -> usize {
        self.executors.len()
    }

    fn locate(
        &mut self,
        executor: usize,
        _index: u64,
        _tx_hash: B256,
    ) -> Result<LocatorAnswer, ExecFetchError> {
        Ok(self.executors[executor].answer.clone())
    }

    fn replay(
        &mut self,
        at: &ArchiveLocator,
        sink: impl FnMut(ExecTxRecord),
    ) -> Result<u64, ExecFetchError> {
        self.replays.fetch_add(1, Ordering::SeqCst);
        let executor = self
            .executors
            .iter()
            .find(|e| e.answer == LocatorAnswer::Located(at.clone()))
            .expect("a replay names a located executor");
        executor.records.iter().cloned().for_each(sink);
        Ok(u64::try_from(executor.records.len()).expect("a small count"))
    }
}

impl FakeExecutor {
    fn located(archive_id: &str, records: Vec<ExecTxRecord>) -> Self {
        Self {
            answer: LocatorAnswer::Located(ArchiveLocator {
                archive_id: archive_id.into(),
                session_id: 7,
                position: 0,
            }),
            records,
        }
    }

    fn answering(answer: LocatorAnswer) -> Self {
        Self {
            answer,
            records: Vec::new(),
        }
    }
}

/// The transactions of one test chain, with their canonical references
/// and their honest executor records.
struct Chain {
    signer: PrivateKeySigner,
}

impl Chain {
    fn new() -> Self {
        Self {
            signer: PrivateKeySigner::random(),
        }
    }

    fn envelope(&self, nonce: u64) -> TxEnvelope {
        crate::actor::test_support::legacy(&self.signer, Address::from([0x22u8; 20]), nonce, 1)
    }

    /// The canonical reference of the transaction at `index`.
    fn tx_ref(&self, index: u64) -> TxRef {
        let env = self.envelope(index);
        TxRef::new(env.tx_hash, 0, BPosition::from_index(index), 0)
    }

    /// The honest record of the transaction at `index`.
    fn record(&self, index: u64) -> ExecTxRecord {
        ExecTxRecord {
            index,
            tx_ref: self.tx_ref(index),
            envelope: self.envelope(index),
        }
    }

    /// A record at `index` whose bytes do not hash to the canonical hash.
    fn forged(&self, index: u64) -> ExecTxRecord {
        let mut record = self.record(index);
        let mut raw = record.envelope.raw_tx.to_vec();
        raw.push(0x00);
        record.envelope.raw_tx = Bytes::from(raw);
        record
    }

    /// The canonical order from `start`: a `TxRef` at each of the `count`
    /// indices.
    fn order(&self, start: u64, count: u64) -> VotingSub {
        let refs = (start..start + count)
            .map(|i| TxOrderingMessage::TxRef(self.tx_ref(i)))
            .collect();
        VotingSub::new(start, refs)
    }
}

/// A reader config whose live wait is `live`.
fn cfg(live: Duration) -> ReaderConfig {
    ReaderConfig {
        join_refetch_after: live,
        ..ReaderConfig::default()
    }
}

/// A run of the `tx_ordering` reader over the executor stream source.
struct Run {
    feed: Sender<ExecTxRecord>,
    exec: Receiver<ReaderToExec>,
    reader: std::thread::JoinHandle<Result<(), ExecutorError>>,
    feeds: Vec<crate::reader::FeedHandle>,
}

impl Run {
    /// Start the source and the reader. `live` records reach the feed
    /// before the reader starts.
    fn start<O: TxOrderingSubscription + 'static>(
        order: O,
        live: Vec<ExecTxRecord>,
        archive: FakeArchive,
        cfg: ReaderConfig,
    ) -> Self {
        let (feed, rx) = unbounded();
        for record in live {
            feed.send(record).expect("the feed is open");
        }
        let SourceStart { feeds, seed } = ExecStreamSource::new(ChanFeed(rx), archive).start();
        let (exec_out, exec) = unbounded();
        let reader = TxOrderingReader::spawn(TxOrderingInputs {
            sub: order,
            cfg,
            exec_out,
            exec_stream: NoExecStream,
            join: seed,
        });
        Self {
            feed,
            exec,
            reader,
            feeds,
        }
    }

    /// Join the reader, close the feed, and return the reader's outcome
    /// and every message the exec thread got.
    fn finish(self) -> (Result<(), ExecutorError>, Vec<ReaderToExec>) {
        let outcome = self.reader.join().expect("no panic");
        drop_sender(self.feed);
        self.feeds
            .into_iter()
            .for_each(|h| h.join().expect("no panic").expect("a clean feed"));
        (outcome, self.exec.try_iter().collect())
    }
}

/// Close the feed, so its thread ends.
fn drop_sender(_feed: Sender<ExecTxRecord>) {}

/// The transactions the exec thread got, as `(index, tx_hash)`.
fn txs(out: &[ReaderToExec]) -> Vec<(u64, B256)> {
    out.iter()
        .filter_map(|msg| match msg {
            ReaderToExec::Tx {
                position, envelope, ..
            } => Some((position.as_index(), envelope.tx_hash)),
            _ => None,
        })
        .collect()
}

#[test]
fn the_buffer_keeps_one_of_three_identical_copies() {
    let chain = Chain::new();
    let buffer = RecordBuffer::default();
    (0..3).for_each(|_| buffer.insert(chain.record(4)));
    let copies = buffer
        .take(4, Duration::ZERO)
        .expect("the record is buffered");
    assert_eq!(copies, vec![chain.record(4)]);
}

#[test]
fn three_copies_of_each_record_reach_the_exec_thread_once() {
    let chain = Chain::new();
    let live = (0..3)
        .flat_map(|i| [chain.record(i), chain.record(i), chain.record(i)])
        .collect();
    let run = Run::start(
        chain.order(0, 3),
        live,
        FakeArchive::default(),
        cfg(Duration::from_secs(5)),
    );
    let (outcome, out) = run.finish();
    outcome.expect("a clean close");
    let want: Vec<(u64, B256)> = (0..3).map(|i| (i, chain.tx_ref(i).tx_hash)).collect();
    assert_eq!(txs(&out), want);
}

#[test]
fn a_copy_that_fails_the_check_drops_and_the_good_copy_wins() {
    let chain = Chain::new();
    let canonical = Canonical {
        index: 2,
        tx_ref: &chain.tx_ref(2),
    };
    assert_eq!(canonical.reject(&chain.forged(2)), Some(Reject::Hash));
    let mut wrong_ref = chain.record(2);
    wrong_ref.tx_ref.shard_id = 3;
    assert_eq!(canonical.reject(&wrong_ref), Some(Reject::TxRef));
    assert_eq!(canonical.reject(&chain.record(2)), None);

    let run = Run::start(
        chain.order(2, 1),
        vec![chain.forged(2), chain.record(2)],
        FakeArchive::default(),
        cfg(Duration::from_secs(5)),
    );
    let (outcome, out) = run.finish();
    outcome.expect("a clean close");
    assert_eq!(txs(&out), vec![(2, chain.tx_ref(2).tx_hash)]);
}

#[test]
fn a_gap_in_the_live_stream_fills_from_an_executor_archive() {
    let chain = Chain::new();
    let archive = FakeArchive {
        executors: vec![
            FakeExecutor::answering(LocatorAnswer::NotReached),
            FakeExecutor::located("executor-1", (5..8).map(|i| chain.record(i)).collect()),
        ],
        ..FakeArchive::default()
    };
    let replays = archive.replays.clone();
    // The live stream lost 5 and 6, and delivered 7.
    let run = Run::start(
        chain.order(5, 3),
        vec![chain.record(7)],
        archive,
        cfg(Duration::from_millis(50)),
    );
    let (outcome, out) = run.finish();
    outcome.expect("a clean close");
    let want: Vec<(u64, B256)> = (5..8).map(|i| (i, chain.tx_ref(i).tx_hash)).collect();
    assert_eq!(txs(&out), want);
    assert_eq!(
        replays.load(Ordering::SeqCst),
        1,
        "one replay delivers the run of records"
    );
}

#[test]
fn a_void_drops_the_entry_and_the_consumer_never_votes() {
    let chain = Chain::new();
    let lost = chain.tx_ref(0);
    let order = VotingSub::new(
        0,
        vec![
            TxOrderingMessage::TxRef(lost),
            TxOrderingMessage::TxRef(chain.tx_ref(1)),
            TxOrderingMessage::BoundaryStart(BlockBoundaryStart {
                block_number: 1,
                end_tx_idx: BPosition::from_index(2),
                l2_timestamp: 1_700_000_000,
                l1_origin: 0,
            }),
            TxOrderingMessage::Void(VoidRecord {
                index: 0,
                tx_hash: lost.tx_hash,
            }),
        ],
    );
    let votes = order.votes.clone();
    let archive = FakeArchive {
        executors: vec![FakeExecutor::answering(LocatorAnswer::NotHeld); 3],
        ..FakeArchive::default()
    };
    let run = Run::start(
        order,
        vec![chain.record(1)],
        archive,
        ReaderConfig {
            voter_id: Some(3),
            ..cfg(Duration::from_millis(50))
        },
    );
    let (outcome, out) = run.finish();
    outcome.expect("a clean close");
    let slots: Vec<(char, u64)> = out
        .iter()
        .map(|msg| match msg {
            ReaderToExec::Vacant { position } => ('V', position.as_index()),
            ReaderToExec::Tx { position, .. } => ('T', position.as_index()),
            ReaderToExec::Boundary(b) => ('B', b.block_number),
            other => panic!("unexpected message {other:?}"),
        })
        .collect();
    assert_eq!(slots, vec![('V', 0), ('T', 1), ('B', 1), ('V', 2)]);
    assert_eq!(*votes.lock().unwrap(), vec![], "the consumer never votes");
}

#[test]
fn a_restart_refetches_its_cursor_range_without_the_live_wait() {
    let chain = Chain::new();
    let start = 100;
    // The live stream is at the head, far past the cursor.
    let head = start + 10_000;
    let live = (head..head + 8).map(|i| chain.record(i)).collect();
    let archive = FakeArchive {
        executors: vec![FakeExecutor::located(
            "executor-0",
            (start..start + 4).map(|i| chain.record(i)).collect(),
        )],
        ..FakeArchive::default()
    };
    let replays = archive.replays.clone();
    let began = Instant::now();
    let run = Run::start(
        chain.order(start, 4),
        live,
        archive,
        cfg(Duration::from_secs(30)),
    );
    let (outcome, out) = run.finish();
    outcome.expect("a clean close");
    let want: Vec<(u64, B256)> = (start..start + 4)
        .map(|i| (i, chain.tx_ref(i).tx_hash))
        .collect();
    assert_eq!(txs(&out), want);
    assert_eq!(replays.load(Ordering::SeqCst), 1);
    assert!(
        began.elapsed() < Duration::from_secs(10),
        "a live head far ahead ends the live wait at once"
    );
}

#[test]
fn every_archive_with_a_mismatched_record_stops_the_reader() {
    let chain = Chain::new();
    let archive = FakeArchive {
        executors: (0..3)
            .map(|k| FakeExecutor::located(&format!("executor-{k}"), vec![chain.forged(9)]))
            .collect(),
        ..FakeArchive::default()
    };
    let run = Run::start(
        chain.order(9, 1),
        vec![chain.forged(9)],
        archive,
        cfg(Duration::from_millis(50)),
    );
    let (outcome, out) = run.finish();
    assert_eq!(txs(&out), vec![]);
    match outcome {
        Err(ExecutorError::ExecRecordMismatch {
            index,
            tx_hash,
            executors,
        }) => {
            assert_eq!((index, executors), (9, 3));
            assert_eq!(tx_hash, chain.tx_ref(9).tx_hash);
        }
        other => panic!("expected ExecRecordMismatch, got {other:?}"),
    }
}

#[test]
fn a_mismatch_with_one_executor_unanswered_waits() {
    let chain = Chain::new();
    let lost = chain.tx_ref(0);
    let order = VotingSub::new(
        0,
        vec![
            TxOrderingMessage::TxRef(lost),
            TxOrderingMessage::Void(VoidRecord {
                index: 0,
                tx_hash: lost.tx_hash,
            }),
        ],
    );
    let archive = FakeArchive {
        executors: vec![
            FakeExecutor::located("executor-0", vec![chain.forged(0)]),
            FakeExecutor::located("executor-1", vec![chain.forged(0)]),
            FakeExecutor::answering(LocatorAnswer::NotReached),
        ],
        ..FakeArchive::default()
    };
    let run = Run::start(order, vec![], archive, cfg(Duration::from_millis(50)));
    let (outcome, out) = run.finish();
    outcome.expect("no stop while one executor has not answered");
    assert!(matches!(
        out.as_slice(),
        [ReaderToExec::Vacant { .. }, ReaderToExec::Vacant { .. }]
    ));
}

#[test]
fn a_peer_answer_maps_to_the_consumer_answer() {
    use kardamom_state::ExecLocatorAnswer;
    let located = ExecLocatorAnswer::Located {
        archive_id: "executor-1".into(),
        session_id: -5,
        position: 4096,
    };
    assert_eq!(
        LocatorAnswer::from(located),
        LocatorAnswer::Located(ArchiveLocator {
            archive_id: "executor-1".into(),
            session_id: -5,
            position: 4096,
        })
    );
    for (peer, want) in [
        (ExecLocatorAnswer::NotHeld, LocatorAnswer::NotHeld),
        (ExecLocatorAnswer::NotReached, LocatorAnswer::NotReached),
        (ExecLocatorAnswer::Lost, LocatorAnswer::Lost),
    ] {
        assert_eq!(LocatorAnswer::from(peer), want);
    }
    assert!(LocatorClient::new(&["executor-0:9024".into()], Duration::from_secs(1)).is_err());
    let client = LocatorClient::new(&[], Duration::from_secs(1)).unwrap();
    assert!(client.ask(0, 5, B256::ZERO).is_err(), "no such executor");
}
