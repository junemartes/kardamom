use std::io::Write;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use alloy_primitives::{Address, B256};
use bytes::Bytes;
use crossbeam_channel::{Sender, bounded};
use kardamom_cluster_adapter::gateway::fakes::{FakeEgress, FakeIngress};
use kardamom_engine::ExecStreamItem;
use kardamom_engine::reader::cluster::ClusterTxOrderingSubscription;
use kardamom_log::error::LogError;
use kardamom_types::{BPosition, ExecTxRecord, TxEnvelope, TxRef};
use rkyv::util::AlignedVec;
use tokio_util::sync::CancellationToken;

use super::cadence::CursorHandoff;
use super::cursor::RecordedCursor;
use super::locators::{LOCATOR_EVERY, Locator, LocatorLog};
use super::publisher::{ExecStreamPublisher, PublisherInputs, StreamPublications};

const SESSION: i32 = 77;

fn record(index: u64) -> ExecTxRecord {
    let fill = u8::try_from(index % 251).expect("below 251");
    ExecTxRecord {
        index,
        tx_ref: TxRef {
            tx_hash: B256::repeat_byte(fill),
            shard_id: 0,
            tx_data_position: BPosition::from_index(index),
            tx_data_session_id: 0,
        },
        envelope: TxEnvelope {
            correlation_id: index,
            raw_tx: Bytes::from(vec![fill; 40]),
            sender: Address::repeat_byte(fill),
            tx_hash: B256::repeat_byte(fill),
            max_inclusion_block: u64::MAX,
        },
    }
}

/// Publications that keep what they take. The recorded side refuses every
/// offer while `open` is false, and counts the refusals.
#[derive(Clone)]
struct FakePublications {
    open: Arc<AtomicBool>,
    refused: Arc<AtomicUsize>,
    end: Arc<AtomicI64>,
    recorded: Arc<Mutex<Vec<(u64, i64)>>>,
    live: Arc<Mutex<Vec<u64>>>,
}

impl FakePublications {
    fn new(open: bool) -> Self {
        Self {
            open: Arc::new(AtomicBool::new(open)),
            refused: Arc::new(AtomicUsize::new(0)),
            end: Arc::new(AtomicI64::new(0)),
            recorded: Arc::default(),
            live: Arc::default(),
        }
    }

    fn index_of(bytes: &AlignedVec) -> u64 {
        kardamom_log::codec::materialize::<ExecTxRecord>(bytes)
            .expect("an ExecTxRecord")
            .index
    }

    fn recorded_indices(&self) -> Vec<u64> {
        self.recorded
            .lock()
            .expect("lock")
            .iter()
            .map(|(i, _)| *i)
            .collect()
    }
}

impl StreamPublications for FakePublications {
    type CursorIngress = FakeIngress;

    fn session_id(&self) -> i32 {
        SESSION
    }

    fn offer_recorded(&self, bytes: &AlignedVec) -> Result<i64, LogError> {
        if !self.open.load(Ordering::Acquire) {
            self.refused.fetch_add(1, Ordering::AcqRel);
            thread::sleep(Duration::from_millis(1));
            return Err(LogError::Aeron("back pressured".into()));
        }
        let len = i64::try_from(bytes.len()).expect("a small frame");
        let end = self.end.fetch_add(len, Ordering::AcqRel) + len;
        self.recorded
            .lock()
            .expect("lock")
            .push((Self::index_of(bytes), end));
        Ok(end)
    }

    fn offer_live(&self, bytes: &AlignedVec) {
        self.live.lock().expect("lock").push(Self::index_of(bytes));
    }
}

/// A publisher over `pubs` on its own thread, with the reader's channel of
/// `depth` and the position channel.
struct Rig {
    items: Sender<ExecStreamItem>,
    positions: Sender<i64>,
    handle: thread::JoinHandle<anyhow::Result<()>>,
    /// The hand-off of the recorded cursor, until a test starts it.
    cursor: Option<CursorHandoff<FakeIngress>>,
    _dir: tempfile::TempDir,
    path: std::path::PathBuf,
}

impl Rig {
    fn spawn(pubs: &FakePublications, depth: usize) -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("exec_stream").join("locators.log");
        let (items, items_rx) = bounded(depth);
        let (positions, positions_rx) = bounded(16);
        positions.send(0).expect("the start position");
        let (cursor, cursor_sender) = CursorHandoff::new();
        let handle = ExecStreamPublisher::spawn(PublisherInputs {
            items: items_rx,
            positions: positions_rx,
            publications: pubs.clone(),
            locators: LocatorLog::open(&path).expect("open the locator log"),
            stop: CancellationToken::new(),
            cursor: cursor_sender,
        })
        .expect("spawn");
        Self {
            items,
            positions,
            handle,
            cursor: Some(cursor),
            _dir: dir,
            path,
        }
    }

    fn send_records(&self, indices: impl Iterator<Item = u64>) {
        indices.for_each(|i| {
            self.items
                .send(ExecStreamItem::Record(record(i)))
                .expect("record");
            self.items.send(ExecStreamItem::Passed(i)).expect("mark");
        });
    }

    /// Start the recorded cursor of executor 2 on `ingress`, or end the
    /// hand-off with none.
    fn start_cursor(&mut self, ingress: Option<&FakeIngress>) {
        let publisher = ingress.map(|ingress| {
            ClusterTxOrderingSubscription::new(FakeEgress::new())
                .with_ingress(ingress.clone())
                .recorded_cursor_publisher(2)
        });
        self.cursor.take().expect("one start").start(publisher);
    }

    /// Close the reader side, join the publisher, and reopen the locator
    /// log it wrote. The recorder stays up until the publisher ended.
    fn finish(self) -> LocatorLog {
        let Self {
            items,
            positions,
            handle,
            _dir,
            path,
            ..
        } = self;
        Self::hang_up(items);
        handle.join().expect("no panic").expect("a clean end");
        Self::hang_up(positions);
        LocatorLog::open(&path).expect("reopen")
    }

    /// End the recorder while the reader stays, and return the publisher's
    /// outcome.
    fn end_recorder(self) -> anyhow::Result<()> {
        let Self {
            items,
            positions,
            handle,
            ..
        } = self;
        Self::hang_up(positions);
        let outcome = handle.join().expect("no panic");
        Self::hang_up(items);
        outcome
    }

    /// The named point at which one side hangs up.
    fn hang_up<T>(_sender: Sender<T>) {}
}

#[test]
fn records_leave_in_the_order_the_reader_sends_them() {
    let pubs = FakePublications::new(true);
    let rig = Rig::spawn(&pubs, 8);
    rig.send_records(0..100);
    rig.finish();
    let expected: Vec<u64> = (0..100).collect();
    assert_eq!(pubs.recorded_indices(), expected);
    assert_eq!(*pubs.live.lock().expect("lock"), expected);
}

#[test]
fn back_pressure_blocks_the_sink_and_drops_nothing() {
    let pubs = FakePublications::new(false);
    let rig = Rig::spawn(&pubs, 2);
    let sent = Arc::new(AtomicUsize::new(0));
    let reader = {
        let items = rig.items.clone();
        let sent = Arc::clone(&sent);
        thread::spawn(move || {
            (0..20u64).for_each(|i| {
                items.send(ExecStreamItem::Record(record(i))).expect("send");
                sent.fetch_add(1, Ordering::AcqRel);
            });
        })
    };
    thread::sleep(Duration::from_millis(200));
    assert!(
        sent.load(Ordering::Acquire) <= 3,
        "the full channel blocks the reader"
    );
    assert!(
        pubs.refused.load(Ordering::Acquire) > 0,
        "the publisher retries"
    );
    assert_eq!(pubs.recorded_indices(), [] as [u64; 0]);
    pubs.open.store(true, Ordering::Release);
    reader.join().expect("the reader ends");
    rig.finish();
    assert_eq!(pubs.recorded_indices(), (0..20).collect::<Vec<u64>>());
}

#[test]
fn the_publisher_writes_a_locator_for_the_session_start_and_every_1024_records() {
    let pubs = FakePublications::new(true);
    let rig = Rig::spawn(&pubs, 64);
    let count = LOCATOR_EVERY * 2 + 5;
    rig.send_records(10..10 + count);
    let log = rig.finish();
    let ends = pubs.recorded.lock().expect("lock").clone();
    let start_of = |n: usize| n.checked_sub(1).map_or(0, |prev| ends[prev].1);
    let first = Locator {
        index: 10,
        session_id: SESSION,
        position: 0,
    };
    assert_eq!(log.lookup(10), Some(first));
    assert_eq!(log.lookup(10 + LOCATOR_EVERY - 1), Some(first));
    let second = log.lookup(10 + LOCATOR_EVERY).expect("the second entry");
    let n = usize::try_from(LOCATOR_EVERY).expect("small");
    assert_eq!(second.position, start_of(n));
    assert_eq!(log.lookup(9), None);
}

#[test]
fn the_locator_log_survives_a_torn_tail() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("locators.log");
    let entries: Vec<Locator> = (0..3)
        .map(|i| Locator {
            index: i * LOCATOR_EVERY,
            session_id: SESSION,
            position: i64::try_from(i).expect("small") * 4096,
        })
        .collect();
    let mut log = LocatorLog::open(&path).expect("open");
    for &l in &entries {
        log.append(l).expect("append");
    }
    // A crash in the middle of the next append leaves 10 bytes of it.
    std::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .and_then(|mut f| f.write_all(&[0xAB; 10]))
        .expect("tear the tail");
    let mut log = LocatorLog::open(&path).expect("reopen");
    assert_eq!(std::fs::metadata(&path).expect("stat").len(), 72);
    assert_eq!(log.lookup(u64::MAX), Some(entries[2]));
    let next = Locator {
        index: 3 * LOCATOR_EVERY,
        session_id: SESSION,
        position: 3 * 4096,
    };
    log.append(next).expect("append after the cut");
    let log = LocatorLog::open(&path).expect("reopen again");
    assert_eq!(log.lookup(u64::MAX), Some(next));
    assert_eq!(log.lookup(3 * LOCATOR_EVERY - 1), Some(entries[2]));
}

#[test]
fn a_corrupt_tail_entry_is_cut_and_the_lookup_takes_the_previous_entry() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("locators.log");
    let mut log = LocatorLog::open(&path).expect("open");
    let good = Locator {
        index: 0,
        session_id: SESSION,
        position: 0,
    };
    log.append(good).expect("append");
    log.append(Locator {
        index: 1024,
        session_id: SESSION,
        position: 9000,
    })
    .expect("append");
    let mut bytes = std::fs::read(&path).expect("read");
    bytes[30] ^= 0xFF;
    std::fs::write(&path, &bytes).expect("flip a bit");
    let log = LocatorLog::open(&path).expect("reopen");
    assert_eq!(log.lookup(2000), Some(good));
}

#[test]
fn a_lookup_returns_the_newest_entry_at_or_below_the_index() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut log = LocatorLog::open(&dir.path().join("locators.log")).expect("open");
    let at = |index, session_id, position| Locator {
        index,
        session_id,
        position,
    };
    // The first session reaches 2048. A restart resumes at 900 in a new
    // session, below the last entry of the first one.
    let entries = [
        at(0, 1, 0),
        at(1024, 1, 70_000),
        at(2048, 1, 140_000),
        at(900, 2, 0),
    ];
    for l in entries {
        log.append(l).expect("append");
    }
    assert_eq!(log.lookup(899), Some(at(0, 1, 0)));
    assert_eq!(log.lookup(950), Some(at(900, 2, 0)));
    assert_eq!(log.lookup(5000), Some(at(900, 2, 0)));
}

#[test]
fn the_recorded_cursor_never_passes_the_recording_position() {
    let mut cursor = RecordedCursor::new(100);
    // Nothing is offered: a mark passes at once.
    assert_eq!(cursor.passed(4), Some(4));
    cursor.offered(200);
    assert_eq!(cursor.passed(5), None, "record 5 ends past the recording");
    cursor.offered(300);
    assert_eq!(cursor.passed(6), None);
    // A mark with no new record waits for the record before it.
    assert_eq!(cursor.passed(7), None);
    assert_eq!(cursor.recorded(199), None);
    assert_eq!(cursor.through(), Some(4));
    assert_eq!(cursor.recorded(250), Some(5));
    // A late, lower report does not move the recording position back.
    assert_eq!(cursor.recorded(120), None);
    assert_eq!(cursor.recorded(300), Some(7));
    assert_eq!(cursor.passed(8), Some(8));
}

#[test]
fn a_recorder_end_makes_the_publisher_fail() {
    let pubs = FakePublications::new(true);
    let rig = Rig::spawn(&pubs, 8);
    rig.send_records(0..3);
    let err = rig.end_recorder().expect_err("a lost recording is fatal");
    assert!(err.to_string().contains("recording ended"), "got {err}");
}

#[test]
fn a_recorder_end_ends_a_wait_for_a_refused_record() {
    let pubs = FakePublications::new(false);
    let rig = Rig::spawn(&pubs, 8);
    rig.send_records(0..1);
    let refused = std::iter::repeat_with(|| {
        thread::sleep(Duration::from_millis(5));
        pubs.refused.load(Ordering::Acquire)
    })
    .take(400)
    .any(|n| n > 0);
    assert!(refused, "the publisher waits on the refused record");
    let err = rig
        .end_recorder()
        .expect_err("a lost recording ends the wait");
    assert!(err.to_string().contains("recording ended"), "got {err}");
    assert_eq!(pubs.recorded_indices(), [] as [u64; 0]);
}

/// The cursors that reached `ingress`, from the kind-9 frames.
fn sent_cursors(ingress: &FakeIngress) -> Vec<u64> {
    ingress
        .accepted()
        .iter()
        .map(|frame| u64::from_le_bytes(frame[2..10].try_into().expect("8 bytes")))
        .collect()
}

/// Whether `check` holds within two seconds.
fn eventually(mut check: impl FnMut() -> bool) -> bool {
    std::iter::repeat_with(|| {
        thread::sleep(Duration::from_millis(5));
        check()
    })
    .take(400)
    .any(|held| held)
}

/// The end position of the record with `index` on the recorded
/// publication.
fn end_of(pubs: &FakePublications, index: u64) -> i64 {
    pubs.recorded
        .lock()
        .expect("lock")
        .iter()
        .find(|(i, _)| *i == index)
        .expect("the record is offered")
        .1
}

#[test]
fn the_publisher_sends_only_recorded_cursors_on_the_cadence() {
    let pubs = FakePublications::new(true);
    let ingress = FakeIngress::new();
    let mut rig = Rig::spawn(&pubs, 64);
    rig.start_cursor(Some(&ingress));
    rig.send_records(0..10);
    assert!(eventually(|| pubs.recorded_indices().len() == 10));
    thread::sleep(Duration::from_millis(150));
    assert_eq!(
        sent_cursors(&ingress),
        [] as [u64; 0],
        "the archive has written no record yet"
    );
    rig.positions.send(end_of(&pubs, 4)).expect("position");
    assert!(
        eventually(|| sent_cursors(&ingress) == [4]),
        "the first cursor goes out at once"
    );
    rig.positions.send(end_of(&pubs, 9)).expect("position");
    assert!(
        eventually(|| sent_cursors(&ingress) == [4, 9]),
        "the next cursor goes out on the time cadence: {:?}",
        sent_cursors(&ingress)
    );
    rig.finish();
}

#[test]
fn the_publisher_sends_no_cursor_while_the_cursor_is_off() {
    let pubs = FakePublications::new(true);
    let ingress = FakeIngress::new();
    let mut rig = Rig::spawn(&pubs, 64);
    rig.start_cursor(None);
    rig.send_records(0..10);
    assert!(eventually(|| pubs.recorded_indices().len() == 10));
    rig.positions.send(end_of(&pubs, 9)).expect("position");
    thread::sleep(Duration::from_millis(300));
    assert_eq!(sent_cursors(&ingress), [] as [u64; 0]);
    rig.finish();
}
