//! [`TestStream`]: a canonical stream of a test, and the records that the
//! `tx_ordering` reader thread sends for it from each source.

use std::path::Path;
use std::thread::JoinHandle;
use std::time::Duration;

use crossbeam_channel::{Receiver, Sender, unbounded};
use kardamom_types::{
    BPosition, BlockBoundaryStart, EpochRecord, TxEnvelope, TxOrderingMessage, TxRef,
};

use super::{ChanTxDataSub, ChanTxOrderingSub, EgressReader, TestEgress};
use crate::ExecutorError;
use crate::reader::cluster::ReplayCursor;
use crate::reader::{
    JoinBuffer, NoExecStream, ReaderConfig, ReaderToExec, TxDataReader, TxOrderingInputs,
    TxOrderingReader,
};

/// The longest wait for one record of the reader thread.
const RECORD_WAIT: Duration = Duration::from_secs(10);

/// One entry of a [`TestStream`].
pub enum TestEntry {
    /// A transaction. It takes one canonical index.
    Tx(TxEnvelope),
    /// A boundary. It takes no canonical index.
    Boundary(BlockBoundaryStart),
    /// An L1 epoch. It takes one canonical index for its marker and one
    /// for each deposit.
    Epoch(EpochRecord),
}

impl TestEntry {
    /// The canonical indexes of this entry.
    fn slots(&self) -> u64 {
        match self {
            Self::Tx(_) => 1,
            Self::Boundary(_) => 0,
            Self::Epoch(epoch) => 1 + u64::try_from(epoch.deposits.len()).unwrap(),
        }
    }
}

/// A canonical stream of a test: its entries in canonical order, from
/// genesis. Each transaction is on lane 0.
pub struct TestStream(pub Vec<TestEntry>);

impl TestStream {
    /// The records of the `tx_ordering` reader thread for this stream from
    /// the cluster egress. The reader joins the reference of each
    /// transaction with its envelope on the `tx_data` lane.
    ///
    /// # Panics
    ///
    /// Panics if a reader thread fails.
    #[must_use]
    pub fn read_joined(&self) -> Vec<ReaderToExec> {
        let buffer = JoinBuffer::new();
        TxDataReader::new(
            ChanTxDataSub {
                sequencer_id: 0,
                rx: self.lane(),
            },
            buffer.clone(),
        )
        .spawn()
        .join()
        .expect("the tx_data reader does not panic")
        .expect("the tx_data reader stops when the lane closes");
        OrderingThread::spawn(ChanTxOrderingSub(self.references()), buffer).all()
    }

    /// The records of the `tx_ordering` reader thread for this stream from
    /// an egress, as a binary with `--egress` reads it. The egress runs on
    /// `rt`, with its store in `dir`.
    ///
    /// # Panics
    ///
    /// Panics if a record does not come in time, or if the thread fails.
    #[must_use]
    pub fn read_from_egress(&self, rt: &tokio::runtime::Runtime, dir: &Path) -> Vec<ReaderToExec> {
        let egress = TestEgress::serve(rt, dir, TestEgress::GENESIS, &self.egress_frames());
        let EgressReader { sub, live } = egress.reader(rt, ReplayCursor::new(0, 1));
        OrderingThread::spawn(sub, JoinBuffer::new()).take(self.0.len(), live)
    }

    /// The frames that an egress serves for this stream.
    #[must_use]
    pub fn egress_frames(&self) -> Vec<Vec<u8>> {
        self.indexed()
            .map(|(index, entry)| match entry {
                TestEntry::Tx(envelope) => TestEgress::tx_frame(index, envelope),
                TestEntry::Boundary(b) => TestEgress::boundary_frame(b),
                TestEntry::Epoch(epoch) => TestEgress::epoch_frame(index, epoch),
            })
            .collect()
    }

    /// The `tx_data` lane: the envelope of each transaction at its
    /// canonical position. The lane closes after the last one.
    fn lane(&self) -> Receiver<(BPosition, TxEnvelope)> {
        let (lane, rx) = unbounded();
        self.indexed()
            .filter_map(|(index, entry)| match entry {
                TestEntry::Tx(envelope) => Some((BPosition::from_index(index), envelope.clone())),
                TestEntry::Boundary(_) | TestEntry::Epoch(_) => None,
            })
            .for_each(|record| lane.send(record).unwrap());
        rx
    }

    /// The cluster egress: the reference of each transaction, and each
    /// boundary. The stream closes after the last entry.
    fn references(&self) -> Receiver<(BPosition, TxOrderingMessage)> {
        let (refs, rx) = unbounded();
        self.indexed()
            .map(|(index, entry)| {
                let position = BPosition::from_index(index);
                let msg = match entry {
                    TestEntry::Tx(envelope) => {
                        TxOrderingMessage::TxRef(TxRef::new(envelope.tx_hash, 0, position, 0))
                    }
                    TestEntry::Boundary(b) => TxOrderingMessage::BoundaryStart(b.clone()),
                    TestEntry::Epoch(epoch) => TxOrderingMessage::Epoch(epoch.clone()),
                };
                (position, msg)
            })
            .for_each(|record| refs.send(record).unwrap());
        rx
    }

    /// Each entry with its canonical index. A boundary has the index of
    /// the next transaction.
    fn indexed(&self) -> impl Iterator<Item = (u64, &TestEntry)> {
        self.0.iter().scan(0u64, |next, entry| {
            let index = *next;
            *next = index.checked_add(entry.slots())?;
            Some((index, entry))
        })
    }
}

/// A `tx_ordering` reader thread, as the batcher runs it: it joins each
/// reference with a buffer, and sends its records to a channel.
struct OrderingThread {
    thread: JoinHandle<Result<(), ExecutorError>>,
    records: Receiver<ReaderToExec>,
}

impl OrderingThread {
    /// Spawn the thread over `sub`. It joins each reference with `buffer`.
    fn spawn<O>(sub: O, buffer: JoinBuffer) -> Self
    where
        O: crate::TxOrderingSubscription + 'static,
    {
        let (exec_out, records) = unbounded();
        let thread = TxOrderingReader::spawn(TxOrderingInputs {
            sub,
            buffer,
            cfg: ReaderConfig::default(),
            exec_out,
            exec_stream: NoExecStream,
            recovery_factory: None,
        });
        Self { thread, records }
    }

    /// Every record, after the source closes and the thread stops.
    fn all(self) -> Vec<ReaderToExec> {
        Self::stop(self.thread);
        self.records.try_iter().collect()
    }

    /// The first `n` records. Then the live stream `live` of an egress
    /// closes, and the thread stops.
    fn take(self, n: usize, live: Sender<Vec<u8>>) -> Vec<ReaderToExec> {
        let taken = Self::first(&self.records, n, live);
        Self::stop(self.thread);
        taken
    }

    /// The first `n` of `records`. The live stream `_live` closes when this
    /// returns.
    fn first(
        records: &Receiver<ReaderToExec>,
        n: usize,
        _live: Sender<Vec<u8>>,
    ) -> Vec<ReaderToExec> {
        let taken: Vec<_> = std::iter::from_fn(|| records.recv_timeout(RECORD_WAIT).ok())
            .take(n)
            .collect();
        assert_eq!(taken.len(), n, "the reader thread sends {n} records");
        taken
    }

    /// Wait for the thread to stop at the end of its source.
    fn stop(thread: JoinHandle<Result<(), ExecutorError>>) {
        thread
            .join()
            .expect("the tx_ordering reader does not panic")
            .expect("the tx_ordering reader stops when its source closes");
    }
}
