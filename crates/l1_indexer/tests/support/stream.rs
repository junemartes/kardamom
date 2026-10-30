//! The `l1_blocks` and `events` streams between the followers and the
//! da-watcher, with the production wire codec and without Aeron.
//!
//! Every record a follower publishes is encoded once and kept in the
//! stream's recording, in publish order, copies included. A feed reads
//! the live records that follow its subscription; its history is the
//! recording from a block on, in block order, one page at a time, as an
//! archive replay serves it. A follower's lifecycle
//! records reach the board of the consumers through the same `Delivery`
//! channel that the Aeron `events` subscriber feeds in a deployment.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use kardamom_da_watcher::BlockFeed;
use kardamom_l1_indexer::sink::BlockSink;
use kardamom_l1_indexer::{IndexerError, L1Block};
use kardamom_log::codec;
use rkyv::util::AlignedVec;
use tokio::sync::watch;

/// The most records one history read returns.
const HISTORY_PAGE: usize = 256;

/// The frames in publish order, and their places by block number.
#[derive(Default)]
struct Recording {
    frames: Vec<AlignedVec>,
    by_number: BTreeMap<u64, Vec<usize>>,
}

/// The recording of the stream. The followers append on their tasks and
/// the feeds read on the watcher's task, so the recording sits behind a
/// lock; the watch channel wakes a feed when the recording grows.
#[derive(Clone)]
pub(crate) struct Stream {
    frames: Arc<Mutex<Recording>>,
    len: Arc<watch::Sender<usize>>,
}

impl Default for Stream {
    fn default() -> Self {
        Self {
            frames: Arc::default(),
            len: Arc::new(watch::channel(0).0),
        }
    }
}

impl Stream {
    /// Append one encoded record.
    pub(crate) fn append(&self, frame: AlignedVec) {
        let number = codec::materialize::<L1Block>(&frame).unwrap().number;
        let mut recording = self.frames.lock().unwrap();
        let index = recording.frames.len();
        recording.frames.push(frame);
        recording.by_number.entry(number).or_default().push(index);
        self.len.send_replace(recording.frames.len());
    }

    /// The highest block on the stream.
    pub(crate) fn head(&self) -> Option<u64> {
        let recording = self.frames.lock().unwrap();
        recording.by_number.keys().next_back().copied()
    }

    /// One page of the records from block `from` on, in block order,
    /// copies included.
    fn page(&self, from: u64) -> Vec<L1Block> {
        let recording = self.frames.lock().unwrap();
        recording
            .by_number
            .range(from..)
            .flat_map(|(_, places)| places)
            .take(HISTORY_PAGE)
            .map(|place| codec::materialize::<L1Block>(&recording.frames[*place]).unwrap())
            .collect()
    }

    /// How many records the recording holds.
    pub(crate) fn len(&self) -> usize {
        *self.len.borrow()
    }

    /// Every record of the recording, decoded, in publish order.
    pub(crate) fn records(&self) -> Vec<L1Block> {
        let recording = self.frames.lock().unwrap();
        recording
            .frames
            .iter()
            .map(|frame| codec::materialize::<L1Block>(frame).unwrap())
            .collect()
    }

    /// A subscriber that sees the records published from now on.
    pub(crate) fn feed(&self) -> Feed {
        let rx = self.len.subscribe();
        let next = *rx.borrow();
        Feed {
            stream: self.clone(),
            rx,
            next,
        }
    }

    /// The sink of one follower.
    pub(crate) fn sink(&self) -> Sink {
        Sink(self.clone())
    }

    fn frame(&self, index: usize) -> Option<L1Block> {
        let recording = self.frames.lock().unwrap();
        recording
            .frames
            .get(index)
            .map(|frame| codec::materialize::<L1Block>(frame).unwrap())
    }
}

/// A follower's sink: the production codec onto the recording.
pub(crate) struct Sink(Stream);

impl BlockSink for Sink {
    fn publish(&self, block: &L1Block) -> Result<(), IndexerError> {
        let frame = codec::encode(block).map_err(|e| IndexerError::Publish(e.to_string()))?;
        self.0.append(frame);
        Ok(())
    }
}

/// One subscriber: its place in the recording.
pub(crate) struct Feed {
    stream: Stream,
    rx: watch::Receiver<usize>,
    next: usize,
}

#[async_trait]
impl BlockFeed for Feed {
    async fn history(&mut self, from: u64) -> Vec<L1Block> {
        self.stream.page(from)
    }

    async fn next(&mut self) -> Option<L1Block> {
        let next = self.next;
        self.rx.wait_for(|len| *len > next).await.ok()?;
        self.try_next()
    }

    fn try_next(&mut self) -> Option<L1Block> {
        let record = self.stream.frame(self.next)?;
        self.next = self.next.checked_add(1).unwrap();
        Some(record)
    }
}
