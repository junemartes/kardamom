//! The executor stream source: the transaction source of a consumer
//! outside the executors (the validator, the batcher).
//!
//! Every executor publishes each `TxRef` that it joins as an
//! [`ExecTxRecord`] on the live `exec_txs` stream, and records the stream
//! in the archive on its node. A consumer reads one live subscription with
//! one destination for each executor:
//!
//! - **Dedup.** The feed thread keys each record by its canonical index.
//!   The buffer keeps the distinct copies of an index and drops the
//!   repeats, so three identical copies become one.
//! - **Check.** The `tx_ordering` reader takes the copies of index `i` and
//!   accepts the first one whose `tx_ref` equals the canonical `TxRef(i)`
//!   and whose `keccak256(raw_tx)` equals `TxRef(i).tx_hash`. A copy that
//!   fails counts in `kardamom_exec_stream_record_rejected_total{reason}`
//!   and drops. The consumer never trusts an executor's choice of bytes.
//! - **Miss.** When no copy passes, the reader asks each executor
//!   `kardamom_getExecLocator(i, tx_hash)` and replays the archive that
//!   answers `located`. One replay delivers a run of records into the
//!   buffer, so a restart catches up from the archives, not one index at a
//!   time.
//! - **Wait.** The consumer never votes. It drops `i` only when the
//!   canonical order carries `Void(i)`, so it reads ahead in the order
//!   during the wait. The wait has no deadline: a consumer outside the
//!   executors cannot decide an entry, and a restart meets the same entry
//!   again. `kardamom_exec_stream_wait_seconds` shows the wait.
//! - **Mismatch.** When every executor archive holds a record at `i` and
//!   no record passes the check, the reader stops with
//!   [`ExecutorError::ExecRecordMismatch`]. That is an integrity fault,
//!   not an availability fault.

mod archive;
mod copies;
mod locator;

use std::collections::BTreeSet;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use alloy_primitives::keccak256;
use tracing::{error, warn};

use kardamom_types::{ExecTxRecord, TxEnvelope, TxRef};

use crate::error::ExecutorError;
use crate::metrics::{
    EXEC_STREAM_RECORD_REJECTED_TOTAL, EXEC_STREAM_REFETCH_TOTAL, EXEC_STREAM_WAIT_SECONDS,
};

pub use archive::{
    ExecArchive, ExecArchiveSeed, ExecFetchError, LiveExecArchive, LiveExecArchiveSeed,
};
use copies::RecordBuffer;
pub(crate) use locator::LocatorClient;
pub use locator::{ArchiveLocator, LocatorAnswer};

use super::join::ReaderConfig;
use super::ports::TxOrderingSubscription;
use super::source::{FeedHandle, JoinAt, JoinSeed, Joined, SourceStart, TxJoin, TxSource};
use super::void::{MAX_READ_AHEAD, ReadAhead};

/// How long one take of the wait waits for the record, between two reads
/// ahead in the canonical order.
const WAIT_POLL: Duration = Duration::from_millis(100);

/// How often the wait asks the executors again. A locator query is one
/// small HTTP request, and a replay runs only on a `located` answer.
const REASK: Duration = Duration::from_secs(1);

/// The live `exec_txs` subscription of a consumer.
pub trait ExecRecordSubscription: Send + 'static {
    /// The next record. `None` when the subscription closes.
    fn next(&mut self) -> Option<ExecTxRecord>;
}

/// The executor stream source: the live subscription, and the executor
/// archives of the miss path.
pub struct ExecStreamSource<R, A> {
    feed: R,
    archive: A,
}

impl<R, A> ExecStreamSource<R, A> {
    #[must_use]
    pub fn new(feed: R, archive: A) -> Self {
        Self { feed, archive }
    }
}

impl<R: ExecRecordSubscription, A: ExecArchiveSeed> TxSource for ExecStreamSource<R, A> {
    type Seed = ExecStreamSeed<A>;

    fn start(self) -> SourceStart<ExecStreamSeed<A>> {
        let buffer = Arc::new(RecordBuffer::default());
        let feed = ExecStreamFeed {
            sub: self.feed,
            buffer: buffer.clone(),
        }
        .spawn();
        SourceStart {
            feeds: vec![feed],
            seed: ExecStreamSeed {
                buffer,
                archive: self.archive,
            },
        }
    }
}

/// The feed thread: it inserts every live record into the buffer.
struct ExecStreamFeed<R> {
    sub: R,
    buffer: Arc<RecordBuffer>,
}

impl<R: ExecRecordSubscription> ExecStreamFeed<R> {
    fn spawn(self) -> FeedHandle {
        thread::Builder::new()
            .name("exec-stream-feed".into())
            .spawn(move || {
                self.run();
                Ok(())
            })
            .expect("spawn exec_txs feed")
    }

    /// Insert records until the subscription closes.
    fn run(mut self) {
        let buffer = self.buffer;
        std::iter::from_fn(|| self.sub.next()).for_each(|record| buffer.insert(record));
    }
}

/// The buffer and the archive seed, on their way to the reader thread.
pub struct ExecStreamSeed<A> {
    buffer: Arc<RecordBuffer>,
    archive: A,
}

impl<A: ExecArchiveSeed> JoinSeed for ExecStreamSeed<A> {
    type Join = ExecStreamJoin<A::Archive>;

    fn build(self, _cfg: &ReaderConfig) -> Self::Join {
        ExecStreamJoin {
            buffer: self.buffer,
            archive: self.archive.build(),
        }
    }
}

/// The executor stream join on the reader thread.
pub struct ExecStreamJoin<X> {
    buffer: Arc<RecordBuffer>,
    archive: X,
}

impl<X: ExecArchive> TxJoin for ExecStreamJoin<X> {
    fn join<O: TxOrderingSubscription>(
        &mut self,
        at: JoinAt<'_, O>,
    ) -> Result<Joined, ExecutorError> {
        StreamWait::new(self, at).run()
    }
}

/// Why a copy fails the check against the canonical `TxRef`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Reject {
    /// The record's `tx_ref` is not the canonical one.
    TxRef,
    /// The keccak of the record's bytes is not the canonical hash.
    Hash,
}

impl Reject {
    fn id(self) -> &'static str {
        match self {
            Self::TxRef => "tx_ref",
            Self::Hash => "hash",
        }
    }
}

/// The canonical `TxRef` at one index, and the check of a copy against
/// it.
#[derive(Clone, Copy)]
struct Canonical<'a> {
    index: u64,
    tx_ref: &'a TxRef,
}

impl Canonical<'_> {
    fn reject(self, record: &ExecTxRecord) -> Option<Reject> {
        if record.tx_ref != *self.tx_ref {
            return Some(Reject::TxRef);
        }
        (keccak256(&record.envelope.raw_tx) != self.tx_ref.tx_hash).then_some(Reject::Hash)
    }

    /// The first copy that passes the check. Each copy that fails counts
    /// and drops.
    fn accept(self, copies: Vec<ExecTxRecord>) -> Option<TxEnvelope> {
        copies
            .into_iter()
            .find(|record| self.passes(record))
            .map(|record| record.envelope)
    }

    fn passes(self, record: &ExecTxRecord) -> bool {
        let Some(why) = self.reject(record) else {
            return true;
        };
        ::metrics::counter!(EXEC_STREAM_RECORD_REJECTED_TOTAL, "reason" => why.id()).increment(1);
        warn!(
            target: "kardamom_executor::reader",
            index = self.index,
            reason = why.id(),
            tx_hash = ?self.tx_ref.tx_hash,
            "executor stream record fails the check against the canonical TxRef; dropped"
        );
        false
    }
}

/// What one ask of one executor gave.
enum Asked {
    /// A copy that passes the check.
    Good(TxEnvelope),
    /// The executor's archive holds a record at the index, and no copy of
    /// it passes the check.
    Mismatch,
    /// No usable answer now: not reached, not held, lost, a failed query
    /// or replay, or a replay with no record at the index.
    Nothing,
}

/// One wait for the record at one canonical index.
struct StreamWait<'a, O, X> {
    buffer: &'a RecordBuffer,
    archive: &'a mut X,
    canonical: Canonical<'a>,
    join_refetch_after: Duration,
    order: &'a mut O,
    backlog: &'a mut ReadAhead,
    /// Messages this wait read ahead, in canonical order.
    ahead: ReadAhead,
    started: Instant,
    next_ask: Instant,
    /// The executors whose archive holds a record at the index that fails
    /// the check.
    mismatched: BTreeSet<usize>,
}

impl<'a, O: TxOrderingSubscription, X: ExecArchive> StreamWait<'a, O, X> {
    fn new(join: &'a mut ExecStreamJoin<X>, at: JoinAt<'a, O>) -> Self {
        let JoinAt {
            tx_ref,
            position,
            cfg,
            order,
            backlog,
        } = at;
        let now = Instant::now();
        Self {
            buffer: &join.buffer,
            archive: &mut join.archive,
            canonical: Canonical {
                index: position.as_index(),
                tx_ref,
            },
            join_refetch_after: cfg.join_refetch_after,
            order,
            backlog,
            ahead: ReadAhead::new(),
            started: now,
            next_ask: now,
            mismatched: BTreeSet::new(),
        }
    }

    /// Wait for the live record first. On a miss, wait without a
    /// deadline: refetch from the archives, and read ahead for the void
    /// record. On return, the backlog holds every message read, in
    /// canonical order.
    fn run(mut self) -> Result<Joined, ExecutorError> {
        if let Some(env) = self.take(self.join_refetch_after) {
            return Ok(Joined::Tx(env));
        }
        warn!(
            target: "kardamom_executor::reader",
            index = self.canonical.index,
            tx_hash = ?self.canonical.tx_ref.tx_hash,
            "executor stream miss: refetching from the executor archives; \
             the reader waits for the record or a void record"
        );
        self.next_ask = Instant::now();
        let outcome = loop {
            match self.step() {
                Ok(None) => (),
                Ok(Some(joined)) => break Ok(joined),
                Err(e) => break Err(e),
            }
        };
        ::metrics::gauge!(EXEC_STREAM_WAIT_SECONDS).set(0.0);
        self.ahead.append(self.backlog);
        *self.backlog = self.ahead;
        outcome
    }

    /// One step of the wait: the archives when an ask is due, then one
    /// take, then one read ahead.
    fn step(&mut self) -> Result<Option<Joined>, ExecutorError> {
        ::metrics::gauge!(EXEC_STREAM_WAIT_SECONDS).set(self.started.elapsed().as_secs_f64());
        if Instant::now() >= self.next_ask {
            if let Some(env) = self.ask_all()? {
                return Ok(Some(Joined::Tx(env)));
            }
            self.next_ask = Instant::now() + REASK;
        }
        if let Some(env) = self.take(WAIT_POLL) {
            return Ok(Some(Joined::Tx(env)));
        }
        self.read_ahead()
    }

    /// Take the copies of the index from the buffer, and keep the first
    /// one that passes the check.
    fn take(&self, timeout: Duration) -> Option<TxEnvelope> {
        let copies = self.buffer.take(self.canonical.index, timeout)?;
        self.canonical.accept(copies)
    }

    /// Ask every executor in turn, and stop at the first good copy.
    ///
    /// # Errors
    ///
    /// Returns [`ExecutorError::ExecRecordMismatch`] when every executor's
    /// archive holds a record at the index and none passes the check.
    fn ask_all(&mut self) -> Result<Option<TxEnvelope>, ExecutorError> {
        let executors = self.archive.executors();
        let good = (0..executors).find_map(|executor| match self.ask(executor) {
            Asked::Good(env) => Some(env),
            Asked::Mismatch => {
                self.mismatched.insert(executor);
                None
            }
            Asked::Nothing => None,
        });
        if good.is_some() || executors == 0 || self.mismatched.len() < executors {
            return Ok(good);
        }
        error!(
            target: "kardamom_executor::reader",
            index = self.canonical.index,
            tx_hash = ?self.canonical.tx_ref.tx_hash,
            executors,
            "every executor archive holds a record at the index that fails the check"
        );
        Err(ExecutorError::ExecRecordMismatch {
            index: self.canonical.index,
            tx_hash: self.canonical.tx_ref.tx_hash,
            executors,
        })
    }

    /// Ask one executor where its archive holds the index, and replay it.
    /// Records at other indices go into the buffer.
    fn ask(&mut self, executor: usize) -> Asked {
        let canonical = self.canonical;
        let answer = self
            .archive
            .locate(executor, canonical.index, canonical.tx_ref.tx_hash);
        let at = match answer {
            Ok(LocatorAnswer::Located(at)) => at,
            Ok(other) => return Self::count(other.label(), Asked::Nothing),
            Err(e) => {
                warn!(target: "kardamom_executor::reader", executor, error = %e, "locator query failed");
                return Self::count("no_answer", Asked::Nothing);
            }
        };
        let buffer = self.buffer;
        let mut copies = Vec::new();
        let replayed = self.archive.replay(&at, |record| {
            if record.index == canonical.index {
                copies.push(record);
            } else {
                buffer.insert(record);
            }
        });
        if let Err(e) = replayed {
            warn!(target: "kardamom_executor::reader", executor, error = %e, "executor archive replay failed");
            return Self::count("replay_failed", Asked::Nothing);
        }
        if copies.is_empty() {
            return Self::count("absent", Asked::Nothing);
        }
        match canonical.accept(copies) {
            Some(env) => Self::count("located", Asked::Good(env)),
            None => Self::count("mismatch", Asked::Mismatch),
        }
    }

    /// Count one ask by its outcome.
    fn count(outcome: &'static str, asked: Asked) -> Asked {
        ::metrics::counter!(EXEC_STREAM_REFETCH_TOTAL, "outcome" => outcome).increment(1);
        asked
    }

    /// Read one message ahead in the canonical order, and end the wait on
    /// the void record of the index. At the read-ahead bound the wait only
    /// takes and asks: the sealer refuses a vote for an entry more than
    /// one void window behind its head, so no void record comes later.
    /// The step then sleeps one poll, because a take can end at once when
    /// the live head is far ahead.
    fn read_ahead(&mut self) -> Result<Option<Joined>, ExecutorError> {
        if self.ahead.len() >= MAX_READ_AHEAD {
            thread::sleep(WAIT_POLL);
            return Ok(None);
        }
        let read = match self.backlog.pop_front() {
            Some(read) => Ok(read),
            None => self.order.next(),
        };
        let (position, msg) = match read {
            Ok(read) => read,
            Err(ExecutorError::TxOrderingClosed) => return Ok(Some(Joined::Closed)),
            Err(e) => return Err(e),
        };
        let voided = msg.as_void().is_some_and(|void| {
            void.index == self.canonical.index && void.tx_hash == self.canonical.tx_ref.tx_hash
        });
        self.ahead.push_back((position, msg));
        Ok(voided.then_some(Joined::Voided))
    }
}

#[cfg(test)]
#[path = "exec_stream/tests.rs"]
mod tests;
