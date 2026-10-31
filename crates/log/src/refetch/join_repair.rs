//! The repair of a late join on one `tx_data` lane.
//!
//! A publisher of a lane is connected as soon as one subscriber joins it.
//! The ingress records each of its lanes through its own archive, so a
//! restarted ingress publishes as soon as its archive joins. A subscriber
//! on another node that joins the new publication later starts at the
//! tail: it never sees the first frames of the publication.
//!
//! [`JoinRepair`] finds such a join: the join marker of a new image, or
//! the first frame of a new publisher session, that is not at the start of
//! the session. The marker matters when no frame follows the join: a
//! client that waits for its first transaction sends nothing more. The
//! repair then refetches the frames from the start of the session up to
//! the join from the ingress archives, and gives them before the live
//! frames, in order. The subscriber then reads the session as if it had
//! joined at its start.
//!
//! The repair has two bounds:
//! - A session that the lane first sees in its start grace is not
//!   repaired. A subscriber that starts joins each running session late,
//!   by design: the frames before its start are older than its own state.
//! - A gap above `max_gap` bytes is not repaired. Such a session ran for a
//!   long time before the join, so it is not a new publication.
//!
//! A refetch that fails loses the frames, as a late join did before the
//! repair. Each outcome counts in [`JOIN_REPAIRS_TOTAL`].

use std::collections::{HashSet, VecDeque};
use std::time::{Duration, Instant};

use kardamom_types::{BPosition, TxDataLoc, TxEnvelope};
use tracing::{info, warn};

use super::ArchiveRefetcher;
use crate::aeron_live::{ImageJoin, PositionedTxData, TxDataItem};
use crate::error::LogError;

/// The late joins of `tx_data` lanes, by outcome: `repaired`, `failed`,
/// `start` (a session of the start grace) and `too_large`.
pub const JOIN_REPAIRS_TOTAL: &str = "kardamom_log_tx_data_join_repairs_total";

/// The refetch of one publisher session from one position, to the end of
/// its recording.
pub trait SessionFetch {
    /// Give each recorded frame of `session_id` on `stream_id` from `from`
    /// to `sink`, in order. Return the count of frames.
    ///
    /// # Errors
    ///
    /// Returns an error when no archive gives the range.
    fn fetch(
        &mut self,
        stream_id: i32,
        session_id: i32,
        from: BPosition,
        sink: impl FnMut(TxDataLoc, TxEnvelope),
    ) -> Result<u64, LogError>;
}

impl SessionFetch for ArchiveRefetcher {
    fn fetch(
        &mut self,
        stream_id: i32,
        session_id: i32,
        from: BPosition,
        sink: impl FnMut(TxDataLoc, TxEnvelope),
    ) -> Result<u64, LogError> {
        self.fetch_tx_data(stream_id, session_id, from, sink)
    }
}

/// A refetch client on a thread of its own, for a reader that must be
/// `Send`: the Aeron resources of an [`ArchiveRefetcher`] are bound to
/// the thread that made them. The reader sends each request and waits for
/// its frames. The thread ends when the worker drops.
pub struct RefetchWorker {
    requests: crossbeam_channel::Sender<FetchRequest>,
}

/// One request to a [`RefetchWorker`], with the channel of its answer.
struct FetchRequest {
    stream_id: i32,
    session_id: i32,
    from: BPosition,
    answer: crossbeam_channel::Sender<Result<Vec<(TxDataLoc, TxEnvelope)>, LogError>>,
}

impl RefetchWorker {
    /// Start the thread of the worker. Its refetch client makes no Aeron
    /// resource before the first request.
    #[must_use]
    pub fn spawn(cfg: super::RefetchConfig) -> Self {
        let (requests, rx) = crossbeam_channel::unbounded::<FetchRequest>();
        std::thread::spawn(move || {
            let mut refetcher = ArchiveRefetcher::new(cfg);
            for request in rx {
                let mut frames = Vec::new();
                let result = refetcher.fetch_tx_data(
                    request.stream_id,
                    request.session_id,
                    request.from,
                    |loc, envelope| frames.push((loc, envelope)),
                );
                let _ = request.answer.send(result.map(|_| frames));
            }
        });
        Self { requests }
    }
}

impl SessionFetch for RefetchWorker {
    fn fetch(
        &mut self,
        stream_id: i32,
        session_id: i32,
        from: BPosition,
        mut sink: impl FnMut(TxDataLoc, TxEnvelope),
    ) -> Result<u64, LogError> {
        let gone = || LogError::Aeron("the refetch worker stopped".into());
        let (answer, reply) = crossbeam_channel::bounded(1);
        self.requests
            .send(FetchRequest {
                stream_id,
                session_id,
                from,
                answer,
            })
            .map_err(|_| gone())?;
        let frames = reply.recv().map_err(|_| gone())??;
        let count = frames.len() as u64;
        for (loc, envelope) in frames {
            sink(loc, envelope);
        }
        Ok(count)
    }
}

/// The repair of the late joins of one lane.
pub struct JoinRepair {
    stream_id: i32,
    /// The publisher sessions that the lane has seen.
    seen: HashSet<i32>,
    /// A session that the lane first sees before this instant is a session
    /// of the start.
    armed_at: Instant,
    max_gap: u64,
    /// The frames to give, oldest first.
    ready: VecDeque<PositionedTxData>,
}

impl JoinRepair {
    /// The repair of lane stream `stream_id`, armed `grace` from now.
    #[must_use]
    pub fn new(stream_id: i32, grace: Duration, max_gap: u64) -> Self {
        Self {
            stream_id,
            seen: HashSet::new(),
            armed_at: Instant::now() + grace,
            max_gap,
            ready: VecDeque::new(),
        }
    }

    /// Take one item of the lane. The join of a new session that is not
    /// at its start first queues the refetched frames of the session up to
    /// the join. A join comes as a marker, or as the first frame of the
    /// session when the frame comes first.
    pub fn take(&mut self, item: TxDataItem, fetch: &mut impl SessionFetch) {
        match item {
            TxDataItem::Joined(join) => self.joined(join, fetch),
            TxDataItem::Frame(frame) => {
                self.joined(Self::join_of(&frame), fetch);
                self.ready.push_back(frame);
            }
        }
    }

    /// The join that the first frame of a session would be.
    fn join_of(frame: &PositionedTxData) -> ImageJoin {
        ImageJoin {
            session_id: frame.loc.session_id,
            join: frame.loc.position,
            stream_position: frame.stream_position,
            session_start: frame.session_start,
        }
    }

    /// Repair `join` when it is the first of its session and not at its
    /// start.
    fn joined(&mut self, join: ImageJoin, fetch: &mut impl SessionFetch) {
        if self.seen.insert(join.session_id) && join.stream_position > 0 {
            self.repair(&join, fetch);
        }
    }

    /// The next frame to give.
    pub fn pop(&mut self) -> Option<PositionedTxData> {
        self.ready.pop_front()
    }

    /// The frames that wait to be given.
    #[must_use]
    pub fn len(&self) -> usize {
        self.ready.len()
    }

    /// Whether no frame waits.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.ready.is_empty()
    }

    /// Queue the frames of the session of `join` from its start to `join`.
    fn repair(&mut self, join: &ImageJoin, fetch: &mut impl SessionFetch) {
        let session = join.session_id;
        let gap = join.stream_position;
        let outcome = if Instant::now() < self.armed_at {
            "start"
        } else if gap > self.max_gap {
            "too_large"
        } else {
            self.refetch(join, fetch)
        };
        if outcome != "repaired" {
            warn!(
                stream_id = self.stream_id,
                session, gap, outcome, "tx_data late join not repaired"
            );
        }
        metrics::counter!(JOIN_REPAIRS_TOTAL, "outcome" => outcome).increment(1);
    }

    /// Refetch the frames before `join` and queue them. Return the outcome.
    fn refetch(&mut self, join: &ImageJoin, fetch: &mut impl SessionFetch) -> &'static str {
        let place = StreamPlace::of(join);
        let ready = &mut self.ready;
        let mut taken = 0u64;
        let result = fetch.fetch(
            self.stream_id,
            join.session_id,
            join.session_start,
            |loc, envelope| {
                if loc.position < join.join {
                    taken += 1;
                    ready.push_back(PositionedTxData {
                        loc,
                        stream_position: place.stream_position(loc.position),
                        session_start: join.session_start,
                        envelope,
                    });
                }
            },
        );
        match result {
            Ok(_) => {
                info!(
                    stream_id = self.stream_id,
                    session = join.session_id,
                    frames = taken,
                    gap = join.stream_position,
                    "tx_data late join repaired"
                );
                "repaired"
            }
            Err(error) => {
                warn!(stream_id = self.stream_id, %error, "tx_data late join refetch failed");
                "failed"
            }
        }
    }
}

/// The map from a position of one session to its stream position, from
/// the join of the session.
#[derive(Clone, Copy)]
struct StreamPlace {
    initial_term_id: i32,
    /// The bytes of one term. Zero when the live frame is in the first
    /// term: each frame before it is then in the first term too.
    term_length: u64,
}

impl StreamPlace {
    fn of(join: &ImageJoin) -> Self {
        let initial_term_id = join.session_start.term_id;
        let terms = Self::terms(initial_term_id, join.join);
        let before = join.stream_position - Self::offset(join.join);
        // Zero terms: the live frame is in the first term, and so is each
        // frame before it.
        let term_length = before.checked_div(terms).unwrap_or(0);
        Self {
            initial_term_id,
            term_length,
        }
    }

    /// The offset of `position` in its term. Aeron never gives a negative
    /// offset.
    fn offset(position: BPosition) -> u64 {
        u64::from(position.term_offset.cast_unsigned())
    }

    /// The terms of the session before the term of `position`.
    fn terms(initial_term_id: i32, position: BPosition) -> u64 {
        u64::from(
            position
                .term_id
                .wrapping_sub(initial_term_id)
                .cast_unsigned(),
        )
    }

    fn stream_position(self, position: BPosition) -> u64 {
        Self::terms(self.initial_term_id, position) * self.term_length + Self::offset(position)
    }
}

#[cfg(test)]
#[path = "join_repair_tests.rs"]
mod tests;
