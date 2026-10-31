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
//! The refetch does not block the reader. While it runs, the repair holds
//! the live frames of its own lane and gives none of them; the reader
//! polls its other lanes. The repair gives the held frames when the
//! answer comes, or when [`REFETCH_BUDGET`] passes with no answer.
//!
//! The repair has two bounds:
//! - A session that the lane first sees in its start grace is not
//!   repaired. A subscriber that starts joins each running session late,
//!   by design: the frames before its start are older than its own state.
//!   An ingress and a sequencer that restart together fall in this case:
//!   the frames of the first seconds of the new session are lost to that
//!   sequencer, as before the repair.
//! - A gap above `max_gap` bytes is not repaired. Such a session ran for a
//!   long time before the join, so it is not a new publication.
//!
//! A refetch that fails or gives a short range loses the missing frames,
//! as a late join did before the repair. Each outcome counts in
//! [`JOIN_REPAIRS_TOTAL`].

use std::collections::{HashSet, VecDeque};
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver, TryRecvError};
use kardamom_types::{BPosition, TxDataLoc, TxEnvelope};
use tracing::{info, warn};

use super::ArchiveRefetcher;
use crate::aeron_live::{ImageJoin, PositionedTxData, TxDataItem};
use crate::error::LogError;

/// The late joins of `tx_data` lanes, by outcome:
/// - `repaired`: the refetch covered the range from the session start to
///   the join;
/// - `short`: the replay ended before the join, or gave no frame;
/// - `failed`: the refetch failed, passed its budget, or gave a position
///   that does not map to the stream;
/// - `start`: a session of the start grace;
/// - `too_large`: a gap above the bound.
pub const JOIN_REPAIRS_TOTAL: &str = "kardamom_log_tx_data_join_repairs_total";

/// The longest wait for the answer of one refetch: the connect timeout
/// and the drain cap of the refetch client, and some slack.
pub const REFETCH_BUDGET: Duration = Duration::from_secs(15);

/// The frames of one replayed range, and the raw end of the range.
#[derive(Debug, Default)]
pub struct Refetched {
    pub frames: Vec<(TxDataLoc, TxEnvelope)>,
    /// `None` when the archive had nothing to replay yet.
    pub end: Option<i64>,
}

/// The answer of one refetch.
pub type FetchAnswer = Result<Refetched, LogError>;

/// The refetch of one publisher session from one position, to the end of
/// its recording.
pub trait SessionFetch {
    /// Start the refetch of the frames of `session_id` on `stream_id` from
    /// `from`. The answer comes on the returned channel.
    fn start(&mut self, stream_id: i32, session_id: i32, from: BPosition) -> Receiver<FetchAnswer>;
}

impl SessionFetch for ArchiveRefetcher {
    /// The refetch runs here, on the caller's thread, and the channel
    /// holds its answer at once. For a reader that may block.
    fn start(&mut self, stream_id: i32, session_id: i32, from: BPosition) -> Receiver<FetchAnswer> {
        let (answer, reply) = crossbeam_channel::bounded(1);
        let _ = answer.send(Self::fetch_all(self, stream_id, session_id, from));
        reply
    }
}

impl ArchiveRefetcher {
    /// Each frame of the session from `from`, and the end of the replay.
    fn fetch_all(&mut self, stream_id: i32, session_id: i32, from: BPosition) -> FetchAnswer {
        let mut frames = Vec::new();
        self.fetch_tx_data_range(stream_id, session_id, from, |loc, envelope| {
            frames.push((loc, envelope));
        })
        .map(|replayed| Refetched {
            frames,
            end: replayed.end,
        })
    }
}

/// A refetch client on a thread of its own, for a reader that must be
/// `Send` and must not block: the Aeron resources of an
/// [`ArchiveRefetcher`] are bound to the thread that made them. The
/// thread runs one refetch at a time and ends when the worker drops.
pub struct RefetchWorker {
    requests: crossbeam_channel::Sender<FetchRequest>,
}

/// One request to a [`RefetchWorker`], with the channel of its answer.
struct FetchRequest {
    stream_id: i32,
    session_id: i32,
    from: BPosition,
    answer: crossbeam_channel::Sender<FetchAnswer>,
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
                let answer =
                    refetcher.fetch_all(request.stream_id, request.session_id, request.from);
                // A repair that passed its budget dropped its channel.
                let _ = request.answer.send(answer);
            }
        });
        Self { requests }
    }
}

impl SessionFetch for RefetchWorker {
    fn start(&mut self, stream_id: i32, session_id: i32, from: BPosition) -> Receiver<FetchAnswer> {
        let (answer, reply) = crossbeam_channel::bounded(1);
        // A worker that stopped drops the request, and with it the answer
        // channel: the repair then reads a disconnect.
        let _ = self.requests.send(FetchRequest {
            stream_id,
            session_id,
            from,
            answer,
        });
        reply
    }
}

/// The repair of the late joins of one lane.
pub struct JoinRepair {
    stream_id: i32,
    /// The publisher sessions that the lane has seen.
    seen: HashSet<i32>,
    /// A session that the lane first sees within `grace` of `started` is a
    /// session of the start.
    started: Instant,
    grace: Duration,
    max_gap: u64,
    /// The longest wait for the answer of one refetch.
    budget: Duration,
    /// The frames to give, oldest first.
    ready: VecDeque<PositionedTxData>,
    /// The refetch in flight.
    pending: Option<Pending>,
    /// The joins that wait for the refetch in flight.
    waiting: VecDeque<ImageJoin>,
    /// The live frames of the lane that wait behind the refetch in flight.
    held: VecDeque<PositionedTxData>,
}

/// One refetch in flight.
struct Pending {
    join: ImageJoin,
    answer: Receiver<FetchAnswer>,
    started: Instant,
}

impl JoinRepair {
    /// The repair of lane stream `stream_id`, with a start grace of
    /// `grace` from now.
    #[must_use]
    pub fn new(stream_id: i32, grace: Duration, max_gap: u64) -> Self {
        Self {
            stream_id,
            seen: HashSet::new(),
            started: Instant::now(),
            grace,
            max_gap,
            budget: REFETCH_BUDGET,
            ready: VecDeque::new(),
            pending: None,
            waiting: VecDeque::new(),
            held: VecDeque::new(),
        }
    }

    /// The repair with `budget` as the longest wait for one refetch, in
    /// place of [`REFETCH_BUDGET`].
    #[must_use]
    pub fn with_budget(mut self, budget: Duration) -> Self {
        self.budget = budget;
        self
    }

    /// Take one item of the lane. The join of a new session that is not
    /// at its start starts the refetch of the frames before the join. A
    /// join comes as a marker, or as the first frame of the session when
    /// the frame comes first.
    pub fn take(&mut self, item: TxDataItem, fetch: &mut impl SessionFetch) {
        match item {
            TxDataItem::Joined(join) => self.joined(join, fetch),
            TxDataItem::Frame(frame) => {
                self.joined(Self::join_of(&frame), fetch);
                self.queue_live(frame);
            }
        }
    }

    /// The next frame to give. `None` while the refetch in flight holds
    /// the lane.
    pub fn pop(&mut self, fetch: &mut impl SessionFetch) -> Option<PositionedTxData> {
        self.advance(fetch);
        self.ready.pop_front()
    }

    /// The frames that wait to be given, held ones too.
    #[must_use]
    pub fn len(&self) -> usize {
        self.ready.len() + self.held.len()
    }

    /// Whether no frame waits.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
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

    /// Give `frame` at once, or hold it behind the refetch in flight.
    fn queue_live(&mut self, frame: PositionedTxData) {
        if self.pending.is_some() {
            self.held.push_back(frame);
        } else {
            self.ready.push_back(frame);
        }
    }

    /// Repair `join` when it is the first of its session and not at its
    /// start.
    fn joined(&mut self, join: ImageJoin, fetch: &mut impl SessionFetch) {
        if !self.seen.insert(join.session_id) || join.stream_position == 0 {
            return;
        }
        if self.started.elapsed() < self.grace {
            self.count(&join, "start");
        } else if join.stream_position > self.max_gap {
            self.count(&join, "too_large");
        } else if self.pending.is_some() {
            self.waiting.push_back(join);
        } else {
            self.start(join, fetch);
        }
    }

    fn start(&mut self, join: ImageJoin, fetch: &mut impl SessionFetch) {
        self.pending = Some(Pending {
            answer: fetch.start(self.stream_id, join.session_id, join.session_start),
            join,
            started: Instant::now(),
        });
    }

    /// Take the answer of the refetch in flight, when it came or its
    /// budget passed. Then start the next waiting join, or give the held
    /// frames.
    fn advance(&mut self, fetch: &mut impl SessionFetch) {
        let Some(pending) = &self.pending else {
            return;
        };
        let answer = match pending.answer.try_recv() {
            Ok(answer) => answer,
            Err(TryRecvError::Empty) if pending.started.elapsed() < self.budget => return,
            Err(TryRecvError::Empty) => Err(LogError::Aeron("refetch passed its budget".into())),
            Err(TryRecvError::Disconnected) => {
                Err(LogError::Aeron("the refetch worker stopped".into()))
            }
        };
        let join = pending.join;
        self.pending = None;
        let outcome = self.place(&join, answer);
        self.count(&join, outcome);
        match self.waiting.pop_front() {
            Some(next) => self.start(next, fetch),
            None => self.ready.extend(self.held.drain(..)),
        }
    }

    /// Queue the refetched frames before `join`. Return the outcome.
    fn place(&mut self, join: &ImageJoin, answer: FetchAnswer) -> &'static str {
        let refetched = match answer {
            Ok(refetched) => refetched,
            Err(error) => {
                warn!(stream_id = self.stream_id, %error, "tx_data late join refetch failed");
                return "failed";
            }
        };
        let Some(frames) = Self::before(join, refetched.frames) else {
            return "failed";
        };
        let reached = refetched
            .end
            .and_then(|end| u64::try_from(end).ok())
            .is_some_and(|end| end >= join.stream_position);
        let count = frames.len();
        self.ready.extend(frames);
        if !reached || count == 0 {
            return "short";
        }
        info!(
            stream_id = self.stream_id,
            session = join.session_id,
            frames = count,
            gap = join.stream_position,
            "tx_data late join repaired"
        );
        "repaired"
    }

    /// The frames of `frames` before `join`, with their stream positions,
    /// or `None` when a position does not map to the stream.
    fn before(
        join: &ImageJoin,
        frames: Vec<(TxDataLoc, TxEnvelope)>,
    ) -> Option<Vec<PositionedTxData>> {
        let place = StreamPlace::of(join)?;
        frames
            .into_iter()
            .filter(|(loc, _)| loc.position < join.join)
            .map(|(loc, envelope)| {
                Some(PositionedTxData {
                    loc,
                    stream_position: place.stream_position(loc.position)?,
                    session_start: join.session_start,
                    envelope,
                })
            })
            .collect()
    }

    fn count(&self, join: &ImageJoin, outcome: &'static str) {
        if outcome != "repaired" {
            warn!(
                stream_id = self.stream_id,
                session = join.session_id,
                gap = join.stream_position,
                outcome,
                "tx_data late join not repaired"
            );
        }
        metrics::counter!(JOIN_REPAIRS_TOTAL, "outcome" => outcome).increment(1);
    }
}

/// The map from a position of one session to its stream position, from
/// the join of the session.
#[derive(Clone, Copy)]
struct StreamPlace {
    initial_term_id: i32,
    /// The bytes of one term. Zero when the join is in the first term:
    /// each frame before it is then in the first term too.
    term_length: u64,
}

impl StreamPlace {
    /// The map of the session of `join`, or `None` when the join does not
    /// fit its own stream position.
    fn of(join: &ImageJoin) -> Option<Self> {
        let initial_term_id = join.session_start.term_id;
        let terms = Self::terms(initial_term_id, join.join);
        let before = join.stream_position.checked_sub(Self::offset(join.join))?;
        // Zero terms: the join is in the first term, and so is each frame
        // before it.
        let term_length = before.checked_div(terms).unwrap_or(0);
        Some(Self {
            initial_term_id,
            term_length,
        })
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

    /// The stream position of `position`, or `None` on an overflow.
    fn stream_position(self, position: BPosition) -> Option<u64> {
        Self::terms(self.initial_term_id, position)
            .checked_mul(self.term_length)?
            .checked_add(Self::offset(position))
    }
}

#[cfg(test)]
#[path = "join_repair_tests.rs"]
mod tests;
