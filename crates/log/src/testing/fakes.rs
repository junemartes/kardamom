//! In-memory pub/sub fakes used by other crates' unit tests.
//!
//! These fakes are deliberately simple: an `Arc<Mutex<Vec<bytes>>>` per
//! stream id, with no Aeron involvement. They preserve per-publisher FIFO
//! order, which is enough for unit tests of components that consume the
//! channel, but they do not model Aeron's concurrent-publisher
//! interleaving. Tests that must check behavior under realistic
//! interleaving go through the real Aeron Docker harness
//! (`docker_e2e.rs`), not these fakes.

use std::cell::RefCell;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::rc::Rc;
use std::sync::{Arc, Mutex};

use rkyv::Serialize;
use rkyv::api::high::HighSerializer;
use rkyv::rancor;
use rkyv::ser::allocator::ArenaHandle;
use rkyv::util::AlignedVec;

use crate::error::LogError;
use kardamom_types::{BPosition, FsyncWatermark, TxDataLoc};

/// Map from `(channel, stream_id)` to shared per-stream state.
type StreamMap = HashMap<(String, i32), Arc<Mutex<StreamState>>>;

/// In-memory bus shared by all `FakePublication` and `FakeSubscription`
/// handles that target the same `(channel, stream_id)` pair. Clone is
/// cheap, since it is an `Arc`.
#[derive(Clone, Default)]
pub struct FakeBus {
    streams: Arc<Mutex<StreamMap>>,
}

#[derive(Default)]
struct StreamState {
    /// Append-only log of (offset, `session_id`, bytes). Subscribers track
    /// their read cursor. `session_id` lets the `tx_data` fake model
    /// concurrent (active/active) publishers with distinct sessions.
    /// Non-tx_data publishers record `0`.
    log: Vec<(i64, i32, Vec<u8>)>,
    /// Next byte offset (mimics Aeron's stream position).
    next_offset: i64,
}

impl FakeBus {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn stream(&self, channel: &str, stream_id: i32) -> Arc<Mutex<StreamState>> {
        let mut g = self.streams.lock().unwrap();
        g.entry((channel.to_string(), stream_id))
            .or_insert_with(|| Arc::new(Mutex::new(StreamState::default())))
            .clone()
    }
}

/// Drop-in for `rusteron_client::AeronPublication` (the shared, concurrent
/// variant) in tests.
pub struct FakeConcurrentPublication {
    state: Arc<Mutex<StreamState>>,
}

impl FakeConcurrentPublication {
    /// Open onto `bus`'s `(channel, stream_id)` stream.
    #[must_use]
    pub fn new(bus: &FakeBus, channel: &str, stream_id: i32) -> Self {
        Self {
            state: bus.stream(channel, stream_id),
        }
    }

    #[must_use]
    pub fn offer(&self, bytes: &[u8]) -> i64 {
        self.offer_with_session(bytes, 0)
    }

    /// `offer` with an explicit publisher `session_id` on the fragment, so
    /// the `tx_data` fake can model concurrent ingress publishers.
    ///
    /// # Panics
    ///
    /// Panics if the bus's internal lock is poisoned (a previous holder
    /// panicked while holding it), or if the stream offset overflows
    /// `i64` (exbibytes of fixture data, never approached by a test).
    #[must_use]
    pub fn offer_with_session(&self, bytes: &[u8], session_id: i32) -> i64 {
        let mut g = self.state.lock().unwrap();
        let off = g.next_offset;
        g.log.push((off, session_id, bytes.to_vec()));
        let len = i64::try_from(bytes.len()).expect("fixture payload fits i64");
        g.next_offset = g
            .next_offset
            .checked_add(len)
            .expect("fixture offsets fit i64");
        g.next_offset
    }

    /// Encode `msg` with rkyv, append it to the stream, and return the
    /// fragment's start `BPosition`. This matches the real Aeron-backed
    /// publish convention in [`crate::aeron_live`].
    fn publish<T>(&self, msg: &T) -> Result<BPosition, LogError>
    where
        T: for<'a> Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>,
    {
        self.publish_with_session(msg, 0)
    }

    /// [`publish`](Self::publish) with an explicit publisher `session_id`.
    fn publish_with_session<T>(&self, msg: &T, session_id: i32) -> Result<BPosition, LogError>
    where
        T: for<'a> Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>,
    {
        let bytes =
            rkyv::to_bytes::<rancor::Error>(msg).map_err(|e| LogError::Codec(e.to_string()))?;
        let off = self.offer_with_session(bytes.as_slice(), session_id);
        let frag_start = off - i64::try_from(bytes.len()).expect("fixture payload fits i64");
        let header = FakeHeader::from_offset_session(frag_start, session_id);
        Ok(BPosition {
            term_id: header.term_id(),
            term_offset: header.term_offset(),
        })
    }
}

/// Mimics `rusteron_client::Header` enough for this crate's consumers.
#[derive(Clone, Copy, Debug)]
pub struct FakeHeader {
    term_id: i32,
    term_offset: i32,
    session_id: i32,
}

impl FakeHeader {
    #[must_use]
    pub fn from_offset(off: i64) -> Self {
        Self::from_offset_session(off, 0)
    }
    /// # Panics
    ///
    /// Panics if `off` needs more than `i32::MAX` terms at `TERM_LEN`
    /// (16 MiB) to represent — about 32 exbibytes of fixture data, never
    /// approached by a test.
    #[must_use]
    pub fn from_offset_session(off: i64, session_id: i32) -> Self {
        const TERM_LEN: i64 = 16 * 1024 * 1024;
        // Test fixtures never approach i32::MAX terms or a term-relative
        // offset beyond TERM_LEN (already bounded by the `%` above).
        let term_id = i32::try_from(off / TERM_LEN).expect("fixture offset fits i32 terms");
        let term_offset =
            i32::try_from(off % TERM_LEN).expect("remainder is always below TERM_LEN");
        Self {
            term_id,
            term_offset,
            session_id,
        }
    }
    #[must_use]
    pub fn term_id(&self) -> i32 {
        self.term_id
    }
    #[must_use]
    pub fn term_offset(&self) -> i32 {
        self.term_offset
    }
    #[must_use]
    pub fn session_id(&self) -> i32 {
        self.session_id
    }

    /// This fragment's `BPosition`, built from `term_id`/`term_offset`.
    /// Every typed poll callback below builds the same pair from a
    /// `FakeHeader`; this is the one place that does.
    #[must_use]
    pub fn position(&self) -> BPosition {
        BPosition {
            term_id: self.term_id,
            term_offset: self.term_offset,
        }
    }
}

/// Drop-in for `rusteron_client::Subscription` in tests.
pub struct FakeSubscription {
    state: Arc<Mutex<StreamState>>,
    cursor: usize,
}

impl FakeSubscription {
    /// Open onto `bus`'s `(channel, stream_id)` stream, cursor at the
    /// start.
    #[must_use]
    pub fn new(bus: &FakeBus, channel: &str, stream_id: i32) -> Self {
        Self {
            state: bus.stream(channel, stream_id),
            cursor: 0,
        }
    }

    /// Mirrors the real subscriber's
    /// `poll(&mut self, callback, fragment_limit)`.
    ///
    /// # Panics
    ///
    /// Panics if the bus's internal lock is poisoned (a previous holder
    /// panicked while holding it).
    pub fn poll<F: FnMut(&[u8], FakeHeader)>(&mut self, mut f: F, fragment_limit: usize) -> usize {
        let g = self.state.lock().unwrap();
        let remaining = &g.log[self.cursor..];
        let delivered = remaining.len().min(fragment_limit);
        for (off, session_id, bytes) in &remaining[..delivered] {
            f(
                bytes.as_slice(),
                FakeHeader::from_offset_session(*off, *session_id),
            );
        }
        self.cursor += delivered;
        delivered
    }

    /// Poll, decode each fragment's bytes as `T` via rkyv, and invoke `f`
    /// with the decoded value, the fragment's `BPosition`, and its
    /// publisher `session_id`. A fragment that fails to decode is
    /// skipped, matching every real `WireMessage` decode path
    /// (`materialize`/`from_bytes`, never a panic on bad bytes).
    ///
    /// This is the one decode-then-callback body [`FakeTypedSubscription`],
    /// [`FakeTxDataSubscription`], and [`FakeTxOrderingSubscription`] all
    /// share; each adapts the callback's argument order and shape to its
    /// own `poll` signature.
    pub fn poll_decoded<T, F>(&mut self, mut f: F, fragment_limit: usize) -> usize
    where
        T: crate::codec::WireMessage,
        F: FnMut(T, BPosition, i32),
    {
        self.poll(
            |bytes: &[u8], header: FakeHeader| {
                if let Ok(v) = rkyv::from_bytes::<T, rancor::Error>(bytes) {
                    f(v, header.position(), header.session_id());
                }
            },
            fragment_limit,
        )
    }
}

/// High-level fake publication that consumers can use in place of a real
/// Aeron-backed publisher handle from [`crate::aeron_live`].
pub struct FakePublication {
    pub_handle: FakeConcurrentPublication,
}

impl FakePublication {
    #[must_use]
    pub fn open(bus: &FakeBus, channel: &str, stream_id: i32) -> Self {
        Self {
            pub_handle: FakeConcurrentPublication::new(bus, channel, stream_id),
        }
    }

    /// # Errors
    ///
    /// Returns an error if rkyv serialization of `msg` fails.
    pub fn publish<T>(&self, msg: &T) -> Result<BPosition, LogError>
    where
        T: for<'a> Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>,
    {
        self.pub_handle.publish(msg)
    }
}

/// High-level fake subscription for owned-value reads.
pub struct FakeTypedSubscription<T> {
    sub: FakeSubscription,
    _marker: std::marker::PhantomData<T>,
}

impl<T> FakeTypedSubscription<T>
where
    T: crate::codec::WireMessage,
{
    #[must_use]
    pub fn open(bus: &FakeBus, channel: &str, stream_id: i32) -> Self {
        Self {
            sub: FakeSubscription::new(bus, channel, stream_id),
            _marker: std::marker::PhantomData,
        }
    }

    pub fn poll<F: FnMut(T, BPosition)>(&mut self, mut f: F, fragment_limit: usize) -> usize {
        self.sub
            .poll_decoded::<T, _>(|v, pos, _session| f(v, pos), fragment_limit)
    }
}

// ============================================================================
// TxData and TxOrdering typed fakes.
//
//   - TxData[i] is an Aeron exclusive publication carrying full
//     TxEnvelopes. There are M of them, one per sequencer, each its own
//     stream.
//   - TxOrdering is the canonical orderer: an Aeron concurrent
//     multi-publisher stream carrying TxOrderingMessage records
//     (TxRef | BoundaryStart), about 16-32 bytes.
//
// At the in-memory-fake level, "exclusive vs concurrent" collapses. This
// module only models one publisher at a time, so there is no CAS-cursor
// contention to simulate. The distinction is at the type level: the
// tx_data pub/sub pair is parameterised by `TxEnvelope` and builds
// single-producer streams keyed by `sequencer_id`. The tx_ordering pub/sub
// pair is parameterised by `TxOrderingMessage` and shares one stream.
// Tests that must check real concurrent-publisher interleaving need the
// docker e2e harness instead.
// ============================================================================

use kardamom_types::{TxEnvelope, TxOrderingMessage, TxRef};

/// `TxData`[i]: per-sequencer exclusive publication of full `TxEnvelope`
/// bytes. In production this maps to `Aeron::add_exclusive_publication`.
/// The fake only preserves FIFO order.
///
/// `FakeTxDataPublication::new` requires the caller to hand in the
/// `sequencer_id`. The fake keeps the id so callers can build a `TxRef`
/// pointing at the produced `BPosition` without having to track the id
/// separately.
pub struct FakeTxDataPublication {
    sequencer_id: u8,
    session_id: i32,
    pub_handle: FakeConcurrentPublication,
}

impl FakeTxDataPublication {
    /// Open the `tx_data`[i] pub on `bus`, using the channel URI and
    /// stream-id convention "<channel>"/"<`stream_id`>". Real wiring uses
    /// `kardamom_log::config::ChannelsConfig::tx_data_channel_template` to
    /// derive per-sequencer URIs. Session id defaults to `0` (single
    /// publisher).
    #[must_use]
    pub fn open(bus: &FakeBus, sequencer_id: u8, channel: &str, stream_id: i32) -> Self {
        Self::open_with_session(bus, sequencer_id, 0, channel, stream_id)
    }

    /// Like [`open`](Self::open), but with an explicit publisher
    /// `session_id`, so tests can model concurrent (active/active) ingress
    /// publishers on one shard. Distinct session ids keep the executor
    /// join key `(shard, session, position)` unique, even when their
    /// `BPosition`s collide.
    #[must_use]
    pub fn open_with_session(
        bus: &FakeBus,
        sequencer_id: u8,
        session_id: i32,
        channel: &str,
        stream_id: i32,
    ) -> Self {
        Self {
            sequencer_id,
            session_id,
            pub_handle: FakeConcurrentPublication::new(bus, channel, stream_id),
        }
    }

    #[must_use]
    pub fn sequencer_id(&self) -> u8 {
        self.sequencer_id
    }

    /// This publisher's Aeron `session_id` (what a real media driver
    /// assigns per publication).
    #[must_use]
    pub fn session_id(&self) -> i32 {
        self.session_id
    }

    /// Publish a `TxEnvelope` and return its fragment-start `BPosition` on
    /// `tx_data`[i]. The subscriber pairs this position with `session_id`
    /// into a `TxDataLoc`. The sequencer stamps the session into `TxRef`.
    ///
    /// # Errors
    ///
    /// Returns an error if rkyv serialization of `env` fails.
    pub fn publish(&self, env: &TxEnvelope) -> Result<BPosition, LogError> {
        self.pub_handle.publish_with_session(env, self.session_id)
    }
}

/// `TxData`[i]: per-sequencer subscription returning `(BPosition, TxEnvelope)`.
///
/// Executors run M+1 of these (one per `tx_data`) plus one `tx_ordering`
/// subscription. They buffer `tx_data` messages keyed by `BPosition` until
/// the matching `TxRef` arrives on `tx_ordering`.
pub struct FakeTxDataSubscription {
    sub: FakeSubscription,
}

impl FakeTxDataSubscription {
    #[must_use]
    pub fn open(bus: &FakeBus, channel: &str, stream_id: i32) -> Self {
        Self {
            sub: FakeSubscription::new(bus, channel, stream_id),
        }
    }

    /// Poll and invoke `f` with `(TxDataLoc, TxEnvelope)` per fragment. The
    /// `TxDataLoc` pairs the publisher `session_id` with the fragment-start
    /// `BPosition`. This is exactly what the sequencer stamps into `TxRef`
    /// (`tx_data_session_id` and `tx_data_position`).
    pub fn poll<F: FnMut(TxDataLoc, TxEnvelope)>(
        &mut self,
        mut f: F,
        fragment_limit: usize,
    ) -> usize {
        self.sub.poll_decoded::<TxEnvelope, _>(
            |env, pos, session_id| f(TxDataLoc::new(session_id, pos), env),
            fragment_limit,
        )
    }
}

/// `TxOrdering`: the canonical orderer, a concurrent multi-publisher stream
/// carrying [`TxOrderingMessage`] records (`TxRef` | `BoundaryStart`). In
/// production this maps to `Aeron::add_publication` (the shared, concurrent
/// variant).
pub struct FakeTxOrderingPublication {
    pub_handle: FakeConcurrentPublication,
}

impl FakeTxOrderingPublication {
    #[must_use]
    pub fn open(bus: &FakeBus, channel: &str, stream_id: i32) -> Self {
        Self {
            pub_handle: FakeConcurrentPublication::new(bus, channel, stream_id),
        }
    }

    /// Publish a [`TxRef`] onto `tx_ordering`. Returns the fragment's start
    /// position (the canonical B-position).
    ///
    /// # Errors
    ///
    /// Returns an error if rkyv serialization of `r` fails.
    pub fn publish_ref(&self, r: &TxRef) -> Result<BPosition, LogError> {
        self.publish_message(&TxOrderingMessage::TxRef(*r))
    }

    /// Publish a [`kardamom_types::BlockBoundaryStart`] onto `tx_ordering`
    /// (sealer-emitted). Returns the boundary's canonical position.
    ///
    /// # Errors
    ///
    /// Returns an error if rkyv serialization of `b` fails.
    pub fn publish_boundary(
        &self,
        b: &kardamom_types::BlockBoundaryStart,
    ) -> Result<BPosition, LogError> {
        self.publish_message(&TxOrderingMessage::BoundaryStart(b.clone()))
    }

    fn publish_message(&self, m: &TxOrderingMessage) -> Result<BPosition, LogError> {
        self.pub_handle.publish(m)
    }
}

/// `TxOrdering` subscription returning `(BPosition, TxOrderingMessage)` per
/// fragment. The B-position is the canonical L2 tx ordering identifier
/// (system invariant I1).
pub struct FakeTxOrderingSubscription {
    sub: FakeSubscription,
}

impl FakeTxOrderingSubscription {
    #[must_use]
    pub fn open(bus: &FakeBus, channel: &str, stream_id: i32) -> Self {
        Self {
            sub: FakeSubscription::new(bus, channel, stream_id),
        }
    }

    pub fn poll<F: FnMut(BPosition, TxOrderingMessage)>(
        &mut self,
        mut f: F,
        fragment_limit: usize,
    ) -> usize {
        self.sub
            .poll_decoded::<TxOrderingMessage, _>(|m, pos, _session| f(pos, m), fragment_limit)
    }
}

/// In-memory fake fsync-watermark stream: an
/// `Rc<RefCell<HashMap<recorder_id, VecDeque>>>`. A test publishes into one
/// handle and drains from a clone of the same handle, on one thread; a
/// clone shares the underlying queues.
#[derive(Clone, Default)]
pub struct FakeFsyncWatermarkStream {
    inner: Rc<RefCell<HashMap<u8, VecDeque<FsyncWatermark>>>>,
}

impl FakeFsyncWatermarkStream {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn publish(&self, w: FsyncWatermark) {
        let mut g = self.inner.borrow_mut();
        g.entry(w.recorder_id).or_default().push_back(w);
    }

    #[must_use]
    pub fn drain(&self, recorder_id: u8) -> Vec<FsyncWatermark> {
        let mut g = self.inner.borrow_mut();
        g.get_mut(&recorder_id)
            .map(|q| q.drain(..).collect())
            .unwrap_or_default()
    }
}
