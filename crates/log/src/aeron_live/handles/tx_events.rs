//! The publisher of `tx_events` (`docs/specs/2026-10-25-client-events.md`).
//!
//! The stream is best effort, and its publisher never waits. Each publish
//! is one offer on the Aeron thread. A refused offer drops the frame and
//! counts it in
//! [`BEST_EFFORT_DROPPED_TOTAL`](super::super::BEST_EFFORT_DROPPED_TOTAL),
//! with no retry. A frame above one MTU goes out in fragments. The frames
//! are not rkyv: they have the fixed layout of
//! `kardamom_types::tx_events`.

use std::time::{Duration, Instant};

use alloy_primitives::B256;
use kardamom_types::tx_events::{TxEventKind, TxEventsFrame};
use rkyv::util::AlignedVec;

use super::super::{AeronRuntime, PubHandle};
use crate::config::ChannelsConfig;
use crate::discovery::Topic;
use crate::discovery::plane::DiscoveredPublisher;
use crate::error::LogError;

/// Stamp out one best-effort publisher of raw frames on one stream.
macro_rules! raw_publisher {
    ($(#[$doc:meta])* $name:ident, $topic:expr, $channel:ident, $stream:ident) => {
        $(#[$doc])*
        #[derive(Clone)]
        pub struct $name {
            inner: PubHandle,
        }

        impl $name {
            /// Offer `frame` once, and drop it when the offer is refused.
            /// The copy into an aligned buffer is the one allocation of a
            /// publish, once for each frame.
            pub fn publish_lossy(&self, frame: &[u8]) {
                let mut bytes = AlignedVec::with_capacity(frame.len());
                bytes.extend_from_slice(frame);
                self.inner.publish_lossy(bytes);
            }
        }

        impl DiscoveredPublisher for $name {
            const TOPIC: Topic = $topic;

            fn stream_id(ch: &ChannelsConfig) -> i32 {
                ch.$stream
            }

            fn open_static(rt: &AeronRuntime, ch: &ChannelsConfig) -> Result<Self, LogError> {
                Ok(Self {
                    inner: rt.open_publication(&ch.$channel, ch.$stream)?,
                })
            }

            fn from_publication(inner: PubHandle) -> Self {
                Self { inner }
            }
        }
    };
}

raw_publisher!(
    /// `tx_events` publisher (ingress, sequencer → notifier).
    TxEventsPublisherHandle,
    Topic::TxEvents,
    tx_events_channel,
    tx_events_stream_id
);

/// The oldest that a frame gets before [`TxEventsSink::flush_due`] sends
/// it. So a publisher sends at most 1,000 frames a second for each kind,
/// at any rate of transactions, and the copy of each frame is the only
/// allocation of the stream.
pub const FRAME_MAX_AGE: Duration = Duration::from_millis(1);

/// The one offer of a finished frame. The live [`TxEventsPublisherHandle`]
/// offers it on Aeron; a test counts it.
pub trait FrameOffer {
    /// Offer `frame` once. Never waits.
    fn offer_frame(&self, frame: &[u8]);
}

impl FrameOffer for TxEventsPublisherHandle {
    fn offer_frame(&self, frame: &[u8]) {
        self.publish_lossy(frame);
    }
}

/// The frames of one publisher of `tx_events`: one frame for each kind
/// that it publishes. An append copies the record into the frame. A frame
/// goes out when it is full, or at the first [`Self::flush_due`] after it
/// is [`FRAME_MAX_AGE`] old. The sink never waits: a refused offer drops
/// the frame (see [`TxEventsPublisherHandle::publish_lossy`]).
pub struct TxEventsSink<P = TxEventsPublisherHandle> {
    publisher: P,
    /// Indexed by the code of the kind. A kind that this publisher does not
    /// publish has no frame.
    frames: [Option<PendingFrame>; 3],
}

/// One frame of a sink, and the time of its first record.
struct PendingFrame {
    frame: TxEventsFrame,
    since: Option<Instant>,
}

impl<P: FrameOffer> TxEventsSink<P> {
    /// A sink for `origin` with one frame for each of `kinds`. The frames
    /// are allocated here, once.
    #[must_use]
    pub fn new(publisher: P, origin: u16, kinds: &[TxEventKind]) -> Self {
        let mut frames: [Option<PendingFrame>; 3] = Default::default();
        for &kind in kinds {
            frames[kind as usize] = Some(PendingFrame {
                frame: TxEventsFrame::new(kind, origin),
                since: None,
            });
        }
        Self { publisher, frames }
    }

    /// Append an `accepted` record.
    pub fn accepted(&mut self, hash: &B256) {
        self.append(TxEventKind::Accepted, |f| f.push_accepted(hash));
    }

    /// Append a `sent` record.
    pub fn sent(&mut self, hash: &B256, lane: u8) {
        self.append(TxEventKind::Sent, |f| f.push_sent(hash, lane));
    }

    /// Send each frame that is [`FRAME_MAX_AGE`] old. A sink with no
    /// record reads no clock.
    pub fn flush_due(&mut self) {
        if self.frames.iter().flatten().all(|p| p.since.is_none()) {
            return;
        }
        let now = Instant::now();
        let publisher = &self.publisher;
        self.frames
            .iter_mut()
            .flatten()
            .filter(|p| {
                p.since
                    .is_some_and(|since| now.duration_since(since) >= FRAME_MAX_AGE)
            })
            .for_each(|p| p.send(publisher));
    }

    /// Send every frame that holds a record.
    pub fn flush(&mut self) {
        let publisher = &self.publisher;
        self.frames
            .iter_mut()
            .flatten()
            .filter(|p| !p.frame.is_empty())
            .for_each(|p| p.send(publisher));
    }

    /// Append one record with `push`. The frame of `kind` exists: each
    /// publisher appends only the kinds that it named at `new`.
    fn append(&mut self, kind: TxEventKind, push: impl FnOnce(&mut TxEventsFrame) -> bool) {
        let Some(pending) = self.frames[kind as usize].as_mut() else {
            return;
        };
        pending.since.get_or_insert_with(Instant::now);
        if push(&mut pending.frame) {
            pending.send(&self.publisher);
        }
    }
}

impl PendingFrame {
    /// Offer the frame once, then start the next one.
    fn send(&mut self, publisher: &impl FrameOffer) {
        publisher.offer_frame(self.frame.bytes());
        self.frame.advance();
        self.since = None;
    }
}
