//! The `tx_events` frame: the client stages that no other stream carries.
//!
//! The ingress publishes `accepted`, and the sequencer `sent`. The notifier
//! reads them, with its taps of the other streams. See
//! `docs/specs/2026-10-25-client-events.md`.
//!
//! The frame has a fixed layout, not rkyv, so the encode on the hot path
//! is a copy of fixed records into a buffer that the publisher allocates
//! at its start:
//!
//! ```text
//! header, 16 bytes, little endian:
//!     magic: u16    0x4B45 ("KE")
//!     version: u8   1
//!     kind: u8      1 accepted, 2 sent; 3, 4 and 5 are reserved
//!     origin: u16   the instance of the publisher
//!     count: u16    the number of records
//!     seq: u64      the frame number of this origin and kind, from 0
//! records, `count` of them, of the fixed size of the kind:
//!     accepted   32  hash
//!     sent       33  hash, lane: u8
//! ```
//!
//! The kinds 3, 4 and 5 are reserved for the `rejected`, `posted` and
//! `included` records of the spec. A reader of this version refuses them
//! as unknown kinds.
//!
//! A frame holds one kind. A publisher sends each frame once and does not
//! send it again, so a lost frame shows to the notifier as a gap in `seq`.

use alloc::boxed::Box;
use alloc::vec;
use alloy_primitives::B256;

/// The first two bytes of every frame.
pub const MAGIC: u16 = 0x4B45;
/// The version of the layout.
pub const VERSION: u8 = 1;
/// The bytes of the header.
pub const HEADER_LEN: usize = 16;
/// The most records in one frame. A full frame of `sent` records is about
/// 34 KB, below the largest message of the term.
pub const MAX_RECORDS: usize = 1024;

/// The stage that the records of a frame carry.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum TxEventKind {
    Accepted = 1,
    Sent = 2,
}

impl TxEventKind {
    /// Every kind that a frame can carry, in the order of their codes.
    pub const ALL: [Self; 2] = [Self::Accepted, Self::Sent];

    /// The place of the kind in [`Self::ALL`].
    #[must_use]
    pub const fn index(self) -> usize {
        self as usize - 1
    }

    /// The bytes of one record of this kind.
    #[must_use]
    pub const fn record_len(self) -> usize {
        match self {
            Self::Accepted => 32,
            Self::Sent => 33,
        }
    }

    /// The kind of the header byte `code`.
    #[must_use]
    pub const fn from_code(code: u8) -> Option<Self> {
        match code {
            1 => Some(Self::Accepted),
            2 => Some(Self::Sent),
            _ => None,
        }
    }

    /// One record of this kind. `record` has the length of the kind.
    fn decode(self, record: &[u8]) -> TxEvent {
        match self {
            Self::Accepted => TxEvent::Accepted {
                hash: B256::from_slice(record),
            },
            Self::Sent => TxEvent::Sent {
                hash: B256::from_slice(&record[..32]),
                lane: record[32],
            },
        }
    }

    /// The name of the kind as a metric label.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::Sent => "sent",
        }
    }
}

/// One frame under construction. The publisher allocates it once, appends
/// records, takes the bytes once for each publish cycle, then moves to the
/// next frame. An append allocates nothing.
pub struct TxEventsFrame {
    buf: Box<[u8]>,
    kind: TxEventKind,
    origin: u16,
    count: u16,
    seq: u64,
}

impl TxEventsFrame {
    /// An empty frame of `kind` for the publisher `origin`, with room for
    /// [`MAX_RECORDS`] records.
    #[must_use]
    pub fn new(kind: TxEventKind, origin: u16) -> Self {
        Self {
            buf: vec![0; HEADER_LEN + MAX_RECORDS * kind.record_len()].into_boxed_slice(),
            kind,
            origin,
            count: 0,
            seq: 0,
        }
    }

    /// Append an `accepted` record. Returns `true` when the frame is full.
    pub fn push_accepted(&mut self, hash: &B256) -> bool {
        self.slot().copy_from_slice(hash.as_slice());
        self.count_one()
    }

    /// Append a `sent` record. Returns `true` when the frame is full.
    pub fn push_sent(&mut self, hash: &B256, lane: u8) -> bool {
        let slot = self.slot();
        slot[..32].copy_from_slice(hash.as_slice());
        slot[32] = lane;
        self.count_one()
    }

    /// True when the frame holds no record.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// The bytes of the frame: the header, then the records.
    #[must_use]
    pub fn bytes(&mut self) -> &[u8] {
        let header = &mut self.buf[..HEADER_LEN];
        header[..2].copy_from_slice(&MAGIC.to_le_bytes());
        header[2] = VERSION;
        header[3] = self.kind as u8;
        header[4..6].copy_from_slice(&self.origin.to_le_bytes());
        header[6..8].copy_from_slice(&self.count.to_le_bytes());
        header[8..16].copy_from_slice(&self.seq.to_le_bytes());
        &self.buf[..HEADER_LEN + usize::from(self.count) * self.kind.record_len()]
    }

    /// Start the next frame: no records, and the next `seq`. The publisher
    /// calls it after each publish, also after a refused one.
    pub fn advance(&mut self) {
        self.count = 0;
        self.seq += 1;
    }

    /// The slot of the next record. The caller appends only while the
    /// frame is not full: each push reports a full frame, and the
    /// publisher sends it before the next push.
    fn slot(&mut self) -> &mut [u8] {
        let len = self.kind.record_len();
        let at = HEADER_LEN + usize::from(self.count) * len;
        &mut self.buf[at..at + len]
    }

    fn count_one(&mut self) -> bool {
        self.count += 1;
        usize::from(self.count) == MAX_RECORDS
    }
}

/// One record of a frame.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TxEvent {
    Accepted { hash: B256 },
    Sent { hash: B256, lane: u8 },
}

/// Why a frame does not read.
#[derive(Debug, Eq, PartialEq, thiserror::Error)]
pub enum FrameError {
    #[error("the frame is shorter than its header")]
    Short,
    #[error("the frame does not start with the magic 0x4B45")]
    Magic,
    #[error("the frame has the version {0}, and this build reads 1")]
    Version(u8),
    #[error("the frame has the unknown kind {0}")]
    Kind(u8),
    #[error("the frame holds {got} bytes of records, and its count needs {want}")]
    Length { got: usize, want: usize },
}

/// A frame as it arrives, read in place.
#[derive(Clone, Copy, Debug)]
pub struct TxEventsView<'a> {
    pub kind: TxEventKind,
    pub origin: u16,
    pub seq: u64,
    records: &'a [u8],
}

impl<'a> TxEventsView<'a> {
    /// Read the header of `frame`, and check that its records fill it.
    ///
    /// # Errors
    ///
    /// Returns a [`FrameError`] when the header is wrong or the length
    /// does not match the count.
    pub fn parse(frame: &'a [u8]) -> Result<Self, FrameError> {
        let (header, records) = frame
            .split_first_chunk::<HEADER_LEN>()
            .ok_or(FrameError::Short)?;
        let [
            m0,
            m1,
            version,
            code,
            o0,
            o1,
            c0,
            c1,
            s0,
            s1,
            s2,
            s3,
            s4,
            s5,
            s6,
            s7,
        ] = *header;
        if u16::from_le_bytes([m0, m1]) != MAGIC {
            return Err(FrameError::Magic);
        }
        if version != VERSION {
            return Err(FrameError::Version(version));
        }
        let kind = TxEventKind::from_code(code).ok_or(FrameError::Kind(code))?;
        let want = usize::from(u16::from_le_bytes([c0, c1])) * kind.record_len();
        if records.len() != want {
            return Err(FrameError::Length {
                got: records.len(),
                want,
            });
        }
        Ok(Self {
            kind,
            origin: u16::from_le_bytes([o0, o1]),
            seq: u64::from_le_bytes([s0, s1, s2, s3, s4, s5, s6, s7]),
            records,
        })
    }

    /// The number of records.
    #[must_use]
    pub fn len(&self) -> usize {
        self.records.len() / self.kind.record_len()
    }

    /// True when the frame holds no record.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// The records, in order.
    pub fn events(&self) -> impl Iterator<Item = TxEvent> + 'a {
        let kind = self.kind;
        self.records
            .chunks_exact(kind.record_len())
            .map(move |record| kind.decode(record))
    }
}

#[cfg(test)]
#[path = "tx_events_tests.rs"]
mod tests;
