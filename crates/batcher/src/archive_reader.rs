//! Offline Aeron Archive segment reader.
//!
//! The batcher reads on-disk Aeron Archive segment files. It does not
//! subscribe to `tx_receipts`. It does not talk to the live sequencer.
//!
//! ## Architecture: split data and ordering
//!
//! The on-disk archives carry two different payload types:
//!
//! - **`TxOrdering`** segments carry [`TxOrderingMessage`] records (`TxRef +
//!   BoundaryStart`). This is the canonical orderer data.
//! - **Per-sequencer `TxData`** segments carry full [`TxEnvelope`] records.
//!   This is the bulk transaction data.
//!
//! The batcher uses a simplified on-disk frame format for v0: one
//! length-prefixed rkyv archive per record.
//!
//! ```text
//!   length      u32 LE      total frame length including this header
//!   reserved    u32 zero
//!   term_id     i32 LE
//!   term_offset i32 LE
//!   payload     length - 16 bytes (rkyv archive of T)
//!   pad         zero, to next 8-byte boundary
//! ```
//!
//! The header does not depend on the payload type. The payload type depends
//! on which archive the segment file came from (`tx_ordering` or `tx_data`[i]).
//! The reader is generic over the payload type, so it can deserialize either
//! one.
//!
//! The active segment can end mid-frame. The reader stops at the last full
//! frame.

use std::fs::File;
use std::io::Read;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

use kardamom_types::{BPosition, TxEnvelope, TxOrderingMessage};
use rkyv::api::high::{HighDeserializer, HighValidator};
use rkyv::rancor;

use crate::error::BatcherError;

const FRAME_HEADER_LEN: usize = 4 + 4 + 4 + 4; // 16 bytes: length, reserved, term_id, term_offset
const FRAME_ALIGN: usize = 8;

/// One decoded record from a typed segment file. The position is the start
/// position of the fragment on the originating Aeron stream. For `tx_data`
/// segments, this is the position a sequencer recorded in
/// [`TxRef::tx_data_position`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TypedRecord<T> {
    pub position: BPosition,
    pub value: T,
}

/// An rkyv-archived payload a [`TypedSegmentReader`] can decode: archived,
/// and deserializable and checkable through the high-level rkyv API this
/// crate's frames use.
pub trait ArchivedRecord:
    Sized
    + rkyv::Archive<
        Archived: rkyv::Deserialize<Self, HighDeserializer<rancor::Error>>
                      + for<'a> rkyv::bytecheck::CheckBytes<HighValidator<'a, rancor::Error>>,
    >
{
}

impl<T> ArchivedRecord for T where
    T: rkyv::Archive<
            Archived: rkyv::Deserialize<T, HighDeserializer<rancor::Error>>
                          + for<'a> rkyv::bytecheck::CheckBytes<HighValidator<'a, rancor::Error>>,
        >
{
}

/// A generic offline reader for an archive segment file. The type parameter
/// `T` is the rkyv-archived payload in every frame of this segment. Use
/// `T = TxOrderingMessage` for `tx_ordering` archives. Use `T = TxEnvelope`
/// for `tx_data` archives.
pub struct TypedSegmentReader<T> {
    bytes: Vec<u8>,
    pos: usize,
    _marker: PhantomData<T>,
}

impl<T: ArchivedRecord> TypedSegmentReader<T> {
    /// Open a segment file. `segment_path` is the full path to the `.rec`
    /// file (typically `<archive_dir>/<recording_id>-<segmentBase>.rec`).
    ///
    /// # Errors
    /// Returns an error when `segment_path` cannot be opened or read.
    pub fn open(segment_path: &Path) -> Result<Self, BatcherError> {
        let mut f = File::open(segment_path)?;
        let mut bytes = Vec::new();
        f.read_to_end(&mut bytes)?;
        Ok(Self {
            bytes,
            pos: 0,
            _marker: PhantomData,
        })
    }

    /// Compose the canonical segment file path:
    /// `<archive_dir>/<recording_id>-<segment_base_position>.rec`.
    #[must_use]
    pub fn segment_path(archive_dir: &Path, recording_id: i64, segment_base: i64) -> PathBuf {
        archive_dir.join(format!("{recording_id}-{segment_base}.rec"))
    }
}

impl<T: ArchivedRecord> Iterator for TypedSegmentReader<T> {
    type Item = Result<TypedRecord<T>, BatcherError>;

    fn next(&mut self) -> Option<Self::Item> {
        // Need at least a header.
        if self.pos + FRAME_HEADER_LEN > self.bytes.len() {
            return None;
        }
        let buf = &self.bytes[self.pos..];
        let len = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
        if len == 0 {
            // A pre-allocated tail that is not yet written is zero-filled. A
            // zero length followed by any non-zero byte means a damaged
            // header with real data behind it. This is corruption, not a
            // live tail. Without this check, a length wipe in the middle of
            // the file would silently truncate the stream.
            if buf.iter().any(|&b| b != 0) {
                let at = self.pos;
                self.pos = self.bytes.len(); // fuse the iterator
                return Some(Err(BatcherError::Corruption(format!(
                    "zeroed frame header followed by data at segment offset {at}"
                ))));
            }
            return None;
        }
        if len < FRAME_HEADER_LEN {
            // A frame cannot be smaller than its own header.
            let at = self.pos;
            self.pos = self.bytes.len(); // fuse the iterator
            return Some(Err(BatcherError::Corruption(format!(
                "frame length {len} below header size at segment offset {at}"
            ))));
        }
        if self.pos + len > self.bytes.len() {
            // Stop here. The active segment is truncated by a frame
            // mid-write at the tail.
            return None;
        }
        // Bytes 4 to 8 are reserved.
        let term_id = i32::from_le_bytes([buf[8], buf[9], buf[10], buf[11]]);
        let term_offset = i32::from_le_bytes([buf[12], buf[13], buf[14], buf[15]]);
        let payload = &buf[FRAME_HEADER_LEN..len];

        let position = BPosition {
            term_id,
            term_offset,
        };

        let rec = match access_owned::<T>(payload) {
            Ok(value) => Ok(TypedRecord { position, value }),
            Err(e) => Err(e),
        };

        // Advance by the aligned frame length.
        let aligned = len.div_ceil(FRAME_ALIGN) * FRAME_ALIGN;
        self.pos += aligned.min(self.bytes.len() - self.pos);

        Some(rec)
    }
}

/// `TxOrdering` segment reader: yields [`TxOrderingMessage`] records (`TxRef` +
/// `BoundaryStart`) in canonical order.
pub type TxOrderingSegmentReader = TypedSegmentReader<TxOrderingMessage>;

/// `TxData`[i] segment reader: yields full [`TxEnvelope`] records in the
/// order sequencer `i` wrote them.
pub type TxDataSegmentReader = TypedSegmentReader<TxEnvelope>;

/// # Errors
/// Returns [`BatcherError::Codec`] when `bytes` does not decode as a valid
/// archived `T`.
fn access_owned<T: ArchivedRecord>(bytes: &[u8]) -> Result<T, BatcherError> {
    rkyv::from_bytes::<T, rancor::Error>(bytes).map_err(|e| BatcherError::Codec(e.to_string()))
}

/// A record type [`append_frame`] can encode into the simplified KAR1
/// segment format, through the high-level rkyv serializer this crate uses.
pub trait SegmentRecord:
    for<'a> rkyv::Serialize<
        rkyv::api::high::HighSerializer<
            rkyv::util::AlignedVec,
            rkyv::ser::allocator::ArenaHandle<'a>,
            rancor::Error,
        >,
    >
{
}

impl<T> SegmentRecord for T where
    T: for<'a> rkyv::Serialize<
            rkyv::api::high::HighSerializer<
                rkyv::util::AlignedVec,
                rkyv::ser::allocator::ArenaHandle<'a>,
                rancor::Error,
            >,
        >
{
}

/// Append one frame to `out`. This crate's tests and the writer-side
/// adapter use this helper to encode one record in the simplified KAR1
/// segment format.
///
/// The frame does not depend on the type. The caller writes `tx_data`
/// (`T = TxEnvelope`) and `tx_ordering` (`T = TxOrderingMessage`) the same way.
///
/// # Panics
/// Panics if rkyv encoding of `value` fails (an rkyv-internal allocation
/// failure, not a data error, so there is no error to return), or if the
/// encoded frame exceeds `u32::MAX` bytes (no real record does).
pub fn append_frame<T: SegmentRecord>(out: &mut Vec<u8>, position: BPosition, value: &T) {
    let payload = rkyv::to_bytes::<rancor::Error>(value).expect("rkyv encode");
    let total = u32::try_from(FRAME_HEADER_LEN + payload.len()).expect("frame fits in u32 bytes");
    out.extend_from_slice(&total.to_le_bytes());
    out.extend_from_slice(&[0u8; 4]); // reserved
    out.extend_from_slice(&position.term_id.to_le_bytes());
    out.extend_from_slice(&position.term_offset.to_le_bytes());
    out.extend_from_slice(payload.as_slice());
    // Pad to the 8-byte boundary.
    while !out.len().is_multiple_of(FRAME_ALIGN) {
        out.push(0);
    }
}
