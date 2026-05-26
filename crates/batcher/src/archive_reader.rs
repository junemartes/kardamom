//! Offline Aeron Archive segment reader.
//!
//! S0: the batcher reads from on-disk Aeron Archive segment files. It
//! does not subscribe to tx_receipts, does not talk to the live sequencer.
//!
//! ## architecture: split data and ordering
//!
//! After the on-disk archives carry **two different payload types**:
//!
//! - **TxOrdering** segments carry [`TxOrderingMessage`] records (`TxRef +
//!   BoundaryStart`) — the canonical orderer.
//! - **Per-sequencer TxData** segments carry full [`TxEnvelope`] records —
//!   the bulk transaction data.
//!
//! The simplified on-disk frame format the batcher uses for v0 is one
//! length-prefixed rkyv archive per record:
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
//! The header is type-agnostic; the payload type is determined by *which
//! archive* the segment file came from (tx_ordering vs tx_data[i]). The
//! reader is generic over the payload type so it can deserialise either.
//!
//! The active segment may end mid-frame; the reader truncates at the last
//! full frame.

use std::fs::File;
use std::io::Read;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

use rkyv::api::high::{HighDeserializer, HighValidator};
use rkyv::rancor;
use types::{BPosition, TxEnvelope, TxOrderingMessage};

use crate::error::BatcherError;

const FRAME_HEADER_LEN: usize = 4 + 4 + 4 + 4; // 16 bytes (length, reserved, term_id, term_offset)
const FRAME_ALIGN: usize = 8;

/// One decoded record from a typed segment file. The position is the
/// fragment's start position on the originating Aeron stream — for tx_data
/// segments this is the position a sequencer recorded in [`TxRef::tx_data_position`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TypedRecord<T> {
    pub position: BPosition,
    pub value: T,
}

/// Generic offline reader over an archive segment file. The type parameter
/// `T` is the rkyv-archived payload carried by every frame in this segment.
/// For tx_ordering archives use `T = TxOrderingMessage`; for tx_data archives
/// use `T = TxEnvelope`.
pub struct TypedSegmentReader<T> {
    bytes: Vec<u8>,
    pos: usize,
    _marker: PhantomData<T>,
}

impl<T> TypedSegmentReader<T>
where
    T: rkyv::Archive,
    T::Archived: rkyv::Deserialize<T, HighDeserializer<rancor::Error>>
        + for<'a> rkyv::bytecheck::CheckBytes<HighValidator<'a, rancor::Error>>,
{
    /// Open a segment file. `segment_path` is the full path to the `.rec`
    /// file (typically `<archive_dir>/<recording_id>-<segmentBase>.rec`).
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
    pub fn segment_path(archive_dir: &Path, recording_id: i64, segment_base: i64) -> PathBuf {
        archive_dir.join(format!("{recording_id}-{segment_base}.rec"))
    }

    /// Drain the reader into a `Vec<TypedRecord<T>>`. Convenience for
    /// pre-loading an entire (tx_data) segment into the per-A position
    /// index used by [`crate::multi_archive_reader::MultiArchiveReader`].
    pub fn collect_all(self) -> Result<Vec<TypedRecord<T>>, BatcherError> {
        self.collect::<Result<Vec<_>, _>>()
    }
}

impl<T> Iterator for TypedSegmentReader<T>
where
    T: rkyv::Archive,
    T::Archived: rkyv::Deserialize<T, HighDeserializer<rancor::Error>>
        + for<'a> rkyv::bytecheck::CheckBytes<HighValidator<'a, rancor::Error>>,
{
    type Item = Result<TypedRecord<T>, BatcherError>;

    fn next(&mut self) -> Option<Self::Item> {
        // Need at least a header.
        if self.pos + FRAME_HEADER_LEN > self.bytes.len() {
            return None;
        }
        let buf = &self.bytes[self.pos..];
        let len = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
        if len < FRAME_HEADER_LEN || self.pos + len > self.bytes.len() {
            // Truncated active segment — stop.
            return None;
        }
        // bytes 4..8 reserved
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

/// TxOrdering segment reader: yields [`TxOrderingMessage`] records (TxRef +
/// BoundaryStart) in canonical order.
pub type TxOrderingSegmentReader = TypedSegmentReader<TxOrderingMessage>;

/// TxData[i] segment reader: yields full [`TxEnvelope`] records in the
/// order sequencer `i` wrote them.
pub type TxDataSegmentReader = TypedSegmentReader<TxEnvelope>;

fn access_owned<T>(bytes: &[u8]) -> Result<T, BatcherError>
where
    T: rkyv::Archive,
    T::Archived: rkyv::Deserialize<T, HighDeserializer<rancor::Error>>
        + for<'a> rkyv::bytecheck::CheckBytes<HighValidator<'a, rancor::Error>>,
{
    rkyv::from_bytes::<T, rancor::Error>(bytes).map_err(|e| BatcherError::Codec(e.to_string()))
}

/// Append one frame to `out`. Helper used by tests and the future writer-side
/// adapter; encodes one record in the simplified KAR1-internal segment format.
///
/// The frame is type-agnostic: caller writes the same way for tx_data
/// (T = TxEnvelope) and tx_ordering (T = TxOrderingMessage).
pub fn append_frame<T>(out: &mut Vec<u8>, position: BPosition, value: &T)
where
    T: for<'a> rkyv::Serialize<
            rkyv::api::high::HighSerializer<
                rkyv::util::AlignedVec,
                rkyv::ser::allocator::ArenaHandle<'a>,
                rancor::Error,
            >,
        >,
{
    let payload = rkyv::to_bytes::<rancor::Error>(value).expect("rkyv encode");
    let total: u32 = (FRAME_HEADER_LEN + payload.len()) as u32;
    out.extend_from_slice(&total.to_le_bytes());
    out.extend_from_slice(&[0u8; 4]); // reserved (was: stream_kind + 3 reserved bytes; collapsed in)
    out.extend_from_slice(&position.term_id.to_le_bytes());
    out.extend_from_slice(&position.term_offset.to_le_bytes());
    out.extend_from_slice(payload.as_slice());
    // pad to 8-byte alignment
    while !out.len().is_multiple_of(FRAME_ALIGN) {
        out.push(0);
    }
}
