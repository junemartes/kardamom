//! Offline Aeron Archive segment reader.
//!
//! S0 D-Sh10: the batcher reads from on-disk Aeron Archive segment files. It
//! does not subscribe to channel C, does not talk to the live sequencer.
//!
//! Aeron's real on-disk frame format is documented in `aeron-driver`; for v0
//! the offline reader operates over a **simplified KAR1-internal frame format**
//! that the batcher itself can write (used by tests + the in-process reader
//! path). The real-Aeron live path (Task 11) uses `rusteron-archive`'s replay
//! protocol; the filesystem path documented here covers the spec when the
//! batcher has direct read access to the archive directory.
//!
//! The simplified format used by [`SegmentReader`] is one frame per record:
//!
//! ```text
//!   length      u32 LE      total frame length including this header
//!   stream_kind u8          0 = tx (TxEnvelope), 1 = boundary (BlockBoundaryStart)
//!   reserved    u24 zero
//!   term_id     i32 LE
//!   term_offset i32 LE
//!   payload     length - 16 bytes
//!   pad         zero, to next 8-byte boundary
//! ```
//!
//! Each payload is a rkyv-archived value of the corresponding `kardamom-types`
//! struct. The active segment may end mid-frame; the reader truncates at the
//! last full frame.

use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};

use kardamom_types::{BPosition, BlockBoundaryStart, TxEnvelope};
use rkyv::api::high::{HighDeserializer, HighValidator};
use rkyv::rancor;

use crate::error::BatcherError;

pub const STREAM_KIND_TX: u8 = 0;
pub const STREAM_KIND_BOUNDARY: u8 = 1;

const FRAME_HEADER_LEN: usize = 4 + 1 + 3 + 4 + 4; // 16 bytes
const FRAME_ALIGN: usize = 8;

/// A single record decoded from a segment file.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SegmentRecord {
    Tx {
        position: BPosition,
        env: TxEnvelope,
    },
    Boundary {
        position: BPosition,
        marker: BlockBoundaryStart,
    },
}

/// Iterator-style reader over one or more archive segment files. For the v0
/// filesystem path we accept a single segment file plus the descriptor fields;
/// multi-segment iteration is the same algorithm applied to consecutive files.
pub struct SegmentReader {
    bytes: Vec<u8>,
    pos: usize,
}

impl SegmentReader {
    /// Open a segment file. `segment_path` is the full path to the `.rec`
    /// file (typically `<archive_dir>/<recording_id>-<segmentBase>.rec`).
    pub fn open(segment_path: &Path) -> Result<Self, BatcherError> {
        let mut f = File::open(segment_path)?;
        let mut bytes = Vec::new();
        f.read_to_end(&mut bytes)?;
        Ok(Self { bytes, pos: 0 })
    }

    /// Compose the canonical segment file path:
    /// `<archive_dir>/<recording_id>-<segment_base_position>.rec`.
    pub fn segment_path(archive_dir: &Path, recording_id: i64, segment_base: i64) -> PathBuf {
        archive_dir.join(format!("{recording_id}-{segment_base}.rec"))
    }
}

impl Iterator for SegmentReader {
    type Item = Result<SegmentRecord, BatcherError>;

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
        let stream_kind = buf[4];
        // bytes 5..8 reserved
        let term_id = i32::from_le_bytes([buf[8], buf[9], buf[10], buf[11]]);
        let term_offset = i32::from_le_bytes([buf[12], buf[13], buf[14], buf[15]]);
        let payload = &buf[FRAME_HEADER_LEN..len];

        let position = BPosition {
            term_id,
            term_offset,
        };

        let rec = match stream_kind {
            STREAM_KIND_TX => match access_owned::<TxEnvelope>(payload) {
                Ok(env) => Ok(SegmentRecord::Tx { position, env }),
                Err(e) => Err(e),
            },
            STREAM_KIND_BOUNDARY => match access_owned::<BlockBoundaryStart>(payload) {
                Ok(marker) => Ok(SegmentRecord::Boundary { position, marker }),
                Err(e) => Err(e),
            },
            other => Err(BatcherError::Frame(format!("unknown stream_kind {other}"))),
        };

        // Advance by the aligned frame length.
        let aligned = len.div_ceil(FRAME_ALIGN) * FRAME_ALIGN;
        self.pos += aligned.min(self.bytes.len() - self.pos);

        Some(rec)
    }
}

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
pub fn append_frame<T>(out: &mut Vec<u8>, stream_kind: u8, position: BPosition, value: &T)
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
    out.push(stream_kind);
    out.extend_from_slice(&[0, 0, 0]); // reserved
    out.extend_from_slice(&position.term_id.to_le_bytes());
    out.extend_from_slice(&position.term_offset.to_le_bytes());
    out.extend_from_slice(payload.as_slice());
    // pad to 8-byte alignment
    while !out.len().is_multiple_of(FRAME_ALIGN) {
        out.push(0);
    }
}
