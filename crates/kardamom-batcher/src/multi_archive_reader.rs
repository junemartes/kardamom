//! M-archive offline reader (D-Sh12 / spec D11 / §2.8).
//!
//! After D-Sh12 the canonical-ordering archive (channel B) carries only
//! [`ChannelBMessage`] records — `TxRef` (pointer into channel A[i]) and
//! `BlockBoundaryStart` (sealer). Full [`TxEnvelope`] bytes live on the
//! per-sequencer channel A archives.
//!
//! This module ties the two together for the offline batcher pipeline:
//!
//! 1. Open one [`ChannelBSegmentReader`] for the canonical orderer.
//! 2. Open one [`ChannelASegmentReader`] per discovered sequencer; pre-load
//!    each into a `(BPosition -> TxEnvelope)` map. (For v0 the offline path
//!    materialises the per-A index in RAM — fine for batch sizes that fit a
//!    few segment files. Streaming/page-cache modes are a future scale-up.)
//! 3. Walk channel B in order; for each [`ChannelBMessage::TxRef`] resolve
//!    `(sequencer_id, position_a)` against the right per-A index; for each
//!    [`ChannelBMessage::BoundaryStart`] yield the boundary marker.
//!
//! The output is a stream of [`ResolvedRecord`]s — exactly the shape the
//! existing [`crate::batch::BatchAccumulator`] consumed before D-Sh12. The
//! BatchAccumulator therefore needs no behavioural change.
//!
//! Out-of-order opens — refs on B for A-positions that haven't been read
//! yet — are handled transparently: the per-A index is populated up front
//! from the segment file, so any position the B-walker encounters is either
//! present (we resolve it) or genuinely missing (we error). Tests exercise
//! both the in-order and out-of-order cases.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use kardamom_types::{BPosition, BlockBoundaryStart, ChannelBMessage, TxEnvelope, TxRef};

use crate::archive_reader::{ChannelASegmentReader, ChannelBSegmentReader};
use crate::error::BatcherError;

/// A record resolved from the M+1 archive topology back into the legacy
/// "stream of tx + boundary" shape that the [`crate::batch::BatchAccumulator`]
/// already understands. The `position` field is the canonical B-position of
/// the originating channel-B record (system invariant I1), not the channel-A
/// position the envelope was fetched from. Canonical L2 ordering is by B.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ResolvedRecord {
    Tx {
        /// Canonical position of the `TxRef` on channel B. This is the value
        /// the batch accumulator stores in `RecordedTx::position`.
        position: BPosition,
        /// The originating channel-A position. Useful for diagnostics and
        /// for the per-A reader's own bookkeeping. Carried through so test
        /// asserts can verify resolution went to the right A-archive.
        sequencer_id: u8,
        position_a: BPosition,
        env: TxEnvelope,
    },
    Boundary {
        position: BPosition,
        marker: BlockBoundaryStart,
    },
}

/// Configuration for the M-archive reader. `b_segment` is the channel-B
/// segment file path; `a_segments` maps `sequencer_id` to the corresponding
/// channel-A segment file path.
///
/// For v0 each archive is one segment file; multi-segment iteration is the
/// same algorithm applied to consecutive files and is left for a follow-up
/// (the offline batcher consumes whole epochs at a time, so segment-roll
/// handling happens at the orchestration layer, not in the reader itself).
#[derive(Clone, Debug)]
pub struct MultiArchiveConfig {
    pub b_segment: PathBuf,
    pub a_segments: HashMap<u8, PathBuf>,
}

impl MultiArchiveConfig {
    /// Parse the `--channel-a-archive sid=path,sid=path,...` CLI form. Used
    /// by the CLI driver; centralised here so tests can exercise the same
    /// parser without depending on `clap`.
    pub fn parse_a_spec(spec: &str) -> Result<HashMap<u8, PathBuf>, BatcherError> {
        let mut out = HashMap::new();
        for entry in spec.split(',').map(str::trim).filter(|s| !s.is_empty()) {
            let (sid_str, path_str) = entry.split_once('=').ok_or_else(|| {
                BatcherError::Config(format!(
                    "channel-a-archive entry '{entry}' missing '=' separator"
                ))
            })?;
            let sid: u8 = sid_str.trim().parse().map_err(|e| {
                BatcherError::Config(format!(
                    "channel-a-archive sequencer id '{sid_str}' is not a u8: {e}"
                ))
            })?;
            let path = PathBuf::from(path_str.trim());
            if out.insert(sid, path).is_some() {
                return Err(BatcherError::Config(format!(
                    "channel-a-archive sequencer id {sid} listed twice"
                )));
            }
        }
        Ok(out)
    }
}

/// Per-sequencer channel-A position index: maps `BPosition` to the decoded
/// `TxEnvelope` so the B-walker can resolve refs in `O(1)`.
///
/// Built once per A-archive during [`MultiArchiveReader::open`].
type PerASegmentIndex = HashMap<BPosition, TxEnvelope>;

/// M-archive offline reader. Walks channel B in canonical order, resolving
/// each `TxRef` against the per-sequencer A indexes.
pub struct MultiArchiveReader {
    b_reader: ChannelBSegmentReader,
    /// `sequencer_id -> (BPosition -> TxEnvelope)` index.
    a_indexes: HashMap<u8, PerASegmentIndex>,
}

impl MultiArchiveReader {
    /// Open all archives, eagerly load per-A indexes, and return a reader
    /// that can be iterated to yield [`ResolvedRecord`]s in canonical order.
    pub fn open(cfg: &MultiArchiveConfig) -> Result<Self, BatcherError> {
        let b_reader = ChannelBSegmentReader::open(&cfg.b_segment)?;
        let mut a_indexes = HashMap::with_capacity(cfg.a_segments.len());
        for (sid, path) in &cfg.a_segments {
            let idx = load_a_index(path)?;
            a_indexes.insert(*sid, idx);
        }
        Ok(Self {
            b_reader,
            a_indexes,
        })
    }

    /// Open the channel-B reader only, with **explicitly-supplied**
    /// per-A indexes. Useful for tests that synthesise an in-memory index
    /// rather than write a segment file to disk.
    pub fn with_indexes(
        b_segment: &Path,
        a_indexes: HashMap<u8, PerASegmentIndex>,
    ) -> Result<Self, BatcherError> {
        let b_reader = ChannelBSegmentReader::open(b_segment)?;
        Ok(Self {
            b_reader,
            a_indexes,
        })
    }

    /// Number of channel-A archives this reader is resolving against.
    pub fn a_archive_count(&self) -> usize {
        self.a_indexes.len()
    }

    /// Number of envelopes indexed for sequencer `sid` (0 if unknown).
    pub fn a_archive_len(&self, sid: u8) -> usize {
        self.a_indexes.get(&sid).map(|m| m.len()).unwrap_or(0)
    }

    fn resolve(&self, r: &TxRef) -> Result<TxEnvelope, BatcherError> {
        let idx = self.a_indexes.get(&r.sequencer_id).ok_or_else(|| {
            BatcherError::Config(format!(
                "channel-A archive for sequencer_id={} not configured",
                r.sequencer_id
            ))
        })?;
        idx.get(&r.position_a).cloned().ok_or_else(|| {
            BatcherError::Frame(format!(
                "TxRef sequencer_id={} position_a={:?} not found in channel-A index",
                r.sequencer_id, r.position_a
            ))
        })
    }
}

impl Iterator for MultiArchiveReader {
    type Item = Result<ResolvedRecord, BatcherError>;

    fn next(&mut self) -> Option<Self::Item> {
        let next_b = self.b_reader.next()?;
        let rec = match next_b {
            Err(e) => return Some(Err(e)),
            Ok(r) => r,
        };
        let position = rec.position;
        match rec.value {
            ChannelBMessage::TxRef(r) => match self.resolve(&r) {
                Ok(env) => Some(Ok(ResolvedRecord::Tx {
                    position,
                    sequencer_id: r.sequencer_id,
                    position_a: r.position_a,
                    env,
                })),
                Err(e) => Some(Err(e)),
            },
            ChannelBMessage::BoundaryStart(b) => Some(Ok(ResolvedRecord::Boundary {
                position,
                marker: b,
            })),
        }
    }
}

/// Read a channel-A segment file fully and build the `BPosition ->
/// TxEnvelope` lookup.
pub fn load_a_index(path: &Path) -> Result<PerASegmentIndex, BatcherError> {
    let reader = ChannelASegmentReader::open(path)?;
    let mut idx = HashMap::new();
    for rec in reader {
        let rec = rec?;
        if idx.insert(rec.position, rec.value).is_some() {
            return Err(BatcherError::Frame(format!(
                "duplicate channel-A position {:?} in {}",
                rec.position,
                path.display()
            )));
        }
    }
    Ok(idx)
}
