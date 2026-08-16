//! M-archive offline reader.
//!
//! After the canonical-ordering archive (tx_ordering) carries only
//! [`TxOrderingMessage`] records — `TxRef` (pointer into tx_data[i]) and
//! `BlockBoundaryStart` (sealer). Full [`TxEnvelope`] bytes live on the
//! per-sequencer tx_data archives.
//!
//! This module ties the two together for the offline batcher pipeline:
//!
//! 1. Open one [`TxOrderingSegmentReader`] for the canonical orderer.
//! 2. Open one [`TxDataSegmentReader`] per discovered sequencer; pre-load
//!    each into a `(BPosition -> TxEnvelope)` map. (For v0 the offline path
//!    materialises the per-A index in RAM — fine for batch sizes that fit a
//!    few segment files. Streaming/page-cache modes are a future scale-up.)
//! 3. Walk tx_ordering in order; for each [`TxOrderingMessage::TxRef`] resolve
//!    `(sequencer_id, tx_data_position)` against the right per-A index; for each
//!    [`TxOrderingMessage::BoundaryStart`] yield the boundary marker.
//!
//! The output is a stream of [`ResolvedRecord`]s — exactly the shape the
//! existing [`crate::batch::BatchAccumulator`] consumed before. The
//! BatchAccumulator therefore needs no behavioural change.
//!
//! Out-of-order opens — refs on B for A-positions that haven't been read
//! yet — are handled transparently: the per-A index is populated up front
//! from the segment file, so any position the B-walker encounters is either
//! present (we resolve it) or genuinely missing (we error). Tests exercise
//! both the in-order and out-of-order cases.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use kardamom_types::xchain::RemoteEpochRecord;
use kardamom_types::{BPosition, BlockBoundaryStart, TxEnvelope, TxOrderingMessage, TxRef};

use crate::archive_reader::{TxDataSegmentReader, TxOrderingSegmentReader};
use crate::error::BatcherError;

/// A record resolved from the M+1 archive topology back into the legacy
/// "stream of tx + boundary" shape that the [`crate::batch::BatchAccumulator`]
/// already understands. The `position` field is the canonical B-position of
/// the originating tx_ordering record (system invariant I1), not the tx_data
/// position the envelope was fetched from. Canonical L2 ordering is by B.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ResolvedRecord {
    Tx {
        /// Canonical position of the `TxRef` on tx_ordering. This is the value
        /// the batch accumulator stores in `RecordedTx::position`.
        position: BPosition,
        /// The originating tx_data position. Useful for diagnostics and
        /// for the per-A reader's own bookkeeping. Carried through so test
        /// asserts can verify resolution went to the right A-archive.
        sequencer_id: u8,
        tx_data_position: BPosition,
        env: TxEnvelope,
    },
    Boundary {
        position: BPosition,
        marker: BlockBoundaryStart,
    },
    /// A remote-epoch record (interop): one peer chain's contiguous
    /// outbox-message batch, messages by value. Fed to
    /// [`crate::batch::BatchAccumulator::observe_remote_epoch`] so the record
    /// travels in the DA payload of the block it leads (spec §16 Q8).
    RemoteEpoch {
        position: BPosition,
        record: RemoteEpochRecord,
    },
}

/// Configuration for the M-archive reader. `b_segment` is the tx_ordering
/// segment file path; `a_segments` maps `sequencer_id` to the corresponding
/// tx_data segment file path.
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
    /// Parse the `--tx_data-archive sid=path,sid=path,...` CLI form. Used
    /// by the CLI driver; centralised here so tests can exercise the same
    /// parser without depending on `clap`.
    pub fn parse_a_spec(spec: &str) -> Result<HashMap<u8, PathBuf>, BatcherError> {
        let mut out = HashMap::new();
        for entry in spec.split(',').map(str::trim).filter(|s| !s.is_empty()) {
            let (sid_str, path_str) = entry.split_once('=').ok_or_else(|| {
                BatcherError::Config(format!(
                    "tx_data-archive entry '{entry}' missing '=' separator"
                ))
            })?;
            let sid: u8 = sid_str.trim().parse().map_err(|e| {
                BatcherError::Config(format!(
                    "tx_data-archive sequencer id '{sid_str}' is not a u8: {e}"
                ))
            })?;
            let path = PathBuf::from(path_str.trim());
            if out.insert(sid, path).is_some() {
                return Err(BatcherError::Config(format!(
                    "tx_data-archive sequencer id {sid} listed twice"
                )));
            }
        }
        Ok(out)
    }
}

/// Per-sequencer tx_data position index: maps `BPosition` to the decoded
/// `TxEnvelope` so the B-walker can resolve refs in `O(1)`.
///
/// Built once per A-archive during [`MultiArchiveReader::open`].
type PerASegmentIndex = HashMap<BPosition, TxEnvelope>;

/// M-archive offline reader. Walks tx_ordering in canonical order, resolving
/// each `TxRef` against the per-sequencer A indexes.
pub struct MultiArchiveReader {
    b_reader: TxOrderingSegmentReader,
    /// `sequencer_id -> (BPosition -> TxEnvelope)` index.
    a_indexes: HashMap<u8, PerASegmentIndex>,
}

impl MultiArchiveReader {
    /// Open all archives, eagerly load per-A indexes, and return a reader
    /// that can be iterated to yield [`ResolvedRecord`]s in canonical order.
    pub fn open(cfg: &MultiArchiveConfig) -> Result<Self, BatcherError> {
        let b_reader = TxOrderingSegmentReader::open(&cfg.b_segment)?;
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

    /// Number of tx_data archives this reader is resolving against.
    pub fn a_archive_count(&self) -> usize {
        self.a_indexes.len()
    }

    /// Number of envelopes indexed for sequencer `sid` (0 if unknown).
    pub fn a_archive_len(&self, sid: u8) -> usize {
        self.a_indexes.get(&sid).map(|m| m.len()).unwrap_or(0)
    }

    fn resolve(&self, r: &TxRef) -> Result<TxEnvelope, BatcherError> {
        let idx = self.a_indexes.get(&r.shard_id).ok_or_else(|| {
            BatcherError::Config(format!(
                "tx_data archive for sequencer_id={} not configured",
                r.shard_id
            ))
        })?;
        idx.get(&r.tx_data_position).cloned().ok_or_else(|| {
            BatcherError::Frame(format!(
                "TxRef sequencer_id={} tx_data_position={:?} not found in tx_data index",
                r.shard_id, r.tx_data_position
            ))
        })
    }
}

impl Iterator for MultiArchiveReader {
    type Item = Result<ResolvedRecord, BatcherError>;

    fn next(&mut self) -> Option<Self::Item> {
        // The batcher only emits user-tx data + block boundaries to L1.
        // DepositRefs are L1-originated; we skip them by recursing until we
        // hit a record the batcher cares about.
        loop {
            let next_b = self.b_reader.next()?;
            let rec = match next_b {
                Err(e) => return Some(Err(e)),
                Ok(r) => r,
            };
            let position = rec.position;
            match rec.value {
                TxOrderingMessage::TxRef(r) => {
                    return match self.resolve(&r) {
                        Ok(env) => Some(Ok(ResolvedRecord::Tx {
                            position,
                            sequencer_id: r.shard_id,
                            tx_data_position: r.tx_data_position,
                            env,
                        })),
                        Err(e) => Some(Err(e)),
                    };
                }
                TxOrderingMessage::DepositRef(_) | TxOrderingMessage::Epoch(_) => {
                    // L1 deposits originated on L1; they don't need to be
                    // re-batched back. Skip.
                    //
                    // Epochs are skipped for the same reason and, under the
                    // deposit-derivation design, deliberately so: a
                    // reconstructor re-derives an epoch's deposits from L1
                    // itself, guided by the block's `l1_origin`, so putting
                    // them in the blob would be both wasted space and an
                    // unverifiable claim (deposits are unsigned). See
                    // docs/agents/l1-origin-deposit-derivation-spec.md.
                    continue;
                }
                TxOrderingMessage::RemoteEpoch(rec) => {
                    // Unlike epoch deposits, remote messages are NOT
                    // re-derivable from this chain's L1 origin, so they DO
                    // travel in DA (spec §16 Q8): yield the record — messages
                    // by value — for the accumulator to buffer into the block
                    // it leads (mirrors `live.rs`).
                    return Some(Ok(ResolvedRecord::RemoteEpoch {
                        position,
                        record: rec,
                    }));
                }
                TxOrderingMessage::BoundaryStart(b) => {
                    return Some(Ok(ResolvedRecord::Boundary {
                        position,
                        marker: b,
                    }));
                }
            }
        }
    }
}

/// Read a tx_data segment file fully and build the `BPosition ->
/// TxEnvelope` lookup.
pub fn load_a_index(path: &Path) -> Result<PerASegmentIndex, BatcherError> {
    let reader = TxDataSegmentReader::open(path)?;
    let mut idx = HashMap::new();
    for rec in reader {
        let rec = rec?;
        if idx.insert(rec.position, rec.value).is_some() {
            return Err(BatcherError::Frame(format!(
                "duplicate tx_data position {:?} in {}",
                rec.position,
                path.display()
            )));
        }
    }
    Ok(idx)
}
