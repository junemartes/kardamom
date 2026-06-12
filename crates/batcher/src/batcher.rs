//! Top-level batcher loop.
//!
//! - Pulls records from a stream-like source (segment reader or live archive).
//! - Feeds [`BatchAccumulator`] which yields [`ClosedBlock`]s at every
//!   `BlockBoundaryStart`.
//! - For each closed block (or group of blocks — configurable via
//!   `blocks_per_batch`), encodes KAR1, optionally zstd-compresses, packs into
//!   blobs, and hands the packed batch to a [`Sender`] for L1 broadcast.
//!
//! Single-instance for v1: there is no election or standby. If the batcher
//! process dies the L2 stops settling blocks until an operator restarts it.

use alloy_eips::eip4844::Blob;
use metrics::counter;

use crate::batch::{BatchAccumulator, ClosedBlock};
use crate::blob::pack_to_blobs;
use crate::compress::{DEFAULT_LEVEL, encode_zstd};
use crate::error::BatcherError;
use crate::frame::{BlockFrame, Kar1Payload, TxFrame, encode as frame_encode};

/// Metric names. Use `metrics::Recorder` to scrape; the runtime wires up a
/// Prometheus exporter via `metrics-exporter-prometheus`.
pub mod metric_names {
    pub const BLOCKS_OBSERVED: &str = "kardamom_batcher_blocks_observed_total";
    pub const BATCHES_POSTED: &str = "kardamom_batcher_batches_posted_total";
    pub const BLOBS_POSTED: &str = "kardamom_batcher_blobs_posted_total";
}

/// Configuration for the batching loop.
#[derive(Clone, Debug)]
pub struct BatcherConfig {
    /// Number of closed blocks to group into a single L1 post. Defaults to 1.
    pub blocks_per_batch: usize,
    /// Whether to zstd-compress the framed payload before blob packing.
    pub compress: bool,
    /// zstd compression level when `compress` is true.
    pub compression_level: i32,
}

impl Default for BatcherConfig {
    fn default() -> Self {
        Self {
            blocks_per_batch: 1,
            compress: true,
            compression_level: DEFAULT_LEVEL,
        }
    }
}

/// A batch ready to post to L1.
#[derive(Clone, Debug)]
pub struct PostedBatch {
    pub blobs: Vec<Blob>,
    pub l2_block_start: u64,
    pub l2_block_end: u64,
}

/// Sink for posted batches. The production impl wraps an alloy provider and
/// builds a 4844 transaction (see [`crate::settlement`]); the test impl just
/// captures.
pub trait Sender {
    fn post(&mut self, batch: PostedBatch) -> Result<(), BatcherError>;
}

#[derive(Debug, Default)]
pub struct MockSender {
    pub sent: Vec<PostedBatch>,
}

impl Sender for MockSender {
    fn post(&mut self, batch: PostedBatch) -> Result<(), BatcherError> {
        self.sent.push(batch);
        Ok(())
    }
}

/// In-process Batcher state. Generic over the `Sender` so tests can substitute
/// [`MockSender`] for the real settlement client.
pub struct Batcher<S> {
    cfg: BatcherConfig,
    accumulator: BatchAccumulator,
    sender: S,
    /// Pending closed blocks waiting to be grouped into a batch.
    pending_blocks: Vec<ClosedBlock>,
}

impl<S: Sender> Batcher<S> {
    pub fn new(cfg: BatcherConfig, sender: S) -> Self {
        Self {
            cfg,
            accumulator: BatchAccumulator::new(),
            sender,
            pending_blocks: Vec::new(),
        }
    }

    pub fn accumulator(&mut self) -> &mut BatchAccumulator {
        &mut self.accumulator
    }

    pub fn sender(&self) -> &S {
        &self.sender
    }

    /// Called by the reader thread whenever a `ClosedBlock` becomes available.
    /// If we have enough blocks to form a batch, builds the blobs and forwards
    /// to the sender.
    pub fn on_closed_block(&mut self, block: ClosedBlock) -> Result<(), BatcherError> {
        counter!(metric_names::BLOCKS_OBSERVED).increment(1);
        self.pending_blocks.push(block);
        if self.pending_blocks.len() < self.cfg.blocks_per_batch {
            return Ok(());
        }
        let group = std::mem::take(&mut self.pending_blocks);
        let batch = pack_blocks(&self.cfg, &group)?;
        let blob_count = batch.blobs.len() as u64;
        self.sender.post(batch)?;
        counter!(metric_names::BATCHES_POSTED).increment(1);
        counter!(metric_names::BLOBS_POSTED).increment(blob_count);
        Ok(())
    }
}

/// Pure helper: turn a group of `ClosedBlock`s into a `PostedBatch`.
///
/// Encode KAR1 → optionally zstd-compress → pack into ≤6 blobs.
pub fn pack_blocks(
    cfg: &BatcherConfig,
    blocks: &[ClosedBlock],
) -> Result<PostedBatch, BatcherError> {
    if blocks.is_empty() {
        return Err(BatcherError::Frame("cannot pack zero blocks".into()));
    }
    let payload = build_payload(blocks);
    let framed = frame_encode(&payload)?;
    let to_pack = if cfg.compress {
        encode_zstd(&framed, cfg.compression_level)?
    } else {
        framed
    };
    let blobs = pack_to_blobs(&to_pack)?;
    if blobs.len() > 6 {
        return Err(BatcherError::Blob(format!(
            "batch overflowed 6-blob ceiling: produced {}",
            blobs.len()
        )));
    }
    let l2_block_start = blocks.first().map(|b| b.block_number).unwrap_or(0);
    let l2_block_end = blocks.last().map(|b| b.block_number).unwrap_or(0);
    Ok(PostedBatch {
        blobs,
        l2_block_start,
        l2_block_end,
    })
}

fn build_payload(blocks: &[ClosedBlock]) -> Kar1Payload {
    let block_frames = blocks
        .iter()
        .map(|b| BlockFrame {
            block_number: b.block_number,
            l2_timestamp: b.l2_timestamp,
            txs: b
                .txs
                .iter()
                .map(|t| TxFrame {
                    correlation_id: t.envelope.correlation_id,
                    sender: t.envelope.sender,
                    tx_hash: t.envelope.tx_hash,
                    raw_tx: t.envelope.raw_tx.clone(),
                })
                .collect(),
        })
        .collect();
    // `compressed = false` in the framing header: the outer zstd layer (if any)
    // wraps the framed buffer transparently, so the reader detects compression
    // via the zstd magic. See `recon::is_zstd`.
    Kar1Payload {
        blocks: block_frames,
        compressed: false,
    }
}
