//! Top-level batcher loop.
//!
//! - Pulls records from a stream-like source (segment reader or live archive).
//! - Feeds [`BatchAccumulator`], which yields a [`ClosedBlock`] at every
//!   `BlockBoundaryStart`.
//! - For each closed block, or group of blocks (set by `blocks_per_batch`),
//!   encodes KAR1, compresses with zstd if enabled, packs the result into
//!   blobs, and hands the batch to a [`Sender`] for L1 broadcast.
//!
//! This is a single-instance design for v1. There is no election or standby.
//! If the batcher process dies, the L2 stops settling blocks until an
//! operator restarts it.

use alloy_eips::eip4844::Blob;
use metrics::counter;

use crate::batch::{BatchAccumulator, ClosedBlock};
use crate::blob::pack_to_blobs;
use crate::compress::{DEFAULT_LEVEL, encode_zstd};
use crate::error::BatcherError;
use crate::frame::{BlockFrame, Kar1Payload, TxFrame, encode as frame_encode};

/// Metric names. Use `metrics::Recorder` to scrape them. The runtime sets up
/// a Prometheus exporter with `metrics-exporter-prometheus`.
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
    /// The batch records commitment: the fold of per-block digests over the
    /// batch's L2 tx identities. It uses the same `kardamom-types::prover`
    /// primitives as the batch guest. The settlement contract stores it, and
    /// the proof's public values must carry it for the proof oracle.
    pub records_commitment: alloy_primitives::B256,
}

/// A sink for posted batches. The production version wraps an alloy
/// provider and builds a 4844 transaction (see [`crate::settlement`]). The
/// test version only captures the batch.
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

/// In-process batcher state. It is generic over `Sender`, so tests can use
/// [`MockSender`] instead of the real settlement client.
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

    /// The reader thread calls this method when a `ClosedBlock` becomes
    /// available. If enough blocks are ready to form a batch, this method
    /// builds the blobs and sends them to the sender.
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

/// A pure helper that turns a group of `ClosedBlock`s into a `PostedBatch`.
///
/// Steps: encode KAR1, compress with zstd if enabled, then pack into at
/// most 6 blobs.
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
    let records_commitment = kardamom_types::batch_records_commitment(blocks.iter().map(|b| {
        let mut d = kardamom_types::BlockRecordsDigest::new(b.block_number);
        for t in &b.txs {
            d.add_tx(&t.envelope.raw_tx);
        }
        d.finish()
    }));
    Ok(PostedBatch {
        blobs,
        l2_block_start,
        l2_block_end,
        records_commitment,
    })
}

fn build_payload(blocks: &[ClosedBlock]) -> Kar1Payload {
    let block_frames = blocks
        .iter()
        .map(|b| BlockFrame {
            block_number: b.block_number,
            l2_timestamp: b.l2_timestamp,
            remote_epochs: b.remote_epochs.clone(),
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
    // Set `compressed = false` in the framing header. The outer zstd layer,
    // if any, wraps the framed buffer without changing it. The reader
    // detects compression by the zstd magic bytes. See `recon::is_zstd`.
    Kar1Payload {
        blocks: block_frames,
        compressed: false,
    }
}
