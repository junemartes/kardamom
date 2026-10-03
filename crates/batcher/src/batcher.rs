//! Top-level batcher loop.
//!
//! - Pulls records from a stream-like source (segment reader or live archive).
//! - Feeds [`BatchAccumulator`], which yields a [`ClosedBlock`] at every
//!   `BlockBoundaryStart`.
//! - For each closed block, or group of blocks (set by `blocks_per_batch`),
//!   encodes KAR1, compresses with zstd if enabled, packs the result into
//!   payload, and hands the batch to a [`Sender`] for L1 broadcast.
//!
//! This is a single-instance design: no election or standby. If the batcher
//! process dies, the L2 stops settling blocks until an operator restarts
//! it.

use std::num::NonZeroUsize;

use metrics::counter;

use crate::batch::{BatchAccumulator, ClosedBlock};
use crate::compress::{DEFAULT_LEVEL, encode_zstd};
use crate::error::BatcherError;
use kardamom_types::kar1::{BlockCursor, BlockFrame, Kar1Payload, TxFrame, encode as frame_encode};

/// Metric names. Use `metrics::Recorder` to scrape them. The runtime sets up
/// a Prometheus exporter with `metrics-exporter-prometheus`.
pub mod metric_names {
    pub const BLOCKS_OBSERVED: &str = "kardamom_batcher_blocks_observed_total";
    pub const BATCHES_POSTED: &str = "kardamom_batcher_batches_posted_total";
    pub const PAYLOAD_BYTES_POSTED: &str = "kardamom_batcher_payload_bytes_posted_total";
}

/// Configuration for the batching loop.
#[derive(Clone, Debug)]
pub struct BatcherConfig {
    /// Number of closed blocks to group into a single L1 post. Defaults to
    /// one. Nonzero at the type level: zero would make every "group is
    /// full" comparison at the post site vacuously true.
    pub blocks_per_batch: NonZeroUsize,
    /// Whether to zstd-compress the framed payload before it is posted.
    pub compress: bool,
    /// The most bytes one post's payload may have. EigenDA takes 16 MiB
    /// per blob; the proxy's encoding adds a little, so the default leaves
    /// room. Nonzero at the type level: a zero ceiling fits no block.
    pub max_payload_bytes: NonZeroUsize,
    /// zstd compression level when `compress` is true.
    pub compression_level: i32,
    /// The L2 chain id of the chain this batcher posts for. The records
    /// commitment digests each remote-epoch message leaf, and the leaf
    /// commits to the destination chain id. Defaults to 1, like the
    /// sibling services' `--chain-id`.
    pub chain_id: u64,
}

impl Default for BatcherConfig {
    fn default() -> Self {
        Self {
            blocks_per_batch: NonZeroUsize::MIN,
            compress: true,
            compression_level: DEFAULT_LEVEL,
            max_payload_bytes: DEFAULT_MAX_PAYLOAD_BYTES,
            chain_id: 1,
        }
    }
}

/// The default payload ceiling: 15 MiB, under EigenDA's 16 MiB blob.
pub const DEFAULT_MAX_PAYLOAD_BYTES: NonZeroUsize = NonZeroUsize::new(15 * 1024 * 1024).unwrap();

/// A batch ready to post to L1.
#[derive(Clone, Debug)]
pub struct PostedBatch {
    /// The framed blocks, zstd-compressed when the config says so: the
    /// bytes EigenDA holds.
    pub payload: Vec<u8>,
    pub l2_block_start: u64,
    pub l2_block_end: u64,
    /// The batch records commitment: the fold of per-block digests over the
    /// batch's L2 tx identities. It uses the same `kardamom-types::prover`
    /// primitives as the batch guest. The `settlement` contract stores it, and
    /// the proof's public values must carry it for the proof oracle.
    pub records_commitment: alloy_primitives::B256,
}

/// A sink for posted batches. The production version wraps an alloy
/// provider and builds a 4844 transaction (see [`crate::settlement`]). The
/// test version only captures the batch.
pub trait Sender {
    /// # Errors
    /// Returns an error when the batch cannot be sent (for example, an L1
    /// transaction failure in the production sink).
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
/// [`MockSender`] instead of the real `settlement` client.
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
    /// packs the payload and sends it to the sender.
    ///
    /// # Errors
    /// Returns an error when packing the group fails, or when
    /// [`Sender::post`] fails.
    pub fn on_closed_block(&mut self, block: ClosedBlock) -> Result<(), BatcherError> {
        counter!(metric_names::BLOCKS_OBSERVED).increment(1);
        self.pending_blocks.push(block);
        if self.pending_blocks.len() < self.cfg.blocks_per_batch.get() {
            return Ok(());
        }
        let group = std::mem::take(&mut self.pending_blocks);
        for batch in pack_block_groups(&self.cfg, &group)? {
            let payload_bytes = batch.payload.len() as u64;
            self.sender.post(batch)?;
            counter!(metric_names::BATCHES_POSTED).increment(1);
            counter!(metric_names::PAYLOAD_BYTES_POSTED).increment(payload_bytes);
        }
        Ok(())
    }
}

/// A pure helper that turns a group of `ClosedBlock`s into a `PostedBatch`.
///
/// Steps: encode KAR1, compress with zstd if enabled, then pack into at
/// most `max_payload_bytes`. A group that overflows the ceiling
/// is an error. Use [`pack_block_groups`] to split such a group.
///
/// # Errors
/// Returns an error when `blocks` is empty, when frame encoding fails, or
/// when the group overflows the payload ceiling.
pub fn pack_blocks(
    cfg: &BatcherConfig,
    blocks: &[ClosedBlock],
) -> Result<PostedBatch, BatcherError> {
    let (Some(first_block), Some(last_block)) = (blocks.first(), blocks.last()) else {
        return Err(BatcherError::Frame("cannot pack zero blocks".into()));
    };
    let payload = build_payload(blocks);
    let framed = frame_encode(&payload)?;
    let to_pack = if cfg.compress {
        encode_zstd(&framed, cfg.compression_level)?
    } else {
        framed
    };
    let ceiling = cfg.max_payload_bytes.get();
    if to_pack.len() > ceiling {
        if let [only] = blocks {
            return Err(BatcherError::BlockTooLarge {
                block_number: only.block_number,
                bytes: to_pack.len(),
                ceiling,
            });
        }
        return Err(BatcherError::Payload(format!(
            "batch overflowed the {ceiling}-byte ceiling: {} bytes",
            to_pack.len()
        )));
    }
    let l2_block_start = first_block.block_number;
    let l2_block_end = last_block.block_number;
    let records_commitment = kardamom_types::batch_records_commitment(
        blocks.iter().map(|b| block_records_digest(cfg.chain_id, b)),
    );
    Ok(PostedBatch {
        payload: to_pack,
        l2_block_start,
        l2_block_end,
        records_commitment,
    })
}

/// Split a group of `ClosedBlock`s into as many batches as the payload
/// ceiling needs, in block order.
///
/// The rule: take the largest prefix that packs to at most
/// `max_payload_bytes`, post it, then repeat on the rest. A prefix search
/// by bisection assumes that a longer prefix packs to more bytes. zstd
/// can break that assumption. That only costs an extra batch:
/// every batch this function returns did pack within the ceiling. A block
/// that overflows the ceiling on its own returns
/// [`BatcherError::BlockTooLarge`].
///
/// # Errors
/// Returns [`BatcherError::BlockTooLarge`] for a block that overflows on
/// its own, and every [`pack_blocks`] error otherwise.
pub fn pack_block_groups(
    cfg: &BatcherConfig,
    blocks: &[ClosedBlock],
) -> Result<Vec<PostedBatch>, BatcherError> {
    let mut out = Vec::new();
    let mut rest = blocks;
    while !rest.is_empty() {
        let (batch, taken) = pack_largest_prefix(cfg, rest)?;
        out.push(batch);
        rest = &rest[taken..];
    }
    Ok(out)
}

/// Pack the largest prefix of `blocks` that fits the payload ceiling. Returns
/// the batch and the prefix length.
fn pack_largest_prefix(
    cfg: &BatcherConfig,
    blocks: &[ClosedBlock],
) -> Result<(PostedBatch, usize), BatcherError> {
    match pack_blocks(cfg, blocks) {
        Ok(batch) => return Ok((batch, blocks.len())),
        Err(e) if e.is_payload_overflow() => {}
        Err(e) => return Err(e),
    }
    // The whole group overflows. A single block does not reach here: it
    // fails as `BlockTooLarge` above. Bisect on the prefix length.
    let mut search = PrefixSearch {
        best: None,
        lo: 0,
        hi: blocks.len(),
    };
    while search.open() {
        search.probe(cfg, blocks)?;
    }
    match search.best {
        Some(found) => Ok(found),
        // No prefix of length >= 1 fit, so the first block alone overflows.
        None => pack_blocks(cfg, &blocks[..1]).map(|b| (b, 1)),
    }
}

/// The bisection state of [`pack_largest_prefix`]: the longest
/// known-good prefix is `lo` (with its batch in `best`); `hi` is known to
/// overflow.
struct PrefixSearch {
    best: Option<(PostedBatch, usize)>,
    lo: usize,
    hi: usize,
}

impl PrefixSearch {
    /// True while a prefix length between `lo` and `hi` is untested.
    fn open(&self) -> bool {
        self.hi.saturating_sub(self.lo) > 1
    }

    /// Pack the midpoint prefix, and move `lo` or `hi` to it.
    fn probe(&mut self, cfg: &BatcherConfig, blocks: &[ClosedBlock]) -> Result<(), BatcherError> {
        let mid = self.lo.midpoint(self.hi);
        match pack_blocks(cfg, &blocks[..mid]) {
            Ok(batch) => {
                self.best = Some((batch, mid));
                self.lo = mid;
                Ok(())
            }
            Err(e) if e.is_payload_overflow() => {
                self.hi = mid;
                Ok(())
            }
            Err(e) => Err(e),
        }
    }
}

/// The per-block records digest the L1 commitment folds. Remote-epoch
/// records lead the block, so their arms come before the tx arms. See
/// [`kardamom_types::BlockRecordsDigest`] for the layout.
#[must_use]
pub fn block_records_digest(chain_id: u64, block: &ClosedBlock) -> alloy_primitives::B256 {
    let mut d = kardamom_types::BlockRecordsDigest::new(block.block_number);
    for rec in &block.remote_epochs {
        d.add_remote_epoch(chain_id, rec);
    }
    for t in &block.txs {
        d.add_tx(&t.envelope.raw_tx);
    }
    d.finish()
}

fn build_payload(blocks: &[ClosedBlock]) -> Kar1Payload {
    let block_frames = blocks
        .iter()
        .map(|b| BlockFrame {
            block_number: b.block_number,
            l2_timestamp: b.l2_timestamp,
            cursor: Some(BlockCursor {
                end_tx_idx: b.end_tx_idx.as_index(),
                l1_origin: b.l1_origin,
            }),
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
