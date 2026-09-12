//! Executor-side BAL publication.
//!
//! Publishes one EIP-7928 Block Access List frame per block on `tx_bal`.
//!
//! Emission is not best-effort. Once BALs drive the validator's parallel
//! re-execution, their availability over the catch-up window is a
//! liveness property. A validator that lags and finds BALs missing must
//! re-execute sequentially, and at high tps sequential re-execution can
//! be slower than the chain, so it would never catch up. So the code
//! emits every block's state transition, with an acked publish and a
//! bounded retry deadline, from a dedicated thread. The exec thread's
//! only cost is moving the captured `Bal` onto a channel.
//!
//! The frame is a [`BalFrame`]: the receipts-free merged `BlockDelta`
//! (receipts dominate byte size, and a fat frame shrinks the
//! validator's lapse window) plus the canonical RLP-encoded
//! `alloy_eip7928::BlockAccessList`, carrying per-slot `(tx_index, value)`
//! writes and per-account storage reads.

use std::num::NonZeroU16;
use std::ops::ControlFlow;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use alloy_rlp::Encodable;
use crossbeam_channel::{Receiver, RecvTimeoutError};
use kardamom_engine::actor::BalHandoff;
use kardamom_log::aeron_live::PubHandle;
use kardamom_types::{BalFrame, BlockDelta};

/// Default attribution granularity written into the frame: K-tx chunks
/// that match the validator's scheduling unit (`--validation-batch-size`,
/// default 8; at K > 1 the validator chunk-aligns its batches to the
/// frame's K anyway, see `parallel::execute_block_parallel`).
///
/// The BAL is a seeding artifact: its claims exist so the validator's
/// batches can re-execute in parallel, and the seeding induction only
/// ever consumes chunk-final values. So chunk-boundary verification is
/// all the artifact needs, and quantizing costs no parallelism. Per-tx
/// execution integrity comes from the per-tx receipts cross-check
/// (`write_set_hash`), not from BAL attribution. Chunk-collapsing
/// shrinks frames (within-chunk writes to one item collapse to the
/// chunk-final value), which protects the validator's lapse window.
///
/// `1` (per-tx, the standard EIP-7928 artifact) stays available through
/// the override, for external consumers or divergence forensics. If the
/// chunk-collapsed path ever diverges live, the flight recorder dumps
/// replay inputs at the point of failure; see `kardamom_validator::flight`.
const DEFAULT_GRANULARITY: NonZeroU16 = NonZeroU16::new(8).unwrap();

/// Per-frame publish deadline. A frame that cannot be delivered live
/// within this window is still retained (replay-serving is the
/// durability path). The deadline only bounds how long one frame
/// occupies the publisher. It must be well under the block interval: at
/// 2s (the tick length), a `NOT_CONNECTED` window drained at exactly the
/// arrival rate, so the bounded handoff channel hovered at capacity and
/// any jitter back-pressured the exec thread. 500ms keeps the drain rate
/// 4x the arrival rate during subscriber outages.
const PUBLISH_DEADLINE: Duration = Duration::from_millis(500);

/// The catch-up window the `BAL_RETAINED_BLOCKS` gauge reports: the
/// last 256 published blocks, about 8.5 min at the 2s tick. The frames
/// themselves live in the Aeron archive ring, not in this process.
const RETENTION_BLOCKS: usize = 256;

/// Quantize a Bal's tx indices into K-sized chunks. Within a chunk,
/// writes to the same item collapse to the chunk-final value. This
/// applies to the exported alloy BAL, so the wire artifact matches what
/// the validator will verify at that granularity.
use kardamom_engine::bal_ladder::quantize;

/// One handoff's encoded wire frame: the rkyv bytes, plus the BAL
/// section's size (for telemetry).
struct EncodedFrame {
    bytes: Vec<u8>,
    bal_bytes: usize,
}

impl EncodedFrame {
    /// Encode one handoff into a wire frame at `granularity`.
    fn encode(
        delta: BlockDelta,
        bal: revm::state::bal::Bal,
        granularity: NonZeroU16,
    ) -> Result<Self, String> {
        let alloy_bal = quantize(bal.into_alloy_bal(), granularity.get());
        let mut bal_rlp = Vec::new();
        alloy_bal.encode(&mut bal_rlp);
        let bal_bytes = bal_rlp.len();
        let frame = BalFrame {
            delta,
            bal_rlp,
            granularity,
        };
        kardamom_log::codec::encode(&frame)
            .map(|b| Self {
                bytes: b.to_vec(),
                bal_bytes,
            })
            .map_err(|e| e.to_string())
    }

    /// Copy the bytes into the aligned buffer the publish API expects.
    fn aligned(&self) -> rkyv::util::AlignedVec {
        let mut av = rkyv::util::AlignedVec::new();
        av.extend_from_slice(&self.bytes);
        av
    }
}

/// One publish attempt's terminal outcome, reported as the
/// `BAL_PUBLISH_TOTAL` metric's `outcome` label.
#[derive(Clone, Copy)]
enum PublishOutcome {
    Ok,
    NotConnected,
    DeadlineExhausted,
}

impl PublishOutcome {
    /// The `BAL_PUBLISH_TOTAL` metric's `outcome` label value.
    const fn label(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::NotConnected => "not_connected",
            Self::DeadlineExhausted => "deadline",
        }
    }
}

/// How many recent blocks the archive ring can serve to a validator
/// that catches up. Counts published blocks up to `RETENTION_BLOCKS`
/// and reports the count on the `BAL_RETAINED_BLOCKS` gauge.
#[derive(Default)]
struct RetentionWindow {
    blocks: usize,
}

impl RetentionWindow {
    /// Count one more published block and update the gauge. The count
    /// stops at `RETENTION_BLOCKS`; the gauge stays far under 2^52.
    #[allow(
        clippy::cast_precision_loss,
        reason = "the count is at most RETENTION_BLOCKS"
    )]
    fn count_block(&mut self) {
        self.blocks = self.blocks.saturating_add(1).min(RETENTION_BLOCKS);
        metrics::gauge!(kardamom_engine::metrics::BAL_RETAINED_BLOCKS).set(self.blocks as f64);
    }
}

/// The BAL publisher thread's state: the handoff channel from the exec
/// thread, the `tx_bal` publication, the frame granularity, the optional
/// measurement granularities, and the retention window.
pub struct BalPublisher {
    rx: Receiver<BalHandoff>,
    pubh: PubHandle,
    granularity: NonZeroU16,
    /// Measurement mode: also encode at these granularities and report
    /// their sizes (no extra publishes) so batch size can be chosen from
    /// data. Read from `KARDAMOM_BAL_MEASURE`.
    measure: Vec<u16>,
    retention: RetentionWindow,
}

impl BalPublisher {
    /// Build the publisher. The frame granularity is
    /// [`DEFAULT_GRANULARITY`] unless `KARDAMOM_BAL_GRANULARITY`
    /// overrides it (1 means per-tx; other K values mean K-tx chunks, the
    /// size ladder's rungs).
    #[must_use]
    pub fn new(rx: Receiver<BalHandoff>, pubh: PubHandle) -> Self {
        let granularity = std::env::var("KARDAMOM_BAL_GRANULARITY")
            .ok()
            .and_then(|v| v.parse::<NonZeroU16>().ok())
            .unwrap_or(DEFAULT_GRANULARITY);
        let measure: Vec<u16> = std::env::var("KARDAMOM_BAL_MEASURE")
            .ok()
            .map(|v| v.split(',').filter_map(|x| x.trim().parse().ok()).collect())
            .unwrap_or_default();
        Self {
            rx,
            pubh,
            granularity,
            measure,
            retention: RetentionWindow::default(),
        }
    }

    /// Spawn the publisher thread.
    ///
    /// # Errors
    ///
    /// Returns the OS error if the thread cannot be spawned.
    pub fn spawn(self) -> std::io::Result<JoinHandle<()>> {
        std::thread::Builder::new()
            .name("bal-publisher".into())
            .spawn(move || self.run())
    }

    /// Run the publisher loop: encode each handoff off the exec thread and
    /// deliver it with retries. Returns when the handoff channel closes
    /// (executor shutdown).
    fn run(mut self) {
        tracing::info!(
            granularity = self.granularity.get(),
            measure = ?self.measure,
            "BAL publisher started"
        );
        loop {
            let ControlFlow::Continue(handoff) = self.recv_tick() else {
                break;
            };
            self.process_tick(handoff);
        }
        tracing::info!("BAL publisher stopped");
    }

    /// One tick: a handoff to process, an idle timeout, or the channel
    /// closing.
    fn recv_tick(&self) -> ControlFlow<(), Option<BalHandoff>> {
        match self.rx.recv_timeout(Duration::from_millis(500)) {
            Ok(v) => ControlFlow::Continue(Some(v)),
            Err(RecvTimeoutError::Timeout) => {
                // Idle tick: no block closed in the last 500ms. Use trace
                // level, not debug: this fires twice a second on a quiet
                // chain.
                tracing::trace!("BAL publisher idle: no handoff in 500ms");
                ControlFlow::Continue(None)
            }
            Err(RecvTimeoutError::Disconnected) => {
                tracing::debug!(
                    "BAL handoff channel closed (exec thread gone); stopping publisher"
                );
                ControlFlow::Break(())
            }
        }
    }

    /// Process one tick's handoff, if any: measure (if configured),
    /// encode, publish with retry, and count the block in the retention
    /// window. Does nothing if `handoff` is `None` (an idle tick) or
    /// encoding fails.
    ///
    /// The frame-bytes histogram below records a byte count that stays
    /// far under 2^52 (the f64 mantissa).
    #[allow(
        clippy::cast_precision_loss,
        reason = "byte totals over one run stay far under 2^52"
    )]
    fn process_tick(&mut self, handoff: Option<BalHandoff>) {
        let Some(BalHandoff {
            boundary,
            delta,
            bal,
        }) = handoff
        else {
            return;
        };
        let block = boundary.block_number;

        if !self.measure.is_empty() {
            self.measure_granularities(&bal, block);
        }

        let t0 = Instant::now();
        let Some(frame) = self.encode_and_count(delta, bal, block) else {
            return;
        };
        metrics::histogram!(kardamom_engine::metrics::BAL_FRAME_BYTES)
            .record(frame.bytes.len() as f64);
        metrics::histogram!(kardamom_engine::metrics::BAL_ENCODE_SECONDS)
            .record(t0.elapsed().as_secs_f64());
        tracing::debug!(
            block,
            frame_bytes = frame.bytes.len(),
            bal_bytes = frame.bal_bytes,
            encode_ms = t0.elapsed().as_millis(),
            "BAL frame encoded"
        );

        // Acked delivery with a bounded deadline. The block counts toward
        // the retention window regardless of the live-delivery outcome:
        // the archive ring is the durability path (a validator that was
        // down catches up from it). See `publish_with_retry`'s doc
        // comment for the retry logic.
        let outcome = self.publish_with_retry(&frame, block);
        metrics::counter!(kardamom_engine::metrics::BAL_PUBLISH_TOTAL, "outcome" => outcome.label())
            .increment(1);
        self.retention.count_block();
    }

    /// Report each measurement granularity's encoded size for `bal`, at
    /// `block`, with no extra publish. Lets batch size be chosen from
    /// data.
    fn measure_granularities(&self, bal: &revm::state::bal::Bal, block: u64) {
        let alloy_bal = bal.clone().into_alloy_bal();
        let sizes: Vec<(u16, usize)> = self
            .measure
            .iter()
            .map(|k| {
                let mut buf = Vec::new();
                quantize(alloy_bal.clone(), *k).encode(&mut buf);
                (*k, buf.len())
            })
            .collect();
        tracing::info!(block, ?sizes, "BAL granularity measurement");
    }

    /// Encode, log, and count a failed encode. Returns `None` on failure,
    /// so the caller can skip the rest of this handoff.
    fn encode_and_count(
        &self,
        delta: BlockDelta,
        bal: revm::state::bal::Bal,
        block: u64,
    ) -> Option<EncodedFrame> {
        match EncodedFrame::encode(delta, bal, self.granularity) {
            Ok(frame) => Some(frame),
            Err(e) => {
                tracing::error!(block, error = %e, "BAL encode failed");
                metrics::counter!(kardamom_engine::metrics::BAL_PUBLISH_TOTAL, "outcome" => "encode_error")
                    .increment(1);
                None
            }
        }
    }

    /// Publish `frame` for `block`, retrying transient failures until
    /// `PUBLISH_DEADLINE` elapses. The archive ring keeps the frame
    /// regardless of the outcome; this only decides the reported metric
    /// outcome.
    fn publish_with_retry(&self, frame: &EncodedFrame, block: u64) -> PublishOutcome {
        let deadline = Instant::now() + PUBLISH_DEADLINE;
        loop {
            let ControlFlow::Break(result) = self.try_publish_or_wait(frame, block, deadline)
            else {
                continue;
            };
            return result;
        }
    }

    /// One publish attempt. Returns [`ControlFlow::Break`] with the result
    /// once [`Self::try_publish`] resolves; otherwise sleeps the retry
    /// backoff and returns [`ControlFlow::Continue`], so the caller's loop
    /// tries again.
    fn try_publish_or_wait(
        &self,
        frame: &EncodedFrame,
        block: u64,
        deadline: Instant,
    ) -> ControlFlow<PublishOutcome> {
        let Some(result) = self.try_publish(frame, block, deadline) else {
            std::thread::sleep(Duration::from_millis(5));
            return ControlFlow::Continue(());
        };
        ControlFlow::Break(result)
    }

    /// One publish attempt. `Some` means stop retrying and return this
    /// outcome; `None` means the caller should sleep and retry.
    ///
    /// A `NOT_CONNECTED` result is terminal. Retrying it would spend the
    /// full deadline on every frame, cap the drain rate below the block
    /// cadence, and block the exec thread behind a full handoff channel.
    /// Only `BACK_PRESSURED`-class transients are worth the bounded retry.
    fn try_publish(
        &self,
        frame: &EncodedFrame,
        block: u64,
        deadline: Instant,
    ) -> Option<PublishOutcome> {
        match self.pubh.publish_bytes(frame.aligned()) {
            Ok(_) => Some(PublishOutcome::Ok),
            Err(e) => Self::classify_publish_error(&e, block, deadline),
        }
    }

    /// Classifies one `publish_bytes` failure. Returns `Some` when
    /// retrying is pointless: `NOT_CONNECTED` is terminal, and a spent
    /// deadline stops the retry loop. Returns `None` when the caller
    /// should sleep and retry.
    fn classify_publish_error(
        e: &kardamom_log::LogError,
        block: u64,
        deadline: Instant,
    ) -> Option<PublishOutcome> {
        let msg = e.to_string();
        if msg.contains("NOT_CONNECTED") {
            tracing::debug!(block, "BAL: no subscriber; frame retained for replay");
            return Some(PublishOutcome::NotConnected);
        }
        if Instant::now() >= deadline {
            tracing::warn!(block, error = %e, "BAL live publish deadline exhausted; frame retained for replay");
            return Some(PublishOutcome::DeadlineExhausted);
        }
        None
    }
}
