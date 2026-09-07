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

use std::ops::ControlFlow;
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
const DEFAULT_GRANULARITY: u16 = 8;

/// Attribution granularity written into the frame. It is
/// [`DEFAULT_GRANULARITY`] unless overridden by `KARDAMOM_BAL_GRANULARITY`
/// (1 means per-tx; other K values mean K-tx chunks, the size ladder's
/// rungs).
fn configured_granularity() -> u16 {
    std::env::var("KARDAMOM_BAL_GRANULARITY")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|k| *k >= 1)
        .unwrap_or(DEFAULT_GRANULARITY)
}

/// Per-frame publish deadline. A frame that cannot be delivered live
/// within this window is still retained (replay-serving is the
/// durability path). The deadline only bounds how long one frame
/// occupies the publisher. It must be well under the block interval: at
/// 2s (the tick length), a `NOT_CONNECTED` window drained at exactly the
/// arrival rate, so the bounded handoff channel hovered at capacity and
/// any jitter back-pressured the exec thread. 500ms keeps the drain rate
/// 4x the arrival rate during subscriber outages.
const PUBLISH_DEADLINE: Duration = Duration::from_millis(500);

/// Retained encoded frames for validator catch-up. 256 blocks is about
/// 8.5 min at the 2s tick.
const RETENTION_BLOCKS: usize = 256;

/// Quantize a Bal's tx indices into K-sized chunks. Within a chunk,
/// writes to the same item collapse to the chunk-final value. This
/// applies to the exported alloy BAL, so the wire artifact matches what
/// the validator will verify at that granularity.
use kardamom_engine::bal_ladder::quantize;

/// Copy bytes into the aligned buffer the publish API expects.
fn aligned(bytes: &[u8]) -> rkyv::util::AlignedVec {
    let mut av = rkyv::util::AlignedVec::new();
    av.extend_from_slice(bytes);
    av
}

/// One handoff's encoded wire frame: the rkyv bytes, plus the BAL
/// section's size (for telemetry).
struct EncodedFrame {
    bytes: Vec<u8>,
    bal_bytes: usize,
}

/// Encode one handoff into a wire frame at `granularity`.
fn encode_frame(
    delta: BlockDelta,
    bal: revm::state::bal::Bal,
    granularity: u16,
) -> Result<EncodedFrame, String> {
    let alloy_bal = quantize(bal.into_alloy_bal(), granularity);
    let mut bal_rlp = Vec::new();
    alloy_bal.encode(&mut bal_rlp);
    let bal_bytes = bal_rlp.len();
    let frame = BalFrame {
        delta,
        bal_rlp,
        granularity,
    };
    kardamom_log::codec::encode(&frame)
        .map(|b| EncodedFrame {
            bytes: b.to_vec(),
            bal_bytes,
        })
        .map_err(|e| e.to_string())
}

/// One publish attempt. `Some` means stop retrying and return this
/// outcome; `None` means the caller should sleep and retry.
///
/// A `NOT_CONNECTED` result is terminal, not retried. It means no
/// subscriber exists right now (for example, a validator-less stack).
/// Spinning the full deadline on every frame capped this pump's drain
/// rate at 2 frames/s, below the local 250ms block cadence. That
/// filled the bounded exec-to-pump handoff and blocked the exec
/// thread, delaying every receipt behind it. Only
/// `BACK_PRESSURED`-class transients are worth the bounded retry.
fn try_publish(
    pubh: &PubHandle,
    bytes: &[u8],
    block: u64,
    deadline: Instant,
) -> Option<&'static str> {
    match pubh.publish_bytes(aligned(bytes)) {
        Ok(_) => return Some("ok"),
        Err(e) => {
            let msg = e.to_string();
            if msg.contains("NOT_CONNECTED") {
                tracing::debug!(block, "BAL: no subscriber; frame retained for replay");
                return Some("not_connected");
            }
            if Instant::now() >= deadline {
                tracing::warn!(block, error = %e, "BAL live publish deadline exhausted; frame retained for replay");
                return Some("deadline");
            }
        }
    }
    None
}

/// Publish `bytes` for `block`, retrying transient failures until
/// `PUBLISH_DEADLINE` elapses. The frame is retained regardless of the
/// outcome (see `run_bal_publisher`'s doc comment); this only decides
/// the reported metric outcome.
fn publish_with_retry(pubh: &PubHandle, bytes: &[u8], block: u64) -> &'static str {
    let deadline = Instant::now() + PUBLISH_DEADLINE;
    loop {
        let Some(result) = try_publish(pubh, bytes, block, deadline) else {
            std::thread::sleep(Duration::from_millis(5));
            continue;
        };
        return result;
    }
}

/// Drop the oldest retained frames past `RETENTION_BLOCKS`.
fn trim_retention(retained: &mut std::collections::VecDeque<(u64, Vec<u8>)>) {
    while retained.len() > RETENTION_BLOCKS {
        retained.pop_front();
    }
}

/// One [`run_bal_publisher`] tick: a handoff to process, an idle
/// timeout, or the channel closing.
fn recv_tick(rx: &Receiver<BalHandoff>) -> ControlFlow<(), Option<BalHandoff>> {
    match rx.recv_timeout(Duration::from_millis(500)) {
        Ok(v) => ControlFlow::Continue(Some(v)),
        Err(RecvTimeoutError::Timeout) => {
            // Idle tick: no block closed in the last 500ms. Use trace
            // level, not debug: this fires twice a second on a quiet chain.
            tracing::trace!("BAL publisher idle: no handoff in 500ms");
            ControlFlow::Continue(None)
        }
        Err(RecvTimeoutError::Disconnected) => {
            tracing::debug!("BAL handoff channel closed (exec thread gone); stopping publisher");
            ControlFlow::Break(())
        }
    }
}

/// Report each of `measure`'s granularities' encoded size for `bal`, at
/// `block`, with no extra publish. Lets batch size be chosen from data.
fn measure_granularities(bal: &revm::state::bal::Bal, block: u64, measure: &[u16]) {
    let alloy_bal = bal.clone().into_alloy_bal();
    let sizes: Vec<(u16, usize)> = measure
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
    delta: BlockDelta,
    bal: revm::state::bal::Bal,
    granularity: u16,
    block: u64,
) -> Option<EncodedFrame> {
    match encode_frame(delta, bal, granularity) {
        Ok(frame) => Some(frame),
        Err(e) => {
            tracing::error!(block, error = %e, "BAL encode failed");
            metrics::counter!(kardamom_engine::metrics::BAL_PUBLISH_TOTAL, "outcome" => "encode_error")
                .increment(1);
            None
        }
    }
}

/// Process one tick's handoff, if any: measure (if configured), encode,
/// publish with retry, and retain the frame. Does nothing if `handoff`
/// is `None` (an idle tick) or encoding fails.
///
/// The metric gauges below record a frame byte count and a
/// retained-block count; both stay far under 2^52 (the f64 mantissa).
#[allow(
    clippy::cast_precision_loss,
    reason = "byte and count totals over one run stay far under 2^52"
)]
fn process_tick(
    pubh: &PubHandle,
    granularity: u16,
    measure: &[u16],
    retained: &mut std::collections::VecDeque<(u64, Vec<u8>)>,
    handoff: Option<BalHandoff>,
) {
    let Some((boundary, delta, bal)) = handoff else {
        return;
    };
    let block = boundary.block_number;

    if !measure.is_empty() {
        measure_granularities(&bal, block, measure);
    }

    let t0 = Instant::now();
    let Some(frame) = encode_and_count(delta, bal, granularity, block) else {
        return;
    };
    metrics::histogram!(kardamom_engine::metrics::BAL_FRAME_BYTES).record(frame.bytes.len() as f64);
    metrics::histogram!(kardamom_engine::metrics::BAL_ENCODE_SECONDS)
        .record(t0.elapsed().as_secs_f64());
    tracing::debug!(
        block,
        frame_bytes = frame.bytes.len(),
        bal_bytes = frame.bal_bytes,
        encode_ms = t0.elapsed().as_millis(),
        "BAL frame encoded"
    );

    // Acked delivery with a bounded deadline. The frame is retained
    // regardless of the live-delivery outcome: retention is the
    // durability path (a validator that was down catches up from the
    // ring). See `publish_with_retry`'s doc comment for the retry logic.
    let outcome = publish_with_retry(pubh, &frame.bytes, block);
    metrics::counter!(kardamom_engine::metrics::BAL_PUBLISH_TOTAL, "outcome" => outcome)
        .increment(1);

    retained.push_back((block, frame.bytes));
    trim_retention(retained);
    metrics::gauge!(kardamom_engine::metrics::BAL_RETAINED_BLOCKS).set(retained.len() as f64);
}

/// Run the BAL publisher loop: encode each handoff off the exec thread and
/// deliver it with retries, retaining recent frames for validator catch-up.
/// Returns when the handoff channel closes (executor shutdown).
///
/// `pubh` is owned for the thread's whole life; there is no caller
/// left to hand a reference back to.
#[allow(
    clippy::needless_pass_by_value,
    reason = "pubh has no caller to return a reference to"
)]
pub fn run_bal_publisher(rx: Receiver<BalHandoff>, pubh: PubHandle) {
    let granularity = configured_granularity();
    // Measurement mode: also encode at these granularities and report their
    // sizes (no extra publishes) so batch size can be chosen from data.
    let measure: Vec<u16> = std::env::var("KARDAMOM_BAL_MEASURE")
        .ok()
        .map(|v| v.split(',').filter_map(|x| x.trim().parse().ok()).collect())
        .unwrap_or_default();
    let mut retained: std::collections::VecDeque<(u64, Vec<u8>)> =
        std::collections::VecDeque::new();

    tracing::info!(granularity, ?measure, "BAL publisher started");
    loop {
        let ControlFlow::Continue(handoff) = recv_tick(&rx) else {
            break;
        };
        process_tick(&pubh, granularity, &measure, &mut retained, handoff);
    }
    tracing::info!("BAL publisher stopped");
}
