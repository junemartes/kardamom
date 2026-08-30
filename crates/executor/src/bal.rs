//! Executor-side BAL publication.
//!
//! Publishes one EIP-7928 Block Access List frame per block on `tx_bal`.
//! See docs/agents/bal-attribution-parallel-validation-spec.md.
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
//! (unchanged from v1; receipts dominate byte size, and fat frames once
//! shrank the validator's lapse window) plus the canonical RLP-encoded
//! `alloy_eip7928::BlockAccessList`, carrying per-slot `(tx_index, value)`
//! writes and per-account storage reads.

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

/// Encode one handoff into a wire frame at `granularity`, returning the
/// rkyv bytes plus the BAL section's size (for telemetry).
fn encode_frame(
    delta: BlockDelta,
    bal: revm::state::bal::Bal,
    granularity: u16,
) -> Result<(Vec<u8>, usize), String> {
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
        .map(|b| (b.to_vec(), bal_bytes))
        .map_err(|e| e.to_string())
}

/// Run the BAL publisher loop: encode each handoff off the exec thread and
/// deliver it with retries, retaining recent frames for validator catch-up.
/// Returns when the handoff channel closes (executor shutdown).
pub fn run_bal_publisher(rx: Receiver<BalHandoff>, pubh: PubHandle) {
    let granularity = configured_granularity();
    // Measurement mode: also encode at these granularities and report their
    // sizes (no extra publishes) so batch size can be chosen from data.
    let measure: Vec<u16> = std::env::var("KARDAMOM_BAL_MEASURE")
        .ok()
        .map(|v| v.split(',').filter_map(|x| x.trim().parse().ok()).collect())
        .unwrap_or_default();
    let mut retained: std::collections::VecDeque<(u64, Vec<u8>)> = Default::default();

    tracing::info!(granularity, ?measure, "BAL publisher started");
    loop {
        let (boundary, delta, bal) = match rx.recv_timeout(Duration::from_millis(500)) {
            Ok(v) => v,
            Err(RecvTimeoutError::Timeout) => {
                // Idle tick: no block closed in the last 500ms. Use trace
                // level, not debug: this fires twice a second on a quiet
                // chain.
                tracing::trace!("BAL publisher idle: no handoff in 500ms");
                continue;
            }
            Err(RecvTimeoutError::Disconnected) => {
                tracing::debug!(
                    "BAL handoff channel closed (exec thread gone); stopping publisher"
                );
                break;
            }
        };
        let block = boundary.block_number;

        if !measure.is_empty() {
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

        let t0 = Instant::now();
        let (bytes, bal_bytes) = match encode_frame(delta, bal, granularity) {
            Ok(v) => v,
            Err(e) => {
                tracing::error!(block, error = %e, "BAL encode failed");
                metrics::counter!(kardamom_engine::metrics::BAL_PUBLISH_TOTAL, "outcome" => "encode_error")
                    .increment(1);
                continue;
            }
        };
        metrics::histogram!(kardamom_engine::metrics::BAL_FRAME_BYTES).record(bytes.len() as f64);
        metrics::histogram!(kardamom_engine::metrics::BAL_ENCODE_SECONDS)
            .record(t0.elapsed().as_secs_f64());
        tracing::debug!(
            block,
            frame_bytes = bytes.len(),
            bal_bytes,
            encode_ms = t0.elapsed().as_millis(),
            "BAL frame encoded"
        );

        // Acked delivery with a bounded deadline. The frame is retained
        // regardless of the live-delivery outcome: retention is the
        // durability path (a validator that was down catches up from the
        // ring).
        //
        // A `NOT_CONNECTED` result is terminal, not retried. It means no
        // subscriber exists right now (for example, a validator-less
        // stack). Spinning the full deadline on every frame capped this
        // pump's drain rate at 2 frames/s, below the local 250ms block
        // cadence. That filled the bounded exec-to-pump handoff and
        // blocked the exec thread, delaying every receipt behind it. The
        // frame still lands in the retention ring either way. Only
        // `BACK_PRESSURED`-class transients are worth the bounded retry.
        let deadline = Instant::now() + PUBLISH_DEADLINE;
        let mut outcome = "ok";
        loop {
            match pubh.publish_bytes(aligned(&bytes)) {
                Ok(_) => break,
                Err(e) => {
                    let msg = e.to_string();
                    if msg.contains("NOT_CONNECTED") {
                        tracing::debug!(block, "BAL: no subscriber; frame retained for replay");
                        outcome = "not_connected";
                        break;
                    }
                    if Instant::now() >= deadline {
                        tracing::warn!(block, error = %e, "BAL live publish deadline exhausted; frame retained for replay");
                        outcome = "deadline";
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(5));
                }
            }
        }
        metrics::counter!(kardamom_engine::metrics::BAL_PUBLISH_TOTAL, "outcome" => outcome)
            .increment(1);

        retained.push_back((block, bytes));
        while retained.len() > RETENTION_BLOCKS {
            retained.pop_front();
        }
        metrics::gauge!(kardamom_engine::metrics::BAL_RETAINED_BLOCKS).set(retained.len() as f64);
    }
    tracing::info!("BAL publisher stopped");
}
