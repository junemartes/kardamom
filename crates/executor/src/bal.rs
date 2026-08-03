//! Executor-side BAL publication.
//!
//! Publishes one EIP-7928 Block Access List frame per block on `tx_bal`
//! (spec: `docs/agents/bal-attribution-parallel-validation-spec.md`).
//!
//! **Emission is not best-effort.** Once BALs drive the validator's parallel
//! re-execution, their availability over the catch-up window is a LIVENESS
//! property: a validator that lags and finds BALs missing must re-execute
//! sequentially, and at high tps sequential re-execution can be slower than
//! the chain — it would never catch up. So every block's state transition is
//! emitted, with an ack'd publish and a bounded retry deadline, from a
//! DEDICATED thread: the exec thread's only cost is moving the captured
//! `Bal` onto a channel.
//!
//! The frame is a [`BalFrame`]: the receipts-free merged `BlockDelta`
//! (unchanged from v1 — receipts dominate byte size and fat frames once
//! collapsed the validator's lapse window, #113) plus the canonical
//! RLP-encoded `alloy_eip7928::BlockAccessList` carrying per-slot
//! `(tx_index, value)` writes and per-account storage reads.

use std::time::{Duration, Instant};

use alloy_rlp::Encodable;
use crossbeam_channel::{Receiver, RecvTimeoutError};
use kardamom_engine::actor::BalHandoff;
use kardamom_log::aeron_live::PubHandle;
use kardamom_types::{BalFrame, BlockDelta};

/// Attribution granularity written into the frame. 1 = per-tx (default);
/// K > 1 quantizes BalIndex into K-tx chunks (the size ladder — under the
/// seeded execution model this costs no parallelism, only artifact
/// fineness). Overridable for measurement via `KARDAMOM_BAL_GRANULARITY`.
fn configured_granularity() -> u16 {
    std::env::var("KARDAMOM_BAL_GRANULARITY")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|k| *k >= 1)
        .unwrap_or(1)
}

/// Per-frame publish deadline. A frame that cannot be delivered live within
/// this window is still RETAINED (replay-serving is the durability path);
/// the deadline only bounds how long one frame occupies the publisher. It
/// must be WELL UNDER the block interval: at 2s (== the tick) a
/// NOT_CONNECTED window drained exactly at the arrival rate, so the bounded
/// handoff channel hovered at capacity and any jitter back-pressured the
/// exec thread. 500ms keeps drain 4x arrival during subscriber outages.
const PUBLISH_DEADLINE: Duration = Duration::from_millis(500);

/// Retained encoded frames for validator catch-up. 256 blocks ≈ 8.5 min at
/// the 2s tick.
const RETENTION_BLOCKS: usize = 256;

/// Quantize a Bal's tx indices into K-sized chunks: within a chunk, writes
/// to the same item collapse to the chunk-final value. Applied to the
/// exported alloy BAL so the wire artifact matches what the validator will
/// verify against at that granularity.
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
            Err(RecvTimeoutError::Timeout) => continue,
            Err(RecvTimeoutError::Disconnected) => break,
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

        // Ack'd delivery with a bounded deadline. Retained regardless of
        // live-delivery outcome: retention IS the durability path (a
        // validator that was down catches up from the ring).
        //
        // NOT_CONNECTED is TERMINAL, not retried: it means no subscriber
        // exists right now (e.g. a validator-less stack), and spinning the
        // full deadline on every frame capped this pump's drain rate at
        // 2 frames/s — below the local 250ms block cadence — so the
        // bounded exec→pump handoff filled and BLOCKED THE EXEC THREAD,
        // delaying every receipt behind it (S4's -32000 park timeouts;
        // chain-semantics red). The frame still lands in the retention
        // ring either way; only BACK_PRESSURED-class transients are worth
        // the bounded retry.
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
