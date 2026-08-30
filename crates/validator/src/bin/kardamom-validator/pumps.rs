//! Verification-stream pump tasks: tx_bal (with its silence watchdog),
//! tx_receipts, and the committed-block metrics/attester poller. Each runs
//! on the binary's tokio runtime for the process lifetime.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use kardamom_log::aeron_live::{AeronRuntime, TxReceiptsSubscriberHandle};
use kardamom_log::config::ChannelsConfig;
use kardamom_state::SnapshotReceiver;
use kardamom_validator::attester::AttesterHandle;
use kardamom_validator::{BalBuffer, ClaimBuffer, ReceiptBuffer, metrics};
use tokio_util::sync::CancellationToken;

/// tx_bal: per-block BlockDelta (BAL). Simple (multicast/IPC) subscription,
/// wrapped in a SILENCE WATCHDOG (#144): a multicast image that never
/// joins (or silently dies) starves verification while everything else
/// works — observed as `validator_blocks_verified_total == 0` for a whole
/// run. Executors publish one BAL per committed block (empty blocks
/// included), so 60s of silence on a progressing chain is a dead
/// subscription, not an idle one: drop it and reopen. On a genuinely idle
/// cluster the reopen is a harmless no-op churn.
///
/// The pump holds an AeronRuntime clone (needed to reopen), which is the
/// documented SIGTERM-deadlock trap (see the tx_receipts comment on
/// [`spawn_receipts_pump`]): the runtime only shuts down when its last clone
/// drops. Hence the `shutdown` token: the main path cancels it BEFORE it
/// drops `rt`, the `select!` below wakes at once, and this task releases
/// its clone so graceful shutdown completes. No wake tick is needed —
/// cancellation interrupts the `recv` directly.
pub fn spawn_bal_pump(
    rt: &AeronRuntime,
    channels: &ChannelsConfig,
    bals: Arc<BalBuffer>,
    claims: Arc<ClaimBuffer>,
    shutdown: CancellationToken,
) -> Result<()> {
    const BAL_SILENCE_REOPEN: Duration = Duration::from_secs(60);
    // BalFrame (spec: bal-attribution-parallel-validation): the merged
    // delta plus the EIP-7928 access list; the write-set cross-check
    // consumes the merged section, attribution drives the parallel
    // engine.
    let mut bal_rx = rt
        .open_subscription::<kardamom_types::BalFrame>(
            &channels.tx_bal_channel,
            channels.tx_bal_stream_id,
        )
        .context("open tx_bal subscription")?;
    let bal_rt = rt.clone();
    let bal_channel = channels.tx_bal_channel.clone();
    let bal_stream_id = channels.tx_bal_stream_id;
    tokio::spawn(async move {
        loop {
            let recv = tokio::select! {
                biased;
                // Release the runtime clone promptly on shutdown.
                _ = shutdown.cancelled() => return,
                r = tokio::time::timeout(BAL_SILENCE_REOPEN, bal_rx.recv()) => r,
            };
            let frame = match recv {
                Ok(Some((_pos, frame))) => frame,
                Ok(None) => return, // runtime shutting down
                Err(_) => {
                    metrics::counter_bal_sub_reopen();
                    tracing::warn!(
                        silence_s = BAL_SILENCE_REOPEN.as_secs(),
                        "tx_bal silent — reopening the subscription \
                         (never-joined or dead multicast image, #144)"
                    );
                    match bal_rt
                        .open_subscription::<kardamom_types::BalFrame>(&bal_channel, bal_stream_id)
                    {
                        Ok(rx) => bal_rx = rx,
                        Err(e) => tracing::warn!(
                            error = %e,
                            "tx_bal reopen failed; retrying after the next window"
                        ),
                    }
                    continue;
                }
            };
            {
                let kardamom_types::BalFrame {
                    bal_rlp,
                    granularity,
                    delta,
                } = &frame;
                // Decode the access list into seed-lookup form for the
                // parallel engine. A decode failure degrades to
                // sequential re-execution (the merged cross-check below
                // is unaffected), never to a verification gap.
                let mut slice: &[u8] = bal_rlp;
                match <alloy_eip7928::BlockAccessList as alloy_rlp::Decodable>::decode(&mut slice) {
                    // Empty lists are skipped: empty blocks never take
                    // claims (the parallel path short-circuits before
                    // its take), so inserting them would grow the
                    // buffer for the whole idle period — the cursor
                    // only advances on takes. Quantized frames carry
                    // their granularity so verification coarsens to
                    // the chunk with batches aligned to it — the
                    // validator's ladder view always follows the wire.
                    Ok(bal) if !bal.is_empty() => {
                        claims.insert(
                            delta.block_number,
                            *granularity,
                            kardamom_validator::parallel::ClaimIndex::from_alloy(&bal),
                        );
                    }
                    Ok(_) => {}
                    Err(e) => tracing::warn!(
                        block = delta.block_number,
                        error = %e,
                        granularity,
                        "BAL access-list decode failed; block validates sequentially"
                    ),
                }
            }
            bals.insert(frame.delta().clone());
        }
    });
    Ok(())
}

/// tx_receipts: the executor's published receipts (MDS fan-in in the cluster).
///
/// `into_receiver()` is load-bearing for shutdown: the handle carries an
/// `AeronRuntime` clone (for MDS destination churn), and moving that clone
/// into this pump task would deadlock process exit — the runtime shuts
/// down only when its last clone drops, that shutdown is what ends
/// `recv()`, and this task would be holding the clone that prevents it.
/// The symptom was a validator that ignored SIGTERM entirely (`drop(rt)`
/// became a no-op, so the engine's tx_data subscriptions never closed and
/// the join below never returned) while the executor — which publishes
/// receipts rather than subscribing — shut down fine. MDS destinations are
/// attached inside `open_tx_receipts`, so nothing needs the clone after
/// this point. The `shutdown` token is a second, explicit exit so the pump
/// stops at the same moment as the others instead of waiting for `recv` to
/// observe the runtime teardown.
pub fn spawn_receipts_pump(
    rt: &AeronRuntime,
    channels: &ChannelsConfig,
    executor_count_flag: Option<u32>,
    receipts: Arc<ReceiptBuffer>,
    shutdown: CancellationToken,
) -> Result<()> {
    let executor_count = executor_count_flag.unwrap_or(channels.tx_receipts_executor_count);
    let mut rx = TxReceiptsSubscriberHandle::open_auto(rt, channels, executor_count)
        .context("open tx_receipts")?
        .into_receiver();
    tokio::spawn(async move {
        loop {
            let next = tokio::select! {
                biased;
                _ = shutdown.cancelled() => return,
                r = rx.recv() => r,
            };
            let Some((_pos, r)) = next else { return };
            receipts.insert(r);
        }
    });
    Ok(())
}

/// Background poller: expose committed-block + state-root height as
/// metrics, and feed each block's observed MPT root to the attester.
/// `validator_state_root_block` is set only when the committed snapshot
/// actually yielded a root — an independent measurement, not a mirror of
/// the committed-block gauge.
pub fn spawn_commit_poller(
    snap_rx: SnapshotReceiver,
    attester_handle: Option<AttesterHandle>,
    shutdown: CancellationToken,
) {
    tokio::spawn(async move {
        let mut last = 0u64;
        loop {
            if shutdown.is_cancelled() {
                return;
            }
            if let Some(snap) = snap_rx.current() {
                let block = snap.block_number();
                if block != last {
                    last = block;
                    metrics::set_committed_block(block);
                    match snap.state_root() {
                        Ok(Some(root)) => {
                            metrics::set_state_root_block(block);
                            tracing::debug!(block, state_root = %root, "validator committed block");
                            if let Some(h) = attester_handle.as_ref() {
                                h.submit_root(block, root);
                            }
                        }
                        Ok(None) => {}
                        Err(e) => {
                            tracing::warn!(block, error = %e, "state_root read failed")
                        }
                    }
                }
            }
            tokio::select! {
                _ = shutdown.cancelled() => return,
                _ = tokio::time::sleep(Duration::from_millis(200)) => {}
            }
        }
    });
}
