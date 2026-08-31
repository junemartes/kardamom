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

/// tx_bal: per-block BlockDelta (BAL). This is a simple multicast or IPC
/// subscription, wrapped in a silence watchdog. A multicast image that
/// never joins, or silently dies, starves verification while everything
/// else works, which shows up as `validator_blocks_verified_total == 0`
/// for a whole run. Executors publish one BAL per committed block,
/// including empty blocks, so 60s of silence on a progressing chain
/// means a dead subscription, not an idle one: drop it and reopen it. On
/// a genuinely idle cluster, the reopen is harmless no-op churn.
///
/// The pump holds an AeronRuntime clone. It needs the clone to reopen the
/// subscription. This is the documented SIGTERM deadlock trap (see the
/// tx_receipts comment on [`spawn_receipts_pump`]). The runtime shuts down
/// only when its last clone drops. The main path cancels the `shutdown`
/// token before it drops `rt`. The `select!` below wakes at once, and this
/// task releases its clone, so graceful shutdown completes. No wake tick
/// is needed. Cancellation interrupts the `recv` directly.
pub fn spawn_bal_pump(
    rt: &AeronRuntime,
    channels: &ChannelsConfig,
    bals: Arc<BalBuffer>,
    claims: Arc<ClaimBuffer>,
    // The interop outbox extractor's own claim buffer. The engine's
    // `claims` buffer is consumed by the whole-block strategy (and never
    // drained in streaming mode), so this pump feeds both, sharing the
    // one decoded index instead of cloning it per consumer.
    extract_claims: Option<Arc<ClaimBuffer>>,
    shutdown: CancellationToken,
) -> Result<()> {
    const BAL_SILENCE_REOPEN: Duration = Duration::from_secs(60);
    // BalFrame is the merged delta plus the EIP-7928 access list. The
    // write-set check uses the
    // merged section; attribution drives the parallel engine.
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
                Ok(None) => return, // The runtime is shutting down.
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
                // parallel engine. A decode failure falls back to
                // sequential re-execution (the merged check below is
                // unaffected), never to a verification gap.
                let mut slice: &[u8] = bal_rlp;
                match <alloy_eip7928::BlockAccessList as alloy_rlp::Decodable>::decode(&mut slice) {
                    // Skip empty lists: empty blocks never take claims,
                    // since the parallel path short-circuits before its
                    // take, so inserting them would grow the buffer for
                    // the whole idle period. The cursor advances only on
                    // takes. Quantized frames carry their granularity,
                    // so verification coarsens to the chunk, with
                    // batches aligned to it. The validator's ladder view
                    // always follows the wire.
                    Ok(bal) if !bal.is_empty() => {
                        let idx =
                            Arc::new(kardamom_validator::parallel::ClaimIndex::from_alloy(&bal));
                        claims.insert_arc(delta.block_number, *granularity, idx.clone());
                        if let Some(ec) = extract_claims.as_ref() {
                            ec.insert_arc(delta.block_number, *granularity, idx);
                        }
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
/// Keep the `into_receiver()` call: it matters for shutdown. The handle
/// carries an `AeronRuntime` clone, for MDS destination churn, and moving
/// that clone into this pump task would deadlock process exit. The
/// runtime shuts down only when its last clone drops, that shutdown is
/// what ends `recv()`, and this task would hold the clone that blocks it.
/// Without this, the validator would ignore SIGTERM entirely, since
/// `drop(rt)` would become a no-op, leaving the engine's tx_data
/// subscriptions open and the join below never returning. MDS
/// destinations attach inside `open_tx_receipts`, so nothing needs the
/// clone after this point. The `shutdown` token gives a second, explicit
/// exit. The pump then stops at the same time as the others, instead of
/// waiting for `recv` to see the runtime shut down.
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

/// Background poller: expose the committed-block and state-root height
/// as metrics, and feed each block's observed MPT root to the attester.
/// `validator_state_root_block` is set only when the committed snapshot
/// actually yielded a root. This is an independent measurement, not a
/// mirror of the committed-block gauge.
pub fn spawn_commit_poller(
    snap_rx: SnapshotReceiver,
    attester_handle: Option<AttesterHandle>,
    // Interop attestation stream (unsigned): the same observed roots,
    // retained and served over `kardamom_subscribeAttestations`.
    attestation_store: Option<Arc<kardamom_validator::interop::AttestationStore>>,
    shutdown: CancellationToken,
) {
    tokio::spawn(async move {
        // Park on the snapshot watch: one wake per commit. The 200 ms
        // timer and the last != block dedup are gone — each publish IS a
        // new committed block.
        let mut watch = snap_rx.watch();
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => return,
                changed = watch.changed() => {
                    if changed.is_err() {
                        // Writer gone: shutdown path.
                        return;
                    }
                }
            }
            let Some(snap) = watch.borrow_and_update().clone() else {
                continue;
            };
            let block = snap.block_number();
            metrics::set_committed_block(block);
            match snap.state_root() {
                Ok(Some(root)) => {
                    metrics::set_state_root_block(block);
                    tracing::debug!(block, state_root = %root, "validator committed block");
                    if let Some(h) = attester_handle.as_ref() {
                        h.submit_root(block, root);
                    }
                    if let Some(s) = attestation_store.as_ref() {
                        s.push(block, root);
                    }
                }
                Ok(None) => {}
                Err(e) => {
                    tracing::warn!(block, error = %e, "state_root read failed")
                }
            }
        }
    });
}
