//! Verification-stream pump tasks: `tx_bal` (with its silence watchdog),
//! `tx_receipts`, and the committed-block metrics/attester poller. Each runs
//! on the binary's tokio runtime for the process lifetime.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use kardamom_log::aeron_live::{AeronRuntime, TxReceiptsReceiver, TypedSubscription};
use kardamom_log::config::{ChannelUri, ChannelsConfig};
use kardamom_log::discovery::StreamPlane;
use kardamom_state::{SnapshotReceiver, StateSnapshot};
use kardamom_validator::attester::AttesterHandle;
use kardamom_validator::{BalBuffer, ClaimBuffer, ReceiptBuffer, metrics};
use tokio_util::sync::CancellationToken;

/// Silence window on `tx_bal` before the pump reopens the subscription.
/// See [`spawn_bal_pump`] for why silence, not just absence, triggers it.
const BAL_SILENCE_REOPEN: Duration = Duration::from_secs(60);

/// `tx_bal`: per-block `BlockDelta` (BAL). This is a simple multicast or IPC
/// subscription, wrapped in a silence watchdog. A multicast image that
/// never joins, or silently dies, starves verification while everything
/// else works, which shows up as `validator_blocks_verified_total == 0`
/// for a whole run. Executors publish one BAL per committed block,
/// including empty blocks, so 60s of silence on a progressing chain
/// means a dead subscription, not an idle one: drop it and reopen it. On
/// a genuinely idle cluster, the reopen is harmless no-op churn.
///
/// The pump holds an `AeronRuntime` clone. It needs the clone to reopen the
/// subscription. This is the documented SIGTERM deadlock trap (see the
/// `tx_receipts` comment on [`spawn_receipts_pump`]). The runtime shuts down
/// only when its last clone drops. The main path cancels the `shutdown`
/// token before it drops `rt`. The `select!` below wakes at once, and this
/// task releases its clone, so graceful shutdown completes. No wake tick
/// is needed. Cancellation interrupts the `recv` directly.
pub(crate) fn spawn_bal_pump(
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
    let bal_channel = channels.tx_bal_channel.clone();
    let bal_stream_id = channels.tx_bal_stream_id;
    let bal_rx = open_bal_sub(rt, bal_channel.as_str(), bal_stream_id)
        .context("open tx_bal subscription")?;
    let mut pump = BalPump {
        bal_rx,
        reopen_rt: rt.clone(),
        bal_channel,
        bal_stream_id,
        claims,
        extract_claims,
        bals,
    };
    tokio::spawn(async move { while pump.step(&shutdown).await.is_some() {} });
    Ok(())
}

/// The `tx_bal` pump's live subscription plus its fixed reopen target and
/// insertion buffers. Grouped so [`Self::step`] takes `&mut self` instead
/// of one argument per port.
struct BalPump {
    bal_rx: TypedSubscription<kardamom_types::BalFrame>,
    reopen_rt: AeronRuntime,
    bal_channel: ChannelUri,
    bal_stream_id: i32,
    claims: Arc<ClaimBuffer>,
    extract_claims: Option<Arc<ClaimBuffer>>,
    bals: Arc<BalBuffer>,
}

impl BalPump {
    /// One receive step: wait for a frame under the silence watchdog,
    /// index and insert it, or reopen the subscription after a silent
    /// window. Returns `None` once shutdown is requested or the runtime
    /// is gone, which releases this task's `AeronRuntime` clone.
    async fn step(&mut self, shutdown: &CancellationToken) -> Option<()> {
        let recv = tokio::select! {
            biased;
            // Release the runtime clone promptly on shutdown.
            () = shutdown.cancelled() => return None,
            r = tokio::time::timeout(BAL_SILENCE_REOPEN, self.bal_rx.recv()) => r,
        };
        let frame = match recv {
            Ok(Some((_pos, frame))) => frame,
            Ok(None) => return None, // The runtime is shutting down.
            Err(_) => {
                metrics::counter_bal_sub_reopen();
                tracing::warn!(
                    silence_s = BAL_SILENCE_REOPEN.as_secs(),
                    "tx_bal silent — reopening the subscription \
                     (never-joined or dead multicast image, #144)"
                );
                match open_bal_sub(
                    &self.reopen_rt,
                    self.bal_channel.as_str(),
                    self.bal_stream_id,
                ) {
                    Ok(rx) => self.bal_rx = rx,
                    Err(e) => tracing::warn!(
                        error = %e,
                        "tx_bal reopen failed; retrying after the next window"
                    ),
                }
                return Some(());
            }
        };
        index_claims(&frame, &self.claims, self.extract_claims.as_deref());
        self.bals.insert(frame.delta().clone());
        Some(())
    }
}

/// Open the `tx_bal` subscription: on startup, and again to reopen after
/// prolonged silence. `BalFrame` is the merged delta plus the EIP-7928
/// access list: the write-set check uses the merged section, and
/// attribution drives the parallel engine.
fn open_bal_sub(
    rt: &AeronRuntime,
    channel: &str,
    stream_id: i32,
) -> std::result::Result<TypedSubscription<kardamom_types::BalFrame>, kardamom_log::error::LogError>
{
    rt.open_subscription::<kardamom_types::BalFrame>(channel, stream_id)
}

/// Decode the frame's access list, and insert it into `claims` (and
/// `extract_claims`, if given). A decode failure falls back to sequential
/// re-execution (the merged write-set check is unaffected), never to a
/// verification gap. `granularity` carries its own never-zero guarantee
/// from the wire decode, so every downstream consumer takes a
/// [`std::num::NonZeroU16`] with nothing left to check here. Skips an empty list:
/// empty blocks never take claims, since the parallel path short-circuits
/// before its take, so inserting them would grow the buffer for the whole
/// idle period. Quantized frames carry their granularity, so verification
/// coarsens to the chunk, with batches aligned to it. The validator's
/// ladder view always follows the wire.
fn index_claims(
    frame: &kardamom_types::BalFrame,
    claims: &ClaimBuffer,
    extract_claims: Option<&ClaimBuffer>,
) {
    let kardamom_types::BalFrame {
        bal_rlp,
        granularity,
        delta,
    } = frame;
    let granularity = *granularity;
    let mut slice: &[u8] = bal_rlp;
    match <alloy_eip7928::BlockAccessList as alloy_rlp::Decodable>::decode(&mut slice) {
        Ok(bal) if !bal.is_empty() => {
            let idx = Arc::new(kardamom_validator::parallel::ClaimIndex::from_alloy(&bal));
            claims.insert_arc(delta.block_number, granularity, idx.clone());
            if let Some(ec) = extract_claims {
                ec.insert_arc(delta.block_number, granularity, idx);
            }
        }
        Ok(_) => {}
        Err(e) => tracing::warn!(
            block = delta.block_number,
            error = %e,
            granularity = granularity.get(),
            "BAL access-list decode failed; block validates sequentially"
        ),
    }
}

/// `tx_receipts`: the executor's published receipts, over every publisher
/// the plane discovers, or the static channel's members.
///
/// Keep the `into_receiver()` call: it matters for shutdown. The handle
/// carries an `AeronRuntime` clone, for MDS destination churn, and moving
/// that clone into this pump task would deadlock process exit. The
/// runtime shuts down only when its last clone drops, that shutdown is
/// what ends `recv()`, and this task would hold the clone that blocks it.
/// Without this, the validator would ignore SIGTERM entirely, since
/// `drop(rt)` would become a no-op, leaving the engine's `tx_data`
/// subscriptions open and the join below never returning. The `shutdown`
/// token gives a second, explicit exit. The pump then stops at the same
/// time as the others, instead of waiting for `recv` to see the runtime
/// shut down.
pub(crate) fn spawn_receipts_pump(
    rt: &AeronRuntime,
    plane: &mut StreamPlane,
    executor_count_flag: Option<u32>,
    receipts: Arc<ReceiptBuffer>,
    shutdown: CancellationToken,
) -> Result<()> {
    // The flag, parsed to `NonZeroU32` (an explicit 0 is the same
    // "unset" `open_auto` warns about), takes precedence; with no flag,
    // the channel config's own typed count carries forward unchanged.
    let executor_count = match executor_count_flag {
        Some(n) => std::num::NonZeroU32::new(n),
        None => plane.channels().tx_receipts_executor_count,
    };
    let rx = plane
        .tx_receipts_subscriber(rt, executor_count)
        .context("open tx_receipts")?
        .into_receiver();
    let mut pump = ReceiptsPump { rx, receipts };
    tokio::spawn(async move { while pump.step(&shutdown).await.is_some() {} });
    Ok(())
}

/// The `tx_receipts` pump's live receiver plus its insertion buffer.
struct ReceiptsPump {
    rx: TxReceiptsReceiver,
    receipts: Arc<ReceiptBuffer>,
}

impl ReceiptsPump {
    /// One receive step. Returns `None` once shutdown is requested or the
    /// sender side is gone, either of which releases this task's
    /// `AeronRuntime` clone.
    async fn step(&mut self, shutdown: &CancellationToken) -> Option<()> {
        let next = tokio::select! {
            biased;
            () = shutdown.cancelled() => return None,
            r = self.rx.recv() => r,
        };
        let (_pos, r) = next?;
        self.receipts.insert(r);
        Some(())
    }
}

/// Background poller: expose the committed-block and state-root height
/// as metrics, and feed each block's observed MPT root to the attester.
/// `validator_state_root_block` is set only when the committed snapshot
/// actually yielded a root. This is an independent measurement, not a
/// mirror of the committed-block gauge.
pub(crate) fn spawn_commit_poller(
    snap_rx: SnapshotReceiver,
    attester_handle: Option<AttesterHandle>,
    // Interop attestation stream (unsigned): the same observed roots,
    // retained and served over `kardamom_subscribeAttestations`.
    attestation_store: Option<Arc<kardamom_validator::interop::AttestationStore>>,
    shutdown: CancellationToken,
) {
    tokio::spawn(async move {
        // Park on the snapshot watch: one wake per commit. Each publish
        // is a new committed block.
        let mut poller = CommitPoller {
            watch: snap_rx.watch(),
            attester_handle,
            attestation_store,
        };
        while poller.step(&shutdown).await.is_some() {}
    });
}

/// The commit poller's live snapshot watch plus the report targets.
struct CommitPoller {
    watch: tokio::sync::watch::Receiver<Option<StateSnapshot>>,
    attester_handle: Option<AttesterHandle>,
    attestation_store: Option<Arc<kardamom_validator::interop::AttestationStore>>,
}

impl CommitPoller {
    /// One step: wait for the next committed snapshot, and report its
    /// height and state root. Returns `None` once shutdown is requested
    /// or the writer side is gone.
    async fn step(&mut self, shutdown: &CancellationToken) -> Option<()> {
        tokio::select! {
            () = shutdown.cancelled() => return None,
            // Writer gone: shutdown path.
            changed = self.watch.changed() => changed.ok()?,
        }
        let Some(snap) = self.watch.borrow_and_update().clone() else {
            // No snapshot published yet: nothing to report this tick.
            return Some(());
        };
        let block = snap.block_number();
        metrics::set_committed_block(block);
        match snap.state_root() {
            Ok(Some(root)) => {
                metrics::set_state_root_block(block);
                tracing::debug!(block, state_root = %root, "validator committed block");
                if let Some(h) = &self.attester_handle {
                    h.submit_root(block, root);
                }
                if let Some(s) = &self.attestation_store {
                    s.push(block, root);
                }
            }
            Ok(None) => {}
            Err(e) => {
                tracing::warn!(block, error = %e, "state_root read failed");
            }
        }
        Some(())
    }
}
