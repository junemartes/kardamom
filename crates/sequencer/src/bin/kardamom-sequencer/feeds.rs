//! The sequencer's background feeds, and the publish-loop spawner.
//!
//! Three loops feed, or drain, the publish path. The cluster-egress
//! watermark task and the tx_receipts floor task supply the
//! `ResyncController` (see the sequencer-lag-resync spec).
//! `spawn_publish_loops` runs the canonical `TxRef` loop and the
//! `DepositRef` epoch pump on blocking tasks.
//!
//! Seam rule: the async shell is tokio. Anything that blocks, such as
//! Aeron polls, the crossbeam egress receiver, or the microsecond-backoff
//! publish loop, runs on `spawn_blocking`. It polls `Shutdown::is_signaled`.
//! Async-capable work, such as the receipts fan-in on an existing tokio
//! channel, is a plain task. It uses `select!` on `Shutdown::cancelled`.

use std::time::Duration;

use alloy_primitives::Address;
use kardamom_cluster_adapter::LiveEgress;
use kardamom_log::aeron_live::{IdleBackoff, TxReceiptsSubscriberHandle};
use kardamom_sequencer::config::SequencerConfig;
use kardamom_sequencer::epoch::process_epoch;
use kardamom_sequencer::error::SequencerError;
use kardamom_sequencer::outbound::TxOrderingRefPublisher;
use kardamom_sequencer::remote_epoch::process_remote_epoch;
use kardamom_sequencer::resync::{FloorUpdate, ResyncController, SharedWatermark};
use kardamom_sequencer::sequencer::{Sequencer, Shutdown};
use kardamom_types::{BPosition, Receipt};

use crate::adapters::{LiveEpochSub, LiveRemoteEpochSub, LiveTxDataSub, LiveTxErrorPub};

/// Spawn the egress-watermark feed. This is the silence authority: it
/// measures boundary-arrival gaps. Idle traffic still emits a boundary
/// every cluster tick, so arrivals, not count changes, are the liveness
/// signal. It raises the sticky lag flag and a starvation-proof metric.
///
/// This feed must never block without a bound (it uses `recv_timeout`).
/// The publish loop can block: a session offer waits on the session
/// thread, which may be mid-reconnect after a process freeze. A detector
/// that only runs when the publish loop runs would miss the freeze
/// entirely.
///
/// `LiveEgress` holds nothing `!Send`. But its only wait primitive is a
/// blocking crossbeam `recv_timeout`. So the body runs on `spawn_blocking`
/// and checks `shutdown` once per 500 ms tick. `reject_tx` stays a std
/// channel, because its consumer is the sync `ResyncController` in the
/// publish loop.
pub fn spawn_egress_watermark_feed(
    egress: LiveEgress,
    silence_ms: u64,
    partition: u32,
    watermark: SharedWatermark,
    reject_tx: crossbeam_channel::Sender<(Address, u64, u64)>,
    shutdown: Shutdown,
) -> tokio::task::JoinHandle<()> {
    tokio::task::spawn_blocking(move || {
        run_egress_watermark_feed(
            egress, silence_ms, partition, watermark, reject_tx, shutdown,
        )
    })
}

/// Body of the egress-watermark feed (see [`spawn_egress_watermark_feed`]).
fn run_egress_watermark_feed(
    mut egress: LiveEgress,
    silence_ms: u64,
    partition: u32,
    watermark: SharedWatermark,
    reject_tx: crossbeam_channel::Sender<(Address, u64, u64)>,
    shutdown: Shutdown,
) {
    use kardamom_cluster_adapter::live::EgressPoll;
    use kardamom_cluster_adapter::wire::{self, EgressItem, decode_egress};
    use kardamom_sequencer::metrics as seq_metrics;
    // Anchor at feed start, not None. The cluster emits a boundary every
    // tick, so "never seen a boundary" past the silence window is itself
    // the lag state. A restarted replica whose session never
    // re-establishes must flag, not stay silent forever.
    //
    // While the condition persists, the re-arm below repeats the flag
    // once per silence window: a bounded, genuinely alarming heartbeat.
    let mut last_boundary_at: Option<std::time::Instant> = Some(std::time::Instant::now());
    let flag = |at: &mut Option<std::time::Instant>, now: std::time::Instant| {
        if let Some(prev) = *at {
            let gap = now.duration_since(prev).as_millis() as u64;
            if gap >= silence_ms {
                watermark.flag_lag(gap);
                seq_metrics::record_lag_suspected(partition);
                tracing::info!(
                    partition,
                    gap_ms = gap,
                    "sequencer LAG suspected (boundary-arrival gap)"
                );
                // Re-arm from now, so a persistent outage flags
                // once per silence window, not on every poll.
                *at = Some(now);
            }
        }
    };
    while !shutdown.is_signaled() {
        match egress.recv_timeout(Duration::from_millis(500)) {
            EgressPoll::Frame(frame) => {
                // The sealer rejected one of this sequencer's refs
                // because a known sender's nonce was not the
                // expected next one. Forward (sender, expected) to
                // the publish loop, which rewinds the unconfirmed
                // ledger and republishes right away, instead of
                // waiting out the confirm timeout.
                if frame.first() == Some(&wire::EGRESS_KIND_CONTIGUITY_REJECT) {
                    if let Ok(EgressItem::ContiguityReject {
                        sender,
                        nonce,
                        expected,
                    }) = decode_egress(&frame)
                    {
                        tracing::warn!(
                            partition,
                            ?sender,
                            nonce,
                            expected,
                            "sealer contiguity reject received"
                        );
                        let _ = reject_tx.send((sender, nonce, expected));
                    }
                    continue;
                }
                // Check the cheap kind byte first. Relayed records
                // arrive at full line rate on every replica, and
                // fully decoding them here, just to discard them,
                // costs measurable CPU.
                if frame.first() != Some(&wire::EGRESS_KIND_BOUNDARY) {
                    continue;
                }
                if let Ok(EgressItem::Boundary(b)) = decode_egress(&frame) {
                    let now = std::time::Instant::now();
                    // A 30 second freeze shows up here as one long
                    // inter-arrival gap. The backlog drains
                    // instantly on resume, but the gap between
                    // the last pre-freeze arrival and this one
                    // is real wall-clock time.
                    flag(&mut last_boundary_at, now);
                    last_boundary_at = Some(now);
                    watermark.store(b.end_tx_idx.as_index());
                }
            }
            EgressPoll::Idle => {
                // Egress is silent while this thread is demonstrably
                // alive. This could be a partition from egress, or a
                // dead cluster boundary clock. Either way, the
                // response is the same.
                flag(&mut last_boundary_at, std::time::Instant::now());
            }
            EgressPoll::Closed => return,
        }
    }
}

/// Spawn the tx_receipts-to-per-sender executed-truth floor feed. Only
/// this shard's senders reach the floor channel.
///
/// The receipts handle already fans in over a tokio unbounded channel. The
/// Aeron poll thread is the producer. So this is a plain async task. It
/// awaits `recv()` and `Shutdown::cancelled`, with no idle-sleep polling.
/// The handle reduces to its receiver (`into_receiver`), so the task holds
/// no `AeronRuntime` clone. `floor_tx` stays a std channel, because its
/// consumer is the sync `ResyncController` in the publish loop.
pub fn spawn_receipt_floor_feed(
    sub: TxReceiptsSubscriberHandle,
    shutdown: Shutdown,
    partition_count: u32,
    partition_index: u32,
    floor_tx: crossbeam_channel::Sender<FloorUpdate>,
) -> tokio::task::JoinHandle<()> {
    let rx = sub.into_receiver();
    tokio::spawn(run_receipt_floor_feed(
        rx,
        shutdown,
        partition_count,
        partition_index,
        floor_tx,
    ))
}

/// Body of the receipts-floors task (see [`spawn_receipt_floor_feed`]).
async fn run_receipt_floor_feed(
    mut rx: tokio::sync::mpsc::UnboundedReceiver<(BPosition, Receipt)>,
    shutdown: Shutdown,
    partition_count: u32,
    partition_index: u32,
    floor_tx: crossbeam_channel::Sender<FloorUpdate>,
) {
    loop {
        let receipt = tokio::select! {
            biased;
            _ = shutdown.cancelled() => return,
            msg = rx.recv() => match msg {
                Some((_pos, receipt)) => receipt,
                // The subscription closed. The runtime shut down.
                // Nothing more to feed.
                None => return,
            },
        };
        // Nonce-0 receipts are excluded from floor
        // evidence. A deposit receipt stamps a filler
        // `nonce: 0` (deposits run with the nonce check
        // disabled; see executor.rs `tx_env_from_deposit`).
        // This makes it indistinguishable, on the wire,
        // from a genuine nonce-0 transaction receipt.
        // Treating one as proof that L2 tx-nonce 0 executed
        // could wrongly Past-reject a sender's first
        // transaction. The cost: floors only ever prove
        // from nonce 1 upward. This degrades toward
        // publish, the safer side.
        //
        // Only this shard's senders can appear in this
        // replica's publish stream, so the floor map stays
        // bounded to them.
        //
        // Forward every partition-matched receipt. The
        // controller splits floor evidence (skip and
        // deposit receipts excluded, since they consume no
        // L2 nonce) from publish confirmations (skip
        // receipts count as confirmations: ordering is the
        // claim).
        if kardamom_sequencer::partition::partition_for(receipt.from, partition_count)
            != partition_index
        {
            continue;
        }
        // A send failure means the publish loop is gone. Exit.
        if floor_tx
            .send(FloorUpdate {
                sender: receipt.from,
                executed_nonce: receipt.nonce,
                skip_reason: receipt.skip_reason,
                deposit: receipt.is_deposit(),
            })
            .is_err()
        {
            return;
        }
    }
}

pub type LoopHandle = tokio::task::JoinHandle<Result<(), SequencerError>>;

/// Spawn the main sequencer loop and the two origin pumps, over three
/// `TxOrderingRefPublisher`s (`main_pub` for the canonical `TxRef` loop,
/// `deposit_pub` for the L1 epoch pump, `remote_epoch_pub` for the interop
/// one). This is generic over the publisher type, so the Aeron and cluster
/// branches share one implementation. Both branches supply concrete
/// publishers that implement the trait.
///
/// The two origin pumps run as separate loops, not one merged poll, because
/// the origins are independent: a peer whose feed has stalled must not
/// delay L1 deposits, and an L1 RPC outage must not stall cross-chain
/// delivery.
#[allow(clippy::too_many_arguments)]
pub fn spawn_publish_loops<P>(
    cfg: SequencerConfig,
    mut tx_data: LiveTxDataSub,
    main_pub: P,
    deposit_pub: P,
    remote_epoch_pub: P,
    mut tx_errors: LiveTxErrorPub,
    mut epoch_sub: LiveEpochSub,
    mut remote_epoch_sub: LiveRemoteEpochSub,
    resync: Option<ResyncController>,
    shutdown_for_main: Shutdown,
    shutdown_for_deposits: Shutdown,
    shutdown_for_remote_epochs: Shutdown,
) -> (LoopHandle, LoopHandle, LoopHandle)
where
    P: TxOrderingRefPublisher + Send + 'static,
{
    // The sequencer main loop is sync (it uses std::thread and
    // std::thread::sleep for backoff). Hand it to spawn_blocking, so the
    // async runtime stays responsive for shutdown handling.
    let mut main_pub = main_pub;
    let join_main = tokio::task::spawn_blocking(move || -> Result<(), SequencerError> {
        let mut sequencer = Sequencer::new(cfg);
        if let Some(controller) = resync {
            sequencer.enable_resync(controller);
        }
        sequencer.run(
            &mut tx_data,
            &mut main_pub,
            &mut tx_errors,
            shutdown_for_main,
        )
    });

    // Independent pump for tx_deposits to epoch on tx_ordering. The epoch
    // path is not nonce-gated. It is a simple poll-and-publish loop that
    // runs alongside the canonical TxData-to-TxRef path. It stays on
    // spawn_blocking. `process_epoch` does a sync Aeron poll and a sync
    // cluster offer. So the loop polls `is_signaled` between backoff
    // sleeps.
    let mut epoch_pub = deposit_pub;
    let join_deposits = tokio::task::spawn_blocking(move || -> Result<(), SequencerError> {
        let mut idle = IdleBackoff::new(Duration::from_micros(1), Duration::from_micros(100), 1);
        loop {
            if shutdown_for_deposits.is_signaled() {
                return Ok(());
            }
            match process_epoch(&mut epoch_sub, &mut epoch_pub) {
                Ok(true) => idle.reset(),
                Ok(false) => std::thread::sleep(idle.idle_wait()),
                Err(SequencerError::Backpressure) => {
                    std::thread::sleep(Duration::from_micros(10));
                }
                Err(SequencerError::IngressDisconnected) => return Ok(()),
                Err(e) => return Err(e),
            }
        }
    });

    // Independent pump for tx_remote_epochs to a remote-origin record on
    // tx_ordering, on the same terms as the deposit pump above.
    let mut remote_epoch_pub = remote_epoch_pub;
    let join_remote_epochs = tokio::task::spawn_blocking(move || -> Result<(), SequencerError> {
        let mut idle = IdleBackoff::new(Duration::from_micros(1), Duration::from_micros(100), 1);
        loop {
            if shutdown_for_remote_epochs.is_signaled() {
                return Ok(());
            }
            match process_remote_epoch(&mut remote_epoch_sub, &mut remote_epoch_pub) {
                Ok(true) => idle.reset(),
                Ok(false) => std::thread::sleep(idle.idle_wait()),
                Err(SequencerError::Backpressure) => {
                    std::thread::sleep(Duration::from_micros(10));
                }
                Err(SequencerError::IngressDisconnected) => return Ok(()),
                Err(e) => return Err(e),
            }
        }
    });

    (join_main, join_deposits, join_remote_epochs)
}
