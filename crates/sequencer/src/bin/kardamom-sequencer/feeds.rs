//! The sequencer's background feed threads, and the publish-loop spawner.
//!
//! Three loops feed, or drain, the publish path. The cluster-egress
//! watermark thread and the tx_receipts floor thread supply the
//! `ResyncController` (see the sequencer-lag-resync spec).
//! `spawn_publish_loops` runs the canonical `TxRef` loop and the
//! `DepositRef` epoch pump on blocking tasks.

use std::time::Duration;

use alloy_primitives::Address;
use kardamom_cluster_adapter::LiveEgress;
use kardamom_log::aeron_live::{IdleBackoff, TxReceiptsSubscriberHandle};
use kardamom_sequencer::config::SequencerConfig;
use kardamom_sequencer::epoch::process_epoch;
use kardamom_sequencer::error::SequencerError;
use kardamom_sequencer::outbound::TxOrderingRefPublisher;
use kardamom_sequencer::resync::{FloorUpdate, ResyncController, SharedWatermark};
use kardamom_sequencer::sequencer::{Sequencer, Shutdown};

use crate::adapters::{LiveEpochSub, LiveTxDataSub, LiveTxErrorPub};

/// Spawn the egress-watermark FEED thread. This is the silence authority:
/// it measures boundary-arrival gaps. Idle traffic still emits a boundary
/// every cluster tick, so arrivals, not count changes, are the liveness
/// signal. It raises the sticky lag flag and a starvation-proof metric.
///
/// This thread must never block without a bound (it uses `recv_timeout`).
/// The publish loop can block: a session offer waits on the session
/// thread, which may be mid-reconnect after a process freeze. A detector
/// that only runs when the publish loop runs would miss the freeze
/// entirely.
pub fn spawn_egress_watermark_feed(
    egress: LiveEgress,
    silence_ms: u64,
    partition: u32,
    watermark: SharedWatermark,
    reject_tx: std::sync::mpsc::Sender<(Address, u64, u64)>,
) -> std::io::Result<std::thread::JoinHandle<()>> {
    std::thread::Builder::new()
        .name("cluster-egress-watermark".into())
        .spawn(move || {
            run_egress_watermark_feed(egress, silence_ms, partition, watermark, reject_tx)
        })
}

/// Body of the egress-watermark thread (see [`spawn_egress_watermark_feed`]).
fn run_egress_watermark_feed(
    mut egress: LiveEgress,
    silence_ms: u64,
    partition: u32,
    watermark: SharedWatermark,
    reject_tx: std::sync::mpsc::Sender<(Address, u64, u64)>,
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
    loop {
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
pub fn spawn_receipt_floor_feed(
    sub: TxReceiptsSubscriberHandle,
    shutdown: Shutdown,
    partition_count: u32,
    partition_index: u32,
    floor_tx: std::sync::mpsc::Sender<FloorUpdate>,
) -> std::io::Result<std::thread::JoinHandle<()>> {
    std::thread::Builder::new()
        .name("tx-receipts-floors".into())
        .spawn(move || {
            run_receipt_floor_feed(sub, shutdown, partition_count, partition_index, floor_tx)
        })
}

/// Body of the receipts-floors thread (see [`spawn_receipt_floor_feed`]).
fn run_receipt_floor_feed(
    mut sub: TxReceiptsSubscriberHandle,
    shutdown: Shutdown,
    partition_count: u32,
    partition_index: u32,
    floor_tx: std::sync::mpsc::Sender<FloorUpdate>,
) {
    let mut idle = IdleBackoff::new(Duration::from_micros(1), Duration::from_micros(500), 1);
    while !shutdown.is_signaled() {
        match sub.try_recv() {
            Some((_pos, receipt)) => {
                idle.reset();
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
                    == partition_index
                {
                    // A send failure means the publish loop is gone. Exit.
                    if floor_tx
                        .send(FloorUpdate {
                            sender: receipt.from,
                            executed_nonce: receipt.nonce,
                            invalid_skip: receipt.is_invalid_skip(),
                            deposit: receipt.is_deposit(),
                        })
                        .is_err()
                    {
                        return;
                    }
                }
            }
            None => std::thread::sleep(idle.idle_wait()),
        }
    }
}

pub type LoopHandle = tokio::task::JoinHandle<Result<(), SequencerError>>;

/// Spawn the main sequencer loop and the deposit pump, over a pair of
/// `TxOrderingRefPublisher`s (`main_pub` for the canonical `TxRef` loop,
/// `deposit_pub` for the epoch pump). This is generic over the publisher
/// type, so the Aeron and cluster branches share one implementation. Both
/// branches supply concrete publishers that implement the trait.
#[allow(clippy::too_many_arguments)]
pub fn spawn_publish_loops<P>(
    cfg: SequencerConfig,
    mut tx_data: LiveTxDataSub,
    main_pub: P,
    deposit_pub: P,
    mut tx_errors: LiveTxErrorPub,
    mut epoch_sub: LiveEpochSub,
    resync: Option<ResyncController>,
    shutdown_for_main: Shutdown,
    shutdown_for_deposits: Shutdown,
) -> (LoopHandle, LoopHandle)
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
    // runs alongside the canonical TxData-to-TxRef path.
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

    (join_main, join_deposits)
}
