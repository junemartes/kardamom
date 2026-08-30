//! The sequencer's background feeds + the publish-loop spawner.
//!
//! Three loops feed (or drain) the publish path: the cluster-egress
//! watermark task and the tx_receipts floor task supply the
//! `ResyncController` (spec: sequencer-lag-resync); `spawn_publish_loops`
//! runs the canonical `TxRef` loop and the `DepositRef` epoch pump on
//! blocking tasks.
//!
//! Seam rule: the async shell is tokio; anything that blocks (Aeron polls,
//! the crossbeam egress receiver, the µs-backoff publish loop) runs on
//! `spawn_blocking` and polls `Shutdown::is_signaled`; async-capable work
//! (the receipts fan-in, already a tokio channel) is a plain task that
//! `select!`s on `Shutdown::cancelled`.

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
use kardamom_types::{BPosition, Receipt};

use crate::adapters::{LiveEpochSub, LiveTxDataSub, LiveTxErrorPub};

/// Spawn the egress-watermark FEED — the silence authority: it
/// measures BOUNDARY-ARRIVAL gaps (idle traffic still emits a boundary every
/// cluster tick, so arrivals — not count changes — are the liveness signal)
/// and raises the sticky lag flag + a starvation-proof metric. It must never
/// block unboundedly (recv_timeout), because the publish loop CAN — a session
/// offer waits on the session thread, which after a process freeze is mid
/// reconnect — and a detector that only runs when the loop runs misses
/// the freeze entirely (observed: sequencer-lapse, CI run 30163255470).
///
/// `LiveEgress` holds nothing `!Send`, but its only wait primitive is a
/// blocking crossbeam `recv_timeout`, so the body runs on `spawn_blocking`
/// and checks `shutdown` once per 500 ms tick. `reject_tx` stays a std
/// channel: its consumer is the sync `ResyncController` in the publish loop.
pub fn spawn_egress_watermark_feed(
    egress: LiveEgress,
    silence_ms: u64,
    partition: u32,
    watermark: SharedWatermark,
    reject_tx: std::sync::mpsc::Sender<(Address, u64, u64)>,
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
    reject_tx: std::sync::mpsc::Sender<(Address, u64, u64)>,
    shutdown: Shutdown,
) {
    use kardamom_cluster_adapter::live::EgressPoll;
    use kardamom_cluster_adapter::wire::{self, EgressItem, decode_egress};
    use kardamom_sequencer::metrics as seq_metrics;
    // Anchored at FEED START, not None: the cluster emits a
    // boundary every tick, so "never seen a boundary" past the
    // silence window IS the lag state — a restarted replica
    // whose session never (re)establishes must flag, not stay
    // silent forever (observed: seq-a restarted by an earlier
    // chaos kill sat egress-dead through the whole lapse case
    // with lag_suspected pinned at 0 — CI run 30164871699).
    // While the condition persists, the re-arm below repeats the
    // flag once per silence window — a bounded, genuinely
    // alarming heartbeat.
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
                // Re-arm from now so a persistent outage flags
                // once per silence window, not per poll.
                *at = Some(now);
            }
        }
    };
    while !shutdown.is_signaled() {
        match egress.recv_timeout(Duration::from_millis(500)) {
            EgressPoll::Frame(frame) => {
                // #85 fix B: the sealer rejected one of OUR refs
                // because a known sender's nonce was not the
                // expected next one — forward (sender, expected)
                // to the publish loop, which rewinds the
                // unconfirmed ledger and republishes immediately
                // instead of waiting out the confirm timeout.
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
                // Cheap kind check FIRST: relayed records arrive
                // at full line rate on every replica, and fully
                // decoding them here just to discard them is
                // measurable CPU on the shared CI hosts.
                if frame.first() != Some(&wire::EGRESS_KIND_BOUNDARY) {
                    continue;
                }
                if let Ok(EgressItem::Boundary(b)) = decode_egress(&frame) {
                    let now = std::time::Instant::now();
                    // A 30 s freeze shows up HERE as one long
                    // inter-arrival gap: the backlog drains
                    // instantly on resume, but the gap between
                    // the last pre-freeze arrival and this one
                    // is wall-clock real.
                    flag(&mut last_boundary_at, now);
                    last_boundary_at = Some(now);
                    watermark.store(b.end_tx_idx.as_index());
                }
            }
            EgressPoll::Idle => {
                // Egress silent while we are demonstrably alive:
                // partitioned from egress (or the cluster's
                // boundary clock is dead) — same response.
                flag(&mut last_boundary_at, std::time::Instant::now());
            }
            EgressPoll::Closed => return,
        }
    }
}

/// Spawn the tx_receipts → per-sender executed-truth floor feed (only this
/// shard's senders reach the floor channel).
///
/// The receipts handle already fans in over a tokio unbounded channel (the
/// Aeron poll thread is the producer), so this is a plain async task: it
/// awaits `recv()` and `Shutdown::cancelled` — no idle-sleep polling. The
/// handle is reduced to its receiver (`into_receiver`) so the task holds no
/// `AeronRuntime` clone; `floor_tx` stays a std channel because its consumer
/// is the sync `ResyncController` in the publish loop.
pub fn spawn_receipt_floor_feed(
    sub: TxReceiptsSubscriberHandle,
    shutdown: Shutdown,
    partition_count: u32,
    partition_index: u32,
    floor_tx: std::sync::mpsc::Sender<FloorUpdate>,
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
    floor_tx: std::sync::mpsc::Sender<FloorUpdate>,
) {
    loop {
        let receipt = tokio::select! {
            biased;
            _ = shutdown.cancelled() => return,
            msg = rx.recv() => match msg {
                Some((_pos, receipt)) => receipt,
                // Subscription closed (runtime shut down) — nothing more to feed.
                None => return,
            },
        };
        // nonce == 0 receipts are EXCLUDED from floor
        // evidence: deposit receipts stamp a filler
        // `nonce: 0` (deposits run with the nonce check
        // disabled; executor.rs `tx_env_from_deposit`)
        // and are indistinguishable from a genuine
        // nonce-0 tx receipt on the wire. Treating one
        // as proof that L2 tx-nonce 0 executed could
        // wrongly Past-reject a sender's first tx. Cost:
        // floors only ever prove from nonce >= 1 —
        // degradation toward publish, the guarded side.
        // Only this shard's senders can appear in this
        // replica's publish stream — keep the floor map
        // bounded to them.
        // Every partition-matched receipt is forwarded:
        // the controller splits floor evidence (skip and
        // deposits excluded — they consume no L2 nonce)
        // from publish CONFIRMATIONS (#85: skip receipts
        // count, ordering is the claim).
        if kardamom_sequencer::partition::partition_for(receipt.from, partition_count)
            != partition_index
        {
            continue;
        }
        // Send failure = publish loop gone; exit.
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

pub type LoopHandle = tokio::task::JoinHandle<Result<(), SequencerError>>;

/// Spawn the main sequencer loop + the deposit pump over a pair of
/// `TxOrderingRefPublisher`s (`main_pub` for the canonical `TxRef` loop,
/// `deposit_pub` for the epoch pump). Generic over the publisher type so
/// the Aeron and cluster branches share one implementation; both supply
/// concrete publishers that impl the trait.
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
    // The sequencer main loop is sync (std::thread + std::thread::sleep
    // backoff). Hand it to spawn_blocking so the async runtime stays
    // responsive for shutdown handling.
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

    // Independent pump for tx_deposits → epoch on tx_ordering. The epoch
    // path is not nonce-gated; it's a simple poll → publish loop
    // that runs alongside the canonical TxData → TxRef path. It stays on
    // spawn_blocking: `process_epoch` is a sync Aeron poll + a sync cluster
    // offer, so the loop polls `is_signaled` between backoff sleeps.
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
