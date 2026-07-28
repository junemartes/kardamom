//! `kardamom-sequencer`: per-partition sequencer process.
//!
//! Parses a TOML [`SequencerConfig`], opens its shard's tx_data subscriber +
//! the Aeron Cluster (Raft) ref publisher (tx_ordering) + a tx_errors publisher
//! for rejection signals, and runs the sequencer main loop on a dedicated
//! blocking thread until SIGTERM / Ctrl-C.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use kardamom_log::aeron_live::{
    AeronRuntime, TxDataSubscriberHandle, TxDepositsSubscriberHandle, TxErrorsPublisherHandle,
};
use kardamom_log::config::{ChannelsConfig, LogConfig};
use kardamom_sequencer::config::SequencerConfig;
use kardamom_sequencer::deposit::{DepositSubscriber, process_deposit};
use kardamom_sequencer::error::SequencerError;
use kardamom_sequencer::inbound::TxDataSubscriber;
use kardamom_sequencer::outbound::{TxErrorPublisher, TxOrderingRefPublisher};
use kardamom_sequencer::sequencer::{Sequencer, Shutdown};
use kardamom_types::{BPosition, Deposit, TxDataLoc, TxEnvelope, TxError};

#[derive(Debug, Parser)]
#[command(
    name = "kardamom-sequencer",
    version,
    about = "kardamom sequencer process"
)]
struct Args {
    /// Path to a TOML config file (schema: `SequencerConfig`).
    #[arg(long)]
    config: PathBuf,
    /// Optional `LogConfig` TOML supplying the Aeron `[channels]` config.
    /// Unset ⇒ built-in single-host IPC defaults (preserves local/e2e
    /// behaviour); multi-host deployments point this at the rendered UDP
    /// channels config.
    #[arg(long, env = "KARDAMOM_LOG_CONFIG")]
    log_config: Option<PathBuf>,
    /// Aeron Media Driver directory (`aeron.dir`).
    #[arg(long)]
    aeron_dir: Option<PathBuf>,
    /// Override the partition index from the config.
    #[arg(long)]
    partition_index: Option<u32>,
    /// Override the partition count (M).
    #[arg(long)]
    partition_count: Option<u32>,
    /// Replica-group shard rotation: the effective partition becomes
    /// `(partition_index + partition_offset) % partition_count`, and
    /// `sequencer_id` follows it (unless explicitly overridden). Lets a
    /// second Nomad group of racing replicas reuse the same node-derived
    /// `--partition-index` while serving a rotated shard, so the two
    /// replicas of any shard land on different nodes deterministically.
    /// Incompatible with an explicit `--sequencer-id` (the tx_data
    /// subscription and `TxRef.shard_id` must both follow the rotated
    /// shard).
    /// Racing replicas are safe by construction: refs encode
    /// deterministically from the shared per-shard tx_data stream and the
    /// Aeron Cluster dedups records by canonical_id first-seen — the same
    /// mechanism that already absorbs the M duplicate DepositRefs.
    #[arg(long, default_value_t = 0)]
    partition_offset: u32,
    /// Override the sequencer id embedded in every tx_ordering `TxRef`.
    /// If omitted and the TOML did not set it, falls back to
    /// `partition_index as u8`.
    #[arg(long)]
    sequencer_id: Option<u8>,
    /// Override the CPU core to pin to.
    #[arg(long)]
    core_id: Option<usize>,
    /// This node's cluster-egress endpoint `ip:port` (cluster mode). Overrides/sets
    /// the [cluster] egress_channel as `aeron:udp?endpoint=<ip:port>`. Injected per
    /// node by the Nomad job as ${meta.node_ip}:<cluster_egress_port>.
    #[arg(long, env = "KARDAMOM_CLUSTER_EGRESS_ENDPOINT")]
    cluster_egress_endpoint: Option<String>,
    /// Address for the Prometheus /metrics HTTP listener.
    #[arg(long, env = "KARDAMOM_METRICS_ADDR", default_value = "127.0.0.1:9001")]
    metrics_addr: std::net::SocketAddr,
    /// Host identifier; stamped on every metric.
    #[arg(long, env = "KARDAMOM_HOST_ID", default_value = "local")]
    host_id: String,
    /// The cluster's first-seen dedup window (`[resync] dedup_capacity`).
    /// MUST equal the JVM's `-Dkardamom.cluster.dedupCapacity` — the lag
    /// horizon the resync mechanism protects
    /// (docs/agents/sequencer-lag-resync-spec.md).
    #[arg(long, env = "KARDAMOM_CLUSTER_DEDUP_CAPACITY")]
    cluster_dedup_capacity: Option<u64>,
    /// Watermark-jump enter threshold as a percent of the dedup capacity
    /// (`[resync] enter_percent`).
    #[arg(long, env = "KARDAMOM_RESYNC_ENTER_PERCENT")]
    resync_enter_percent: Option<u64>,
    /// Boundary-silence resync trigger, ms (`[resync] boundary_silence_ms`).
    #[arg(long, env = "KARDAMOM_RESYNC_BOUNDARY_SILENCE_MS")]
    resync_boundary_silence_ms: Option<u64>,
    /// Executor replica count for the tx_receipts MDS fan-in (parity with
    /// the validator). Falls back to `channels.tx_receipts_executor_count`;
    /// irrelevant when receipts ride multicast (the cluster deploy).
    #[arg(long)]
    executor_count: Option<u32>,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> anyhow::Result<()> {
    init_tracing();
    let args = Args::parse();
    kardamom_obs::init(
        "sequencer",
        args.metrics_addr,
        &args.host_id,
        env!("CARGO_PKG_VERSION"),
        option_env!("KARDAMOM_GIT_SHA").unwrap_or("unknown"),
    )?;
    let raw = std::fs::read_to_string(&args.config).context("read config")?;
    let mut cfg: SequencerConfig = toml::from_str(&raw).context("parse config")?;

    if let Some(i) = args.partition_index {
        cfg.partition_index = i;
    }
    if let Some(m) = args.partition_count {
        cfg.partition_count = m;
    }
    if args.partition_offset != 0 {
        // An explicit --sequencer-id combined with rotation would subscribe
        // to tx_data stream `sequencer_id` while the wrong-shard guard
        // filters on the rotated `partition_index`: the replica would
        // silently drop every envelope (and its TxRefs would stamp a
        // shard_id its twin doesn't, breaking byte-identical dedup).
        anyhow::ensure!(
            args.sequencer_id.is_none(),
            "--sequencer-id cannot be combined with --partition-offset: \
             the sequencer id must follow the rotated shard \
             (sequencer_id == partition_index)"
        );
        let raw_index = cfg.partition_index;
        cfg.rotate_partition(args.partition_offset);
        tracing::info!(
            raw_index,
            offset = args.partition_offset,
            rotated = cfg.partition_index,
            "partition-offset: rotated shard assignment (racing replica group)"
        );
    }
    if let Some(id) = args.sequencer_id {
        cfg.sequencer_id = id;
    } else if cfg.sequencer_id == 0 && cfg.partition_index != 0 {
        cfg.sequencer_id = cfg.partition_index as u8;
    }
    if let Some(c) = args.core_id {
        cfg.core_id = Some(c);
    }
    // Per-node cluster egress endpoint: the cluster client's egress_channel is
    // this node's reachable address (the node IP differs per replica), so it's
    // injected by the Nomad job rather than baked into the static config template.
    if let Some(ep) = args.cluster_egress_endpoint.as_deref() {
        cfg.cluster.egress_channel = format!("aeron:udp?endpoint={ep}");
    }
    if let Some(cap) = args.cluster_dedup_capacity {
        cfg.resync.dedup_capacity = cap;
    }
    if let Some(p) = args.resync_enter_percent {
        cfg.resync.enter_percent = p;
    }
    if let Some(ms) = args.resync_boundary_silence_ms {
        cfg.resync.boundary_silence_ms = ms;
    }
    cfg.validate().context("validate config")?;
    // Contract line for the CI drift check: this MUST match the cluster JVM's
    // -Dkardamom.cluster.dedupCapacity (see cluster.nomad.hcl).
    kardamom_sequencer::metrics::record_start_time();
    tracing::info!(
        dedup_capacity = cfg.resync.dedup_capacity,
        enter_percent = cfg.resync.enter_percent,
        boundary_silence_ms = cfg.resync.boundary_silence_ms,
        "resync contract: dedup_capacity must equal the cluster's -Dkardamom.cluster.dedupCapacity"
    );

    tracing::info!(
        partition_index = cfg.partition_index,
        sequencer_id = cfg.sequencer_id,
        "kardamom-sequencer starting"
    );

    let channels: ChannelsConfig = LogConfig::resolve(args.log_config.as_deref())
        .context("resolve log config")?
        .channels;
    let rt = match args.aeron_dir.as_ref() {
        Some(dir) => AeronRuntime::spawn_with_dir(dir).context("spawn AeronRuntime with dir")?,
        None => AeronRuntime::spawn_default().context("spawn AeronRuntime")?,
    };

    let shard_id = cfg.sequencer_id;
    let tx_data_sub = TxDataSubscriberHandle::open(&rt, &channels, shard_id)
        .context("open TxDataSubscriberHandle")?;
    let tx_deposits_sub = TxDepositsSubscriberHandle::open(&rt, &channels)
        .context("open TxDepositsSubscriberHandle")?;
    let tx_errors_pub =
        TxErrorsPublisherHandle::open(&rt, &channels).context("open TxErrorsPublisherHandle")?;

    let shutdown = Shutdown::new();
    let shutdown_for_main = shutdown.clone();
    let shutdown_for_deposits = shutdown.clone();

    tracing::info!(
        "nonce floors: sequencer holds no state-DB reader; cold senders seed at \
         0 and committed floors are recovered from the tx_receipts stream via \
         the receipt-floor resync. NOTE: a restarted replica does NOT regain \
         coverage of established senders until resync floors catch up (F02.1 \
         re-opened)"
    );
    let cfg_clone = cfg.clone();

    // tx_ordering is ALWAYS published to the Aeron Cluster (Raft) ingress. The
    // cluster-session guard (`LiveCluster`) + its dedicated Aeron runtime must
    // outlive both publish loops, so bind the guard in the outer scope; it is
    // dropped only after the `join_*` awaits below.
    //
    // DEDICATED cluster runtime (own Aeron thread, same aeron dir) so the cluster
    // session never contends with the tx_data subscription on the main `rt`.
    let cluster_rt = match args.aeron_dir.as_ref() {
        Some(dir) => {
            AeronRuntime::spawn_with_dir(dir).context("spawn cluster AeronRuntime with dir")?
        }
        None => AeronRuntime::spawn_default().context("spawn cluster AeronRuntime")?,
    };
    let (cluster_guard, cluster_pub, cluster_egress) =
        kardamom_sequencer::outbound::cluster::cluster_ref_publisher_with_egress(
            cluster_rt,
            cfg.cluster.to_live(),
        )
        .context("connect cluster ref publisher")?;
    tracing::info!("kardamom-sequencer: tx_ordering via Aeron Cluster");

    // --- lag detection + receipt-floor resync (spec: sequencer-lag-resync) —
    // three feeds into the publish loop's ResyncController:
    //  1. egress-watermark thread: the cluster broadcasts every boundary to
    //     this publisher session; decode `end_tx_idx` (global canonical
    //     count) into the shared watermark, discard records.
    //  2. receipts thread: tx_receipts → per-sender executed-truth floors
    //     (only this shard's senders).
    //  3. the controller itself, handed to the Sequencer via enable_resync.
    let (resync_controller, floor_tx, watermark) =
        kardamom_sequencer::resync::resync_channel(cfg.resync.clone(), cfg.partition_index);

    // The FEED thread is the silence authority: it measures BOUNDARY-ARRIVAL
    // gaps (idle traffic still emits a boundary every cluster tick, so
    // arrivals — not count changes — are the liveness signal) and raises the
    // sticky lag flag + a starvation-proof metric. It must never block
    // unboundedly (recv_timeout), because the publish loop CAN — a session
    // offer waits on the session thread, which after a process freeze is mid
    // reconnect — and a detector that only runs when the loop runs misses
    // the freeze entirely (observed: sequencer-lapse, CI run 30163255470).
    let watermark_thread = std::thread::Builder::new()
        .name("cluster-egress-watermark".into())
        .spawn({
            let mut egress = cluster_egress;
            let silence_ms = cfg.resync.boundary_silence_ms;
            let partition = cfg.partition_index;
            move || {
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
                let mut last_boundary_at: Option<std::time::Instant> =
                    Some(std::time::Instant::now());
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
                loop {
                    match egress.recv_timeout(Duration::from_millis(500)) {
                        EgressPoll::Frame(frame) => {
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
        })
        .context("spawn egress-watermark thread")?;

    // DEDICATED receipts runtime: receipt decode runs at full line rate, and
    // the main `rt`'s polling thread must stay dedicated to the tx_data
    // subscription (same isolation rationale as `cluster_rt` above; sharing
    // was observed to collapse the sequencer's sustainable ingest rate).
    let receipts_rt = match args.aeron_dir.as_ref() {
        Some(dir) => {
            AeronRuntime::spawn_with_dir(dir).context("spawn receipts AeronRuntime with dir")?
        }
        None => AeronRuntime::spawn_default().context("spawn receipts AeronRuntime")?,
    };
    let receipts_sub = open_tx_receipts(
        &receipts_rt,
        &channels,
        args.executor_count
            .unwrap_or(channels.tx_receipts_executor_count),
    )?;
    let receipts_thread = std::thread::Builder::new()
        .name("tx-receipts-floors".into())
        .spawn({
            let shutdown = shutdown.clone();
            let partition_count = cfg.partition_count;
            let partition_index = cfg.partition_index;
            let mut sub = receipts_sub;
            move || {
                let mut backoff_us = 1u64;
                while !shutdown.is_signaled() {
                    match sub.try_recv() {
                        Some((_pos, receipt)) => {
                            backoff_us = 1;
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
                            // Invalid-skip receipts (#92: status=false,
                            // gas_used=0) mark a tx that did NOT happen — no
                            // nonce consumed — so they are NOT floor
                            // evidence: a skipped NonceTooHigh tx's high
                            // nonce would otherwise advance the floor past
                            // nonces that never executed (a canonical gap,
                            // the exact disaster floors exist to prevent).
                            if !receipt.is_invalid_skip()
                                && receipt.nonce > 0
                                && kardamom_sequencer::partition::partition_for(
                                    receipt.from,
                                    partition_count,
                                ) == partition_index
                            {
                                // Send failure = publish loop gone; exit.
                                if floor_tx
                                    .send(kardamom_sequencer::resync::FloorUpdate {
                                        sender: receipt.from,
                                        executed_nonce: receipt.nonce,
                                    })
                                    .is_err()
                                {
                                    return;
                                }
                            }
                        }
                        None => {
                            std::thread::sleep(Duration::from_micros(backoff_us));
                            backoff_us = backoff_us.saturating_mul(2).min(500);
                        }
                    }
                }
            }
        })
        .context("spawn receipts-floors thread")?;

    // Clone shares the single session thread; offers serialise through it. Both
    // loops use `cluster_pub` (impl `TxOrderingRefPublisher`) — the canonical
    // `TxRef` loop and the `DepositRef` pump.
    let (join_main, join_deposits) = spawn_publish_loops(
        cfg_clone,
        LiveTxDataSub::new(tx_data_sub),
        cluster_pub.clone(),
        cluster_pub,
        LiveTxErrorPub::new(tx_errors_pub),
        LiveDepositSub::new(tx_deposits_sub),
        Some(resync_controller),
        shutdown_for_main,
        shutdown_for_deposits,
    );

    wait_for_shutdown().await;
    tracing::info!("kardamom-sequencer: shutdown signal received");
    shutdown.signal();
    match join_main.await {
        Ok(Ok(())) => tracing::info!("sequencer main loop returned cleanly"),
        Ok(Err(e)) => tracing::error!(error = %e, "sequencer main loop returned an error"),
        Err(e) => tracing::error!(error = %e, "sequencer task panicked"),
    }
    match join_deposits.await {
        Ok(Ok(())) => tracing::info!("sequencer deposit pump returned cleanly"),
        Ok(Err(e)) => tracing::error!(error = %e, "sequencer deposit pump returned an error"),
        Err(e) => tracing::error!(error = %e, "sequencer deposit task panicked"),
    }
    // Drop the cluster session only after both loops have stopped. This also
    // closes the egress channel, unblocking the watermark thread; the
    // receipts thread exits on the shutdown flag (or the closed floor
    // channel once the main loop is gone).
    drop(cluster_guard);
    if let Err(e) = watermark_thread.join() {
        tracing::warn!(?e, "egress-watermark thread panicked");
    }
    if let Err(e) = receipts_thread.join() {
        tracing::warn!(?e, "receipts-floors thread panicked");
    }
    drop(rt);
    Ok(())
}

/// Open the tx_receipts subscription: MDS fan-in (attach each executor
/// replica's endpoint) when configured, else the shared (multicast) channel —
/// the cluster deploy's shape. Mirrors the validator's helper. NOTE: in MDS
/// mode each destination BINDS its UDP socket, so two sequencer replicas on
/// one host (seq-a + seq-b) would collide — MDS receipts + co-located
/// replicas needs per-group endpoint bases before it can be enabled.
fn open_tx_receipts(
    rt: &AeronRuntime,
    channels: &ChannelsConfig,
    executor_count: u32,
) -> Result<kardamom_log::aeron_live::TxReceiptsSubscriberHandle> {
    use kardamom_log::aeron_live::TxReceiptsSubscriberHandle;
    if channels.tx_receipts_mds_enabled() {
        let sub =
            TxReceiptsSubscriberHandle::open_mds(rt, channels).context("open tx_receipts (MDS)")?;
        for i in 0..executor_count {
            if let Some(uri) = channels.tx_receipts_endpoint(i) {
                sub.add_destination(&uri)
                    .with_context(|| format!("attach tx_receipts endpoint {i}"))?;
            }
        }
        Ok(sub)
    } else {
        TxReceiptsSubscriberHandle::open(rt, channels).context("open tx_receipts")
    }
}

type LoopHandle = tokio::task::JoinHandle<Result<(), SequencerError>>;

/// Spawn the main sequencer loop + the deposit pump over a pair of
/// `TxOrderingRefPublisher`s (`main_pub` for the canonical `TxRef` loop,
/// `deposit_pub` for the `DepositRef` pump). Generic over the publisher type so
/// the Aeron and cluster branches share one implementation; both supply
/// concrete publishers that impl the trait.
#[allow(clippy::too_many_arguments)]
fn spawn_publish_loops<P>(
    cfg: SequencerConfig,
    mut tx_data: LiveTxDataSub,
    main_pub: P,
    deposit_pub: P,
    mut tx_errors: LiveTxErrorPub,
    mut deposit_sub: LiveDepositSub,
    resync: Option<kardamom_sequencer::resync::ResyncController>,
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

    // Independent pump for tx_deposits → DepositRef on tx_ordering. The
    // deposit path is not nonce-gated; it's a simple poll → publish loop
    // that runs alongside the canonical TxData → TxRef path.
    let mut deposit_pub = deposit_pub;
    let join_deposits = tokio::task::spawn_blocking(move || -> Result<(), SequencerError> {
        let mut backoff_us = 1u64;
        loop {
            if shutdown_for_deposits.is_signaled() {
                return Ok(());
            }
            match process_deposit(&mut deposit_sub, &mut deposit_pub) {
                Ok(true) => backoff_us = 1,
                Ok(false) => {
                    std::thread::sleep(Duration::from_micros(backoff_us));
                    backoff_us = backoff_us.saturating_mul(2).min(100);
                }
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

// ---------------------------------------------------------------------------
// Adapters: log::aeron_live handles → sequencer trait surface.
// ---------------------------------------------------------------------------

struct LiveTxDataSub {
    handle: TxDataSubscriberHandle,
}

impl LiveTxDataSub {
    fn new(handle: TxDataSubscriberHandle) -> Self {
        Self { handle }
    }
}

impl TxDataSubscriber for LiveTxDataSub {
    fn poll(&mut self) -> Result<Option<(TxDataLoc, TxEnvelope)>, SequencerError> {
        // try_recv is non-blocking. The Sequencer's run loop handles
        // backoff when poll returns None.
        Ok(self.handle.try_recv())
    }
}

struct LiveDepositSub {
    handle: TxDepositsSubscriberHandle,
}

impl LiveDepositSub {
    fn new(handle: TxDepositsSubscriberHandle) -> Self {
        Self { handle }
    }
}

impl DepositSubscriber for LiveDepositSub {
    fn poll(&mut self) -> Result<Option<(BPosition, Deposit)>, SequencerError> {
        Ok(self.handle.try_recv())
    }
}

/// Live `TxErrorPublisher` wrapping a `TxErrorsPublisherHandle`. Sequencer-
/// emitted rejections (duplicate / past-nonce today) are published on the
/// `tx_errors` Aeron channel; ingress consumes them to release parked
/// clients early. Publish failures are logged and dropped — the canonical
/// state has already advanced (or the tx was rejected), so there is nothing
/// to roll back.
struct LiveTxErrorPub {
    handle: TxErrorsPublisherHandle,
}

impl LiveTxErrorPub {
    fn new(handle: TxErrorsPublisherHandle) -> Self {
        Self { handle }
    }
}

impl TxErrorPublisher for LiveTxErrorPub {
    fn publish_error(&mut self, e: TxError) {
        if let Err(err) = self.handle.publish(&e) {
            tracing::warn!(error = %err, "tx_errors publish failed (dropped)");
        }
    }
}

fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
}

async fn wait_for_shutdown() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut sigterm = match signal(SignalKind::terminate()) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(error = %e, "failed to install SIGTERM handler; falling back to Ctrl-C only");
                let _ = tokio::signal::ctrl_c().await;
                return;
            }
        };
        tokio::select! {
            _ = sigterm.recv() => tracing::info!("SIGTERM received"),
            _ = tokio::signal::ctrl_c() => tracing::info!("Ctrl-C received"),
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
        tracing::info!("Ctrl-C received");
    }
}
