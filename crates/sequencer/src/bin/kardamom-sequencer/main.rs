//! `kardamom-sequencer`: per-partition sequencer process.
//!
//! Parses a TOML [`SequencerConfig`], opens its shard's tx_data subscriber +
//! the Aeron Cluster (Raft) ref publisher (tx_ordering) + a tx_errors publisher
//! for rejection signals, and runs the sequencer main loop on a dedicated
//! blocking thread until SIGTERM / Ctrl-C.
//!
//! The lag-detection / receipt-floor feed threads live in [`feeds`]; the
//! aeron_live-handle → sequencer-trait adapters in [`adapters`].

mod adapters;
mod feeds;

use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use kardamom_log::aeron_live::{
    AeronRuntime, TxDataSubscriberHandle, TxDepositsSubscriberHandle, TxErrorsPublisherHandle,
    TxReceiptsSubscriberHandle,
};
use kardamom_log::config::{ChannelsConfig, LogConfig};
use kardamom_obs::bin::wait_for_shutdown;
use kardamom_sequencer::config::SequencerConfig;
use kardamom_sequencer::sequencer::Shutdown;

use adapters::{LiveEpochSub, LiveTxDataSub, LiveTxErrorPub};

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

/// Fold the CLI/env overrides into the TOML-loaded config: partition
/// index/count, the replica-group shard rotation, sequencer id fallback,
/// core pin, per-node cluster egress endpoint, and the resync contract
/// knobs.
fn apply_cli_overrides(args: &Args, cfg: &mut SequencerConfig) -> Result<()> {
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
    Ok(())
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> anyhow::Result<()> {
    kardamom_obs::bin::init_tracing();
    let args = Args::parse();
    kardamom_obs::init_service!("sequencer", args.metrics_addr, &args.host_id).await?;
    let raw = std::fs::read_to_string(&args.config).context("read config")?;
    let mut cfg: SequencerConfig = toml::from_str(&raw).context("parse config")?;
    apply_cli_overrides(&args, &mut cfg)?;
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
    let rt = AeronRuntime::spawn(args.aeron_dir.as_deref()).context("spawn AeronRuntime")?;

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
    let cluster_rt =
        AeronRuntime::spawn(args.aeron_dir.as_deref()).context("spawn cluster AeronRuntime")?;
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
    let (resync_controller, floor_tx, reject_tx, watermark) =
        kardamom_sequencer::resync::resync_channel(cfg.resync.clone(), cfg.partition_index);

    let watermark_task = feeds::spawn_egress_watermark_feed(
        cluster_egress,
        cfg.resync.boundary_silence_ms,
        cfg.partition_index,
        watermark,
        reject_tx,
        shutdown.clone(),
    );

    // DEDICATED receipts runtime: receipt decode runs at full line rate, and
    // the main `rt`'s polling thread must stay dedicated to the tx_data
    // subscription (same isolation rationale as `cluster_rt` above; sharing
    // was observed to collapse the sequencer's sustainable ingest rate).
    let receipts_rt =
        AeronRuntime::spawn(args.aeron_dir.as_deref()).context("spawn receipts AeronRuntime")?;
    // NOTE: in MDS mode each attached destination BINDS its UDP socket, so two
    // sequencer replicas on one host (seq-a + seq-b) would collide — MDS
    // receipts + co-located replicas needs per-group endpoint bases before it
    // can be enabled here. The cluster deploy rides the shared multicast
    // channel instead.
    let receipts_sub = TxReceiptsSubscriberHandle::open_auto(
        &receipts_rt,
        &channels,
        args.executor_count
            .unwrap_or(channels.tx_receipts_executor_count),
    )
    .context("open tx_receipts")?;
    let receipts_task = feeds::spawn_receipt_floor_feed(
        receipts_sub,
        shutdown.clone(),
        cfg.partition_count,
        cfg.partition_index,
        floor_tx,
    );

    // Clone shares the single session thread; offers serialise through it. Both
    // loops use `cluster_pub` (impl `TxOrderingRefPublisher`) — the canonical
    // `TxRef` loop and the `DepositRef` pump.
    let (join_main, join_deposits) = feeds::spawn_publish_loops(
        cfg_clone,
        LiveTxDataSub::new(tx_data_sub),
        cluster_pub.clone(),
        cluster_pub,
        LiveTxErrorPub::new(tx_errors_pub),
        LiveEpochSub::new(tx_deposits_sub),
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
        Ok(Ok(())) => tracing::info!("sequencer epoch pump returned cleanly"),
        Ok(Err(e)) => tracing::error!(error = %e, "sequencer epoch pump returned an error"),
        Err(e) => tracing::error!(error = %e, "sequencer epoch task panicked"),
    }
    // Drop the cluster session only after both loops have stopped. This also
    // closes the egress channel, unblocking the watermark feed (which also
    // checks the shutdown token every tick); the receipts task exits on the
    // token (or the closed floor channel once the main loop is gone).
    drop(cluster_guard);
    if let Err(e) = watermark_task.await {
        tracing::warn!(?e, "egress-watermark task panicked");
    }
    if let Err(e) = receipts_task.await {
        tracing::warn!(?e, "receipts-floors task panicked");
    }
    drop(rt);
    Ok(())
}
