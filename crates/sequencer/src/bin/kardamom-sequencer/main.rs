//! `kardamom-sequencer`: per-partition sequencer process.
//!
//! Parses a TOML [`SequencerConfig`], opens its shard's tx_data subscriber,
//! the Aeron Cluster (Raft) ref publisher (tx_ordering), and a tx_errors
//! publisher for rejection signals. Runs the sequencer main loop on a
//! dedicated blocking thread until SIGTERM or Ctrl-C.
//!
//! The lag-detection and receipt-floor feed threads live in [`feeds`].
//! The aeron_live-handle-to-sequencer-trait adapters live in [`adapters`].

mod adapters;
mod feeds;

use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use kardamom_log::aeron_live::{
    AeronRuntime, TxDataSubscriberHandle, TxDepositsSubscriberHandle, TxErrorsPublisherHandle,
    TxReceiptsSubscriberHandle, TxRemoteEpochsSubscriberHandle,
};
use kardamom_log::config::{ChannelsConfig, LogConfig};
use kardamom_obs::bin::wait_for_shutdown;
use kardamom_sequencer::config::SequencerConfig;
use kardamom_sequencer::sequencer::Shutdown;

use adapters::{LiveEpochSub, LiveRemoteEpochSub, LiveTxDataSub, LiveTxErrorPub};

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
    /// Optional `LogConfig` TOML that supplies the Aeron `[channels]`
    /// config. If unset, it uses built-in single-host IPC defaults (this
    /// keeps local and e2e behavior). Multi-host deployments point this at
    /// the rendered UDP channels config.
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
    /// Replica-group shard rotation. The effective partition becomes
    /// `(partition_index + partition_offset) % partition_count`, and
    /// `sequencer_id` follows it, unless explicitly overridden. This lets
    /// a second Nomad group of racing replicas reuse the same
    /// node-derived `--partition-index` while it serves a rotated shard.
    /// So the two replicas of any shard land on different nodes,
    /// deterministically. This is incompatible with an explicit
    /// `--sequencer-id`: the tx_data subscription and `TxRef.shard_id`
    /// must both follow the rotated shard.
    ///
    /// Racing replicas are safe by construction. Refs encode
    /// deterministically from the shared per-shard tx_data stream, and
    /// the Aeron Cluster dedups records by canonical_id first-seen. This
    /// is the same mechanism that already absorbs the M duplicate
    /// DepositRefs.
    #[arg(long, default_value_t = 0)]
    partition_offset: u32,
    /// Override the sequencer id embedded in every tx_ordering `TxRef`.
    /// If omitted and the TOML did not set it, falls back to
    /// `partition_index as u8`.
    #[arg(long)]
    sequencer_id: Option<u8>,
    /// The own tx_data lane (`lane`). Defaults to the sequencer id.
    #[arg(long, env = "KARDAMOM_LANE")]
    lane: Option<u8>,
    /// The vslots this replica serves (`vslots`), as ranges: `0-7,16`.
    /// Defaults to the identity map over the partition count.
    #[arg(long, env = "KARDAMOM_VSLOTS")]
    vslots: Option<String>,
    /// The old lanes to read during a resize (`extra_lanes`).
    #[arg(long, env = "KARDAMOM_EXTRA_LANES", value_delimiter = ',')]
    extra_lanes: Vec<u8>,
    /// The incoming vslots that start in shadow mode (`shadow_vslots`).
    #[arg(long, env = "KARDAMOM_SHADOW_VSLOTS")]
    shadow_vslots: Option<String>,
    /// The shadow warm-up in ms (`shadow_warm_ms`). Defaults to
    /// `tx_ttl_ms + 5000`.
    #[arg(long, env = "KARDAMOM_SHADOW_WARM_MS")]
    shadow_warm_ms: Option<u64>,
    /// The lifetime of a transaction that waits on a nonce gap, in ms
    /// (`tx_ttl_ms`). The deploy passes the same value to the ingress as
    /// `--pending-receipt-timeout-ms`.
    #[arg(long, env = "KARDAMOM_TX_TTL_MS")]
    tx_ttl_ms: Option<u64>,
    /// Override the CPU core to pin to.
    #[arg(long)]
    core_id: Option<usize>,
    /// This node's cluster-egress endpoint `ip:port` (cluster mode). Sets
    /// or overrides the [cluster] egress_channel as
    /// `aeron:udp?endpoint=<ip:port>`. The Nomad job injects this per node
    /// as ${meta.node_ip}:<cluster_egress_port>.
    #[arg(long, env = "KARDAMOM_CLUSTER_EGRESS_ENDPOINT")]
    cluster_egress_endpoint: Option<String>,
    /// Address for the Prometheus /metrics HTTP listener.
    #[arg(long, env = "KARDAMOM_METRICS_ADDR", default_value = "127.0.0.1:9001")]
    metrics_addr: std::net::SocketAddr,
    /// Host identifier; stamped on every metric.
    #[arg(long, env = "KARDAMOM_HOST_ID", default_value = "local")]
    host_id: String,
    /// The cluster's first-seen dedup window (`[resync] dedup_capacity`).
    /// Must equal the JVM's `-Dkardamom.cluster.dedupCapacity`: the lag
    /// horizon the resync mechanism protects. See
    /// docs/agents/sequencer-lag-resync-spec.md.
    #[arg(long, env = "KARDAMOM_CLUSTER_DEDUP_CAPACITY")]
    cluster_dedup_capacity: Option<u64>,
    /// Watermark-jump enter threshold as a percent of the dedup capacity
    /// (`[resync] enter_percent`).
    #[arg(long, env = "KARDAMOM_RESYNC_ENTER_PERCENT")]
    resync_enter_percent: Option<u64>,
    /// Boundary-silence resync trigger, ms (`[resync] boundary_silence_ms`).
    #[arg(long, env = "KARDAMOM_RESYNC_BOUNDARY_SILENCE_MS")]
    resync_boundary_silence_ms: Option<u64>,
    /// The executor nonce query endpoints, as a comma-separated list of
    /// `http://host:port` (`[lookup] executor_endpoints`). Empty means no
    /// lookup. See `docs/specs/dynamic-sequencer-sizing.md`, section 3.4.
    #[arg(long, env = "KARDAMOM_EXECUTOR_QUERY_ENDPOINTS", value_delimiter = ',')]
    executor_query_endpoints: Vec<String>,
    /// Executor replica count for the tx_receipts MDS fan-in (parity with
    /// the validator). Falls back to `channels.tx_receipts_executor_count`.
    /// Not relevant when receipts ride multicast (the cluster deploy).
    #[arg(long)]
    executor_count: Option<u32>,
}

/// Fold the CLI and env overrides into the TOML-loaded config:
/// partition index and count, the replica-group shard rotation,
/// sequencer id fallback, core pin, per-node cluster egress endpoint, and
/// the resync contract settings.
fn apply_cli_overrides(args: &Args, cfg: &mut SequencerConfig) -> Result<()> {
    if let Some(i) = args.partition_index {
        cfg.partition_index = i;
    }
    if let Some(m) = args.partition_count {
        cfg.partition_count = m;
    }
    if let Some(ttl) = args.tx_ttl_ms {
        cfg.tx_ttl_ms = ttl;
    }
    if !args.executor_query_endpoints.is_empty() {
        cfg.lookup.executor_endpoints = args.executor_query_endpoints.clone();
    }
    if let Some(lane) = args.lane {
        cfg.lane = Some(lane);
    }
    if let Some(text) = args.vslots.as_deref() {
        cfg.vslots = Some(text.parse().context("--vslots")?);
    }
    if !args.extra_lanes.is_empty() {
        cfg.extra_lanes = args.extra_lanes.clone();
    }
    if let Some(text) = args.shadow_vslots.as_deref() {
        cfg.shadow_vslots = text.parse().context("--shadow-vslots")?;
    }
    if let Some(warm) = args.shadow_warm_ms {
        cfg.shadow_warm_ms = Some(warm);
    }
    if args.partition_offset != 0 {
        // An explicit --sequencer-id combined with rotation would
        // subscribe to tx_data stream `sequencer_id`, while the
        // wrong-shard guard filters on the rotated `partition_index`. The
        // replica would silently drop every envelope, and its TxRefs
        // would stamp a shard_id its twin does not, breaking
        // byte-identical dedup.
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
    // Per-node cluster egress endpoint. The cluster client's
    // egress_channel is this node's reachable address (the node IP
    // differs per replica). So the Nomad job injects it, instead of
    // baking it into the static config template.
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
    // Contract line for the CI drift check: this must match the cluster
    // JVM's -Dkardamom.cluster.dedupCapacity (see cluster.nomad.hcl).
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
        tx_ttl_ms = cfg.tx_ttl_ms,
        lane = cfg.lane(),
        extra_lanes = ?cfg.extra_lanes,
        vslots = %cfg.vslot_set().context("vslots")?,
        shadow_vslots = %cfg.shadow_vslots,
        shadow_warm_ms = cfg.shadow_warm().as_millis() as u64,
        "kardamom-sequencer starting"
    );

    let channels: ChannelsConfig = LogConfig::resolve(args.log_config.as_deref())
        .context("resolve log config")?
        .channels;
    let rt = AeronRuntime::spawn(args.aeron_dir.as_deref()).context("spawn AeronRuntime")?;

    // One subscription per lane: the own lane, then the old lanes of a
    // resize. Every lane shares the multicast group; a lane is a stream
    // id. The shadow warm-up starts when the sequencer is constructed,
    // after these open.
    let tx_data_subs: Vec<(u8, TxDataSubscriberHandle)> = cfg
        .lanes()
        .into_iter()
        .map(|lane| {
            TxDataSubscriberHandle::open(&rt, &channels, lane)
                .with_context(|| format!("open TxDataSubscriberHandle lane={lane}"))
                .map(|h| (lane, h))
        })
        .collect::<Result<_, _>>()?;
    let tx_deposits_sub = TxDepositsSubscriberHandle::open(&rt, &channels)
        .context("open TxDepositsSubscriberHandle")?;
    let tx_remote_epochs_sub = TxRemoteEpochsSubscriberHandle::open(&rt, &channels)
        .context("open TxRemoteEpochsSubscriberHandle")?;
    let tx_errors_pub =
        TxErrorsPublisherHandle::open(&rt, &channels).context("open TxErrorsPublisherHandle")?;

    let shutdown = Shutdown::new();
    let shutdown_for_main = shutdown.clone();
    let shutdown_for_deposits = shutdown.clone();
    let shutdown_for_remote_epochs = shutdown.clone();

    if cfg.lookup.enabled() {
        tracing::info!(
            endpoints = ?cfg.lookup.executor_endpoints,
            timeout_ms = cfg.lookup.timeout_ms,
            max_in_flight = cfg.lookup.max_in_flight,
            "nonce floors: cold senders seed at 0; committed floors come from the \
             tx_receipts stream and from the executor nonce lookup, so a restarted \
             replica regains an established sender on its first park (F02.1 closed)"
        );
    } else {
        tracing::warn!(
            "nonce floors: no executor query endpoints; cold senders seed at 0 and \
             committed floors come from the tx_receipts stream only. A restarted \
             replica does NOT regain coverage of an established sender until a \
             receipt arrives (F02.1). Set --executor-query-endpoints."
        );
    }
    let cfg_clone = cfg.clone();

    // tx_ordering always publishes to the Aeron Cluster (Raft) ingress. The
    // cluster-session guard (`LiveCluster`) and its dedicated Aeron runtime
    // must outlive both publish loops. So bind the guard in the outer
    // scope; it is dropped only after the `join_*` awaits below.
    //
    // This is a dedicated cluster runtime (its own Aeron thread, same
    // aeron dir), so the cluster session never contends with the tx_data
    // subscription on the main `rt`.
    let cluster_rt =
        AeronRuntime::spawn(args.aeron_dir.as_deref()).context("spawn cluster AeronRuntime")?;
    let (cluster_guard, cluster_pub, cluster_egress) =
        kardamom_sequencer::outbound::cluster::cluster_ref_publisher_with_egress(
            cluster_rt,
            cfg.cluster.to_live(),
        )
        .context("connect cluster ref publisher")?;
    tracing::info!("kardamom-sequencer: tx_ordering via Aeron Cluster");

    // Lag detection and receipt-floor resync (see the sequencer-lag-resync
    // spec). Three feeds go into the publish loop's ResyncController:
    //  1. The egress-watermark thread. The cluster broadcasts every
    //     boundary to this publisher session. Decode `end_tx_idx` (the
    //     global canonical count) into the shared watermark, and discard
    //     the records.
    //  2. The receipts thread: tx_receipts to per-sender executed-truth
    //     floors (only this shard's senders).
    //  3. The controller itself, handed to the Sequencer through
    //     enable_resync.
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

    // A dedicated receipts runtime. Receipt decode runs at full line
    // rate, and the main `rt`'s polling thread must stay dedicated to the
    // tx_data subscription (the same isolation reason as `cluster_rt`
    // above). Sharing was observed to collapse the sequencer's
    // sustainable ingest rate.
    let receipts_rt =
        AeronRuntime::spawn(args.aeron_dir.as_deref()).context("spawn receipts AeronRuntime")?;
    // Note: in MDS mode, each attached destination binds its UDP
    // socket, so two sequencer replicas on one host would collide. MDS
    // receipts with co-located replicas needs per-group endpoint bases
    // before this can be enabled here. The cluster deploy rides the
    // shared multicast channel instead.
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
        cfg.vslot_set().context("vslots")?,
        floor_tx.clone(),
    );

    // The nonce lookup task. It shares the floor channel with the receipts
    // feed: an executor's committed nonce is floor evidence of the same
    // kind as a receipt.
    let (lookup_requester, lookup_task) = if cfg.lookup.enabled() {
        let (requester, rx) = kardamom_sequencer::lookup::LookupRequester::channel();
        let task = feeds::spawn_nonce_lookup_feed(
            rx,
            cfg.lookup.clone(),
            cfg.partition_index,
            floor_tx,
            shutdown.clone(),
        );
        (Some(requester), Some(task))
    } else {
        drop(floor_tx);
        (None, None)
    };

    // Cloning shares the single session thread, and offers serialize
    // through it. All three loops use `cluster_pub` (it implements
    // `TxOrderingRefPublisher`): the canonical `TxRef` loop and the two
    // origin pumps.
    let (join_main, join_deposits, join_remote_epochs) = feeds::spawn_publish_loops(
        cfg_clone,
        LiveTxDataSub::new(tx_data_subs),
        cluster_pub.clone(),
        cluster_pub.clone(),
        cluster_pub,
        LiveTxErrorPub::new(tx_errors_pub),
        LiveEpochSub::new(tx_deposits_sub),
        LiveRemoteEpochSub::new(tx_remote_epochs_sub),
        Some(resync_controller),
        lookup_requester,
        shutdown_for_main,
        shutdown_for_deposits,
        shutdown_for_remote_epochs,
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
    match join_remote_epochs.await {
        Ok(Ok(())) => tracing::info!("sequencer remote-epoch pump returned cleanly"),
        Ok(Err(e)) => {
            tracing::error!(error = %e, "sequencer remote-epoch pump returned an error")
        }
        Err(e) => tracing::error!(error = %e, "sequencer remote-epoch task panicked"),
    }
    // Drop the cluster session only after every loop has stopped. This
    // also closes the egress channel, which unblocks the watermark feed.
    // The feed also checks the shutdown token on each tick. The receipts
    // task exits on the token, or on the closed floor channel after the
    // main loop ends.
    drop(cluster_guard);
    if let Err(e) = watermark_task.await {
        tracing::warn!(?e, "egress-watermark task panicked");
    }
    if let Err(e) = receipts_task.await {
        tracing::warn!(?e, "receipts-floors task panicked");
    }
    // The lookup task exits on the token, or on the closed request
    // channel after the main loop ends.
    if let Some(task) = lookup_task
        && let Err(e) = task.await
    {
        tracing::warn!(?e, "nonce-lookup task panicked");
    }
    drop(rt);
    Ok(())
}
