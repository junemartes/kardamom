//! kardamom-l1-indexer: the L1 follower. It reads the finalized L1 once
//! per finality step, archives the inbox, and publishes one record per
//! finalized block on the `l1_blocks` Aeron stream.
//!
//! One process runs the follower and the API. `--l1-rpc` lists the L1
//! endpoints: a block, a log query, or a hash reaches the archive and the
//! stream only when two endpoints agree on it, or when the light client
//! (`--l1-light-client-rpc`) serves it, verified against the beacon
//! chain's sync committee; the last header of each finality step must be
//! the light client's. The payloads come from the EigenDA proxy, which
//! checks them against their certificates.

use std::net::SocketAddr;
use std::num::NonZeroU64;
use std::path::PathBuf;
use std::time::Duration;

use alloy_primitives::Address;
use anyhow::Context;
use clap::Parser;
use kardamom_batcher::da::DaProxy;
use kardamom_da_watcher::L1Endpoints;
use kardamom_l1_indexer::api::Api;
use kardamom_l1_indexer::follow::{FollowConfig, Follower, FollowerParts};
use kardamom_l1_indexer::schedule::FinalitySchedule;
use kardamom_l1_indexer::store::Store;
use kardamom_log::aeron_live::{
    AeronRuntime, L1BlocksPublisherHandle, ServiceEventsPublisherHandle,
};
use kardamom_log::config::LogConfig;
use kardamom_log::discovery::{OwnRecording, StreamPlane, Topic};
use kardamom_obs::HostId;
use kardamom_obs::halt::{Halt, HaltCause};

#[derive(Debug, Parser)]
#[command(
    name = "kardamom-l1-indexer",
    version,
    about = "the L1 follower: archives the posted batches, their payloads, and the epoch inputs of the finalized L1, and publishes each finalized block on l1_blocks"
)]
struct Args {
    /// L1 JSON-RPC HTTP endpoints: repeat the flag, or separate the
    /// endpoints with commas. With two or more, a read is accepted when
    /// two agree; a source that fails or lies rotates out for a backoff.
    /// A deployment passes the list in the environment, so a keyed URL
    /// stays out of the process arguments.
    #[arg(long, env = "KARDAMOM_L1_RPC", hide_env_values = true, value_delimiter = ',', num_args = 1..)]
    l1_rpc: Vec<String>,
    /// The L1 light client's endpoint. Its answer settles a read when it
    /// serves the block; a public endpoint that disagrees with it is the
    /// liar. The last header of each finality step must be its header.
    #[arg(long)]
    l1_light_client_rpc: Option<String>,
    /// A beacon API endpoint. The follower reads the chain's genesis time
    /// and slot length from it once, and then reads L1 on the finality
    /// schedule. Without it, the follower reads every poll interval.
    #[arg(long, env = "KARDAMOM_BEACON_API", hide_env_values = true)]
    beacon_api: Option<String>,
    /// The EigenDA proxy (`http://host:port`), for the payloads.
    #[arg(long, env = "KARDAMOM_DA_PROXY")]
    da_proxy: String,
    /// L1 address of `KardamomL2Settlement`.
    #[arg(long)]
    settlement: Address,
    /// L1 address of the `ETHLockbox` proxy.
    #[arg(long)]
    lockbox: Address,
    /// The first L1 block to index on an empty archive: the block of the
    /// contract deploy. Without it, the finalized block at first start.
    #[arg(long)]
    start_block: Option<u64>,
    /// The archive directory.
    #[arg(long)]
    data_dir: PathBuf,
    /// The API listen address.
    #[arg(long, default_value = "0.0.0.0:8549")]
    listen: SocketAddr,
    /// One slot, in seconds: the read cadence while the finalized tip does
    /// not move or the follower is halted, and the whole cadence without
    /// `--beacon-api`.
    #[arg(long, default_value = "12")]
    poll_interval_secs: NonZeroU64,
    /// The most blocks one tick indexes, and one header batch holds.
    #[arg(long, default_value = "64")]
    blocks_per_tick: NonZeroU64,
    /// The most blocks one log query spans. A provider caps it: 10 on
    /// Alchemy's free plan.
    #[arg(long, default_value = "10")]
    max_log_range: NonZeroU64,
    /// Optional `LogConfig` TOML: the Aeron channels and discovery. Unset,
    /// the built-in single-host IPC defaults apply.
    #[arg(long, env = "KARDAMOM_LOG_CONFIG")]
    log_config: Option<PathBuf>,
    /// Aeron Media Driver directory (`aeron.dir`).
    #[arg(long)]
    aeron_dir: Option<PathBuf>,
    /// Record the `l1_blocks` stream on this node's archive, and publish
    /// nothing before the recording is live. A consumer replays the
    /// stream from the archive. It needs `[discovery]` in the log config.
    #[arg(long, env = "KARDAMOM_ARCHIVE_DURABILITY", default_value_t = false)]
    archive_durability: bool,
    /// Prometheus exporter listen address.
    #[arg(long, env = "KARDAMOM_METRICS_ADDR", default_value = "127.0.0.1:9549")]
    metrics_addr: SocketAddr,
    /// Host label on every metric.
    #[arg(long, env = "KARDAMOM_HOST_ID", default_value = "local")]
    host_id: HostId,
}

impl Args {
    /// The finality schedule, read from the beacon API. A beacon API that
    /// does not answer holds the follower in the `l1_unreachable` halt and
    /// is asked again every slot.
    async fn schedule(&self) -> Option<FinalitySchedule> {
        let url = self.beacon_api.as_deref()?;
        let first = FinalitySchedule::from_beacon(url).await;
        let schedule = match first {
            Ok(schedule) => schedule,
            Err(e) => {
                kardamom_obs::halt::hold_until(
                    Halt::new(HaltCause::L1Unreachable, e.to_string()),
                    Duration::from_secs(self.poll_interval_secs.get()),
                    || FinalitySchedule::from_beacon(url),
                )
                .await
            }
        };
        tracing::info!(?schedule, "reading L1 on the finality schedule");
        Some(schedule)
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    kardamom_obs::bin::init_tracing();
    let args = Args::parse();
    // Ready while now is before the planned wake time plus one slot, with
    // ten seconds of slack for the tick's own L1 and DA round trips.
    let readiness = kardamom_obs::Readiness::up().fresh(
        kardamom_l1_indexer::metrics::NEXT_WAKE,
        Duration::from_secs(args.poll_interval_secs.get().saturating_add(10)),
    );
    kardamom_obs::init_service!(
        "l1-indexer",
        args.metrics_addr,
        args.host_id.as_ref(),
        readiness
    )
    .await
    .context("init prometheus exporter")?;
    kardamom_l1_indexer::metrics::describe();
    let log_cfg = LogConfig::resolve(args.log_config.as_deref()).context("resolve log config")?;
    serve(args, log_cfg).await
}

/// Open the stream, start the recording, then run the follower and the
/// API until a signal. The Aeron runtime and the recorder live as long
/// as this function.
async fn serve(args: Args, log_cfg: LogConfig) -> anyhow::Result<()> {
    let aeron_rt = AeronRuntime::spawn(args.aeron_dir.as_deref()).context("spawn AeronRuntime")?;
    let mut plane =
        StreamPlane::from_config(&log_cfg, "l1-indexer").context("build the stream plane")?;
    let sink = plane
        .publisher::<L1BlocksPublisherHandle>(&aeron_rt)
        .await
        .context("open the l1_blocks publication")?;
    plane
        .publisher::<ServiceEventsPublisherHandle>(&aeron_rt)
        .await
        .context("open events")?
        .spawn_process_beacon();
    let recorders = if args.archive_durability {
        let own = OwnRecording {
            topic: Topic::L1Blocks,
            aeron_dir: args.aeron_dir.clone(),
            aeron_cfg: log_cfg.aeron.clone(),
        };
        Some(plane.record_own(own).await?.context(
            "--archive-durability records l1_blocks through [discovery]; the log config has it off",
        )?)
    } else {
        None
    };

    let source = L1Endpoints {
        rpcs: args.l1_rpc.clone(),
        light_client: args.l1_light_client_rpc.clone(),
    }
    .connect()
    .await
    .context("connect the L1 sources")?;
    let da = DaProxy::new(&args.da_proxy).context("DA proxy client")?;
    let store = Store::open(&args.data_dir).context("open archive")?;
    let follower = Follower::open(FollowerParts {
        source,
        sink,
        da,
        store: store.clone(),
        cfg: FollowConfig {
            settlement: args.settlement,
            lockbox: args.lockbox,
            start_block: args.start_block,
            poll_interval: Duration::from_secs(args.poll_interval_secs.get()),
            blocks_per_tick: args.blocks_per_tick,
            max_log_range: args.max_log_range,
            schedule: args.schedule().await,
        },
    })
    .context("open follower")?;
    let (local, handle) = Api::new(store)
        .serve(args.listen)
        .await
        .context("serve API")?;
    tracing::info!(%local, cursor = ?follower.cursor(), "l1-indexer up");

    tokio::select! {
        () = follower.run() => {}
        r = tokio::signal::ctrl_c() => r.context("signal")?,
    }
    handle.stop().context("stop API")?;
    handle.stopped().await;
    if let Some(recorders) = recorders {
        let _ = tokio::task::spawn_blocking(move || recorders.join()).await;
    }
    plane.shutdown().await;
    Ok(())
}
