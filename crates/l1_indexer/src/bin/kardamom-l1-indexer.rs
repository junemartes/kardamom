//! kardamom-l1-indexer: follow the finalized L1 and archive the inbox.
//!
//! One process runs the follower and the API. `--l1-rpc` lists the L1
//! endpoints: a block, a log query, or a hash reaches the archive only
//! when two endpoints agree on it, or when the light client
//! (`--l1-light-client-rpc`) serves it, verified against the beacon
//! chain's sync committee. The payloads come from the EigenDA proxy,
//! which checks them against their certificates.

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
use kardamom_l1_indexer::store::Store;
use kardamom_obs::HostId;

#[derive(Debug, Parser)]
#[command(
    name = "kardamom-l1-indexer",
    version,
    about = "archives the posted batches, their payloads, and the epoch inputs of the finalized L1"
)]
struct Args {
    /// L1 JSON-RPC HTTP endpoints: repeat the flag, or separate the
    /// endpoints with commas. With two or more, a read is accepted when
    /// two agree; a source that fails or lies rotates out for a backoff.
    #[arg(long, value_delimiter = ',', num_args = 1..)]
    l1_rpc: Vec<String>,
    /// The L1 light client's endpoint. Its answer settles a read when it
    /// serves the block; a public endpoint that disagrees with it is the
    /// liar.
    #[arg(long)]
    l1_light_client_rpc: Option<String>,
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
    /// Polling cadence in seconds.
    #[arg(long, default_value = "12")]
    poll_interval_secs: NonZeroU64,
    /// The most blocks one tick indexes.
    #[arg(long, default_value = "64")]
    blocks_per_tick: NonZeroU64,
    /// Prometheus exporter listen address.
    #[arg(long, env = "KARDAMOM_METRICS_ADDR", default_value = "127.0.0.1:9549")]
    metrics_addr: SocketAddr,
    /// Host label on every metric.
    #[arg(long, env = "KARDAMOM_HOST_ID", default_value = "local")]
    host_id: HostId,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    kardamom_obs::bin::init_tracing();
    let args = Args::parse();
    kardamom_obs::init(
        "l1-indexer",
        args.metrics_addr,
        args.host_id.as_ref(),
        env!("CARGO_PKG_VERSION"),
        option_env!("KARDAMOM_GIT_SHA").unwrap_or("unknown"),
    )
    .await
    .context("init prometheus exporter")?;
    kardamom_l1_indexer::metrics::describe();

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
        da,
        store: store.clone(),
        cfg: FollowConfig {
            settlement: args.settlement,
            lockbox: args.lockbox,
            start_block: args.start_block,
            poll_interval: Duration::from_secs(args.poll_interval_secs.get()),
            blocks_per_tick: args.blocks_per_tick,
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
    Ok(())
}
