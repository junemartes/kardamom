use std::env;
use std::net::SocketAddr;
use std::path::PathBuf;

use clap::Parser;
use metrics_exporter_prometheus::PrometheusBuilder;
use tracing_flame::FlameLayer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer, fmt};

use kardamom_node::{Node, metrics as kmetrics, start_server};

mod chain;

/// Kardamom — a small revm-backed L2 scaffold.
#[derive(Parser, Debug)]
#[command(
    name = "kardamom",
    about = "A fast, simple L2 rollup scaffold.",
    long_about = "Boots an in-memory revm-backed node and exposes a small \
Ethereum JSON-RPC surface.\n\n\
Pass a TOML genesis file via --chain. For a quick start, use the \
checked-in dev chain:\n  \
  kardamom --chain chains/dev.toml\n\n\
The dev chain prefunds the well-known Anvil/Hardhat account #0 with 1000 ETH. \
Do not use that account on real chains."
)]
struct Args {
    /// Path to the TOML genesis file (chain_id + alloc).
    #[arg(long = "chain", value_name = "PATH")]
    chain: PathBuf,

    /// Address to bind the JSON-RPC server on.
    #[arg(long, default_value = "127.0.0.1:8545")]
    rpc_addr: SocketAddr,

    /// Address to bind the Prometheus `/metrics` endpoint on.
    #[arg(long, default_value = "127.0.0.1:9000")]
    metrics_addr: SocketAddr,
}

fn init_tracing() -> Option<tracing_flame::FlushGuard<std::io::BufWriter<std::fs::File>>> {
    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("kardamom=info"));

    let fmt_layer: Box<dyn Layer<_> + Send + Sync> = fmt::layer().boxed();

    let mut layers: Vec<Box<dyn Layer<_> + Send + Sync>> = vec![fmt_layer];
    let mut guard = None;

    let flame_path = env::var("KARDAMOM_FLAME").ok().map(PathBuf::from);
    if let Some(path) = &flame_path {
        match FlameLayer::with_file(path) {
            Ok((flame, g)) => {
                layers.push(flame.boxed());
                guard = Some(g);
            }
            Err(e) => {
                eprintln!("failed to enable tracing-flame at {path:?}: {e}");
            }
        }
    }

    tracing_subscriber::registry()
        .with(env_filter)
        .with(layers)
        .init();

    if let Some(path) = flame_path {
        tracing::info!(flame = ?path, "tracing-flame layer enabled");
    }

    guard
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    PrometheusBuilder::new()
        .with_http_listener(args.metrics_addr)
        .set_buckets(kmetrics::DURATION_BUCKETS)?
        .install()?;

    let _flame_guard = init_tracing();

    metrics::gauge!(
        kmetrics::BUILD_INFO,
        "version" => env!("CARGO_PKG_VERSION"),
        "git_sha" => option_env!("GIT_SHA").unwrap_or("unknown"),
    )
    .set(1.0);

    let genesis = chain::load(&args.chain)?;
    let alloc_entries = genesis.alloc.len();
    let contracts = genesis.alloc.values().filter(|e| e.code.is_some()).count();
    let chain_id = genesis.chain_id;
    let node = Node::new(&genesis);

    tracing::info!(
        rpc = %args.rpc_addr,
        metrics = %args.metrics_addr,
        chain_id,
        alloc_entries,
        contracts,
        chain_file = %args.chain.display(),
        "starting kardamom"
    );

    let handle = start_server(node, args.rpc_addr).await?;

    tokio::signal::ctrl_c().await?;
    tracing::info!("shutting down");
    handle.stop()?;
    handle.stopped().await;

    drop(_flame_guard);
    Ok(())
}
