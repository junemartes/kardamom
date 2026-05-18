use std::env;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;

use alloy_primitives::{Address, Bytes, U256, address, hex};
use clap::Parser;
use metrics_exporter_prometheus::PrometheusBuilder;
use tracing_flame::FlameLayer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer, fmt};

use kardamom_node::{Node, metrics as kmetrics, start_server};

mod chain;

/// `addr=wei` (decimal or `0x…` hex) entry consumed by `--prefund`.
#[derive(Debug, Clone)]
struct PrefundEntry {
    addr: Address,
    balance: U256,
}

impl FromStr for PrefundEntry {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> anyhow::Result<Self> {
        let (a, b) = s
            .split_once('=')
            .ok_or_else(|| anyhow::anyhow!("expected addr=wei, got `{s}`"))?;
        let addr: Address = a.parse().map_err(|e| anyhow::anyhow!("bad address: {e}"))?;
        let balance = if let Some(stripped) = b.strip_prefix("0x").or_else(|| b.strip_prefix("0X"))
        {
            U256::from_str_radix(stripped, 16)
                .map_err(|e| anyhow::anyhow!("bad hex balance: {e}"))?
        } else {
            U256::from_str_radix(b, 10).map_err(|e| anyhow::anyhow!("bad decimal balance: {e}"))?
        };
        Ok(Self { addr, balance })
    }
}

/// `addr=hex` entry consumed by `--insert-code`.
#[derive(Debug, Clone)]
struct CodeEntry {
    addr: Address,
    code: Bytes,
}

impl FromStr for CodeEntry {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> anyhow::Result<Self> {
        let (a, b) = s
            .split_once('=')
            .ok_or_else(|| anyhow::anyhow!("expected addr=hex, got `{s}`"))?;
        let addr: Address = a.parse().map_err(|e| anyhow::anyhow!("bad address: {e}"))?;
        let trimmed = b
            .strip_prefix("0x")
            .or_else(|| b.strip_prefix("0X"))
            .unwrap_or(b);
        let bytes = hex::decode(trimmed).map_err(|e| anyhow::anyhow!("bad hex code: {e}"))?;
        Ok(Self {
            addr,
            code: Bytes::from(bytes),
        })
    }
}

/// Kardamom — a small revm-backed L2 scaffold.
#[derive(Parser, Debug)]
#[command(
    name = "kardamom",
    about = "A fast, simple L2 rollup scaffold.",
    long_about = "Boots an in-memory revm-backed node and exposes a small \
Ethereum JSON-RPC surface.\n\n\
Dev account (prefunded with 1000 ETH):\n  \
  address: 0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266\n  \
  private key: 0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80\n\n\
(This is the well-known Anvil/Hardhat account #0. Do not use on real chains.)"
)]
struct Args {
    /// Address to bind the JSON-RPC server on.
    #[arg(long, default_value = "127.0.0.1:8545")]
    rpc_addr: SocketAddr,

    /// Address to bind the Prometheus `/metrics` endpoint on.
    #[arg(long, default_value = "127.0.0.1:9000")]
    metrics_addr: SocketAddr,

    /// Chain ID to advertise via eth_chainId.
    #[arg(long, default_value_t = 412346)]
    chain_id: u64,

    /// Prefund `addr=wei` (decimal or `0x…` hex). Repeatable.
    #[arg(long = "prefund", value_name = "ADDR=WEI", value_parser = clap::value_parser!(PrefundEntry))]
    prefund: Vec<PrefundEntry>,

    /// Install `addr=hex` bytecode at startup. Dev-only. Repeatable.
    #[arg(long = "insert-code", value_name = "ADDR=HEX", value_parser = clap::value_parser!(CodeEntry))]
    insert_code: Vec<CodeEntry>,
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

    // STOPGAP (replaced in Task 5): build an empty genesis from --chain-id only.
    let genesis = kardamom_node::Genesis {
        chain_id: args.chain_id,
        alloc: std::collections::BTreeMap::new(),
    };
    let node = Node::new(&genesis);

    tracing::info!(
        rpc = %args.rpc_addr,
        metrics = %args.metrics_addr,
        chain_id = args.chain_id,
        alloc_entries = genesis.alloc.len(),
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
