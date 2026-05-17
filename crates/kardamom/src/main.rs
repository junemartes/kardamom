use std::net::SocketAddr;

use alloy_primitives::{Address, U256, address};
use clap::Parser;
use tracing_subscriber::EnvFilter;

use kardamom_node::{Node, start_server};

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

    /// Chain ID to advertise via eth_chainId.
    #[arg(long, default_value_t = 412346)]
    chain_id: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::from_default_env().add_directive("kardamom=info".parse().unwrap()),
        )
        .init();

    let args = Args::parse();

    let dev_account: Address = address!("f39Fd6e51aad88F6F4ce6aB8827279cffFb92266");
    let dev_balance = U256::from(1_000u64) * U256::from(10u64).pow(U256::from(18u64));

    let node = Node::new(args.chain_id, &[(dev_account, dev_balance)]);

    tracing::info!(addr = %args.rpc_addr, chain_id = args.chain_id, "starting kardamom");

    let handle = start_server(node, args.rpc_addr).await?;

    tokio::signal::ctrl_c().await?;
    tracing::info!("shutting down");
    handle.stop()?;
    handle.stopped().await;
    Ok(())
}
