//! `kardamom-l1-fault-proxy`: an L1 JSON-RPC proxy that lies on command.
//! The chaos harness puts it in front of the in-cluster L1 and drives
//! its faults through `POST /fault`. See the library's documentation for
//! the faults and the control endpoint.

use std::net::SocketAddr;

use clap::Parser;
use kardamom_l1_fault_proxy::FaultProxy;

#[derive(Debug, Parser)]
#[command(
    name = "kardamom-l1-fault-proxy",
    version,
    about = "an L1 JSON-RPC proxy with faults a control endpoint sets at run time"
)]
struct Args {
    /// The L1 JSON-RPC endpoint every call is forwarded to.
    #[arg(long, env = "KARDAMOM_L1_UPSTREAM")]
    upstream: String,
    /// The listen address of the JSON-RPC pipe and the control endpoint.
    #[arg(
        long,
        env = "KARDAMOM_L1_FAULT_PROXY_LISTEN",
        default_value = "0.0.0.0:8547"
    )]
    listen: SocketAddr,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    kardamom_obs::bin::init_tracing();
    let args = Args::parse();
    let proxy = FaultProxy::spawn(&args.upstream, args.listen).await?;
    tracing::info!(
        listen = %proxy.addr(),
        upstream = %args.upstream,
        "kardamom-l1-fault-proxy: serving"
    );
    proxy.serve_forever().await;
    Ok(())
}
