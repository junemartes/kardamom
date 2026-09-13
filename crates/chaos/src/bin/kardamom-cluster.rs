//! `kardamom-cluster`: the operator commands against the container
//! cluster that ran as shell scripts. Each reads the node contract the
//! last `tofu apply` wrote.

use std::time::Duration;

use clap::{Parser, Subcommand};
use kardamom_chaos::diagnostics::Diagnostics;
use kardamom_chaos::harness::INGRESS_RPC_PORT;
use kardamom_chaos::rpc::Rpc;
use kardamom_chaos::scale::Resize;
use kardamom_chaos::stages::GATE_ACCOUNT;
use kardamom_chaos::{Knobs, Lifecycle};

const SMOKE_BUDGET: Duration = Duration::from_secs(60);

#[derive(Debug, Parser)]
#[command(
    name = "kardamom-cluster",
    about = "operator commands against the container cluster"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// One signed transfer through the ingress, with a receipt poll.
    Smoke {
        /// The ingress JSON-RPC URL; the default is ingress-0 of the
        /// node contract.
        #[arg(long)]
        rpc: Option<String>,
        /// The funded account that sends.
        #[arg(long, default_value_t = GATE_ACCOUNT)]
        account: u32,
    },
    /// Dump the failure evidence of the cluster that is up.
    Diagnostics,
    /// Resize the sequencer lanes with the shadow-first overlap rollout.
    ScaleSequencers {
        /// The target lane count, 1 to 8.
        lanes: u32,
        /// Render and log; submit nothing.
        #[arg(long)]
        dry_run: bool,
    },
}

impl Command {
    async fn run(self, lifecycle: &Lifecycle) -> anyhow::Result<()> {
        match self {
            Self::Smoke { rpc, account } => smoke(lifecycle, rpc, account).await,
            Self::Diagnostics => {
                Diagnostics::new(lifecycle.contract()?)?.dump().await;
                Ok(())
            }
            Self::ScaleSequencers { lanes, dry_run } => {
                Resize::new(
                    lifecycle.cluster_dir(),
                    &lifecycle.contract()?,
                    lanes,
                    dry_run,
                )?
                .run()
                .await
            }
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    Cli::parse().command.run(&Lifecycle::in_workspace()).await
}

async fn smoke(lifecycle: &Lifecycle, rpc: Option<String>, account: u32) -> anyhow::Result<()> {
    let url = match rpc {
        Some(url) => url,
        None => ingress_url(lifecycle)?,
    };
    let chain_id = Knobs::from_env()?.chain_id;
    Rpc::new(&url, chain_id)?
        .transfer_smoke(account, SMOKE_BUDGET)
        .await
}

fn ingress_url(lifecycle: &Lifecycle) -> anyhow::Result<String> {
    let contract = lifecycle.contract()?;
    Ok(format!(
        "http://{}:{INGRESS_RPC_PORT}",
        contract.node("ingress-0")?.ip
    ))
}
