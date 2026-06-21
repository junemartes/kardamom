//! `cluster-e2e` — RPC-driven end-to-end client for the deployed kardamom
//! nomad cluster.
//!
//! Replaces the old single-host `multiprocess_e2e` test: instead of spawning
//! the `kardamom-*` binaries as subprocesses against a single Aeron container
//! over IPC, this drives the **real** cluster (multi-host UDP MDC/MDS, launched
//! by ansible/nomad/consul) from the outside over its ingress JSON-RPC + the
//! in-cluster anvil L1. Invoked by `deploy/cluster/scripts/ci-cluster.sh`.
//!
//! ```bash
//! cluster-e2e all \
//!   --rpc-url http://192.168.56.31:8545 \
//!   --l1-rpc  http://192.168.56.10:8546 \
//!   --lockbox 0x<ETHLockbox proxy>
//! ```

use std::time::Duration;

use alloy_primitives::{Address, address};
use anyhow::{Context, Result};
use clap::{Parser, Subcommand};

use e2e::cluster_client::{
    DEV_ACCOUNT_0_KEY, ingress_client, run_contract_deploy, run_deposit, run_transfer,
    signer_from_hex, wait_for_ingress,
};

/// Burn / sink address for value transfers (matches `scripts/smoke.sh`).
const SINK: Address = address!("000000000000000000000000000000000000dEaD");
/// L2 recipient of the deposit's inner call.
const L2_DEPOSIT_TARGET: Address = address!("4444444444444444444444444444444444444444");

#[derive(Debug, Parser)]
#[command(name = "cluster-e2e", version, about = "RPC-driven cluster e2e client")]
struct Cli {
    /// Ingress JSON-RPC endpoint.
    #[arg(long, default_value = "http://192.168.56.31:8545", global = true)]
    rpc_url: String,

    /// In-cluster anvil L1 JSON-RPC endpoint (deposit scenario only).
    #[arg(long, default_value = "http://192.168.56.10:8546", global = true)]
    l1_rpc: String,

    /// L2 chain id.
    #[arg(long, default_value_t = 412346, global = true)]
    chain_id: u64,

    /// ETHLockbox proxy address on the L1 (deposit scenario only).
    #[arg(long, env = "KARDAMOM_LOCKBOX", global = true)]
    lockbox: Option<Address>,

    /// Signer private key (hex). Defaults to anvil dev account #0.
    #[arg(long, default_value_t = DEV_ACCOUNT_0_KEY.to_string(), global = true)]
    l2_key: String,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Signed value transfers → assert each gets a success receipt.
    Transfer {
        /// Number of transfers to submit.
        #[arg(long, default_value_t = 3)]
        count: u64,
        /// First nonce (sequential per submission).
        #[arg(long, default_value_t = 0)]
        start_nonce: u64,
    },
    /// L1 → L2 deposit via anvil + da-watcher → assert the minted L2 receipt.
    Deposit,
    /// Deploy a tiny contract → assert success + non-zero contractAddress.
    ContractDeploy {
        /// Nonce for the creation tx.
        #[arg(long, default_value_t = 0)]
        nonce: u64,
    },
    /// transfer (×3) → deposit → contract-deploy, with non-colliding nonces.
    All,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let signer = signer_from_hex(&cli.l2_key)?;
    let ingress = ingress_client(&cli.rpc_url)?;

    eprintln!(
        "==> cluster-e2e: ingress {} (chain {})",
        cli.rpc_url, cli.chain_id
    );
    wait_for_ingress(&ingress, Duration::from_secs(30))
        .await
        .context("ingress JSON-RPC not ready")?;

    match cli.command {
        Command::Transfer { count, start_nonce } => {
            eprintln!("==> transfer x{count} from nonce {start_nonce}");
            run_transfer(&ingress, &signer, SINK, count, start_nonce, cli.chain_id).await?;
        }
        Command::Deposit => {
            let lockbox = cli
                .lockbox
                .context("--lockbox required for the deposit scenario")?;
            eprintln!("==> deposit via L1 {} lockbox {lockbox}", cli.l1_rpc);
            run_deposit(&ingress, &cli.l1_rpc, &signer, lockbox, L2_DEPOSIT_TARGET).await?;
        }
        Command::ContractDeploy { nonce } => {
            eprintln!("==> contract-deploy at nonce {nonce}");
            run_contract_deploy(&ingress, &signer, nonce, cli.chain_id).await?;
        }
        Command::All => {
            let lockbox = cli
                .lockbox
                .context("--lockbox required for the `all` scenario")?;
            eprintln!("==> all: transfer x3 → deposit → contract-deploy");
            // Account #0 L2 nonces: 0,1,2 (transfers) then 3 (deploy). The
            // deposit mints to a fresh L2 target and consumes no account-#0 nonce.
            run_transfer(&ingress, &signer, SINK, 3, 0, cli.chain_id).await?;
            run_deposit(&ingress, &cli.l1_rpc, &signer, lockbox, L2_DEPOSIT_TARGET).await?;
            run_contract_deploy(&ingress, &signer, 3, cli.chain_id).await?;
        }
    }

    eprintln!("RESULT: PASS");
    Ok(())
}
