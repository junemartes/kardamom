//! kardamom-deploy: stateless CLI for deploying and upgrading kardamom L1 contracts.

use std::str::FromStr;

use alloy_primitives::{Address, Bytes};
use alloy_provider::ProviderBuilder;
use alloy_signer_local::PrivateKeySigner;
use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};

use kardamom_deployer::{
    ContractId, Deployer, FactoryStatus, Op, RegistryEntry, VerifyMismatch, encode_address_arg,
};

// ---------------------------------------------------------------------------
// CLI definition
// ---------------------------------------------------------------------------

/// Stateless deployer for kardamom L1 contracts.
#[derive(Debug, Parser)]
#[command(name = "kardamom-deploy", version)]
struct Cli {
    /// JSON-RPC endpoint.
    #[arg(long, default_value = "http://127.0.0.1:8545", global = true)]
    rpc_url: String,

    /// Hex private key or "env:VAR_NAME" to read from an environment variable.
    /// Required for write commands (ensure-factory, deploy, upgrade).
    #[arg(long, global = true)]
    private_key: Option<String>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Bootstrap the factory if absent (idempotent).
    EnsureFactory,

    /// Deploy one or more contracts in one tx. Implies ensure-factory.
    Deploy {
        /// Contract IDs to deploy (e.g. ETHLockbox, eth-lockbox).
        #[arg(required = true)]
        ids: Vec<String>,

        /// L2 minter address for ETHLockbox initialize(address).
        #[arg(long)]
        l2_minter: Option<Address>,
    },

    /// Upgrade one or more contracts to the next version (one tx).
    Upgrade {
        /// Contract IDs to upgrade (e.g. ETHLockbox, eth-lockbox).
        #[arg(required = true)]
        ids: Vec<String>,
    },

    /// Print all registered ids and their proxy/impl/version (read-only).
    Addresses,

    /// Cross-check registry against ERC1967 impl slots (read-only).
    Verify,
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();

    match cli.command {
        Command::EnsureFactory => {
            let key = require_key(cli.private_key, "ensure-factory")?;
            run_ensure_factory(cli.rpc_url, key).await
        }
        Command::Deploy { ids, l2_minter } => {
            let key = require_key(cli.private_key, "deploy")?;
            let l2_minter = l2_minter.context("--l2-minter is required for deploy ETHLockbox")?;
            let contract_ids = parse_ids(&ids)?;
            run_deploy(cli.rpc_url, key, contract_ids, l2_minter).await
        }
        Command::Upgrade { ids } => {
            let key = require_key(cli.private_key, "upgrade")?;
            let contract_ids = parse_ids(&ids)?;
            run_upgrade(cli.rpc_url, key, contract_ids).await
        }
        Command::Addresses => run_addresses(cli.rpc_url).await,
        Command::Verify => run_verify(cli.rpc_url).await,
    }
}

// ---------------------------------------------------------------------------
// Subcommand implementations
// ---------------------------------------------------------------------------

async fn run_ensure_factory(rpc_url: String, private_key: String) -> Result<()> {
    let signer = parse_key(&private_key)?;
    let operator = signer.address();
    let provider = ProviderBuilder::new()
        .wallet(signer)
        .connect_http(rpc_url.parse()?);
    let deployer = Deployer::new(provider, operator);
    let factory_addr = deployer.factory_address().await?;
    match deployer.ensure_factory().await? {
        FactoryStatus::AlreadyDeployed => {
            println!("factory already deployed at {factory_addr}");
        }
        FactoryStatus::Deployed => {
            println!("factory deployed at {factory_addr}");
        }
    }
    Ok(())
}

async fn run_deploy(
    rpc_url: String,
    private_key: String,
    ids: Vec<ContractId>,
    l2_minter: Address,
) -> Result<()> {
    let signer = parse_key(&private_key)?;
    let operator = signer.address();
    let provider = ProviderBuilder::new()
        .wallet(signer)
        .connect_http(rpc_url.parse()?);
    let deployer = Deployer::new(provider, operator);

    // ensure-factory is implied
    deployer.ensure_factory().await?;

    let ops: Vec<Op> = ids
        .into_iter()
        .map(|id| Op::Deploy {
            id,
            init_args: encode_address_arg(l2_minter),
        })
        .collect();

    let tx = deployer.apply(&ops).await?;
    println!("deployed in tx {tx}");

    let entries = deployer.addresses().await?;
    for e in &entries {
        print_entry(e);
    }
    Ok(())
}

async fn run_upgrade(rpc_url: String, private_key: String, ids: Vec<ContractId>) -> Result<()> {
    let signer = parse_key(&private_key)?;
    let operator = signer.address();
    let provider = ProviderBuilder::new()
        .wallet(signer)
        .connect_http(rpc_url.parse()?);
    let deployer = Deployer::new(provider, operator);

    // For upgrades, we need to know the new version. We read the current version
    // from the registry and increment by 1.
    let current_entries = deployer.addresses().await?;

    let ops: Vec<Op> = ids
        .into_iter()
        .map(|id| {
            // Find current version for this id; if not found, assume new_version = 2.
            let new_version = current_entries
                .iter()
                .find(|e| e.id == id.id())
                .map(|e| e.version + 1)
                .unwrap_or(2);
            Op::Upgrade {
                id,
                new_version,
                init_args: Bytes::new(),
            }
        })
        .collect();

    let tx = deployer.apply(&ops).await?;
    println!("upgraded in tx {tx}");

    let entries = deployer.addresses().await?;
    for e in &entries {
        print_entry(e);
    }
    Ok(())
}

async fn run_addresses(rpc_url: String) -> Result<()> {
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
    let deployer = Deployer::new(provider, Address::ZERO);
    let entries = deployer.addresses().await?;
    for e in &entries {
        print_entry(e);
    }
    Ok(())
}

async fn run_verify(rpc_url: String) -> Result<()> {
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
    let deployer = Deployer::new(provider, Address::ZERO);
    let report = deployer.verify().await?;

    for e in &report.entries {
        print_entry(e);
    }

    if report.mismatches.is_empty() {
        println!("all entries match ERC1967 impl slot");
    } else {
        for m in &report.mismatches {
            print_mismatch(m);
        }
        bail!("verify: {} mismatch(es) found", report.mismatches.len());
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Require that --private-key was supplied for write commands.
fn require_key(key: Option<String>, cmd: &str) -> Result<String> {
    key.with_context(|| format!("--private-key is required for `{cmd}`"))
}

/// Parse a private key from a hex literal or "env:VAR_NAME".
fn parse_key(key: &str) -> Result<PrivateKeySigner> {
    let hex = if let Some(var_name) = key.strip_prefix("env:") {
        std::env::var(var_name).with_context(|| format!("env var `{var_name}` not set"))?
    } else {
        key.to_string()
    };
    // Strip optional 0x prefix.
    let hex = hex.strip_prefix("0x").unwrap_or(&hex);
    PrivateKeySigner::from_str(hex).context("invalid private key")
}

/// Parse a list of contract ID strings (case-insensitive).
fn parse_ids(ids: &[String]) -> Result<Vec<ContractId>> {
    ids.iter().map(|s| parse_contract_id(s)).collect()
}

/// Parse a single contract ID string (case-insensitive, hyphen-tolerant).
fn parse_contract_id(s: &str) -> Result<ContractId> {
    match s.to_lowercase().replace('-', "").as_str() {
        "ethlockbox" => Ok(ContractId::EthLockbox),
        other => bail!(
            "unknown contract id `{other}`; valid values: ETHLockbox, eth-lockbox, ethLockbox"
        ),
    }
}

/// Print a registry entry in the canonical output format.
fn print_entry(e: &RegistryEntry) {
    println!("id          {}", e.id);
    println!("  proxy     {}", e.proxy);
    println!("  impl      {}", e.current_impl);
    println!("  version   {}", e.version);
    println!("  deployed  block {}", e.deployed_at);
    println!("  upgraded  block {}", e.upgraded_at);
}

/// Print a verify mismatch in the canonical output format.
fn print_mismatch(m: &VerifyMismatch) {
    println!(
        "MISMATCH id={} proxy={} registry_impl={} erc1967_impl={}",
        m.id, m.proxy, m.registry_impl, m.erc1967_impl
    );
}
