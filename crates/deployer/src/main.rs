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

#[derive(Debug, Parser)]
#[command(name = "kardamom-deploy", version)]
struct Cli {
    /// JSON-RPC endpoint.
    #[arg(long, default_value = "http://127.0.0.1:8545", global = true)]
    rpc_url: String,

    /// Hex private key or "env:VAR_NAME". Required for write commands.
    #[arg(long, global = true)]
    private_key: Option<String>,

    /// Canonical owner address (Safe or EOA). Same owner ⇒ same factory address.
    /// Required for any command that touches the factory.
    #[arg(long, global = true)]
    owner: Option<Address>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Bootstrap the factory if absent. Anyone can run this; the resulting factory
    /// is always owned by `--owner`.
    EnsureFactory,

    /// Deploy one or more contracts in one tx. Implies ensure-factory.
    /// `--l2-chain-id` and `--l2-minter` are positionally paired and repeat together.
    Deploy {
        /// Contract IDs to deploy (e.g. ETHLockbox). Same id repeats per L2.
        #[arg(required = true)]
        ids: Vec<String>,

        /// L2 chain IDs to target (one per id × L2 combination).
        #[arg(long = "l2-chain-id", required = true)]
        l2_chain_ids: Vec<u64>,

        /// L2 minter addresses for ETHLockbox initialize. Must match `--l2-chain-id` count.
        #[arg(long = "l2-minter")]
        l2_minters: Vec<Address>,
    },

    /// Upgrade contracts to the next version across one or more L2s in one tx.
    Upgrade {
        #[arg(required = true)]
        ids: Vec<String>,

        #[arg(long = "l2-chain-id", required = true)]
        l2_chain_ids: Vec<u64>,
    },

    /// Print registered ids and their proxy/impl/version. Optionally filter by L2.
    Addresses {
        #[arg(long = "l2-chain-id")]
        l2_chain_id: Option<u64>,
    },

    /// Cross-check registry against ERC1967 impl slots.
    Verify,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();

    match cli.command {
        Command::EnsureFactory => {
            let key = require_key(cli.private_key, "ensure-factory")?;
            let owner = require_owner(cli.owner, "ensure-factory")?;
            run_ensure_factory(cli.rpc_url, key, owner).await
        }
        Command::Deploy {
            ids,
            l2_chain_ids,
            l2_minters,
        } => {
            let key = require_key(cli.private_key, "deploy")?;
            let owner = require_owner(cli.owner, "deploy")?;
            if l2_chain_ids.len() != l2_minters.len() {
                bail!(
                    "--l2-chain-id ({}) and --l2-minter ({}) counts must match",
                    l2_chain_ids.len(),
                    l2_minters.len()
                );
            }
            let contract_ids = parse_ids(&ids)?;
            run_deploy(cli.rpc_url, key, owner, contract_ids, l2_chain_ids, l2_minters).await
        }
        Command::Upgrade { ids, l2_chain_ids } => {
            let key = require_key(cli.private_key, "upgrade")?;
            let owner = require_owner(cli.owner, "upgrade")?;
            let contract_ids = parse_ids(&ids)?;
            run_upgrade(cli.rpc_url, key, owner, contract_ids, l2_chain_ids).await
        }
        Command::Addresses { l2_chain_id } => {
            let owner = require_owner(cli.owner, "addresses")?;
            run_addresses(cli.rpc_url, owner, l2_chain_id).await
        }
        Command::Verify => {
            let owner = require_owner(cli.owner, "verify")?;
            run_verify(cli.rpc_url, owner).await
        }
    }
}

async fn run_ensure_factory(rpc_url: String, private_key: String, owner: Address) -> Result<()> {
    let signer = parse_key(&private_key)?;
    let operator = signer.address();
    let provider = ProviderBuilder::new()
        .wallet(signer)
        .connect_http(rpc_url.parse()?);
    let deployer = Deployer::new(provider, operator);
    let factory_addr = deployer.factory_address(owner).await?;
    match deployer.ensure_factory(owner).await? {
        FactoryStatus::AlreadyDeployed => {
            println!("factory already deployed at {factory_addr} (owner: {owner})");
        }
        FactoryStatus::Deployed => {
            println!("factory deployed at {factory_addr} (owner: {owner})");
        }
    }
    Ok(())
}

async fn run_deploy(
    rpc_url: String,
    private_key: String,
    owner: Address,
    ids: Vec<ContractId>,
    l2_chain_ids: Vec<u64>,
    l2_minters: Vec<Address>,
) -> Result<()> {
    let signer = parse_key(&private_key)?;
    let operator = signer.address();
    let provider = ProviderBuilder::new()
        .wallet(signer)
        .connect_http(rpc_url.parse()?);
    let deployer = Deployer::new(provider, operator);

    deployer.ensure_factory(owner).await?;

    // For each id × each (l2_chain_id, l2_minter) pair, emit one Op::Deploy.
    let mut ops: Vec<Op> = Vec::new();
    for id in &ids {
        for (chain_id, minter) in l2_chain_ids.iter().zip(l2_minters.iter()) {
            ops.push(Op::Deploy {
                l2_chain_id: *chain_id,
                id: *id,
                init_args: encode_address_arg(*minter),
            });
        }
    }

    let tx = deployer.apply(&ops, owner).await?;
    println!("deployed in tx {tx}");

    let entries = deployer.addresses(owner, None).await?;
    for e in &entries {
        print_entry(e);
    }
    Ok(())
}

async fn run_upgrade(
    rpc_url: String,
    private_key: String,
    owner: Address,
    ids: Vec<ContractId>,
    l2_chain_ids: Vec<u64>,
) -> Result<()> {
    let signer = parse_key(&private_key)?;
    let operator = signer.address();
    let provider = ProviderBuilder::new()
        .wallet(signer)
        .connect_http(rpc_url.parse()?);
    let deployer = Deployer::new(provider, operator);

    let current_entries = deployer.addresses(owner, None).await?;

    let mut ops: Vec<Op> = Vec::new();
    for id in &ids {
        for chain_id in &l2_chain_ids {
            let new_version = current_entries
                .iter()
                .find(|e| e.l2_chain_id == *chain_id && e.id == id.id())
                .map(|e| e.version + 1)
                .unwrap_or(2);
            ops.push(Op::Upgrade {
                l2_chain_id: *chain_id,
                id: *id,
                new_version,
                init_args: Bytes::new(),
            });
        }
    }

    let tx = deployer.apply(&ops, owner).await?;
    println!("upgraded in tx {tx}");

    let entries = deployer.addresses(owner, None).await?;
    for e in &entries {
        print_entry(e);
    }
    Ok(())
}

async fn run_addresses(rpc_url: String, owner: Address, l2_chain_id: Option<u64>) -> Result<()> {
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
    let deployer = Deployer::new(provider, Address::ZERO);
    let entries = deployer.addresses(owner, l2_chain_id).await?;
    for e in &entries {
        print_entry(e);
    }
    Ok(())
}

async fn run_verify(rpc_url: String, owner: Address) -> Result<()> {
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
    let deployer = Deployer::new(provider, Address::ZERO);
    let report = deployer.verify(owner).await?;
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

fn require_key(key: Option<String>, cmd: &str) -> Result<String> {
    key.with_context(|| format!("--private-key is required for `{cmd}`"))
}

fn require_owner(owner: Option<Address>, cmd: &str) -> Result<Address> {
    owner.with_context(|| format!("--owner is required for `{cmd}`"))
}

fn parse_key(key: &str) -> Result<PrivateKeySigner> {
    let hex = if let Some(var_name) = key.strip_prefix("env:") {
        std::env::var(var_name).with_context(|| format!("env var `{var_name}` not set"))?
    } else {
        key.to_string()
    };
    let hex = hex.strip_prefix("0x").unwrap_or(&hex);
    PrivateKeySigner::from_str(hex).context("invalid private key")
}

fn parse_ids(ids: &[String]) -> Result<Vec<ContractId>> {
    ids.iter().map(|s| parse_contract_id(s)).collect()
}

fn parse_contract_id(s: &str) -> Result<ContractId> {
    match s.to_lowercase().replace('-', "").as_str() {
        "ethlockbox" => Ok(ContractId::EthLockbox),
        other => bail!(
            "unknown contract id `{other}`; valid values: ETHLockbox, eth-lockbox, ethLockbox"
        ),
    }
}

fn print_entry(e: &RegistryEntry) {
    println!("l2_chain_id {}", e.l2_chain_id);
    println!("  id        {}", e.id);
    println!("  proxy     {}", e.proxy);
    println!("  impl      {}", e.current_impl);
    println!("  version   {}", e.version);
    println!("  deployed  block {}", e.deployed_at);
    println!("  upgraded  block {}", e.upgraded_at);
}

fn print_mismatch(m: &VerifyMismatch) {
    println!(
        "MISMATCH id={} proxy={} registry_impl={} erc1967_impl={}",
        m.id, m.proxy, m.registry_impl, m.erc1967_impl
    );
}
