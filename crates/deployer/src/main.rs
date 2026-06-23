//! kardamom-deploy: stateless CLI for deploying and upgrading kardamom L1 contracts.

use std::str::FromStr;

use alloy_primitives::{Address, Bytes};
use alloy_provider::ProviderBuilder;
use alloy_signer_local::PrivateKeySigner;
use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};

use kardamom_deployer::{
    ContractId, Deployer, FactoryStatus, Op, RegistryEntry, VerifyMismatch, encode_address_arg,
    encode_address_pair, encode_oracle_init_args,
};

#[derive(Debug, Parser)]
#[command(name = "kardamom-deploy", version)]
struct Cli {
    /// JSON-RPC endpoint.
    #[arg(long, default_value = "http://127.0.0.1:8545", global = true)]
    rpc_url: String,

    /// Canonical owner address (Safe or EOA). Same owner ⇒ same factory address.
    #[arg(long, global = true, required = true)]
    owner: Address,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Bootstrap the factory if absent. Anyone can run this; the resulting factory
    /// is always owned by `--owner`.
    EnsureFactory {
        /// Hex private key or "env:VAR_NAME". Pays for the bootstrap tx.
        #[arg(long)]
        private_key: String,
    },

    /// Deploy one or more contracts in one tx. Implies ensure-factory.
    /// `--l2-chain-id` and `--l2-minter` are positionally paired and repeat together.
    Deploy {
        /// Hex private key or "env:VAR_NAME".
        #[arg(long)]
        private_key: String,

        /// Contract IDs to deploy (e.g. ETHLockbox, WithdrawalOutputOracle).
        /// Same id repeats per L2.
        #[arg(required = true)]
        ids: Vec<String>,

        /// L2 chain IDs to target (one per id × L2 combination).
        #[arg(long = "l2-chain-id", required = true)]
        l2_chain_ids: Vec<u64>,

        /// L2 minter addresses for ETHLockbox initialize. Must match `--l2-chain-id`
        /// count when ETHLockbox is among the deployed ids.
        #[arg(long = "l2-minter")]
        l2_minters: Vec<Address>,

        /// Output oracle address wired into ETHLockbox.initialize. If omitted, the
        /// oracle being deployed in this same invocation is used (its address is
        /// predicted); otherwise defaults to the zero address (deposit-only).
        #[arg(long = "output-oracle")]
        output_oracle: Option<Address>,

        /// Attester address for WithdrawalOutputOracle.initialize.
        #[arg(long)]
        attester: Option<Address>,

        /// Challenger address for WithdrawalOutputOracle.initialize.
        #[arg(long)]
        challenger: Option<Address>,

        /// Finalization window (seconds) for WithdrawalOutputOracle.initialize.
        #[arg(long, default_value_t = 86_400)]
        finalization_window: u64,
    },

    /// Upgrade contracts to the next version across one or more L2s in one tx.
    Upgrade {
        /// Hex private key or "env:VAR_NAME".
        #[arg(long)]
        private_key: String,

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
        Command::EnsureFactory { private_key } => {
            run_ensure_factory(cli.rpc_url, private_key, cli.owner).await
        }
        Command::Deploy {
            private_key,
            ids,
            l2_chain_ids,
            l2_minters,
            output_oracle,
            attester,
            challenger,
            finalization_window,
        } => {
            let contract_ids = parse_ids(&ids)?;
            if contract_ids.contains(&ContractId::EthLockbox)
                && l2_chain_ids.len() != l2_minters.len()
            {
                bail!(
                    "--l2-chain-id ({}) and --l2-minter ({}) counts must match when deploying ETHLockbox",
                    l2_chain_ids.len(),
                    l2_minters.len()
                );
            }
            run_deploy(
                cli.rpc_url,
                private_key,
                cli.owner,
                contract_ids,
                l2_chain_ids,
                l2_minters,
                output_oracle,
                attester,
                challenger,
                finalization_window,
            )
            .await
        }
        Command::Upgrade {
            private_key,
            ids,
            l2_chain_ids,
        } => {
            let contract_ids = parse_ids(&ids)?;
            run_upgrade(
                cli.rpc_url,
                private_key,
                cli.owner,
                contract_ids,
                l2_chain_ids,
            )
            .await
        }
        Command::Addresses { l2_chain_id } => {
            run_addresses(cli.rpc_url, cli.owner, l2_chain_id).await
        }
        Command::Verify => run_verify(cli.rpc_url, cli.owner).await,
    }
}

async fn run_ensure_factory(rpc_url: String, private_key: String, owner: Address) -> Result<()> {
    let signer = parse_key(&private_key)?;
    let operator = signer.address();
    let provider = ProviderBuilder::new()
        .wallet(signer)
        .connect_http(rpc_url.parse()?);
    let deployer = Deployer::new(provider, owner);
    let factory_addr = deployer.factory_address();
    match deployer.ensure_factory(operator).await? {
        FactoryStatus::AlreadyDeployed => {
            println!("factory already deployed at {factory_addr} (owner: {owner})");
        }
        FactoryStatus::Deployed => {
            println!("factory deployed at {factory_addr} (owner: {owner})");
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn run_deploy(
    rpc_url: String,
    private_key: String,
    owner: Address,
    ids: Vec<ContractId>,
    l2_chain_ids: Vec<u64>,
    l2_minters: Vec<Address>,
    output_oracle: Option<Address>,
    attester: Option<Address>,
    challenger: Option<Address>,
    finalization_window: u64,
) -> Result<()> {
    let signer = parse_key(&private_key)?;
    let operator = signer.address();
    let provider = ProviderBuilder::new()
        .wallet(signer)
        .connect_http(rpc_url.parse()?);
    let deployer = Deployer::new(provider, owner);

    deployer.ensure_factory(operator).await?;

    // Per-contract init args. For each id × L2, emit one Op::Deploy with the
    // contract-specific initialize calldata.
    let deploying_oracle = ids.contains(&ContractId::WithdrawalOutputOracle);
    let mut ops: Vec<Op> = Vec::new();
    for id in &ids {
        for (i, chain_id) in l2_chain_ids.iter().enumerate() {
            let init_args = match id {
                ContractId::EthLockbox => {
                    let minter = l2_minters[i];
                    // Wire the oracle: explicit flag, else the oracle deployed in
                    // this same batch (predicted), else zero (deposit-only).
                    let oracle = match output_oracle {
                        Some(a) => a,
                        None if deploying_oracle => {
                            let oargs = oracle_init_args(attester, challenger, finalization_window)?;
                            deployer.predict_proxy_address(
                                *chain_id,
                                ContractId::WithdrawalOutputOracle,
                                &oargs,
                            )
                        }
                        None => Address::ZERO,
                    };
                    encode_address_pair(minter, oracle)
                }
                ContractId::WithdrawalOutputOracle => {
                    oracle_init_args(attester, challenger, finalization_window)?
                }
                ContractId::KardamomL2Settlement => encode_address_arg(l2_minters[i]),
            };
            ops.push(Op::Deploy {
                l2_chain_id: *chain_id,
                id: *id,
                init_args,
            });
        }
    }

    let tx = deployer.apply(&ops, operator).await?;
    println!("deployed in tx {tx}");

    let entries = deployer.addresses(None).await?;
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
    let deployer = Deployer::new(provider, owner);

    let current_entries = deployer.addresses(None).await?;

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

    let tx = deployer.apply(&ops, operator).await?;
    println!("upgraded in tx {tx}");

    let entries = deployer.addresses(None).await?;
    for e in &entries {
        print_entry(e);
    }
    Ok(())
}

async fn run_addresses(rpc_url: String, owner: Address, l2_chain_id: Option<u64>) -> Result<()> {
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
    let deployer = Deployer::new(provider, owner);
    let entries = deployer.addresses(l2_chain_id).await?;
    for e in &entries {
        print_entry(e);
    }
    Ok(())
}

async fn run_verify(rpc_url: String, owner: Address) -> Result<()> {
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
    let deployer = Deployer::new(provider, owner);
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
    match s.to_lowercase().replace(['-', '_'], "").as_str() {
        "ethlockbox" => Ok(ContractId::EthLockbox),
        "kardamoml2settlement" => Ok(ContractId::KardamomL2Settlement),
        "withdrawaloutputoracle" => Ok(ContractId::WithdrawalOutputOracle),
        other => bail!(
            "unknown contract id `{other}`; valid values: ETHLockbox, \
             WithdrawalOutputOracle, KardamomL2Settlement"
        ),
    }
}

/// Encode `WithdrawalOutputOracle.initialize(attester, challenger, window)`,
/// requiring the attester/challenger flags.
fn oracle_init_args(
    attester: Option<Address>,
    challenger: Option<Address>,
    window: u64,
) -> Result<Bytes> {
    let attester = attester.context("--attester required to deploy WithdrawalOutputOracle")?;
    let challenger =
        challenger.context("--challenger required to deploy WithdrawalOutputOracle")?;
    Ok(encode_oracle_init_args(attester, challenger, window))
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
