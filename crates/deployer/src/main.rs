//! kardamom-deploy: stateless CLI for deploying and upgrading kardamom L1 contracts.

use std::num::NonZeroU64;
use std::str::FromStr;

use alloy_primitives::{Address, Bytes};
use alloy_provider::{Provider, ProviderBuilder};
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

    /// Canonical owner address (Safe or EOA). The same owner gives the same
    /// factory address.
    ///
    /// This field is not `global`. Clap forbids a required global argument.
    /// A debug build panics on the assert; a release build skips the assert
    /// and parsing breaks, because no pre-subcommand `--owner` ever satisfies
    /// the requirement. Pass `--owner` before the subcommand.
    #[arg(long, required = true)]
    owner: Address,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Bootstrap the factory if it is absent. Anyone can run this. The
    /// resulting factory is always owned by `--owner`.
    EnsureFactory {
        /// Hex private key or "`env:VAR_NAME`". Pays for the bootstrap tx.
        #[arg(long)]
        private_key: String,
    },

    /// Deploy one or more contracts in one transaction. This implies
    /// ensure-factory. `--l2-chain-id` and `--l2-minter` are paired by
    /// position and repeat together.
    Deploy {
        /// Hex private key or "`env:VAR_NAME`".
        #[arg(long)]
        private_key: String,

        /// Contract IDs to deploy, for example `ETHLockbox` or
        /// `WithdrawalOutputOracle`. The same id can repeat per L2.
        #[arg(required = true)]
        ids: Vec<String>,

        /// L2 chain IDs to target, one for each id-L2 combination. Chain id
        /// 0 is not valid: it feeds `ContractId::proxy_salt`, so a 0 would
        /// give a valid but wrong CREATE2 salt.
        #[arg(long = "l2-chain-id", required = true)]
        l2_chain_ids: Vec<NonZeroU64>,

        /// L2 minter addresses, paired by position with `--l2-chain-id`. The
        /// count must match when you deploy `ETHLockbox` (its `_l2Minter` init
        /// arg) or `KardamomL2Settlement` (reused as its `_l1Batcher` init arg).
        #[arg(long = "l2-minter")]
        l2_minters: Vec<Address>,

        /// Output oracle address for ETHLockbox.initialize. If you omit it,
        /// the deployer uses the oracle deployed in this same run, with its
        /// address predicted. If no such oracle is deployed, it defaults to
        /// the zero address, for deposit-only mode.
        #[arg(long = "output-oracle")]
        output_oracle: Option<Address>,

        /// Attester address for WithdrawalOutputOracle.initialize.
        #[arg(long)]
        attester: Option<Address>,

        /// Challenger address for WithdrawalOutputOracle.initialize.
        #[arg(long)]
        challenger: Option<Address>,

        /// Finalization window (seconds) for WithdrawalOutputOracle.initialize.
        /// The contract reverts with `ZeroWindow()` on 0; nonzero at the
        /// type level checks this before the deploy tx spends gas.
        #[arg(long, default_value = "86400")]
        finalization_window: NonZeroU64,
    },

    /// Upgrade contracts to the next version, across one or more L2s, in one
    /// transaction.
    Upgrade {
        /// Hex private key or "`env:VAR_NAME`".
        #[arg(long)]
        private_key: String,

        #[arg(required = true)]
        ids: Vec<String>,

        #[arg(long = "l2-chain-id", required = true)]
        l2_chain_ids: Vec<NonZeroU64>,
    },

    /// Install the ERC-7955 CREATE2 factory runtime through `anvil_setCode`.
    /// This works only on dev chains; a real chain uses the ERC-7955
    /// presigned bootstrap transaction. This command is idempotent. Run it
    /// before the first `deploy` against a fresh anvil.
    #[command(name = "bootstrap-7955-anvil")]
    Bootstrap7955Anvil,

    /// Print registered ids and their proxy/impl/version. Optionally filter by L2.
    Addresses {
        #[arg(long = "l2-chain-id")]
        l2_chain_id: Option<u64>,

        /// Restrict results to a named contract, for example `KardamomL2Settlement`.
        #[arg(long)]
        contract: Option<String>,

        /// Emit a JSON array for deployment automation.
        #[arg(long)]
        json: bool,
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
            // Every id whose init args index into `l2_minters` must appear
            // here. Otherwise, the positional `l2_minters[i]` below panics
            // with an index-out-of-bounds error, instead of a clear message.
            let minter_consumers: Vec<&str> = contract_ids
                .iter()
                .filter_map(|id| match id {
                    ContractId::EthLockbox => Some("ETHLockbox"),
                    ContractId::KardamomL2Settlement => Some("KardamomL2Settlement"),
                    ContractId::WithdrawalOutputOracle | ContractId::KardamomProofOracle => None,
                })
                .collect();
            if !minter_consumers.is_empty() && l2_chain_ids.len() != l2_minters.len() {
                bail!(
                    "--l2-chain-id ({}) and --l2-minter ({}) counts must match when deploying {}",
                    l2_chain_ids.len(),
                    l2_minters.len(),
                    minter_consumers.join(", ")
                );
            }
            DeployArgs {
                rpc_url: cli.rpc_url,
                private_key,
                owner: cli.owner,
                ids: contract_ids,
                l2_chain_ids,
                l2_minters,
                output_oracle,
                oracle: OracleArgs {
                    attester,
                    challenger,
                    finalization_window,
                },
            }
            .run()
            .await
        }
        Command::Upgrade {
            private_key,
            ids,
            l2_chain_ids,
        } => {
            let contract_ids = parse_ids(&ids)?;
            UpgradeArgs {
                ids: contract_ids,
                l2_chain_ids,
            }
            .run(cli.rpc_url, private_key, cli.owner)
            .await
        }
        Command::Bootstrap7955Anvil => {
            use kardamom_deployer::addresses::{ERC7955_FACTORY, ERC7955_RUNTIME_HEX};
            let provider = ProviderBuilder::new().connect_http(cli.rpc_url.parse()?);
            let bytes_hex = format!("0x{ERC7955_RUNTIME_HEX}");
            let _: serde_json::Value = alloy_provider::Provider::raw_request(
                &provider,
                "anvil_setCode".into(),
                (ERC7955_FACTORY, bytes_hex),
            )
            .await
            .context("anvil_setCode (is this a dev anvil chain?)")?;
            println!("ERC-7955 factory runtime installed at {ERC7955_FACTORY}");
            Ok(())
        }
        Command::Addresses {
            l2_chain_id,
            contract,
            json,
        } => run_addresses(cli.rpc_url, cli.owner, l2_chain_id, contract, json).await,
        Command::Verify => run_verify(cli.rpc_url, cli.owner).await,
    }
}

/// Build a signed [`Deployer`] connected to `rpc_url`, for write paths.
/// Returns the signer's own address, to use as the operator.
fn signed_deployer(
    rpc_url: &str,
    owner: Address,
    key: &str,
) -> Result<(Deployer<impl Provider + Clone>, Address)> {
    let url = rpc_url.parse()?;
    let signer = parse_key(key)?;
    let operator = signer.address();
    let provider = ProviderBuilder::new().wallet(signer).connect_http(url);
    Ok((Deployer::new(provider, owner), operator))
}

/// Build a read-only [`Deployer`] connected to `rpc_url`, for `addresses`
/// and `verify`; neither signs a transaction.
fn readonly_deployer(rpc_url: &str, owner: Address) -> Result<Deployer<impl Provider + Clone>> {
    let url = rpc_url.parse()?;
    let provider = ProviderBuilder::new().connect_http(url);
    Ok(Deployer::new(provider, owner))
}

async fn run_ensure_factory(rpc_url: String, private_key: String, owner: Address) -> Result<()> {
    let (deployer, operator) = signed_deployer(&rpc_url, owner, &private_key)?;
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

/// Arguments for [`DeployArgs::run`], grouped from the CLI's `Deploy` subcommand.
struct DeployArgs {
    rpc_url: String,
    private_key: String,
    owner: Address,
    ids: Vec<ContractId>,
    l2_chain_ids: Vec<NonZeroU64>,
    /// Minter addresses, paired by position with `l2_chain_ids`. Read only
    /// for ids that consume a minter (`EthLockbox`, `KardamomL2Settlement`).
    l2_minters: Vec<Address>,
    output_oracle: Option<Address>,
    oracle: OracleArgs,
}

/// `WithdrawalOutputOracle.initialize` arguments, required only when
/// deploying that contract.
struct OracleArgs {
    attester: Option<Address>,
    challenger: Option<Address>,
    finalization_window: NonZeroU64,
}

impl DeployArgs {
    /// The `--l2-minter` at position `i`, for the init args of the
    /// contract named `for_id` (a display name, e.g. "`ETHLockbox`"). Both
    /// `EthLockbox` and `KardamomL2Settlement` index into `l2_minters` by
    /// position and need this same "count must match" error.
    fn minter_at(&self, i: usize, for_id: &str) -> Result<Address> {
        self.l2_minters.get(i).copied().with_context(|| {
            format!("--l2-minter count must match --l2-chain-id when deploying {for_id}")
        })
    }

    /// The `Op::Deploy` entries for `id`, one per `l2_chain_ids` entry, in
    /// order. Each entry independently errors when `id`'s init args need
    /// `--l2-minter` but `l2_minters` is too short for that position (the
    /// caller pre-validates lengths when any requested id consumes a
    /// minter, but a per-position `.get` still guards against a future id
    /// added here without updating that check), or when `id` is
    /// `KardamomProofOracle` (not yet modeled by this CLI).
    fn ops_for<P: Provider + Clone>(
        &self,
        id: ContractId,
        deployer: &Deployer<P>,
    ) -> Vec<Result<Op>> {
        let deploying_oracle = self.ids.contains(&ContractId::WithdrawalOutputOracle);
        self.l2_chain_ids
            .iter()
            .enumerate()
            .map(|(i, chain_id)| -> Result<Op> {
                let chain_id = chain_id.get();
                let init_args = match id {
                    ContractId::EthLockbox => {
                        let minter = self.minter_at(i, "ETHLockbox")?;
                        // Pick the oracle address: the explicit flag, else
                        // the oracle deployed in this same batch
                        // (predicted), else zero for deposit-only mode.
                        let oracle = match self.output_oracle {
                            Some(a) => a,
                            None if deploying_oracle => {
                                let oargs = self.oracle.init_args()?;
                                deployer.predict_proxy_address(
                                    chain_id,
                                    ContractId::WithdrawalOutputOracle,
                                    &oargs,
                                )
                            }
                            None => Address::ZERO,
                        };
                        encode_address_pair(minter, oracle)
                    }
                    ContractId::WithdrawalOutputOracle => self.oracle.init_args()?,
                    // Reuse the positional --l2-minter as the settlement
                    // contract's `_l1Batcher` init arg (documented on the
                    // flag). Add a dedicated --l1-batcher flag if the roles
                    // ever diverge.
                    ContractId::KardamomL2Settlement => {
                        let minter = self.minter_at(i, "KardamomL2Settlement")?;
                        encode_address_arg(minter)
                    }
                    // The proof oracle's init needs the SP1 verifier
                    // gateway, program vkey, and genesis root. The CLI does
                    // not model these parameters yet. Deploy it through the
                    // library API (`ProofOracleInit::encode`, as the e2e
                    // test does) until a dedicated flag set is added.
                    ContractId::KardamomProofOracle => bail!(
                        "KardamomProofOracle CLI deployment needs --sp1-verifier/--program-vkey/\
                         --genesis-root flags (not yet modeled); use the library API"
                    ),
                };
                Ok(Op::Deploy {
                    l2_chain_id: chain_id,
                    id,
                    init_args,
                })
            })
            .collect()
    }

    /// Run the `Deploy` subcommand: ensure the factory, build one
    /// `Op::Deploy` per (id, L2) pair, apply them, and print the result.
    async fn run(self) -> Result<()> {
        let (deployer, operator) = signed_deployer(&self.rpc_url, self.owner, &self.private_key)?;

        deployer.ensure_factory(operator).await?;

        // Per-contract init args. For each id and L2 pair, one Op::Deploy
        // with the contract-specific initialize calldata.
        let ops: Vec<Op> = self
            .ids
            .iter()
            .flat_map(|id| self.ops_for(*id, &deployer))
            .collect::<Result<Vec<Op>>>()?;

        let tx = deployer.apply(&ops, operator).await?;
        println!("deployed in tx {tx}");

        print_addresses(&deployer, None).await
    }
}

/// Arguments for [`UpgradeArgs::run`], grouped from the CLI's `Upgrade`
/// subcommand.
struct UpgradeArgs {
    ids: Vec<ContractId>,
    l2_chain_ids: Vec<NonZeroU64>,
}

impl UpgradeArgs {
    /// The `Op::Upgrade` entries for `id`, one per `l2_chain_ids` entry.
    /// Each entry's `new_version` is the current on-chain version plus
    /// one, or 2 for a first upgrade with no existing entry. Errors on an
    /// implausible version overflow rather than wrapping into a wrong
    /// version number.
    fn ops_for(&self, id: ContractId, current_entries: &[RegistryEntry]) -> Vec<Result<Op>> {
        self.l2_chain_ids
            .iter()
            .map(|chain_id| -> Result<Op> {
                let chain_id = chain_id.get();
                let new_version = match current_entries
                    .iter()
                    .find(|e| e.l2_chain_id == chain_id && e.id == id.id())
                {
                    Some(e) => e
                        .version
                        .checked_add(1)
                        .context("registry version overflowed u64")?,
                    None => 2,
                };
                Ok(Op::Upgrade {
                    l2_chain_id: chain_id,
                    id,
                    new_version,
                    init_args: Bytes::new(),
                })
            })
            .collect()
    }

    /// Run the `Upgrade` subcommand: read the current registry, build one
    /// `Op::Upgrade` per (id, L2) pair, apply them, and print the result.
    /// Takes the connection arguments separately from `self` because,
    /// unlike `Deploy`, `Upgrade`'s own flags carry no rpc/key/owner state.
    async fn run(self, rpc_url: String, private_key: String, owner: Address) -> Result<()> {
        let (deployer, operator) = signed_deployer(&rpc_url, owner, &private_key)?;

        let current_entries = deployer.addresses(None).await?;

        let ops: Vec<Op> = self
            .ids
            .iter()
            .flat_map(|id| self.ops_for(*id, &current_entries))
            .collect::<Result<Vec<Op>>>()?;

        let tx = deployer.apply(&ops, operator).await?;
        println!("upgraded in tx {tx}");

        print_addresses(&deployer, None).await
    }
}

async fn run_addresses(
    rpc_url: String,
    owner: Address,
    l2_chain_id: Option<u64>,
    contract: Option<String>,
    json: bool,
) -> Result<()> {
    let contract = contract.as_deref().map(parse_contract_id).transpose()?;
    let deployer = readonly_deployer(&rpc_url, owner)?;
    let mut entries = deployer.addresses(l2_chain_id).await?;
    if let Some(contract) = contract {
        entries.retain(|entry| entry.id == contract.id());
    }
    if json {
        let records: Vec<_> = entries.iter().map(entry_json).collect();
        println!("{}", serde_json::to_string(&records)?);
    } else {
        print_entries(&entries);
    }
    Ok(())
}

fn entry_json(entry: &RegistryEntry) -> serde_json::Value {
    serde_json::json!({
        "l2_chain_id": entry.l2_chain_id,
        "id": entry.id.to_string(),
        "proxy": entry.proxy.to_string(),
        "impl": entry.current_impl.to_string(),
        "version": entry.version,
        "deployed_at": entry.deployed_at,
        "upgraded_at": entry.upgraded_at,
    })
}

async fn run_verify(rpc_url: String, owner: Address) -> Result<()> {
    let deployer = readonly_deployer(&rpc_url, owner)?;
    let report = deployer.verify().await?;
    print_entries(&report.entries);
    if report.mismatches.is_empty() {
        println!("all entries match ERC1967 impl slot");
        return Ok(());
    }
    report.mismatches.iter().for_each(print_mismatch);
    bail!("verify: {} mismatch(es) found", report.mismatches.len());
}

fn parse_key(key: &str) -> Result<PrivateKeySigner> {
    let hex = kardamom_deployer::KeyFlag::new(key).resolve()?;
    let hex = kardamom_deployer::strip_hex_prefix(&hex);
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

impl OracleArgs {
    /// Encode `WithdrawalOutputOracle.initialize(attester, challenger,
    /// window)`. The attester and challenger flags are required.
    ///
    /// # Errors
    /// Returns an error when `--attester` or `--challenger` is missing.
    fn init_args(&self) -> Result<Bytes> {
        let attester = self
            .attester
            .context("--attester required to deploy WithdrawalOutputOracle")?;
        let challenger = self
            .challenger
            .context("--challenger required to deploy WithdrawalOutputOracle")?;
        Ok(encode_oracle_init_args(
            attester,
            challenger,
            self.finalization_window.get(),
        ))
    }
}

/// Fetch registry entries, optionally filtered by L2, and print them.
async fn print_addresses<P: Provider + Clone>(
    deployer: &Deployer<P>,
    l2_chain_id: Option<u64>,
) -> Result<()> {
    print_entries(&deployer.addresses(l2_chain_id).await?);
    Ok(())
}

fn print_entries(entries: &[RegistryEntry]) {
    for e in entries {
        print_entry(e);
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
