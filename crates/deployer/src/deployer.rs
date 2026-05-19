//! Stateful deployer: wraps a provider + operator address and exposes
//! high-level `ensure_factory`, `apply`, `addresses`, and `verify` methods.

use std::path::PathBuf;

use alloy_network::{Ethereum, ReceiptResponse, TransactionBuilder};
use alloy_primitives::{Address, B256, Bytes, TxHash, U256};
use alloy_provider::Provider;
use alloy_rpc_types_eth::TransactionRequest;
use alloy_sol_types::{SolCall, sol};

use crate::addresses::{
    ERC1967_IMPL_SLOT, ERC7955_FACTORY, factory_impl_address, factory_init_data,
    factory_proxy_address, proxy_full_initcode,
};
use crate::artifacts::{ArtifactError, creation_bytecode};
use crate::spec::{DeploymentSpec, Op, build_spec};

// ---------------------------------------------------------------------------
// Sol! bindings for the kardamom factory
// ---------------------------------------------------------------------------

sol! {
    #[sol(rpc)]
    #[derive(Debug)]
    interface IKardamomFactory {
        struct DeploymentSpecAbi {
            uint256 l2ChainId;
            bytes32 id;
            uint8 action;
            bytes implInitcode;
            bytes initData;
            bytes32 implSalt;
            address targetImpl;
        }

        struct Entry {
            address proxy;
            address currentImpl;
            uint64 version;
            uint64 deployedAt;
            uint64 upgradedAt;
            bool exists;
        }

        function applyDeployments(DeploymentSpecAbi[] calldata specs) external;
        function entry(uint256 l2ChainId, bytes32 id) external view returns (Entry memory);
        function l2ChainIdCount() external view returns (uint256);
        function l2ChainIdAt(uint256 i) external view returns (uint256);
        function idCount(uint256 l2ChainId) external view returns (uint256);
        function idAt(uint256 l2ChainId, uint256 i) external view returns (bytes32);
    }
}

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
pub enum DeployError {
    #[error("artifact: {0}")]
    Artifact(#[from] ArtifactError),
    #[error("ERC-7955 CREATE2 factory not deployed at {address}; see https://github.com/safe-research/erc-7955 for bootstrap procedure")]
    Erc7955FactoryAbsent { address: Address },
    #[error("factory not deployed at expected address {0}; run ensure-factory")]
    FactoryNotDeployed(Address),
    #[error("transaction reverted")]
    Reverted,
    #[error("provider error: {0}")]
    Provider(String),
}

impl From<alloy_provider::PendingTransactionError> for DeployError {
    fn from(e: alloy_provider::PendingTransactionError) -> Self {
        DeployError::Provider(e.to_string())
    }
}

impl From<alloy_contract::Error> for DeployError {
    fn from(e: alloy_contract::Error) -> Self {
        DeployError::Provider(e.to_string())
    }
}

// ---------------------------------------------------------------------------
// Status and report types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FactoryStatus {
    AlreadyDeployed,
    Deployed,
}

#[derive(Debug, Clone)]
pub struct RegistryEntry {
    pub l2_chain_id: u64,
    pub id: B256,
    pub proxy: Address,
    pub current_impl: Address,
    pub version: u64,
    pub deployed_at: u64,
    pub upgraded_at: u64,
}

#[derive(Debug, Clone)]
pub struct VerifyReport {
    pub entries: Vec<RegistryEntry>,
    pub mismatches: Vec<VerifyMismatch>,
}

#[derive(Debug, Clone)]
pub struct VerifyMismatch {
    pub id: B256,
    pub proxy: Address,
    pub registry_impl: Address,
    pub erc1967_impl: Address,
}

// ---------------------------------------------------------------------------
// Deployer
// ---------------------------------------------------------------------------

/// Stateful deployer: wraps a provider and operator address.
///
/// Generic over the provider so it works with any alloy provider (including
/// anvil-backed providers in tests).
pub struct Deployer<P> {
    provider: P,
    /// The "from" address used when sending transactions.
    operator: Address,
    /// Path to the `contracts/` directory (containing `out/`).
    contracts_root: PathBuf,
}

impl<P: Provider<Ethereum> + Clone> Deployer<P> {
    /// Create a new `Deployer`.
    ///
    /// `contracts_root` defaults to `crate::artifacts::default_contracts_root()`.
    pub fn new(provider: P, operator: Address) -> Self {
        Self {
            provider,
            operator,
            contracts_root: crate::artifacts::default_contracts_root(),
        }
    }

    /// Override the contracts root (e.g. in tests that compile to a tmpdir).
    pub fn with_contracts_root(mut self, root: PathBuf) -> Self {
        self.contracts_root = root;
        self
    }

    // -----------------------------------------------------------------------
    // factory_address — pure computation (async for API consistency)
    // -----------------------------------------------------------------------

    /// Derive the factory proxy address from compiled forge artifacts + the canonical owner.
    pub async fn factory_address(&self, owner: Address) -> Result<Address, DeployError> {
        let (impl_initcode, proxy_creation_code) = self.read_factory_artifacts()?;
        let impl_addr = factory_impl_address(&impl_initcode);
        Ok(factory_proxy_address(&proxy_creation_code, impl_addr, owner))
    }

    // -----------------------------------------------------------------------
    // ensure_factory — bootstrap workflow
    // -----------------------------------------------------------------------

    /// Ensure the kardamom factory is deployed on the connected chain.
    ///
    /// `owner` is the canonical owner for this environment — same owner across L1s
    /// means same factory address. Anyone with gas can call this; the owner is set
    /// at initialize time, not by tx.origin.
    pub async fn ensure_factory(&self, owner: Address) -> Result<FactoryStatus, DeployError> {
        // (a) ERC-7955 factory must be present.
        let factory_code = self
            .provider
            .get_code_at(ERC7955_FACTORY)
            .await
            .map_err(|e| DeployError::Provider(e.to_string()))?;
        if factory_code.is_empty() {
            return Err(DeployError::Erc7955FactoryAbsent {
                address: ERC7955_FACTORY,
            });
        }

        let (factory_impl_initcode, proxy_creation_code) = self.read_factory_artifacts()?;
        let impl_addr = factory_impl_address(&factory_impl_initcode);
        let init_data = factory_init_data(owner);
        let factory_proxy = factory_proxy_address(&proxy_creation_code, impl_addr, owner);

        // (b) Already deployed?
        let proxy_code = self
            .provider
            .get_code_at(factory_proxy)
            .await
            .map_err(|e| DeployError::Provider(e.to_string()))?;
        if !proxy_code.is_empty() {
            return Ok(FactoryStatus::AlreadyDeployed);
        }

        // (c) Deploy impl via ERC-7955.
        let impl_salt = crate::addresses::factory_impl_salt();
        self.send_erc7955_tx(impl_salt, &factory_impl_initcode)
            .await?;

        // (d) Deploy proxy via ERC-7955.
        let proxy_salt = crate::addresses::factory_proxy_salt();
        let full_proxy_initcode = proxy_full_initcode(&proxy_creation_code, impl_addr, &init_data);
        self.send_erc7955_tx(proxy_salt, &full_proxy_initcode)
            .await?;

        // (e) Verify.
        let proxy_code_after = self
            .provider
            .get_code_at(factory_proxy)
            .await
            .map_err(|e| DeployError::Provider(e.to_string()))?;
        if proxy_code_after.is_empty() {
            return Err(DeployError::FactoryNotDeployed(factory_proxy));
        }

        Ok(FactoryStatus::Deployed)
    }

    // -----------------------------------------------------------------------
    // apply — send applyDeployments to the factory
    // -----------------------------------------------------------------------

    /// Build `DeploymentSpec[]` from `ops`, encode as `applyDeployments` calldata,
    /// and send to the factory proxy.  Returns the transaction hash.
    pub async fn apply(&self, ops: &[Op]) -> Result<TxHash, DeployError> {
        let factory_proxy = self.factory_address().await?;

        // Verify factory is deployed.
        let code = self
            .provider
            .get_code_at(factory_proxy)
            .await
            .map_err(|e| DeployError::Provider(e.to_string()))?;
        if code.is_empty() {
            return Err(DeployError::FactoryNotDeployed(factory_proxy));
        }

        // Build specs and convert to ABI types.
        let specs: Vec<IKardamomFactory::DeploymentSpecAbi> = ops
            .iter()
            .map(|op| {
                let s: DeploymentSpec = build_spec(&self.contracts_root, op)?;
                Ok(spec_to_abi(s))
            })
            .collect::<Result<_, DeployError>>()?;

        // Encode calldata using the sol!-generated SolCall impl.
        let call = IKardamomFactory::applyDeploymentsCall { specs };
        let calldata = call.abi_encode();

        let tx = TransactionRequest::default()
            .with_from(self.operator)
            .with_to(factory_proxy)
            .with_input(Bytes::from(calldata));

        let receipt = self
            .provider
            .send_transaction(tx)
            .await
            .map_err(|e| DeployError::Provider(e.to_string()))?
            .get_receipt()
            .await?;

        if !receipt.status() {
            return Err(DeployError::Reverted);
        }
        Ok(receipt.transaction_hash())
    }

    // -----------------------------------------------------------------------
    // addresses — read the factory registry
    // -----------------------------------------------------------------------

    /// Read the factory's on-chain registry.
    ///
    /// Calls `idCount()`, then for each index calls `idAt(i)` and `entry(id)`.
    pub async fn addresses(&self) -> Result<Vec<RegistryEntry>, DeployError> {
        let factory_proxy = self.factory_address().await?;

        // Verify factory is deployed.
        let code = self
            .provider
            .get_code_at(factory_proxy)
            .await
            .map_err(|e| DeployError::Provider(e.to_string()))?;
        if code.is_empty() {
            return Err(DeployError::FactoryNotDeployed(factory_proxy));
        }

        let factory = IKardamomFactory::new(factory_proxy, &self.provider);

        let count: U256 = factory.idCount().call().await?;
        let count: u64 = count.to();

        let mut entries = Vec::with_capacity(count as usize);

        for i in 0..count {
            let id: B256 = factory.idAt(U256::from(i)).call().await?;
            let e: IKardamomFactory::Entry = factory.entry(id).call().await?;

            entries.push(RegistryEntry {
                id,
                proxy: e.proxy,
                current_impl: e.currentImpl,
                version: e.version,
                deployed_at: e.deployedAt,
                upgraded_at: e.upgradedAt,
            });
        }

        Ok(entries)
    }

    // -----------------------------------------------------------------------
    // verify — cross-check registry vs ERC1967 storage slot
    // -----------------------------------------------------------------------

    /// Verify that every registry entry's `currentImpl` matches the actual
    /// ERC1967 implementation slot in the proxy.
    pub async fn verify(&self) -> Result<VerifyReport, DeployError> {
        let entries = self.addresses().await?;
        let mut mismatches = Vec::new();

        // Convert B256 slot constant to U256 (big-endian) for get_storage_at.
        let slot_u256 = U256::from_be_bytes(*ERC1967_IMPL_SLOT);

        for entry in &entries {
            let raw_slot: U256 = self
                .provider
                .get_storage_at(entry.proxy, slot_u256)
                .await
                .map_err(|e| DeployError::Provider(e.to_string()))?;

            // The impl address is right-aligned in the 32-byte word.
            let erc1967_impl = Address::from_word(B256::from(raw_slot));

            if erc1967_impl != entry.current_impl {
                mismatches.push(VerifyMismatch {
                    id: entry.id,
                    proxy: entry.proxy,
                    registry_impl: entry.current_impl,
                    erc1967_impl,
                });
            }
        }

        Ok(VerifyReport {
            entries,
            mismatches,
        })
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    /// Read `KardamomFactoryV1` and `ERC1967Proxy` (or `ProxyArtifact`) creation
    /// bytecode from forge artifacts.
    fn read_factory_artifacts(&self) -> Result<(Bytes, Bytes), DeployError> {
        let factory_impl_initcode = creation_bytecode(&self.contracts_root, "KardamomFactoryV1")?;
        let proxy_creation_code = match creation_bytecode(&self.contracts_root, "ERC1967Proxy") {
            Ok(b) => b,
            Err(_) => creation_bytecode(&self.contracts_root, "ProxyArtifact")?,
        };
        Ok((factory_impl_initcode, proxy_creation_code))
    }

    async fn send_erc7955_tx(&self, salt: B256, initcode: &Bytes) -> Result<TxHash, DeployError> {
        let calldata = erc7955_calldata(salt, initcode);
        let tx = TransactionRequest::default()
            .with_from(self.operator)
            .with_to(ERC7955_FACTORY)
            .with_input(calldata);

        let receipt = self
            .provider
            .send_transaction(tx)
            .await
            .map_err(|e| DeployError::Provider(e.to_string()))?
            .get_receipt()
            .await?;

        if !receipt.status() {
            return Err(DeployError::Reverted);
        }
        Ok(receipt.transaction_hash())
    }
}

// ---------------------------------------------------------------------------
// Free helpers
// ---------------------------------------------------------------------------

/// Build `salt(32) || initcode` calldata for the ERC-7955 CREATE2 factory.
fn erc7955_calldata(salt: B256, initcode: &Bytes) -> Bytes {
    let mut buf = Vec::with_capacity(32 + initcode.len());
    buf.extend_from_slice(salt.as_slice());
    buf.extend_from_slice(initcode);
    Bytes::from(buf)
}

/// Convert a `crate::spec::DeploymentSpec` into its ABI counterpart.
fn spec_to_abi(s: DeploymentSpec) -> IKardamomFactory::DeploymentSpecAbi {
    IKardamomFactory::DeploymentSpecAbi {
        id: s.id,
        action: s.action as u8,
        implInitcode: s.impl_initcode,
        initData: s.init_data,
        implSalt: s.impl_salt,
    }
}
