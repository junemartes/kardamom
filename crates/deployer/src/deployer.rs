//! Stateful deployer: wraps a provider + operator address and exposes
//! high-level `ensure_factory`, `apply`, `addresses`, and `verify` methods.

use alloy_network::{Ethereum, ReceiptResponse, TransactionBuilder};
use alloy_primitives::{Address, B256, Bytes, TxHash, U256};
use alloy_provider::Provider;
use alloy_rpc_types_eth::{TransactionReceipt, TransactionRequest};
use alloy_sol_types::{SolCall, sol};

use crate::addresses::{
    ERC1967_IMPL_SLOT, ERC7955_FACTORY, app_impl_address, app_proxy_address, factory_impl_address,
    factory_init_data, factory_proxy_address, proxy_full_initcode,
};
use crate::embedded;
use crate::ids::ContractId;
use crate::spec::{DeploymentSpec, Op, build_spec};

// ---------------------------------------------------------------------------
// Sol! bindings for the kardamom factory
// ---------------------------------------------------------------------------

sol!(
    #[sol(rpc)]
    #[derive(Debug)]
    IKardamomFactory,
    concat!(
        env!("CARGO_WORKSPACE_DIR"),
        "/contracts/out/IKardamomFactory.sol/IKardamomFactory.json"
    )
);

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
pub enum DeployError {
    #[error(
        "ERC-7955 CREATE2 factory not deployed at {address}; see https://github.com/safe-research/erc-7955 for bootstrap procedure"
    )]
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

impl From<alloy_provider::transport::TransportError> for DeployError {
    fn from(e: alloy_provider::transport::TransportError) -> Self {
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

/// Stateful deployer: wraps a provider and the canonical factory `owner`.
///
/// `owner` is part of the factory's CREATE2 initcode, so it pins the factory
/// address. Write methods take an `operator` (the tx `from`).
///
/// Generic over the provider so it works with any alloy provider (including
/// anvil-backed providers in tests).
pub struct Deployer<P> {
    provider: P,
    owner: Address,
}

impl<P: Provider<Ethereum> + Clone> Deployer<P> {
    pub fn new(provider: P, owner: Address) -> Self {
        Self { provider, owner }
    }

    pub fn owner(&self) -> Address {
        self.owner
    }

    // -----------------------------------------------------------------------
    // factory_address — pure computation
    // -----------------------------------------------------------------------

    /// Derive the factory proxy address from embedded creation bytecode + the
    /// configured owner. Pure (no I/O).
    pub fn factory_address(&self) -> Address {
        let impl_initcode = embedded::factory_v1_creation();
        let proxy_creation_code = embedded::erc1967_proxy_creation();
        let impl_addr = factory_impl_address(&impl_initcode);
        factory_proxy_address(&proxy_creation_code, impl_addr, self.owner)
    }

    /// Predict the proxy address a fresh `Op::Deploy { l2_chain_id, id, .. }`
    /// (version 1) will produce, given the exact `init_args` that deploy will
    /// use. Pure (no I/O). Used to wire one contract's address into another's
    /// init data within the same atomic `apply` batch (e.g. the output oracle
    /// address into `ETHLockbox.initialize`).
    pub fn predict_proxy_address(
        &self,
        l2_chain_id: u64,
        id: ContractId,
        init_args: &Bytes,
    ) -> Address {
        let factory = self.factory_address();
        let impl_initcode = id.creation_bytecode();
        let impl_addr = app_impl_address(factory, id.impl_salt(1), &impl_initcode);
        let init_data = crate::spec::encode_init_calldata(id, init_args);
        app_proxy_address(
            factory,
            &embedded::erc1967_proxy_creation(),
            impl_addr,
            &init_data,
            id.proxy_salt(l2_chain_id),
        )
    }

    // -----------------------------------------------------------------------
    // ensure_factory — bootstrap workflow
    // -----------------------------------------------------------------------

    /// Ensure the kardamom factory is deployed on the connected chain. Anyone
    /// with gas can call this; the on-chain owner is set at initialize time.
    pub async fn ensure_factory(&self, operator: Address) -> Result<FactoryStatus, DeployError> {
        // (a) ERC-7955 factory must be present.
        if !self.code_present(ERC7955_FACTORY).await? {
            return Err(DeployError::Erc7955FactoryAbsent {
                address: ERC7955_FACTORY,
            });
        }

        let factory_impl_initcode = embedded::factory_v1_creation();
        let proxy_creation_code = embedded::erc1967_proxy_creation();
        let impl_addr = factory_impl_address(&factory_impl_initcode);
        let init_data = factory_init_data(self.owner);
        let factory_proxy = self.factory_address();

        // (b) Already deployed?
        if self.code_present(factory_proxy).await? {
            return Ok(FactoryStatus::AlreadyDeployed);
        }

        // (c) Deploy impl via ERC-7955.
        let impl_salt = crate::addresses::factory_impl_salt();
        self.send_erc7955_tx(operator, impl_salt, &factory_impl_initcode)
            .await?;

        // (d) Deploy proxy via ERC-7955.
        let proxy_salt = crate::addresses::factory_proxy_salt();
        let full_proxy_initcode = proxy_full_initcode(&proxy_creation_code, impl_addr, &init_data);
        self.send_erc7955_tx(operator, proxy_salt, &full_proxy_initcode)
            .await?;

        // (e) Verify.
        if !self.code_present(factory_proxy).await? {
            return Err(DeployError::FactoryNotDeployed(factory_proxy));
        }

        Ok(FactoryStatus::Deployed)
    }

    // -----------------------------------------------------------------------
    // apply — send applyDeployments to the factory
    // -----------------------------------------------------------------------

    /// Send one `applyDeployments` tx for the given ops.
    ///
    /// Performs impl-dedup grouping: ops sharing the same `(id, version)` produce specs
    /// where only the first triggers a CREATE2 of the impl; subsequent specs reference
    /// the (offline-computed) impl address via `target_impl`. This makes "upgrade across
    /// N L2s to the same new impl" a one-impl-deploy operation instead of N copies.
    pub async fn apply(&self, ops: &[Op], operator: Address) -> Result<TxHash, DeployError> {
        let factory_proxy = self.factory_address();

        // Verify factory is deployed.
        if !self.code_present(factory_proxy).await? {
            return Err(DeployError::FactoryNotDeployed(factory_proxy));
        }

        // Build raw specs (each has target_impl = zero), then run the dedup pass.
        let mut specs: Vec<DeploymentSpec> = ops.iter().map(build_spec).collect();
        dedup_impl_specs(factory_proxy, &mut specs);

        // Encode + send.
        let abi_specs: Vec<IKardamomFactory::DeploymentSpec> =
            specs.into_iter().map(spec_to_abi).collect();
        let call = IKardamomFactory::applyDeploymentsCall { specs: abi_specs };
        let calldata = call.abi_encode();

        let tx = TransactionRequest::default()
            .with_from(operator)
            .with_to(factory_proxy)
            .with_input(Bytes::from(calldata));

        let receipt = self.send_and_confirm(tx).await?;
        Ok(receipt.transaction_hash())
    }

    // -----------------------------------------------------------------------
    // addresses — read the factory registry
    // -----------------------------------------------------------------------

    /// Read the factory's on-chain registry. If `l2_chain_id` is `Some`, returns entries
    /// only for that L2; otherwise returns entries across all registered L2s.
    pub async fn addresses(
        &self,
        l2_chain_id: Option<u64>,
    ) -> Result<Vec<RegistryEntry>, DeployError> {
        let factory_proxy = self.factory_address();

        if !self.code_present(factory_proxy).await? {
            return Err(DeployError::FactoryNotDeployed(factory_proxy));
        }

        let factory = IKardamomFactory::new(factory_proxy, &self.provider);

        let l2s: Vec<u64> = if let Some(id) = l2_chain_id {
            vec![id]
        } else {
            let count: U256 = factory.l2ChainIdCount().call().await?;
            let count: u64 = count.to();
            let mut out = Vec::with_capacity(count as usize);
            for i in 0..count {
                let v: U256 = factory.l2ChainIdAt(U256::from(i)).call().await?;
                out.push(v.to());
            }
            out
        };

        let mut entries = Vec::new();
        for l2 in l2s {
            let count: U256 = factory.idCount(U256::from(l2)).call().await?;
            let count: u64 = count.to();
            for i in 0..count {
                let id: B256 = factory.idAt(U256::from(l2), U256::from(i)).call().await?;
                let e: IKardamomFactory::Entry = factory.entry(U256::from(l2), id).call().await?;
                entries.push(RegistryEntry {
                    l2_chain_id: l2,
                    id,
                    proxy: e.proxy,
                    current_impl: e.currentImpl,
                    version: e.version,
                    deployed_at: e.deployedAt,
                    upgraded_at: e.upgradedAt,
                });
            }
        }
        Ok(entries)
    }

    // -----------------------------------------------------------------------
    // verify — cross-check registry vs ERC1967 storage slot
    // -----------------------------------------------------------------------

    /// Verify every registry entry's `currentImpl` matches the proxy's ERC1967 impl slot.
    pub async fn verify(&self) -> Result<VerifyReport, DeployError> {
        let entries = self.addresses(None).await?;
        let mut mismatches = Vec::new();
        let slot_u256 = U256::from_be_bytes(*ERC1967_IMPL_SLOT);

        for entry in &entries {
            let raw_slot: U256 = self.provider.get_storage_at(entry.proxy, slot_u256).await?;
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

    /// True iff `addr` has non-empty code on the connected chain.
    async fn code_present(&self, addr: Address) -> Result<bool, DeployError> {
        Ok(!self.provider.get_code_at(addr).await?.is_empty())
    }

    /// Send `tx`, wait for its receipt, and fail with [`DeployError::Reverted`]
    /// if the transaction did not succeed.
    async fn send_and_confirm(
        &self,
        tx: TransactionRequest,
    ) -> Result<TransactionReceipt, DeployError> {
        let receipt = self
            .provider
            .send_transaction(tx)
            .await?
            .get_receipt()
            .await?;
        if !receipt.status() {
            return Err(DeployError::Reverted);
        }
        Ok(receipt)
    }

    async fn send_erc7955_tx(
        &self,
        operator: Address,
        salt: B256,
        initcode: &Bytes,
    ) -> Result<TxHash, DeployError> {
        let calldata = erc7955_calldata(salt, initcode);
        let tx = TransactionRequest::default()
            .with_from(operator)
            .with_to(ERC7955_FACTORY)
            .with_input(calldata);

        let receipt = self.send_and_confirm(tx).await?;
        Ok(receipt.transaction_hash())
    }
}

// ---------------------------------------------------------------------------
// Free helpers
// ---------------------------------------------------------------------------

/// Impl-dedup pass used by [`Deployer::apply`]: within each `(id, impl_salt)`
/// group, the first spec keeps `target_impl = zero` (the factory CREATE2's the
/// impl); subsequent specs reference the impl via `target_impl`, computed
/// offline from the factory address and the spec's `impl_salt` + `impl_initcode`.
fn dedup_impl_specs(factory: Address, specs: &mut [DeploymentSpec]) {
    let mut seen_impl: std::collections::HashMap<(B256, B256), Address> =
        std::collections::HashMap::new();
    for s in specs {
        let key = (s.id, s.impl_salt);
        if let Some(addr) = seen_impl.get(&key) {
            s.target_impl = *addr;
        } else {
            let computed =
                crate::addresses::app_impl_address(factory, s.impl_salt, &s.impl_initcode);
            seen_impl.insert(key, computed);
            // First spec in the group keeps target_impl = zero (factory CREATE2's the impl).
        }
    }
}

/// Build `salt(32) || initcode` calldata for the ERC-7955 CREATE2 factory.
fn erc7955_calldata(salt: B256, initcode: &Bytes) -> Bytes {
    let mut buf = Vec::with_capacity(32 + initcode.len());
    buf.extend_from_slice(salt.as_slice());
    buf.extend_from_slice(initcode);
    Bytes::from(buf)
}

/// Convert a `crate::spec::DeploymentSpec` into its ABI counterpart.
fn spec_to_abi(s: DeploymentSpec) -> IKardamomFactory::DeploymentSpec {
    IKardamomFactory::DeploymentSpec {
        l2ChainId: U256::from(s.l2_chain_id),
        id: s.id,
        action: s.action as u8,
        implInitcode: s.impl_initcode,
        initData: s.init_data,
        implSalt: s.impl_salt,
        targetImpl: s.target_impl,
    }
}

#[cfg(test)]
mod apply_dedup_tests {
    use crate::spec::{Action, DeploymentSpec};
    use alloy_primitives::{Address, B256, Bytes};

    fn raw_spec(l2: u64, id_byte: u8, salt_byte: u8) -> DeploymentSpec {
        let mut id = [0u8; 32];
        id[0] = id_byte;
        let mut salt = [0u8; 32];
        salt[0] = salt_byte;
        DeploymentSpec {
            l2_chain_id: l2,
            id: B256::from(id),
            action: Action::Deploy,
            impl_initcode: Bytes::from(vec![0u8; 16]),
            init_data: Bytes::new(),
            impl_salt: B256::from(salt),
            target_impl: Address::ZERO,
        }
    }

    /// Sanity round-trip: build a `DeploymentSpec`, encode + decode through the
    /// JSON-ABI-derived `IKardamomFactory::applyDeploymentsCall`, and assert every
    /// field survives. Catches accidental field reshuffling in `spec_to_abi`
    /// (and would catch a Rust-side struct mismatch, though the JSON ABI itself is
    /// the source of truth for layout). The cargo↔Solidity layout pin is the
    /// bytecode-hash CI gate plus the e2e deploy/upgrade integration tests
    /// against anvil.
    #[test]
    fn apply_deployments_calldata_roundtrip() {
        use super::IKardamomFactory;
        use alloy_sol_types::{SolCall, SolValue};

        let spec = DeploymentSpec {
            l2_chain_id: 1234,
            id: B256::from([0xAB; 32]),
            action: Action::Upgrade,
            impl_initcode: Bytes::from(vec![0xDE, 0xAD, 0xBE, 0xEF]),
            init_data: Bytes::from(vec![0x01, 0x02, 0x03]),
            impl_salt: B256::from([0xCD; 32]),
            target_impl: Address::from([0x42; 20]),
        };
        let abi = super::spec_to_abi(spec.clone());
        let call = IKardamomFactory::applyDeploymentsCall {
            specs: vec![abi.clone()],
        };
        let encoded = call.abi_encode();
        let decoded = IKardamomFactory::applyDeploymentsCall::abi_decode(&encoded).unwrap();
        assert_eq!(decoded.specs.len(), 1);
        let d = &decoded.specs[0];
        assert_eq!(d.l2ChainId, alloy_primitives::U256::from(spec.l2_chain_id));
        assert_eq!(d.id, spec.id);
        assert_eq!(d.action, spec.action as u8);
        assert_eq!(d.implInitcode, spec.impl_initcode);
        assert_eq!(d.initData, spec.init_data);
        assert_eq!(d.implSalt, spec.impl_salt);
        assert_eq!(d.targetImpl, spec.target_impl);
        // Sanity: the abi round-trips byte-for-byte too.
        assert_eq!(d.abi_encode(), abi.abi_encode());
    }

    /// Exercises the dedup pass `apply` runs (`dedup_impl_specs`) against a
    /// fake factory address.
    #[test]
    fn dedup_picks_first_in_group_for_create2() {
        let factory = Address::from([0x11; 20]);
        let mut specs = vec![
            raw_spec(42, 1, 1),
            raw_spec(43, 1, 1), // same (id, salt) as #0 — must reuse
            raw_spec(42, 2, 2), // different id — own group
            raw_spec(44, 1, 1), // same (id, salt) as #0 — must reuse
        ];

        super::dedup_impl_specs(factory, &mut specs);

        assert_eq!(
            specs[0].target_impl,
            Address::ZERO,
            "first in group must CREATE2"
        );
        assert_ne!(
            specs[1].target_impl,
            Address::ZERO,
            "second in same group reuses"
        );
        assert_eq!(
            specs[2].target_impl,
            Address::ZERO,
            "different id starts new group"
        );
        assert_eq!(
            specs[1].target_impl, specs[3].target_impl,
            "same group, same target"
        );
    }
}
