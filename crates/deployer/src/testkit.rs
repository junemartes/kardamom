//! Shared e2e test scaffold: anvil plus the ERC-7955 CREATE2 factory
//! bootstrap. Behind the `test-support` feature, so downstream crates'
//! tests can use it without duplicating the anvil setup dance.

use std::num::NonZeroU64;

use alloy_node_bindings::{Anvil, AnvilInstance};
use alloy_primitives::{Address, B256, U256};
use alloy_provider::{Provider, ProviderBuilder, RootProvider};

use crate::addresses::{ERC7955_FACTORY, ERC7955_RUNTIME_HEX};
use crate::spec::ProofOracleInit;
use crate::{ContractId, Deployer, Op, encode_address_arg};

/// Funding amount every e2e fixture in this workspace uses for a dev
/// account: 1000 ETH.
const FUND_WEI: u128 = 1_000_000_000_000_000_000_000;

/// Whether a funded dev account is also impersonated, so a test can call
/// as it with no signature (`FundAndImpersonate`), or funded only, so it
/// keeps signing real transactions with its own key (`FundOnly` — the
/// batcher EOA, which must sign genuine EIP-4844 blob transactions that
/// an impersonated account cannot).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Funding {
    FundAndImpersonate,
    FundOnly,
}

/// A spawned anvil instance and a provider connected to it, with the
/// ERC-7955 CREATE2 factory runtime installed. Returned by
/// [`AnvilRig::spawn`].
pub struct AnvilRig {
    pub anvil: AnvilInstance,
    pub provider: RootProvider,
}

impl AnvilRig {
    /// Spawn `anvil` (already configured with the flags the caller needs,
    /// for example `Anvil::new().block_time(1)`), install the ERC-7955
    /// factory runtime, and fund every address in `fund` — impersonating
    /// it too, unless its [`Funding`] says `FundOnly`. Returns `None` if
    /// anvil is not installed — the convention every e2e test in this
    /// workspace uses to skip cleanly, rather than fail, when the local
    /// toolchain lacks it.
    pub async fn spawn(anvil: Anvil, fund: &[(Address, Funding)]) -> Option<Self> {
        let anvil = anvil.try_spawn().ok()?;
        let provider: RootProvider = ProviderBuilder::new()
            .disable_recommended_fillers()
            .connect_http(anvil.endpoint_url());

        let bytes_hex = format!("0x{ERC7955_RUNTIME_HEX}");
        let _: serde_json::Value = provider
            .raw_request("anvil_setCode".into(), (ERC7955_FACTORY, bytes_hex))
            .await
            .ok()?;
        for (addr, funding) in fund {
            let _: serde_json::Value = provider
                .raw_request("anvil_setBalance".into(), (*addr, U256::from(FUND_WEI)))
                .await
                .ok()?;
            if *funding == Funding::FundAndImpersonate {
                let _: serde_json::Value = provider
                    .raw_request("anvil_impersonateAccount".into(), (*addr,))
                    .await
                    .ok()?;
            }
        }
        Some(AnvilRig { anvil, provider })
    }
}

/// The v2 `KardamomProofOracle.initialize` fields that do not depend on
/// the settlement address. [`Deployer::deploy_settlement_and_oracle`]
/// fills in `settlement` once the settlement deploy resolves its address.
#[derive(Debug, Clone, Copy)]
pub struct OracleInitArgs {
    pub verifier: Address,
    pub batch_vkey: B256,
    pub block_vkey: B256,
    pub genesis_root: B256,
    pub challenge_window_secs: NonZeroU64,
    pub min_bond_wei: U256,
}

impl<P: Provider + Clone> Deployer<P> {
    /// Deploy the factory and a `KardamomL2Settlement` for `l2_chain_id`,
    /// with `l1_batcher` as its `l1Batcher`. Returns the settlement's
    /// proxy address.
    ///
    /// # Panics
    /// Panics if the deploy or the address lookup fails; this is a test
    /// helper, so a deploy failure should fail the test loudly.
    pub async fn deploy_settlement(
        &self,
        owner: Address,
        l2_chain_id: u64,
        l1_batcher: Address,
    ) -> Address {
        self.ensure_factory(owner).await.unwrap();
        self.apply(
            &[Op::Deploy {
                l2_chain_id,
                id: ContractId::KardamomL2Settlement,
                init_args: encode_address_arg(l1_batcher),
            }],
            owner,
        )
        .await
        .expect("deploy KardamomL2Settlement");
        self.addresses(Some(l2_chain_id))
            .await
            .unwrap()
            .into_iter()
            .find(|e| e.id == ContractId::KardamomL2Settlement.id())
            .expect("settlement registry entry")
            .proxy
    }

    /// Deploy the factory, a `KardamomL2Settlement`, and a
    /// `KardamomProofOracle` wired to it, for `l2_chain_id`.
    ///
    /// # Panics
    /// Panics if a deploy or an address lookup fails; this is a test
    /// helper, so a deploy failure should fail the test loudly.
    pub async fn deploy_settlement_and_oracle(
        &self,
        owner: Address,
        l2_chain_id: u64,
        l1_batcher: Address,
        oracle_args: OracleInitArgs,
    ) -> SettlementAndOracle {
        let settlement = self.deploy_settlement(owner, l2_chain_id, l1_batcher).await;
        self.apply(
            &[Op::Deploy {
                l2_chain_id,
                id: ContractId::KardamomProofOracle,
                init_args: ProofOracleInit {
                    settlement,
                    verifier: oracle_args.verifier,
                    batch_vkey: oracle_args.batch_vkey,
                    block_vkey: oracle_args.block_vkey,
                    genesis_root: oracle_args.genesis_root,
                    challenge_window_secs: oracle_args.challenge_window_secs,
                    min_bond_wei: oracle_args.min_bond_wei,
                }
                .encode(),
            }],
            owner,
        )
        .await
        .expect("deploy KardamomProofOracle");
        let oracle = self
            .addresses(Some(l2_chain_id))
            .await
            .unwrap()
            .into_iter()
            .find(|e| e.id == ContractId::KardamomProofOracle.id())
            .expect("oracle registry entry")
            .proxy;
        SettlementAndOracle { settlement, oracle }
    }
}

/// [`Deployer::deploy_settlement_and_oracle`]'s result: the deployed
/// settlement and proof-oracle proxy addresses.
pub struct SettlementAndOracle {
    pub settlement: Address,
    pub oracle: Address,
}
