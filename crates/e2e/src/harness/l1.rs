//! Anvil as the L1, with the bridge contracts deployed.
//!
//! Reuses the pattern `crates/validator/tests/withdrawal_e2e.rs` established:
//! predeploy the ERC-7955 factory with `anvil_setCode`, fund + impersonate
//! `DEV_OWNER`, bootstrap the kardamom factory, then deploy the
//! `WithdrawalOutputOracle` and `ETHLockbox` **atomically** with the oracle's
//! address predicted and wired into the lockbox's initializer.
//!
//! Two anvil flags are load-bearing here:
//! - `--slots-in-an-epoch 1` so the `finalized` tag advances. The da-watcher
//!   polls `eth_getBlockByNumber("finalized")`, which by default lags
//!   `latest` by ~64 blocks — without this the watcher never sees a deposit
//!   and the scenario stalls forever. (This cost the deleted #67 e2e real
//!   debugging time; it is the single most important line in this file.)
//! - `block_time(1)` so blocks are produced without explicit mining.

use std::time::Duration;

use alloy_network::EthereumWallet;
use alloy_primitives::{Address, B256, Bytes, U256, address};
use alloy_provider::{Provider, ProviderBuilder};
use alloy_signer_local::PrivateKeySigner;
use alloy_sol_types::sol;
use anyhow::{Context, Result};
use kardamom_deployer::addresses::{ERC7955_FACTORY, ERC7955_RUNTIME_HEX};
use kardamom_deployer::{ContractId, Deployer, Op, encode_address_pair, encode_oracle_init_args};

/// Factory owner; impersonated on anvil rather than key-signed.
pub const DEV_OWNER: Address = address!("00000000000000000000000000000000DEAD0001");
/// The L2 minter authorized on the lockbox (unused by these scenarios, which
/// exercise the deposit/withdraw paths rather than minter-gated calls).
pub const L2_MINTER: Address = address!("00000000000000000000000000000000000000BE");

/// Anvil dev account #0 — the oracle's attester (and the account prefunded by
/// `chains/dev-withdrawals.toml` on L2).
pub const ATTESTER_KEY: &str = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
pub const ATTESTER_ADDR: Address = address!("f39Fd6e51aad88F6F4ce6aB8827279cffFb92266");
/// Anvil dev account #1 — the oracle's challenger, and the EOA these
/// scenarios deposit from.
pub const DEPOSITOR_KEY: &str =
    "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d";
pub const DEPOSITOR_ADDR: Address = address!("70997970C51812dc3A010C7d01b50e0d17dc79C8");

/// Short finalization window so a scenario can warp past it quickly (the
/// production default is 86_400).
pub const FINALIZATION_WINDOW: u64 = 60;

sol! {
    #[sol(rpc)]
    contract ETHLockbox {
        struct WithdrawalTransaction {
            uint256 nonce;
            address sender;
            address target;
            uint256 value;
        }
        function depositETH(address to, uint64 gasLimit, bytes calldata data) external payable;
        function outputOracle() external view returns (address);
        function finalizeWithdrawal(
            WithdrawalTransaction calldata wtx,
            uint256 outputIndex,
            bytes32 stateRoot,
            bytes32 withdrawalsRoot,
            uint256 leafIndex,
            bytes32[] calldata proof
        ) external;
        event DepositInitiated(
            uint64 indexed depositNonce,
            address indexed from,
            address indexed to,
            uint256 mint,
            uint64 gasLimit,
            bytes data
        );
    }

    #[sol(rpc)]
    contract WithdrawalOutputOracle {
        function outputCount() external view returns (uint256);
        function outputRootAt(uint256 index) external view returns (bytes32);
        function isFinalizable(uint256 index) external view returns (bool);
    }

    /// The L2 predeploy at `kardamom_types::withdrawals::MESSAGE_PASSER`.
    /// No Rust binding exists in the workspace, so declare one here.
    #[sol(rpc)]
    contract L2ToL1MessagePasser {
        function initiateWithdrawal(address target) external payable;
        event MessagePassed(
            uint256 indexed nonce,
            address indexed sender,
            address indexed target,
            uint256 value,
            bytes32 withdrawalHash
        );
    }
}

/// A running anvil L1 with the bridge contracts deployed.
pub struct L1 {
    anvil: alloy_node_bindings::AnvilInstance,
    pub lockbox: Address,
    pub oracle: Address,
}

impl L1 {
    /// Spawn anvil and deploy the bridge. `Ok(None)` when the `anvil` binary
    /// is absent, so bridge scenarios skip rather than fail on a machine
    /// without Foundry (the convention every other anvil test here uses).
    pub async fn launch(l2_chain_id: u64) -> Result<Option<Self>> {
        let Ok(anvil) = alloy_node_bindings::Anvil::new()
            .block_time(1)
            .arg("--slots-in-an-epoch")
            .arg("1")
            .try_spawn()
        else {
            return Ok(None);
        };

        let deploy_provider = ProviderBuilder::new()
            .disable_recommended_fillers()
            .connect_http(anvil.endpoint_url());
        deploy_provider
            .client()
            .set_poll_interval(Duration::from_millis(50));

        let _: serde_json::Value = deploy_provider
            .raw_request(
                "anvil_setCode".into(),
                (ERC7955_FACTORY, format!("0x{ERC7955_RUNTIME_HEX}")),
            )
            .await
            .context("predeploy ERC-7955 factory")?;
        let _: serde_json::Value = deploy_provider
            .raw_request(
                "anvil_setBalance".into(),
                (DEV_OWNER, U256::from(1_000_000_000_000_000_000_000u128)),
            )
            .await
            .context("fund DEV_OWNER")?;
        let _: serde_json::Value = deploy_provider
            .raw_request("anvil_impersonateAccount".into(), (DEV_OWNER,))
            .await
            .context("impersonate DEV_OWNER")?;

        let deployer = Deployer::new(deploy_provider.clone(), DEV_OWNER);
        deployer
            .ensure_factory(DEV_OWNER)
            .await
            .context("ensure kardamom factory")?;

        // The lockbox's initializer needs the oracle's address, so predict it
        // from the EXACT init args the deploy will use and deploy both in one
        // transaction.
        let oracle_init =
            encode_oracle_init_args(ATTESTER_ADDR, DEPOSITOR_ADDR, FINALIZATION_WINDOW);
        let predicted_oracle = deployer.predict_proxy_address(
            l2_chain_id,
            ContractId::WithdrawalOutputOracle,
            &oracle_init,
        );
        let lockbox_init = encode_address_pair(L2_MINTER, predicted_oracle);
        deployer
            .apply(
                &[
                    Op::Deploy {
                        l2_chain_id,
                        id: ContractId::WithdrawalOutputOracle,
                        init_args: oracle_init,
                    },
                    Op::Deploy {
                        l2_chain_id,
                        id: ContractId::EthLockbox,
                        init_args: lockbox_init,
                    },
                ],
                DEV_OWNER,
            )
            .await
            .context("deploy oracle + lockbox")?;

        let entries = deployer
            .addresses(Some(l2_chain_id))
            .await
            .context("read deployed addresses")?;
        let find = |id: ContractId| -> Result<Address> {
            entries
                .iter()
                .find(|e| e.id == id.id())
                .map(|e| e.proxy)
                .with_context(|| format!("{id:?} missing from the registry"))
        };
        let oracle = find(ContractId::WithdrawalOutputOracle)?;
        let lockbox = find(ContractId::EthLockbox)?;
        anyhow::ensure!(
            oracle == predicted_oracle,
            "predicted oracle {predicted_oracle} != deployed {oracle}"
        );

        Ok(Some(Self {
            anvil,
            lockbox,
            oracle,
        }))
    }

    pub fn rpc_url(&self) -> String {
        self.anvil.endpoint()
    }

    /// Provider without a wallet (reads + anvil_* cheatcodes).
    pub fn provider(&self) -> impl Provider + Clone {
        let p = ProviderBuilder::new()
            .disable_recommended_fillers()
            .connect_http(self.anvil.endpoint_url());
        p.client().set_poll_interval(Duration::from_millis(50));
        p
    }

    /// Provider that signs with `key`.
    pub fn wallet(&self, key: &str) -> Result<impl Provider + Clone> {
        let signer: PrivateKeySigner = key.parse().context("parse dev key")?;
        let p = ProviderBuilder::new()
            .wallet(EthereumWallet::from(signer))
            .connect_http(self.anvil.endpoint_url());
        p.client().set_poll_interval(Duration::from_millis(50));
        Ok(p)
    }

    /// Mine `n` empty blocks (also advances `finalized`, given
    /// `--slots-in-an-epoch 1`).
    pub async fn mine(&self, n: u64) -> Result<()> {
        let p = self.provider();
        for _ in 0..n {
            let _: serde_json::Value = p
                .raw_request("evm_mine".into(), ())
                .await
                .context("evm_mine")?;
        }
        Ok(())
    }

    /// Jump past the oracle's finalization window, then mine so the new
    /// timestamp is observable.
    pub async fn warp_past_window(&self) -> Result<()> {
        let p = self.provider();
        let _: serde_json::Value = p
            .raw_request("evm_increaseTime".into(), (FINALIZATION_WINDOW + 10,))
            .await
            .context("evm_increaseTime")?;
        self.mine(2).await
    }

    /// `depositETH(to, gas_limit, "")` with `value` wei, from `DEPOSITOR_ADDR`.
    /// Returns the L1 block hash and log index the OP-style `source_hash` is
    /// derived from.
    pub async fn deposit_eth(&self, to: Address, value: U256) -> Result<(B256, u64)> {
        let provider = self.wallet(DEPOSITOR_KEY)?;
        let lockbox = ETHLockbox::new(self.lockbox, &provider);
        let receipt = lockbox
            .depositETH(to, 200_000u64, Bytes::new())
            .value(value)
            .send()
            .await
            .context("send depositETH")?
            .get_receipt()
            .await
            .context("depositETH receipt")?;
        anyhow::ensure!(receipt.status(), "depositETH reverted");
        let log = receipt
            .inner
            .logs()
            .iter()
            .find(|l| l.address() == self.lockbox)
            .context("no DepositInitiated log from the lockbox")?;
        let block_hash = log.block_hash.context("log carries no block hash")?;
        let log_index = log.log_index.context("log carries no index")?;
        Ok((block_hash, log_index))
    }
}
