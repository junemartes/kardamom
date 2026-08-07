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

mod contracts;

use std::time::Duration;

use alloy_network::EthereumWallet;
use alloy_primitives::{Address, B256, Bytes, U256};
use alloy_provider::{Provider, ProviderBuilder};
use alloy_signer_local::PrivateKeySigner;
use anyhow::{Context, Result};
use kardamom_deployer::addresses::{ERC7955_FACTORY, ERC7955_RUNTIME_HEX};
use kardamom_deployer::{ContractId, Deployer, Op, encode_address_pair, encode_oracle_init_args};

pub use contracts::*;

/// Short finalization window so a scenario can warp past it quickly (the
/// production default is 86_400).
pub const FINALIZATION_WINDOW: u64 = 60;

/// The receipt type the wallet providers hand back.
type L1TxReceipt = <alloy_network::Ethereum as alloy_network::Network>::ReceiptResponse;

/// Provider without a wallet (reads + `anvil_*` cheatcodes), tuned for tests
/// (50 ms poll). Takes the URL by value and captures no lifetime, so the
/// result is usable where `'static` is required (`deposit_logs` hands it to
/// `L1Source`).
fn provider_for(url: String) -> impl Provider + Clone {
    let p = ProviderBuilder::new()
        .disable_recommended_fillers()
        .connect_http(url.parse().expect("valid anvil RPC url"));
    p.client().set_poll_interval(Duration::from_millis(50));
    p
}

/// A running anvil L1 with the bridge contracts deployed.
pub struct L1 {
    anvil: alloy_node_bindings::AnvilInstance,
    pub lockbox: Address,
    pub oracle: Address,
    /// `KardamomL2Settlement` — the DA batch inbox (S8).
    pub settlement: Address,
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

        let deploy_provider = provider_for(anvil.endpoint());

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
        // The batcher signs real blob txs, so it needs a real balance.
        let _: serde_json::Value = deploy_provider
            .raw_request(
                "anvil_setBalance".into(),
                (BATCHER_ADDR, U256::from(1_000_000_000_000_000_000_000u128)),
            )
            .await
            .context("fund the batcher EOA")?;

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
                    // The DA batch inbox. `l1Batcher` is the only address
                    // allowed to post, so it is the key S8 signs blob txs with.
                    Op::Deploy {
                        l2_chain_id,
                        id: ContractId::KardamomL2Settlement,
                        init_args: kardamom_deployer::encode_address_arg(BATCHER_ADDR),
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
        let settlement = find(ContractId::KardamomL2Settlement)?;
        anyhow::ensure!(
            oracle == predicted_oracle,
            "predicted oracle {predicted_oracle} != deployed {oracle}"
        );

        Ok(Some(Self {
            anvil,
            lockbox,
            oracle,
            settlement,
        }))
    }

    pub fn rpc_url(&self) -> String {
        self.anvil.endpoint()
    }

    /// Provider without a wallet (reads + anvil_* cheatcodes).
    pub fn provider(&self) -> impl Provider + Clone {
        provider_for(self.anvil.endpoint())
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

    /// The `(block_hash, block_number, log_index)` of the `DepositInitiated`
    /// log carried by a `depositETH` receipt (which must be successful).
    fn deposit_log(&self, receipt: &L1TxReceipt) -> Result<(B256, u64, u64)> {
        anyhow::ensure!(receipt.status(), "depositETH reverted");
        let log = receipt
            .inner
            .logs()
            .iter()
            .find(|l| l.address() == self.lockbox)
            .context("no DepositInitiated log from the lockbox")?;
        Ok((
            log.block_hash.context("log carries no block hash")?,
            log.block_number.context("log carries no block number")?,
            log.log_index.context("log carries no index")?,
        ))
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
        let (block_hash, _block_number, log_index) = self.deposit_log(&receipt)?;
        Ok((block_hash, log_index))
    }

    /// Several `depositETH` calls landing in ONE L1 block — i.e. one epoch
    /// with several deposits.
    ///
    /// Auto-mining is disabled for the duration so the sends queue up, then a
    /// single `evm_mine` seals them together. Sending them under auto-mine
    /// would put each in its own block and test nothing about grouping.
    ///
    /// Returns `(block_hash, block_number, log_index)` per deposit.
    pub async fn deposit_eth_batch(
        &self,
        recipients: &[Address],
        value: U256,
    ) -> Result<Vec<(B256, u64, u64)>> {
        let provider = self.wallet(DEPOSITOR_KEY)?;
        self.pause_block_production().await?;

        let lockbox = ETHLockbox::new(self.lockbox, &provider);
        let mut pending = Vec::new();
        for to in recipients {
            let p = lockbox
                .depositETH(*to, 200_000u64, Bytes::new())
                .value(value)
                .send()
                .await
                .context("send depositETH (batched)")?;
            pending.push(p);
        }

        let _: serde_json::Value = self
            .provider()
            .raw_request("evm_mine".into(), ())
            .await
            .context("evm_mine (seal batch)")?;
        self.resume_block_production().await?;

        let mut out = Vec::new();
        for p in pending {
            let receipt = p
                .get_receipt()
                .await
                .context("batched depositETH receipt")?;
            out.push(self.deposit_log(&receipt)?);
        }
        Ok(out)
    }

    /// `evm_setAutomine` — the single switch behind
    /// [`pause_block_production`](Self::pause_block_production) /
    /// [`resume_block_production`](Self::resume_block_production).
    async fn set_automine(&self, on: bool) -> Result<()> {
        let _: serde_json::Value = self
            .provider()
            .raw_request("evm_setAutomine".into(), (on,))
            .await
            .with_context(|| format!("evm_setAutomine({on})"))?;
        Ok(())
    }

    /// Stop L1 block production entirely (anvil runs with `block_time(1)`, so
    /// simply not calling [`mine`](Self::mine) does NOT make L1 idle).
    /// Pairs with [`resume_block_production`](Self::resume_block_production).
    pub async fn pause_block_production(&self) -> Result<()> {
        self.set_automine(false).await
    }

    /// Resume automatic L1 block production.
    pub async fn resume_block_production(&self) -> Result<()> {
        self.set_automine(true).await
    }

    /// Latest finalized L1 block number — the same view the da-watcher tails.
    pub async fn finalized_block_number(&self) -> Result<u64> {
        let block = self
            .provider()
            .get_block_by_number(alloy_eips::BlockNumberOrTag::Finalized)
            .await
            .context("get finalized block")?
            .context("L1 has no finalized block yet")?;
        Ok(block.header.number)
    }

    /// Every `DepositInitiated` log the lockbox emitted in `[from, to]`,
    /// decoded through the SAME rule the producer uses.
    ///
    /// Tests compare the chain against this, so re-implementing the decode
    /// here would let a producer bug and a test bug cancel out.
    pub async fn deposit_logs(
        &self,
        from_block: u64,
        to_block: u64,
    ) -> Result<Vec<kardamom_types::DepositLog>> {
        // Via `provider_for` rather than `provider()`: the method returns an
        // `impl Provider` capturing `&self`, and `L1Source` needs `'static`.
        let source = kardamom_da_watcher::RpcL1Source::new(provider_for(self.anvil.endpoint()));
        kardamom_da_watcher::L1Source::deposit_logs(&source, self.lockbox, from_block, to_block)
            .await
            .context("read deposit logs")
    }
}
