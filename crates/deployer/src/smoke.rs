//! The transfer smoke of a deploy: one signed transfer through an ingress
//! JSON-RPC, with a receipt. The canary gate of the deploy role runs it
//! through `kardamom-deploy smoke`, because every release ships that
//! binary.

use std::num::NonZeroU64;
use std::time::Duration;

use alloy_network::TransactionBuilder;
use alloy_primitives::{Address, B256, U256, address};
use alloy_provider::{Provider, ProviderBuilder};
use alloy_rpc_types_eth::TransactionRequest;
use alloy_signer_local::{MnemonicBuilder, coins_bip39::English};
use anyhow::Context;

/// The mnemonic that the genesis accounts of the chain derive from.
const GENESIS_MNEMONIC: &str = "test test test test test test test test test test test junk";
/// The burn address of every smoke transfer.
const SINK: Address = address!("000000000000000000000000000000000000dEaD");
const GAS_PRICE: u128 = 1_000_000_000;
const GAS_LIMIT: u64 = 21_000;
/// The interval between two receipt reads.
const RECEIPT_POLL: Duration = Duration::from_secs(1);

/// One transfer of one wei from a genesis account to the burn address.
/// The nonce is the next nonce of the account at the latest block, so a
/// smoke runs again on a chain that holds the earlier transfers of the
/// account.
#[derive(Debug, Clone)]
pub struct TransferSmoke {
    /// The JSON-RPC URL of the ingress.
    pub rpc_url: String,
    /// The chain id that the transfer signs for.
    pub chain_id: NonZeroU64,
    /// The index of the genesis account that sends.
    pub account: u32,
    /// The time that the receipt has to arrive in.
    pub budget: Duration,
}

impl TransferSmoke {
    /// Sign and submit the transfer, and wait for its receipt. Returns the
    /// hash of the transfer.
    ///
    /// # Errors
    ///
    /// Returns an error if the URL does not parse, the signer does not
    /// derive, the nonce read or the submit fails, no receipt arrives in
    /// the budget, or the receipt status is a failure.
    pub async fn run(&self) -> anyhow::Result<B256> {
        let signer = MnemonicBuilder::<English>::default()
            .phrase(GENESIS_MNEMONIC)
            .index(self.account)
            .with_context(|| format!("derivation path of genesis account #{}", self.account))?
            .build()
            .with_context(|| format!("derive genesis account #{}", self.account))?;
        let from = signer.address();
        let url = self
            .rpc_url
            .parse()
            .with_context(|| format!("parse the RPC URL {}", self.rpc_url))?;
        let provider = ProviderBuilder::new()
            .disable_recommended_fillers()
            .wallet(signer)
            .connect_http(url);
        let nonce = provider
            .get_transaction_count(from)
            .latest()
            .await
            .with_context(|| {
                format!("eth_getTransactionCount of {from} through {}", self.rpc_url)
            })?;
        let request = TransactionRequest::default()
            .with_from(from)
            .with_to(SINK)
            .with_value(U256::from(1))
            .with_nonce(nonce)
            .with_chain_id(self.chain_id.get())
            .with_gas_limit(GAS_LIMIT)
            .with_gas_price(GAS_PRICE);
        let hash = *provider
            .send_transaction(request)
            .await
            .with_context(|| format!("submit the smoke transfer through {}", self.rpc_url))?
            .tx_hash();
        println!(
            "smoke: account #{} sent {hash} through {}",
            self.account, self.rpc_url
        );
        let submitted = Submitted { provider, hash };
        let success = tokio::time::timeout(self.budget, submitted.status())
            .await
            .map_err(|_| anyhow::anyhow!("no receipt for {hash} within {:?}", self.budget))??;
        anyhow::ensure!(success, "the receipt of {hash} has a failure status");
        println!("smoke: {hash} has a receipt with a success status");
        Ok(hash)
    }
}

/// A submitted transfer and the provider that reads its receipt.
struct Submitted<P> {
    provider: P,
    hash: B256,
}

impl<P: Provider> Submitted<P> {
    /// The status of the receipt of the transfer: `true` for a success.
    /// The reads repeat until the ingress serves the receipt.
    async fn status(&self) -> anyhow::Result<bool> {
        loop {
            if let Some(success) = self.read_status().await? {
                return Ok(success);
            }
        }
    }

    /// One receipt read. `None` while the ingress serves no receipt; the
    /// read then waits one poll interval before it returns.
    async fn read_status(&self) -> anyhow::Result<Option<bool>> {
        let receipt = self
            .provider
            .get_transaction_receipt(self.hash)
            .await
            .with_context(|| format!("eth_getTransactionReceipt of {}", self.hash))?;
        if receipt.is_none() {
            tokio::time::sleep(RECEIPT_POLL).await;
        }
        Ok(receipt.map(|receipt| receipt.status()))
    }
}
