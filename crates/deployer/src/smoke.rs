//! The transfer smoke of a deploy: one signed transfer through an ingress
//! JSON-RPC, with a receipt. The canary gate of the deploy role runs it
//! through `kardamom-deploy smoke`, because every release ships that
//! binary.

use std::num::NonZeroU64;
use std::time::Duration;

use alloy_network::TransactionBuilder;
use alloy_primitives::{Address, B256, U256, address};
use alloy_provider::transport::TransportError;
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
/// The JSON-RPC error code of `account state unavailable`: the ingress
/// answers a nonce read with it when no account read layer answers.
const STATE_UNAVAILABLE_CODE: i64 = -32000;
/// The message prefix of `account state unavailable`. Other refusals
/// share the code, so the prefix names this one.
const STATE_UNAVAILABLE: &str = "account state unavailable";
/// The time that a nonce read repeats through `account state
/// unavailable`. The ingress tries every executor in one call, so the
/// refusal means that no executor answered in its timeout, which a short
/// stall of the executors causes.
const NONCE_READ_BUDGET: Duration = Duration::from_secs(60);
/// The interval between two nonce reads.
const NONCE_READ_RETRY: Duration = Duration::from_secs(1);

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
        let nonce = NonceRead {
            provider: &provider,
            from,
        }
        .nonce()
        .await
        .with_context(|| format!("eth_getTransactionCount of {from} through {}", self.rpc_url))?;
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

/// The nonce read of the sending account, at the latest block.
struct NonceRead<'a, P> {
    provider: &'a P,
    from: Address,
}

impl<P: Provider> NonceRead<'_, P> {
    /// The next nonce of the account. The read repeats on `account state
    /// unavailable` for at most [`NONCE_READ_BUDGET`]; any other failure
    /// ends it at once.
    async fn nonce(&self) -> anyhow::Result<u64> {
        tokio::time::timeout(NONCE_READ_BUDGET, self.repeat())
            .await
            .map_err(|_| anyhow::anyhow!("{STATE_UNAVAILABLE} for {NONCE_READ_BUDGET:?}"))?
    }

    /// The reads, until one gives a nonce or fails with another error.
    async fn repeat(&self) -> anyhow::Result<u64> {
        loop {
            if let Some(nonce) = self.read().await? {
                return Ok(nonce);
            }
        }
    }

    /// One nonce read. `None` is `account state unavailable`; the read
    /// then waits one retry interval before it returns.
    async fn read(&self) -> anyhow::Result<Option<u64>> {
        match self
            .provider
            .get_transaction_count(self.from)
            .latest()
            .await
        {
            Ok(nonce) => Ok(Some(nonce)),
            Err(error) if Self::is_state_unavailable(&error) => {
                println!("smoke: eth_getTransactionCount: {error}; the read repeats");
                tokio::time::sleep(NONCE_READ_RETRY).await;
                Ok(None)
            }
            Err(error) => Err(error.into()),
        }
    }

    /// Whether the ingress refused the read with `account state
    /// unavailable`.
    fn is_state_unavailable(error: &TransportError) -> bool {
        error.as_error_resp().is_some_and(|payload| {
            payload.code == STATE_UNAVAILABLE_CODE && payload.message.starts_with(STATE_UNAVAILABLE)
        })
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

#[cfg(test)]
#[path = "smoke_tests.rs"]
mod tests;
