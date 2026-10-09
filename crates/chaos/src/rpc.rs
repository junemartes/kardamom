//! One signed transfer through the ingress JSON-RPC, with a receipt
//! poll: the mechanic every smoke gate and re-smoke uses. The transfer
//! reads the account's next nonce first, so a smoke runs again on a
//! chain that already holds the account's earlier transfers. The
//! recovery probe of a case reads the nonce of the case's account the
//! same way. The balance probe drives the ingress's account layers for
//! the Redis cases.

use std::time::Duration;

use alloy_consensus::{SignableTransaction, TxEnvelope, TxLegacy};
use alloy_eips::eip2718::Encodable2718;
use alloy_network::TxSignerSync;
use alloy_primitives::{Address, TxKind, U256, address};
use anyhow::Context;
use kardamom_bench::mnemonic::derive_signers;
use kardamom_bench::signers::DerivedSigner;

/// The mnemonic the genesis accounts derive from.
pub use kardamom_bench::ANVIL_MNEMONIC;

/// The burn address of every smoke transfer.
const SINK: Address = address!("000000000000000000000000000000000000dEaD");
const GAS_PRICE: u128 = 1_000_000_000;
const GAS_LIMIT: u64 = 21_000;

/// The JSON-RPC error code of a halted chain, as the ingress answers a
/// submit with (`kardamom_ingress::error::CHAIN_HALTED_CODE`).
pub const CHAIN_HALTED_CODE: i64 = -32010;

/// The error object of a refused call: its code and its message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RpcError {
    pub code: i64,
    pub message: String,
}

impl RpcError {
    fn from_json(err: &serde_json::Value) -> Self {
        Self {
            code: err
                .get("code")
                .and_then(serde_json::Value::as_i64)
                .unwrap_or(0),
            message: err
                .get("message")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_string(),
        }
    }
}

/// A JSON-RPC client of one ingress.
#[derive(Debug, Clone)]
pub struct Rpc {
    http: reqwest::Client,
    pub url: String,
    pub chain_id: u64,
}

impl Rpc {
    /// A client of the ingress at `url` for chain `chain_id`.
    ///
    /// # Errors
    ///
    /// Returns an error if the HTTP client fails to build.
    pub fn new(url: &str, chain_id: u64) -> anyhow::Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(20))
            .build()
            .context("build the RPC HTTP client")?;
        Ok(Self {
            http,
            url: url.to_string(),
            chain_id,
        })
    }

    async fn call(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> anyhow::Result<serde_json::Value> {
        match self.call_typed(method, params).await? {
            Ok(result) => Ok(result),
            Err(err) => anyhow::bail!("{method} error: {} ({})", err.message, err.code),
        }
    }

    /// One call whose refusal the caller reads: `Ok(Err(..))` carries the
    /// error object, `Err(..)` a transport or decode failure.
    async fn call_typed(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> anyhow::Result<Result<serde_json::Value, RpcError>> {
        let body =
            serde_json::json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params});
        let response: serde_json::Value = self
            .http
            .post(&self.url)
            .json(&body)
            .send()
            .await
            .with_context(|| format!("{method} to {}", self.url))?
            .json()
            .await
            .with_context(|| format!("decode {method} response"))?;
        if let Some(err) = response.get("error") {
            return Ok(Err(RpcError::from_json(err)));
        }
        Ok(Ok(response
            .get("result")
            .cloned()
            .unwrap_or(serde_json::Value::Null)))
    }

    /// Whether the JSON-RPC listener answers. `eth_chainId` reads no state,
    /// so a false answer means the listener, not the pipeline behind it.
    pub async fn answers(&self) -> bool {
        self.call("eth_chainId", serde_json::json!([]))
            .await
            .is_ok()
    }

    /// One `eth_getBalance` of `address` at the head. A cold address
    /// misses the ingress's local layer, so the read touches Redis when
    /// it is on, then the executor. The value is not the point; the
    /// answer is.
    ///
    /// # Errors
    ///
    /// Returns an error when the call fails or answers an error.
    pub async fn balance_probe(&self, address: Address) -> anyhow::Result<()> {
        self.call(
            "eth_getBalance",
            serde_json::json!([format!("{address}"), "latest"]),
        )
        .await
        .map(|_| ())
    }

    /// The ingress's chain status: the heads, the roots, and every
    /// service's latest state on the `events` stream.
    ///
    /// # Errors
    ///
    /// Returns an error if the call fails.
    pub async fn chain_status(&self) -> anyhow::Result<serde_json::Value> {
        self.call("kardamom_chainStatus", serde_json::json!([]))
            .await
    }

    /// The next nonce of genesis account `account`, from the latest
    /// block the ingress serves.
    ///
    /// # Errors
    ///
    /// Returns an error if the signer cannot derive or the call fails.
    pub async fn nonce_of(&self, account: u32) -> anyhow::Result<u64> {
        self.nonce_at(Self::genesis_signer(account)?.address).await
    }

    /// The signer of genesis account `account` of the dev mnemonic.
    fn genesis_signer(account: u32) -> anyhow::Result<DerivedSigner> {
        derive_signers(
            ANVIL_MNEMONIC,
            account.checked_add(1).context("account index")?,
        )?
        .pop()
        .context("derive the genesis account")
    }

    /// The next nonce of `address`, from the latest block the ingress
    /// serves.
    async fn nonce_at(&self, address: Address) -> anyhow::Result<u64> {
        let nonce = self
            .call(
                "eth_getTransactionCount",
                serde_json::json!([format!("{address:#x}"), "latest"]),
            )
            .await?;
        let nonce = nonce
            .as_str()
            .context("eth_getTransactionCount returned no quantity")?;
        u64::from_str_radix(nonce.trim_start_matches("0x"), 16)
            .with_context(|| format!("parse the nonce {nonce}"))
    }

    /// Sign a one-wei transfer from genesis account `account` at its next
    /// nonce, submit it, and poll its receipt for up to `budget`. The
    /// ingress answers the nonce from the head, so an earlier transfer of
    /// the account with no receipt yet makes this one a duplicate.
    ///
    /// # Errors
    ///
    /// Returns an error if the signer cannot derive, the nonce read or
    /// the submit fails, no receipt arrives in time, or the receipt
    /// status is not `0x1`.
    pub async fn transfer_smoke(&self, account: u32, budget: Duration) -> anyhow::Result<()> {
        let nonce = self.nonce_of(account).await?;
        self.transfer_at(account, nonce, budget).await
    }

    /// [`Self::transfer_smoke`] at `nonce`, for a case that sets the nonce
    /// itself.
    ///
    /// # Errors
    ///
    /// Returns an error if the signer cannot derive, the submit fails,
    /// no receipt arrives in time, or the receipt status is not `0x1`.
    pub async fn transfer_at(
        &self,
        account: u32,
        nonce: u64,
        budget: Duration,
    ) -> anyhow::Result<()> {
        self.transfer_hash(account, nonce, budget).await.map(|_| ())
    }

    /// [`Self::transfer_at`], with the hash of the receipted transfer, for
    /// a case that asks for its receipt again later.
    ///
    /// # Errors
    ///
    /// Returns an error if the signer cannot derive, the submit fails,
    /// no receipt arrives in time, or the receipt status is not `0x1`.
    pub async fn transfer_hash(
        &self,
        account: u32,
        nonce: u64,
        budget: Duration,
    ) -> anyhow::Result<String> {
        let hash = self.send_transfer(account, nonce).await?.map_err(|e| {
            anyhow::anyhow!("eth_sendRawTransaction error: {} ({})", e.message, e.code)
        })?;
        crate::log(format!(
            "smoke: account #{account} sent {hash} through {}",
            self.url
        ));
        self.await_receipt(&hash, budget).await?;
        Ok(hash)
    }

    /// Sign and submit a one-wei transfer from genesis account `account`
    /// at `nonce`. `Ok(Ok(hash))` is an accepted submit, `Ok(Err(..))` the
    /// ingress's refusal, for a case that expects one.
    ///
    /// # Errors
    ///
    /// Returns an error if the signer cannot derive or the call fails.
    pub async fn send_transfer(
        &self,
        account: u32,
        nonce: u64,
    ) -> anyhow::Result<Result<String, RpcError>> {
        let signer = Self::genesis_signer(account)?;
        let mut tx = TxLegacy {
            chain_id: Some(self.chain_id),
            nonce,
            gas_price: GAS_PRICE,
            gas_limit: GAS_LIMIT,
            to: TxKind::Call(SINK),
            value: U256::from(1),
            input: alloy_primitives::Bytes::new(),
        };
        let sig = signer
            .signer
            .sign_transaction_sync(&mut tx)
            .map_err(|e| anyhow::anyhow!("sign the smoke transfer: {e}"))?;
        let envelope: TxEnvelope = tx.into_signed(sig).into();
        let mut raw = Vec::with_capacity(110);
        envelope.encode_2718(&mut raw);
        let answer = self
            .call_typed(
                "eth_sendRawTransaction",
                serde_json::json!([format!("0x{}", hex(&raw))]),
            )
            .await?;
        Ok(answer.and_then(|hash| {
            hash.as_str().map(str::to_string).ok_or_else(|| RpcError {
                code: 0,
                message: "eth_sendRawTransaction returned no hash".to_string(),
            })
        }))
    }

    async fn await_receipt(&self, hash: &str, budget: Duration) -> anyhow::Result<()> {
        let deadline = tokio::time::Instant::now() + budget;
        loop {
            match self.receipt_status(hash).await? {
                Some(status) => return check_status(hash, &status),
                None if tokio::time::Instant::now() >= deadline => {
                    anyhow::bail!("no receipt for {hash} within {budget:?}")
                }
                None => tokio::time::sleep(Duration::from_secs(1)).await,
            }
        }
    }

    /// The status of the receipt of `hash`, or `None` while the ingress
    /// serves no receipt for it.
    ///
    /// # Errors
    ///
    /// Returns an error if the call fails.
    pub async fn receipt_status(&self, hash: &str) -> anyhow::Result<Option<String>> {
        let receipt = self
            .call("eth_getTransactionReceipt", serde_json::json!([hash]))
            .await?;
        Ok(receipt
            .get("status")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string))
    }
}

fn check_status(hash: &str, status: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        status == "0x1",
        "receipt status of {hash} was {status} (expected 0x1)"
    );
    crate::log(format!("smoke: {hash} receipt status 0x1"));
    Ok(())
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut s, b| {
            let _ = write!(s, "{b:02x}");
            s
        })
}

#[cfg(test)]
mod tests;
