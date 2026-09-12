//! One signed transfer through the ingress JSON-RPC, with a receipt
//! poll: the mechanic every smoke gate and re-smoke uses. Each caller
//! owns a dedicated funded account, so every transfer has nonce 0. The
//! ingress does not implement `eth_getTransactionCount`, on purpose.

use std::time::Duration;

use alloy_consensus::{SignableTransaction, TxEnvelope, TxLegacy};
use alloy_eips::eip2718::Encodable2718;
use alloy_network::TxSignerSync;
use alloy_primitives::{Address, TxKind, U256, address};
use anyhow::Context;
use kardamom_bench::mnemonic::derive_signers;

/// The mnemonic the genesis accounts derive from.
pub use kardamom_bench::ANVIL_MNEMONIC;

/// The burn address of every smoke transfer.
const SINK: Address = address!("000000000000000000000000000000000000dEaD");
const GAS_PRICE: u128 = 1_000_000_000;
const GAS_LIMIT: u64 = 21_000;

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
            anyhow::bail!("{method} error: {err}");
        }
        Ok(response
            .get("result")
            .cloned()
            .unwrap_or(serde_json::Value::Null))
    }

    /// Sign a one-wei transfer from genesis account `account` at nonce 0,
    /// submit it, and poll its receipt for up to `budget`.
    ///
    /// # Errors
    ///
    /// Returns an error if the signer cannot derive, the submit fails,
    /// no receipt arrives in time, or the receipt status is not `0x1`.
    pub async fn transfer_smoke(&self, account: u32, budget: Duration) -> anyhow::Result<()> {
        let signer = derive_signers(
            ANVIL_MNEMONIC,
            account.checked_add(1).context("account index")?,
        )?
        .pop()
        .context("derive the smoke signer")?;
        let mut tx = TxLegacy {
            chain_id: Some(self.chain_id),
            nonce: 0,
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
        let hash = self
            .call(
                "eth_sendRawTransaction",
                serde_json::json!([format!("0x{}", hex(&raw))]),
            )
            .await?;
        let hash = hash
            .as_str()
            .context("eth_sendRawTransaction returned no hash")?
            .to_string();
        crate::log(format!(
            "smoke: account #{account} sent {hash} through {}",
            self.url
        ));
        self.await_receipt(&hash, budget).await
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

    async fn receipt_status(&self, hash: &str) -> anyhow::Result<Option<String>> {
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
