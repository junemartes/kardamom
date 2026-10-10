//! One ingress endpoint's JSON-RPC client, with the few methods the
//! probes call. An answer is typed once here; an error is either a
//! refusal (a JSON-RPC error object, a definite answer) or unknown (the
//! transport failed or the answer did not parse).

use std::time::Duration;

use alloy_primitives::{Address, B256, Bytes, U64, U256};
use jsonrpsee::core::ClientError;
use jsonrpsee::core::client::ClientT;
use jsonrpsee::http_client::{HttpClient, HttpClientBuilder};
use jsonrpsee::rpc_params;
use serde::Deserialize;
use serde::de::DeserializeOwned;

use crate::config::Endpoint;

/// Why a call has no answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RpcError {
    /// The server answered with a JSON-RPC error object: a definite no.
    Refused { code: i32, message: String },
    /// The transport failed, timed out, or the answer did not parse. The
    /// call can have taken effect.
    Unknown(String),
}

impl std::fmt::Display for RpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Refused { code, message } => write!(f, "refused {code}: {message}"),
            Self::Unknown(detail) => write!(f, "unknown: {detail}"),
        }
    }
}

impl std::error::Error for RpcError {}

impl From<ClientError> for RpcError {
    fn from(e: ClientError) -> Self {
        match e {
            ClientError::Call(obj) => Self::Refused {
                code: obj.code(),
                message: obj.message().to_string(),
            },
            other => Self::Unknown(other.to_string()),
        }
    }
}

/// One log of a receipt.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Log {
    pub address: Address,
    pub topics: Vec<B256>,
    pub data: Bytes,
    /// The log's index in its block: an L1 deposit's position.
    #[serde(default)]
    pub log_index: Option<U64>,
}

/// The fields of a receipt the probes read.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Receipt {
    pub status: U64,
    pub block_number: Option<U64>,
    #[serde(default)]
    pub contract_address: Option<Address>,
    #[serde(default)]
    pub logs: Vec<Log>,
    #[serde(default)]
    pub block_hash: Option<B256>,
    #[serde(default)]
    pub effective_gas_price: Option<U256>,
    #[serde(default)]
    pub priority_fee_per_gas: Option<U256>,
    #[serde(default)]
    pub priority_fee_paid: Option<U256>,
}

impl Receipt {
    /// Whether the transaction succeeded.
    #[must_use]
    pub fn succeeded(&self) -> bool {
        self.status == U64::from(1)
    }

    /// The block number, or zero for a receipt without one.
    #[must_use]
    pub fn block(&self) -> u64 {
        self.block_number.map_or(0, |b| b.to::<u64>())
    }
}

/// The part of an `eth_feeHistory` answer the probes read.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct FeeHistory {
    base_fee_per_gas: Vec<U256>,
}

/// One ingress endpoint.
#[derive(Debug, Clone)]
pub struct Rpc {
    pub endpoint: Endpoint,
    client: HttpClient,
}

impl Rpc {
    /// A client for `endpoint` whose calls give up after `timeout`.
    ///
    /// # Errors
    ///
    /// Returns an error when the URL does not parse.
    pub fn new(endpoint: Endpoint, timeout: Duration) -> anyhow::Result<Self> {
        let client = HttpClientBuilder::default()
            .request_timeout(timeout)
            .build(&endpoint.url)
            .map_err(|e| anyhow::anyhow!("ingress client {}: {e}", endpoint.url))?;
        Ok(Self { endpoint, client })
    }

    async fn call<T: DeserializeOwned>(
        &self,
        method: &str,
        params: jsonrpsee::core::params::ArrayParams,
    ) -> Result<T, RpcError> {
        self.client
            .request(method, params)
            .await
            .map_err(RpcError::from)
    }

    /// `eth_chainId`.
    ///
    /// # Errors
    ///
    /// The call's error.
    pub async fn chain_id(&self) -> Result<u64, RpcError> {
        self.call::<U64>("eth_chainId", rpc_params![])
            .await
            .map(|n| n.to::<u64>())
    }

    /// `eth_blockNumber`.
    ///
    /// # Errors
    ///
    /// The call's error.
    pub async fn block_number(&self) -> Result<u64, RpcError> {
        self.call::<U64>("eth_blockNumber", rpc_params![])
            .await
            .map(|n| n.to::<u64>())
    }

    /// `eth_gasPrice`: the next base fee plus the suggested tip rate.
    ///
    /// # Errors
    ///
    /// The call's error, or a price above `u128`.
    pub async fn gas_price(&self) -> Result<u128, RpcError> {
        let price: U256 = self.call("eth_gasPrice", rpc_params![]).await?;
        u128::try_from(price).map_err(|e| RpcError::Unknown(format!("gas price {price}: {e}")))
    }

    /// `eth_getTransactionCount` at `latest`: the committed nonce.
    ///
    /// # Errors
    ///
    /// The call's error.
    pub async fn nonce(&self, address: Address) -> Result<u64, RpcError> {
        self.call::<U64>("eth_getTransactionCount", rpc_params![address, "latest"])
            .await
            .map(|n| n.to::<u64>())
    }

    /// `eth_getBalance` at `latest`.
    ///
    /// # Errors
    ///
    /// The call's error.
    pub async fn balance(&self, address: Address) -> Result<U256, RpcError> {
        self.call("eth_getBalance", rpc_params![address, "latest"])
            .await
    }

    /// `kardamom_sendRawTransactionAsync`: the hash once the ingress
    /// publishes the transaction.
    ///
    /// # Errors
    ///
    /// The call's error.
    pub async fn send(&self, raw: &Bytes) -> Result<B256, RpcError> {
        self.call("kardamom_sendRawTransactionAsync", rpc_params![raw])
            .await
    }

    /// `kardamom_blockNumberByTag("safe")`: the posted head.
    ///
    /// # Errors
    ///
    /// The call's error.
    pub async fn safe_head(&self) -> Result<u64, RpcError> {
        self.call::<U64>("kardamom_blockNumberByTag", rpc_params!["safe"])
            .await
            .map(|n| n.to::<u64>())
    }

    /// The base fee of block `number`, from `eth_feeHistory`.
    ///
    /// # Errors
    ///
    /// The call's error, or an answer without the block's base fee.
    pub async fn base_fee(&self, number: u64) -> Result<U256, RpcError> {
        let history: FeeHistory = self
            .call(
                "eth_feeHistory",
                rpc_params![U64::from(1), U64::from(number), [50.0]],
            )
            .await?;
        history
            .base_fee_per_gas
            .first()
            .copied()
            .ok_or_else(|| RpcError::Unknown(format!("no base fee for block {number}")))
    }

    /// `eth_maxPriorityFeePerGas`.
    ///
    /// # Errors
    ///
    /// The call's error.
    pub async fn max_priority_fee(&self) -> Result<U256, RpcError> {
        self.call("eth_maxPriorityFeePerGas", rpc_params![]).await
    }

    /// `eth_sendRawTransaction`: the submit of an L1 endpoint.
    ///
    /// # Errors
    ///
    /// The call's error.
    pub async fn send_raw(&self, raw: &Bytes) -> Result<B256, RpcError> {
        self.call("eth_sendRawTransaction", rpc_params![raw]).await
    }

    /// The number of the block the `finalized` tag names on an L1
    /// endpoint.
    ///
    /// # Errors
    ///
    /// The call's error, or a block without a number.
    pub async fn finalized(&self) -> Result<u64, RpcError> {
        let block: serde_json::Value = self
            .call("eth_getBlockByNumber", rpc_params!["finalized", false])
            .await?;
        block["number"]
            .as_str()
            .and_then(|n| u64::from_str_radix(n.trim_start_matches("0x"), 16).ok())
            .ok_or_else(|| RpcError::Unknown("no finalized block number".to_string()))
    }

    /// `eth_getTransactionReceipt`.
    ///
    /// # Errors
    ///
    /// The call's error.
    pub async fn receipt(&self, hash: B256) -> Result<Option<Receipt>, RpcError> {
        self.call("eth_getTransactionReceipt", rpc_params![hash])
            .await
    }
}
