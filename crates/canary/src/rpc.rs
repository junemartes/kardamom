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
#[derive(Debug, Clone, Deserialize)]
pub struct Log {
    pub address: Address,
    pub topics: Vec<B256>,
    pub data: Bytes,
}

/// The fields of a receipt the probes read.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Receipt {
    pub status: U64,
    pub block_number: Option<U64>,
    #[serde(default)]
    pub contract_address: Option<Address>,
    #[serde(default)]
    pub logs: Vec<Log>,
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
