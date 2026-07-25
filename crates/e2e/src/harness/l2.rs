//! JSON-RPC client + transaction signing for the scenario drivers.
//!
//! Thin wrapper over jsonrpsee's HTTP client that (a) surfaces JSON-RPC error
//! codes as data (the scenarios assert on them) and (b) measures per-call
//! latency (the "RPC never hangs" assertions are latency bounds).
//!
//! Signing reuses `kardamom_bench`'s mnemonic derivation (the anvil dev
//! mnemonic funded by `deploy/cluster/config/genesis/dev.toml`) and the exact
//! `TxLegacy → sign → encode_2718` pattern of `kardamom-load`.

use std::time::{Duration, Instant};

use alloy_consensus::{SignableTransaction, TxEnvelope, TxLegacy};
use alloy_eips::eip2718::Encodable2718;
use alloy_network::TxSignerSync;
use alloy_primitives::{Address, B256, Bytes, TxKind, U256};
use anyhow::{Context, Result};
use jsonrpsee::core::client::ClientT;
use jsonrpsee::http_client::{HttpClient, HttpClientBuilder};
use jsonrpsee::rpc_params;
use kardamom_bench::mnemonic;
pub use kardamom_bench::signers::DerivedSigner;

/// The anvil/hardhat dev mnemonic; accounts #0..#17 are prefunded by
/// `deploy/cluster/config/genesis/dev.toml`.
pub const DEV_MNEMONIC: &str = "test test test test test test test test test test test junk";

/// Outcome of a JSON-RPC call, with the wall-clock latency of the round trip.
#[derive(Debug)]
pub struct RpcOutcome<T> {
    pub result: std::result::Result<T, RpcError>,
    pub elapsed: Duration,
}

/// A JSON-RPC layer error: either a server-side error object (code +
/// message) or a transport-level failure (timeout, refused connection, …).
#[derive(Debug, Clone)]
pub enum RpcError {
    /// The server answered with a JSON-RPC error object.
    Call { code: i32, message: String },
    /// The call failed below the JSON-RPC layer (connect refused, client
    /// timeout, connection dropped).
    Transport(String),
}

impl std::fmt::Display for RpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RpcError::Call { code, message } => write!(f, "rpc error {code}: {message}"),
            RpcError::Transport(m) => write!(f, "transport error: {m}"),
        }
    }
}

fn classify(e: jsonrpsee::core::client::Error) -> RpcError {
    match e {
        jsonrpsee::core::client::Error::Call(obj) => RpcError::Call {
            code: obj.code(),
            message: obj.message().to_string(),
        },
        other => RpcError::Transport(other.to_string()),
    }
}

/// JSON-RPC client bound to one ingress endpoint.
#[derive(Clone)]
pub struct L2Client {
    http: HttpClient,
    pub url: String,
}

impl L2Client {
    /// `request_timeout` bounds every call at the client; scenarios set it
    /// ABOVE the ingress-side pending-receipt timeout so a server-side
    /// timeout error is distinguishable from a client-side abort.
    pub fn new(url: &str, request_timeout: Duration) -> Result<Self> {
        let http = HttpClientBuilder::default()
            .request_timeout(request_timeout)
            .max_concurrent_requests(4096)
            .build(url)
            .with_context(|| format!("build http client for {url}"))?;
        Ok(Self {
            http,
            url: url.to_string(),
        })
    }

    async fn call<T: jsonrpsee::core::DeserializeOwned>(
        &self,
        method: &str,
        params: jsonrpsee::core::params::ArrayParams,
    ) -> RpcOutcome<T> {
        let start = Instant::now();
        let result = self
            .http
            .request::<T, _>(method, params)
            .await
            .map_err(classify);
        RpcOutcome {
            result,
            elapsed: start.elapsed(),
        }
    }

    /// `eth_sendRawTransaction`. Blocks (server-side) until the receipt lands
    /// or the ingress pending-receipt timeout fires.
    pub async fn send_raw(&self, raw: &Bytes) -> RpcOutcome<B256> {
        self.call(
            "eth_sendRawTransaction",
            rpc_params![format!("0x{}", hex::encode(raw))],
        )
        .await
    }

    /// `eth_getTransactionReceipt` — `None` on cache miss.
    pub async fn receipt(&self, hash: B256) -> RpcOutcome<Option<serde_json::Value>> {
        self.call("eth_getTransactionReceipt", rpc_params![format!("{hash}")])
            .await
    }

    pub async fn chain_id(&self) -> RpcOutcome<String> {
        self.call("eth_chainId", rpc_params![]).await
    }

    pub async fn block_number(&self) -> RpcOutcome<String> {
        self.call("eth_blockNumber", rpc_params![]).await
    }

    /// `eth_getBalance` — expected to answer with a clean error (deferred
    /// endpoint), never to hang; kept for the S5 liveness matrix.
    pub async fn get_balance(&self, addr: Address) -> RpcOutcome<String> {
        self.call("eth_getBalance", rpc_params![format!("{addr}"), "latest"])
            .await
    }

    /// `eth_getTransactionCount` — same deferred-endpoint contract.
    pub async fn get_transaction_count(&self, addr: Address) -> RpcOutcome<String> {
        self.call(
            "eth_getTransactionCount",
            rpc_params![format!("{addr}"), "latest"],
        )
        .await
    }
}

/// Derive `count` dev signers (mnemonic index 0..count).
pub fn dev_signers(count: u32) -> Result<Vec<DerivedSigner>> {
    mnemonic::derive_signers(DEV_MNEMONIC, count)
}

/// A signed legacy transfer ready for `eth_sendRawTransaction`.
#[derive(Debug, Clone)]
pub struct SignedTransfer {
    pub raw: Bytes,
    pub hash: B256,
    pub sender: Address,
    pub nonce: u64,
}

/// Sign a 21k-gas legacy value transfer (the `kardamom-load` shape).
/// `value_wei` perturbs the payload so two txs at the same nonce can be
/// distinguished (the S5 past-nonce probe needs a *different* tx, not an
/// idempotent resubmit).
pub fn sign_transfer(
    signer: &DerivedSigner,
    chain_id: u64,
    nonce: u64,
    to: Address,
    value_wei: u64,
) -> Result<SignedTransfer> {
    let mut tx = TxLegacy {
        chain_id: Some(chain_id),
        nonce,
        gas_price: 1_000_000_000,
        gas_limit: 21_000,
        to: TxKind::Call(to),
        value: U256::from(value_wei),
        input: Bytes::new(),
    };
    let sig = signer
        .signer
        .sign_transaction_sync(&mut tx)
        .with_context(|| format!("sign transfer nonce {nonce}"))?;
    let signed = tx.into_signed(sig);
    let hash = *signed.hash();
    let envelope: TxEnvelope = signed.into();
    let mut bytes = Vec::with_capacity(110);
    envelope.encode_2718(&mut bytes);
    Ok(SignedTransfer {
        raw: Bytes::from(bytes),
        hash,
        sender: signer.address,
        nonce,
    })
}

/// Sign a legacy contract-creation tx. `init_code` is the deployment
/// bytecode; 100k gas covers the trivial creates the scenarios deploy.
pub fn sign_create(
    signer: &DerivedSigner,
    chain_id: u64,
    nonce: u64,
    init_code: &[u8],
) -> Result<SignedTransfer> {
    let mut tx = TxLegacy {
        chain_id: Some(chain_id),
        nonce,
        gas_price: 1_000_000_000,
        gas_limit: 100_000,
        to: TxKind::Create,
        value: U256::ZERO,
        input: Bytes::copy_from_slice(init_code),
    };
    let sig = signer
        .signer
        .sign_transaction_sync(&mut tx)
        .with_context(|| format!("sign create nonce {nonce}"))?;
    let signed = tx.into_signed(sig);
    let hash = *signed.hash();
    let envelope: TxEnvelope = signed.into();
    let mut bytes = Vec::with_capacity(160);
    envelope.encode_2718(&mut bytes);
    Ok(SignedTransfer {
        raw: Bytes::from(bytes),
        hash,
        sender: signer.address,
        nonce,
    })
}

/// Sign a legacy call to `to` carrying `value` and `input` (contract calls
/// such as the withdrawal predeploy's `initiateWithdrawal`).
pub fn sign_call(
    signer: &DerivedSigner,
    chain_id: u64,
    nonce: u64,
    to: Address,
    value: U256,
    input: &[u8],
) -> Result<SignedTransfer> {
    let mut tx = TxLegacy {
        chain_id: Some(chain_id),
        nonce,
        gas_price: 1_000_000_000,
        gas_limit: 200_000,
        to: TxKind::Call(to),
        value,
        input: Bytes::copy_from_slice(input),
    };
    let sig = signer
        .signer
        .sign_transaction_sync(&mut tx)
        .with_context(|| format!("sign call nonce {nonce}"))?;
    let signed = tx.into_signed(sig);
    let hash = *signed.hash();
    let envelope: TxEnvelope = signed.into();
    let mut bytes = Vec::with_capacity(200);
    envelope.encode_2718(&mut bytes);
    Ok(SignedTransfer {
        raw: Bytes::from(bytes),
        hash,
        sender: signer.address,
        nonce,
    })
}

/// Deterministic in-place Fisher–Yates shuffle (xorshift64*), so scenario
/// orderings are reproducible from a seed without pulling in `rand`.
pub fn seeded_shuffle<T>(items: &mut [T], mut seed: u64) {
    debug_assert!(seed != 0, "xorshift seed must be non-zero");
    let mut next = move || {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed
    };
    for i in (1..items.len()).rev() {
        let j = (next() % (i as u64 + 1)) as usize;
        items.swap(i, j);
    }
}
