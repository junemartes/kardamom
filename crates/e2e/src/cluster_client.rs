//! RPC-driven cluster e2e scenarios.
//!
//! These drive a **deployed** kardamom nomad cluster from the outside — exactly
//! the way an operator would — over two network endpoints:
//!
//!   * the **ingress JSON-RPC** (`eth_sendRawTransaction` /
//!     `eth_getTransactionReceipt` / `eth_chainId`), and
//!   * the **in-cluster anvil L1** (for the deposit path's `depositETH` call +
//!     `evm_mine` finality nudges).
//!
//! Nothing here touches Aeron: every assertion is observable over plain
//! JSON-RPC because the ingress serves deposit receipts by `source_hash`
//! straight from its in-memory receipt cache (see `crates/ingress`). That is
//! what lets this client replace the old single-host `multiprocess_e2e` test,
//! which had to subscribe to Aeron directly.
//!
//! The ingress deliberately does NOT implement `eth_getTransactionCount`
//! (it errors "deferred to S6 state writer"), so — like the cluster smoke
//! scripts — nonces are managed entirely here, sequential per sender.

use std::str::FromStr;
use std::time::Duration;

use alloy_consensus::{SignableTransaction, TxEnvelope as ConsensusEnvelope, TxLegacy};
use alloy_network::TxSignerSync;
use alloy_primitives::{Address, B256, Bytes, TxKind, U256};
use alloy_provider::{Provider, ProviderBuilder};
use alloy_rlp::{Decodable, Encodable};
use alloy_signer_local::PrivateKeySigner;
use alloy_sol_types::sol;
use anyhow::{Context, Result, anyhow, bail};
use jsonrpsee::core::client::ClientT;
use jsonrpsee::http_client::{HttpClient, HttpClientBuilder};
use jsonrpsee::rpc_params;
use serde_json::Value;

sol! {
    #[sol(rpc)]
    contract ETHLockbox {
        function depositETH(address to, uint64 gasLimit, bytes calldata data) external payable;
    }
}

/// Anvil/Hardhat dev account #0 private key (public, dev-only). Prefunded on
/// the anvil L1 (mnemonic default) AND on L2 via `config/genesis/dev.toml`, so
/// it can both call `depositETH` on L1 and submit signed L2 transfers.
pub const DEV_ACCOUNT_0_KEY: &str =
    "ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";

/// Legacy gas price for L2 txs (1 gwei) — matches `scripts/smoke.sh`.
pub const L2_GAS_PRICE: u128 = 1_000_000_000;

/// Build a signer from a hex private key (with or without the `0x` prefix).
pub fn signer_from_hex(key_hex: &str) -> Result<PrivateKeySigner> {
    let hex = key_hex.strip_prefix("0x").unwrap_or(key_hex);
    PrivateKeySigner::from_str(hex).context("invalid private key")
}

/// Sign a legacy value-transfer and RLP-encode it to the 2718 wire bytes.
pub fn signed_transfer(
    signer: &PrivateKeySigner,
    to: Address,
    value: U256,
    nonce: u64,
    chain_id: u64,
) -> Vec<u8> {
    let mut tx = TxLegacy {
        chain_id: Some(chain_id),
        nonce,
        gas_price: L2_GAS_PRICE,
        gas_limit: 21_000,
        to: to.into(),
        value,
        input: Default::default(),
    };
    encode_signed(signer, &mut tx)
}

/// Sign a legacy contract-creation tx (no `to`, `input` = creation code).
pub fn signed_create_contract(
    signer: &PrivateKeySigner,
    code: &[u8],
    nonce: u64,
    chain_id: u64,
) -> Vec<u8> {
    let mut tx = TxLegacy {
        chain_id: Some(chain_id),
        nonce,
        gas_price: L2_GAS_PRICE,
        gas_limit: 500_000,
        to: TxKind::Create,
        value: U256::ZERO,
        input: code.to_vec().into(),
    };
    encode_signed(signer, &mut tx)
}

fn encode_signed(signer: &PrivateKeySigner, tx: &mut TxLegacy) -> Vec<u8> {
    let sig = signer.sign_transaction_sync(tx).expect("sign tx");
    let env: ConsensusEnvelope = tx.clone().into_signed(sig).into();
    let mut out = Vec::with_capacity(256 + tx.input.len());
    env.encode(&mut out);
    out
}

/// Build an ingress JSON-RPC client with a generous request timeout.
pub fn ingress_client(rpc_url: &str) -> Result<HttpClient> {
    HttpClientBuilder::default()
        .request_timeout(Duration::from_secs(60))
        .build(rpc_url)
        .map_err(|e| anyhow!("build ingress http client: {e}"))
}

/// Poll `eth_chainId` until the ingress answers (or the deadline elapses).
pub async fn wait_for_ingress(client: &HttpClient, timeout: Duration) -> Result<()> {
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        let res: Result<Value, _> = client.request("eth_chainId", rpc_params![]).await;
        if res.is_ok() {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    bail!("timed out waiting for ingress JSON-RPC at {timeout:?}")
}

/// Poll `eth_getTransactionReceipt(hash)` until a non-null receipt object
/// surfaces, then return it. A null result or a transient RPC error is treated
/// as "not yet" and retried until the deadline.
pub async fn poll_receipt(client: &HttpClient, hash: B256, timeout: Duration) -> Result<Value> {
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        match client
            .request::<Value, _>("eth_getTransactionReceipt", rpc_params![hash])
            .await
        {
            Ok(v) if !v.is_null() => return Ok(v),
            _ => tokio::time::sleep(Duration::from_millis(250)).await,
        }
    }
    bail!("no receipt for {hash} within {timeout:?}")
}

fn receipt_status(receipt: &Value) -> &str {
    receipt.get("status").and_then(Value::as_str).unwrap_or("")
}

/// Scenario: submit `count` signed transfers from `signer`, each waiting for a
/// success receipt. Nonces run sequentially from `start_nonce`.
pub async fn run_transfer(
    client: &HttpClient,
    signer: &PrivateKeySigner,
    to: Address,
    count: u64,
    start_nonce: u64,
    chain_id: u64,
) -> Result<()> {
    for i in 0..count {
        let nonce = start_nonce + i;
        let raw = signed_transfer(signer, to, U256::from(1u64), nonce, chain_id);
        let raw_hex = format!("0x{}", hex::encode(&raw));

        let returned: B256 = client
            .request("eth_sendRawTransaction", rpc_params![raw_hex])
            .await
            .with_context(|| format!("eth_sendRawTransaction (transfer nonce {nonce})"))?;

        let expected = *ConsensusEnvelope::decode(&mut raw.as_slice())
            .context("decode signed transfer")?
            .tx_hash();
        if returned != expected {
            bail!("transfer nonce {nonce}: returned hash {returned} != signed {expected}");
        }

        let receipt = poll_receipt(client, returned, Duration::from_secs(60)).await?;
        let status = receipt_status(&receipt);
        if status != "0x1" {
            bail!("transfer nonce {nonce}: receipt status {status} (expected 0x1): {receipt}");
        }
        eprintln!("  transfer nonce {nonce} OK (tx {returned})");
    }
    Ok(())
}

/// Scenario: deploy a tiny contract and assert success + a non-zero
/// `contractAddress`. Uses `nonce` on `signer`.
pub async fn run_contract_deploy(
    client: &HttpClient,
    signer: &PrivateKeySigner,
    nonce: u64,
    chain_id: u64,
) -> Result<()> {
    // Minimal valid creation bytecode returning a 1-byte runtime (0x01):
    //   60 01 60 0c 60 00 39 60 01 60 00 f3 01
    let creation = hex::decode("6001600c60003960016000f301").expect("decode creation bytecode");
    let raw = signed_create_contract(signer, &creation, nonce, chain_id);
    let raw_hex = format!("0x{}", hex::encode(&raw));

    let returned: B256 = client
        .request("eth_sendRawTransaction", rpc_params![raw_hex])
        .await
        .context("eth_sendRawTransaction (deploy)")?;

    let receipt = poll_receipt(client, returned, Duration::from_secs(60)).await?;
    let status = receipt_status(&receipt);
    if status != "0x1" {
        bail!("contract deploy: receipt status {status} (expected 0x1): {receipt}");
    }
    let contract_address = receipt
        .get("contractAddress")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if !contract_address.starts_with("0x")
        || contract_address == "0x0000000000000000000000000000000000000000"
    {
        bail!("contract deploy: contractAddress not set ({contract_address})");
    }
    eprintln!("  contract deploy OK (address {contract_address})");
    Ok(())
}

/// Scenario: full L1 → L2 deposit round-trip.
///
/// Calls `ETHLockbox.depositETH` on the anvil L1 as `signer`, derives the
/// expected L2 `source_hash` from the `DepositInitiated` log, nudges the L1
/// `finalized` tag forward (the da-watcher only reads finalized blocks), then
/// polls the ingress for the minted deposit receipt keyed by `source_hash`.
pub async fn run_deposit(
    ingress: &HttpClient,
    l1_rpc: &str,
    signer: &PrivateKeySigner,
    lockbox: Address,
    l2_target: Address,
) -> Result<()> {
    const MINT_WEI: u128 = 50_000_000_000_000_000_000; // 50 ETH

    let l1 = ProviderBuilder::new()
        .wallet(signer.clone())
        .connect_http(l1_rpc.parse().context("parse l1 rpc url")?);

    let lockbox_contract = ETHLockbox::new(lockbox, l1.clone());
    let l1_receipt = lockbox_contract
        .depositETH(l2_target, 200_000u64, Bytes::new())
        .value(U256::from(MINT_WEI))
        .send()
        .await
        .context("send depositETH")?
        .get_receipt()
        .await
        .context("L1 depositETH receipt")?;

    let log = l1_receipt
        .inner
        .logs()
        .iter()
        .find(|l| l.address() == lockbox)
        .context("no DepositInitiated log on lockbox")?;
    let block_hash = log.block_hash.context("deposit log missing block_hash")?;
    let log_index = log.log_index.context("deposit log missing log_index")?;
    let expected = kardamom_da_watcher::source_hash(block_hash, log_index);
    eprintln!("  L1 deposit submitted; expecting L2 source_hash {expected}");

    // Advance the L1 `finalized` tag past the deposit's block. Anvil runs with
    // `--slots-in-an-epoch 1`, so finalised lags `latest` by ~2 blocks; mining
    // 6 comfortably covers it.
    for _ in 0..6 {
        let _: Value = l1
            .raw_request("evm_mine".into(), ())
            .await
            .context("evm_mine")?;
    }

    let receipt = poll_receipt(ingress, expected, Duration::from_secs(90)).await?;
    let status = receipt_status(&receipt);
    if status != "0x1" {
        bail!("deposit: L2 receipt status {status} (expected 0x1): {receipt}");
    }
    let to = receipt
        .get("to")
        .and_then(Value::as_str)
        .and_then(|s| Address::from_str(s).ok())
        .context("deposit receipt missing `to`")?;
    if to != l2_target {
        bail!("deposit: receipt.to {to} != l2_target {l2_target}");
    }
    let egp = receipt
        .get("effectiveGasPrice")
        .and_then(Value::as_str)
        .unwrap_or("");
    if egp != "0x0" && egp != "0x00" {
        bail!("deposit: effectiveGasPrice {egp} (deposits pay no fee, expected 0x0)");
    }
    eprintln!("  L1 → L2 deposit OK (source_hash {expected}, to {l2_target})");
    Ok(())
}
