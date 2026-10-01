//! `tx_status_feed_shows_every_stage`.
//!
//! A client subscribes to the notifier's status feed for its sender,
//! sends transfers, and sees the three stages of each one, `offered`,
//! `sealed` and `executed`, within the receipt's latency. The replay
//! covers a client that subscribes after the submit: the second half of
//! the transfers goes out before the subscription opens.

use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::time::Duration;

use alloy_primitives::{Address, B256};
use anyhow::{Context, Result};
use jsonrpsee::core::client::{Subscription, SubscriptionClientT};
use jsonrpsee::rpc_params;
use jsonrpsee::ws_client::WsClientBuilder;

use super::Target;
use crate::harness::l2;

pub struct Params {
    /// Transfers sent before the subscription opens, and again after.
    pub txs_per_half: NonZeroUsize,
    /// The dev-mnemonic account index of the sender.
    pub sender_index: usize,
}

impl Default for Params {
    fn default() -> Self {
        Self {
            txs_per_half: NonZeroUsize::new(4).unwrap(),
            sender_index: 1,
        }
    }
}

/// The stage words, in the order a transaction passes them.
const STAGES: [&str; 3] = ["offered", "sealed", "executed"];

/// # Errors
/// Returns an error when a transfer fails to sign, send or land, when
/// the subscription fails, or when a stage is missing or out of order.
pub async fn run(t: &Target, ws_url: &str, p: Params) -> Result<()> {
    let signer = l2::dev_signers_through(p.sender_index)?
        .pop()
        .context("sender signer")?;
    let sender = signer.address;
    let to = Address::from([0x52u8; 20]);
    let half = u64::try_from(p.txs_per_half.get()).context("txs_per_half fits a u64")?;

    let mut hashes = send_transfers(t, &signer, to, 0, half).await?;

    let client = WsClientBuilder::default()
        .build(ws_url)
        .await
        .context("connect to the notifier feed")?;
    let mut sub: Subscription<serde_json::Value> = client
        .subscribe(
            "kardamom_subscribeTxStatus",
            rpc_params![serde_json::json!({ "sender": sender })],
            "kardamom_unsubscribeTxStatus",
        )
        .await
        .context("subscribe to the status feed")?;

    hashes.extend(send_transfers(t, &signer, to, half, half).await?);

    // Every transfer has its receipt, so every stage was observable
    // before the last submit returned. The feed delivers them within
    // the receipt's latency; the park bound is the generous version of
    // that.
    let stages = collect_stages(&mut sub, &hashes, t.pending_receipt_timeout).await?;
    for hash in &hashes {
        let seen = stages.get(hash).cloned().unwrap_or_default();
        anyhow::ensure!(
            seen.iter().map(String::as_str).eq(STAGES),
            "{hash:#x}: stages {seen:?}, expected {STAGES:?}"
        );
    }
    Ok(())
}

/// Send `count` transfers from `nonce` on, each parked until its
/// receipt. Returns their hashes.
async fn send_transfers(
    t: &Target,
    signer: &l2::DerivedSigner,
    to: Address,
    nonce: u64,
    count: u64,
) -> Result<Vec<B256>> {
    let end = nonce.checked_add(count).context("nonce range overflows")?;
    let mut hashes = Vec::new();
    for n in nonce..end {
        let tx = l2::sign_transfer(signer, t.chain_id, n, to, 1)?;
        let out = t.rpc.send_raw(&tx.raw).await;
        let hash = out
            .result
            .map_err(|e| anyhow::anyhow!("send transfer nonce {n}: {e:?}"))?;
        anyhow::ensure!(hash == tx.hash, "the ingress returned another hash");
        hashes.push(hash);
    }
    Ok(hashes)
}

/// Drain the feed until every hash has its three stages, or `patience`
/// passes. Returns the stage words per hash, in arrival order.
async fn collect_stages(
    sub: &mut Subscription<serde_json::Value>,
    hashes: &[B256],
    patience: Duration,
) -> Result<HashMap<B256, Vec<String>>> {
    let mut stages: HashMap<B256, Vec<String>> = HashMap::new();
    let deadline = tokio::time::Instant::now() + patience;
    while !complete(&stages, hashes) {
        let item = tokio::time::timeout_at(deadline, sub.next())
            .await
            .context("the feed went quiet before every stage arrived")?
            .context("the feed ended")?
            .context("a feed item did not parse")?;
        record(&mut stages, &item);
    }
    Ok(stages)
}

fn complete(stages: &HashMap<B256, Vec<String>>, hashes: &[B256]) -> bool {
    hashes
        .iter()
        .all(|h| stages.get(h).is_some_and(|s| s.len() >= STAGES.len()))
}

/// Fold one feed item in. A lag marker or an unknown hash is skipped.
fn record(stages: &mut HashMap<B256, Vec<String>>, item: &serde_json::Value) {
    let Some(hash) = item["tx_hash"]
        .as_str()
        .and_then(|s| s.parse::<B256>().ok())
    else {
        return;
    };
    let Some(stage) = item["stage"].as_str() else {
        return;
    };
    stages.entry(hash).or_default().push(stage.to_string());
}
