//! This is the subscribe-mode receipt feed: one WebSocket subscription,
//! filtered to the run's senders, that confirms transactions into the
//! shared tracker.

use std::sync::Arc;
use std::time::Duration;

use alloy_primitives::Address;
use jsonrpsee::rpc_params;

use crate::load::json_hex_u64;
use crate::load::tracker::Tracker;

/// The subscribe-mode receipt feed: one WebSocket subscription, filtered
/// to the run's senders, that confirms transactions into the shared
/// tracker. It reconnects without limit, because chaos mode restarts the
/// ingress under it, and the drain's HTTP polling settles anything that
/// slipped through a gap. The caller aborts this task.
pub(crate) async fn receipt_feed_task(
    ws_url: String,
    senders: Vec<Address>,
    tracker: Arc<Tracker>,
) {
    loop {
        run_receipt_feed_once(&ws_url, &senders, &tracker).await;
    }
}

/// One connect-subscribe-drain attempt. Connects to `ws_url`, subscribes
/// filtered to `senders`, and drains the subscription into `tracker`
/// until it ends. Returns after a retry delay, on a connect failure, a
/// subscribe failure, or a stream end, so the caller's `loop` always
/// retries.
async fn run_receipt_feed_once(ws_url: &str, senders: &[Address], tracker: &Tracker) {
    use jsonrpsee::core::client::{Subscription, SubscriptionClientT};
    use jsonrpsee::ws_client::WsClientBuilder;

    let client = match WsClientBuilder::default().build(ws_url).await {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(error = %e, "receipt feed: ws connect failed; retrying");
            tokio::time::sleep(Duration::from_millis(500)).await;
            return;
        }
    };
    let sub: Result<Subscription<serde_json::Value>, _> = client
        .subscribe(
            "kardamom_subscribeReceipts",
            rpc_params![Some(senders.to_vec())],
            "kardamom_unsubscribeReceipts",
        )
        .await;
    let mut sub = match sub {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!(error = %e, "receipt feed: subscribe failed; retrying");
            tokio::time::sleep(Duration::from_millis(500)).await;
            return;
        }
    };
    tracing::info!("receipt feed: subscribed");
    drain_receipts(&mut sub, tracker).await;
    tracing::warn!("receipt feed: stream ended; reconnecting");
    tokio::time::sleep(Duration::from_millis(300)).await;
}

/// Drain the subscription until the stream ends, confirming each
/// receipt item into `tracker`.
async fn drain_receipts(
    sub: &mut jsonrpsee::core::client::Subscription<serde_json::Value>,
    tracker: &Tracker,
) {
    while let Some(item) = sub.next().await {
        record_receipt_item(item, tracker);
    }
}

/// Fold one subscription item into `tracker`. Ignores a malformed item:
/// an `Err`, an unrecognized `type`, or a receipt with no parseable
/// `transactionHash`.
fn record_receipt_item(item: Result<serde_json::Value, serde_json::Error>, tracker: &Tracker) {
    let Ok(v) = item else { return };
    match v["type"].as_str() {
        Some("receipt") => {
            let r = &v["receipt"];
            let Some(hash) = r["transactionHash"]
                .as_str()
                .and_then(|s| s.parse::<alloy_primitives::B256>().ok())
            else {
                return;
            };
            let status = json_hex_u64(&r["status"]).unwrap_or(0);
            let gas = json_hex_u64(&r["gasUsed"]).unwrap_or(0);
            tracker.confirm_from_feed(hash, status, gas);
        }
        Some("txError") => {
            tracing::warn!(payload = %v, "receipt feed: sequencer rejection");
        }
        Some("lagged") => {
            tracing::warn!(
                payload = %v,
                "receipt feed: lagged — drain will settle the gap"
            );
        }
        _ => {}
    }
}
