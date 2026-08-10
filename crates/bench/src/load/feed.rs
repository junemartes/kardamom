//! Subscribe-mode receipt feed: one WebSocket subscription (filtered to the
//! run's senders) confirming txs into the shared tracker.

use std::sync::Arc;
use std::time::Duration;

use alloy_primitives::Address;
use jsonrpsee::rpc_params;

use crate::load::json_hex_u64;
use crate::load::tracker::Tracker;

/// Subscribe-mode receipt feed: one WebSocket subscription (filtered to the
/// run's senders) confirming txs into the shared tracker. Reconnects forever
/// — chaos restarts the ingress under it — and the drain's HTTP polling
/// settles anything that slipped through a gap. Aborted by the caller.
pub(crate) async fn receipt_feed_task(
    ws_url: String,
    senders: Vec<Address>,
    tracker: Arc<Tracker>,
) {
    use jsonrpsee::core::client::{Subscription, SubscriptionClientT};
    use jsonrpsee::ws_client::WsClientBuilder;

    loop {
        let client = match WsClientBuilder::default().build(&ws_url).await {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(error = %e, "receipt feed: ws connect failed; retrying");
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }
        };
        let sub: Result<Subscription<serde_json::Value>, _> = client
            .subscribe(
                "kardamom_subscribeReceipts",
                rpc_params![Some(senders.clone())],
                "kardamom_unsubscribeReceipts",
            )
            .await;
        let mut sub = match sub {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(error = %e, "receipt feed: subscribe failed; retrying");
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }
        };
        tracing::info!("receipt feed: subscribed");
        while let Some(item) = sub.next().await {
            let Ok(v) = item else { continue };
            match v["type"].as_str() {
                Some("receipt") => {
                    let r = &v["receipt"];
                    let Some(hash) = r["transactionHash"]
                        .as_str()
                        .and_then(|s| s.parse::<alloy_primitives::B256>().ok())
                    else {
                        continue;
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
        tracing::warn!("receipt feed: stream ended; reconnecting");
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}
