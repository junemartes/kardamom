//! The inclusion deadline moves with the sealer's clock.
//!
//! With every executor down, no executor boundary arrives, but the sealer
//! keeps sealing blocks. The proxy must stamp its deadline from the sealed
//! head of the sealer's status, or every new transaction is past its
//! deadline when it reaches the sealer.

use std::num::{NonZeroU32, NonZeroUsize};
use std::sync::Arc;
use std::time::Duration;

use kardamom_ingress::config::{IngressConfig, NO_DEADLINE};
use kardamom_ingress::test_support::sign_legacy;
use kardamom_ingress::{IngressProxy, MockChannels};
use kardamom_types::ClusterStatus;

/// Submit nonce `nonce` from a fresh sender and return the deadline of
/// the envelope the proxy publishes. No receipt comes, so the submit
/// itself parks and is dropped.
async fn stamped_deadline(
    proxy: &Arc<IngressProxy<MockChannels, MockChannels>>,
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<kardamom_types::TxEnvelope>,
) -> u64 {
    let raw = sign_legacy(&alloy_signer_local::PrivateKeySigner::random(), 0);
    let p = proxy.clone();
    let submit = tokio::spawn(async move { p.submit_raw("127.0.0.1".parse().unwrap(), raw).await });
    let envelope = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("the proxy publishes the envelope")
        .expect("the tx_data channel is open");
    submit.abort();
    envelope.max_inclusion_block
}

#[tokio::test(flavor = "multi_thread")]
async fn the_deadline_follows_the_sealed_head_without_executor_boundaries() {
    let cfg = IngressConfig {
        partition_count_m: NonZeroU32::MIN,
        pending_receipt_timeout: Duration::from_secs(30),
        ..IngressConfig::default()
    };
    let horizon = cfg.inclusion_horizon_blocks.get();
    let (mock, mut rx) = MockChannels::new(NonZeroUsize::MIN);
    let proxy = Arc::new(IngressProxy::new(cfg, mock.clone(), mock.clone()));

    assert_eq!(
        stamped_deadline(&proxy, &mut rx[0]).await,
        NO_DEADLINE,
        "a proxy that has seen no block stamps no deadline"
    );

    mock.cluster_status_bus.send_replace(ClusterStatus {
        sealed_head: 500,
        ..ClusterStatus::default()
    });
    assert_eq!(
        stamped_deadline(&proxy, &mut rx[0]).await,
        500 + horizon,
        "the sealed head moves the deadline with no executor boundary"
    );
}
