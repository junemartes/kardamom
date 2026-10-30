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

/// The executor head lags the sealer by more than the horizon, as after a
/// pipeline restart while the executor catches up. Every new submit still
/// gets a deadline past the sealer's own block: the sealed head of the
/// sealer's status, not the executor head, is the base.
#[tokio::test(flavor = "multi_thread")]
async fn a_lagging_executor_head_does_not_age_the_deadline_of_a_new_submit() {
    let cfg = IngressConfig {
        partition_count_m: NonZeroU32::MIN,
        pending_receipt_timeout: Duration::from_secs(30),
        ..IngressConfig::default()
    };
    let horizon = cfg.inclusion_horizon_blocks.get();
    let (mock, mut rx) = MockChannels::new(NonZeroUsize::MIN);
    let proxy = Arc::new(IngressProxy::new(cfg, mock.clone(), mock.clone()));
    let executor_head = 10;
    mock.send_boundary(kardamom_types::BlockBoundary {
        block_number: executor_head,
        ..kardamom_types::BlockBoundary::default()
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        while proxy.latest_block_number() != executor_head {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the proxy takes the executor boundary");
    for sealer_head in [executor_head + 3 * horizon, executor_head + 5 * horizon] {
        mock.cluster_status_bus.send_replace(ClusterStatus {
            sealed_head: sealer_head,
            ..ClusterStatus::default()
        });
        let deadline = stamped_deadline(&proxy, &mut rx[0]).await;
        assert_eq!(
            deadline,
            sealer_head + horizon,
            "the executor head {executor_head} lags the sealer head {sealer_head}; the deadline must follow the sealer"
        );
        assert!(
            deadline > sealer_head,
            "the sealer would refuse a deadline at or below its block"
        );
    }
}
