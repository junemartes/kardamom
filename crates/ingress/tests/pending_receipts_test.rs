//! End-to-end timeout test. When no fake executor drains the partition
//! channel, the proxy must time out the client after
//! `pending_receipt_timeout`.

use std::num::NonZeroU32;
use std::time::Duration;

use alloy_signer_local::PrivateKeySigner;

use kardamom_ingress::config::IngressConfig;
use kardamom_ingress::error::IngressError;
use kardamom_ingress::test_support::sign_legacy;
use kardamom_ingress::{IngressProxy, MockChannels};

/// `IngressConfig::partition_count_m` this test starts with.
const SHARDS: NonZeroU32 = NonZeroU32::new(4).unwrap();
/// The shard count `MockChannels` starts with — matches [`SHARDS`].
const MOCK_SHARDS: std::num::NonZeroUsize = std::num::NonZeroUsize::new(4).unwrap();

#[tokio::test]
async fn submit_times_out_when_no_executor_responds() {
    let cfg = IngressConfig {
        partition_count_m: SHARDS,
        pending_receipt_timeout: Duration::from_millis(80),
        ..IngressConfig::default()
    };
    let (mock, _rx) = MockChannels::new(MOCK_SHARDS);
    let proxy = IngressProxy::new(cfg, mock.clone(), mock);

    let signer = PrivateKeySigner::random();
    let raw = sign_legacy(&signer, 0);
    let res = proxy.submit_raw("127.0.0.1".parse().unwrap(), raw).await;
    assert!(matches!(res.unwrap_err(), IngressError::Timeout));
}
