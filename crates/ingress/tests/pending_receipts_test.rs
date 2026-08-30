//! End-to-end timeout test. When no fake executor drains the partition
//! channel, the proxy must time out the client after
//! `pending_receipt_timeout`.

mod common;

use std::time::Duration;

use alloy_signer_local::PrivateKeySigner;

use kardamom_ingress::config::IngressConfig;
use kardamom_ingress::error::IngressError;
use kardamom_ingress::{IngressProxy, MockChannels};

#[tokio::test]
async fn submit_times_out_when_no_executor_responds() {
    let cfg = IngressConfig {
        partition_count_m: 4,
        pending_receipt_timeout: Duration::from_millis(80),
        ..IngressConfig::default()
    };
    let (mock, _rx) = MockChannels::new(4);
    let proxy = IngressProxy::new(cfg, mock.clone(), mock);

    let signer = PrivateKeySigner::random();
    let raw = common::sign_legacy(&signer, 0);
    let res = proxy.submit_raw("127.0.0.1".parse().unwrap(), raw).await;
    assert!(matches!(res.unwrap_err(), IngressError::Timeout));
}
