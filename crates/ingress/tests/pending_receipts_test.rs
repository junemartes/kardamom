//! End-to-end timeout: with no fake executor draining the partition
//! channel, the proxy must time out the client after
//! `pending_receipt_timeout`.

mod common;

use std::sync::Arc;
use std::time::Duration;

use alloy_signer_local::PrivateKeySigner;

use ingress::config::IngressConfig;
use ingress::error::IngressError;
use ingress::{InMemoryStateDb, IngressProxy, MockChannels};

#[tokio::test]
async fn submit_times_out_when_no_executor_responds() {
    let cfg = IngressConfig {
        partition_count_m: 4,
        pending_receipt_timeout: Duration::from_millis(80),
        ..IngressConfig::default()
    };
    let (mock, _rx) = MockChannels::new(4);
    let state_db = Arc::new(InMemoryStateDb::new());
    let proxy = IngressProxy::new(cfg, mock.clone(), mock, state_db);

    let signer = PrivateKeySigner::random();
    let raw = common::sign_legacy(&signer, 0);
    let res = proxy.submit_raw("127.0.0.1".parse().unwrap(), raw).await;
    assert!(matches!(res.unwrap_err(), IngressError::Timeout));
}
