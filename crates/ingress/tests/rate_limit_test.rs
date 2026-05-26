//! Per-IP rate-limit integration. Drives the proxy's `submit_raw` entry
//! point with garbage bytes and asserts that `IngressError::RateLimited`
//! surfaces after the burst is exhausted.

use std::sync::Arc;
use std::time::Duration;

use alloy_primitives::Bytes;
use nonzero_ext::nonzero;

use kardamom_ingress::config::IngressConfig;
use kardamom_ingress::error::IngressError;
use kardamom_ingress::{InMemoryStateDb, IngressProxy, MockChannels};

#[tokio::test]
async fn third_call_from_same_ip_is_rate_limited() {
    let cfg = IngressConfig {
        rate_limit_per_ip_per_sec: nonzero!(1u32),
        rate_limit_burst: nonzero!(2u32),
        ..IngressConfig::default()
    };
    let (mock, _rx) = MockChannels::new(8);
    let state_db = Arc::new(InMemoryStateDb::new());
    let proxy = IngressProxy::new(cfg, mock.clone(), mock, state_db);

    let ip = "10.0.0.7".parse().unwrap();
    let garbage = Bytes::from(vec![0xc0u8]);
    // First two pass the limiter (then fail at decode).
    let r1 = proxy.submit_raw(ip, garbage.clone()).await;
    assert!(matches!(r1.unwrap_err(), IngressError::Decode(_)));
    let r2 = proxy.submit_raw(ip, garbage.clone()).await;
    assert!(matches!(r2.unwrap_err(), IngressError::Decode(_)));
    // Third in the same burst window is rate-limited.
    let r3 = proxy.submit_raw(ip, garbage.clone()).await;
    assert!(matches!(r3.unwrap_err(), IngressError::RateLimited(_)));

    // After ~1.1s the per-sec replenishment should let it through again.
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let r4 = proxy.submit_raw(ip, garbage).await;
    assert!(matches!(r4.unwrap_err(), IngressError::Decode(_)));
}

#[tokio::test]
async fn other_ips_unaffected_by_first_ips_throttle() {
    let cfg = IngressConfig {
        rate_limit_per_ip_per_sec: nonzero!(1u32),
        rate_limit_burst: nonzero!(1u32),
        ..IngressConfig::default()
    };
    let (mock, _rx) = MockChannels::new(8);
    let state_db = Arc::new(InMemoryStateDb::new());
    let proxy = IngressProxy::new(cfg, mock.clone(), mock, state_db);
    let garbage = Bytes::from(vec![0xc0u8]);
    let ip_a = "10.0.0.1".parse().unwrap();
    let ip_b = "10.0.0.2".parse().unwrap();
    let _ = proxy.submit_raw(ip_a, garbage.clone()).await;
    let r = proxy.submit_raw(ip_a, garbage.clone()).await;
    assert!(matches!(r.unwrap_err(), IngressError::RateLimited(_)));
    let r2 = proxy.submit_raw(ip_b, garbage).await;
    assert!(matches!(r2.unwrap_err(), IngressError::Decode(_)));
}
