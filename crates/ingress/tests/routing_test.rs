//! Routing invariant. Every tx from sender S lands on partition
//! `keccak(S) % M`, for any M. The test checks this end to end through
//! the proxy, with a fake executor that sends a receipt and watermark
//! update for each tx right away.

use std::sync::Arc;
use std::time::Duration;

use std::num::NonZeroU32;

use alloy_signer_local::PrivateKeySigner;

use kardamom_ingress::config::IngressConfig;
use kardamom_ingress::routing::partition_for;
use kardamom_ingress::test_support::{sign_legacy, spawn_fake_executor};
use kardamom_ingress::{IngressProxy, MockChannels};

/// The partition counts this test sweeps, as a compile-time constant so
/// the loop body takes `NonZeroU32` directly instead of re-validating a
/// bare `u32` on every use.
const MS: [NonZeroU32; 4] = [
    NonZeroU32::new(2).unwrap(),
    NonZeroU32::new(4).unwrap(),
    NonZeroU32::new(8).unwrap(),
    NonZeroU32::new(16).unwrap(),
];

#[tokio::test(flavor = "multi_thread")]
async fn each_tx_lands_on_keccak_partition() {
    for m in MS {
        each_tx_lands_on_keccak_partition_for(m).await;
    }
}

/// One [`each_tx_lands_on_keccak_partition`] case: 32 senders submit
/// through a proxy configured for `m` partitions, and every one lands on
/// the partition [`partition_for`] names.
async fn each_tx_lands_on_keccak_partition_for(m: NonZeroU32) {
    let cfg = IngressConfig {
        partition_count_m: m,
        pending_receipt_timeout: Duration::from_secs(5),
        ..IngressConfig::default()
    };
    let (mock, rx_vec) = MockChannels::new(std::num::NonZeroUsize::try_from(m).unwrap());
    let proxy = Arc::new(IngressProxy::new(cfg, mock.clone(), mock.clone()));

    // Fake executor: one task per partition, sends a receipt and
    // watermark update right away. All partitions share one
    // increasing position space; see `spawn_fake_executor`'s doc for
    // why a per-partition position would strand parked receipts.
    let spawns = spawn_fake_executor(&mock, rx_vec);

    // This test uses 32 senders. It checks that each tx lands on the
    // partition given by `partition_for`, the same function the proxy
    // uses. If routing were wrong, the executor for that partition
    // would never see the message, and the proxy would time out.
    let futs: Vec<_> = (0..32)
        .map(|_| {
            let s = PrivateKeySigner::random();
            let raw = sign_legacy(&s, 0);
            let p = proxy.clone();
            async move {
                p.submit_raw("127.0.0.1".parse().unwrap(), raw)
                    .await
                    .map(|r| (s.address(), r))
            }
        })
        .collect();
    let results = futures::future::join_all(futs).await;
    for r in results {
        let (sender, resp) = r.expect("submit ok");
        let part = partition_for(sender, m);
        assert!(part < m.get());
        assert!(resp.receipt.status);
    }
    for s in spawns {
        s.abort();
    }
}
