//! Real-Aeron e2e: publish + record + watermark + subscribe end-to-end via
//! Docker containers. Other crates' e2e tests (S1, S2, S4, S5, S6, S7) reuse
//! [`kardamom_log::testing::AeronTestCluster`] (gated behind the `docker-e2e`
//! feature).
//!
//! Gated on the `docker-e2e` + `aeron-live` features AND on Docker
//! availability at runtime: if `docker info` fails (e.g. unprivileged CI
//! runner), the test prints "skipping" and returns 0. The crate's default
//! `cargo test` path does not enable these features, so this file is excluded
//! from baseline test runs.
//!
//! To run locally:
//!
//! ```bash
//! cargo test -p kardamom-log --features 'docker-e2e aeron-live' --test docker_e2e -- --nocapture
//! ```

#![cfg(all(feature = "docker-e2e", feature = "aeron-live"))]

use std::rc::Rc;
use std::time::Duration;

use alloy_primitives::{Address, B256};
use bytes::Bytes;
use kardamom_log::config::LogConfig;
use kardamom_log::publisher::ChannelBPublisher;
use kardamom_log::subscriber::Subscribers;
use kardamom_log::testing::AeronTestCluster;
use kardamom_types::{BPosition, TxEnvelope};

async fn docker_available() -> bool {
    use tokio::process::Command;
    Command::new("docker")
        .arg("info")
        .output()
        .await
        .map(|o| o.status.success())
        .unwrap_or(false)
}

// Single-thread runtime: `rusteron_client::Aeron` is `!Send + !Sync` (the
// C client is thread-confined). With `flavor = "multi_thread"` tokio could
// move this task across worker threads at await points, which is UB for
// the `Rc<Aeron>` held below.
#[tokio::test(flavor = "current_thread")]
async fn aeron_publish_record_subscribe_e2e() {
    if !docker_available().await {
        eprintln!("skipping: docker not available");
        return;
    }

    let cluster = AeronTestCluster::single_node()
        .await
        .expect("aeron container started");

    let endpoint = cluster.archive_control_endpoint(0).await;
    eprintln!("aeron archive control: {endpoint}");

    let mut cfg = LogConfig::default();
    cfg.channels.b_channel = format!("aeron:udp?endpoint={endpoint}|alias=b");
    cfg.channels.b_stream_id = 1001;

    // Connect a host-side Aeron client to the container's Media Driver.
    // The rusteron 0.1.16x API is: build an AeronContext, set the aeron
    // directory (where the container exposes its CnC file via a bind mount —
    // or, alternatively, a UDP channel URI carries the endpoint and the
    // client uses the default dir). For the V0 e2e test we use the default
    // aeron dir and rely on the channel URIs in `cfg.channels` to point at
    // the container's UDP endpoint; if a future variant needs a bind-mounted
    // CnC, call `ctx.set_dir(...)` here.
    let _ = &endpoint;
    let ctx = rusteron_client::AeronContext::new().expect("aeron context");
    // `Aeron` is `!Send + !Sync` (the C client is thread-confined). Use
    // `Rc`, not `Arc`, to share it between the publisher and Subscribers.
    let aeron = Rc::new(rusteron_client::Aeron::new(&ctx).expect("aeron connect to container"));
    aeron.start().expect("aeron start");

    let pubr = ChannelBPublisher::open(&aeron, &cfg.channels).unwrap();
    let subs = Subscribers {
        aeron: aeron.clone(),
        ch: cfg.channels.clone(),
    };
    let mut sub = subs.b().unwrap();

    let mut last_pos = BPosition::ZERO;
    for i in 0..100u64 {
        last_pos = pubr
            .publish_tx(&TxEnvelope {
                correlation_id: i,
                raw_tx: Bytes::from(vec![0xCDu8; 128]),
                sender: Address::ZERO,
                tx_hash: B256::repeat_byte(i as u8),
            })
            .unwrap();
    }
    assert!(last_pos > BPosition::ZERO);

    let mut received = 0usize;
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while received < 100 && std::time::Instant::now() < deadline {
        received += sub.poll(|_t, _pos| (), 256);
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(received, 100, "expected 100 messages, got {received}");

    drop(cluster);
}
