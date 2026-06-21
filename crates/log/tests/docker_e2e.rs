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

#![cfg(feature = "docker-e2e")]

use std::rc::Rc;
use std::time::Duration;

use kardamom_log::config::LogConfig;
use kardamom_log::publisher::TxOrderingPublisher;
use kardamom_log::subscriber::Subscribers;
use kardamom_log::testing::AeronTestCluster;
use kardamom_types::{BPosition, TxOrderingMessage, TxRef};

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
    let aeron_dir = cluster.aeron_dir_host(0).to_string_lossy().to_string();
    eprintln!("aeron archive control: {endpoint}");
    eprintln!("aeron.dir (host): {aeron_dir}");

    // tx_ordering rides IPC over the bind-mounted aeron.dir — the host client
    // and the container's Media Driver share the same shared-memory directory,
    // so no UDP is required (the archive-control endpoint can't be reused as
    // a data endpoint anyway). Same pattern as `aeron_live_e2e_arch_v2.rs`.
    let mut cfg = LogConfig::default();
    cfg.channels.tx_ordering_channel = "aeron:ipc?alias=b".to_string();
    cfg.channels.tx_ordering_stream_id = 1001;

    // Connect a host-side Aeron client to the container's Media Driver via
    // the bind-mounted aeron.dir; without `set_dir`, rusteron defaults to
    // `/dev/shm/aeron-dev` which the container does not expose.
    let aeron_dir_c =
        std::ffi::CString::new(aeron_dir.clone()).expect("aeron.dir contains a NUL byte");
    let ctx = rusteron_client::AeronContext::new().expect("aeron context");
    ctx.set_dir(aeron_dir_c.as_c_str())
        .expect("aeron context set_dir");
    // `Aeron` is `!Send + !Sync` (the C client is thread-confined). Use
    // `Rc`, not `Arc`, to share it between the publisher and Subscribers.
    let aeron = Rc::new(rusteron_client::Aeron::new(&ctx).expect("aeron connect to container"));
    aeron.start().expect("aeron start");

    let pubr = TxOrderingPublisher::open(&aeron, &cfg.channels).unwrap();
    let subs = Subscribers {
        aeron: aeron.clone(),
        ch: cfg.channels.clone(),
    };
    let mut sub = subs.tx_ordering().unwrap();

    // TxOrdering now carries TxRefs, not TxEnvelopes. Publish 100
    // refs (alternating which sequencer they belong to) and assert the
    // subscriber sees them all in the canonical order Aeron produced.
    let mut last_pos = BPosition::ZERO;
    for i in 0..100u64 {
        last_pos = pubr
            .publish_ref(&TxRef {
                tx_hash: alloy_primitives::B256::ZERO,
                shard_id: (i % 4) as u8,
                tx_data_position: BPosition {
                    term_id: 0,
                    term_offset: (i as i32) * 64,
                },
                tx_data_session_id: 0,
            })
            .unwrap();
    }
    assert!(last_pos > BPosition::ZERO);

    let mut received = 0usize;
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while received < 100 && std::time::Instant::now() < deadline {
        received += sub.poll(
            |m, _pos| {
                assert!(matches!(m, TxOrderingMessage::TxRef(_)));
            },
            256,
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(received, 100, "expected 100 refs, got {received}");

    drop(cluster);
}
