//! Regression test for the cluster `tx_ordering` freeze: a back-pressured
//! publish must not starve subscription polling on the shared Aeron thread.
//!
//! ## The bug this guards against
//!
//! [`AeronRuntime`] runs **one** thread that services every publication *and*
//! subscription in a process. The old publish path offered in a blocking
//! spin/sleep loop (up to `OFFER_TIMEOUT` ≈ 5 s) — so while one publication was
//! back-pressured (e.g. its subscriber slow or not yet joined), that same thread
//! **stopped polling all subscriptions**.
//!
//! In the container cluster that was fatal: the executor publishes `tx_receipts`
//! and subscribes `tx_ordering` on the same runtime. A momentarily back-pressured
//! `tx_receipts` offer parked the thread long enough (> Aeron's MIN flow-control
//! receiver timeout, ~2 s) that the sealer dropped the executor from flow
//! control, advanced past it, and the executor's `tx_ordering` image developed an
//! unfillable gap and went end-of-stream — a permanent freeze (the subscription
//! uses `no_unavailable_image_handler` and never re-subscribes). Observed live as
//! two executors pinned forever at block 48 while a freshly-restarted one tracked
//! the sealer exactly.
//!
//! The fix parks a back-pressured offer on a retry queue (`drain_pending`) and
//! keeps polling subscriptions between attempts. This test proves it end-to-end:
//! with a never-connecting publication mid-offer, a *live* subscription still
//! receives its frame promptly (instead of being stalled for ~5 s).
//!
//! Gated on the `docker-e2e` feature AND on Docker availability (the real Aeron
//! Media Driver runs in a container), same as `offer_connect_race.rs`.
//!
//! NOTE: runs under **Linux CI only**. On macOS Docker Desktop the host Aeron
//! client mmaps the bind-mounted `aeron.dir`, whose shared-memory semantics the
//! virtualized filesystem does not honour (→ `SIGBUS` / `add_subscription`
//! timeout) — the same reason the pipeline e2e runs the client and driver
//! together inside Linux node containers. The platform-independent regression
//! guard for this fix is `aeron_live::drain_pending_tests` (runs everywhere).

#![cfg(feature = "docker-e2e")]

use std::time::{Duration, Instant};

use alloy_primitives::{Address, B256};
use bytes::Bytes;
use kardamom_log::aeron_live::{AeronRuntime, TxDataPublisherHandle, TxDataSubscriberHandle};
use kardamom_log::config::LogConfig;
use kardamom_log::testing::AeronTestCluster;
use kardamom_types::TxEnvelope;

async fn docker_available() -> bool {
    use tokio::process::Command;
    Command::new("docker")
        .arg("info")
        .output()
        .await
        .map(|o| o.status.success())
        .unwrap_or(false)
}

fn env(correlation_id: u64, fill: u8) -> TxEnvelope {
    TxEnvelope {
        correlation_id,
        raw_tx: Bytes::from(vec![fill; 48]),
        sender: Address::repeat_byte(fill),
        tx_hash: B256::repeat_byte(fill),
    }
}

/// How long a delivery may take before we call it "starved". The bug stalls the
/// poll loop for `OFFER_TIMEOUT` (~5 s); a healthy runtime delivers in
/// milliseconds. 1.5 s sits comfortably between the two.
const MAX_DELIVERY: Duration = Duration::from_millis(1500);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker; run with `cargo test -p kardamom-log --features docker-e2e --test offer_starvation -- --ignored`"]
async fn back_pressured_publish_does_not_starve_a_live_subscription() {
    if !docker_available().await {
        eprintln!("skipping: docker not available");
        return;
    }

    let cluster = AeronTestCluster::single_node()
        .await
        .expect("aeron container started");
    let aeron_dir = cluster.aeron_dir_host(0).to_string_lossy().to_string();

    let mut cfg = LogConfig::default();
    // Plain IPC over the shared aeron.dir; a distinct stream base so this test
    // can't collide with the other e2e tests' streams.
    cfg.channels.tx_data_channel_template = "aeron:ipc?alias=a-{sid}".to_string();
    cfg.channels.tx_data_stream_id_base = 5201;

    let rt = AeronRuntime::spawn_with_dir(&aeron_dir).expect("aeron runtime");

    // --- LIVE stream (sid 0): a connected publisher + subscriber. ---
    let live_sid = 0u8;
    let live_pub = TxDataPublisherHandle::open(&rt, &cfg.channels, live_sid).expect("live pub");
    let mut live_sub =
        TxDataSubscriberHandle::open(&rt, &cfg.channels, live_sid).expect("live sub");

    // Warm up so the live image is fully formed BEFORE we induce back-pressure
    // elsewhere — this isolates the variable under test (thread starvation), not
    // connection setup.
    {
        let live_pub = live_pub.clone();
        tokio::task::spawn_blocking(move || live_pub.publish(&env(1, 0x11)))
            .await
            .expect("warmup join")
            .expect("warmup publish");
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut warmed = false;
        while !warmed && Instant::now() < deadline {
            if let Ok(Some((_p, e))) =
                tokio::time::timeout(Duration::from_millis(50), live_sub.recv()).await
            {
                assert_eq!(e.correlation_id, 1);
                warmed = true;
            }
        }
        assert!(warmed, "live image never formed during warm-up");
    }

    // --- DEAD stream (sid 1): a publisher with NO subscriber. Every offer to it
    // returns NOT_CONNECTED and (old code) would block the shared thread. ---
    let dead_sid = 1u8;
    let dead_pub = TxDataPublisherHandle::open(&rt, &cfg.channels, dead_sid).expect("dead pub");

    // Fire the never-connecting publish. With the old blocking offer this pins
    // the Aeron thread for ~5 s; with the fix it is parked and retried. We detach
    // it — it eventually errors at the offer deadline, which we don't wait for.
    let dead_task = tokio::task::spawn_blocking(move || dead_pub.publish(&env(999, 0xDD)));

    // Give the runtime a beat to pick up the dead publish (so, in the buggy
    // version, the thread is *already* inside its blocking offer).
    tokio::time::sleep(Duration::from_millis(150)).await;

    // --- The assertion: a fresh publish on the LIVE stream must be delivered
    // promptly, even though the dead publish is mid-flight. ---
    let t0 = Instant::now();
    {
        let live_pub = live_pub.clone();
        tokio::task::spawn_blocking(move || live_pub.publish(&env(2, 0x22)))
            .await
            .expect("live publish join")
            .expect("live publish must succeed (not blocked by the dead offer)");
    }

    let recv_deadline = Instant::now() + MAX_DELIVERY;
    let mut got: Option<TxEnvelope> = None;
    while got.is_none() && Instant::now() < recv_deadline {
        if let Ok(Some((_p, e))) =
            tokio::time::timeout(Duration::from_millis(25), live_sub.recv()).await
            && e.correlation_id == 2
        {
            got = Some(e);
        }
    }
    let elapsed = t0.elapsed();

    let env2 = got.unwrap_or_else(|| {
        panic!(
            "live subscription was starved: frame #2 not delivered within {MAX_DELIVERY:?} \
             (a back-pressured publish on another publication blocked the poll loop)"
        )
    });
    assert_eq!(env2.correlation_id, 2);
    assert!(
        elapsed < MAX_DELIVERY,
        "live delivery took {elapsed:?} — the dead publish starved the poll loop"
    );

    dead_task.abort();
    drop(rt);
    drop(cluster);
}
