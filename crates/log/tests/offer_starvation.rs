//! Regression test for the cluster `tx_ordering` freeze: a back-pressured
//! publish must not starve subscription polling on the shared Aeron thread.
//!
//! ## The invariant this guards
//!
//! [`AeronRuntime`] runs one thread that services every publication and
//! subscription in a process. A back-pressured offer (for example a
//! subscriber that is slow or has not joined yet) must not stop that
//! thread from polling other subscriptions. In the cluster, `tx_receipts`
//! and `tx_ordering` share one runtime: if a back-pressured `tx_receipts`
//! offer ever parked the thread past Aeron's minimum flow-control receiver
//! timeout (about 2 s), the sealer would drop the executor from flow
//! control, and the executor's `tx_ordering` image would develop an
//! unfillable gap and go end-of-stream — a permanent freeze, since the
//! subscription uses `no_unavailable_image_handler` and never
//! re-subscribes.
//!
//! A back-pressured offer parks on a retry queue (`drain_pending`), and
//! the Aeron thread keeps polling subscriptions between attempts. This
//! test proves it end-to-end: with a never-connecting publication
//! mid-offer, a live subscription still receives its frame promptly,
//! instead of stalling for about 5 s.
//!
//! Gated on the `docker-e2e` feature and on Docker availability (the real
//! Aeron Media Driver runs in a container), same as `offer_connect_race.rs`.
//!
//! Note: runs under Linux CI only. On macOS Docker Desktop the host Aeron
//! client mmaps the bind-mounted `aeron.dir`, whose shared-memory
//! semantics the virtualized filesystem does not honor (giving `SIGBUS`
//! or an `add_subscription` timeout). This is the same reason the
//! pipeline e2e runs the client and driver together inside Linux node
//! containers. The platform-independent regression guard for this fix is
//! `aeron_live::drain_pending_tests` (runs everywhere).

#![cfg(feature = "docker-e2e")]

mod common;

use std::time::{Duration, Instant};

use kardamom_log::aeron_live::{TxDataPublisherHandle, TxDataSubscriberHandle};
use kardamom_log::testing::{AeronTestCluster, SingleNodeRig};
use kardamom_types::TxEnvelope;

/// How long a delivery may take before this test calls it "starved". The
/// bug stalls the poll loop for `OFFER_TIMEOUT` (about 5 s); a healthy
/// runtime delivers in milliseconds. 1.5 s sits comfortably between the two.
const MAX_DELIVERY: Duration = Duration::from_millis(1500);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker; run with `cargo test -p kardamom-log --features docker-e2e --test offer_starvation -- --ignored`"]
// `live_pub`/`live_sub` name the publisher and subscriber sides of the
// same stream; the crate's handle types use this pub/sub naming
// throughout, so it is clearer kept than renamed.
#[allow(clippy::similar_names)]
async fn back_pressured_publish_does_not_starve_a_live_subscription() {
    // Explicit opt-in test (`--features docker-e2e -- --ignored`). A
    // missing Docker must fail loudly, not silently pass through an early
    // return.
    kardamom_log::testing::require_docker().await;

    // Plain IPC over the shared aeron.dir. Stream base 5201 keeps this
    // test from colliding with the other e2e tests' streams.
    let SingleNodeRig { cluster, rt, cfg } = AeronTestCluster::single_node_runtime(5201).await;

    // --- Live stream (sid 0): a connected publisher and subscriber. ---
    let live_sid = 0u8;
    let live_pub = TxDataPublisherHandle::open(&rt, &cfg.channels, live_sid).expect("live pub");
    let mut live_sub =
        TxDataSubscriberHandle::open(&rt, &cfg.channels, live_sid).expect("live sub");

    // Warm up so the live image is fully formed before this test induces
    // back-pressure elsewhere. This isolates the variable under test
    // (thread starvation), not connection setup.
    {
        let live_pub = live_pub.clone();
        tokio::task::spawn_blocking(move || live_pub.publish(&common::tx_envelope(1, 0x11, 48)))
            .await
            .expect("warmup join")
            .expect("warmup publish");
        let warmed =
            kardamom_log::testing::recv_within(&mut live_sub, Duration::from_secs(5), |e| {
                e.correlation_id == 1
            })
            .await;
        assert!(warmed.is_some(), "live image never formed during warm-up");
    }

    // --- Dead stream (sid 1): a publisher with no subscriber. Every offer
    // to it returns NOT_CONNECTED. ---
    let dead_sid = 1u8;
    let dead_pub = TxDataPublisherHandle::open(&rt, &cfg.channels, dead_sid).expect("dead pub");

    // Fire the never-connecting publish. It is parked on the retry queue
    // and retried each poll. This test detaches it; it eventually errors
    // at the offer deadline, which this test does not wait for.
    let dead_task =
        tokio::task::spawn_blocking(move || dead_pub.publish(&common::tx_envelope(999, 0xDD, 48)));

    // Give the runtime a beat to pick up the dead publish, so the
    // assertion below runs while it is parked on the retry queue.
    tokio::time::sleep(Duration::from_millis(150)).await;

    // --- The assertion: a fresh publish on the live stream must be
    // delivered promptly, even though the dead publish is mid-flight. ---
    let t0 = Instant::now();
    {
        let live_pub = live_pub.clone();
        tokio::task::spawn_blocking(move || live_pub.publish(&common::tx_envelope(2, 0x22, 48)))
            .await
            .expect("live publish join")
            .expect("live publish must succeed (not blocked by the dead offer)");
    }

    let got: Option<TxEnvelope> =
        kardamom_log::testing::recv_within(&mut live_sub, MAX_DELIVERY, |e| e.correlation_id == 2)
            .await;
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
