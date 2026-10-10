//! A subscription with a live image closes, and the Aeron client keeps
//! running. The client conductor calls the unavailable-image handler of a
//! subscription while it lingers the images of a closed subscription,
//! after the row of the runtime's table is gone. The handlers of the
//! image log must therefore outlive every subscription. A handler that
//! was released with its row is a dangling pointer on that call, and the
//! process dies without a line.
//!
//! Gated on the `docker-e2e` feature and on Docker availability (the real
//! Aeron Media Driver runs in a container), as `offer_starvation.rs` is.

#![cfg(feature = "docker-e2e")]

use std::time::{Duration, Instant};

use kardamom_log::testing::{AeronTestCluster, SingleNodeRig};
use rkyv::util::AlignedVec;

const CHANNEL: &str = "aeron:ipc?alias=image-log-close";

/// Publish `frame` until the publication connects: the first offer lands
/// once the subscriber has an image.
fn publish_until_connected(publication: &kardamom_log::aeron_live::PubHandle, frame: &AlignedVec) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while publication.publish_bytes(frame.clone()).is_err() {
        assert!(Instant::now() < deadline, "the publication never connected");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker; run with `cargo test -p kardamom-log --features docker-e2e --test image_log_close -- --ignored`"]
async fn a_closed_subscription_with_live_images_leaves_the_client_running() {
    kardamom_log::testing::require_docker().await;
    let SingleNodeRig {
        cluster: _cluster,
        rt,
        cfg: _cfg,
    } = AeronTestCluster::single_node_runtime(5301).await;
    let stream_id = 5301;
    let mut frame = AlignedVec::new();
    frame.extend_from_slice(&[7u8; 16]);

    let (sub_id, mut rx) = rt
        .open_subscription_raw(CHANNEL, stream_id)
        .expect("first subscription");
    let publication = rt
        .open_publication(CHANNEL, stream_id)
        .expect("publication");
    publish_until_connected(&publication, &frame);
    let first = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("a frame within 5 s")
        .expect("the frame");
    assert_eq!(first.bytes, vec![7u8; 16]);

    // Close the subscription while its image is live. The conductor
    // lingers the image and calls the unavailable-image handler.
    rt.close_subscription(sub_id).expect("close");
    tokio::time::sleep(Duration::from_secs(2)).await;

    // The client still runs: a new subscription on the same stream gets
    // an image and a frame.
    let (_sub_id, mut again) = rt
        .open_subscription_raw(CHANNEL, stream_id)
        .expect("second subscription");
    publish_until_connected(&publication, &frame);
    let second = tokio::time::timeout(Duration::from_secs(5), again.recv())
        .await
        .expect("a frame on the new subscription within 5 s")
        .expect("the frame");
    assert_eq!(second.bytes, vec![7u8; 16]);
}
