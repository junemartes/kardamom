//! The member feed over a real media driver. The test plays the member:
//! it owns the feed publication on stream 1006 and reads the requests on
//! stream 1007 of the same driver.
//!
//! Gated on the `docker-e2e` feature and on Docker: the media driver runs
//! in a container, as for the `kardamom-log` live tests.

use std::time::{Duration, Instant};

use kardamom_cluster_adapter::wire::{
    FEED_ANSWER_LEN, FEED_CONTROL_STREAM_ID, FEED_REQUEST_ID_OFFSET, FEED_STREAM_ID,
};
use kardamom_log::aeron_live::PubHandle;
use kardamom_log::testing::{AeronTestCluster, SingleNodeRig};
use rkyv::util::AlignedVec;
use tokio_util::sync::CancellationToken;

use super::tests::{boundary, done, label, tx_at};
use super::*;

/// Publish `frame` until the publication takes it.
fn publish(publication: &PubHandle, frame: &[u8]) {
    let mut bytes = AlignedVec::new();
    bytes.extend_from_slice(frame);
    let deadline = Instant::now() + Duration::from_secs(20);
    while publication.publish_bytes(bytes.clone()).is_err() {
        assert!(Instant::now() < deadline, "the feed never took the frame");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker; run with `cargo test -p kardamom-engine --features docker-e2e --lib feed_tests -- --ignored`"]
async fn the_reader_takes_the_answer_and_the_live_frames_of_a_real_feed() {
    kardamom_log::testing::require_docker().await;
    let SingleNodeRig {
        cluster: _cluster,
        rt,
        cfg: _cfg,
    } = AeronTestCluster::single_node_runtime(5_595).await;
    let stop = CancellationToken::new();
    let feed = AeronFeed::open(rt.clone(), stop.clone()).expect("the feed opens");
    let (_control_id, requests) = rt
        .open_subscription_raw_crossbeam("aeron:ipc", FEED_CONTROL_STREAM_ID)
        .expect("the control subscription");
    let member = rt
        .open_exclusive_publication("aeron:ipc", FEED_STREAM_ID)
        .expect("the feed publication");
    let reader = std::thread::spawn(move || {
        let mut sub = CarriedSubscription::new(feed, NoSource, ReplayCursor::new(0, 1));
        (0..2)
            .map(|_| sub.next().map(label))
            .collect::<Result<Vec<_>, _>>()
    });

    let request = requests
        .recv_timeout(Duration::from_secs(20))
        .expect("a request on the control stream");
    assert_eq!(request.bytes.len(), FEED_ANSWER_LEN);
    let newest = requests.try_iter().last().unwrap_or(request);
    let id_bytes: [u8; 8] = newest.bytes[FEED_REQUEST_ID_OFFSET..]
        .try_into()
        .expect("an 8-byte id");
    let id = u64::from_le_bytes(id_bytes);
    publish(&member, &done(id, 0, 1));
    publish(&member, &tx_at(0));
    publish(&member, &boundary(1, 1));

    let delivered = tokio::task::spawn_blocking(move || reader.join())
        .await
        .expect("the join task")
        .expect("the reader thread")
        .expect("the reader delivers");
    stop.cancel();
    assert_eq!(delivered, ["t0", "b1"]);
}
