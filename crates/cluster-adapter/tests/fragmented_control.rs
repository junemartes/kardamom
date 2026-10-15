//! A cluster control channel carries a message of up to one session
//! message (1 MiB) through a real media driver at the deployed MTU. The
//! publication fragments the message, and the fragment assembler of the
//! subscription hands the whole message over, intact and in order. The
//! channel and the subscription call are the ones the session thread uses
//! for the cluster ingress and egress.
//!
//! Gated on the `docker-e2e` feature and on Docker: the media driver runs
//! in a container, as for the `kardamom-log` live tests.

#![cfg(feature = "docker-e2e")]

use std::time::{Duration, Instant};

use kardamom_cluster_adapter::live::control_channel;
use kardamom_cluster_client::message::MAX_SESSION_MESSAGE_LEN;
use kardamom_log::aeron_live::PubHandle;
use kardamom_log::testing::{AeronTestCluster, SingleNodeRig};
use rkyv::util::AlignedVec;

const STREAM_ID: i32 = 5_586;
/// The deployed MTU (`aeron.mtu.length`), so that each message fragments.
const CHANNEL: &str = "aeron:udp?endpoint=localhost:24586|mtu=1344";

/// A message of `len` bytes, with a pattern that differs for each `tag`.
fn message(tag: u8, len: usize) -> AlignedVec {
    let mut bytes = AlignedVec::new();
    bytes.extend_from_slice(
        &(0..len)
            .map(|n| n.to_le_bytes()[0] ^ tag)
            .collect::<Vec<u8>>(),
    );
    bytes
}

/// Publish `bytes` until the publication connects.
fn publish_until_connected(publication: &PubHandle, bytes: &AlignedVec) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while publication.publish_bytes(bytes.clone()).is_err() {
        assert!(
            Instant::now() < deadline,
            "the publication never took the message"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker; run with `cargo test -p kardamom-cluster-adapter --features docker-e2e --test fragmented_control -- --ignored`"]
async fn a_control_channel_carries_messages_up_to_one_mib_whole_and_in_order() {
    kardamom_log::testing::require_docker().await;
    let SingleNodeRig {
        cluster: _cluster,
        rt,
        cfg: _cfg,
    } = AeronTestCluster::single_node_runtime(STREAM_ID).await;
    let uri = control_channel(CHANNEL);
    let (_sub_id, frames) = rt
        .open_subscription_raw_crossbeam(&uri, STREAM_ID)
        .expect("subscription");
    let publication = rt.open_publication(&uri, STREAM_ID).expect("publication");
    let sent = [
        message(1, 64 << 10),
        message(2, MAX_SESSION_MESSAGE_LEN),
        message(3, 64 << 10),
    ];

    for bytes in &sent {
        publish_until_connected(&publication, bytes);
    }

    for (i, bytes) in sent.iter().enumerate() {
        let frame = frames
            .recv_timeout(Duration::from_secs(10))
            .unwrap_or_else(|e| panic!("message {i} within 10 s: {e}"));
        assert_eq!(frame.bytes.len(), bytes.len(), "message {i} length");
        assert!(frame.bytes == bytes.as_slice(), "message {i} bytes");
    }
}
