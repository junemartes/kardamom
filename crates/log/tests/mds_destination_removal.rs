//! A real-Aeron check that the removal of a subscriber destination leaves
//! the media driver without an error.
//!
//! The Java media driver sizes the connection table of an image from the
//! destination index of the image, and grows it only when a destination
//! is added after the image exists. An MDS subscription whose images form
//! after all of its destinations are attached thus holds an image with a
//! table shorter than the highest destination index. The removal of that
//! destination throws `ArrayIndexOutOfBoundsException` in
//! `PublicationImage.removeDestination`. The runtime must never send the
//! driver that removal, and the other publishers must keep their delivery.
//!
//! Gated on the `docker-e2e` feature and on Docker availability.

#![cfg(feature = "docker-e2e")]

use std::time::{Duration, Instant};

use kardamom_log::aeron_live::{PubHandle, RawFrame};
use kardamom_log::testing::{AeronTestCluster, SingleNodeRig};
use rkyv::util::AlignedVec;
use rusteron_client::AeronCnc;
use tokio::sync::mpsc::UnboundedReceiver;

const STREAM: i32 = 4521;
const FRAMES: usize = 20;
const PORT_A: u16 = 41010;
const PORT_B: u16 = 41011;

fn frame(fill: u8) -> AlignedVec {
    let mut v = AlignedVec::new();
    v.extend_from_slice(&[fill; 64]);
    v
}

fn control_uri(port: u16) -> String {
    format!("aeron:udp?control=127.0.0.1:{port}|control-mode=dynamic")
}

fn destination(port: u16) -> String {
    format!("aeron:udp?endpoint=127.0.0.1:0|control=127.0.0.1:{port}|control-mode=dynamic")
}

async fn publish(publisher: &PubHandle, fill: u8) {
    let p = publisher.clone();
    tokio::task::spawn_blocking(move || {
        (0..FRAMES).for_each(|_| {
            p.publish_bytes(frame(fill)).expect("publish");
        });
    })
    .await
    .expect("publisher task");
}

/// The count of frames with the first byte `fill` among up to `want`
/// frames received within `budget`.
async fn received(rx: &mut UnboundedReceiver<RawFrame>, fill: u8, want: usize) -> usize {
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut seen = 0;
    while seen < want && Instant::now() < deadline {
        if let Ok(Some(f)) = tokio::time::timeout(Duration::from_millis(200), rx.recv()).await {
            seen += usize::from(f.bytes[0] == fill);
        }
    }
    seen
}

/// Every distinct error in the error log of the driver at `aeron_dir`.
fn driver_errors(aeron_dir: &std::path::Path) -> Vec<String> {
    let cnc = AeronCnc::new_on_heap(&aeron_dir.to_string_lossy()).expect("open cnc");
    let mut errors = Vec::new();
    cnc.error_log_read_once(|_, _, _, error: &str| errors.push(error.to_string()), 0);
    errors
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker; run with `cargo test -p kardamom-log --features docker-e2e --test mds_destination_removal -- --ignored`"]
async fn removing_a_destination_after_its_images_form_keeps_the_driver_clean() {
    kardamom_log::testing::require_docker().await;
    let SingleNodeRig { cluster, rt, .. } = AeronTestCluster::single_node_runtime(4520).await;
    let aeron_dir = cluster.aeron_dir_host(0).to_path_buf();

    // Both destinations attach before either publisher exists, so both
    // images form after the last attach.
    let (sub_id, mut rx) = rt
        .open_subscription_raw("aeron:udp?control-mode=manual", STREAM)
        .expect("manual subscription");
    rt.add_destination(sub_id, &destination(PORT_A))
        .expect("destination a");
    rt.add_destination(sub_id, &destination(PORT_B))
        .expect("destination b");
    let pub_a = rt
        .open_publication(&control_uri(PORT_A), STREAM)
        .expect("publisher a");
    let pub_b = rt
        .open_publication(&control_uri(PORT_B), STREAM)
        .expect("publisher b");
    publish(&pub_a, 0xAA).await;
    assert_eq!(received(&mut rx, 0xAA, FRAMES).await, FRAMES, "a arrives");
    publish(&pub_b, 0xBB).await;
    assert_eq!(received(&mut rx, 0xBB, FRAMES).await, FRAMES, "b arrives");

    rt.remove_destination(sub_id, &destination(PORT_B))
        .expect("remove b");
    tokio::time::sleep(Duration::from_secs(1)).await;

    publish(&pub_a, 0xA1).await;
    assert_eq!(
        received(&mut rx, 0xA1, FRAMES).await,
        FRAMES,
        "a keeps its delivery after the removal of b"
    );
    rt.add_destination(sub_id, &destination(PORT_B))
        .expect("destination b again");
    publish(&pub_b, 0xB1).await;
    assert_eq!(
        received(&mut rx, 0xB1, FRAMES).await,
        FRAMES,
        "b arrives again after a new attach"
    );

    let errors = driver_errors(&aeron_dir);
    assert!(
        errors.iter().all(|e| !e.contains("removeDestination")),
        "the driver logs no destination removal error: {errors:#?}"
    );
}
