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

use std::collections::BTreeSet;
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

/// Every frame received until the stream stays quiet for 1 s, or for at
/// most 10 s.
async fn drain(rx: &mut UnboundedReceiver<RawFrame>) -> Vec<RawFrame> {
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut frames = Vec::new();
    while Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(1), rx.recv()).await {
            Ok(Some(f)) => frames.push(f),
            _ => break,
        }
    }
    frames
}

/// Assert that `frames` hold exactly `FRAMES` copies of `fill`, all from
/// one session, and nothing else. Return that session.
fn exactly_once(frames: &[RawFrame], fill: u8, what: &str) -> i32 {
    assert_eq!(frames.len(), FRAMES, "{what}: every frame arrives once");
    assert!(
        frames.iter().all(|f| f.bytes[0] == fill),
        "{what}: no other frame arrives"
    );
    let sessions: BTreeSet<i32> = frames.iter().map(|f| f.session).collect();
    assert_eq!(sessions.len(), 1, "{what}: one session per publisher");
    frames[0].session
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
    let session_a = exactly_once(&drain(&mut rx).await, 0xAA, "a");
    publish(&pub_b, 0xBB).await;
    let session_b = exactly_once(&drain(&mut rx).await, 0xBB, "b");
    assert_ne!(session_a, session_b, "two publishers are two sessions");

    rt.remove_destination(sub_id, &destination(PORT_B))
        .expect("remove b");
    tokio::time::sleep(Duration::from_secs(1)).await;

    publish(&pub_a, 0xA1).await;
    let again_a = exactly_once(&drain(&mut rx).await, 0xA1, "a after the removal of b");
    assert_eq!(again_a, session_a, "a keeps its session");
    rt.add_destination(sub_id, &destination(PORT_B))
        .expect("destination b again");
    publish(&pub_b, 0xB1).await;
    exactly_once(&drain(&mut rx).await, 0xB1, "b after a new attach");

    let errors = driver_errors(&aeron_dir);
    assert!(
        errors.iter().all(|e| !e.contains("removeDestination")),
        "the driver logs no destination removal error: {errors:#?}"
    );
}
