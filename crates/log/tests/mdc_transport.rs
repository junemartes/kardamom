//! Real-Aeron checks for the dynamic MDC transport shape:
//!
//! 1. one `control-mode=manual` subscription joins several dynamic MDC
//!    publishers through `add_destination`, each on an ephemeral receive
//!    port, and each publisher stays a distinct image;
//! 2. `remove_destination` stops delivery from that publisher;
//! 3. an archive records a dynamic MDC publisher through its control
//!    endpoint, and the recording carries the publisher's session id;
//! 4. one control endpoint carries two stream ids to two manual
//!    subscriptions.
//!
//! Gated on the `docker-e2e` feature and on Docker availability.

#![cfg(feature = "docker-e2e")]

use std::cell::RefCell;
use std::collections::BTreeSet;
use std::rc::Rc;
use std::time::{Duration, Instant};

use kardamom_log::aeron_live::{AeronRuntime, PubHandle, RawFrame};
use kardamom_log::recorder::{Recorder, RecorderKind, connect_archive};
use kardamom_log::testing::{AeronTestCluster, SingleNodeRig};
use rkyv::util::AlignedVec;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio_util::sync::CancellationToken;

/// Stream ids 4501 to 4512 keep this test apart from the other e2e tests.
const STREAM: i32 = 4501;
const FRAMES: usize = 20;

fn frame(fill: u8) -> AlignedVec {
    let mut v = AlignedVec::new();
    v.extend_from_slice(&[fill; 64]);
    v
}

fn control_uri(port: u16) -> String {
    format!("aeron:udp?control=127.0.0.1:{port}|control-mode=dynamic")
}

/// The destination a manual subscription attaches to join the publisher
/// whose control endpoint is `port`. Port 0 lets the driver pick the
/// receive port.
fn destination(port: u16) -> String {
    format!("aeron:udp?endpoint=127.0.0.1:0|control=127.0.0.1:{port}|control-mode=dynamic")
}

fn publish_frames(publisher: &PubHandle, fill: u8) {
    for _ in 0..FRAMES {
        publisher.publish_bytes(frame(fill)).expect("publish");
    }
}

/// `(first payload byte, session id)` of up to `want` frames received
/// within `budget`.
async fn collect(
    rx: &mut UnboundedReceiver<RawFrame>,
    budget: Duration,
    want: usize,
) -> Vec<(u8, i32)> {
    let mut out = Vec::new();
    let deadline = Instant::now() + budget;
    while out.len() < want && Instant::now() < deadline {
        if let Ok(Some(f)) = tokio::time::timeout(Duration::from_millis(200), rx.recv()).await {
            out.push((f.bytes[0], f.session));
        }
    }
    out
}

fn count(frames: &[(u8, i32)], fill: u8) -> usize {
    frames.iter().filter(|(f, _)| *f == fill).count()
}

/// The session id the archive catalog holds for `recording_id`.
fn recording_session(archive: &rusteron_archive::AeronArchive, recording_id: i64) -> i32 {
    struct Capture(Rc<RefCell<Option<i32>>>);
    impl rusteron_archive::AeronArchiveRecordingDescriptorConsumerFuncCallback for Capture {
        fn handle_aeron_archive_recording_descriptor_consumer_func(
            &mut self,
            desc: rusteron_archive::AeronArchiveRecordingDescriptor,
        ) {
            *self.0.borrow_mut() = Some(desc.session_id());
        }
    }
    let cell = Rc::new(RefCell::new(None));
    let mut handler = rusteron_archive::Handler::leak(Capture(cell.clone()));
    let listed = archive.list_recording(recording_id, Some(&handler));
    handler.release();
    listed.expect("list_recording");
    let session = *cell.borrow();
    session.expect("the recording has a descriptor")
}

/// Record publisher A through its control endpoint on a recorder thread,
/// and return the recorded session id once the recording is live.
fn record_publisher_a(
    aeron_dir: std::path::PathBuf,
    aeron_cfg: kardamom_log::config::AeronConfig,
    stop: CancellationToken,
) -> std::thread::JoinHandle<i32> {
    std::thread::spawn(move || {
        let session = connect_archive(Some(&aeron_dir), &aeron_cfg).expect("archive");
        let recorder = Recorder::start_stream(
            session.archive,
            &destination(41000),
            STREAM,
            RecorderKind::TxData { sequencer_id: 0 },
            &stop,
        )
        .expect("start recording")
        .expect("the recording appears");
        let catalog = connect_archive(Some(&aeron_dir), &aeron_cfg).expect("archive catalog");
        recording_session(&catalog.archive, recorder.recording_id())
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker; run with `cargo test -p kardamom-log --features docker-e2e --test mdc_transport -- --ignored`"]
async fn manual_subscription_joins_and_leaves_dynamic_mdc_publishers() {
    kardamom_log::testing::require_docker().await;
    let SingleNodeRig { cluster, rt, cfg } = AeronTestCluster::single_node_runtime(4500).await;
    let aeron_dir = cluster.aeron_dir_host(0).to_path_buf();

    let pub_a = rt
        .open_publication(&control_uri(41000), STREAM)
        .expect("publisher a");
    let pub_b = rt
        .open_publication(&control_uri(41001), STREAM)
        .expect("publisher b");

    // The recording starts before any live subscriber joins.
    let stop = CancellationToken::new();
    let recorder = record_publisher_a(aeron_dir.clone(), cfg.aeron.clone(), stop.clone());

    let (sub_id, mut rx) = rt
        .open_subscription_raw("aeron:udp?control-mode=manual", STREAM)
        .expect("manual subscription");
    rt.add_destination(sub_id, &destination(41000))
        .expect("destination a");
    rt.add_destination(sub_id, &destination(41001))
        .expect("destination b");

    let a = tokio::task::spawn_blocking({
        let p = pub_a.clone();
        move || publish_frames(&p, 0xAA)
    });
    let b = tokio::task::spawn_blocking({
        let p = pub_b.clone();
        move || publish_frames(&p, 0xBB)
    });
    a.await.expect("publisher a task");
    b.await.expect("publisher b task");

    let got = collect(&mut rx, Duration::from_secs(10), 2 * FRAMES).await;
    assert_eq!(count(&got, 0xAA), FRAMES, "every frame of a arrives");
    assert_eq!(count(&got, 0xBB), FRAMES, "every frame of b arrives");
    let sessions: BTreeSet<i32> = got.iter().map(|(_, s)| *s).collect();
    assert_eq!(sessions.len(), 2, "two publishers are two images");

    let session_a = got.iter().find(|(f, _)| *f == 0xAA).map(|(_, s)| *s);
    let recorded_session = recorder.join().expect("recorder thread");
    assert_eq!(
        Some(recorded_session),
        session_a,
        "the recording is publisher a's image"
    );

    rt.remove_destination(sub_id, &destination(41001))
        .expect("remove b");
    tokio::time::sleep(Duration::from_secs(1)).await;
    let _ = tokio::task::spawn_blocking(move || pub_b.publish_bytes(frame(0xBB)))
        .await
        .expect("publisher b task");
    let late = collect(&mut rx, Duration::from_secs(3), 1).await;
    assert_eq!(count(&late, 0xBB), 0, "b is silent after the remove");

    two_streams_share_one_control_endpoint(&rt, &aeron_dir).await;
    stop.cancel();
}

/// Publisher C publishes two stream ids from one control endpoint, and
/// two manual subscriptions each receive their own stream.
async fn two_streams_share_one_control_endpoint(rt: &AeronRuntime, aeron_dir: &std::path::Path) {
    let rt_c = AeronRuntime::spawn_with_dir(aeron_dir).expect("publisher runtime");
    let pub_c1 = rt_c
        .open_publication(&control_uri(41002), STREAM + 10)
        .expect("publisher c1");
    let pub_c2 = rt_c
        .open_publication(&control_uri(41002), STREAM + 11)
        .expect("publisher c2");
    let (s1, mut rx1) = rt
        .open_subscription_raw("aeron:udp?control-mode=manual", STREAM + 10)
        .expect("subscription c1");
    let (s2, mut rx2) = rt
        .open_subscription_raw("aeron:udp?control-mode=manual", STREAM + 11)
        .expect("subscription c2");
    rt.add_destination(s1, &destination(41002))
        .expect("destination c1");
    rt.add_destination(s2, &destination(41002))
        .expect("destination c2");
    let c1 = tokio::task::spawn_blocking(move || publish_frames(&pub_c1, 0xC1));
    let c2 = tokio::task::spawn_blocking(move || publish_frames(&pub_c2, 0xC2));
    c1.await.expect("publisher c1 task");
    c2.await.expect("publisher c2 task");
    let g1 = collect(&mut rx1, Duration::from_secs(10), FRAMES).await;
    let g2 = collect(&mut rx2, Duration::from_secs(10), FRAMES).await;
    assert_eq!(count(&g1, 0xC1), FRAMES, "stream c1 arrives whole");
    assert_eq!(count(&g1, 0xC2), 0, "stream c1 carries no c2 frames");
    assert_eq!(count(&g2, 0xC2), FRAMES, "stream c2 arrives whole");
    assert_eq!(count(&g2, 0xC1), 0, "stream c2 carries no c1 frames");
}
