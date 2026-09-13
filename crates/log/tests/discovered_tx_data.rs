//! Real-Aeron check of `tx_data` over the stream plane and the in-memory
//! catalog:
//!
//! 1. two ingress-like publishers of one lane reach one executor-like
//!    subscriber as two distinct sessions, and a lane-1 publisher never
//!    reaches the lane-0 subscription;
//! 2. the discovery-driven recorder records every publisher on the local
//!    archive, one recording per control endpoint, and reports ready once
//!    its own instance's lanes record;
//! 3. a publisher that restarts on the same control port, against the
//!    archive's still-live recording subscription, is recorded as a new
//!    session before the recorder reports ready.
//!
//! Gated on the `docker-e2e` feature and on Docker availability.

#![cfg(feature = "docker-e2e")]

mod common;

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::net::Ipv4Addr;
use std::num::NonZeroU64;
use std::rc::Rc;
use std::time::{Duration, Instant};

use kardamom_log::aeron_live::{AeronRuntime, TxDataPublisherHandle, TxDataSubscription};
use kardamom_log::config::LogConfig;
use kardamom_log::discovery::memory::MemoryCatalog;
use kardamom_log::discovery::{
    Catalog, DiscoveredRecorder, Instance, PortRange, RecorderProgress, StreamPlane, Topic,
};
use kardamom_log::recorder::connect_archive;
use kardamom_log::testing::{AeronTestCluster, SingleNodeRig};
use tokio_util::sync::CancellationToken;

fn config(base: &LogConfig) -> LogConfig {
    let mut cfg = base.clone();
    cfg.discovery.enabled = true;
    cfg.discovery.cluster_id = "test".into();
    cfg.discovery.chain_id = 7;
    cfg.discovery.blocking_wait_ms = NonZeroU64::new(200).unwrap();
    cfg.discovery.backoff_min_ms = NonZeroU64::new(50).unwrap();
    cfg.discovery.removal_grace_ms = 500;
    cfg
}

fn plane(cfg: &LogConfig, label: &str, catalog: &MemoryCatalog) -> StreamPlane {
    plane_with_ports(cfg, label, catalog, None)
}

fn plane_with_ports(
    cfg: &LogConfig,
    label: &str,
    catalog: &MemoryCatalog,
    ports: Option<PortRange>,
) -> StreamPlane {
    StreamPlane::with_catalog(
        cfg,
        label,
        Catalog::Memory(catalog.clone()),
        Instance {
            id: format!("alloc-{label}"),
            ports,
        },
        Ipv4Addr::LOCALHOST,
    )
}

/// A recorder thread over `membership`, gated on `expected_own` own
/// recordings, plus its stop token and readiness receiver.
fn spawn_recorder(
    cfg: &LogConfig,
    aeron_dir: &std::path::Path,
    membership: tokio::sync::watch::Receiver<kardamom_log::discovery::Membership>,
    expected_own: usize,
) -> (
    CancellationToken,
    tokio::sync::oneshot::Receiver<RecorderProgress>,
    std::thread::JoinHandle<()>,
) {
    let stop = CancellationToken::new();
    let recorder = DiscoveredRecorder {
        aeron_dir: Some(aeron_dir.to_path_buf()),
        aeron_cfg: cfg.aeron.clone(),
        local_ip: Ipv4Addr::LOCALHOST,
        own_instance: "alloc-ingress-a".into(),
        expected_own,
        membership,
        stop: stop.clone(),
        runtime: tokio::runtime::Handle::current(),
    };
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let thread = std::thread::spawn(move || {
        recorder
            .run(|progress| {
                let _ = ready_tx.send(progress);
            })
            .expect("recorder thread");
    });
    (stop, ready_rx, thread)
}

async fn wait_ready(ready_rx: tokio::sync::oneshot::Receiver<RecorderProgress>, own: usize) {
    let progress = tokio::time::timeout(Duration::from_secs(20), ready_rx)
        .await
        .expect("recorder readiness within the budget")
        .expect("recorder thread alive");
    assert_eq!(
        progress,
        RecorderProgress::Ready {
            own_recordings: own
        }
    );
}

/// Wait until `port` binds on the loopback again: the driver releases a
/// control socket once the last publication on it has lingered out.
async fn wait_port_free(port: u16) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while std::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, port)).is_err() {
        assert!(Instant::now() < deadline, "port {port} never freed");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Wait until the catalog marks recording `id` stopped: the publisher's
/// image has closed on the archive. A publication re-added before that
/// would join the same Aeron session, which is not a restart.
async fn wait_recording_stopped(archive: &rusteron_archive::AeronArchive, stream: i32, id: i64) {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let stopped = recordings(archive, stream)
            .iter()
            .any(|r| r.id == id && r.stop >= 0);
        if stopped {
            return;
        }
        assert!(Instant::now() < deadline, "recording {id} never stopped");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Publish envelope `id` until the offer is acknowledged.
async fn publish_until_connected(publisher: &TxDataPublisherHandle, id: u64) {
    let deadline = Instant::now() + Duration::from_secs(10);
    let p = publisher.clone();
    let connected = tokio::task::spawn_blocking(move || {
        while Instant::now() < deadline {
            if p.publish(&common::tx_envelope(id, 1, 32)).is_ok() {
                return true;
            }
        }
        false
    })
    .await
    .expect("publish task");
    assert!(connected, "publisher of envelope {id} never connected");
}

/// Publish `(publisher, id)` pairs over and over until `sub` has seen
/// every id, or `budget` runs out. A publication connected through the
/// recorder alone accepts offers before a late subscriber joins, and
/// Aeron never replays history, so the test keeps sending.
async fn publish_until_seen(
    sub: &mut TxDataSubscription,
    sources: &[(&TxDataPublisherHandle, u64)],
    budget: Duration,
) -> BTreeMap<u64, i32> {
    let want: BTreeSet<u64> = sources.iter().map(|(_, id)| *id).collect();
    let mut seen = BTreeMap::new();
    let deadline = Instant::now() + budget;
    while !want.iter().all(|id| seen.contains_key(id)) && Instant::now() < deadline {
        for (publisher, id) in sources {
            let _ = publisher.publish(&common::tx_envelope(*id, 1, 32));
        }
        seen.extend(collect(sub, &want, Duration::from_millis(300)).await);
    }
    seen
}

/// `(correlation id, session)` of every frame received within `budget`,
/// stopping once `want` ids are seen.
async fn collect(
    sub: &mut TxDataSubscription,
    want: &BTreeSet<u64>,
    budget: Duration,
) -> BTreeMap<u64, i32> {
    let mut seen = BTreeMap::new();
    let deadline = Instant::now() + budget;
    while !want.iter().all(|id| seen.contains_key(id)) && Instant::now() < deadline {
        if let Ok(Some((loc, env))) =
            tokio::time::timeout(Duration::from_millis(200), sub.recv()).await
        {
            seen.insert(env.correlation_id, loc.session_id);
        }
    }
    seen
}

/// One catalog entry, as the assertions read it.
#[derive(Debug, Clone)]
struct Recorded {
    id: i64,
    session: i32,
    channel: String,
    /// `-1` while the recording is live.
    stop: i64,
}

/// Every recording the archive catalog holds for `stream_id`.
fn recordings(archive: &rusteron_archive::AeronArchive, stream_id: i32) -> Vec<Recorded> {
    struct Capture(Rc<RefCell<Vec<Recorded>>>);
    impl rusteron_archive::AeronArchiveRecordingDescriptorConsumerFuncCallback for Capture {
        fn handle_aeron_archive_recording_descriptor_consumer_func(
            &mut self,
            desc: rusteron_archive::AeronArchiveRecordingDescriptor,
        ) {
            self.0.borrow_mut().push(Recorded {
                id: desc.recording_id(),
                session: desc.session_id(),
                channel: desc.original_channel().to_string(),
                stop: desc.stop_position(),
            });
        }
    }
    let cell = Rc::new(RefCell::new(Vec::new()));
    let mut handler = rusteron_archive::Handler::leak(Capture(cell.clone()));
    let empty = std::ffi::CString::new("").unwrap();
    let listed = archive.list_recordings_for_uri(0, 100, &empty, stream_id, Some(&handler));
    handler.release();
    listed.expect("list_recordings_for_uri");
    cell.borrow().clone()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker; run with `cargo test -p kardamom-log --features docker-e2e --test discovered_tx_data -- --ignored`"]
async fn two_lane_publishers_reach_one_subscriber_and_the_recorder_records_each() {
    kardamom_log::testing::require_docker().await;
    let SingleNodeRig { cluster, rt, cfg } = AeronTestCluster::single_node_runtime(4800).await;
    let cfg = config(&cfg);
    let catalog = MemoryCatalog::new();
    let aeron_dir = cluster.aeron_dir_host(0).to_path_buf();

    // The recorder follows the tx_data topic before any publisher exists.
    let mut plane_rec = plane(&cfg, "ingress-a", &catalog);
    let membership = plane_rec
        .watch_topic(Topic::TxData)
        .expect("discovered plane");
    let (stop, ready_rx, recorder_thread) = spawn_recorder(&cfg, &aeron_dir, membership, 2);

    // Ingress A publishes lanes 0 and 1; ingress B publishes lane 0.
    let rt_a = AeronRuntime::spawn_with_dir(&aeron_dir).expect("runtime a");
    let ingress_a_lane0 = plane_rec
        .tx_data_publisher(&rt_a, 0)
        .await
        .expect("publisher a lane 0");
    let ingress_a_lane1 = plane_rec
        .tx_data_publisher(&rt_a, 1)
        .await
        .expect("publisher a lane 1");
    let rt_b = AeronRuntime::spawn_with_dir(&aeron_dir).expect("runtime b");
    let mut plane_b = plane(&cfg, "ingress-b", &catalog);
    let other_lane0 = plane_b
        .tx_data_publisher(&rt_b, 0)
        .await
        .expect("publisher b lane 0");

    // Readiness: both of A's lanes record, whatever B does.
    wait_ready(ready_rx, 2).await;

    // An executor-like subscriber of lane 0 sees both lane-0 publishers as
    // two sessions, and nothing from lane 1.
    let mut plane_sub = plane(&cfg, "executor", &catalog);
    let mut sub0 = plane_sub
        .tx_data_subscription(&rt, 0)
        .expect("lane 0 subscription");
    // Lane 1 has the recorder as its only subscriber, so this connects
    // through the recording alone.
    publish_until_connected(&ingress_a_lane1, 3).await;
    let seen = publish_until_seen(
        &mut sub0,
        &[(&ingress_a_lane0, 1), (&other_lane0, 2)],
        Duration::from_secs(15),
    )
    .await;
    assert!(
        seen.contains_key(&1) && seen.contains_key(&2),
        "got {seen:?}"
    );
    assert_ne!(seen[&1], seen[&2], "two publishers are two sessions");
    assert!(
        !seen.contains_key(&3),
        "a lane-1 envelope never reaches the lane-0 subscription"
    );

    // The catalog holds one recording per publisher, keyed by its control
    // endpoint, with the live session ids.
    let stream0 = cfg.channels.tx_data_stream_id(0);
    let stream1 = cfg.channels.tx_data_stream_id(1);
    let session = connect_archive(Some(&aeron_dir), &cfg.aeron).expect("archive catalog");
    let lane0 = recordings(&session.archive, stream0);
    let lane1 = recordings(&session.archive, stream1);
    let lane0_sessions: BTreeSet<i32> = lane0.iter().map(|r| r.session).collect();
    assert_eq!(
        lane0_sessions,
        BTreeSet::from([seen[&1], seen[&2]]),
        "lane 0 recordings: {lane0:?}"
    );
    assert!(
        lane0
            .iter()
            .all(|r| r.channel.contains("control=127.0.0.1:")),
        "each recording is keyed by a control endpoint: {lane0:?}"
    );
    assert_eq!(lane1.len(), 1, "lane 1 recordings: {lane1:?}");

    stop.cancel();
    recorder_thread.join().expect("recorder thread");
    plane_b.shutdown().await;
    plane_sub.shutdown().await;
    plane_rec.shutdown().await;
}

/// What the archive holds for the dead publisher's channel when the new
/// incarnation starts.
#[derive(Clone, Copy)]
enum ArchiveSubscription {
    /// The recording subscription is still live, as when the archive
    /// daemon outlives the ingress: the new image forms at once and
    /// `start_recording` is rejected, so the recorder adopts.
    Kept,
    /// The subscription is gone, as after an archive restart: the image
    /// forms only after the recorder's own `start_recording`, so the
    /// catalog holds nothing but the earlier incarnation's recording
    /// when the recorder first resolves.
    Stopped,
}

/// One publisher on a fixed control port dies and comes back. Ready must
/// wait for the new session's recording, not adopt the first one.
async fn restart_on_same_port(stream_id_base: i32, port: u16, archive_sub: ArchiveSubscription) {
    kardamom_log::testing::require_docker().await;
    let SingleNodeRig {
        cluster,
        rt: _rt,
        cfg,
    } = AeronTestCluster::single_node_runtime(stream_id_base).await;
    let cfg = config(&cfg);
    let catalog = MemoryCatalog::new();
    let aeron_dir = cluster.aeron_dir_host(0).to_path_buf();
    let ports: PortRange = format!("{port}-{port}").parse().expect("port range");
    let stream0 = cfg.channels.tx_data_stream_id(0);
    let session = connect_archive(Some(&aeron_dir), &cfg.aeron).expect("archive catalog");

    // First incarnation: one lane, recorded and ready.
    let mut plane_first = plane_with_ports(&cfg, "ingress-a", &catalog, Some(ports));
    let membership = plane_first
        .watch_topic(Topic::TxData)
        .expect("discovered plane");
    let (stop_first, ready_first, thread_first) = spawn_recorder(&cfg, &aeron_dir, membership, 1);
    let rt_first = AeronRuntime::spawn_with_dir(&aeron_dir).expect("runtime 1");
    let pub_first = plane_first
        .tx_data_publisher(&rt_first, 0)
        .await
        .expect("publisher 1");
    wait_ready(ready_first, 1).await;
    publish_until_connected(&pub_first, 1).await;
    let first = recordings(&session.archive, stream0);
    assert_eq!(first.len(), 1, "first incarnation: {first:?}");
    assert!(
        first[0]
            .channel
            .contains(&format!("control=127.0.0.1:{port}"))
    );

    // The ingress dies: its recorder thread ends, its registrations go,
    // its publication closes, and the archive marks the recording stopped.
    stop_first.cancel();
    thread_first.join().expect("recorder thread 1");
    plane_first.shutdown().await;
    drop(pub_first);
    drop(rt_first);
    wait_recording_stopped(&session.archive, stream0, first[0].id).await;
    wait_port_free(port).await;
    if let ArchiveSubscription::Stopped = archive_sub {
        let channel = std::ffi::CString::new(first[0].channel.clone()).expect("channel");
        session
            .archive
            .stop_recording_channel_and_stream(&channel, stream0)
            .expect("stop the archive subscription");
    }

    // Second incarnation on the same port.
    let mut plane_second = plane_with_ports(&cfg, "ingress-a", &catalog, Some(ports));
    let membership = plane_second
        .watch_topic(Topic::TxData)
        .expect("discovered plane");
    let (stop_second, ready_second, thread_second) =
        spawn_recorder(&cfg, &aeron_dir, membership, 1);
    let rt_second = AeronRuntime::spawn_with_dir(&aeron_dir).expect("runtime 2");
    let pub_second = plane_second
        .tx_data_publisher(&rt_second, 0)
        .await
        .expect("publisher 2");
    wait_ready(ready_second, 1).await;
    let at_ready = recordings(&session.archive, stream0);
    assert_eq!(
        at_ready.len(),
        2,
        "at ready, after first session {}: {at_ready:?}",
        first[0].session
    );
    let newest = at_ready
        .iter()
        .max_by_key(|r| r.id)
        .expect("two recordings");
    assert_ne!(
        newest.session, first[0].session,
        "a new session: {at_ready:?}"
    );
    assert_eq!(newest.stop, -1, "the new recording is live: {at_ready:?}");
    assert!(
        newest
            .channel
            .contains(&format!("control=127.0.0.1:{port}"))
    );
    publish_until_connected(&pub_second, 2).await;

    stop_second.cancel();
    thread_second.join().expect("recorder thread 2");
    plane_second.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker; run with `cargo test -p kardamom-log --features docker-e2e --test discovered_tx_data -- --ignored`"]
async fn a_restarted_publisher_is_adopted_under_the_archive_s_live_subscription() {
    restart_on_same_port(4900, 47100, ArchiveSubscription::Kept).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker; run with `cargo test -p kardamom-log --features docker-e2e --test discovered_tx_data -- --ignored`"]
async fn a_restarted_publisher_never_adopts_the_earlier_incarnation_s_recording() {
    restart_on_same_port(5000, 47110, ArchiveSubscription::Stopped).await;
}
