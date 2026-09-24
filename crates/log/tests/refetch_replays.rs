//! Real-Aeron check of the join-miss refetch client against bounded
//! replays of one recording, one right after the other, as the executor's
//! reader issues them when `TxRef`s miss in turn: each replay starts one
//! frame before the last. Each replay must deliver the frame at its own
//! start position, under the recorded session.
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
    Catalog, DiscoveredRecorder, Instance, RecorderProgress, StreamPlane, Topic,
};
use kardamom_log::recorder::connect_archive;
use kardamom_log::refetch::{ArchiveRefetcher, EndpointSource, RefetchConfig};
use kardamom_log::testing::{AeronTestCluster, SingleNodeRig};
use kardamom_types::{BPosition, TxDataLoc};
use tokio_util::sync::CancellationToken;

/// The envelopes the recording holds, published once each.
const ENVELOPES: u64 = 40;
/// The archive endpoints as the container's own media driver reaches them.
const ARCHIVE_CONTROL: &str = "127.0.0.1:8010";
const REFETCH_RESPONSE: &str = "127.0.0.1:8012";
const REFETCH_REPLAY: &str = "127.0.0.1:8013";

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
    StreamPlane::with_catalog(
        cfg,
        label,
        Catalog::Memory(catalog.clone()),
        Instance {
            id: format!("alloc-{label}"),
            ports: None,
        },
        Ipv4Addr::LOCALHOST,
    )
}

fn spawn_recorder(
    cfg: &LogConfig,
    aeron_dir: &std::path::Path,
    membership: tokio::sync::watch::Receiver<kardamom_log::discovery::Membership>,
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
        expected_own: 1,
        removal_grace: cfg.discovery.removal_grace(),
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

/// Publish envelope `id` until `sub` sees it: the subscriber joins late,
/// and Aeron never replays history to a live subscription.
async fn publish_until_seen(
    publisher: &TxDataPublisherHandle,
    sub: &mut TxDataSubscription,
    id: u64,
) {
    let deadline = Instant::now() + Duration::from_secs(15);
    let want = BTreeSet::from([id]);
    while Instant::now() < deadline {
        let _ = publisher.publish(&common::tx_envelope(id, 1, 32));
        if collect(sub, &want, Duration::from_millis(300))
            .await
            .contains_key(&id)
        {
            return;
        }
    }
    panic!("envelope {id} never reached the subscriber");
}

/// The location of every wanted envelope received within `budget`.
async fn collect(
    sub: &mut TxDataSubscription,
    want: &BTreeSet<u64>,
    budget: Duration,
) -> BTreeMap<u64, TxDataLoc> {
    let mut seen = BTreeMap::new();
    let deadline = Instant::now() + budget;
    while !want.iter().all(|id| seen.contains_key(id)) && Instant::now() < deadline {
        if let Ok(Some((loc, env))) =
            tokio::time::timeout(Duration::from_millis(200), sub.recv()).await
        {
            seen.insert(env.correlation_id, loc);
        }
    }
    seen
}

/// `(recording id, stop position)` of every recording of `stream_id`.
fn recordings(archive: &rusteron_archive::AeronArchive, stream_id: i32) -> Vec<(i64, i64)> {
    struct Capture(Rc<RefCell<Vec<(i64, i64)>>>);
    impl rusteron_archive::AeronArchiveRecordingDescriptorConsumerFuncCallback for Capture {
        fn handle_aeron_archive_recording_descriptor_consumer_func(
            &mut self,
            desc: rusteron_archive::AeronArchiveRecordingDescriptor,
        ) {
            self.0
                .borrow_mut()
                .push((desc.recording_id(), desc.stop_position()));
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

async fn wait_recording_stopped(archive: &rusteron_archive::AeronArchive, stream: i32) {
    let deadline = Instant::now() + Duration::from_mins(1);
    loop {
        let recs = recordings(archive, stream);
        if !recs.is_empty() && recs.iter().all(|(_, stop)| *stop >= 0) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "the recording never stopped: {recs:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Bounded replays one after the other, on the reader's thread model:
/// the refetch client is thread-confined, so one blocking task builds it
/// and runs every replay. Returns the delivered positions of each replay
/// in delivery order.
async fn fetch_each(
    cfg: RefetchConfig,
    stream_id: i32,
    session_id: i32,
    froms: [BPosition; 3],
) -> [Vec<BPosition>; 3] {
    tokio::task::spawn_blocking(move || {
        let mut refetcher = ArchiveRefetcher::new(cfg);
        froms.map(|from| {
            let mut got = Vec::new();
            let delivered = refetcher
                .fetch_tx_data(stream_id, session_id, from, |loc, _env| {
                    assert_eq!(
                        loc.session_id, session_id,
                        "a replayed frame keeps its recorded session"
                    );
                    got.push(loc.position);
                })
                .expect("refetch");
            assert_eq!(delivered, u64::try_from(got.len()).expect("small count"));
            got
        })
    })
    .await
    .expect("refetch task")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker; run with `cargo test -p kardamom-log --features docker-e2e --test refetch_replays -- --ignored`"]
async fn replays_in_a_row_each_deliver_their_first_frame() {
    kardamom_log::testing::require_docker().await;
    let SingleNodeRig { cluster, rt, cfg } = AeronTestCluster::single_node_runtime(4900).await;
    let cfg = config(&cfg);
    let catalog = MemoryCatalog::new();
    let aeron_dir = cluster.aeron_dir_host(0).to_path_buf();
    let stream0 = cfg.channels.tx_data_stream_id(0);

    // One recorded publisher of lane 0, and a late subscriber that reads
    // the location of every envelope.
    let mut plane_rec = plane(&cfg, "ingress-a", &catalog);
    let membership = plane_rec
        .watch_topic(Topic::TxData)
        .expect("discovered plane");
    let (stop, ready_rx, recorder_thread) = spawn_recorder(&cfg, &aeron_dir, membership);
    let rt_a = AeronRuntime::spawn_with_dir(&aeron_dir).expect("runtime a");
    let publisher = plane_rec
        .tx_data_publisher(&rt_a, 0)
        .await
        .expect("publisher");
    let progress = tokio::time::timeout(Duration::from_secs(20), ready_rx)
        .await
        .expect("recorder readiness within the budget")
        .expect("recorder thread alive");
    assert_eq!(progress, RecorderProgress::Ready { own_recordings: 1 });
    let mut plane_sub = plane(&cfg, "executor", &catalog);
    let mut sub0 = plane_sub
        .tx_data_subscription(&rt, 0)
        .expect("lane 0 subscription");
    publish_until_seen(&publisher, &mut sub0, 0).await;
    for id in 1..=ENVELOPES {
        publisher
            .publish(&common::tx_envelope(id, 1, 32))
            .expect("publish once the subscriber is connected");
    }
    let want: BTreeSet<u64> = (1..=ENVELOPES).collect();
    let locs = collect(&mut sub0, &want, Duration::from_secs(10)).await;
    assert_eq!(
        locs.len() as u64,
        ENVELOPES,
        "every envelope reached the subscriber"
    );
    let session_id = locs[&1].session_id;

    // The publisher dies, so the recording stops: the replay bound is
    // static, as it is for a restarted executor replaying old refs.
    stop.cancel();
    recorder_thread.join().expect("recorder thread");
    plane_rec.shutdown().await;
    drop(publisher);
    drop(rt_a);
    let archive = connect_archive(Some(&aeron_dir), &cfg.aeron).expect("archive catalog");
    wait_recording_stopped(&archive.archive, stream0).await;

    // Two replays as the reader issues them on two join misses in turn:
    // the second one starts one frame before the first.
    let refetch_cfg = RefetchConfig {
        tx_data_endpoints: EndpointSource::Static(vec![ARCHIVE_CONTROL.to_string()]),
        tx_deposits_endpoints: EndpointSource::Static(Vec::new()),
        response_endpoint: REFETCH_RESPONSE.to_string(),
        replay_endpoint: REFETCH_REPLAY.to_string(),
        aeron_dir: Some(aeron_dir.clone()),
        aeron: cfg.aeron.clone(),
    };
    let k = ENVELOPES / 2;
    let replays = fetch_each(
        refetch_cfg,
        stream0,
        session_id,
        [
            locs[&k].position,
            locs[&(k - 1)].position,
            locs[&(k - 2)].position,
        ],
    )
    .await;
    for (i, got) in replays.iter().enumerate() {
        let start = k - u64::try_from(i).expect("small index");
        assert_eq!(
            got.first(),
            Some(&locs[&start].position),
            "replay {i} starts at its frame: {got:?}"
        );
        assert_eq!(
            u64::try_from(got.len()).expect("small count"),
            ENVELOPES - start + 1,
            "replay {i}: {got:?}"
        );
    }
    plane_sub.shutdown().await;
}
