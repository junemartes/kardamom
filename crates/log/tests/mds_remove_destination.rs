//! Real-Aeron check of the receive-destination removal that #545 named:
//! one manual subscription joins four dynamic MDC publishers, one
//! destination each. Two publishers stream while the subscription
//! removes the other two destinations. Every frame of the live streams
//! must still arrive in order, and the media driver must log no error.
//!
//! Gated on the `docker-e2e` feature and on Docker availability.

#![cfg(feature = "docker-e2e")]

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use kardamom_log::aeron_live::{AeronRuntime, PubHandle, RawFrame};
use kardamom_log::testing::{AeronTestCluster, SingleNodeRig};
use rkyv::util::AlignedVec;
use tokio::sync::mpsc::UnboundedReceiver;

/// Stream id 4601 keeps this test apart from the other e2e tests.
const STREAM: i32 = 4601;
const FIRST_PORT: u16 = 41_100;
const PUBLISHERS: u16 = 4;
/// The frames each live publisher sends: half before the removal, half
/// after it.
const FRAMES: u32 = 400;

fn control_uri(port: u16) -> String {
    format!("aeron:udp?control=127.0.0.1:{port}|control-mode=dynamic")
}

fn destination(port: u16) -> String {
    format!("aeron:udp?endpoint=127.0.0.1:0|control=127.0.0.1:{port}|control-mode=dynamic")
}

/// A frame of publisher `id` with sequence number `seq`.
fn frame(id: u8, seq: u32) -> AlignedVec {
    let mut v = AlignedVec::new();
    v.push(id);
    v.extend_from_slice(&seq.to_le_bytes());
    v.extend_from_slice(&[0; 59]);
    v
}

/// Send `range` frames of publisher `id`, with a short pause each, so
/// that the stream runs while the subscription changes.
fn stream(
    publisher: PubHandle,
    id: u8,
    range: std::ops::Range<u32>,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        range.for_each(|seq| {
            publisher.publish_bytes(frame(id, seq)).expect("publish");
            std::thread::sleep(Duration::from_millis(2));
        });
    })
}

/// The sequence numbers each publisher delivered, in arrival order.
/// Frames of returned publishers carry ids 20 and above; they are not
/// part of the count.
struct Delivered(BTreeMap<u8, Vec<u32>>);

impl Delivered {
    async fn collect(rx: &mut UnboundedReceiver<RawFrame>, want: usize, within: Duration) -> Self {
        let deadline = Instant::now() + within;
        let mut seen = Self(BTreeMap::new());
        while seen.count() < want && Instant::now() < deadline {
            seen.take(tokio::time::timeout(Duration::from_millis(200), rx.recv()).await);
        }
        seen
    }

    fn count(&self) -> usize {
        self.0.values().map(Vec::len).sum()
    }

    fn take(&mut self, received: Result<Option<RawFrame>, tokio::time::error::Elapsed>) {
        let Ok(Some(f)) = received else {
            return;
        };
        let seq = u32::from_le_bytes(f.bytes[1..5].try_into().expect("four bytes"));
        if f.bytes[0] < 20 {
            self.0.entry(f.bytes[0]).or_default().push(seq);
        }
    }

    /// Publisher `id` delivered exactly `0..FRAMES`, in order.
    fn assert_whole(&self, id: u8) {
        let got = self.0.get(&id).cloned().unwrap_or_default();
        assert_eq!(
            got,
            (0..FRAMES).collect::<Vec<_>>(),
            "publisher {id} lost or reordered frames"
        );
    }
}

/// The driver, the subscriber runtime, its manual subscription, and the
/// four publishers.
struct Rig {
    cluster: AeronTestCluster,
    rt: AeronRuntime,
    sub_id: u32,
    rx: UnboundedReceiver<RawFrame>,
    publishers: Vec<PubHandle>,
}

impl Rig {
    /// Four publishers, one destination each, and one frame from each, so
    /// that every destination holds an image.
    async fn start() -> Self {
        let SingleNodeRig { cluster, rt, .. } = AeronTestCluster::single_node_runtime(4600).await;
        let publishers = (0..PUBLISHERS)
            .map(|i| {
                rt.open_publication(&control_uri(FIRST_PORT + i), STREAM)
                    .expect("publisher")
            })
            .collect();
        let (sub_id, rx) = rt
            .open_subscription_raw("aeron:udp?control-mode=manual", STREAM)
            .expect("manual subscription");
        let mut rig = Self {
            cluster,
            rt,
            sub_id,
            rx,
            publishers,
        };
        (0..PUBLISHERS).for_each(|i| rig.add(i));
        rig.publishers.iter().zip(0u8..).for_each(|(p, id)| {
            p.publish_bytes(frame(id + 10, 0)).expect("warm-up");
        });
        let warm = Delivered::collect(
            &mut rig.rx,
            usize::from(PUBLISHERS),
            Duration::from_secs(10),
        )
        .await;
        assert_eq!(
            warm.0.len(),
            usize::from(PUBLISHERS),
            "every publisher is an image"
        );
        rig
    }

    fn add(&self, i: u16) {
        self.rt
            .add_destination(self.sub_id, &destination(FIRST_PORT + i))
            .expect("add");
    }

    fn remove(&self, i: u16) {
        self.rt
            .remove_destination(self.sub_id, &destination(FIRST_PORT + i))
            .expect("remove");
    }

    /// A node dies and returns: a new runtime opens the control endpoint
    /// of destination 1 (a new session), sends, ends, and the subscription
    /// removes that destination.
    async fn node_returns(&self, round: u8) {
        let dir = self.cluster.aeron_dir_host(0).to_path_buf();
        let node = AeronRuntime::spawn_with_dir(&dir).expect("node runtime");
        let publisher = node
            .open_publication(&control_uri(FIRST_PORT + 1), STREAM)
            .expect("returned publisher");
        self.add(1);
        let _ = publisher.publish_bytes(frame(40 + round, 0));
        tokio::time::sleep(Duration::from_millis(300)).await;
        Self::end_node(publisher, node);
        tokio::time::sleep(Duration::from_millis(300)).await;
        self.remove(1);
    }

    fn end_node(_publisher: PubHandle, _node: AeronRuntime) {}

    /// Destinations 1 and 2 go back on, their publishers send, and the
    /// destinations come off again.
    async fn rejoin_and_leave(&self, round: u8) {
        self.add(1);
        self.add(2);
        self.publishers[1]
            .publish_bytes(frame(20 + round, 0))
            .expect("returned publisher");
        self.publishers[2]
            .publish_bytes(frame(30 + round, 0))
            .expect("returned publisher");
        tokio::time::sleep(Duration::from_millis(300)).await;
        self.remove(1);
        self.remove(2);
    }

    /// Publishers 0 and 3 send `range`, each on its own thread.
    fn live(&self, range: std::ops::Range<u32>) -> [std::thread::JoinHandle<()>; 2] {
        [
            stream(self.publishers[0].clone(), 0, range.clone()),
            stream(self.publishers[3].clone(), 3, range),
        ]
    }

    fn join(threads: [std::thread::JoinHandle<()>; 2]) {
        for thread in threads {
            thread.join().expect("publisher thread");
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker; run with `cargo test -p kardamom-log --features docker-e2e --test mds_remove_destination -- --ignored`"]
async fn removing_receive_destinations_keeps_the_live_images_and_logs_no_driver_error() {
    kardamom_log::testing::require_docker().await;
    let mut rig = Rig::start().await;
    for round in 0..3 {
        rig.node_returns(round).await;
    }
    let half = FRAMES / 2;
    let first = rig.live(0..half);
    tokio::time::sleep(Duration::from_millis(200)).await;
    rig.remove(2);
    rig.remove(1);
    for round in 0..3 {
        rig.rejoin_and_leave(round).await;
    }
    Rig::join(first);
    // A removed destination lingers for one stall budget before it
    // closes. The second half goes out after that, so a removal that
    // closed a live destination too shows as lost frames.
    tokio::time::sleep(rig.rt.stall_budget() + Duration::from_secs(1)).await;
    Rig::join(rig.live(half..FRAMES));
    let want = 2 * usize::try_from(FRAMES).unwrap();
    let got = Delivered::collect(&mut rig.rx, want, Duration::from_secs(20)).await;
    let errors = rig.cluster.driver_errors(0).await.expect("ErrorStat");
    eprintln!("driver error log:\n{errors}");
    got.assert_whole(0);
    got.assert_whole(3);
    assert!(
        !errors.contains("Exception") && !errors.contains("removeDestination"),
        "the media driver logged an error on the destination removal:\n{errors}"
    );
}
