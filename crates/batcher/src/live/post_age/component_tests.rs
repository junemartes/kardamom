//! The real post-age actor with a silent archive and controlled follower input.

use std::future::Future;
use std::sync::Arc;

use metrics::{
    Counter, Gauge, GaugeFn, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit,
};
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use super::*;

const EVERY: Duration = Duration::from_secs(10);

impl BlockInput for mpsc::Receiver<(BPosition, L1Block)> {
    async fn recv(&mut self) -> Option<(BPosition, L1Block)> {
        self.recv().await
    }
}

pub(super) struct SilentArchive {
    pub(super) url: String,
    entered: oneshot::Receiver<()>,
    task: JoinHandle<()>,
}

impl SilentArchive {
    pub(super) async fn new() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let (entered, receive) = oneshot::channel();
        let task = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            socket.read_exact(&mut [0]).await.unwrap();
            entered.send(()).unwrap();
            let _ = socket.read_to_end(&mut Vec::new()).await;
        });
        Self {
            url,
            entered: receive,
            task,
        }
    }

    pub(super) async fn wait_for_request(&mut self) {
        tokio::time::timeout(Duration::from_secs(5), &mut self.entered)
            .await
            .expect("the actual archive request must reach the peer before time advances")
            .unwrap();
    }
}

impl Drop for SilentArchive {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct AgeGauge(watch::Sender<Option<f64>>);

impl GaugeFn for AgeGauge {
    fn increment(&self, _: f64) {
        panic!("post age is an absolute gauge");
    }
    fn decrement(&self, _: f64) {
        panic!("post age is an absolute gauge");
    }
    fn set(&self, value: f64) {
        self.0.send_replace(Some(value));
    }
}

struct AgeRecorder(Arc<AgeGauge>);

impl Recorder for AgeRecorder {
    fn describe_counter(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_gauge(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_histogram(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn register_counter(&self, _: &Key, _: &Metadata<'_>) -> Counter {
        Counter::noop()
    }
    fn register_histogram(&self, _: &Key, _: &Metadata<'_>) -> Histogram {
        Histogram::noop()
    }
    fn register_gauge(&self, key: &Key, _: &Metadata<'_>) -> Gauge {
        if key.name() == live_metric_names::LAST_POST_AGE {
            Gauge::from_arc(self.0.clone())
        } else {
            Gauge::noop()
        }
    }
}

impl AgeRecorder {
    async fn record<F: Future>(self, future: F) -> F::Output {
        tokio::pin!(future);
        std::future::poll_fn(|cx| metrics::with_local_recorder(&self, || future.as_mut().poll(cx)))
            .await
    }
}

struct Actor(JoinHandle<()>);

impl Actor {
    async fn finish(mut self) {
        tokio::time::timeout(Duration::from_secs(1), &mut self.0)
            .await
            .expect("input closure must stop the actor while the archive is unresponsive")
            .unwrap();
    }
}

impl Drop for Actor {
    fn drop(&mut self) {
        self.0.abort();
    }
}

struct Fixture {
    archive: Option<SilentArchive>,
    input: mpsc::Sender<(BPosition, L1Block)>,
    age: watch::Receiver<Option<f64>>,
    actor: Actor,
}

impl Fixture {
    async fn new() -> Self {
        let archive = SilentArchive::new().await;
        let mut fixture = Self::using(IndexerClient::new(&archive.url));
        fixture.archive = Some(archive);
        fixture
    }

    fn using(indexer: IndexerClient) -> Self {
        let (input, blocks) = mpsc::channel(2);
        let (_, board) = watch::channel(BoardView::default());
        let (age, receive) = watch::channel(None);
        let observer = PostAge::new(blocks, board, Some(indexer), Duration::from_secs(60), EVERY);
        let recorder = AgeRecorder(Arc::new(AgeGauge(age)));
        let actor = Actor(tokio::spawn(recorder.record(observer.run())));
        Self {
            archive: None,
            input,
            age: receive,
            actor,
        }
    }

    async fn fault_reached(&mut self) {
        self.archive.as_mut().unwrap().wait_for_request().await;
        assert_eq!(*self.age.borrow(), None);
        tokio::time::pause();
    }

    fn block() -> L1Block {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        L1Block {
            number: 7,
            timestamp: now.checked_sub(120).unwrap(),
            batches: vec![kardamom_types::BatchEntry {
                index: 1,
                da_cert: alloy_primitives::Bytes::from_static(&[3]),
                l2_block_start: 1,
                l2_block_end: 2,
                records_commitment: alloy_primitives::B256::ZERO,
                l1_block: 7,
                l1_tx: alloy_primitives::B256::ZERO,
            }],
            ..L1Block::default()
        }
    }

    async fn observe_live_post(&mut self) {
        let block = Self::block();
        let timestamp = block.timestamp;
        let earliest = Self::age_now(timestamp);
        self.input
            .send((BPosition::from_index(1), block))
            .await
            .unwrap();
        let value = *tokio::time::timeout(
            EVERY.checked_add(Duration::from_secs(1)).unwrap(),
            self.age.wait_for(Option::is_some),
        )
        .await
        .expect("the live stream must refresh post age while the archive is unresponsive")
        .unwrap();
        let age = value.unwrap();
        assert!(
            (earliest..=Self::age_now(timestamp)).contains(&age),
            "age comes from the actual live block: {age}"
        );
    }

    fn age_now(timestamp: u64) -> f64 {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        f64::from(u32::try_from(now.checked_sub(timestamp).unwrap()).unwrap())
    }

    async fn close(self) {
        let Self {
            archive: _archive,
            input,
            actor,
            ..
        } = self;
        Self::close_input(input);
        actor.finish().await;
    }

    fn close_input(_input: mpsc::Sender<(BPosition, L1Block)>) {}
}

#[tokio::test]
async fn an_unresponsive_archive_does_not_hide_a_live_post() {
    let mut fixture = Fixture::new().await;
    fixture.fault_reached().await;
    fixture.observe_live_post().await;
    fixture.close().await;
}

#[tokio::test]
async fn input_close_stops_the_actor_during_an_unresponsive_archive_read() {
    let mut fixture = Fixture::new().await;
    fixture.fault_reached().await;
    fixture.close().await;
}

mod archive;
