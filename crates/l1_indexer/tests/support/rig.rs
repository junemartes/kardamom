//! One follower in this process, in front of two independent fault
//! proxies, with the real da-watcher reading its stream and its board.
//!
//! The follower is the production `Follower::run` over the production
//! source set (`L1Endpoints::connect`, `RpcL1Source` over HTTP), a real
//! store and the real DA client. Its halts land on the process lifecycle,
//! and a production `Beacon` publishes that lifecycle as the
//! `l1-indexer` instance on the board. The da-watcher is the production
//! `L1Watcher` with that board; the batcher's pause is the production
//! `FollowerWatch` on the same board.

use std::time::Duration;

use alloy_primitives::Bytes;
use kardamom_batcher::da::DaProxy;
use kardamom_batcher::testkit_da::FakeDaProxy;
use kardamom_da_watcher::publisher::fakes::{InMemoryEpochPublisher, PublisherTap};
use kardamom_da_watcher::{CursorFile, DaWatcherConfig, L1Endpoints, L1Watcher, WatcherHandle};
use kardamom_l1_fault_proxy::FaultProxy;
use kardamom_l1_indexer::follow::{FollowConfig, Follower, FollowerParts};
use kardamom_l1_indexer::store::Store;
use kardamom_obs::events::{Beacon, BoardFeed, BoardView, Delivery, Identity, Publisher};
use kardamom_obs::follower::FollowerWatch;
use kardamom_obs::lifecycle::process;
use kardamom_types::EpochRecord;
use kardamom_types::service::{HaltCause, HaltRef, PauseReason};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

use super::chain::{FakeL1, LOCKBOX, SETTLEMENT};
use super::stream::Stream;

/// The follower's slot: the production cadence of the chaos shard is two
/// seconds; this chain moves only when the test mines.
pub(crate) const POLL: Duration = Duration::from_millis(100);
/// The da-watcher's silence window in the chaos shard.
pub(crate) const SILENCE: Duration = Duration::from_secs(30);
/// The blocks of the chain before the follower starts.
const GENESIS: u64 = 10;

/// The rig of one test.
pub(crate) struct Rig {
    pub(crate) l1: FakeL1,
    /// The first provider: the proxy that lies in the single-source cases.
    pub(crate) a: FaultProxy,
    /// The second, independent provider.
    pub(crate) b: FaultProxy,
    da: FakeDaProxy,
    pub(crate) dir: tempfile::TempDir,
    pub(crate) stream: Stream,
    pub(crate) store: Store,
    /// The follower's slot for its next start.
    pub(crate) poll: Duration,
    /// The da-watcher's housekeeping tick for its next start.
    pub(crate) watcher_tick: Duration,
    view: watch::Receiver<BoardView>,
    board: mpsc::Sender<Delivery>,
    follower: Option<JoinHandle<()>>,
    beacon: Option<JoinHandle<()>>,
    watcher: Option<WatcherHandle>,
    publisher: InMemoryEpochPublisher,
    tap: PublisherTap,
    epochs: Vec<EpochRecord>,
}

impl Rig {
    pub(crate) async fn new() -> Self {
        let l1 = FakeL1::start(GENESIS).await;
        let bind = "127.0.0.1:0".parse().unwrap();
        let a = FaultProxy::spawn(&l1.url(), bind).await.unwrap();
        let b = FaultProxy::spawn(&l1.url(), bind).await.unwrap();
        // The da-watcher syncs its cursor file after each pass; memory-backed
        // storage keeps a catch-up of thousands of passes within the
        // test's bound on a busy host.
        let dir = tempfile::tempdir_in("/dev/shm").unwrap();
        let store = Store::open(dir.path().join("archive")).unwrap();
        let (feed, board, view) = BoardFeed::new();
        tokio::spawn(feed.run());
        let (publisher, tap) = InMemoryEpochPublisher::new();
        Self {
            l1,
            a,
            b,
            da: FakeDaProxy::start(),
            dir,
            stream: Stream::default(),
            store,
            poll: POLL,
            watcher_tick: Duration::from_millis(20),
            view,
            board,
            follower: None,
            beacon: None,
            watcher: None,
            publisher,
            tap,
            epochs: Vec::new(),
        }
    }

    /// The follower's configuration: the chain's contracts from block 1.
    pub(crate) fn follow_config() -> FollowConfig {
        FollowConfig {
            settlement: SETTLEMENT,
            lockbox: LOCKBOX,
            start_block: Some(1),
            poll_interval: POLL,
            blocks_per_tick: std::num::NonZeroU64::new(64).unwrap(),
            max_log_range: std::num::NonZeroU64::new(10).unwrap(),
            schedule: None,
        }
    }

    /// Start the follower from its store, and its beacon.
    pub(crate) async fn start_follower(&mut self) {
        let source = L1Endpoints {
            rpcs: vec![self.a.url(), self.b.url()],
            light_client: None,
        }
        .connect()
        .await
        .unwrap();
        let follower = Follower::open(FollowerParts {
            source,
            sink: self.stream.sink(),
            da: DaProxy::new(self.da.url()).unwrap(),
            store: self.store.clone(),
            cfg: FollowConfig {
                poll_interval: self.poll,
                ..Self::follow_config()
            },
        })
        .unwrap();
        self.follower = Some(tokio::spawn(follower.run()));
        let board = self.board.clone();
        let identity = Identity {
            service: HaltRef::L1_FOLLOWER.into(),
            instance: "l1-indexer-0".into(),
        };
        let beacon = Beacon::new(identity, process().subscribe()).run(move |event| {
            let _ = board.try_send(Delivery {
                publisher: Publisher(1),
                event: event.clone(),
            });
        });
        self.beacon = Some(tokio::spawn(beacon));
    }

    /// End the follower task where it stands, as a crash ends it.
    pub(crate) fn stop_follower(&mut self) {
        self.follower
            .take()
            .into_iter()
            .for_each(|task| task.abort());
        self.beacon.take().into_iter().for_each(|task| task.abort());
    }

    /// Start the da-watcher from its cursor file, with the board and the
    /// silence window of the chaos shard.
    pub(crate) fn start_watcher(&mut self) {
        self.start_watcher_with(SILENCE);
    }

    /// Start the da-watcher from its cursor file, with the board.
    pub(crate) fn start_watcher_with(&mut self, silence: Duration) {
        let config = DaWatcherConfig {
            tick: self.watcher_tick,
            silence,
            resume_after: None,
            epoch_pace: Duration::ZERO,
        };
        let cursor = CursorFile::open(self.dir.path().join("l1-cursor")).unwrap();
        let watcher = L1Watcher::new(
            self.publisher.clone(),
            self.stream.feed(),
            config,
            Some(cursor),
        )
        .with_board(self.view.clone());
        self.watcher = Some(watcher.start());
    }

    /// Stop the da-watcher cleanly: it writes its cursor file on the way
    /// out, as a redeploy's stop signal makes it do.
    pub(crate) async fn stop_watcher(&mut self) {
        let Some(handle) = self.watcher.take() else {
            return;
        };
        handle.join().await.unwrap();
    }

    /// Disperse the payload of batch `index`; its certificate.
    pub(crate) async fn disperse(&self, index: u64) -> Bytes {
        let payload = format!("payload of batch {index}");
        DaProxy::new(self.da.url())
            .unwrap()
            .put(payload.as_bytes())
            .await
            .unwrap()
    }

    /// Disperse the payload of batch `index` and post it on L1 in a new
    /// block, with one deposit beside it.
    pub(crate) async fn post(&self, index: u64) -> u64 {
        let cert = self.disperse(index).await;
        self.l1.post(index, cert, index.checked_mul(1_000).unwrap())
    }

    /// The sender of the board's records, for followers in other
    /// processes.
    pub(crate) fn board(&self) -> mpsc::Sender<Delivery> {
        self.board.clone()
    }

    /// The DA store's URL.
    pub(crate) fn da_url(&self) -> String {
        self.da.url()
    }

    /// The cause of the follower's halt, when one stands.
    pub(crate) fn halt() -> Option<HaltCause> {
        process().slots().halt.map(|halt| halt.cause)
    }

    /// The root the da-watcher is paused on, when it is paused upstream.
    pub(crate) fn watcher_root() -> Option<HaltRef> {
        process()
            .slots()
            .pause
            .and_then(|pause| match pause.reason {
                PauseReason::Upstream(root) => Some(root),
                PauseReason::Operator { .. } => None,
            })
    }

    /// The root the batcher pauses on, read through its production watch
    /// of the same board.
    pub(crate) fn batcher_root(&self) -> Option<HaltRef> {
        FollowerWatch::new(SILENCE)
            .with_board(self.view.clone())
            .root(tokio::time::Instant::now(), false)
    }

    /// Every epoch the da-watcher published so far, in order.
    pub(crate) fn epochs(&mut self) -> &[EpochRecord] {
        self.epochs.extend(self.tap.epochs());
        &self.epochs
    }

    /// The last block whose epoch the da-watcher published.
    pub(crate) fn published_through(&mut self) -> Option<u64> {
        self.epochs().last().map(|epoch| epoch.l1_number)
    }
}

impl Drop for Rig {
    fn drop(&mut self) {
        self.stop_follower();
        self.watcher
            .take()
            .into_iter()
            .for_each(|handle| handle.task.abort());
    }
}

/// Wait until `ready` holds, for at most `limit` of wall time.
pub(crate) async fn wait(what: &str, limit: Duration, mut ready: impl FnMut() -> bool) {
    kardamom_obs::testkit::poll_until(what, limit, Duration::from_millis(10), async || {
        Ok(ready().then_some(()))
    })
    .await
    .unwrap();
}
