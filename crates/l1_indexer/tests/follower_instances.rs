//! The two L1 follower instances as real processes, each over its own two
//! providers, and the real da-watcher reading their shared stream and
//! their records on the board.
//!
//! - One instance frozen: the other carries the stream. The da-watcher
//!   never pauses and publishes each block's epoch once, with no gap.
//! - Both instances killed past the silence window: the da-watcher pauses
//!   with the stream as its root. The restarted processes resume from
//!   their archives, and the da-watcher resumes with no gap and no double
//!   epoch.
//! - One instance reads a fork that is consistent in itself: the
//!   da-watcher halts on the disagreement of the two instances. The
//!   runbook heals it: the fault ends, the liar's archive is wiped, the
//!   liar restarts, the operator clears the halt, and the da-watcher
//!   publishes only true epochs again.
//!
//! The process lifecycle is global, so the binary runs one test at a time.

mod support;

use std::time::{Duration, Instant};

use kardamom_l1_fault_proxy::{Fault, FaultProxy};
use kardamom_obs::lifecycle::process;
use kardamom_types::service::{HaltCause, HaltRef};
use support::child::{ChildMain, Hub, Instance, Launch};
use support::rig::{Rig, wait};

/// The da-watcher's silence window here: short, so a total loss crosses
/// it in seconds. The chaos shard uses 30 seconds.
const SILENCE: Duration = Duration::from_secs(3);
/// How long one instance stays frozen: past the board's 15-second expiry
/// of its last heartbeat.
const FREEZE: Duration = Duration::from_secs(20);
/// How long both instances stay down: past twice the silence window.
const OUTAGE: Duration = Duration::from_secs(7);
/// How many true blocks past the fork's start the da-watcher takes before
/// the liar thaws.
const TRUE_LEAD: u64 = 3;
/// How long the da-watcher's halt must stay clear after the operator's
/// clear.
const CLEAR_HOLD: Duration = Duration::from_secs(3);
const LIMIT: Duration = Duration::from_secs(15);

#[test]
#[ignore = "child entry point; the parent starts and signals this process"]
fn follower_instance_child() {
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(ChildMain::run());
}

/// Two instances, the providers of the second, and the hub.
struct Fleet {
    rig: Rig,
    c: FaultProxy,
    d: FaultProxy,
    hub: Hub,
    instances: [Option<Instance>; 2],
    next_batch: u64,
}

impl Fleet {
    async fn start() -> Self {
        process().clear();
        process().resume();
        let mut rig = Rig::new().await;
        let bind = "127.0.0.1:0".parse().unwrap();
        let c = FaultProxy::spawn(&rig.l1.url(), bind).await.unwrap();
        let d = FaultProxy::spawn(&rig.l1.url(), bind).await.unwrap();
        let hub = Hub::start(rig.stream.clone(), rig.board()).await;
        rig.start_watcher_with(SILENCE);
        let mut fleet = Self {
            rig,
            c,
            d,
            hub,
            instances: [None, None],
            next_batch: 1,
        };
        fleet.launch(0);
        fleet.launch(1);
        fleet.post().await;
        fleet.await_caught_up().await;
        fleet
    }

    /// The archive directory of instance `i`: the rig's archive for the
    /// first.
    fn dir(&self, i: usize) -> std::path::PathBuf {
        let name = if i == 0 { "archive" } else { "archive-1" };
        self.rig.dir.path().join(name)
    }

    fn launch(&mut self, i: usize) {
        let rpcs = if i == 0 {
            [self.rig.a.url(), self.rig.b.url()]
        } else {
            [self.c.url(), self.d.url()]
        };
        let dir = self.dir(i);
        let instance = Instance::spawn(&Launch {
            name: &format!("l1-indexer-{i}"),
            rpcs,
            da: self.rig.da_url(),
            dir: &dir,
            hub: &self.hub,
        });
        self.instances[i] = Some(instance);
    }

    fn instance(&self, i: usize) -> &Instance {
        self.instances[i].as_ref().unwrap()
    }

    /// Post the next batch in a new block, and finalize it.
    async fn post(&mut self) {
        self.rig.post(self.next_batch).await;
        self.next_batch += 1;
        self.rig.l1.mine(2);
    }

    fn last_batch(&self) -> u64 {
        self.next_batch - 1
    }

    /// The da-watcher published through the finalized block, the first
    /// instance archived the last batch, and neither a halt nor a pause
    /// stands.
    async fn await_caught_up(&mut self) {
        let finalized = self.rig.l1.finalized();
        let last_batch = Some(self.last_batch());
        let rig = &mut self.rig;
        wait("the da-watcher catches up", LIMIT, || {
            rig.published_through() == Some(finalized)
                && rig.store.cursor().unwrap().last_batch == last_batch
                && Rig::halt().is_none()
                && Rig::watcher_root().is_none()
        })
        .await;
    }

    /// One step of a window: a post, then a sample a quarter second on.
    async fn step(&mut self) -> Option<HaltRef> {
        self.post().await;
        tokio::time::sleep(Duration::from_millis(250)).await;
        Rig::watcher_root()
    }
}

#[tokio::test]
async fn one_frozen_instance_leaves_the_stream_to_the_other_without_a_pause() {
    let mut fleet = Fleet::start().await;
    fleet.instance(1).freeze().await;
    let base = fleet.rig.published_through();
    let started = Instant::now();
    while started.elapsed() < FREEZE {
        assert_eq!(fleet.step().await, None, "the da-watcher must not pause");
    }
    assert!(
        fleet.rig.published_through() > base,
        "the other instance carried the stream"
    );
    fleet.instance(1).thaw();
    fleet.await_caught_up().await;
    let finalized = fleet.rig.l1.finalized();
    fleet.rig.assert_epochs_once_through(finalized);
    fleet.rig.assert_stream_true_through(finalized);
    fleet.rig.assert_archive_through(fleet.last_batch());
}

#[tokio::test]
async fn both_instances_lost_pause_the_watcher_and_their_restart_resumes_it() {
    let mut fleet = Fleet::start().await;
    let stuck = fleet.rig.published_through();
    fleet
        .instances
        .iter_mut()
        .for_each(|i| i.take().unwrap().kill());
    let started = Instant::now();
    let mut root = None;
    while started.elapsed() < OUTAGE {
        root = fleet.step().await.or(root);
    }
    assert_eq!(root, Some(HaltRef::l1_blocks_silent()));
    assert_eq!(Rig::watcher_root(), Some(HaltRef::l1_blocks_silent()));
    assert_eq!(
        fleet.rig.published_through(),
        stuck,
        "no epoch without a follower"
    );
    fleet.launch(0);
    fleet.launch(1);
    fleet.await_caught_up().await;
    let finalized = fleet.rig.l1.finalized();
    fleet.rig.assert_epochs_once_through(finalized);
    fleet.rig.assert_stream_true_through(finalized);
    fleet.rig.assert_archive_through(fleet.last_batch());
}

#[tokio::test]
async fn a_forked_instance_halts_the_watcher_until_the_runbook_heals_it() {
    let mut fleet = Fleet::start().await;
    fleet.instance(1).freeze().await;
    let from_block = fleet.rig.l1.head() + 1;
    let fork = Fault::ForkedChain {
        from_block,
        client: None,
    };
    fleet.c.set_fault(fork);
    fleet.d.set_fault(fork);
    (0..TRUE_LEAD).for_each(|_| fleet.rig.l1.mine(1));
    fleet.post().await;
    let lead = from_block + TRUE_LEAD;
    let rig = &mut fleet.rig;
    wait("the true lead", LIMIT, || {
        rig.published_through() >= Some(lead)
    })
    .await;
    fleet.instance(1).thaw();
    wait("the halt on the disagreement", LIMIT, || {
        Rig::halt() == Some(HaltCause::L1FollowerDisagreement)
    })
    .await;
    let forked = fleet.rig.stream.records();
    assert!(
        forked
            .iter()
            .any(|record| record != &fleet.rig.l1.record(record.number)),
        "the liar published its fork"
    );
    fleet.heal().await;
    let finalized = fleet.rig.l1.finalized();
    fleet.rig.assert_epochs_once_through(finalized);
    fleet.rig.assert_archive_through(fleet.last_batch());
}

impl Fleet {
    /// The runbook: end the fault, wipe the liar's archive, restart it,
    /// clear the da-watcher's halt, and see it publish again and stay
    /// clear.
    async fn heal(&mut self) {
        self.c.set_fault(Fault::None);
        self.d.set_fault(Fault::None);
        self.instances[1].take().unwrap().kill();
        std::fs::remove_dir_all(self.dir(1)).unwrap();
        self.launch(1);
        let before = self.rig.published_through();
        assert!(
            kardamom_obs::halt::clear().is_some(),
            "the operator clears the halt"
        );
        self.post().await;
        let rig = &mut self.rig;
        wait("a publish after the clear", LIMIT, || {
            rig.published_through() > before
        })
        .await;
        let started = Instant::now();
        while started.elapsed() < CLEAR_HOLD {
            assert_eq!(self.step().await, None);
            assert_eq!(Rig::halt(), None, "the halt stays clear");
        }
        self.await_caught_up().await;
    }
}
