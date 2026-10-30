//! The L1 follower quorum under the faults of the chaos L1 cases, with the
//! real follower, the real source set over HTTP, and two independent
//! fault proxies in front of one controlled L1.
//!
//! - A refusing provider (down, rate-limited, unresponsive) rotates out
//!   and halts the follower on `l1_unreachable`; the source returns after
//!   its unchanged 30-second backoff, which the paused clock crosses.
//! - A lie of one provider (a wrong hash, a broken parent chain,
//!   swallowed settlement logs, null receipts) is a disagreement: the
//!   follower halts on `l1_source_disagreement` and publishes none of it.
//! - In every case the da-watcher and the batcher pause with the follower
//!   as their root, and all of them resume by themselves when the fault
//!   ends. The stream then carries every true record, the da-watcher
//!   publishes every epoch once, and the archive holds every batch.
//!
//! The process lifecycle is global, so the binary runs one test at a time.

mod support;

use std::time::Duration;

use kardamom_l1_fault_proxy::{Fault, FaultProxy};
use kardamom_obs::lifecycle::process;
use kardamom_types::service::{HaltCause, HaltRef};
use support::chain::SETTLEMENT;
use support::rig::{Rig, wait};

/// The production backoff of a source that failed.
const BACKOFF: Duration = Duration::from_secs(30);
/// The production deadline of one source read.
const READ_TIMEOUT: Duration = Duration::from_secs(30);
/// The least posts the batcher lands through one fault window.
const POSTS_THROUGH_FAULT: u64 = 3;
/// The wall-time bound of one wait.
const LIMIT: Duration = Duration::from_secs(10);

/// One test's run: the rig and the next batch index.
struct Case {
    rig: Rig,
    next_batch: u64,
}

impl Case {
    /// A rig whose follower and da-watcher run and caught up with one
    /// posted batch.
    async fn start() -> Self {
        process().clear();
        process().resume();
        let mut rig = Rig::new().await;
        rig.start_watcher();
        rig.start_follower().await;
        let mut case = Self { rig, next_batch: 1 };
        case.post().await;
        case.rig.l1.mine(2);
        case.await_caught_up().await;
        case
    }

    /// Post the next batch in a new block.
    async fn post(&mut self) {
        self.rig.post(self.next_batch).await;
        self.next_batch += 1;
    }

    /// The last batch posted.
    fn last_batch(&self) -> u64 {
        self.next_batch - 1
    }

    /// The stream and the da-watcher reached the finalized block, and no
    /// halt or pause stands.
    async fn await_caught_up(&mut self) {
        let finalized = self.rig.l1.finalized();
        let rig = &mut self.rig;
        wait("the follower and the da-watcher catch up", LIMIT, || {
            rig.stream_head() == Some(finalized)
                && rig.published_through() == Some(finalized)
                && Rig::halt().is_none()
                && Rig::watcher_root().is_none()
                && rig.batcher_root().is_none()
        })
        .await;
    }

    /// The follower halted on `cause`, and the da-watcher and the batcher
    /// pause with that halt as their root.
    async fn await_halted(&self, cause: HaltCause) {
        let rig = &self.rig;
        let on_follower = |root: Option<HaltRef>| {
            root.is_some_and(|r| r.service == HaltRef::L1_FOLLOWER && r.cause == cause)
        };
        wait("the halt and both pauses", LIMIT, || {
            Rig::halt() == Some(cause)
                && on_follower(Rig::watcher_root())
                && on_follower(rig.batcher_root())
        })
        .await;
    }

    /// Land the posts of a fault window, and prove the follower publishes
    /// nothing past the blocks that L1 held before the fault.
    async fn post_through_fault(&mut self, cause: HaltCause) {
        let head = Some(self.rig.l1.head());
        for _ in 0..POSTS_THROUGH_FAULT {
            self.post().await;
        }
        self.rig.l1.mine(3);
        tokio::time::sleep(super_poll()).await;
        self.await_halted(cause).await;
        assert!(self.rig.stream_head() <= head, "nothing passes the fault");
    }

    /// The end state of every case: a true contiguous stream, each epoch
    /// published once, and the archive complete.
    fn assert_complete(&mut self) {
        let finalized = self.rig.l1.finalized();
        self.rig.assert_stream_true_through(finalized);
        self.rig.assert_epochs_once_through(finalized);
        self.rig.assert_archive_through(self.last_batch());
    }
}

/// Several follower slots: a tick that the fault reaches has run.
fn super_poll() -> Duration {
    support::rig::POLL * 5
}

/// A provider refusal and the proxies it applies to.
#[derive(Clone, Copy, Debug)]
enum Refusal {
    Down,
    RateLimited,
    Unresponsive,
}

impl Refusal {
    const ALL: [Self; 3] = [Self::Down, Self::RateLimited, Self::Unresponsive];

    fn fault(self) -> Fault {
        match self {
            Self::Down => Fault::Down,
            Self::RateLimited => Fault::RateLimit,
            Self::Unresponsive => Fault::Unresponsive { all_sources: true },
        }
    }

    /// The proxies that refuse: the first only, or both when no source
    /// answers.
    fn proxies(self, rig: &Rig) -> Vec<&FaultProxy> {
        match self {
            Self::Unresponsive => vec![&rig.a, &rig.b],
            Self::Down | Self::RateLimited => vec![&rig.a],
        }
    }

    /// Arm the refusal, hold it through a fault window, end it, and cross
    /// the production backoff on the paused clock.
    async fn run(self, case: &mut Case) {
        self.proxies(&case.rig)
            .iter()
            .for_each(|proxy| proxy.set_fault(self.fault()));
        if let Self::Unresponsive = self {
            self.expire_reads(case).await;
        }
        case.post_through_fault(HaltCause::L1Unreachable).await;
        tokio::time::pause();
        self.proxies(&case.rig)
            .iter()
            .for_each(|proxy| proxy.set_fault(Fault::None));
        self.cross_backoff(case).await;
        tokio::time::resume();
        case.await_caught_up().await;
    }

    /// The held reads reach both proxies; the unchanged read deadline
    /// returns control to the follower, and not one second earlier.
    async fn expire_reads(self, case: &Case) {
        let served = (case.rig.a.served(), case.rig.b.served());
        let rig = &case.rig;
        tokio::time::sleep(super_poll()).await;
        wait("both reads reach their proxy", LIMIT, || {
            rig.a.served() > served.0 && rig.b.served() > served.1
        })
        .await;
        tokio::time::pause();
        tokio::time::advance(READ_TIMEOUT.saturating_sub(Duration::from_secs(1))).await;
        assert_eq!(Rig::halt(), None, "the read deadline must not be shortened");
        tokio::time::advance(Duration::from_secs(2)).await;
        tokio::time::resume();
    }

    /// Just before the backoff ends, the refused proxy has served no
    /// request and the follower is still halted; then the backoff ends.
    async fn cross_backoff(self, case: &Case) {
        let served = |rig: &Rig| -> Vec<u64> {
            self.proxies(rig)
                .iter()
                .map(|proxy| proxy.served())
                .collect()
        };
        case.jump_halted(Duration::from_secs(1)).await;
        let before = served(&case.rig);
        for _ in 3..BACKOFF.as_secs() {
            case.jump_halted(Duration::from_secs(1)).await;
        }
        assert_eq!(
            served(&case.rig),
            before,
            "{self:?}: the backoff sends no request"
        );
        assert_eq!(Rig::halt(), Some(HaltCause::L1Unreachable));
        tokio::time::advance(Duration::from_secs(3)).await;
    }
}

#[tokio::test]
async fn refused_providers_halt_the_follower_until_the_backoff_returns_them() {
    let mut case = Case::start().await;
    for refusal in Refusal::ALL {
        refusal.run(&mut case).await;
    }
    case.assert_complete();
}

/// One lie of the first provider.
#[derive(Clone, Copy, Debug)]
enum Lie {
    WrongHash,
    BrokenChain,
    SwallowedLogs,
    NullReceiptsAndSwallowedLogs,
}

impl Lie {
    /// The faults, armed from the next block so that nothing the follower
    /// already accepted changes under it.
    fn faults(self, rig: &Rig) -> Vec<Fault> {
        let from_block = rig.l1.head() + 1;
        match self {
            Self::WrongHash => vec![Fault::WrongBlockHash { from_block }],
            Self::BrokenChain => vec![Fault::BrokenParentChain { from_block }],
            Self::SwallowedLogs => vec![Fault::SwallowLogs {
                address: SETTLEMENT,
            }],
            Self::NullReceiptsAndSwallowedLogs => vec![
                Fault::NullReceipts { from_block },
                Fault::SwallowLogs {
                    address: SETTLEMENT,
                },
            ],
        }
    }

    async fn run(self, case: &mut Case) {
        let faults = serde_json::from_value(serde_json::json!(self.faults(&case.rig))).unwrap();
        case.rig.a.set_faults(faults);
        case.post_through_fault(HaltCause::L1SourceDisagreement)
            .await;
        case.rig.a.set_fault(Fault::None);
        case.await_caught_up().await;
    }
}

#[tokio::test]
async fn each_lie_halts_the_follower_and_its_end_resumes_a_true_stream() {
    let mut case = Case::start().await;
    for lie in [Lie::WrongHash, Lie::BrokenChain, Lie::SwallowedLogs] {
        lie.run(&mut case).await;
    }
    case.assert_complete();
}

#[tokio::test]
async fn null_receipts_with_swallowed_logs_halt_the_follower_until_they_end() {
    let mut case = Case::start().await;
    Lie::NullReceiptsAndSwallowedLogs.run(&mut case).await;
    case.assert_complete();
}

/// The L1 blocks of one outage hour. Two days of 12-second blocks are
/// 14,400 blocks; their catch-up alone takes over a minute in a debug
/// build, so the chain grows by a sixth of that: a backlog of 2,400
/// blocks, 38 ranges of the follower.
const BLOCKS_PER_HOUR: u64 = 50;
/// The hours of the two-day outage.
const OUTAGE_HOURS: u64 = 48;
/// The hour of the redeploy of the follower and the da-watcher.
const REDEPLOY_HOUR: u64 = 24;
const HOUR: Duration = Duration::from_secs(3600);
/// The production slot of the follower. Through the outage the follower
/// runs on it, so a jump of the paused clock never lands inside a read.
const SLOT: Duration = Duration::from_secs(12);
/// The da-watcher's housekeeping tick in the chaos cluster.
const WATCHER_TICK: Duration = Duration::from_secs(1);
/// The timer wheel rounds a deadline up to the next millisecond; a jump
/// of one slot plus this always reaches the woken tick.
const TIMER_ROUNDING: Duration = Duration::from_millis(2);

/// The two-day outage: the lies of its first hour, the batches the
/// batcher posts through it, one every two hours, and the block the
/// stream stopped at.
struct Outage {
    certs: Vec<(u64, alloy_primitives::Bytes)>,
    stuck: Option<u64>,
}

impl Outage {
    /// T0: a wrong hash and swallowed settlement logs from the first
    /// provider; the follower halts on them.
    async fn begin(case: &mut Case) -> Self {
        let from_block = case.rig.l1.head() + 1;
        let lies = [
            Fault::WrongBlockHash { from_block },
            Fault::SwallowLogs {
                address: SETTLEMENT,
            },
        ];
        case.rig
            .a
            .set_faults(serde_json::from_value(serde_json::json!(lies)).unwrap());
        case.post_through_fault(HaltCause::L1SourceDisagreement)
            .await;
        case.rig.stop_follower();
        case.rig.stop_watcher().await;
        case.rig.poll = SLOT;
        case.rig.watcher_tick = WATCHER_TICK;
        case.rig.start_watcher();
        case.rig.start_follower().await;
        case.await_halted(HaltCause::L1SourceDisagreement).await;
        let first = case.next_batch;
        case.next_batch += OUTAGE_HOURS / 2;
        let mut certs = Vec::new();
        for index in first..case.next_batch {
            certs.push((index, case.rig.disperse(index).await));
        }
        Self {
            certs,
            stuck: case.rig.stream_head(),
        }
    }

    /// One hour of the outage on the paused clock: its blocks, a post
    /// every two hours, and the follower still halted at the same block,
    /// with the da-watcher and the batcher paused on it.
    async fn hour(&self, case: &mut Case, hour: u64) {
        let post = usize::try_from(hour / 2)
            .ok()
            .filter(|_| hour.is_multiple_of(2));
        if let Some((index, cert)) = post.and_then(|i| self.certs.get(i.checked_sub(1)?)) {
            case.rig.l1.post(*index, cert.clone(), index * 1_000);
        }
        case.rig.l1.mine(BLOCKS_PER_HOUR);
        case.jump_halted(HOUR).await;
        let on_follower =
            |root: Option<HaltRef>| root.is_some_and(|r| r.service == HaltRef::L1_FOLLOWER);
        assert_eq!(
            Rig::halt(),
            Some(HaltCause::L1SourceDisagreement),
            "hour {hour}: {:?}",
            process().slots().halt
        );
        assert!(
            on_follower(Rig::watcher_root()),
            "hour {hour}: the da-watcher waits on the follower"
        );
        assert!(
            on_follower(case.rig.batcher_root()),
            "hour {hour}: the batcher waits on the follower"
        );
        assert_eq!(
            case.rig.stream_head(),
            self.stuck,
            "hour {hour}: nothing passes the lie"
        );
    }

    /// T2: a deploy of the same build stops and starts the follower and
    /// the da-watcher. The da-watcher's cursor file holds the block it
    /// stopped at, and both halt and pause again on the lie.
    async fn redeploy(&self, case: &mut Case) {
        tokio::time::resume();
        case.rig.stop_follower();
        case.rig.stop_watcher().await;
        let cursor: kardamom_da_watcher::L1Cursor =
            std::fs::read_to_string(case.rig.dir.path().join("l1-cursor"))
                .unwrap()
                .trim()
                .parse()
                .unwrap();
        let stuck = self.stuck.unwrap();
        assert_eq!(
            (cursor.number, cursor.hash),
            (stuck, case.rig.l1.record(stuck).hash),
            "the cursor holds the last published block"
        );
        case.rig.start_watcher();
        case.rig.start_follower().await;
        case.await_halted(HaltCause::L1SourceDisagreement).await;
        tokio::time::pause();
    }
}

impl Case {
    /// Jump the paused clock by `by`. The jump fires the follower's slot
    /// timer once, as a process that slept through the jump wakes once.
    /// The clock then stays put until `done` holds: the wait yields
    /// without a timer, so no read of the woken tick sees time pass.
    async fn jump(&self, by: Duration, mut done: impl FnMut(&Rig) -> bool) {
        tokio::time::advance(by).await;
        let started = std::time::Instant::now();
        while !done(&self.rig) {
            assert!(started.elapsed() < LIMIT, "the woken tick must end");
            tokio::task::yield_now().await;
        }
    }

    /// Jump by `by` through a lie: the woken tick ends when it raises its
    /// halt again.
    async fn jump_halted(&self, by: Duration) {
        let mut lifecycle = process().subscribe();
        lifecycle.borrow_and_update();
        self.jump(by, |_| lifecycle.has_changed().unwrap()).await;
    }
}

impl Outage {
    /// T4: the lie ended. The follower reads one range of 64 blocks a
    /// slot until the stream reaches the finalized block.
    async fn catch_up(&self, case: &mut Case) {
        let finalized = Some(case.rig.l1.finalized());
        let started = std::time::Instant::now();
        while case.rig.stream_head() < finalized {
            assert!(started.elapsed() < LIMIT * 3, "the catch-up must end");
            let len = case.rig.stream.len();
            case.jump(SLOT + TIMER_ROUNDING, |rig| rig.stream.len() > len)
                .await;
        }
    }
}

#[tokio::test]
async fn a_two_day_lie_holds_the_follower_and_its_end_resumes_without_a_gap() {
    let mut case = Case::start().await;
    let outage = Outage::begin(&mut case).await;
    tokio::time::pause();
    for hour in 1..=OUTAGE_HOURS {
        outage.hour(&mut case, hour).await;
        if hour == REDEPLOY_HOUR {
            outage.redeploy(&mut case).await;
        }
    }
    case.rig.a.set_fault(Fault::None);
    outage.catch_up(&mut case).await;
    tokio::time::resume();
    case.await_caught_up().await;
    case.assert_complete();
}
