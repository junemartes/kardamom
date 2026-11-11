//! Replica stops, kills and lapses of one lane, with the case load and
//! the recovery probe of the chaos harness.

use std::time::Duration;

use super::lane::{Lane, Lookups};

/// The case sender: it has transactions in flight during each fault.
pub(super) const CASE: u64 = 21;
/// The fresh sender of the recovery probe: nothing in flight before it.
pub(super) const FRESH: u64 = 22;
/// More blocks than the production loss window of a forwarded run.
const LAPSE_BLOCKS: usize = 20;

impl Lane {
    /// The case sender has three transactions ordered.
    pub(super) fn establish(&mut self) {
        (0..3).for_each(|_| {
            self.submit(CASE);
        });
        self.settle();
    }

    /// The pipeline progresses while a replica is down: a new transaction
    /// of the case sender is ordered.
    pub(super) fn assert_progress(&mut self) {
        let before = self.ordered().len();
        self.submit(CASE);
        self.settle();
        assert!(self.ordered().len() > before);
    }

    /// The recovery probe with the twin frozen: the case sender and a fresh
    /// sender each get a transaction ordered through `survivor` alone,
    /// and its process publishes them. Then both replicas order again.
    pub(super) fn assert_orders_alone(&mut self, survivor: usize, twin: usize) {
        let published = self.replica(survivor).published().unwrap_or(0);
        self.replica(twin).set_frozen(true);
        let frozen_offers = self.replica(twin).offers();
        let start = self.ordered().len();
        self.submit(CASE);
        self.submit(FRESH);
        self.settle();
        assert!(
            self.ordered()[start..]
                .iter()
                .all(|o| o.replica == survivor),
            "a frozen twin ordered a probe transaction"
        );
        assert_eq!(self.replica(twin).offers(), frozen_offers);
        assert!(self.replica(survivor).published().unwrap() > published);
        self.replica(twin).set_frozen(false);
        self.submit(CASE);
        self.submit(FRESH);
        self.settle();
    }

    /// One replica of the production matrix: kill it, restart it, and
    /// prove that it orders alone.
    fn replica_kill(id: u8, victim: usize) {
        let twin = 1 - victim;
        let mut lane = Lane::start(id, 2);
        lane.establish();
        lane.kill(victim);
        lane.assert_progress();
        lane.restart(victim, Lookups::Answered);
        lane.until("the restarted replica exports", |lane| {
            lane.replica(victim).exported() > 0
        });
        lane.assert_orders_alone(victim, twin);
        lane.tail();
    }

    /// The common tail: every load transaction ordered once and in order,
    /// both replicas healthy, and a clean stop.
    pub(super) fn tail(self) {
        self.assert_ledger();
        self.replicas
            .iter()
            .flatten()
            .for_each(|r| assert!(r.exported() > 0, "a replica exports no metric"));
        self.finish();
    }
}

#[test]
fn a_graceful_replica_stop_keeps_the_lane_ordering_and_its_restart_rejoins() {
    let mut lane = Lane::start(0, 2);
    lane.establish();
    lane.submit(CASE);
    lane.submit(CASE);
    lane.stop(0);
    lane.settle();
    lane.assert_progress();
    lane.restart(0, Lookups::Answered);
    lane.assert_orders_alone(0, 1);
    lane.tail();
}

#[test]
fn a_hard_replica_kill_loses_no_offer_and_its_cold_restart_rejoins() {
    let mut lane = Lane::start(0, 2);
    lane.establish();
    lane.submit(CASE);
    lane.submit(CASE);
    lane.kill(0);
    // The twin holds the same image: what the killed replica held is
    // ordered with no new client input.
    lane.settle();
    lane.assert_progress();
    lane.restart(0, Lookups::Answered);
    lane.assert_orders_alone(0, 1);
    lane.tail();
}

#[test]
fn each_restarted_replica_orders_an_established_sender_while_its_twin_is_frozen() {
    for (lane, victim) in [(0, 0), (1, 1), (0, 1), (1, 0)] {
        Lane::replica_kill(lane, victim);
    }
}

#[test]
fn a_lapsed_replica_publishes_fresh_work_after_thaw_while_its_twin_ordered() {
    let mut lane = Lane::start(0, 2);
    lane.establish();
    lane.replica(0).set_frozen(true);
    let (offers, published) = (lane.replica(0).offers(), lane.replica(0).published());
    (0..LAPSE_BLOCKS).for_each(|i| {
        lane.submit(if i % 2 == 0 { CASE } else { FRESH });
    });
    lane.settle();
    assert_eq!(
        lane.replica(0).offers(),
        offers,
        "the replica is not frozen"
    );
    assert_eq!(lane.replica(0).published(), published);
    lane.replica(0).set_frozen(false);
    lane.until("the thawed replica drains its account events", |lane| {
        lane.replica(0).backlog() == 0
    });
    lane.quiet(Duration::from_millis(50));
    // Two observations of the same process: fresh work is published
    // between them.
    let observed = lane.replica(0).published().unwrap();
    lane.submit(CASE);
    lane.submit(FRESH);
    lane.settle();
    lane.until("the thawed replica publishes fresh work", |lane| {
        lane.replica(0).published().unwrap() > observed
    });
    lane.assert_orders_alone(0, 1);
    lane.tail();
}
