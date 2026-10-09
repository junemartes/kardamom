//! Tests of the escalation clock: the thresholds derive from the stall
//! budget, the reopen comes once, the exit comes after the total, and a
//! connected publish resets the clock.

use std::time::{Duration, Instant};

use super::{Escalation, PUBLICATION_DEAD_EXIT_CODE, Step};

const BUDGET: Duration = Duration::from_secs(35);

fn at(start: Instant, secs: u64) -> Instant {
    start + Duration::from_secs(secs)
}

#[test]
fn the_thresholds_derive_from_the_stall_budget() {
    let e = Escalation::from_stall_budget(BUDGET);
    assert_eq!(e.reopen_after(), Duration::from_secs(35));
    assert_eq!(e.exit_after(), Duration::from_secs(175));
    let production = Escalation::from_stall_budget(Duration::from_secs(15));
    assert_eq!(production.reopen_after(), Duration::from_secs(15));
    assert_eq!(production.exit_after(), Duration::from_secs(75));
    assert_eq!(PUBLICATION_DEAD_EXIT_CODE, 3);
}

#[test]
fn the_reopen_comes_once_after_one_budget_and_the_exit_after_five() {
    let start = Instant::now();
    let mut e = Escalation::from_stall_budget(BUDGET);
    assert_eq!(e.unconnected(start), Step::Wait);
    assert_eq!(e.unconnected(at(start, 34)), Step::Wait);
    assert_eq!(e.unconnected(at(start, 35)), Step::Reopen);
    assert_eq!(e.unconnected(at(start, 36)), Step::Wait, "one reopen only");
    assert_eq!(e.unconnected(at(start, 174)), Step::Wait);
    assert_eq!(
        e.unconnected(at(start, 175)),
        Step::Exit {
            unconnected: Duration::from_secs(175)
        }
    );
    assert_eq!(e.unconnected_for(at(start, 175)), Duration::from_secs(175));
}

/// The recording start bounds each attempt by the budget and starts the
/// clock before the first attempt. At the production budget the attempts
/// end at 15, 30, 45, 60 and 75 s: the reopen comes at the first end, the
/// exit at the fifth, so a reopen always comes before the exit.
#[test]
fn budget_bounded_attempts_reopen_before_the_exit_at_the_production_budget() {
    let budget = Duration::from_secs(15);
    let start = Instant::now();
    let mut e = Escalation::from_stall_budget(budget);
    e.start(start);
    let steps: Vec<Step> = (1..=5)
        .map(|attempt| e.unconnected(start + budget * attempt))
        .collect();
    assert_eq!(
        steps,
        [
            Step::Reopen,
            Step::Wait,
            Step::Wait,
            Step::Wait,
            Step::Exit {
                unconnected: Duration::from_secs(75)
            }
        ]
    );
    // A second start inside the period moves nothing.
    let mut again = Escalation::from_stall_budget(budget);
    again.start(start);
    again.start(start + budget);
    assert_eq!(again.unconnected_for(start + budget), budget);
}

#[test]
fn a_connected_publish_resets_the_clock_and_the_reopen() {
    let start = Instant::now();
    let mut e = Escalation::from_stall_budget(BUDGET);
    assert_eq!(e.unconnected(start), Step::Wait);
    assert_eq!(e.unconnected(at(start, 40)), Step::Reopen);
    e.connected();
    assert_eq!(e.unconnected_for(at(start, 41)), Duration::ZERO);
    // A new period starts at the next unconnected attempt.
    assert_eq!(e.unconnected(at(start, 50)), Step::Wait);
    assert_eq!(e.unconnected(at(start, 84)), Step::Wait);
    assert_eq!(e.unconnected(at(start, 85)), Step::Reopen);
    assert_eq!(e.unconnected_for(at(start, 85)), Duration::from_secs(35));
}

#[test]
fn a_clock_that_runs_backwards_does_not_underflow() {
    let start = Instant::now();
    let mut e = Escalation::from_stall_budget(BUDGET);
    assert_eq!(e.unconnected(at(start, 10)), Step::Wait);
    assert_eq!(e.unconnected(start), Step::Wait);
    assert_eq!(e.unconnected_for(start), Duration::ZERO);
}

#[test]
fn a_huge_budget_saturates_instead_of_overflowing() {
    let e = Escalation::from_stall_budget(Duration::MAX);
    assert_eq!(e.exit_after(), Duration::MAX);
}

/// The gauges follow the reports: 0 and the period while unconnected,
/// 1 and 0 once connected, and no write on a repeated connected report.
#[test]
fn the_health_gauges_follow_the_reports() {
    use crate::metrics::{PUBLICATION_CONNECTED, PUBLICATION_NOT_CONNECTED_SECONDS};

    let recorder = super::super::test_hooks::GaugeRecorder::default();
    let labels = [("topic", "tx_receipts")];
    metrics::with_local_recorder(&recorder, || {
        let mut health = super::PublicationHealth::new("tx_receipts");
        // The first unconnected report: the clock has counted nothing
        // yet, and the gauge still reads unconnected.
        health.report(false, Duration::ZERO);
        assert_eq!(recorder.gauge(PUBLICATION_CONNECTED, &labels), Some(0.0));
        assert_eq!(
            recorder.gauge(PUBLICATION_NOT_CONNECTED_SECONDS, &labels),
            Some(0.0)
        );
        health.report(false, Duration::from_secs(7));
        assert_eq!(
            recorder.gauge(PUBLICATION_NOT_CONNECTED_SECONDS, &labels),
            Some(7.0)
        );
        health.report(true, Duration::ZERO);
        assert_eq!(recorder.gauge(PUBLICATION_CONNECTED, &labels), Some(1.0));
        assert_eq!(
            recorder.gauge(PUBLICATION_NOT_CONNECTED_SECONDS, &labels),
            Some(0.0)
        );
    });
    // A fresh health reports connected once, and only once.
    let counting = super::super::test_hooks::GaugeRecorder::default();
    metrics::with_local_recorder(&counting, || {
        let mut health = super::PublicationHealth::new("tx_receipts");
        health.report(true, Duration::ZERO);
        assert_eq!(counting.gauge(PUBLICATION_CONNECTED, &labels), Some(1.0));
    });
}
