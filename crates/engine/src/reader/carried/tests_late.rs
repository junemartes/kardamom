//! Late and lost frames: a lossy source that loses a boundary or sends
//! it late, a boundary that seals below the delivery cursor, and the stall
//! window of a reader that asks again with no progress.

use std::time::{Duration, Instant};

use super::tests::{Rig, TIMING, boundary, done, tx_at};
use super::*;

/// A reader of a lossy feed from the cursor `(0, 1)`, live at that cursor.
fn live_lossy() -> Rig {
    let mut rig = Rig::with(ReplayCursor::new(0, 1), TIMING, true);
    let id = rig.request(Source::Feed).request_id;
    rig.feed.send(done(id, 0, 1));
    rig
}

#[test]
fn a_lossy_source_holds_a_live_record_until_its_boundary() {
    let mut rig = live_lossy();
    rig.feed.send(tx_at(0));
    assert_eq!(
        rig.deliver(3),
        [] as [String; 0],
        "the record waits for the boundary of its block"
    );
    rig.feed.send(boundary(1, 1));
    assert_eq!(rig.deliver(3), ["t0", "b1"]);
    assert_eq!(rig.feed.requests(), []);
}

#[test]
fn a_late_boundary_of_a_lossy_source_takes_its_place() {
    let mut rig = live_lossy();
    rig.feed.send(tx_at(0));
    rig.feed.send(boundary(1, 1));
    assert_eq!(rig.deliver(3), ["t0", "b1"]);
    // One copy lost the boundary of block 2. The other copy brings it
    // after the records of block 3.
    rig.feed.send(tx_at(1));
    rig.feed.send(tx_at(2));
    assert_eq!(rig.deliver(3), [] as [String; 0]);
    rig.feed.send(boundary(2, 2));
    assert_eq!(rig.deliver(3), ["t1", "b2"]);
    rig.feed.send(boundary(3, 3));
    assert_eq!(rig.deliver(3), ["t2", "b3"]);
    assert_eq!(rig.feed.requests(), [], "a late boundary is no hole");
}

#[test]
fn a_lost_boundary_of_a_lossy_source_asks_again_before_any_delivery() {
    let mut rig = live_lossy();
    rig.feed.send(tx_at(0));
    rig.feed.send(tx_at(1));
    rig.feed.send(boundary(2, 2));
    let again = rig.request(Source::Feed);
    assert_eq!((again.from_index, again.from_block), (0, 1));
    assert_eq!(rig.deliver(3), [] as [String; 0]);
    rig.feed.send(tx_at(0));
    rig.feed.send(boundary(1, 1));
    rig.feed.send(tx_at(1));
    rig.feed.send(boundary(2, 2));
    rig.feed.send(done(again.request_id, 2, 3));
    assert_eq!(rig.deliver(5), ["t0", "b1", "t1", "b2"]);
}

#[test]
fn a_boundary_below_the_delivery_cursor_restarts_the_run() {
    let mut rig = Rig::new(0, 1);
    let id = rig.request(Source::Feed).request_id;
    rig.feed.send(done(id, 0, 1));
    rig.feed.send(tx_at(0));
    rig.feed.send(tx_at(1));
    assert_eq!(rig.deliver(3), ["t0", "t1"], "an ordered source is live");
    rig.feed.send(boundary(1, 1));
    match rig.error() {
        ExecutorError::SourceRestarted {
            next_index,
            next_block,
        } => assert_eq!((next_index, next_block), (2, 1)),
        other => panic!("expected a restart from the state DB, got {other:?}"),
    }
}

#[test]
fn a_reader_that_asks_again_with_no_progress_restarts_the_run() {
    let timing = Timing {
        stall: Duration::from_millis(400),
        ..TIMING
    };
    let started = Instant::now();
    let mut rig = Rig::with(ReplayCursor::new(0, 1), timing, false);
    rig.request(Source::Feed);
    let error = std::iter::repeat_with(|| {
        assert!(started.elapsed() < Duration::from_secs(5), "no restart");
        rig.feed.send(tx_at(5));
        rig.sub.step()
    })
    .find_map(Result::err)
    .expect("an error");
    assert!(
        matches!(error, ExecutorError::SourceRestarted { .. }),
        "{error:?}"
    );
    assert!(started.elapsed() >= timing.stall);
    assert_eq!(
        rig.sub.source(),
        Source::Feed,
        "the source never went silent"
    );
}

#[test]
fn a_reader_at_rest_does_not_restart() {
    let timing = Timing {
        stall: Duration::from_millis(100),
        ..TIMING
    };
    let mut rig = Rig::with(ReplayCursor::new(0, 1), timing, false);
    let id = rig.request(Source::Feed).request_id;
    rig.feed.send(done(id, 0, 1));
    rig.run_for(TIMING.silence / 2);
    rig.feed.send(boundary(1, 0));
    assert_eq!(rig.deliver(3), ["b1"]);
}
