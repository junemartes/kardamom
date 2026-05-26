use log::watermark::QuorumState;
use types::{BPosition, FsyncWatermark};

fn pos(t: i32, o: i32) -> BPosition {
    BPosition {
        term_id: t,
        term_offset: o,
    }
}
fn w(rid: u8, t: i32, o: i32) -> FsyncWatermark {
    FsyncWatermark {
        recorder_id: rid,
        position: pos(t, o),
    }
}

#[test]
fn no_recorders_no_watermark() {
    let s = QuorumState::new(3, 2);
    assert!(s.quorum().is_none());
}

#[test]
fn one_of_three_with_q2_no_watermark() {
    let mut s = QuorumState::new(3, 2);
    s.observe(w(0, 1, 100));
    assert!(s.quorum().is_none());
}

#[test]
fn two_of_three_with_q2_emits_smaller_position() {
    let mut s = QuorumState::new(3, 2);
    s.observe(w(0, 1, 100));
    s.observe(w(1, 1, 200));
    assert_eq!(s.quorum(), Some(pos(1, 100)));
}

#[test]
fn three_of_three_with_q2_emits_middle_position() {
    // Q=2 means "fsynced on at least 2 of 3". With three reports, that's the
    // 2nd-smallest position (sorted ascending; pick index Q-1 = 1 from start).
    let mut s = QuorumState::new(3, 2);
    s.observe(w(0, 1, 100));
    s.observe(w(1, 1, 200));
    s.observe(w(2, 1, 300));
    assert_eq!(s.quorum(), Some(pos(1, 200)));
}

#[test]
fn watermark_is_monotonic() {
    let mut s = QuorumState::new(3, 2);
    s.observe(w(0, 1, 100));
    s.observe(w(1, 1, 200));
    assert_eq!(s.quorum(), Some(pos(1, 100)));
    s.observe(w(0, 1, 150));
    assert_eq!(s.quorum(), Some(pos(1, 150)));
}

#[test]
fn losing_one_of_three_with_q2_still_holds_quorum() {
    let mut s = QuorumState::new(3, 2);
    s.observe(w(0, 1, 100));
    s.observe(w(1, 1, 200));
    s.observe(w(2, 1, 300));
    assert_eq!(s.quorum(), Some(pos(1, 200)));
    // Recorder 0 keeps reporting; 2 dies (no more updates).
    s.observe(w(0, 1, 250));
    s.observe(w(1, 1, 400));
    // Sorted positions are now [250, 400, 300] → sorted [250, 300, 400], Q-th smallest = 300.
    assert_eq!(s.quorum(), Some(pos(1, 300)));
}
