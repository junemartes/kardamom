//! The verdict of the marks of both sources on a hash mismatch.

use super::{Fault, Marks, Verdict};

const MISMATCH: Fault = Fault::Mismatch { index: 7 };

/// The verdict of the mark `feed` of the feed and `service` of the
/// service.
fn verdict(feed: Option<Fault>, service: Option<Fault>) -> Verdict {
    Marks { feed, service }.verdict()
}

#[test]
fn a_mismatch_with_a_source_that_never_connected_stops() {
    assert_eq!(
        verdict(Some(MISMATCH), Some(Fault::Absent)),
        Verdict::Mismatch { index: 7 }
    );
    assert_eq!(
        verdict(Some(Fault::Absent), Some(MISMATCH)),
        Verdict::Mismatch { index: 7 }
    );
}

#[test]
fn mismatches_at_the_same_index_on_both_sources_stop() {
    assert_eq!(
        verdict(Some(MISMATCH), Some(MISMATCH)),
        Verdict::Mismatch { index: 7 }
    );
}

#[test]
fn a_mismatch_and_a_floor_above_the_cursor_stop() {
    let refused = Fault::Unavailable {
        oldest_index: 20,
        oldest_block: 9,
    };
    assert_eq!(
        verdict(Some(refused), Some(MISMATCH)),
        Verdict::Mismatch { index: 7 }
    );
}

#[test]
fn a_mismatch_on_one_source_switches_while_the_other_can_serve() {
    for service in [
        None,
        Some(Fault::Silent),
        Some(Fault::Lost),
        Some(Fault::Undecodable),
        Some(Fault::Mismatch { index: 8 }),
    ] {
        assert_eq!(
            verdict(Some(MISMATCH), service),
            Verdict::Switched,
            "service mark {service:?}"
        );
    }
}
