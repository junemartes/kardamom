use super::{Counts, Mirror, MirrorRebuilds, RebuildPhase};

impl MirrorRebuilds {
    fn fixture(entries: &[(&str, &str, usize)]) -> Self {
        Self {
            mirrors: entries
                .iter()
                .map(|(node, allocation, completed)| {
                    (
                        (*node).to_owned(),
                        Mirror {
                            allocation: (*allocation).to_owned(),
                            counts: Counts {
                                completed: *completed,
                                ..Counts::default()
                            },
                        },
                    )
                })
                .collect(),
        }
    }
}

#[test]
fn repeated_rebuilds_on_one_mirror_cannot_certify_its_peers() {
    let before = MirrorRebuilds::fixture(&[("n0", "a", 1), ("n1", "b", 1), ("n2", "c", 1)]);
    let now = MirrorRebuilds::fixture(&[("n0", "a", 4), ("n1", "b", 1), ("n2", "c", 1)]);
    assert_eq!(before.pending(&now, RebuildPhase::Complete), ["n1", "n2"]);
}

#[test]
fn replacement_needs_its_own_rebuild_and_uses_its_own_baseline() {
    let before = MirrorRebuilds::fixture(&[("n0", "old", 8)]);
    let empty = MirrorRebuilds::fixture(&[("n0", "new", 0)]);
    let rebuilt = MirrorRebuilds::fixture(&[("n0", "new", 1)]);
    assert_eq!(before.pending(&empty, RebuildPhase::Complete), ["n0"]);
    assert!(before.pending(&rebuilt, RebuildPhase::Complete).is_empty());
}

#[test]
fn missing_or_regressed_evidence_cannot_complete_the_wait() {
    let before = MirrorRebuilds::fixture(&[("n0", "a", 8), ("n1", "b", 2)]);
    let now = MirrorRebuilds::fixture(&[("n0", "a", 1)]);
    assert_eq!(before.pending(&now, RebuildPhase::Complete), ["n0", "n1"]);
}

impl Mirror {
    fn fixture(allocation: &str, scanning: usize, write_retries: usize, completed: usize) -> Self {
        Self {
            allocation: allocation.into(),
            counts: Counts {
                scanning,
                write_retries,
                completed,
            },
        }
    }
}

#[test]
fn an_interruption_needs_a_new_scan_a_failed_write_and_no_completion() {
    let before = Mirror::fixture("old", 20, 30, 10);
    assert!(RebuildPhase::BlockedScan.ready(&before, &Mirror::fixture("new", 1, 1, 0)));
    for after in [
        Mirror::fixture("new", 0, 1, 0),
        Mirror::fixture("new", 1, 0, 0),
        Mirror::fixture("new", 1, 1, 1),
    ] {
        assert!(!RebuildPhase::BlockedScan.ready(&before, &after));
    }
}

#[test]
fn the_same_allocation_must_add_scan_and_retry_evidence() {
    let before = Mirror::fixture("same", 3, 4, 2);
    assert!(RebuildPhase::BlockedScan.ready(&before, &Mirror::fixture("same", 4, 5, 2)));
    for after in [
        Mirror::fixture("same", 3, 5, 2),
        Mirror::fixture("same", 4, 4, 2),
        Mirror::fixture("same", 4, 5, 3),
    ] {
        assert!(!RebuildPhase::BlockedScan.ready(&before, &after));
    }
}

#[test]
fn scan_evidence_is_parsed_from_ansi_logs_without_confusing_a_done_line() {
    let counts = Counts::read(
        "\x1b[31mrebuild: scanning\x1b[0m\nWARN rebuild: write failed; retrying\nINFO rebuild: done",
    );
    assert_eq!(
        (counts.scanning, counts.write_retries, counts.completed),
        (1, 1, 1)
    );
}
