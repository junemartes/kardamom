use super::{Mirror, MirrorRebuilds};

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
                            completed: *completed,
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
    assert_eq!(before.pending(&now), ["n1", "n2"]);
}

#[test]
fn replacement_needs_its_own_rebuild_and_uses_its_own_baseline() {
    let before = MirrorRebuilds::fixture(&[("n0", "old", 8)]);
    let empty = MirrorRebuilds::fixture(&[("n0", "new", 0)]);
    let rebuilt = MirrorRebuilds::fixture(&[("n0", "new", 1)]);
    assert_eq!(before.pending(&empty), ["n0"]);
    assert!(before.pending(&rebuilt).is_empty());
}

#[test]
fn missing_or_regressed_evidence_cannot_complete_the_wait() {
    let before = MirrorRebuilds::fixture(&[("n0", "a", 8), ("n1", "b", 2)]);
    let now = MirrorRebuilds::fixture(&[("n0", "a", 1)]);
    assert_eq!(before.pending(&now), ["n0", "n1"]);
}
