use super::{AtFreeze, Frozen, Lines};
use crate::l1::Posted;

const CTX: &str = "batcher-outage-past-retention";

fn batch(index: u64, start: u64, end: u64) -> Posted {
    Posted {
        index,
        l2_block_start: start,
        l2_block_end: end,
    }
}

fn at_freeze(covered: u64) -> AtFreeze {
    AtFreeze {
        covered,
        lines: Lines {
            starts: 3,
            restored: 1,
        },
    }
}

#[test]
fn the_recovered_batch_is_the_first_past_the_covered_block_not_the_last() {
    let at = at_freeze(1747);
    let posted = [
        batch(40, 1700, 1747),
        batch(41, 1748, 1760),
        batch(42, 1761, 1769),
    ];
    assert_eq!(at.recovered_batch(&posted), Some(batch(41, 1748, 1760)));
    assert_eq!(at.recovered_batch(&posted[..1]), None);
    assert_eq!(at.recovered_batch(&[]), None);
}

#[test]
fn a_recovered_batch_with_a_gap_or_an_overlap_fails() {
    let at = at_freeze(1747);
    let gap = at.judge(batch(41, 1749, 1760), at.lines, CTX).unwrap_err();
    assert!(
        gap.to_string()
            .contains("the recovered batch 41 starts at 1749, not at 1748"),
        "{gap}"
    );
    let overlap = at.judge(batch(41, 1747, 1760), at.lines, CTX).unwrap_err();
    assert!(overlap.to_string().contains("starts at 1747"), "{overlap}");
}

#[test]
fn only_a_restart_that_restores_the_spool_passes() {
    let at = at_freeze(1747);
    let recovered = batch(41, 1748, 1760);
    let restored = Lines {
        starts: 4,
        restored: 2,
    };
    at.judge(recovered, restored, CTX).unwrap();
    let survived = at.judge(recovered, at.lines, CTX).unwrap_err();
    assert!(
        survived
            .to_string()
            .contains("the batcher kept running after the thaw"),
        "{survived}"
    );
    let lost = Lines {
        starts: 4,
        restored: 1,
    };
    let error = at.judge(recovered, lost, CTX).unwrap_err();
    assert!(
        error.to_string().contains("its spool was not restored"),
        "{error}"
    );
}

#[test]
fn a_freeze_attempt_holds_only_a_stopped_batcher_with_a_spool() {
    let held = Frozen::parse("T 12\n", "batcher-1".into(), CTX)
        .unwrap()
        .unwrap();
    assert_eq!((held.inner.as_str(), held.blocks), ("batcher-1", 12));
    assert!(
        Frozen::parse("T 0", "batcher-1".into(), CTX)
            .unwrap()
            .is_none()
    );
    assert!(
        Frozen::parse("gone 0", "batcher-1".into(), CTX)
            .unwrap()
            .is_none()
    );
    assert!(Frozen::parse("S 4", "batcher-1".into(), CTX).is_err());
    assert!(Frozen::parse("garbage", "batcher-1".into(), CTX).is_err());
}
