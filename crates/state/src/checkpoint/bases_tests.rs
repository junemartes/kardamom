use std::num::{NonZeroU64, NonZeroUsize};
use std::time::Duration;

use super::super::block_log::{BlockLogWriter, LogIdentity, SegmentSize};
use super::super::create_checkpoint;
use super::super::manifest::stored_genesis_digest;
use super::super::replay::LOG_DIR;
use super::super::tests::{ADDR, block_batch, commit_logged, seeded_env};
use super::{BaseDue, BaseRule, prune_bases};
use crate::meta::SCHEMA_VERSION;

const FULL: u64 = 1000;

fn rule(log_percent: u64, max_age: Duration) -> BaseRule {
    BaseRule {
        log_percent: NonZeroU64::new(log_percent).unwrap(),
        max_age,
        segment_bytes: NonZeroU64::new(FULL).unwrap(),
    }
}

fn sizes(parts: &[(u64, u64)]) -> Vec<SegmentSize> {
    parts
        .iter()
        .map(|&(first_block, bytes)| SegmentSize { first_block, bytes })
        .collect()
}

const DAY: Duration = Duration::from_hours(24);

#[test]
fn a_segment_that_ended_before_it_was_full_is_a_gap() {
    let r = rule(100, DAY);
    assert!(r.has_gap(&sizes(&[(1, 10), (5, 10)]), 0), "1..4, then 5..");
    assert!(
        !r.has_gap(&sizes(&[(1, FULL), (5, 10)]), 0),
        "1..4 was full"
    );
    assert!(
        !r.has_gap(&sizes(&[(1, 10), (5, 10)]), 6),
        "the gap is before the base"
    );
    assert!(
        r.has_gap(&sizes(&[(10, 10)]), 3),
        "no segment holds block 4"
    );
    assert!(!r.has_gap(&[], 3), "an empty log has no gap");
}

#[test]
fn the_rule_takes_the_first_base_then_waits() {
    let state = tempfile::tempdir().unwrap();
    let ckpt = tempfile::tempdir().unwrap();
    let env = seeded_env(state.path());
    let r = rule(100, DAY);
    assert_eq!(r.due(ckpt.path()).unwrap(), Some(BaseDue::First));
    commit_logged(&env, &ckpt.path().join(LOG_DIR), 1..=3);
    create_checkpoint(&env, ckpt.path()).unwrap();
    assert_eq!(r.due(ckpt.path()).unwrap(), None);
}

/// A log after the base that is larger than the percent of the base asks
/// for a new base. The rule reads only the sizes of the segments. An old
/// base asks for a new one too.
#[test]
fn a_large_log_or_an_old_base_asks_for_a_new_base() {
    let state = tempfile::tempdir().unwrap();
    let ckpt = tempfile::tempdir().unwrap();
    let env = seeded_env(state.path());
    let log = ckpt.path().join(LOG_DIR);
    commit_logged(&env, &log, 1..=3);
    let base = create_checkpoint(&env, ckpt.path()).unwrap();
    let image = std::fs::metadata(super::super::checkpoint_data_file(&base.path).unwrap())
        .unwrap()
        .len();
    let after = log.join("00000000000000000004.blocks");
    std::fs::write(&after, vec![0u8; usize::try_from(image / 2).unwrap()]).unwrap();
    assert_eq!(rule(100, DAY).due(ckpt.path()).unwrap(), None);
    assert_eq!(
        rule(40, DAY).due(ckpt.path()).unwrap(),
        Some(BaseDue::LogSize)
    );
    assert_eq!(
        rule(100, Duration::ZERO).due(ckpt.path()).unwrap(),
        Some(BaseDue::Age)
    );
}

/// The prune keeps the newest bases and the segments after the oldest
/// kept base.
#[test]
fn the_prune_keeps_the_log_of_the_kept_bases() {
    let state = tempfile::tempdir().unwrap();
    let ckpt = tempfile::tempdir().unwrap();
    let env = seeded_env(state.path());
    let identity = LogIdentity {
        genesis: stored_genesis_digest(&env).unwrap(),
        schema: SCHEMA_VERSION,
    };
    let log = ckpt.path().join(LOG_DIR);
    let mut writer = BlockLogWriter::open(&log, identity, NonZeroU64::new(1).unwrap()).unwrap();
    for base in [2u64, 4, 6] {
        super::super::tests::commit_through(
            crate::writer::StateWriter::spawn(env.clone()).unwrap(),
            (base - 1..=base).map(|b| block_batch(ADDR, b, 2)),
        );
        for b in base - 1..=base {
            writer.append(&block_batch(ADDR, b, 2)).unwrap();
        }
        create_checkpoint(&env, ckpt.path()).unwrap();
    }
    writer.sync().unwrap();

    let (bases, segments) = prune_bases(ckpt.path(), NonZeroUsize::new(2).unwrap()).unwrap();
    assert_eq!((bases, segments), (1, 4));
    let firsts: Vec<u64> = SegmentSize::list(&log)
        .unwrap()
        .iter()
        .map(|s| s.first_block)
        .collect();
    assert_eq!(firsts, [5, 6], "base 4 needs the blocks from 5");
}
