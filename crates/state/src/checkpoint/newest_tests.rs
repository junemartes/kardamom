use std::num::NonZeroU64;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use super::super::create_checkpoint;
use super::super::replay::LOG_DIR;
use super::super::tests::{ADDR, block_batch, commit_logged, commit_through, seeded_env};
use super::{LogEnd, NEWEST_BLOCK, NEWEST_TIMESTAMP, NewestCheckpoint};
use crate::env::StateEnv;
use crate::writer::StateWriter;

/// The segment size of `commit_logged`.
const SEGMENT: NonZeroU64 = NonZeroU64::new(1 << 20).unwrap();

/// The time of the last sync of the log in these tests.
fn synced_at() -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(1_800_000_007)
}

fn end(block: u64) -> LogEnd {
    LogEnd {
        block,
        written: synced_at(),
        segment_bytes: SEGMENT,
    }
}

/// The point of the base of `block` in `ckpt`.
fn base_point(ckpt: &Path, block: u64) -> NewestCheckpoint {
    let path = ckpt.join(super::super::checkpoint_name(block));
    NewestCheckpoint {
        block,
        written: std::fs::metadata(path).unwrap().modified().unwrap(),
    }
}

/// Commit `blocks` with no block log.
fn commit_unlogged(env: &StateEnv, blocks: std::ops::RangeInclusive<u64>) {
    commit_through(
        StateWriter::spawn(env.clone()).unwrap(),
        blocks.map(|b| block_batch(ADDR, b, 2)),
    );
}

#[test]
fn a_directory_with_no_base_has_no_newest_checkpoint() {
    let state = tempfile::tempdir().unwrap();
    let ckpt = tempfile::tempdir().unwrap();
    let env = seeded_env(state.path());
    commit_logged(&env, &ckpt.path().join(LOG_DIR), 1..=3);
    assert_eq!(NewestCheckpoint::base(ckpt.path()).unwrap(), None);
    assert_eq!(end(3).newest(ckpt.path()).unwrap(), None);
}

#[test]
fn the_newest_base_is_the_point_at_the_time_of_its_copy() {
    let state = tempfile::tempdir().unwrap();
    let ckpt = tempfile::tempdir().unwrap();
    let env = seeded_env(state.path());
    commit_unlogged(&env, 1..=3);
    create_checkpoint(&env, ckpt.path()).unwrap();
    assert_eq!(
        NewestCheckpoint::base(ckpt.path()).unwrap(),
        Some(base_point(ckpt.path(), 3))
    );
}

#[test]
fn a_log_that_continues_the_base_moves_the_point_to_its_end() {
    let state = tempfile::tempdir().unwrap();
    let ckpt = tempfile::tempdir().unwrap();
    let env = seeded_env(state.path());
    let log = ckpt.path().join(LOG_DIR);
    commit_logged(&env, &log, 1..=3);
    create_checkpoint(&env, ckpt.path()).unwrap();
    commit_logged(&env, &log, 4..=6);
    assert_eq!(
        end(6).newest(ckpt.path()).unwrap(),
        Some(NewestCheckpoint {
            block: 6,
            written: synced_at(),
        })
    );
    assert_eq!(
        end(3).newest(ckpt.path()).unwrap(),
        Some(base_point(ckpt.path(), 3)),
        "a log that ends at the base adds nothing"
    );
}

/// The log has blocks 1 and 2, then 5 and 6: blocks 3 and 4 were not
/// logged. A restore of the base at 2 stops at 2.
#[test]
fn a_gap_after_the_base_keeps_the_point_at_the_base() {
    let state = tempfile::tempdir().unwrap();
    let ckpt = tempfile::tempdir().unwrap();
    let env = seeded_env(state.path());
    let log = ckpt.path().join(LOG_DIR);
    commit_logged(&env, &log, 1..=2);
    create_checkpoint(&env, ckpt.path()).unwrap();
    commit_unlogged(&env, 3..=4);
    commit_logged(&env, &log, 5..=6);
    assert_eq!(
        end(6).newest(ckpt.path()).unwrap(),
        Some(base_point(ckpt.path(), 2))
    );
}

#[test]
fn the_gauges_show_the_block_and_the_unix_time_of_the_point() {
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    metrics::with_local_recorder(&recorder, || {
        NewestCheckpoint::show(Ok(None));
    });
    let text = recorder.handle().render();
    assert!(!text.contains(NEWEST_BLOCK), "no base, no sample:\n{text}");

    metrics::with_local_recorder(&recorder, || {
        NewestCheckpoint::show(Ok(Some(NewestCheckpoint {
            block: 42,
            written: synced_at(),
        })));
    });
    let text = recorder.handle().render();
    assert!(text.contains(&format!("{NEWEST_BLOCK} 42\n")), "{text}");
    assert!(
        text.contains(&format!("{NEWEST_TIMESTAMP} 1800000007\n")),
        "{text}"
    );
}
