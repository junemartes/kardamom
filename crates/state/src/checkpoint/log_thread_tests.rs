use std::num::NonZeroU64;
use std::path::Path;

use super::super::block_log::{BlockLog, BlockLogWriter, LogIdentity};
use super::super::manifest::stored_genesis_digest;
use super::super::tests::{ADDR, block_batch, commit_logged, commit_through, seeded_env};
use super::LogFeed;
use crate::env::StateEnv;
use crate::meta::SCHEMA_VERSION;
use crate::writer::StateWriter;

fn identity(env: &StateEnv) -> LogIdentity {
    LogIdentity {
        genesis: stored_genesis_digest(env).unwrap(),
        schema: SCHEMA_VERSION,
    }
}

fn logged_blocks(env: &StateEnv, dir: &Path) -> Vec<u64> {
    let log = BlockLog::open(dir, identity(env)).unwrap();
    log.after(0, u64::MAX).map(|r| r.block).collect()
}

#[test]
fn the_writer_logs_each_committed_block() {
    let state = tempfile::tempdir().unwrap();
    let log = tempfile::tempdir().unwrap();
    let env = seeded_env(state.path());
    commit_logged(&env, log.path(), 1..=5);
    assert_eq!(logged_blocks(&env, log.path()), [1, 2, 3, 4, 5]);
    let read = BlockLog::open(log.path(), identity(&env)).unwrap();
    let third = read.after(2, 3).next().unwrap();
    assert_eq!(third.batch().unwrap(), block_batch(ADDR, 3, 2));
}

/// The log continues across a restart of the writer.
#[test]
fn the_log_continues_after_a_restart() {
    let state = tempfile::tempdir().unwrap();
    let log = tempfile::tempdir().unwrap();
    let env = seeded_env(state.path());
    commit_logged(&env, log.path(), 1..=3);
    commit_logged(&env, log.path(), 4..=6);
    assert_eq!(logged_blocks(&env, log.path()), [1, 2, 3, 4, 5, 6]);
}

/// A log that is ahead of the state database loses the records above the
/// last committed block: the executor executes those blocks again.
#[test]
fn the_start_cuts_the_records_above_the_committed_block() {
    let state = tempfile::tempdir().unwrap();
    let log = tempfile::tempdir().unwrap();
    let env = seeded_env(state.path());
    commit_through(
        StateWriter::spawn(env.clone()).unwrap(),
        (1..=3).map(|b| block_batch(ADDR, b, 2)),
    );
    let mut ahead =
        BlockLogWriter::open(log.path(), identity(&env), NonZeroU64::new(1).unwrap()).unwrap();
    (1..=5).for_each(|b| ahead.append(&block_batch(ADDR, b, 2)).unwrap());
    ahead.sync().unwrap();

    commit_logged(&env, log.path(), 4..=4);
    assert_eq!(logged_blocks(&env, log.path()), [1, 2, 3, 4]);
}

/// A log that ends below the state database continues after a gap. The
/// blocks before the gap stay in the log.
#[test]
fn a_log_behind_the_database_continues_after_a_gap() {
    let state = tempfile::tempdir().unwrap();
    let log = tempfile::tempdir().unwrap();
    let env = seeded_env(state.path());
    commit_logged(&env, log.path(), 1..=2);
    commit_through(
        StateWriter::spawn(env.clone()).unwrap(),
        (3..=4).map(|b| block_batch(ADDR, b, 2)),
    );
    commit_logged(&env, log.path(), 5..=6);
    assert_eq!(logged_blocks(&env, log.path()), [1, 2]);
    let read = BlockLog::open(log.path(), identity(&env)).unwrap();
    let after_gap: Vec<u64> = read.after(4, u64::MAX).map(|r| r.block).collect();
    assert_eq!(after_gap, [5, 6]);
}

/// A full queue drops the batch. The writer does not wait for the log.
#[test]
fn a_full_queue_drops_the_batch() {
    let (tx, rx) = crossbeam_channel::bounded(1);
    let feed = LogFeed { tx };
    feed.offer(block_batch(ADDR, 1, 0));
    feed.offer(block_batch(ADDR, 2, 0));
    let queued: Vec<u64> = rx.try_iter().map(|b| b.boundary.block_number).collect();
    assert_eq!(queued, [1]);
}
