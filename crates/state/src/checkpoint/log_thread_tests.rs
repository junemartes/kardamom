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

/// A restart must not hide an old log: the open and the cut of the writer
/// touch the last segment, so the gauges take its time before them.
#[test]
fn the_gauges_take_the_time_of_the_log_on_disk_before_the_writer_opens_it() {
    let state = tempfile::tempdir().unwrap();
    let ckpt = tempfile::tempdir().unwrap();
    let env = seeded_env(state.path());
    let dir = ckpt.path().join(super::super::replay::LOG_DIR);
    commit_logged(&env, &dir, 1..=2);
    super::super::create_checkpoint(&env, ckpt.path()).unwrap();
    commit_logged(&env, &dir, 3..=4);
    let old = std::time::UNIX_EPOCH + std::time::Duration::from_secs(1_700_000_000);
    std::fs::read_dir(&dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .try_for_each(|segment| {
            std::fs::File::options()
                .write(true)
                .open(segment)?
                .set_modified(old)
        })
        .unwrap();
    let cfg = super::LogConfig {
        dir: dir.clone(),
        segment_bytes: NonZeroU64::new(1 << 20).unwrap(),
        queue: std::num::NonZeroUsize::new(1).unwrap(),
        sync_every: std::time::Duration::from_secs(1),
    };
    let gauges = super::NewestGauges::new(&cfg).unwrap();
    let mut writer = BlockLogWriter::open(&dir, identity(&env), cfg.segment_bytes).unwrap();
    writer.cut_after(4).unwrap();
    let newest = gauges.newest(&writer).unwrap().unwrap();
    assert_eq!((newest.block, newest.written), (4, old));
}
