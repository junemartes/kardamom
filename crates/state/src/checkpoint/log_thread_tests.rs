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

/// A sync period that no test reaches, so only a flush or a sync at the end
/// puts a record in the file.
const NEVER: std::time::Duration = std::time::Duration::from_secs(3600);

/// The child process of the exit tests reads its directories and its end
/// from this variable.
const CHILD: &str = "KARDAMOM_LOG_EXIT_CHILD";

fn never_synced(dir: &Path) -> super::LogConfig {
    super::LogConfig {
        dir: dir.to_path_buf(),
        segment_bytes: NonZeroU64::new(1 << 20).unwrap(),
        queue: std::num::NonZeroUsize::new(64).unwrap(),
        sync_every: NEVER,
    }
}

/// Run [`exit_child`] in a new process with the end `end`, then return
/// the last committed block of its state and the blocks of its log.
fn after_child_exit(end: &str) -> (u64, Vec<u64>) {
    let state = tempfile::tempdir().unwrap();
    let log = tempfile::tempdir().unwrap();
    {
        let _seeded = seeded_env(state.path());
    }
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "checkpoint::log_thread::tests::exit_child",
            "--nocapture",
        ])
        .env(
            CHILD,
            format!(
                "{end}\n{}\n{}",
                state.path().display(),
                log.path().display()
            ),
        )
        .status()
        .unwrap();
    assert!(status.success(), "the child exits with 0: {status}");
    let env = crate::env::StateEnvBuilder::new(state.path())
        .open()
        .unwrap();
    let committed = crate::recovery::read_recovery_point(&env)
        .unwrap()
        .last_committed_block;
    (committed, logged_blocks(&env, log.path()))
}

/// A process that stops the state writer and then exits at once, as the
/// executor does at the end of `main`, keeps every committed block in the
/// log.
#[test]
fn an_exit_after_the_stop_of_the_writer_keeps_every_committed_block() {
    assert_eq!(after_child_exit("stop"), (50, (1..=50).collect()));
}

/// An exit that runs no drop, such as the exit of the Aeron error handler,
/// keeps every block that the writer committed before it went idle.
#[test]
fn an_exit_after_the_writer_is_idle_keeps_every_committed_block() {
    assert_eq!(after_child_exit("idle"), (50, (1..=50).collect()));
}

/// The child of the exit tests: commit blocks 1 to 50, then end as the
/// variable says, and exit with no drop. Without the variable it does
/// nothing.
#[test]
fn exit_child() {
    let Ok(spec) = std::env::var(CHILD) else {
        return;
    };
    let mut parts = spec.split('\n');
    let (end, state, log) = (
        parts.next().unwrap(),
        parts.next().unwrap(),
        parts.next().unwrap(),
    );
    let env = crate::env::StateEnvBuilder::new(state).open().unwrap();
    let log = super::BlockLogThread::spawn(&env, &never_synced(Path::new(log))).unwrap();
    let mut writer = StateWriter::spawn_with_log(env, log).unwrap();
    (1..=50).for_each(|b| writer.delta_tx.send(block_batch(ADDR, b, 2)).unwrap());
    std::iter::from_fn(|| writer.snapshot_rx.recv())
        .find(|s| s.block_number() == 50)
        .unwrap();
    match end {
        "stop" => writer.shutdown().unwrap(),
        _ => std::thread::sleep(std::time::Duration::from_millis(200)),
    }
    std::process::exit(0);
}
