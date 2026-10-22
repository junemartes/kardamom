use std::num::NonZeroU64;
use std::path::Path;

use super::super::tests::{ADDR, block_batch, commit_logged, seeded_env};
use super::super::{BlockRange, create_checkpoint, restore_best_checkpoint};
use super::LOG_DIR;
use crate::env::{StateEnv, StateEnvBuilder};
use crate::integrity::deep_compare;
use crate::recovery::read_recovery_point;

/// A source database with blocks `1..=head`, a base checkpoint at block
/// `base`, and the block log of every block after genesis.
struct Source {
    _state: tempfile::TempDir,
    ckpt: tempfile::TempDir,
    env: StateEnv,
}

impl Source {
    fn new(base: u64, head: u64) -> Self {
        let state = tempfile::tempdir().unwrap();
        let ckpt = tempfile::tempdir().unwrap();
        let env = seeded_env(state.path());
        let log = ckpt.path().join(LOG_DIR);
        commit_logged(&env, &log, 1..=base);
        create_checkpoint(&env, ckpt.path()).unwrap();
        commit_logged(&env, &log, base + 1..=head);
        Self {
            _state: state,
            ckpt,
            env,
        }
    }

    /// Restore into a new directory with `range`. Returns the directory
    /// and the restored block.
    fn restore(&self, range: BlockRange) -> (tempfile::TempDir, Option<u64>) {
        let dst = tempfile::tempdir().unwrap();
        let restored = restore_best_checkpoint(self.ckpt.path(), dst.path(), range, None)
            .unwrap()
            .map(|(block, _)| block);
        (dst, restored)
    }
}

fn committed_block(dir: &Path) -> u64 {
    let env = StateEnvBuilder::new(dir).open().unwrap();
    read_recovery_point(&env).unwrap().last_committed_block
}

/// The restore of the base and its log gives the database that the live
/// writer made, table by table.
#[test]
fn a_restore_applies_the_log_after_the_base() {
    let source = Source::new(3, 8);
    let (dst, restored) = source.restore(BlockRange::ANY);
    assert_eq!(restored, Some(8));
    let restored_env = StateEnvBuilder::new(dst.path()).open().unwrap();
    let diffs = deep_compare(&source.env, &restored_env).unwrap();
    assert!(diffs.is_empty(), "{diffs:?}");
}

/// A rollback restores the block before the divergence, not the newest
/// block of the log.
#[test]
fn a_restore_stops_at_the_upper_end_of_the_range() {
    let source = Source::new(3, 8);
    let (dst, restored) = source.restore(BlockRange::below(6).unwrap());
    assert_eq!(restored, Some(5));
    assert_eq!(committed_block(dst.path()), 5);
}

/// A resync needs state at or above a floor. A base below the floor
/// reaches it with the log.
#[test]
fn a_base_below_the_floor_reaches_it_with_the_log() {
    let source = Source::new(3, 8);
    let floor = BlockRange::at_or_above(NonZeroU64::new(7).unwrap());
    let (_dst, restored) = source.restore(floor);
    assert_eq!(restored, Some(8));
    let above = BlockRange::at_or_above(NonZeroU64::new(9).unwrap());
    assert_eq!(source.restore(above).1, None);
}

/// A gap in the log ends the restore at the block before the gap.
#[test]
fn a_gap_ends_the_restore() {
    let state = tempfile::tempdir().unwrap();
    let ckpt = tempfile::tempdir().unwrap();
    let env = seeded_env(state.path());
    let log = ckpt.path().join(LOG_DIR);
    commit_logged(&env, &log, 1..=3);
    create_checkpoint(&env, ckpt.path()).unwrap();
    commit_logged(&env, &log, 4..=5);
    super::super::tests::commit_through(
        crate::writer::StateWriter::spawn(env.clone()).unwrap(),
        std::iter::once(block_batch(ADDR, 6, 2)),
    );
    commit_logged(&env, &log, 7..=8);
    let dst = tempfile::tempdir().unwrap();
    let restored = restore_best_checkpoint(ckpt.path(), dst.path(), BlockRange::ANY, None).unwrap();
    assert_eq!(restored.map(|(block, _)| block), Some(5));
}

/// A checkpoint with no log restores as before: at its own block.
#[test]
fn a_base_with_no_log_restores_at_its_block() {
    let source = Source::new(3, 8);
    std::fs::remove_dir_all(source.ckpt.path().join(LOG_DIR)).unwrap();
    assert_eq!(source.restore(BlockRange::ANY).1, Some(3));
}
