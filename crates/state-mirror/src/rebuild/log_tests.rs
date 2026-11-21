//! The rebuild against a checkpoint directory with a block log: a point
//! is restored once per reach, and a point that the log shows behind the
//! first live batch is not restored at all.

use std::num::NonZeroU64;
use std::path::PathBuf;

use alloy_primitives::U256;
use kardamom_state::checkpoint::block_log::{BlockLogWriter, LogIdentity};
use kardamom_state::checkpoint::replay::LOG_DIR;
use kardamom_types::BPosition;

use super::regression_tests::Fixture;
use super::{Rebuild, TriedCheckpoint};

/// The base at block 1 ends at position 1. The block log must reach the
/// block 5, whose end 5 includes the position 4.
const FIRST_LIVE: BPosition = BPosition::from_index(4);

/// A segment limit that no test reaches.
const SEGMENT_BYTES: NonZeroU64 = NonZeroU64::new(1 << 30).unwrap();

/// The executor directory with a base at block 1, the log writer of that
/// directory, and the rebuild of one mirror.
struct LoggedNode {
    fixture: Fixture,
    root: tempfile::TempDir,
    log: BlockLogWriter,
    rebuild: Rebuild,
    tried: TriedCheckpoint,
}

impl LoggedNode {
    fn new() -> Self {
        let fixture = Fixture::new();
        let root = tempfile::tempdir().unwrap();
        fixture.commit(1);
        let executor = root.path().join("executor");
        fixture.checkpoint(&executor);
        let genesis = kardamom_state::load_genesis(&fixture.chain()).unwrap();
        let identity = LogIdentity {
            genesis: kardamom_state::genesis_digest(&genesis.to_alloc()),
            schema: kardamom_state::meta::SCHEMA_VERSION,
        };
        let log = BlockLogWriter::open(&executor.join(LOG_DIR), identity, SEGMENT_BYTES).unwrap();
        let rebuild = Rebuild::new(executor, root.path().join("scratch"));
        Self {
            fixture,
            root,
            log,
            rebuild,
            tried: TriedCheckpoint::new(),
        }
    }

    /// Append the blocks `blocks` to the log of the executor.
    fn append(&mut self, blocks: impl IntoIterator<Item = u64>) {
        blocks
            .into_iter()
            .for_each(|block| self.log.append(&self.fixture.batch(block)).unwrap());
        self.log.sync().unwrap();
    }

    /// One attempt of the wait loop. Returns the end of the restored
    /// snapshot, and whether the attempt restored a copy.
    async fn attempt(&mut self) -> (Option<BPosition>, bool) {
        let _ = std::fs::remove_dir_all(self.scratch());
        let restored = self
            .rebuild
            .try_newest(FIRST_LIVE, &mut self.tried)
            .await
            .unwrap();
        (restored.map(|r| r.end), self.scratch().exists())
    }

    fn scratch(&self) -> PathBuf {
        self.root.path().join("scratch")
    }
}

/// The log reaches the block 2 only, so the point is behind the first
/// live batch. The log after a gap grows, and the reach stays. No attempt
/// copies the base.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_point_behind_the_first_live_batch_is_not_restored() {
    let mut node = LoggedNode::new();
    node.append([2]);
    assert_eq!(node.attempt().await, (None, false));
    node.append(4..=6);
    assert_eq!(
        node.attempt().await,
        (None, false),
        "the gap holds the reach"
    );
    node.append(7..=9);
    assert_eq!(
        node.attempt().await,
        (None, false),
        "the tail grows after the gap"
    );
}

/// A base with no log after it states no end, so one restore tells. The
/// next attempt at the same point does not restore again. A log that
/// reaches the first live batch then restores.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_point_is_restored_once_until_its_reach_passes_the_first_live_batch() {
    let mut node = LoggedNode::new();
    assert_eq!(node.attempt().await, (None, true));
    assert_eq!(
        node.attempt().await,
        (None, false),
        "the same point is tried"
    );
    node.append(2..=5);
    let (end, restored) = node.attempt().await;
    assert!(restored);
    assert_eq!(end, Some(FIRST_LIVE));
}

/// The restored copy holds the state of the last block of the log.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_restored_copy_holds_the_state_of_the_reach() {
    let mut node = LoggedNode::new();
    node.append(2..=6);
    let restored = node
        .rebuild
        .try_newest(FIRST_LIVE, &mut node.tried)
        .await
        .unwrap()
        .expect("the log reaches the first live batch");
    assert_eq!(restored.end, BPosition::from_index(5));
    assert_eq!(
        restored.rows(),
        vec![(node.fixture.address, 6, U256::from(94))]
    );
}
