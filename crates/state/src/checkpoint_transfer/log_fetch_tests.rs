use std::net::SocketAddr;
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};

use super::super::{fetch_best_checkpoint, serve_checkpoints};
use super::LogRequest;
use crate::checkpoint::replay::LOG_DIR;
use crate::checkpoint::tests::{commit_logged, seeded_env};
use crate::checkpoint::{BlockRange, create_checkpoint, restore_best_checkpoint};
use crate::env::{StateEnv, StateEnvBuilder};
use crate::integrity::deep_compare;

/// A peer with a base at block 3 and the block log to a head block.
struct Peer {
    _state: tempfile::TempDir,
    dir: tempfile::TempDir,
    env: StateEnv,
    addr: SocketAddr,
}

impl Peer {
    /// A peer with the block log to block 8.
    fn new() -> Self {
        Self::with_head(8)
    }

    fn with_head(head: u64) -> Self {
        Self::with_base_and_head(3, head)
    }

    /// A peer with a base at block `base` and the block log to `head`.
    fn with_base_and_head(base: u64, head: u64) -> Self {
        let state = tempfile::tempdir().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let env = seeded_env(state.path());
        let log = dir.path().join(LOG_DIR);
        commit_logged(&env, &log, 1..=base);
        create_checkpoint(&env, dir.path()).unwrap();
        commit_logged(&env, &log, base + 1..=head);
        let addr = serve_checkpoints("127.0.0.1:0".parse().unwrap(), dir.path().to_path_buf())
            .unwrap()
            .addr;
        Self {
            _state: state,
            dir,
            env,
            addr,
        }
    }
}

/// Fetch the best checkpoint of `peer` in `range` into `local`, then
/// restore it into `state`. Returns the restored block.
async fn fetch_and_restore(
    peer: SocketAddr,
    local: PathBuf,
    state: PathBuf,
    range: BlockRange,
) -> Option<u64> {
    tokio::task::spawn_blocking(move || {
        fetch_best_checkpoint(&[peer.to_string()], &local, range, None)?;
        restore_best_checkpoint(&local, &state, range, None)
            .unwrap()
            .map(|(block, _)| block)
    })
    .await
    .unwrap()
}

fn open(dir: &Path) -> StateEnv {
    StateEnvBuilder::new(dir).open().unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_fetched_base_and_log_restore_the_state_of_the_peer() {
    let peer = Peer::new();
    let local = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    let restored = fetch_and_restore(
        peer.addr,
        local.path().to_path_buf(),
        state.path().to_path_buf(),
        BlockRange::ANY,
    )
    .await;
    assert_eq!(restored, Some(8));
    let diffs = deep_compare(&peer.env, &open(state.path())).unwrap();
    assert!(diffs.is_empty(), "{diffs:?}");
}

/// A resync needs state at or above a floor. The base of the peer is
/// below it, and its log reaches it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_base_below_the_floor_is_fetched_when_its_log_reaches_the_floor() {
    let peer = Peer::new();
    let local = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    let floor = BlockRange::at_or_above(NonZeroU64::new(7).unwrap());
    let restored = fetch_and_restore(
        peer.addr,
        local.path().to_path_buf(),
        state.path().to_path_buf(),
        floor,
    )
    .await;
    assert_eq!(restored, Some(8));

    let above = BlockRange::at_or_above(NonZeroU64::new(9).unwrap());
    let other = tempfile::tempdir().unwrap();
    let addr = peer.addr;
    let dir = other.path().to_path_buf();
    let fetched = tokio::task::spawn_blocking(move || {
        fetch_best_checkpoint(&[addr.to_string()], &dir, above, None)
    })
    .await
    .unwrap();
    assert_eq!(fetched, None, "the log of the peer ends below block 9");
}

/// A peer with no log serves the base alone, as a peer of an older
/// release does.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_peer_with_no_log_serves_the_base_alone() {
    let peer = Peer::new();
    std::fs::remove_dir_all(peer.dir.path().join(LOG_DIR)).unwrap();
    let local = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    let restored = fetch_and_restore(
        peer.addr,
        local.path().to_path_buf(),
        state.path().to_path_buf(),
        BlockRange::ANY,
    )
    .await;
    assert_eq!(restored, Some(3));
}

/// A node may fetch from several peers in turn, for example on each
/// rebuild attempt of a state mirror, and its peer list may name the node
/// itself with a stale log. A later fetch from a peer with a shorter log
/// of the same base must not cut the log that an earlier fetch gave.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_peer_with_a_shorter_log_does_not_shorten_the_local_log() {
    let full = Peer::new();
    let short = Peer::with_head(5);
    let local = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    let (full, short) = (full.addr.to_string(), short.addr.to_string());
    let (dir, into) = (local.path().to_path_buf(), state.path().to_path_buf());
    let restored = tokio::task::spawn_blocking(move || {
        fetch_best_checkpoint(&[full], &dir, BlockRange::ANY, None)?;
        fetch_best_checkpoint(&[short], &dir, BlockRange::ANY, None)?;
        restore_best_checkpoint(&dir, &into, BlockRange::ANY, None)
            .unwrap()
            .map(|(block, _)| block)
    })
    .await
    .unwrap();
    assert_eq!(restored, Some(8));
}

/// The fetch orders the peers by restore point, the end of the log after
/// the image. An older image with a longer log wins over a newer image
/// with a shorter log, and the newer image is not downloaded.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_furthest_restore_point_wins_over_the_newest_image() {
    let newer = Peer::with_base_and_head(5, 5);
    let further = Peer::new();
    let local = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    let peers = [newer.addr.to_string(), further.addr.to_string()];
    let (dir, into) = (local.path().to_path_buf(), state.path().to_path_buf());
    let (fetched, restored) = tokio::task::spawn_blocking(move || {
        let fetched = fetch_best_checkpoint(&peers, &dir, BlockRange::ANY, None).map(|c| c.block);
        let restored = restore_best_checkpoint(&dir, &into, BlockRange::ANY, None)
            .unwrap()
            .map(|(block, _)| block);
        (fetched, restored)
    })
    .await
    .unwrap();
    assert_eq!(fetched, Some(3), "the base of the further restore point");
    assert_eq!(restored, Some(8));
    let bases = crate::checkpoint::checkpoints_newest_first(local.path()).unwrap();
    assert_eq!(bases.len(), 1, "one download");
}

#[test]
fn a_log_request_reads_back() {
    let request = LogRequest {
        from: NonZeroU64::new(4).unwrap(),
        to: 9,
    };
    let line = request.line();
    assert_eq!(
        LogRequest::parse(line.lines().next().unwrap()),
        Some(request)
    );
    assert_eq!(
        LogRequest::parse("GET /checkpoint/log?from_block=0&to_block=9 HTTP/1.0"),
        None
    );
    assert_eq!(LogRequest::parse("GET /checkpoint/latest HTTP/1.0"), None);
}
