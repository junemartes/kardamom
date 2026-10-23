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
        let state = tempfile::tempdir().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let env = seeded_env(state.path());
        let log = dir.path().join(LOG_DIR);
        commit_logged(&env, &log, 1..=3);
        create_checkpoint(&env, dir.path()).unwrap();
        commit_logged(&env, &log, 4..=head);
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

/// A node fetches from each peer in turn, and its peer list may name the
/// node itself with a stale log. A later peer with a shorter log of the
/// same base must not cut the log that an earlier peer gave.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_peer_with_a_shorter_log_does_not_shorten_the_local_log() {
    let full = Peer::new();
    let short = Peer::with_head(5);
    let local = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    let peers = [full.addr.to_string(), short.addr.to_string()];
    let (dir, into) = (local.path().to_path_buf(), state.path().to_path_buf());
    let restored = tokio::task::spawn_blocking(move || {
        fetch_best_checkpoint(&peers, &dir, BlockRange::ANY, None)?;
        restore_best_checkpoint(&dir, &into, BlockRange::ANY, None)
            .unwrap()
            .map(|(block, _)| block)
    })
    .await
    .unwrap();
    assert_eq!(restored, Some(8));
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
