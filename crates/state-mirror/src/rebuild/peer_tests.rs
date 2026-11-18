//! The rebuild with and without checkpoint peers, against a real
//! checkpoint server.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use alloy_primitives::U256;
use kardamom_types::BPosition;

use super::regression_tests::Fixture;
use super::{PeerFetch, Rebuild, RebuildSnapshot, TriedCheckpoint};

/// A checkpoint at block 1 holds one transaction, so it ends below the
/// receipt at index 1. A checkpoint at block 2 reaches it.
const FIRST_LIVE: BPosition = BPosition::from_index(1);

/// The directories of one mirror and of the executor beside it, and a
/// peer that serves its own checkpoint directory.
struct Node {
    fixture: Fixture,
    root: tempfile::TempDir,
}

impl Node {
    fn new() -> Self {
        Self {
            fixture: Fixture::new(),
            root: tempfile::tempdir().unwrap(),
        }
    }

    /// The checkpoint directory of the co-located executor.
    fn executor_dir(&self) -> PathBuf {
        self.root.path().join("executor")
    }

    fn peer_dir(&self) -> PathBuf {
        self.root.path().join("peer")
    }

    fn mirror_dir(&self) -> PathBuf {
        self.root.path().join("mirror")
    }

    /// Serve the peer directory, as the checkpoint server of an executor.
    fn serve_peer(&self) -> SocketAddr {
        kardamom_state::serve_checkpoints("127.0.0.1:0".parse().unwrap(), self.peer_dir())
            .unwrap()
            .addr
    }

    /// The rebuild of the mirror. `peers` names the peer servers and the
    /// genesis TOML that their images must match.
    fn rebuild(&self, peers: Option<(SocketAddr, &Path)>) -> Rebuild {
        let peers = peers.map(|(addr, chain)| {
            PeerFetch::new(
                vec![addr.to_string()],
                chain,
                self.mirror_dir().join("peer-checkpoints"),
            )
            .unwrap()
        });
        Rebuild::new(self.executor_dir(), self.mirror_dir().join("rebuild")).with_peers(peers)
    }

    /// One rebuild attempt, as the wait loop makes it.
    async fn attempt(&self, rebuild: &Rebuild) -> Option<RebuildSnapshot> {
        rebuild
            .try_newest(FIRST_LIVE, &mut TriedCheckpoint::new())
            .await
            .unwrap()
    }

    /// The checkpoints that the mirror fetched.
    fn fetched(&self) -> Vec<String> {
        Self::entries(&self.mirror_dir().join("peer-checkpoints"))
            .into_iter()
            .filter(|name| name.starts_with("checkpoint-"))
            .collect()
    }

    /// The entry names of `dir`, sorted. Empty for a missing directory.
    fn entries(dir: &Path) -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(dir).map_or_default(|rd| {
            rd.map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
                .collect()
        });
        names.sort();
        names
    }

    /// A node whose executor holds a checkpoint at block 1, behind the
    /// first live batch, and whose peer holds one at block 2.
    fn with_newer_peer() -> Self {
        let node = Self::new();
        node.fixture.commit(1);
        node.fixture.checkpoint(&node.executor_dir());
        node.fixture.commit(2);
        node.fixture.checkpoint(&node.peer_dir());
        node
    }

    /// Give only the peer a checkpoint, at block 2, and return its path.
    fn peer_only(&self) -> PathBuf {
        self.fixture.commit(1);
        self.fixture.commit(2);
        self.fixture.checkpoint(&self.peer_dir())
    }

    /// The peer image is refused for the chain of `chain`: no copy
    /// becomes visible, and the rebuild waits.
    async fn assert_refused(&self, chain: &Path) {
        let addr = self.serve_peer();
        assert!(
            self.attempt(&self.rebuild(Some((addr, chain))))
                .await
                .is_none()
        );
        assert_eq!(
            self.fetched(),
            Vec::<String>::new(),
            "a refused image never becomes visible"
        );
    }
}

/// With peers, the rebuild fetches the newer peer checkpoint into the
/// mirror directory and restores it. The executor directory stays as it
/// was: the mirror never writes to it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_rebuild_with_peers_restores_the_newer_peer_checkpoint() {
    let node = Node::with_newer_peer();
    let executor_before = Node::entries(&node.executor_dir());
    let addr = node.serve_peer();
    let chain = node.fixture.chain();

    let restored = node
        .attempt(&node.rebuild(Some((addr, &chain))))
        .await
        .expect("the peer checkpoint reaches the first live batch");

    assert_eq!(restored.end, FIRST_LIVE);
    assert_eq!(
        restored.rows(),
        vec![(node.fixture.address, 2, U256::from(98))]
    );
    assert_eq!(
        node.fetched(),
        [format!("checkpoint-{:018}", 2)],
        "the fetched copy lands in the mirror directory"
    );
    assert_eq!(Node::entries(&node.executor_dir()), executor_before);
}

/// Without peers, the rebuild reads the executor directory only, as
/// before: the newer peer checkpoint is not fetched, and the local one is
/// behind the first live batch.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_rebuild_without_peers_reads_only_the_executor_directory() {
    let node = Node::with_newer_peer();
    node.serve_peer();

    assert!(node.attempt(&node.rebuild(None)).await.is_none());
    assert!(
        !node.mirror_dir().join("peer-checkpoints").exists(),
        "no fetch without peers"
    );
}

/// A newer checkpoint of the executor wins over an older peer one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_newer_executor_checkpoint_wins_over_an_older_peer_one() {
    let node = Node::new();
    node.fixture.commit(1);
    node.fixture.checkpoint(&node.peer_dir());
    node.fixture.commit(2);
    node.fixture.checkpoint(&node.executor_dir());
    let addr = node.serve_peer();
    let chain = node.fixture.chain();

    let restored = node
        .attempt(&node.rebuild(Some((addr, &chain))))
        .await
        .expect("the executor checkpoint reaches the first live batch");

    assert_eq!(restored.end, FIRST_LIVE);
}

/// A peer image whose bytes do not hash to its manifest is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_corrupt_peer_image_is_refused() {
    let node = Node::new();
    let image = node.peer_only().join("mdbx.dat");
    let mut bytes = std::fs::read(&image).unwrap();
    bytes[0] ^= 0xFF;
    std::fs::write(&image, bytes).unwrap();

    node.assert_refused(&node.fixture.chain()).await;
}

/// A peer image of another genesis is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_peer_image_of_another_chain_is_refused() {
    let node = Node::new();
    node.peer_only();

    node.assert_refused(&node.fixture.foreign_chain()).await;
}
