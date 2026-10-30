//! The validator lapse and join faults against the production validator
//! process. A real media driver carries the `tx_bal`, `tx_receipts` and
//! `egress_order` streams; a real egress serves the canonical stream with
//! its bytes; the validator keeps real MDBX state with the trie on. A
//! reference executor computed the published results of every block with
//! the production engine, and a peer server serves its real checkpoint.
//!
//! The lapse freezes every thread of the validator with `SIGSTOP`, as the
//! deployed case freezes its container. A freeze inside the client
//! liveness timeout of the driver must leave the same process verifying
//! past its count with bounded coverage loss. A longer freeze must end the
//! process through the client timeout; the restarted process must resume
//! from its persisted state and verify live. The join kills the
//! validator, wipes its state, and requires the new process to adopt the
//! peer checkpoint, bootstrap its trie and verify live. Every path must
//! end with zero divergence and the state root of the reference chain.

#![cfg(all(feature = "docker-e2e", target_os = "linux"))]

#[path = "validator_recovery/chain.rs"]
mod chain;
#[path = "validator_recovery/egress.rs"]
mod egress;
#[path = "validator_recovery/feed.rs"]
mod feed;
#[path = "validator_recovery/node.rs"]
mod node;
#[path = "validator_recovery/shared.rs"]
mod shared;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use chain::Chain;
use feed::Feed;
use node::{Node, Sample, Settings};
use shared::Shared;

/// The blocks of the shared chain, and the head of the lapse tests. At the pace of the feed they span 15 s:
/// the long lapse and the restart fit before the chain ends.
pub(crate) const BLOCKS: u64 = 150;
/// The head of the join test: the join needs no long lapse.
pub(crate) const JOIN_BLOCKS: u64 = 70;
/// The block after which the reference executor copies its checkpoint.
pub(crate) const CHECKPOINT_BLOCK: u64 = 20;
/// The blocks the chain runs past the checkpoint before the join, so the
/// joining validator replays a tail.
const JOIN_TAIL: u64 = 10;
/// The client liveness timeout of the driver: the bound between the two
/// lapse paths.
pub(crate) const LIVENESS: Duration = Duration::from_secs(3);
/// A lapse inside the liveness timeout.
const SHORT_LAPSE: Duration = Duration::from_millis(1_500);
/// A lapse of three liveness timeouts.
const LONG_LAPSE: Duration = Duration::from_secs(9);
/// The lag bound of the warm-up, as the chaos case sets it.
const WARM_LAG: u64 = 15;
/// The block a validator commits before the fault: the fault hits a
/// validator that verifies a running chain, not one that just started.
const WARM_BLOCK: u64 = 20;
/// The lag bound of the recovery, as the chaos case sets it.
const RECOVERED_LAG: u64 = 25;
/// The most BALs a survivor may miss over the lapse.
const SURVIVOR_MISSING: u64 = 5;
/// The handler line of a client timeout.
const CLIENT_TIMEOUT_LINE: &str = "aeron client error; exiting";

/// One test: the shared chain and driver, its own feed and scratch
/// directory, and the head its chain ends on.
struct Rig {
    feed: Feed,
    chain: Arc<Chain>,
    aeron_dir: PathBuf,
    dir: tempfile::TempDir,
    started: Instant,
    blocks: u64,
}

impl Rig {
    /// A rig whose chain has `blocks` blocks.
    fn start(blocks: u64) -> Self {
        let started = Instant::now();
        let dir = tempfile::Builder::new()
            .prefix("kardamom-validator-recovery-")
            .tempdir()
            .unwrap();
        let shared = Shared::get();
        let chain = shared.chain.clone();
        let aeron_dir = shared.aeron_dir();
        let feed = Feed::start(dir.path(), chain.clone(), &aeron_dir);
        Self {
            feed,
            chain,
            aeron_dir,
            dir,
            started,
            blocks,
        }
    }

    fn path(&self, name: &str) -> PathBuf {
        self.dir.path().join(name)
    }

    fn settings(&self, checkpoints: Option<(PathBuf, String)>) -> Settings {
        Settings {
            state_dir: self.path("validator-state"),
            aeron_dir: self.aeron_dir.clone(),
            genesis: self.chain.genesis_file.clone(),
            egress: self.feed.egress_base.clone(),
            checkpoints,
            chain_id: chain::CHAIN_ID,
        }
    }

    /// Start the first validator, then let the chain run: the executor
    /// publishes to a subscribed validator from the first block.
    fn first_node(&self, settings: &Settings) -> Node {
        let node = Node::start(settings, &self.path("validator.log"));
        self.feed.run_to(self.blocks);
        node
    }

    /// Wait until `node` verifies live and committed [`WARM_BLOCK`].
    fn warm(&self, node: &Node) -> Sample {
        node.await_sample("warm up", Duration::from_secs(30), |s| {
            s.verifies_after(0, self.feed.head(), WARM_LAG) && s.committed >= WARM_BLOCK
        })
    }

    /// Wait until `node` verifies live, past `floor`, within `lag` blocks
    /// of the executor head.
    fn verifying(&self, node: &Node, what: &str, floor: u64, lag: u64) -> Sample {
        node.await_sample(what, Duration::from_secs(30), |s| {
            s.verifies_after(floor, self.feed.head(), lag)
        })
    }

    /// Let the chain end, require the node to commit its last block with
    /// zero divergence, stop it, and compare its state root with the
    /// root of the reference chain.
    fn assert_final_state(&self, node: &mut Node) {
        let last = node.await_sample("commit the last block", Duration::from_secs(40), |s| {
            s.committed == self.blocks
        });
        assert_eq!(last.divergence, 0, "{last:?}");
        assert!(last.root_block > 0, "no state root observation: {last:?}");
        assert!(node.terminate().success());
        let env = kardamom_state::StateEnvBuilder::new(self.path("validator-state"))
            .open()
            .unwrap();
        let snapshot = kardamom_state::StateSnapshot::open(&env).unwrap();
        assert_eq!(snapshot.block_number(), self.blocks);
        assert_eq!(
            snapshot.state_root().unwrap(),
            Some(self.chain.root(self.blocks)),
            "the validator state root differs from the reference chain"
        );
        println!(
            "VALIDATOR_RECOVERY seconds={:.3}",
            self.started.elapsed().as_secs_f64()
        );
    }
}

fn require_line(node: &Node, needle: &str) {
    assert!(
        node.log_text().contains(needle),
        "missing log line {needle:?} in {}",
        node.log.display()
    );
}

/// The survivor path: a freeze inside the liveness timeout. The same
/// process keeps verifying past its count, misses at most a few BALs,
/// and reaches the reference state with zero divergence.
#[test]
#[ignore = "requires Docker; run the validator-recovery component target"]
fn a_short_lapse_keeps_the_same_process_verifying_past_its_count() {
    let rig = Rig::start(BLOCKS);
    let node = rig.first_node(&rig.settings(None));
    let warm = rig.warm(&node);
    node.freeze();
    std::thread::sleep(SHORT_LAPSE);
    node.thaw();
    let mut node = node;
    assert!(
        node.exited(LIVENESS).is_none(),
        "a lapse inside the liveness timeout ended the process\n{}",
        node.log_text()
    );
    let recovered = rig.verifying(&node, "survivor recovery", warm.verified, RECOVERED_LAG);
    assert_eq!(recovered.divergence, 0, "{recovered:?}");
    assert!(
        recovered.bal_missing.saturating_sub(warm.bal_missing) <= SURVIVOR_MISSING,
        "coverage regressed over the lapse: bal_missing {} -> {}",
        warm.bal_missing,
        recovered.bal_missing
    );
    assert!(!node.log_text().contains(CLIENT_TIMEOUT_LINE));
    rig.assert_final_state(&mut node);
}

/// The newborn path: a freeze beyond the liveness timeout. The driver
/// times the client out, the process exits with status 1, and the
/// restarted process resumes from its persisted cursor and verifies live
/// from a fresh counter.
#[test]
#[ignore = "requires Docker; run the validator-recovery component target"]
fn a_long_lapse_ends_the_process_and_the_restart_resumes_from_its_state() {
    let rig = Rig::start(BLOCKS);
    let settings = rig.settings(None);
    let mut node = rig.first_node(&settings);
    let warm = rig.warm(&node);
    node.freeze();
    std::thread::sleep(LONG_LAPSE);
    node.thaw();
    let status = node
        .exited(Duration::from_secs(30))
        .expect("the client timeout ends the process");
    assert_eq!(status.code(), Some(1), "{}", node.log_text());
    require_line(&node, CLIENT_TIMEOUT_LINE);
    let mut newborn = Node::start(&settings, &rig.path("validator-restarted.log"));
    let recovered = rig.verifying(&newborn, "newborn recovery", 0, RECOVERED_LAG);
    assert_eq!(recovered.divergence, 0, "{recovered:?}");
    rig.assert_final_state(&mut newborn);
    require_line(&newborn, "validator resuming from persisted cursor");
    assert!(
        recovered.verified < BLOCKS.saturating_sub(warm.committed).saturating_add(1),
        "the newborn verified blocks below its persisted cursor again: {recovered:?}, warm {warm:?}"
    );
}

/// The join: the validator dies and loses its state and staged
/// checkpoints. The new process must adopt the peer checkpoint, rebuild
/// its trie, catch up the tail, and verify live with zero divergence.
#[test]
#[ignore = "requires Docker; run the validator-recovery component target"]
fn a_fresh_validator_adopts_the_peer_checkpoint_and_verifies_live() {
    let rig = Rig::start(JOIN_BLOCKS);
    let server = Peer::serve(&rig.chain.checkpoints);
    let staging = rig.path("validator-checkpoints");
    let settings = rig.settings(Some((staging.clone(), server.addr.to_string())));
    let mut node = rig.first_node(&rig.settings(None));
    rig.warm(&node);
    // The chain runs past the checkpoint, so the join replays a tail.
    node.await_sample("chain past the checkpoint", Duration::from_secs(30), |s| {
        s.committed >= rig.chain.checkpoint_block + JOIN_TAIL
    });
    node.kill();
    Peer::wipe(&settings.state_dir);
    Peer::wipe(&staging);
    let mut newborn = Node::start(&settings, &rig.path("validator-joined.log"));
    let joined = rig.verifying(&newborn, "join", 0, RECOVERED_LAG);
    assert_eq!(joined.divergence, 0, "{joined:?}");
    rig.assert_final_state(&mut newborn);
    require_line(&newborn, "adopted state from checkpoint");
    require_line(&newborn, "trie bootstrap complete");
    require_line(
        &newborn,
        &format!("restored_block={}", rig.chain.checkpoint_block),
    );
    assert!(
        joined.verified <= JOIN_BLOCKS - rig.chain.checkpoint_block,
        "the newborn re-verified blocks below the adopted checkpoint: {joined:?}"
    );
}

/// The checkpoint server of an executor peer.
struct Peer {
    addr: std::net::SocketAddr,
    _rt: tokio::runtime::Runtime,
}

impl Peer {
    fn serve(dir: &Path) -> Self {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let addr = rt
            .block_on(async {
                kardamom_state::serve_checkpoints("127.0.0.1:0".parse().unwrap(), dir.to_path_buf())
            })
            .unwrap()
            .addr;
        Self { addr, _rt: rt }
    }

    fn wipe(dir: &Path) {
        if dir.exists() {
            std::fs::remove_dir_all(dir).unwrap();
        }
    }
}
