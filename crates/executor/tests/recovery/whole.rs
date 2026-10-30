//! The cases that take every replica down at once, the damaged local
//! checkpoint, and the absent sealer.

use std::time::Duration;

use kardamom_engine::ReplayGenesis;

use super::fleet::{NodeHandle, World};
use super::verdict::CATCH_UP;
use super::{BLOCK_EVERY, FETCHED, GENESIS_REPLAY, RESTORED, RESUMED};

/// The last block of each case's load.
/// The load runs until the case halts it.
const LAST: u64 = 100_000;

/// executor-fleet-loss-recover: all three replicas die at once with no
/// warning. The sealer keeps ordering through the outage. Each replica
/// returns on its own disk, resumes from its own cursor with no
/// restore or fetch, and the fleet catches up to the exact state.
#[test]
fn a_whole_fleet_kill_resumes_each_replica_from_its_own_disk() {
    let world = World::new();
    world.seal_through(5);
    let load = world.load(5, LAST, BLOCK_EVERY);
    let mut fleet = world.fleet();
    fleet.start();
    fleet.wait_head(12, CATCH_UP);
    fleet.kill();
    fleet
        .0
        .iter()
        .for_each(|n| assert_eq!(n.get("/metrics"), None, "{} still exports", n.name()));
    let dark = fleet.0.iter().map(NodeHandle::head).max().unwrap();
    let outage = load.sealed() + 8;
    let wait = super::fleet::Wait::new(CATCH_UP, "the sealer load past the outage");
    std::iter::repeat_with(|| wait.step((load.sealed() >= outage).then_some(()), String::new))
        .find_map(std::convert::identity);
    fleet.0.iter().for_each(|n| {
        assert!(
            n.durable_block() >= n.head(),
            "{}: a boundary before its commit",
            n.name()
        );
    });
    fleet.start();
    fleet.wait_head(load.sealed() + 5, CATCH_UP);
    let head = load.halt();
    assert!(head > dark, "the sealer ordered past the outage");
    fleet.assert_converged(&world, head, CATCH_UP);
    fleet.0.iter().for_each(NodeHandle::assert_no_republished);
    fleet.0.iter().for_each(|n| {
        let log = n.last_log();
        assert!(log.contains(RESUMED), "{}: no resume:\n{log}", n.name());
        assert!(
            !log.contains(RESTORED) && !log.contains(FETCHED),
            "{}:\n{log}",
            n.name()
        );
    });
}

/// executor-fleet-wipe-recover: all three replicas die and lose their
/// state DBs; each keeps its checkpoints. No peer is up to serve one,
/// so each must restore its own local checkpoint and replay the tail:
/// three restore lines, no fetch, no genesis replay.
#[test]
fn a_whole_fleet_wipe_restores_each_replica_from_its_local_checkpoint() {
    let world = World::new();
    world.seal_through(5);
    let load = world.load(5, LAST, BLOCK_EVERY);
    let mut fleet = world.fleet();
    fleet.start();
    fleet.0.iter_mut().for_each(|n| {
        n.wait_checkpoint(8, CATCH_UP);
    });
    fleet.kill();
    fleet.0.iter().for_each(NodeHandle::wipe_state);
    fleet.start();
    fleet.wait_head(load.sealed() + 5, CATCH_UP);
    let head = load.halt();
    fleet.assert_converged(&world, head, CATCH_UP);
    fleet.0.iter().for_each(|n| {
        let log = n.last_log();
        assert_eq!(
            log.matches(RESTORED).count(),
            1,
            "{}: one local restore:\n{log}",
            n.name()
        );
        assert!(
            !log.contains(FETCHED),
            "{}: no peer was up:\n{log}",
            n.name()
        );
        assert!(!log.contains(GENESIS_REPLAY), "{}:\n{log}", n.name());
    });
}

/// executor-fleet-total-wipe-recover: the job stops, and every state DB
/// and every checkpoint is wiped. The state is rebuilt from the DA
/// batches alone through the production batch codec, reconstruction
/// and executor-image strip, and installed on every node. Each replica
/// resumes from the image's cursor with no restore and no fetch, and
/// the fleet converges to the exact state.
#[test]
fn a_total_wipe_resumes_every_replica_from_a_rebuilt_da_image() {
    let world = World::new();
    world.seal_through(5);
    let load = world.load(5, LAST, BLOCK_EVERY);
    let mut fleet = world.fleet();
    fleet.start();
    fleet.wait_head(15, CATCH_UP);
    fleet.stop();
    let head = fleet.0.iter().map(NodeHandle::durable_block).min().unwrap();
    fleet.0.iter().for_each(NodeHandle::wipe_all);
    let image = world.rebuild_image(head);
    fleet.0.iter().for_each(|n| n.install_image(&image));
    fleet.start();
    fleet.wait_head(load.sealed() + 5, CATCH_UP);
    let last = load.halt();
    fleet.assert_converged(&world, last, CATCH_UP);
    fleet.0.iter().for_each(|n| {
        let log = n.last_log();
        assert!(
            log.contains(&format!("resume_block={head}")) && log.contains(RESUMED),
            "{}: no resume from the image at {head}:\n{log}",
            n.name()
        );
        assert!(
            !log.contains(RESTORED) && !log.contains(FETCHED),
            "{}:\n{log}",
            n.name()
        );
        let env = kardamom_state::StateEnvBuilder::new(&n.cfg.state_dir)
            .open()
            .unwrap();
        assert!(
            !kardamom_state::has_trie(&env).unwrap(),
            "{}: the image keeps a trie that goes stale at the first executor block",
            n.name()
        );
    });
}

/// checkpoint-corrupt-recover: executor 0 is stopped, its newest local
/// checkpoint is verified, then one byte of its image flips; the older
/// local bases and the state DB go. The new process must reject and
/// quarantine the image before it fetches and restores a peer
/// checkpoint, never replay from genesis, keep the quarantined image
/// and manifest byte for byte, and converge.
#[test]
fn a_corrupt_local_checkpoint_is_quarantined_before_a_peer_restore() {
    let world = World::new();
    world.seal_through(5);
    let load = world.load(5, LAST, BLOCK_EVERY);
    let mut fleet = world.fleet();
    fleet.start();
    fleet.0[0].wait_checkpoint(8, CATCH_UP);
    fleet.0[1].wait_checkpoint(8, CATCH_UP);
    fleet.0[0].stop();
    let fault = fleet.0[0].corrupt_newest_checkpoint();
    fleet.0[0].start();
    fleet.wait_head(load.sealed() + 5, CATCH_UP);
    let head = load.halt();
    fleet.assert_converged(&world, head, CATCH_UP);
    let log = fleet.0[0].last_log();
    let at = |needle: &str| log.lines().position(|l| l.contains(needle));
    let rejected = log.lines().position(|l| {
        l.contains(&fault.name)
            && l.contains("CORRUPT")
            && l.contains("checkpoint failed verification; quarantining")
    });
    let order = (rejected, at(FETCHED), at(RESTORED));
    assert!(
        matches!(order, (Some(a), Some(b), Some(c)) if a < b && b < c),
        "reject, fetch and restore in order: {order:?}:\n{log}"
    );
    assert!(!log.contains(GENESIS_REPLAY), "a genesis replay:\n{log}");
    fault.assert_quarantined(&fleet.0[0]);
}

/// The evidence of one corrupted image.
pub(crate) struct CorruptImage {
    name: String,
    image: Vec<u8>,
    manifest: Vec<u8>,
}

impl CorruptImage {
    fn assert_quarantined(&self, node: &NodeHandle) {
        let rejected = node
            .cfg
            .checkpoint_dir
            .join(format!(".rejected-{}", self.name));
        assert!(
            std::fs::read(rejected.join("mdbx.dat")).unwrap() == self.image,
            "the quarantined image is the faulted image"
        );
        assert_eq!(
            std::fs::read(rejected.join("MANIFEST")).unwrap(),
            self.manifest
        );
    }
}

impl NodeHandle {
    /// Verify the newest checkpoint with the production restore, flip
    /// the byte in the middle of its image, delete the older bases and
    /// the state DB.
    fn corrupt_newest_checkpoint(&self) -> CorruptImage {
        let (block, path) = self.newest_checkpoint().unwrap();
        let scratch = tempfile::tempdir().unwrap();
        let copy = scratch.path().join("checkpoints");
        std::fs::create_dir_all(&copy).unwrap();
        NodeHandle::copy_dir(&path, &copy.join(path.file_name().unwrap()));
        let restored = kardamom_state::restore_best_checkpoint(
            &copy,
            &scratch.path().join("state"),
            kardamom_state::BlockRange::ANY,
            None,
        )
        .unwrap();
        assert_eq!(
            restored.map(|(b, _)| b),
            Some(block),
            "the baseline restores"
        );
        let image = path.join("mdbx.dat");
        let mut bytes = std::fs::read(&image).unwrap();
        let middle = bytes.len() / 2;
        bytes[middle] ^= 0xFF;
        std::fs::write(&image, &bytes).unwrap();
        std::fs::read_dir(&self.cfg.checkpoint_dir)
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| {
                p != &path
                    && p.file_name()
                        .unwrap()
                        .to_string_lossy()
                        .starts_with("checkpoint-")
            })
            .for_each(|p| std::fs::remove_dir_all(p).unwrap());
        self.wipe_state();
        CorruptImage {
            name: path.file_name().unwrap().to_string_lossy().into_owned(),
            image: bytes,
            manifest: std::fs::read(path.join("MANIFEST")).unwrap(),
        }
    }

    /// Copy a rebuilt executor image into the empty state directory.
    fn install_image(&self, image: &std::path::Path) {
        NodeHandle::copy_dir(image, &self.cfg.state_dir);
        let _ = std::fs::remove_file(self.cfg.state_dir.join("mdbx.lck"));
    }
}

impl NodeHandle {
    /// The restarted node answers from its committed state: the exact
    /// nonce of every sender at the block it reports, and the receipt of
    /// `committed`.
    fn assert_committed_answers(&self, world: &World, committed: alloy_primitives::B256) {
        world.chain.nonces_after(0).iter().for_each(|(sender, _)| {
            let (block, body) = self.query(
                "eth_getTransactionCount",
                &format!(r#"["{sender}","latest"]"#),
            );
            let want = world
                .chain
                .nonces_after(block)
                .iter()
                .find(|(s, _)| s == sender)
                .unwrap()
                .1;
            assert!(
                block > 0 && body.contains(&format!(r#""result":"{want:#x}""#)),
                "{}: nonce of {sender} at block {block}: {body} (want {want})",
                self.name()
            );
        });
        let (_, body) = self.query("eth_getTransactionReceipt", &format!(r#"["{committed}"]"#));
        assert!(
            body.contains(r#""result":"0x"#),
            "{}: no committed receipt from state: {body}",
            self.name()
        );
    }
}

impl World {
    /// Rebuild an executor image through `head` from the DA batches
    /// alone: pack the closed blocks as the batcher posts them, decode
    /// them as the reconstructor does, replay them into a fresh DB, and
    /// strip it to the executor image.
    fn rebuild_image(&self, head: u64) -> std::path::PathBuf {
        let blocks = self.chain.closed_blocks(head);
        let batch = kardamom_batcher::batcher::pack_blocks(
            &kardamom_batcher::batcher::BatcherConfig::default(),
            &blocks,
        )
        .unwrap();
        let frames = kardamom_batcher::recon::reconstruct(&batch.payload).unwrap();
        let dir = self.root.path().join("rebuilt");
        let (genesis, _) = kardamom_engine::bin_support::resolve_genesis(
            Some(&self.root.path().join("genesis.toml")),
            1,
        )
        .unwrap();
        let genesis = genesis.unwrap();
        let outcome =
            kardamom_reconstruct::reconstruct_state(&dir, &ReplayGenesis::of(&genesis), &frames)
                .unwrap();
        eprintln!("rebuilt image: {outcome:?}");
        kardamom_reconstruct::strip_to_executor_image(&dir).unwrap();
        dir
    }
}

/// The combined executor cases, the executor part: the sealers and the
/// executors die, and the executors return first. A returned executor
/// waits for a member without a failed exit and publishes nothing. While
/// it waits, it already answers the committed nonce of every sender (the
/// floor a parked sequencer looks up when Redis is empty) and the
/// receipt of a transaction committed before the fault (the receipt a
/// restarted ingress reads from state). Once the sealers return, it
/// catches up exactly.
#[test]
fn executors_that_return_before_the_sealers_wait_and_then_catch_up() {
    let world = World::new();
    world.seal_through(5);
    let mut fleet = world.fleet();
    fleet.start();
    fleet.wait_head(5, CATCH_UP);
    world.sealer.set_down(true);
    fleet.kill();
    fleet.start();
    std::thread::sleep(Duration::from_secs(3));
    fleet.0.iter_mut().for_each(|n| {
        n.assert_alive();
        assert_eq!(n.head(), 0, "{}: published with no sealer", n.name());
    });
    let committed = world.chain.envelope(0).tx_hash;
    fleet
        .0
        .iter()
        .for_each(|n| n.assert_committed_answers(&world, committed));
    let load = world.load(5, 100_000, BLOCK_EVERY);
    world.sealer.set_down(false);
    fleet.wait_head(load.sealed() + 5, CATCH_UP);
    let head = load.halt();
    fleet.assert_converged(&world, head, CATCH_UP);
    fleet.0.iter().for_each(NodeHandle::assert_no_republished);
}
