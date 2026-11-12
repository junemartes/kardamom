//! The cases that take one replica down while its peers keep running.

use std::time::Duration;

use super::chain::Chain;
use super::fleet::World;
use super::verdict::{CATCH_UP, Stream};
use super::{BLOCK_EVERY, FETCHED, GENESIS_REPLAY, RESTORED, RESUMED};

/// The last block of each case's load.
/// The load runs until the case halts it.
const LAST: u64 = 100_000;

/// hard-executor and node-failure-executor: SIGKILL takes one replica
/// with no warning, as a node loss does; its disks stay. The two peers
/// keep executing the blocks the sealer keeps ordering. The returned
/// process resumes from its own durable cursor, and all three end with
/// the exact state of an undisturbed replica.
#[test]
fn a_killed_replica_resumes_from_its_disk_while_its_peers_progress() {
    let world = World::new();
    world.seal_through(5);
    let load = world.load(5, LAST, BLOCK_EVERY);
    let mut fleet = world.fleet();
    fleet.start();
    fleet.wait_head(12, CATCH_UP);
    let victim = &mut fleet.0[1];
    victim.kill();
    let published = victim.head();
    let durable = victim.durable_block();
    assert!(
        published <= durable,
        "boundary {published} went out before its durable commit ({durable})"
    );
    let outage = load.sealed() + 8;
    fleet.0[0].wait_head(outage, CATCH_UP);
    fleet.0[2].wait_head(outage, CATCH_UP);
    let victim = &mut fleet.0[1];
    assert_eq!(victim.head(), published, "a dead replica publishes nothing");
    victim.start();
    fleet.wait_head(load.sealed() + 5, CATCH_UP);
    let head = load.halt();
    fleet.assert_converged(&world, head, CATCH_UP);
    fleet
        .0
        .iter()
        .for_each(super::fleet::NodeHandle::assert_no_republished);
    let victim = &fleet.0[1];
    assert_eq!(
        victim.count(RESUMED),
        1,
        "the returned process resumes once"
    );
    assert_eq!(victim.count(RESTORED), 0, "a kept disk needs no restore");
}

/// node-replace-executor: the node of one replica comes back as a new
/// machine, on another address, with empty volumes. The sealer no
/// longer retains block 1, so a replay from genesis is refused. The
/// new replica must fetch a peer checkpoint over HTTP, restore it, and
/// catch up to the exact state of its peers.
#[test]
fn a_blank_replacement_fetches_a_peer_checkpoint_and_catches_up() {
    let world = World::new();
    world.seal_through(5);
    let load = world.load(5, LAST, BLOCK_EVERY);
    let mut fleet = world.fleet();
    fleet.start();
    fleet.wait_head(15, CATCH_UP);
    fleet.0[0].wait_checkpoint(10, CATCH_UP);
    fleet.0[1].kill();
    world.sealer.set_floor(Chain::first_index(5), 5);
    let mut replacement = world.node("executor-1-replacement");
    replacement.cfg.peers = vec![
        fleet.0[0].cfg.serve.to_string(),
        fleet.0[2].cfg.serve.to_string(),
    ];
    let old_address = std::mem::replace(&mut fleet.0[1], replacement).cfg.serve;
    assert_ne!(old_address, fleet.0[1].cfg.serve, "a new address");
    fleet.0[1].start();
    fleet.wait_head(load.sealed() + 5, CATCH_UP);
    let head = load.halt();
    fleet.assert_converged(&world, head, CATCH_UP);
    let new = &fleet.0[1];
    let log = new.all_logs();
    assert!(log.contains(FETCHED), "no peer fetch:\n{log}");
    assert_eq!(new.count(RESTORED), 1, "one restore:\n{log}");
    assert!(!log.contains(GENESIS_REPLAY), "a genesis replay:\n{log}");
}

/// state-checkpoint-restore: the state and the checkpoints of executor
/// 0 are wiped while it is down, and the harness installs the newest
/// base of executor 1, and its block log first, as the deployed case
/// does with the block log on. Only the new process may prove the
/// restore: it restores the base, applies the log past it, replays the
/// tail and converges.
#[test]
fn a_wiped_replica_restores_a_copied_donor_base_and_log() {
    copied_restore(true);
}

/// state-checkpoint-restore with the block log off: the copy is the
/// newest base alone.
#[test]
fn a_wiped_replica_restores_a_copied_donor_checkpoint() {
    copied_restore(false);
}

fn copied_restore(with_log: bool) {
    let world = World::new();
    world.seal_through(5);
    let load = world.load(5, LAST, BLOCK_EVERY);
    let mut fleet = world.fleet();
    fleet.0.iter_mut().for_each(|n| n.cfg.block_log = with_log);
    fleet.start();
    let (block, donor) = fleet.0[1].wait_checkpoint(if with_log { 1 } else { 10 }, CATCH_UP);
    fleet.0[1].wait_head(block + 8, CATCH_UP);
    let victim = &mut fleet.0[0];
    victim.kill();
    victim.wipe_all();
    victim.install_checkpoint(&donor, with_log);
    victim.start();
    fleet.wait_head(load.sealed() + 5, CATCH_UP);
    let head = load.halt();
    fleet.assert_converged(&world, head, CATCH_UP);
    let victim = &fleet.0[0];
    let log = victim.last_log();
    let restored: u64 = log
        .lines()
        .find(|l| l.contains(RESTORED))
        .and_then(|l| {
            l.split("restored_block=")
                .nth(1)?
                .split(' ')
                .next()?
                .parse()
                .ok()
        })
        .unwrap_or_else(|| panic!("the new process restores no copy:\n{log}"));
    let applied = log.contains("applied the block log after the base checkpoint");
    assert!(
        applied || !with_log,
        "the copied log is not applied:\n{log}"
    );
    assert!(
        if with_log {
            restored > block
        } else {
            restored == block
        },
        "restored block {restored} from base {block}:\n{log}"
    );
    assert!(!log.contains(FETCHED), "the copy needs no fetch:\n{log}");
    assert_eq!(victim.count(RESTORED), 1);
}

/// replay-window-resync, wiped path: executor 1 loses its state and its
/// checkpoints, and the sealer no longer retains block 1. With the
/// block log on, the new process fetches the base and the log of a peer
/// before its first join, applies the log past the base, and converges.
#[test]
fn a_wiped_replica_behind_the_window_fetches_a_peer_base_and_log() {
    let world = World::new();
    world.seal_through(5);
    let load = world.load(5, LAST, BLOCK_EVERY);
    let mut fleet = world.fleet();
    fleet.0.iter_mut().for_each(|n| n.cfg.block_log = true);
    fleet.start();
    fleet.wait_head(15, CATCH_UP);
    let (base, _) = fleet.0[0].wait_checkpoint(1, CATCH_UP);
    fleet.0[2].wait_checkpoint(1, CATCH_UP);
    fleet.0[1].kill();
    fleet.0[1].wipe_all();
    world
        .sealer
        .set_floor(Chain::first_index(base + 2), base + 2);
    fleet.0[1].start();
    fleet.wait_head(load.sealed() + 5, CATCH_UP);
    let head = load.halt();
    fleet.assert_converged(&world, head, CATCH_UP);
    let log = fleet.0[1].last_log();
    for line in [
        FETCHED,
        RESTORED,
        "fetched the block log from a peer",
        "applied the block log after the base checkpoint",
    ] {
        assert!(log.contains(line), "no {line:?}:\n{log}");
    }
    assert!(!log.contains(GENESIS_REPLAY), "a genesis replay:\n{log}");
}

/// replay-window-resync, overrun path: executor 2 keeps its state, but
/// its cursor falls below the sealer's retention floor while it is
/// down. The sealer refuses the replay; the process stages a peer
/// checkpoint at or above the floor, parks the stale DB, restores in
/// the same process, and converges.
#[test]
fn a_cursor_below_the_retention_floor_stages_a_peer_checkpoint_in_process() {
    let world = World::new();
    world.seal_through(5);
    let load = world.load(5, LAST, BLOCK_EVERY);
    let mut fleet = world.fleet();
    fleet.start();
    fleet.wait_head(8, CATCH_UP);
    fleet.0[2].stop();
    let stale = fleet.0[2].durable_block();
    let (floor, _) = fleet.0[0].wait_checkpoint(stale + 6, CATCH_UP);
    fleet.0[1].wait_checkpoint(floor, CATCH_UP);
    world.sealer.set_floor(Chain::first_index(floor), floor);
    fleet.0[2].start();
    fleet.wait_head(load.sealed() + 5, CATCH_UP);
    let head = load.halt();
    fleet.assert_converged(&world, head, CATCH_UP);
    let log = fleet.0[2].last_log();
    for line in [
        "cluster replay unavailable",
        FETCHED,
        "parked stale state DB",
        "the pipeline starts again in-process",
        RESTORED,
    ] {
        assert!(log.contains(line), "no {line:?}:\n{log}");
    }
    assert_eq!(
        Stream::read(&fleet.0[2])
            .run(2)
            .first()
            .copied()
            .map(|b| b > floor),
        Some(true)
    );
}

/// deploy-broken-image, the executor part: a build that fails its start
/// checks replaces executor 2 and exits before it opens any state. The
/// two old replicas keep the chain advancing, the broken start leaves
/// the replica's state untouched, and a rolling replacement of all
/// three with the real build converges.
#[test]
fn a_failed_start_keeps_the_state_and_a_rolling_heal_converges() {
    let world = World::new();
    world.seal_through(5);
    let load = world.load(5, LAST, BLOCK_EVERY);
    let mut fleet = world.fleet();
    fleet.start();
    fleet.wait_head(10, CATCH_UP);
    fleet.0[2].stop();
    let before = fleet.0[2].state_bytes();
    let good = fleet.0[2].cfg.genesis.clone();
    fleet.0[2].cfg.genesis = good.with_file_name("not-a-genesis.toml");
    fleet.0[2].start();
    let status = fleet.0[2].wait_failed(CATCH_UP);
    assert!(
        !status.success(),
        "the broken build must fail its start checks"
    );
    assert_eq!(
        fleet.0[2].get("/ready"),
        None,
        "the broken build never serves /ready"
    );
    let spared = load.sealed() + 5;
    fleet.0[0].wait_head(spared, CATCH_UP);
    fleet.0[1].wait_head(spared, CATCH_UP);
    assert_eq!(
        fleet.0[2].state_bytes(),
        before,
        "a failed start changes no state"
    );
    fleet.0[2].cfg.genesis = good;
    fleet.0[2].start();
    fleet.0[2].wait_head(load.sealed(), CATCH_UP);
    fleet.0[2].wait_ready(CATCH_UP);
    (0..2).for_each(|i| {
        let node = &mut fleet.0[i];
        node.stop();
        node.start();
        node.wait_ready(CATCH_UP);
        node.wait_head(load.sealed(), CATCH_UP);
    });
    fleet.wait_head(load.sealed() + 5, CATCH_UP);
    let head = load.halt();
    fleet.assert_converged(&world, head, CATCH_UP);
    fleet
        .0
        .iter()
        .for_each(super::fleet::NodeHandle::assert_no_republished);
}

/// The waits of the restart storm, one per round.
const STORM_ROUNDS: u32 = 10;

/// executor-restart-storm: the executor job stops and starts ten times
/// under load. Every round, every replica must advance past the head it
/// stopped at, and the fleet must end in the exact state.
#[test]
fn ten_stop_and_start_rounds_advance_every_replica_each_round() {
    let world = World::new();
    world.seal_through(5);
    let load = world.load(5, 100_000, Duration::from_millis(15));
    let mut fleet = world.fleet();
    fleet.start();
    fleet.wait_head(5, CATCH_UP);
    let slowest = (1..=STORM_ROUNDS)
        .map(|_| {
            fleet.stop();
            fleet.start();
            fleet.wait_head(load.sealed() + 1, CATCH_UP)
        })
        .max()
        .unwrap_or_default();
    eprintln!("restart storm: slowest round {slowest:?}");
    let head = load.halt();
    fleet.assert_converged(&world, head, CATCH_UP);
    fleet
        .0
        .iter()
        .for_each(super::fleet::NodeHandle::assert_no_republished);
}
