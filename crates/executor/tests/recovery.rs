//! Executor recovery: the chaos executor cases on real local processes.
//!
//! Each node is a child process of this test binary. It runs the
//! production state startup, engine, MDBX writer, checkpoint rounds,
//! checkpoint server and replay repair. The parent plays the sealer: it
//! appends production egress frames to a retained log, which each
//! process reads through one replay session. The parent stops, kills,
//! wipes and replaces nodes, then checks exact state parity with an
//! undisturbed replica, a whole state DB, and an exact receipt stream.

#![cfg(unix)]

#[path = "recovery/chain.rs"]
mod chain;
#[path = "recovery/fleet.rs"]
mod fleet;
#[path = "recovery/node.rs"]
mod node;
#[path = "recovery/replica.rs"]
mod replica;
#[path = "recovery/sealer.rs"]
mod sealer;
#[cfg(feature = "docker-e2e")]
#[path = "recovery/stream.rs"]
mod stream;
#[path = "recovery/verdict.rs"]
mod verdict;
#[path = "recovery/whole.rs"]
mod whole;

use std::time::Duration;

use fleet::{NODE_ENV, World};
use verdict::{CATCH_UP, Stream};

/// The pause between two sealed blocks of the live load.
const BLOCK_EVERY: Duration = Duration::from_millis(40);

/// The resume line of a process that opens its own state.
const RESUMED: &str = "resuming from persisted state cursor via cluster canonical replay";
/// The restore line of a process that restores a checkpoint.
const RESTORED: &str = "restored state from checkpoint";
/// The fetch line of a process that takes a peer checkpoint.
const FETCHED: &str = "fetched checkpoint from peer";
/// The line of a fresh start that finds no checkpoint.
const GENESIS_REPLAY: &str = "fresh start will replay from genesis";

#[test]
#[ignore = "child entry point; the parent supplies owned storage and stops this process"]
fn recovery_node() {
    let path = std::env::var_os(NODE_ENV).unwrap();
    let cfg: node::NodeConfig = toml::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    node::Node::main(cfg).unwrap();
}

/// graceful-executor: SIGTERM stops the process cleanly mid-stream. The
/// next process resumes from its durable cursor while the sealer keeps
/// ordering, and ends with the exact state of an undisturbed replica.
#[test]
fn a_graceful_stop_resumes_from_the_durable_cursor_without_loss_or_duplicate() {
    let world = World::new();
    world.seal_through(5);
    let load = world.load(5, 100_000, BLOCK_EVERY);
    let mut victim = world.node("executor-0");
    victim.start();
    victim.wait_head(10, CATCH_UP);
    victim.stop();
    let durable = victim.durable_block();
    let published = *Stream::read(&victim).run(1).last().unwrap();
    assert!(
        published <= durable,
        "boundary {published} went out before its commit ({durable})"
    );
    std::thread::sleep(BLOCK_EVERY * 5);
    victim.start();
    victim.wait_head(load.sealed() + 5, CATCH_UP);
    let head = load.halt();
    victim.wait_head(head, CATCH_UP);
    victim.stop();
    assert_eq!(victim.count(RESUMED), 1, "the second process resumes once");
    assert_eq!(victim.count(RESTORED), 0, "a kept state needs no restore");
    let reference = world.reference(head);
    victim.assert_matches(&reference, head);
    victim.assert_no_republished();
}
