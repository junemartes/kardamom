//! The readers of the egress across a hard restart of the egress.
//!
//! The egress runs in a child process: its real store, owner thread and
//! HTTP side. The first blocks hold transaction references, as under
//! decision version 4. R, the highest stored reference, is the last of
//! them. The later blocks hold entries with their bytes, as under version
//! 5, so R does not move. Two readers, as the batcher and the validator
//! read the egress, start past R and read the entries with their bytes. A
//! reader below R meets a reference and stops.
//!
//! The parent kills the child with SIGKILL, so the side file of the mark
//! keeps its lead over R. A new child opens the same store. Its R must be
//! the R before the restart, and both readers must read on through it.

#![cfg(unix)]

#[path = "carry_egress_restart/support.rs"]
mod support;

use support::{Chain, EgressChild, Mark, Phase, Reader};

/// The batcher and the validator read past R after the hard restart of the
/// egress, and R stays.
#[test]
fn readers_past_r_read_on_across_a_hard_restart_and_r_stays() {
    let chain = Chain::new();
    let dir = tempfile::tempdir().unwrap();
    let first = EgressChild::start(dir.path(), Phase::First);
    first.await_head(Phase::First.last_block());
    assert_eq!(first.mark(), Chain::R, "R is the last reference");

    let past = Chain::R.next_cursor();
    let batcher = Reader::start(first.base(), past, Reader::BATCHER);
    let validator = Reader::start(first.base(), past, Reader::VALIDATOR);
    for reader in [&batcher, &validator] {
        reader.expect(&chain, Phase::First.blocks_past_r());
    }
    assert!(
        Reader::start(first.base(), (0, 1), Reader::BATCHER).stops(),
        "a reader below R meets a reference and stops"
    );

    first.kill();
    assert!(
        Mark::in_file(dir.path()).leads(Chain::R),
        "the hard stop leaves the side file above R"
    );
    let second = EgressChild::start(dir.path(), Phase::Second);
    second.await_head(Phase::Second.last_block());
    assert_eq!(
        second.mark(),
        Chain::R,
        "R of the egress does not change across its restart"
    );

    for reader in [&batcher, &validator] {
        reader.read_on_after_the_restart(&chain);
    }
    assert_eq!(second.mark(), Chain::R, "entries with bytes do not move R");
}

/// The child entry point. The parent gives it the store and the phase,
/// and kills it.
#[test]
#[ignore = "child entry point; the parent owns the store and kills this process"]
fn carry_egress_child() {
    EgressChild::serve();
}
