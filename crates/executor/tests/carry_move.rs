//! An executor moves from the reference stream to carry mode.
//!
//! An executor runs the real pipeline in reference mode: the reader joins
//! the references of the cluster egress with the envelopes of the
//! `tx_data` lane. It stops after a committed block, and starts again in
//! carry mode from its committed state: the reader reads the entries with
//! their bytes from an egress, from the cursor after that block. Three L1
//! epochs credit deposits before, across and after the move. Every
//! executor that moves must end with the receipts, the boundaries and the
//! accounts of an executor that never moves, and every deposit must
//! credit exactly once, in L1 order.

#[path = "carry_move/chain.rs"]
mod chain;

use chain::Chain;

/// Executor 0 moves to carry mode after block 4, between two deposits.
/// Its receipts, boundaries and accounts match the executors in
/// reference mode.
#[test]
fn the_first_executor_in_carry_mode_matches_the_reference_executors() {
    let chain = Chain::new();
    let reference = chain.reference();
    reference.assert_credits(&chain);
    chain.moving(4).assert_matches(&reference, &chain);
}

/// Executors 1 and 2 move one at a time, at different blocks: one before
/// the second deposit, one after it. Every executor ends with the state of
/// the executor that never moves.
#[test]
fn the_other_executors_move_one_at_a_time_and_match_the_reference() {
    let chain = Chain::new();
    let reference = chain.reference();
    [2, 6]
        .into_iter()
        .for_each(|cut| chain.moving(cut).assert_matches(&reference, &chain));
}
