//! The flags of a block are frozen before its first record. A
//! `setFeature` deposit inside a block changes nothing in that block, and
//! every driver switches at the same block: the live exec thread
//! (streaming and whole-block) and the offline replay.

#[path = "../../../../exec-core/tests/feature_table/mod.rs"]
mod feature_table;

use alloy_primitives::U256;

use super::flag_chain::{Block, FlagChain, Item, Mode};
use feature_table::Row;

/// An activation time that every block of these chains reaches.
const NOW: u64 = 1;
/// An activation time that no block of these chains reaches: the off
/// switch.
const NEVER: u64 = 1_000_000;

/// A block at header time `ts` with `items`.
fn block(ts: u64, items: Vec<Item>) -> Block {
    Block { ts, items }
}

fn schedule(at: u64) -> Item {
    Item::Schedule(vec![at])
}

/// Block 2 carries a skip, then the deposit that turns the feature on,
/// then a second skip of the same nonce. Block 2 executes at 1 000 ms, so
/// a flag read inside the block would burn the second skip. No skip of
/// block 2 burns.
#[test]
fn no_record_of_the_block_sees_its_own_set_feature() {
    let chain = FlagChain::new(vec![
        block(1_000, Vec::new()),
        block(2_000, vec![Item::Skip(0), schedule(NOW), Item::Skip(0)]),
    ]);
    let streaming = chain.live(Mode::Streaming);
    assert_eq!(streaming.burns, [vec![], vec![false, false]]);
    assert_eq!(streaming.poor_nonce, 0);
    assert_eq!(chain.live(Mode::Whole), streaming);
}

/// The chain above, with a block 3: block 3 reads the new flag, so its
/// skip burns, and its transfer at the next nonce lands.
#[test]
fn a_set_feature_inside_a_block_switches_from_the_next_block() {
    let chain = FlagChain::new(vec![
        block(1_000, Vec::new()),
        block(2_000, vec![Item::Skip(0), schedule(NOW), Item::Skip(0)]),
        block(3_000, vec![Item::Skip(0), Item::Tx { nonce: 1, value: 7 }]),
    ]);
    let streaming = chain.live(Mode::Streaming);
    assert_eq!(
        streaming.burns,
        [vec![], vec![false, false], vec![true, false]]
    );
    assert_eq!(
        (streaming.poor_nonce, streaming.payee_balance),
        (2, U256::from(7u64)),
        "block 3 burns nonce 0 and runs nonce 1"
    );
    assert_eq!(chain.live(Mode::Whole), streaming);
}

/// `FEATURE_SEALER_V3` turns on, off and on again. Each switch takes
/// effect in the block after its deposit, and each on-phase has a burning
/// skip. Every driver ends at the same root, with the same burns in every
/// block.
#[test]
fn every_driver_switches_at_the_same_block() {
    let chain = FlagChain::new(vec![
        // Off: the deposit turns the feature on from block 2.
        block(1_000, vec![schedule(NOW), Item::Skip(0)]),
        // On: the skip burns nonce 0.
        block(2_000, vec![Item::Skip(0)]),
        // Still on: the deposit turns the feature off from block 4, and
        // the skip burns nonce 1.
        block(3_000, vec![schedule(NEVER), Item::Skip(1)]),
        // Off: the skip of nonce 2 burns nothing.
        block(4_000, vec![Item::Skip(2)]),
        // Still off: the deposit turns the feature on from block 6.
        block(5_000, vec![schedule(NOW), Item::Skip(2)]),
        // On: the skip burns nonce 2, and nonce 3 lands.
        block(6_000, vec![Item::Skip(2), Item::Tx { nonce: 3, value: 7 }]),
    ]);
    let streaming = chain.live(Mode::Streaming);
    assert_eq!(
        streaming.burns,
        [
            vec![false],
            vec![true],
            vec![true],
            vec![false],
            vec![false],
            vec![true, false],
        ]
    );
    assert_eq!(
        (streaming.poor_nonce, streaming.payee_balance),
        (4, U256::from(7u64)),
        "three burns, then nonce 3 executes"
    );
    assert_eq!(chain.live(Mode::Whole), streaming, "whole-block");
    assert_eq!(chain.replayed(), streaming, "replay");
}

/// The blocks of `exec-core/testdata/feature_switch_blocks.txt`, the table
/// that the sealer tests share, with real `setFeature` deposits where the
/// table has them (one L1 epoch for each such block), and in every block
/// one skip of the poor sender at its chain nonce. The skip of a block
/// burns exactly when the table marks the block on, block by block, on the
/// live thread and in the replay.
#[test]
fn the_table_switches_where_the_chain_switches() {
    let rows = Row::all();
    let blocks = rows
        .iter()
        .scan(0u64, |nonce, row| {
            let items = (!row.set_features.is_empty())
                .then(|| Item::Schedule(row.set_features.clone()))
                .into_iter()
                .chain(std::iter::once(Item::Skip(*nonce)))
                .collect();
            *nonce += u64::from(row.v3);
            Some(block(row.ts, items))
        })
        .collect();
    let chain = FlagChain::new(blocks);
    let streaming = chain.live(Mode::Streaming);
    let expected: Vec<Vec<bool>> = rows.iter().map(|row| vec![row.v3]).collect();
    let numbered = |burns: &[Vec<bool>]| -> Vec<(u64, Vec<bool>)> {
        rows.iter()
            .map(|row| row.number)
            .zip(burns.iter().cloned())
            .collect()
    };
    assert_eq!(numbered(&streaming.burns), numbered(&expected));
    assert_eq!(chain.replayed(), streaming, "replay");
}
