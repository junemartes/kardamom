//! The flags of a block are frozen before its first record. A
//! `setFeature` deposit inside a block changes nothing in that block, and
//! every driver switches at the same block: the live exec thread
//! (streaming and whole-block) and the offline replay.

use alloy_primitives::U256;

use super::flag_chain::{Block, FlagChain, Item, Mode, TOO_MUCH};

/// An activation time that every block of these chains reaches.
const NOW: u64 = 1;
/// An activation time that no block of these chains reaches: the off
/// switch.
const NEVER: u64 = 1_000_000;

fn skip(nonce: u64) -> Item {
    Item::Tx {
        nonce,
        value: TOO_MUCH,
    }
}

/// A block at header time `ts` with `items`.
fn block(ts: u64, items: Vec<Item>) -> Block {
    Block { ts, items }
}

/// Block 2 carries a skip, then the deposit that turns the feature on,
/// then a second skip of the same nonce. Block 2 executes at 1 000 ms, so
/// a flag read inside the block would burn the second skip. No skip of
/// block 2 burns.
#[test]
fn no_record_of_the_block_sees_its_own_set_feature() {
    let chain = FlagChain::new(vec![
        block(1_000, Vec::new()),
        block(2_000, vec![skip(0), Item::Schedule(NOW), skip(0)]),
    ]);
    let streaming = chain.live(Mode::Streaming);
    assert_eq!(streaming.poor_nonce, 0, "no skip of block 2 burns");
    assert_eq!(chain.live(Mode::Whole), streaming);
}

/// The chain above, with a block 3: block 3 reads the new flag, so its
/// skip burns, and its transfer at the next nonce lands.
#[test]
fn a_set_feature_inside_a_block_switches_from_the_next_block() {
    let chain = FlagChain::new(vec![
        block(1_000, Vec::new()),
        block(2_000, vec![skip(0), Item::Schedule(NOW), skip(0)]),
        block(3_000, vec![skip(0), Item::Tx { nonce: 1, value: 7 }]),
    ]);
    let streaming = chain.live(Mode::Streaming);
    assert_eq!(
        (streaming.poor_nonce, streaming.payee_balance),
        (2, U256::from(7u64)),
        "block 3 burns nonce 0 and runs nonce 1"
    );
    assert_eq!(chain.live(Mode::Whole), streaming);
}

/// `FEATURE_SEALER_V3` turns on, off and on again. Each switch takes
/// effect in the block after its deposit, and each on-phase has a burning
/// skip. Every driver ends at the same root.
#[test]
fn every_driver_switches_at_the_same_block() {
    let chain = FlagChain::new(vec![
        // Off: the deposit turns the feature on from block 2.
        block(1_000, vec![Item::Schedule(NOW), skip(0)]),
        // On: the skip burns nonce 0.
        block(2_000, vec![skip(0)]),
        // Still on: the deposit turns the feature off from block 4, and
        // the skip burns nonce 1.
        block(3_000, vec![Item::Schedule(NEVER), skip(1)]),
        // Off: the skip of nonce 2 burns nothing.
        block(4_000, vec![skip(2)]),
        // Still off: the deposit turns the feature on from block 6.
        block(5_000, vec![Item::Schedule(NOW), skip(2)]),
        // On: the skip burns nonce 2, and nonce 3 lands.
        block(6_000, vec![skip(2), Item::Tx { nonce: 3, value: 7 }]),
    ]);
    let streaming = chain.live(Mode::Streaming);
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
/// table has one, and in every block a skip of the poor sender at its chain
/// nonce. A skip burns exactly in the blocks that the table marks on, so
/// the nonce ends at the count of those blocks, on the live thread and in
/// the replay.
#[test]
fn the_table_switches_where_the_chain_switches() {
    let rows: Vec<(u64, Option<u64>, bool)> =
        include_str!("../../../../exec-core/testdata/feature_switch_blocks.txt")
            .lines()
            .filter(|line| !line.trim().is_empty() && !line.starts_with('#'))
            .map(|line| {
                let fields: Vec<&str> = line.split_whitespace().collect();
                (
                    fields[1].parse().expect("ts"),
                    (fields[3] != "-").then(|| fields[3].parse().expect("set_feature")),
                    fields[4] == "1",
                )
            })
            .collect();
    let on_blocks = rows.iter().filter(|(_, _, v3)| *v3).count() as u64;
    let blocks = rows
        .iter()
        .scan(0u64, |nonce, &(ts, set_feature, v3)| {
            let items = set_feature
                .map(Item::Schedule)
                .into_iter()
                .chain(std::iter::once(skip(*nonce)))
                .collect();
            *nonce += u64::from(v3);
            Some(block(ts, items))
        })
        .collect();
    let chain = FlagChain::new(blocks);
    let streaming = chain.live(Mode::Streaming);
    assert_eq!(streaming.poor_nonce, on_blocks, "one burn in each on-block");
    assert_eq!(chain.replayed(), streaming, "replay");
}
