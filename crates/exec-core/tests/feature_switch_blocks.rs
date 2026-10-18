//! The flags of each block of `testdata/feature_switch_blocks.txt`, read
//! with [`BlockFeatures::read`]: the state after the block before, at the
//! time that the block executes with (the header time of the block
//! before, 0 for block 1). A `setFeature` deposit in a block stores its
//! time, and a time of 0 stores the time that its block executes with, as
//! `KardamomChainState.setFeature` does. So a deposit in block N takes
//! effect from block N+1. The Java test `FeatureSwitchBlocksTest` runs the
//! sealer over the same blocks, and the engine test
//! `the_table_switches_where_the_chain_switches` runs the real deposits.

use alloy_primitives::U256;
use kardamom_exec_core::delta::PendingDelta;
use kardamom_exec_core::features::{BlockFeatures, activation_slot};
use kardamom_exec_core::state::MockStateDatabase;
use kardamom_types::upgrades::{CHAIN_STATE, FEATURE_SEALER_V3};

const BLOCKS: &str = include_str!("../testdata/feature_switch_blocks.txt");

/// One block of the fixture.
struct Block {
    number: u64,
    ts: u64,
    set_feature: Option<u64>,
    v3: bool,
}

impl Block {
    fn parse(line: &str) -> Self {
        let fields: Vec<&str> = line.split_whitespace().collect();
        assert_eq!(fields.len(), 5, "a block line has five fields: {line}");
        Self {
            number: fields[0].parse().unwrap(),
            ts: fields[1].parse().unwrap(),
            set_feature: (fields[3] != "-").then(|| fields[3].parse().unwrap()),
            v3: fields[4] == "1",
        }
    }
}

/// The state after the blocks so far (the genesis snapshot under the
/// written layer), and the time that the next block executes with.
struct Chain {
    genesis: MockStateDatabase,
    written: PendingDelta,
    exec_ts: u64,
}

impl Chain {
    /// Run `block`: freeze its flags from the state before it, then let
    /// its deposit store a new activation word.
    fn run(mut self, block: &Block) -> Self {
        let frozen =
            BlockFeatures::read(&self.genesis, Some(&self.written), self.exec_ts).expect("flags");
        assert_eq!(frozen.sealer_v3(), block.v3, "block {}", block.number);
        if let Some(time) = block.set_feature {
            let word = if time == 0 { self.exec_ts } else { time };
            self.written.storage.insert(
                (CHAIN_STATE, activation_slot(FEATURE_SEALER_V3)),
                U256::from(word),
            );
        }
        self.exec_ts = block.ts;
        self
    }
}

#[test]
fn the_executor_switches_the_feature_at_the_blocks_of_the_fixture() {
    let blocks: Vec<Block> = BLOCKS
        .lines()
        .filter(|line| !line.trim().is_empty() && !line.starts_with('#'))
        .map(Block::parse)
        .collect();
    assert_eq!(blocks.len(), 20);
    blocks.iter().fold(
        Chain {
            genesis: MockStateDatabase::builder().build(),
            written: PendingDelta::new(),
            exec_ts: 0,
        },
        Chain::run,
    );
}
