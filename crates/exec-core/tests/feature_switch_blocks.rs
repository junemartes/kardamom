//! The flags of each block of `testdata/feature_switch_blocks.txt`, read
//! with [`BlockFeatures::read`]: the state after the block before, at the
//! time that the block executes with (the header time of the block
//! before, 0 for block 1). A `setFeature` deposit in a block stores its
//! time, and a time of 0 stores the time that its block executes with, as
//! `KardamomChainState.setFeature` does. So a deposit in block N takes
//! effect from block N+1, and the last deposit of a block decides. The
//! Java test `FeatureSwitchBlocksTest` runs the sealer over the same
//! blocks, and the engine test `the_table_switches_where_the_chain_switches`
//! runs the real deposits.

mod feature_table;

use alloy_primitives::U256;
use feature_table::Row;
use kardamom_exec_core::delta::PendingDelta;
use kardamom_exec_core::features::{BlockFeatures, activation_slot};
use kardamom_exec_core::state::MockStateDatabase;
use kardamom_types::upgrades::{CHAIN_STATE, FEATURE_SEALER_V3};

/// The state after the blocks so far (the genesis snapshot under the
/// written layer), and the time that the next block executes with.
struct Chain {
    genesis: MockStateDatabase,
    written: PendingDelta,
    exec_ts: u64,
}

impl Chain {
    /// Run `row`: freeze its flags from the state before it, then let its
    /// deposits store their activation words, in order.
    fn run(mut self, row: &Row) -> Self {
        let frozen =
            BlockFeatures::read(&self.genesis, Some(&self.written), self.exec_ts).expect("flags");
        assert_eq!(frozen.sealer_v3(), row.v3, "block {}", row.number);
        let exec_ts = self.exec_ts;
        self.written
            .storage
            .extend(row.set_features.iter().map(|&time| {
                let word = if time == 0 { exec_ts } else { time };
                (
                    (CHAIN_STATE, activation_slot(FEATURE_SEALER_V3)),
                    U256::from(word),
                )
            }));
        self.exec_ts = row.ts;
        self
    }
}

#[test]
fn the_executor_switches_the_feature_at_the_blocks_of_the_fixture() {
    let rows = Row::all();
    assert_eq!(rows.len(), 24);
    assert!(
        rows.iter().any(|row| row.set_features.len() > 1),
        "the table has a block with two deposits"
    );
    rows.iter().fold(
        Chain {
            genesis: MockStateDatabase::builder().build(),
            written: PendingDelta::new(),
            exec_ts: 0,
        },
        Chain::run,
    );
}
