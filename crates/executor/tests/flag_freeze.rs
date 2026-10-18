//! The Block-STM strategy switches a feature flag at the same block as the
//! sequential driver, on a chain that turns `FEATURE_SEALER_V3` on, off
//! and on again with `setFeature` deposits in the middle of blocks.

use std::num::NonZeroUsize;

use kardamom_engine::MockStateDatabase;
use kardamom_engine::actor::BlockExecStrategy;
use kardamom_engine::actor::flag_switches::FlagSwitches;
use kardamom_executor::parallel::{StmBlockExec, StmExecConfig};
use kardamom_types::Receipt;

/// Run the chain with the Block-STM strategy at `workers`. Each block gets
/// a fresh strategy: it has no timing history, so it runs the block on its
/// pool, never on its sequential decline arm.
fn run(chain: &FlagSwitches, workers: usize) -> Vec<Vec<Receipt>> {
    chain.run(|parent, records, env| {
        StmBlockExec::<MockStateDatabase>::spawn(StmExecConfig {
            workers: NonZeroUsize::new(workers).expect("non-zero"),
            pin_cores: Vec::new(),
            keep_hot: false,
        })
        .execute_block(
            chain.snapshot(),
            Some(parent),
            records,
            env,
            env.block_number,
        )
        .expect("stm strategy")
    })
}

#[test]
fn the_stm_strategy_switches_flags_at_the_same_block() {
    let chain = FlagSwitches::new();
    for workers in [1, 4] {
        FlagSwitches::assert_burns(&run(&chain, workers));
    }
}
