//! The offline replay and the live exec thread give the same state when
//! `FEATURE_SEALER_V3` activates at a time between two block headers.
//! Block N executes with block N-1's header time on both sides, so a block
//! whose own header reaches the activation time, and whose execution time
//! does not, burns no nonce on either side.

use alloy_primitives::U256;

use super::flag_chain::{Block, FlagChain, Item, Mode, TOO_MUCH};

/// The header times of blocks 1, 2 and 3. Block 2 executes with `T1`, and
/// block 3 with `T2`.
const T1: u64 = 1_000;
const T2: u64 = 2_000;
const T3: u64 = 3_000;
/// The activation time: after the execution time of block 2, at or before
/// its header time.
const ACTIVATION: u64 = 1_500;

/// Block 2's header reaches the activation time, and its execution time
/// does not: its skip uses no nonce, so block 3's transfer at nonce 0
/// executes. Block 3 executes past the activation time: its skip of
/// nonce 1 uses the nonce. The replay ends at the live root.
#[test]
fn a_replay_burns_only_where_the_live_chain_burns() {
    let chain = FlagChain::new(vec![
        Block {
            ts: T1,
            items: vec![Item::Schedule(ACTIVATION)],
        },
        Block {
            ts: T2,
            items: vec![Item::Tx {
                nonce: 0,
                value: TOO_MUCH,
            }],
        },
        Block {
            ts: T3,
            items: vec![
                Item::Tx { nonce: 0, value: 7 },
                Item::Tx {
                    nonce: 1,
                    value: TOO_MUCH,
                },
            ],
        },
    ]);
    let live = chain.live(Mode::Streaming);
    assert_eq!(
        (live.poor_nonce, live.payee_balance),
        (2, U256::from(7u64)),
        "nonce 0 executes in block 3, and nonce 1 burns"
    );
    assert_eq!(chain.replayed(), live);
}
