//! A block with a nonce burn: the seeded parallel batches agree with the
//! sequential ground truth, and the burn's nonce write verifies against
//! its BAL claim.

use alloy_primitives::{B256, U256, address};
use alloy_signer_local::PrivateKeySigner;
use kardamom_engine::actor::BufferedRecord;
use kardamom_exec_core::features::activation_slot;
use kardamom_types::SkipReason;
use kardamom_types::upgrades::{CHAIN_STATE, FEATURE_SEALER_V3};

use crate::parallel::{BlockInputs, execute_block_parallel};

use super::fixtures::{env, fund, honest_claims, nz, nz16, seq_delta, test_pool, tx};

#[test]
fn parallel_batches_equal_sequential_on_a_block_with_a_burn() {
    let signer = PrivateKeySigner::random();
    let to = address!("00000000000000000000000000000000000000BB");
    let snap = fund(
        kardamom_exec_core::state::MockStateDatabase::builder(),
        signer.address(),
    )
    .account(CHAIN_STATE, U256::ZERO, 1, B256::repeat_byte(0xC5))
    .storage(
        CHAIN_STATE,
        activation_slot(FEATURE_SEALER_V3),
        U256::from(1u64),
    )
    .build();
    // Nonce 2 asks for more than the funded balance: it skips, and burns.
    let txs: Vec<BufferedRecord> = (0..6)
        .map(|i| {
            let value = if i == 2 { u64::MAX } else { 1_000 + i };
            tx(&signer, to, i, value, i)
        })
        .collect();
    let claims = honest_claims(&snap, &txs);
    let expected = seq_delta(&snap, &txs);
    assert_eq!(
        expected.accounts.get(&signer.address()).map(|a| a.nonce),
        Some(6),
        "every nonce is used"
    );
    for batch_size in [1usize, 2, 4] {
        let out = execute_block_parallel(
            &test_pool(),
            &BlockInputs {
                snapshot: &snap,
                parent: None,
                txs: &txs,
                claims: &claims,
                env: env(),
                granularity: nz16(1),
            },
            nz(batch_size),
        )
        .unwrap_or_else(|e| panic!("batch_size {batch_size}: {e:?}"));
        assert_eq!(
            out.delta.accounts, expected.accounts,
            "batch_size {batch_size}"
        );
        let skip = &out.receipts[2];
        assert_eq!(skip.skip_reason, Some(SkipReason::InsufficientFunds));
        assert!(skip.burns_nonce(), "batch_size {batch_size}: {skip:?}");
        assert!(out.receipts[3].status, "batch_size {batch_size}");
    }
}
