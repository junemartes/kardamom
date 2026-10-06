use super::*;
use alloy_primitives::{Address, U256, address};
use alloy_signer_local::PrivateKeySigner;
use kardamom_state::{StateSnapshot, empty_root};
use kardamom_types::StateDatabase;

use crate::actor::test_support::{deposit_epoch, fresh_env, genesis_for};

const CHAIN_ID: u64 = 1;

/// A signed legacy transfer. Thin wrapper over
/// `actor::test_support::legacy`, the one signed-legacy-transfer
/// fixture this crate's tests share; `CHAIN_ID` here is 1, matching
/// that helper's own chain id.
fn transfer(signer: &PrivateKeySigner, to: Address, nonce: u64, value: u64) -> TxEnvelope {
    crate::actor::test_support::legacy(signer, to, nonce, value)
}

/// The replay chain of a test allocation: chain id 1, no code, no
/// fee schedule.
fn test_genesis(accounts: &[AccountChange]) -> ReplayGenesis<'_> {
    ReplayGenesis {
        chain_id: CHAIN_ID,
        accounts,
        code: &[],
        fees: None,
    }
}

/// Two blocks of transfers: block 1 sends 100 then 50, block 2 sends 25.
fn two_blocks(signer: &PrivateKeySigner, to1: Address, to2: Address) -> Vec<ReplayBlock> {
    vec![
        ReplayBlock {
            block_number: 1,
            l2_timestamp: 1_700_000_000,
            canonical_end: None,
            l1_epochs: Vec::new(),
            remote_epochs: Vec::new(),
            txs: vec![transfer(signer, to1, 0, 100), transfer(signer, to2, 1, 50)],
        },
        ReplayBlock {
            block_number: 2,
            l2_timestamp: 1_700_000_001,
            canonical_end: None,
            l1_epochs: Vec::new(),
            remote_epochs: Vec::new(),
            txs: vec![transfer(signer, to1, 2, 25)],
        },
    ]
}

/// The live chain of `two_blocks` with an epoch before each block:
/// an empty epoch takes one slot before block 1, and an epoch with
/// two deposits takes three before block 2. The payload carries
/// neither, only each block's end index and origin.
fn two_blocks_after_epochs(
    signer: &PrivateKeySigner,
    to1: Address,
    to2: Address,
) -> Vec<ReplayBlock> {
    let ends = [
        CanonicalEnd {
            end_tx_idx: 3,
            l1_origin: 40,
        },
        CanonicalEnd {
            end_tx_idx: 7,
            l1_origin: 41,
        },
    ];
    two_blocks(signer, to1, to2)
        .into_iter()
        .zip(ends)
        .map(|(block, end)| ReplayBlock {
            canonical_end: Some(end),
            ..block
        })
        .collect()
}

/// The rebuilt cursor, headers and receipt positions are the live
/// chain's, not a count of the replayed items, and the root is the
/// same with and without the field.
#[test]
fn a_payload_cursor_gives_the_live_positions_and_the_same_root() {
    let signer = PrivateKeySigner::random();
    let to1 = address!("00000000000000000000000000000000000A0001");
    let to2 = address!("00000000000000000000000000000000000A0002");
    let genesis = genesis_for(signer.address());

    let (_plain_dir, plain_env) = fresh_env();
    let plain = replay_blocks(
        plain_env,
        &test_genesis(&genesis),
        two_blocks(&signer, to1, to2),
    )
    .unwrap();
    assert_eq!(plain.head_end_tx_idx, None);

    let (_dir, env) = fresh_env();
    let blocks = two_blocks_after_epochs(&signer, to1, to2);
    let last_tx = blocks[1].txs[0].tx_hash;
    let outcome = replay_blocks(env.clone(), &test_genesis(&genesis), blocks).unwrap();

    assert_eq!(outcome.state_root, plain.state_root);
    assert_eq!(outcome.head_end_tx_idx, Some(7));
    let snap = StateSnapshot::open(&env).unwrap();
    assert_eq!(snap.end_tx_position().unwrap(), BPosition::from_index(7));
    let point = kardamom_state::read_recovery_point(&env).unwrap();
    assert_eq!(point.last_fsynced_reader_position, BPosition::from_index(7));
    // Block 2 is slots 3..7: the epoch marker and two deposits, then
    // its one transaction in the last slot.
    assert_eq!(
        snap.get_tx_position(last_tx).unwrap(),
        Some(BPosition::from_index(6))
    );
}

#[test]
fn a_cursor_with_no_room_for_the_blocks_items_is_refused() {
    let signer = PrivateKeySigner::random();
    let to = address!("00000000000000000000000000000000000A0001");
    let mut blocks = two_blocks(&signer, to, to);
    blocks[0].canonical_end = Some(CanonicalEnd {
        end_tx_idx: 1,
        l1_origin: 0,
    });
    let (_dir, env) = fresh_env();
    let err =
        replay_blocks(env, &test_genesis(&genesis_for(signer.address())), blocks).unwrap_err();
    assert!(
        matches!(
            err,
            ReplayError::CursorRegress {
                block_number: 1,
                ..
            }
        ),
        "{err}"
    );
}

/// An epoch whose deposits need more slots than the block's range
/// holds is refused: the chain does not hold those deposits there. Block
/// 1 ends at 3, which holds an epoch marker and two transactions, but
/// not a deposit too.
#[test]
fn an_epoch_with_more_deposits_than_the_range_holds_is_refused() {
    let signer = PrivateKeySigner::random();
    let to = address!("00000000000000000000000000000000000A0001");
    let mut blocks = two_blocks(&signer, to, to);
    blocks[0].canonical_end = Some(CanonicalEnd {
        end_tx_idx: 3,
        l1_origin: 40,
    });
    blocks[0].l1_epochs = vec![deposit_epoch(40, &[to], 1_000)];
    let (_dir, env) = fresh_env();
    let err =
        replay_blocks(env, &test_genesis(&genesis_for(signer.address())), blocks).unwrap_err();
    assert!(
        matches!(
            err,
            ReplayError::CursorRegress {
                block_number: 1,
                end_tx_idx: 3,
                slots: 4,
                previous_end: 0,
            }
        ),
        "{err}"
    );
}

#[test]
fn reconstructs_state_and_balances_from_ordered_blocks() {
    let signer = PrivateKeySigner::random();
    let from = signer.address();
    let to1 = address!("00000000000000000000000000000000000A0001");
    let to2 = address!("00000000000000000000000000000000000A0002");

    let (_dir, env) = fresh_env();
    let outcome = replay_blocks(
        env.clone(),
        &test_genesis(&genesis_for(from)),
        two_blocks(&signer, to1, to2),
    )
    .unwrap();

    assert_eq!(outcome.head_block, 2);
    assert_eq!(outcome.blocks_applied, 2);
    assert_eq!(outcome.txs_applied, 3);
    assert_ne!(outcome.state_root, empty_root());

    // Balances reflect the exact transfers. Gas price is 0, so there is no fee burn.
    let snap = StateSnapshot::open(&env).unwrap();
    assert_eq!(snap.block_number(), 2);
    assert_eq!(snap.basic(to1).unwrap().unwrap().1, U256::from(125u64)); // 100 + 25
    assert_eq!(snap.basic(to2).unwrap().unwrap().1, U256::from(50u64));
    let (from_nonce, from_balance, _) = snap.basic(from).unwrap().unwrap();
    assert_eq!(from_nonce, 3);
    assert_eq!(
        from_balance,
        U256::from(1_000_000_000_000_000_000u128 - 175u128)
    );
    // The snapshot's committed root matches the reported one.
    assert_eq!(snap.state_root().unwrap(), Some(outcome.state_root));
}

#[test]
fn reconstruction_is_deterministic() {
    let signer = PrivateKeySigner::random();
    let from = signer.address();
    let to1 = address!("00000000000000000000000000000000000B0001");
    let to2 = address!("00000000000000000000000000000000000B0002");

    let (_d1, env1) = fresh_env();
    let (_d2, env2) = fresh_env();
    let r1 = replay_blocks(
        env1,
        &test_genesis(&genesis_for(from)),
        two_blocks(&signer, to1, to2),
    )
    .unwrap();
    let r2 = replay_blocks(
        env2,
        &test_genesis(&genesis_for(from)),
        two_blocks(&signer, to1, to2),
    )
    .unwrap();

    // Same inputs give a byte-identical reconstructed root. This is the
    // property the DA round-trip depends on.
    assert_eq!(r1.state_root, r2.state_root);
    assert_eq!(r1.head_block, r2.head_block);
}

#[test]
fn empty_block_stream_yields_genesis_root() {
    let signer = PrivateKeySigner::random();
    let from = signer.address();
    let (_dir, env) = fresh_env();
    let outcome =
        replay_blocks(env.clone(), &test_genesis(&genesis_for(from)), Vec::new()).unwrap();
    assert_eq!(outcome.head_block, 0);
    assert_eq!(outcome.blocks_applied, 0);
    // With no blocks, the root is the seeded genesis root.
    let snap = StateSnapshot::open(&env).unwrap();
    assert_eq!(snap.state_root().unwrap(), Some(outcome.state_root));
}
