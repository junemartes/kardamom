use super::*;
use alloy_primitives::{Address, address, b256};

#[test]
fn alias_zero_address_is_offset() {
    let l1 = Address::ZERO;
    let expected = address!("1111000000000000000000000000000000001111");
    assert_eq!(alias_l1_address(l1), expected);
}

#[test]
fn alias_wraps_at_uint160() {
    // 2^160 - 1 + 0x1111...1111 (mod 2^160) = 0x1111...1110.
    let l1 = address!("ffffffffffffffffffffffffffffffffffffffff");
    let expected = address!("1111000000000000000000000000000000001110");
    assert_eq!(alias_l1_address(l1), expected);
}

#[test]
fn source_hash_is_deterministic() {
    let block = B256::repeat_byte(0x11);
    let h1 = source_hash(block, 0);
    let h2 = source_hash(block, 0);
    assert_eq!(h1, h2);
}

#[test]
fn source_hash_differs_for_different_log_indices() {
    let block = B256::repeat_byte(0xAB);
    let a = source_hash(block, 0);
    let b = source_hash(block, 1);
    assert_ne!(a, b);
}

fn deposit_log(block: B256, index: u64, to_byte: u8, mint: u128) -> DepositLog {
    DepositLog {
        block_number: 0,
        block_hash: block,
        log_index: index,
        from: Address::repeat_byte(0xA1),
        to: Address::repeat_byte(to_byte),
        mint,
        gas_limit: 200_000,
        data: AlloyBytes::new(),
    }
}

fn log(block: B256, index: u64, to_byte: u8, mint: u128) -> LockboxLog {
    LockboxLog::Deposit(deposit_log(block, index, to_byte, mint))
}

fn upgrade_log(block: B256, index: u64, feature: u64, activation: u64) -> LockboxLog {
    LockboxLog::Upgrade(UpgradeLog {
        block_number: 0,
        block_hash: block,
        log_index: index,
        feature_id: U256::from(feature),
        activation_timestamp: activation,
    })
}

#[test]
fn epoch_orders_by_log_index_regardless_of_input_order() {
    // The order is a property of L1, not of whoever read the logs. A
    // verifier that fetched them in a different order must derive the
    // same epoch, byte for byte.
    let block = B256::repeat_byte(0x77);
    let forward = derive_epoch(9, block, &[log(block, 0, 1, 10), log(block, 1, 2, 20)]).unwrap();
    let reversed = derive_epoch(9, block, &[log(block, 1, 2, 20), log(block, 0, 1, 10)]).unwrap();
    assert_eq!(forward, reversed);
    assert_eq!(forward.deposits.len(), 2);
    assert_eq!(forward.deposits[0].mint, 10);
    assert_eq!(forward.deposits[1].mint, 20);
}

#[test]
fn empty_epoch_is_valid_and_must_still_exist() {
    // The no-skipping rule only holds if depositless epochs are still
    // emitted. So deriving one is explicitly not an error.
    let block = B256::repeat_byte(0x33);
    let e = derive_epoch(12, block, &[]).unwrap();
    assert!(e.deposits.is_empty());
    assert_eq!(e.l1_number, 12);
    assert_eq!(e.l1_hash, block);
}

#[test]
fn foreign_log_is_rejected_not_silently_mixed_in() {
    let block = B256::repeat_byte(0x01);
    let other = B256::repeat_byte(0x02);
    let err = derive_epoch(1, block, &[log(other, 0, 1, 5)]).unwrap_err();
    assert!(matches!(err, EpochError::ForeignLog { .. }), "got {err:?}");
}

#[test]
fn duplicate_log_index_is_rejected() {
    // Two logs at the same index would produce two deposits with the
    // same source_hash. This dedup collision must never reach the
    // canonical stream.
    let block = B256::repeat_byte(0x04);
    let err = derive_epoch(1, block, &[log(block, 3, 1, 5), log(block, 3, 2, 6)]).unwrap_err();
    assert!(
        matches!(err, EpochError::DuplicateLogIndex { log_index: 3 }),
        "got {err:?}"
    );
}

#[test]
fn epoch_derivation_is_deterministic_across_callers() {
    // The producer-verifier equality this whole design rests on.
    let block = B256::repeat_byte(0x55);
    let logs = [log(block, 2, 9, 7), log(block, 0, 8, 3)];
    assert_eq!(
        derive_epoch(5, block, &logs).unwrap(),
        derive_epoch(5, block, &logs).unwrap()
    );
}

#[test]
fn canonical_id_is_stable_per_l1_block() {
    let a = derive_epoch(1, B256::repeat_byte(0xAA), &[]).unwrap();
    let b = derive_epoch(1, B256::repeat_byte(0xAA), &[]).unwrap();
    let c = derive_epoch(2, B256::repeat_byte(0xBB), &[]).unwrap();
    assert_eq!(a.canonical_id(), b.canonical_id());
    assert_ne!(a.canonical_id(), c.canonical_id());
}

#[test]
fn deposits_carry_aliased_sender_and_derived_source_hash() {
    let block = B256::repeat_byte(0x66);
    let l = deposit_log(block, 4, 7, 100);
    let d = &derive_epoch(3, block, &[l.clone().into()])
        .unwrap()
        .deposits[0];
    assert_eq!(d.source_hash, source_hash(block, 4));
    assert_eq!(d.from, alias_l1_address(l.from));
    assert_eq!(d.to, Some(l.to));
    assert_eq!(d.value, U256::from(100u64));
    assert!(!d.is_system_transaction);
}

// ---------------------------------------------------------------------
// Upgrade transactions (system deposits)
// ---------------------------------------------------------------------

#[test]
fn system_domain_separates_from_the_user_domain_at_the_same_position() {
    // This is the whole point of the domain byte. A deposit and an
    // upgrade at the same (block, log_index) get different ids. That
    // collision is impossible today, but the dedup key must not rely on
    // that.
    let block = B256::repeat_byte(0x91);
    assert_ne!(source_hash(block, 3), source_hash_system(block, 3));
}

#[test]
fn upgrade_derives_a_system_deposit_targeting_the_chain_state() {
    let block = B256::repeat_byte(0x21);
    let e = derive_epoch(4, block, &[upgrade_log(block, 2, 1, 0)]).unwrap();
    let d = &e.deposits[0];

    assert_eq!(d.source_hash, source_hash_system(block, 2));
    assert_eq!(d.from, crate::upgrades::SYSTEM_UPGRADER);
    assert_eq!(d.to, Some(crate::upgrades::CHAIN_STATE));
    assert!(d.is_system_transaction);
    // An upgrade moves no ETH.
    assert_eq!(d.mint, 0);
    assert_eq!(d.value, U256::ZERO);
    // Calldata is exactly setFeature(1, 0).
    assert_eq!(
        d.input.as_ref(),
        crate::upgrades::encode_set_feature(U256::from(1u64), 0).as_ref()
    );
}

#[test]
fn upgrade_sender_is_not_aliased() {
    // Aliasing exists to separate L1 senders from L2 addresses. The
    // system sender is defined on L2, and is what the predeploy
    // authorizes against. Aliasing it would break every upgrade.
    let block = B256::repeat_byte(0x22);
    let e = derive_epoch(1, block, &[upgrade_log(block, 0, 1, 0)]).unwrap();
    assert_eq!(e.deposits[0].from, crate::upgrades::SYSTEM_UPGRADER);
    assert_ne!(
        e.deposits[0].from,
        alias_l1_address(crate::upgrades::SYSTEM_UPGRADER)
    );
}

#[test]
fn upgrade_carries_its_activation_timestamp_into_the_calldata() {
    let block = B256::repeat_byte(0x23);
    let ts = 1_700_000_004_000u64;
    let e = derive_epoch(1, block, &[upgrade_log(block, 0, 9, ts)]).unwrap();
    assert_eq!(
        e.deposits[0].input.as_ref(),
        crate::upgrades::encode_set_feature(U256::from(9u64), ts).as_ref()
    );
}

#[test]
fn deposits_and_upgrades_interleave_in_l1_log_order() {
    // One contract, one log stream. An upgrade must not jump ahead of a
    // deposit that L1 ordered first, no matter the input order.
    let block = B256::repeat_byte(0x24);
    let forward = derive_epoch(
        6,
        block,
        &[
            log(block, 0, 1, 10),
            upgrade_log(block, 1, 1, 0),
            log(block, 2, 2, 20),
        ],
    )
    .unwrap();
    let shuffled = derive_epoch(
        6,
        block,
        &[
            log(block, 2, 2, 20),
            log(block, 0, 1, 10),
            upgrade_log(block, 1, 1, 0),
        ],
    )
    .unwrap();

    assert_eq!(forward, shuffled);
    assert_eq!(forward.deposits.len(), 3);
    assert!(!forward.deposits[0].is_system_transaction);
    assert!(forward.deposits[1].is_system_transaction);
    assert!(!forward.deposits[2].is_system_transaction);
}

#[test]
fn an_upgrade_colliding_with_a_deposit_log_index_is_rejected() {
    let block = B256::repeat_byte(0x25);
    let err = derive_epoch(
        1,
        block,
        &[log(block, 3, 1, 5), upgrade_log(block, 3, 1, 0)],
    )
    .unwrap_err();
    assert!(
        matches!(err, EpochError::DuplicateLogIndex { log_index: 3 }),
        "got {err:?}"
    );
}

#[test]
fn foreign_upgrade_log_is_rejected() {
    let block = B256::repeat_byte(0x26);
    let other = B256::repeat_byte(0x27);
    let err = derive_epoch(1, block, &[upgrade_log(other, 0, 1, 0)]).unwrap_err();
    assert!(matches!(err, EpochError::ForeignLog { .. }), "got {err:?}");
}

#[test]
fn upgrade_derivation_is_deterministic_across_callers() {
    // Same producer-verifier equality the user-deposit path relies on.
    // The validator re-derives upgrades from L1 and fail-stops on any diff.
    let block = B256::repeat_byte(0x28);
    let logs = [upgrade_log(block, 1, 3, 999), log(block, 0, 4, 1)];
    assert_eq!(
        derive_epoch(2, block, &logs).unwrap(),
        derive_epoch(2, block, &logs).unwrap()
    );
}

#[test]
fn system_source_hash_known_vector() {
    // This is anchored like the user-deposit vector above. The id a
    // system deposit dedups on is consensus-visible, so a change must be
    // deliberate.
    //
    // This is cross-checked against a hand-built RLP encoding, not
    // captured from this implementation, so the vector is independent
    // evidence:
    //   inner = e2 a0 <32-byte block hash> 2a          (list, payload 34)
    //   outer = e2 01 a0 keccak(inner)                 (domain 1 is a bare
    //                                                   0x01 byte, not 0x81 0x01)
    // The same procedure with domain 0 (encoded 0x80) reproduces the
    // user-domain vector below. This confirms the method.
    let h = source_hash_system(
        b256!("0000000000000000000000000000000000000000000000000000000000000001"),
        42,
    );
    assert_eq!(
        h,
        b256!("60a7cd0721ff0987010cb857c9439ed4078bec21893625b554f27c03787047cc")
    );
}

#[test]
fn source_hash_known_vector_matches_op_form() {
    // This is the anchored output for a fixed (l1_block_hash, log_index)
    // pair. A change to the algorithm flips this value and forces a
    // code-review conversation. This vector comes from the OP-aligned
    // algorithm in `crates/node/src/deposit.rs`, whose conformance is
    // pinned by the contracts' bytecode-hash CI gate.
    let h = source_hash(
        b256!("0000000000000000000000000000000000000000000000000000000000000001"),
        42,
    );
    assert_eq!(
        h,
        b256!("fce50386841795079cfbaa39a7061f9f746945afa35650f060fa5935c4462c61")
    );
}
