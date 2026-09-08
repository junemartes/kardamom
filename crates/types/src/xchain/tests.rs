use super::*;
use alloy_primitives::{Address, B256, Bytes as AlloyBytes, address, b256, keccak256};

/// One message in origin block 100. Every batch in these tests sits in
/// ONE origin block, because a record is one block (see
/// [`XChainError::MultiBlockBatch`]).
fn msg(seq: u64, dest: u64) -> OutboxMessage {
    msg_in_block(seq, dest, 100)
}

fn msg_in_block(seq: u64, dest: u64, block: u64) -> OutboxMessage {
    OutboxMessage {
        origin_block_number: block,
        origin_block_hash: Anchor {
            origin_chain_id: ORIGIN,
            block_number: block,
        }
        .hash(),
        dest_chain_id: dest,
        seq,
        sender: Address::repeat_byte(0xA1),
        target: Address::repeat_byte(0xB2),
        value: 0,
        gas_limit: 200_000,
        data: AlloyBytes::from(alloc::vec![0xCA, 0xFE]),
        callback: None,
    }
}

const SELF: u64 = 412_347;
const ORIGIN: u64 = 412_346;

#[test]
fn source_hash_is_position_based_and_deterministic() {
    assert_eq!(remote_source_hash(ORIGIN, 7), remote_source_hash(ORIGIN, 7));
    assert_ne!(remote_source_hash(ORIGIN, 7), remote_source_hash(ORIGIN, 8));
    assert_ne!(
        remote_source_hash(ORIGIN, 7),
        remote_source_hash(ORIGIN + 1, 7)
    );
}

#[test]
fn source_hash_domain_separates_from_deposits() {
    // A deposit's source_hash over structurally-similar inputs must never
    // collide with a remote message's — the domain word differs.
    let dep = crate::epoch::source_hash(B256::ZERO, 7);
    // remote_source_hash rlp-encodes (chain_id, seq), not (hash, index),
    // so equality could only happen by keccak collision; assert anyway as
    // the cheap canary.
    assert_ne!(dep, remote_source_hash(0, 7));
}

#[test]
fn alias_is_per_origin_and_deterministic() {
    let s = Address::repeat_byte(0x11);
    assert_eq!(alias_remote_address(1, s), alias_remote_address(1, s));
    assert_ne!(alias_remote_address(1, s), alias_remote_address(2, s));
    assert_ne!(
        alias_remote_address(1, s),
        alias_remote_address(1, Address::repeat_byte(0x12))
    );
    // And never the identity — a remote sender must not impersonate the
    // local account at the same address.
    assert_ne!(alias_remote_address(1, s), s);
}

#[test]
fn derivation_orders_by_seq_regardless_of_input_order() {
    let fwd = derive_remote_epoch(SELF, ORIGIN, 5, &[msg(5, SELF), msg(6, SELF)]).unwrap();
    let rev = derive_remote_epoch(SELF, ORIGIN, 5, &[msg(6, SELF), msg(5, SELF)]).unwrap();
    assert_eq!(fwd, rev);
    assert_eq!(fwd.first_seq, 5);
    assert_eq!(fwd.last_seq(), 6);
    assert_eq!(
        fwd.messages.first().source_hash,
        remote_source_hash(ORIGIN, 5)
    );
}

#[test]
fn empty_batch_is_rejected_unlike_l1_epochs() {
    assert_eq!(
        derive_remote_epoch(SELF, ORIGIN, 0, &[]).unwrap_err(),
        XChainError::Empty
    );
}

#[test]
fn gap_and_regression_and_duplicate_are_faults() {
    let e = derive_remote_epoch(SELF, ORIGIN, 5, &[msg(6, SELF)]).unwrap_err();
    assert!(
        matches!(
            e,
            XChainError::SeqSkipped {
                expected: 5,
                found: 6
            }
        ),
        "got {e:?}"
    );

    let e = derive_remote_epoch(SELF, ORIGIN, 5, &[msg(4, SELF)]).unwrap_err();
    assert!(
        matches!(
            e,
            XChainError::SeqRegressed {
                expected: 5,
                found: 4
            }
        ),
        "got {e:?}"
    );

    let e = derive_remote_epoch(SELF, ORIGIN, 5, &[msg(5, SELF), msg(7, SELF)]).unwrap_err();
    assert!(
        matches!(
            e,
            XChainError::SeqSkipped {
                expected: 6,
                found: 7
            }
        ),
        "got {e:?}"
    );

    let e = derive_remote_epoch(SELF, ORIGIN, 5, &[msg(5, SELF), msg(5, SELF)]).unwrap_err();
    assert!(
        matches!(e, XChainError::DuplicateSeq { seq: 5 }),
        "got {e:?}"
    );
}

#[test]
fn foreign_destination_is_rejected_not_dropped() {
    let e = derive_remote_epoch(SELF, ORIGIN, 0, &[msg(0, SELF), msg(1, SELF + 9)]).unwrap_err();
    assert!(
        matches!(e, XChainError::ForeignDestination { .. }),
        "got {e:?}"
    );
}

#[test]
fn derivation_is_deterministic_across_callers() {
    let batch = [msg(2, SELF), msg(0, SELF), msg(1, SELF)];
    assert_eq!(
        derive_remote_epoch(SELF, ORIGIN, 0, &batch).unwrap(),
        derive_remote_epoch(SELF, ORIGIN, 0, &batch).unwrap()
    );
}

#[test]
fn canonical_id_keys_on_pair_position() {
    let a = derive_remote_epoch(SELF, ORIGIN, 0, &[msg(0, SELF)]).unwrap();
    let b = derive_remote_epoch(SELF, ORIGIN, 0, &[msg(0, SELF)]).unwrap();
    let c = derive_remote_epoch(SELF, ORIGIN, 1, &[msg(1, SELF)]).unwrap();
    assert_eq!(a.canonical_id(), b.canonical_id());
    assert_ne!(a.canonical_id(), c.canonical_id());
    // The same seq range at a different anchor is a different record.
    // The sealer trusts the header's anchor only because the id binds it.
    let mut d = a.clone();
    d.anchor_number += 1;
    assert_ne!(a.canonical_id(), d.canonical_id());
}

/// The id preimage is
/// `origin_be8 ‖ anchor_number_be8 ‖ anchor_hash ‖ first_seq_be8 ‖ last_seq_be8`.
/// A change here changes every record id on the canonical stream.
#[test]
fn canonical_id_known_vector_is_pinned() {
    let rec = RemoteEpochRecord {
        origin_chain_id: 412_346,
        anchor_number: 0x0011_2233_4455_6677,
        anchor_hash: B256::repeat_byte(0x5A),
        first_seq: 9,
        messages: NonEmptyVec::new(
            XChainMessage {
                seq: 9,
                ..Default::default()
            },
            alloc::vec![XChainMessage {
                seq: 10,
                ..Default::default()
            }],
        ),
    };
    let mut preimage = alloc::vec::Vec::new();
    preimage.extend_from_slice(&412_346u64.to_be_bytes());
    preimage.extend_from_slice(&0x0011_2233_4455_6677u64.to_be_bytes());
    preimage.extend_from_slice(&[0x5A; 32]);
    preimage.extend_from_slice(&9u64.to_be_bytes());
    preimage.extend_from_slice(&10u64.to_be_bytes());
    assert_eq!(rec.canonical_id(), keccak256(&preimage));
    assert_eq!(
        rec.canonical_id(),
        b256!("0xd75d04f838d45e94580c13899702239d95e68dc3380e932f23f73113c0a7895a"),
        "pinned vector: regenerate on purpose only"
    );
}

#[test]
fn a_batch_that_spans_two_origin_blocks_is_rejected() {
    let e = derive_remote_epoch(
        SELF,
        ORIGIN,
        0,
        &[msg_in_block(0, SELF, 100), msg_in_block(1, SELF, 101)],
    )
    .unwrap_err();
    assert_eq!(
        e,
        XChainError::MultiBlockBatch {
            first_block: 100,
            found_block: 101
        }
    );
    // Two messages in one block derive.
    let ok = derive_remote_epoch(
        SELF,
        ORIGIN,
        0,
        &[msg_in_block(0, SELF, 100), msg_in_block(1, SELF, 100)],
    )
    .unwrap();
    assert_eq!(ok.anchor_number, 100);
}

#[test]
fn a_multi_block_batch_with_an_over_cap_message_faults_on_bounds_first() {
    // The over-cap message sits in the block-100 half, so the bounds
    // check (run on the borrowed messages, before `check_one_block`)
    // rejects the batch before the multi-block check ever runs.
    let mut over_cap = msg_in_block(0, SELF, 100);
    over_cap.gas_limit = MAX_MESSAGE_GAS + 1;
    let e =
        derive_remote_epoch(SELF, ORIGIN, 0, &[over_cap, msg_in_block(1, SELF, 101)]).unwrap_err();
    assert!(
        matches!(
            e,
            XChainError::Bounds(BoundsFault::GasLimitAboveCap { seq: 0, .. })
        ),
        "got {e:?}"
    );
}

#[test]
fn anchor_check_rejects_a_feed_chosen_hash() {
    let good = msg(3, SELF);
    assert_eq!(good.check_anchor(ORIGIN), Ok(()));
    let mut bad = msg(3, SELF);
    bad.origin_block_hash = B256::repeat_byte(0xEE);
    let e = bad.check_anchor(ORIGIN).unwrap_err();
    assert!(
        matches!(
            e,
            XChainError::AnchorMismatch {
                seq: 3,
                block: 100,
                ..
            }
        ),
        "got {e:?}"
    );
    // The anchor from another origin is also wrong.
    let mut other = msg(3, SELF);
    other.origin_block_hash = (Anchor {
        origin_chain_id: ORIGIN + 1,
        block_number: 100,
    })
    .hash();
    assert!(other.check_anchor(ORIGIN).is_err());
}

/// `keccak256("KARDAMOM_XCHAIN_ANCHOR_V0" ‖ origin_be8 ‖ block_be8)`.
#[test]
fn anchor_hash_known_vector_is_pinned() {
    let mut preimage = alloc::vec::Vec::new();
    preimage.extend_from_slice(b"KARDAMOM_XCHAIN_ANCHOR_V0");
    preimage.extend_from_slice(&412_346u64.to_be_bytes());
    preimage.extend_from_slice(&42u64.to_be_bytes());
    let anchor = Anchor {
        origin_chain_id: 412_346,
        block_number: 42,
    };
    assert_eq!(anchor.hash(), keccak256(&preimage));
    assert_ne!(
        anchor.hash(),
        Anchor {
            origin_chain_id: 412_346,
            block_number: 43,
        }
        .hash()
    );
    assert_ne!(
        anchor.hash(),
        Anchor {
            origin_chain_id: 412_347,
            block_number: 42,
        }
        .hash()
    );
}

#[test]
fn leaf_recomputable_from_wire_record() {
    // The property that justifies carrying the un-aliased sender: a
    // verifier holding only the RemoteEpochRecord and the pair identity
    // can recompute every Outbox commitment.
    let rec = derive_remote_epoch(SELF, ORIGIN, 0, &[msg(0, SELF)]).unwrap();
    let m = rec.messages.first();
    let expect = MsgLeaf {
        origin_chain_id: ORIGIN,
        dest_chain_id: SELF,
        seq: 0,
        sender: Address::repeat_byte(0xA1),
        target: Address::repeat_byte(0xB2),
        value: 0,
        gas_limit: 200_000,
        data_hash: keccak256([0xCA, 0xFE]),
        cb_hash: no_callback_hash(),
    }
    .hash();
    assert_eq!(m.leaf(ORIGIN, SELF), expect);
}

#[test]
fn callback_commitment_binds_all_fields() {
    let base = Callback {
        target: Address::repeat_byte(0x01),
        gas_limit: 100_000,
        context: B256::repeat_byte(0x02),
    };
    let mut c2 = base;
    c2.gas_limit += 1;
    assert_ne!(base.commitment(), c2.commitment());
    assert_ne!(base.commitment(), no_callback_hash());
}

#[test]
fn leaf_known_vector_is_pinned() {
    // Anchored output for fixed inputs — must match Outbox.hashMessage in
    // contracts/test/Outbox.t.sol (the cross-language tie). Changing the
    // leaf layout flips this and forces a review conversation.
    let leaf = MsgLeaf {
        origin_chain_id: 1,
        dest_chain_id: 2,
        seq: 3,
        sender: Address::repeat_byte(0x04),
        target: Address::repeat_byte(0x05),
        value: 6,
        gas_limit: 7,
        data_hash: keccak256([0x08]),
        cb_hash: B256::ZERO,
    }
    .hash();
    // Same inputs, same value, asserted in contracts/test/Outbox.t.sol —
    // the cross-language tie.
    assert_eq!(
        leaf,
        b256!("0df14340efd8c8b32f4c333c3dca8470b0bae319a3dfe32adb213df2b8834d3c")
    );
}

#[test]
fn seq_overflow_is_a_fault_not_a_panic() {
    // Two messages at the top of the u64 range: the gap check would
    // compute `u64::MAX + 1`.
    let e = derive_remote_epoch(
        SELF,
        ORIGIN,
        u64::MAX - 1,
        &[msg(u64::MAX - 1, SELF), msg(u64::MAX, SELF)],
    )
    .unwrap_err();
    assert!(matches!(e, XChainError::SeqOverflow { .. }), "got {e:?}");
    // One message at u64::MAX: the next cursor would overflow.
    let e = derive_remote_epoch(SELF, ORIGIN, u64::MAX, &[msg(u64::MAX, SELF)]).unwrap_err();
    assert!(
        matches!(e, XChainError::SeqOverflow { seq: u64::MAX }),
        "got {e:?}"
    );
    // `first_seq` near `u64::MAX` plus a multi-message batch does not
    // panic `last_seq`; it saturates instead.
    let r = RemoteEpochRecord {
        origin_chain_id: 0,
        anchor_number: 0,
        anchor_hash: B256::ZERO,
        first_seq: u64::MAX,
        messages: NonEmptyVec::new(
            XChainMessage::default(),
            alloc::vec![XChainMessage::default()],
        ),
    };
    let _ = r.last_seq();
}

#[test]
fn outbox_bounds_are_mirrored_on_the_producer() {
    // Value.
    let mut m = msg(0, SELF);
    m.value = 1;
    let e = derive_remote_epoch(SELF, ORIGIN, 0, &[m]).unwrap_err();
    assert!(
        matches!(
            e,
            XChainError::Bounds(BoundsFault::ValueNotAllowed { seq: 0, value: 1 })
        ),
        "got {e:?}"
    );
    // Gas limit.
    let mut m = msg(0, SELF);
    m.gas_limit = MAX_MESSAGE_GAS + 1;
    let e = derive_remote_epoch(SELF, ORIGIN, 0, &[m]).unwrap_err();
    assert!(
        matches!(
            e,
            XChainError::Bounds(BoundsFault::GasLimitAboveCap {
                seq: 0,
                gas_limit: 10_000_001,
                cap: MAX_MESSAGE_GAS
            })
        ),
        "got {e:?}"
    );
    // Data length.
    let mut m = msg(0, SELF);
    m.data = AlloyBytes::from(alloc::vec![0xFFu8; MAX_DATA_BYTES + 1]);
    let e = derive_remote_epoch(SELF, ORIGIN, 0, &[m]).unwrap_err();
    assert!(
        matches!(
            e,
            XChainError::Bounds(BoundsFault::DataAboveCap {
                seq: 0,
                len: 65_537,
                cap: MAX_DATA_BYTES
            })
        ),
        "got {e:?}"
    );
}

#[test]
fn honest_maxima_pass_the_producer_checks() {
    let mut m = msg(0, SELF);
    m.value = 0;
    m.gas_limit = MAX_MESSAGE_GAS;
    m.data = AlloyBytes::from(alloc::vec![0xFFu8; MAX_DATA_BYTES]);
    let r = derive_remote_epoch(SELF, ORIGIN, 0, &[m]).unwrap();
    assert_eq!(r.messages.first().gas_limit, MAX_MESSAGE_GAS);
    assert_eq!(r.messages.first().input.len(), MAX_DATA_BYTES);
}

// Value pins. The Solidity side asserts the same values in
// `contracts/test/L2/Outbox.t.sol`. A change to any of these is a
// chain-splitting change.

#[test]
fn callback_commitment_known_vector_is_pinned() {
    // The `test_hashCallback_matchesRust` inputs of Outbox.t.sol.
    let cb = Callback {
        target: Address::repeat_byte(0x01),
        gas_limit: 100_000,
        context: B256::repeat_byte(0x02),
    };
    assert_eq!(
        cb.commitment(),
        b256!("3cc4851e518423fb0983f20dc6198ffd6ef901107d7b7911ffc4e8f942442b05")
    );
}

#[test]
fn alias_known_vectors_are_pinned() {
    // The `test_aliasVectors_matchRust` values of Outbox.t.sol.
    assert_eq!(
        alias_remote_address(ORIGIN, Address::repeat_byte(0xaa)),
        address!("aa1fbdc71f2e2531f6704edff74f45bff135db61")
    );
    assert_eq!(
        xchain_tx_sender(ORIGIN),
        address!("32122ab04da66c349463091cfda2773e379f678b")
    );
}

#[test]
fn remote_source_hash_known_vector_is_pinned() {
    // rlp([412346, 7]) = 0xc583064aba07; inner = cast keccak of that;
    // rlp([2, inner]) = 0xe202a0‖inner; the value is cast keccak of that.
    assert_eq!(
        remote_source_hash(ORIGIN, 7),
        b256!("4f9bc7dd342a5ae3eae82c238c74109eb4322fd0c8898b7e21487d7e0750e1e4")
    );
}

/// A hostile archive with its `messages` count forced to zero must not
/// decode. `messages` is `RemoteEpochRecord`'s last field, so its
/// archived length word is the archive's trailing 4 bytes (checked
/// against a second, differently-sized record so this is not an assumed
/// offset).
#[test]
fn an_archive_with_zero_messages_fails_to_decode() {
    let one = derive_remote_epoch(SELF, ORIGIN, 0, &[msg(0, SELF)]).unwrap();
    let two = derive_remote_epoch(SELF, ORIGIN, 0, &[msg(0, SELF), msg(1, SELF)]).unwrap();
    let one_bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&one).unwrap();
    let two_bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&two).unwrap();
    // The last 4 bytes are the only ones that must encode a length: they
    // read as 1 and 2 respectively, and every other trailing byte the
    // pointer word covers is free to differ between the two records too.
    let one_len_word = &one_bytes[one_bytes.len() - 4..];
    let two_len_word = &two_bytes[two_bytes.len() - 4..];
    assert_eq!(one_len_word, 1u32.to_le_bytes());
    assert_eq!(two_len_word, 2u32.to_le_bytes());

    let mut corrupted = one_bytes.to_vec();
    let end = corrupted.len();
    corrupted[end - 4..].copy_from_slice(&0u32.to_le_bytes());
    assert!(
        rkyv::from_bytes::<RemoteEpochRecord, rkyv::rancor::Error>(&corrupted).is_err(),
        "a zero-length messages archive must not decode"
    );
}

/// The other half of `RemoteEpochRecord`'s `Verify` impl: an archive whose
/// `first_seq + messages.len()` overflows `u64` must not decode, and one
/// right at the boundary (where it still fits) must. This is the same
/// bound `derive_remote_epoch`'s `check_range_fits` and the validator's
/// `SeqRange::new` both enforce — pinning it here proves the wire decoder
/// agrees with what the producer can build and the validator can accept.
#[test]
fn an_archive_whose_seq_range_overflows_u64_fails_to_decode() {
    let one_msg = |seq: u64| XChainMessage {
        seq,
        ..Default::default()
    };
    let overflowing = RemoteEpochRecord {
        origin_chain_id: ORIGIN,
        anchor_number: 0,
        anchor_hash: B256::ZERO,
        first_seq: u64::MAX,
        messages: NonEmptyVec::new(one_msg(u64::MAX), alloc::vec![]),
    };
    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&overflowing).unwrap();
    assert!(
        rkyv::from_bytes::<RemoteEpochRecord, rkyv::rancor::Error>(&bytes).is_err(),
        "first_seq + len (u64::MAX + 1) overflows u64; must not decode"
    );

    // The positive control, one below the boundary: `first_seq + len`
    // (`u64::MAX - 1 + 1 == u64::MAX`) fits, so this one decodes.
    let at_the_boundary = RemoteEpochRecord {
        first_seq: u64::MAX - 1,
        messages: NonEmptyVec::new(one_msg(u64::MAX - 1), alloc::vec![]),
        ..overflowing
    };
    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&at_the_boundary).unwrap();
    assert!(
        rkyv::from_bytes::<RemoteEpochRecord, rkyv::rancor::Error>(&bytes).is_ok(),
        "first_seq + len (u64::MAX) fits u64; must decode"
    );
}
