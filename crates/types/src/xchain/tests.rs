use super::*;
use alloy_primitives::{Address, B256, Bytes as AlloyBytes, b256, keccak256};

fn msg(seq: u64, dest: u64) -> OutboxMessage {
    OutboxMessage {
        origin_block_number: 100 + seq,
        origin_block_hash: B256::repeat_byte(0x0B),
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
    assert_eq!(fwd.messages[0].source_hash, remote_source_hash(ORIGIN, 5));
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
}

#[test]
fn leaf_recomputable_from_wire_record() {
    // The property that justifies carrying the un-aliased sender: a
    // verifier holding only the RemoteEpochRecord and the pair identity
    // can recompute every Outbox commitment.
    let rec = derive_remote_epoch(SELF, ORIGIN, 0, &[msg(0, SELF)]).unwrap();
    let m = &rec.messages[0];
    let expect = msg_leaf(
        ORIGIN,
        SELF,
        0,
        Address::repeat_byte(0xA1),
        Address::repeat_byte(0xB2),
        0,
        200_000,
        keccak256([0xCA, 0xFE]),
        no_callback_hash(),
    );
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
    let leaf = msg_leaf(
        1,
        2,
        3,
        Address::repeat_byte(0x04),
        Address::repeat_byte(0x05),
        6,
        7,
        keccak256([0x08]),
        B256::ZERO,
    );
    // Same inputs, same value, asserted in contracts/test/Outbox.t.sol —
    // the cross-language tie.
    assert_eq!(
        leaf,
        b256!("0df14340efd8c8b32f4c333c3dca8470b0bae319a3dfe32adb213df2b8834d3c")
    );
}

#[test]
fn last_seq_does_not_underflow_on_an_empty_default_record() {
    // Defect: `first_seq + messages.len() - 1` underflowed when `messages`
    // was empty. `Default` makes the empty case constructible even though
    // the type docs call it invalid, so the method must not panic or wrap.
    let empty = RemoteEpochRecord::default();
    assert_eq!(empty.last_seq(), 0);
}
