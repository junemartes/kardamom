//! Wire roundtrip tests. This is deliberately one module that spans both
//! directions. The roundtrips encode on one side and decode on the other
//! (ingress, then service relay, then egress). That is exactly the
//! property that must hold.

use alloy_primitives::{Address, B256};
use kardamom_types::{BPosition, DepositRef, TxOrderingMessage, TxRef};

use super::*;

fn txref() -> TxRef {
    TxRef::new(
        B256::repeat_byte(0xAB),
        3,
        BPosition {
            term_id: 7,
            term_offset: 9_999,
        },
        5, // tx_data_session_id (exercises the active/active session field roundtrip)
    )
}
fn depositref() -> DepositRef {
    DepositRef::new(
        B256::repeat_byte(0xCD),
        BPosition {
            term_id: 1,
            term_offset: 42,
        },
    )
}

/// Ingress, then the service relays from the canonical ID, then egress,
/// then decode: this reproduces the TxRef. The guard header is consumed,
/// not relayed.
#[test]
fn txref_ingress_relay_egress_roundtrip() {
    let r = txref();
    let sender = Address::repeat_byte(0x42);
    let ingress = encode_ingress_txref(&r, sender, 7);
    assert_eq!(ingress_sender_nonce(&ingress).unwrap(), (sender, 7));
    // Mirror the Java service: parse the id, relay from the canonical id.
    let (cid, relayed) = split_ingress(&ingress).unwrap();
    assert_eq!(cid, r.tx_hash.0);
    let egress = encode_egress_record(5, relayed);
    match decode_egress(&egress).unwrap() {
        EgressItem::Record { index, msg } => {
            assert_eq!(index, 5);
            assert_eq!(msg, TxOrderingMessage::TxRef(r));
        }
        other => panic!("expected Record, got {other:?}"),
    }
}

#[test]
fn depositref_ingress_relay_egress_roundtrip() {
    let r = depositref();
    let ingress = encode_ingress_depositref(&r);
    // Deposits use a zero sender, which the guard check exempts.
    assert_eq!(ingress_sender_nonce(&ingress).unwrap(), (Address::ZERO, 0));
    let (cid, relayed) = split_ingress(&ingress).unwrap();
    assert_eq!(cid, r.source_hash.0);
    let egress = encode_egress_record(8, relayed);
    match decode_egress(&egress).unwrap() {
        EgressItem::Record { index, msg } => {
            assert_eq!(index, 8);
            assert_eq!(msg, TxOrderingMessage::DepositRef(r));
        }
        other => panic!("expected Record, got {other:?}"),
    }
}

#[test]
fn boundary_roundtrip() {
    let egress = encode_egress_boundary(12, 100, 1_700_000_000_250, 0);
    match decode_egress(&egress).unwrap() {
        EgressItem::Boundary(b) => {
            assert_eq!(b.block_number, 12);
            assert_eq!(b.end_tx_idx.as_index(), 100);
            assert_eq!(b.l2_timestamp, 1_700_000_000_250);
        }
        other => panic!("expected Boundary, got {other:?}"),
    }
}

#[test]
fn ingress_layout_is_kind_sender_nonce_id_then_fields() {
    let r = txref();
    let sender = Address::repeat_byte(0x42);
    let b = encode_ingress_txref(&r, sender, 0x0102_0304_0506_0708);
    assert_eq!(b[0], KIND_INGRESS_RECORD);
    assert_eq!(&b[1..21], sender.as_slice());
    assert_eq!(
        b[21..29],
        0x0102_0304_0506_0708u64.to_le_bytes(),
        "nonce is little-endian at offset 21 (Java NONCE_OFFSET)"
    );
    assert_eq!(&b[29..61], r.tx_hash.as_slice());
    assert_eq!(b[61], RT_TXREF);
    assert_eq!(b[62], 3); // shard_id
    // The relayed payload begins with the canonical ID. The guard header
    // never reaches the executors.
    let (_cid, relayed) = split_ingress(&b).unwrap();
    assert_eq!(&relayed[0..32], r.tx_hash.as_slice());
}

#[test]
fn contiguity_reject_roundtrip() {
    let sender = Address::repeat_byte(0x99);
    let b = encode_contiguity_reject(sender, 12, 8);
    match decode_egress(&b).unwrap() {
        EgressItem::ContiguityReject {
            sender: s,
            nonce,
            expected,
        } => {
            assert_eq!(s, sender);
            assert_eq!(nonce, 12);
            assert_eq!(expected, 8);
        }
        other => panic!("expected ContiguityReject, got {other:?}"),
    }
}

#[test]
fn replay_request_roundtrip() {
    let b = encode_replay_request(1234, 56);
    assert_eq!(b[0], KIND_REPLAY_REQUEST);
    assert_eq!(decode_replay_request(&b).unwrap(), (1234, 56));
    // A record ingress message is not a replay request.
    assert!(decode_replay_request(&encode_ingress_txref(&txref(), Address::ZERO, 0)).is_err());
}

#[test]
fn replay_unavailable_roundtrip() {
    let b = encode_replay_unavailable(100, 7);
    match decode_egress(&b).unwrap() {
        EgressItem::ReplayUnavailable {
            oldest_index,
            oldest_block,
        } => {
            assert_eq!(oldest_index, 100);
            assert_eq!(oldest_block, 7);
        }
        other => panic!("expected ReplayUnavailable, got {other:?}"),
    }
}

#[test]
fn bad_kind_and_record_type_error() {
    assert_eq!(decode_egress(&[9, 0, 0]), Err(WireError::BadEgressKind(9)));
    // A relayed payload with an unknown record type.
    let mut payload = vec![0u8; 32];
    payload.push(7); // record_type 7
    let egress = encode_egress_record(0, &payload);
    assert_eq!(decode_egress(&egress), Err(WireError::BadRecordType(7)));
}

#[test]
fn truncated_egress_errors_cleanly() {
    assert!(matches!(
        decode_egress(&[EGRESS_KIND_RELAYED, 0, 0]),
        Err(WireError::TooShort { .. })
    ));
    assert!(decode_egress(&[]).is_err());
}
