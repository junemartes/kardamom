//! Wire roundtrip tests. This is deliberately one module that spans both
//! directions. The roundtrips encode on one side and decode on the other
//! (ingress, then service relay, then egress). That is exactly the
//! property that must hold.

use alloy_primitives::{Address, B256, U256};
use kardamom_types::epoch::EpochRecord;
use kardamom_types::xchain::{Callback, RemoteEpochRecord};
use kardamom_types::{BPosition, Deposit, DepositRef, TxOrderingMessage, TxRef};

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

fn remote_epoch() -> kardamom_types::xchain::RemoteEpochRecord {
    use kardamom_types::xchain::{RemoteEpochRecord, XChainMessage};
    RemoteEpochRecord {
        origin_chain_id: 412_346,
        anchor_number: 0x0011_2233_4455_6677,
        anchor_hash: B256::repeat_byte(0x5A),
        first_seq: 9,
        messages: vec![
            XChainMessage {
                source_hash: B256::repeat_byte(0xE1),
                seq: 9,
                origin_sender: Address::repeat_byte(0xA1),
                target: Address::repeat_byte(0xB2),
                value: 0,
                gas_limit: 200_000,
                input: (&[0xCAu8, 0xFE][..]).into(),
                callback: None,
            },
            XChainMessage {
                source_hash: B256::repeat_byte(0xE2),
                seq: 10,
                origin_sender: Address::repeat_byte(0xA1),
                target: Address::repeat_byte(0xB3),
                value: 5,
                gas_limit: 100_000,
                input: Default::default(),
                callback: None,
            },
        ],
    }
}

/// The Java decoder reads this frame by fixed offsets and never parses the
/// payload, so the offsets ARE the contract — a field that moves is a silent
/// mis-parse on the other side of a language boundary no compiler checks.
#[test]
fn remote_origin_record_layout_is_pinned_byte_for_byte() {
    let rec = remote_epoch();
    let b = encode_ingress_remote_epoch(&rec).unwrap();

    assert_eq!(b[0], KIND_REMOTE_ORIGIN_RECORD);
    assert_eq!(b[0], 5, "kind 5 is the Java KIND_REMOTE_ORIGIN_RECORD");
    assert_eq!(&b[1..33], rec.canonical_id().as_slice());
    assert_eq!(
        u64::from_le_bytes(b[33..41].try_into().unwrap()),
        412_346,
        "origin_chain_id is little-endian at offset 33"
    );
    assert_eq!(
        u64::from_le_bytes(b[41..49].try_into().unwrap()),
        0x0011_2233_4455_6677,
        "anchor_number is little-endian at offset 41 — the pair's position, \
         not a global one"
    );
    assert_eq!(
        u32::from_le_bytes(b[49..53].try_into().unwrap()),
        3,
        "slot_count = marker + 2 messages"
    );
    assert_eq!(b[53], RT_REMOTE_EPOCH);
    assert_eq!(b[53], 3);
    // Everything from offset 54 on is the opaque rkyv payload.
    assert!(b.len() > 54, "the record body must be present");

    // The kind byte alone separates the two origin-advancing frames; the
    // sealer branches on it without opening either payload.
    let l1 = encode_ingress_epoch(&EpochRecord {
        l1_number: 1,
        l1_hash: B256::ZERO,
        deposits: Vec::new(),
    })
    .unwrap();
    assert_ne!(b[0], l1[0]);
}

/// Ingress → (service relays from the canonical id) → egress → decode
/// reproduces the record. `slot_count` on the frame must equal what a
/// consumer that DOES parse the payload re-derives.
#[test]
fn remote_epoch_ingress_relay_egress_roundtrip() {
    let rec = remote_epoch();
    let ingress = encode_ingress_remote_epoch(&rec).unwrap();

    // Mirror the Java service: dedup on the canonical id, relay
    // `[canonical_id][record_type][fields…]` — the kind, the origin pair and
    // the slot count are consumed by the sealer, not forwarded.
    let cid: [u8; 32] = ingress[1..33].try_into().unwrap();
    assert_eq!(cid, rec.canonical_id().0);
    let mut relayed = Vec::with_capacity(32 + ingress.len() - 53);
    relayed.extend_from_slice(&cid);
    relayed.extend_from_slice(&ingress[53..]);

    match decode_egress(&encode_egress_record(11, &relayed)).unwrap() {
        EgressItem::Record { index, msg } => {
            assert_eq!(index, 11);
            assert_eq!(msg, TxOrderingMessage::RemoteEpoch(rec.clone()));
        }
        other => panic!("expected Record, got {other:?}"),
    }
    assert_eq!(
        u32::from_le_bytes(ingress[49..53].try_into().unwrap()) as u64,
        remote_epoch_slots(&rec),
    );
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

/// A remote epoch whose first message carries a callback: the `Some` arm of
/// the archived `Option<Callback>`, which no other wire test covers.
fn remote_epoch_with_callback() -> RemoteEpochRecord {
    let mut rec = remote_epoch();
    rec.messages[0].callback = Some(Callback {
        target: Address::repeat_byte(0xC1),
        gas_limit: 90_000,
        context: B256::repeat_byte(0xC2),
    });
    rec
}

/// An L1 epoch with one deposit: the archived `Deposit` carries a `u128`
/// and a `U256`, so it needs the 16-byte alignment too.
fn epoch_with_deposit() -> EpochRecord {
    EpochRecord {
        l1_number: 77,
        l1_hash: B256::repeat_byte(0x7A),
        deposits: vec![Deposit {
            source_hash: B256::repeat_byte(0xAA),
            from: Address::repeat_byte(0x11),
            to: Some(Address::repeat_byte(0x22)),
            mint: 1_000_000_000_000u128,
            value: U256::from(500u64),
            gas_limit: 200_000,
            is_system_transaction: false,
            input: (&[0xDEu8, 0xAD, 0xBE, 0xEF][..]).into(),
        }],
    }
}

/// Relay an origin-record ingress frame the way the Java service does: keep
/// the canonical id, drop the `header_len` bytes of sealer-only header,
/// forward the record type and the rkyv body.
fn relay_origin_record(ingress: &[u8], header_len: usize) -> Vec<u8> {
    let mut relayed = ingress[1..33].to_vec();
    relayed.extend_from_slice(&ingress[33 + header_len..]);
    relayed
}

fn decode_record(buf: &[u8]) -> TxOrderingMessage {
    match decode_egress(buf).unwrap() {
        EgressItem::Record { msg, .. } => msg,
        other => panic!("expected Record, got {other:?}"),
    }
}

/// The decoder copies each rkyv body into a 16-aligned buffer before it
/// reads it (audit 2026-09-03, L2). This test walks the input through every
/// offset mod 16, so the decode never depends on where the allocator placed
/// the frame. Both epoch kinds carry a `u128`-bearing archived type.
#[test]
fn epoch_bodies_decode_from_every_input_offset() {
    let remote = remote_epoch_with_callback();
    let remote_frame = encode_egress_record(
        1,
        &relay_origin_record(&encode_ingress_remote_epoch(&remote).unwrap(), 20),
    );
    let epoch = epoch_with_deposit();
    let epoch_frame = encode_egress_record(
        2,
        &relay_origin_record(&encode_ingress_epoch(&epoch).unwrap(), 12),
    );

    for shift in 0..16usize {
        let mut buf = vec![0u8; shift];
        buf.extend_from_slice(&remote_frame);
        assert_eq!(
            decode_record(&buf[shift..]),
            TxOrderingMessage::RemoteEpoch(remote.clone()),
            "remote epoch at input offset {shift}"
        );

        let mut buf = vec![0u8; shift];
        buf.extend_from_slice(&epoch_frame);
        assert_eq!(
            decode_record(&buf[shift..]),
            TxOrderingMessage::Epoch(epoch.clone()),
            "epoch at input offset {shift}"
        );
    }
}

/// Why the copy target is 16-aligned and not 8: rkyv refuses to read the
/// archived record from an address that is 8 mod 16.
#[test]
fn archived_remote_epoch_needs_sixteen_byte_alignment() {
    let body = rkyv::to_bytes::<rkyv::rancor::Error>(&remote_epoch_with_callback()).unwrap();
    let mut shifted = rkyv::util::AlignedVec::<16>::with_capacity(8 + body.len());
    shifted.extend_from_slice(&[0u8; 8]);
    shifted.extend_from_slice(&body);
    assert!(
        rkyv::from_bytes::<RemoteEpochRecord, rkyv::rancor::Error>(&shifted[8..]).is_err(),
        "an 8 mod 16 address must be rejected"
    );
}
