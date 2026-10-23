//! The full reference (record type 7) on the golden frames that the sealer
//! tests also read.

use super::entry_tests::{Fixture, golden_frame};
use super::relay::RelayShape;
use super::*;

/// The first 99 bytes of a full reference are the frame of a positioned
/// reference with record type 7. The envelope follows.
#[test]
fn the_full_reference_is_the_positioned_reference_then_the_envelope() {
    let frame = golden_frame("txref_offer_ingress");
    let envelope = Fixture::archived(&Fixture::envelope());
    let offer = Fixture::txref_offer(&envelope);
    let mut reference = encode_ingress_txref(&offer.reference, offer.guard);
    assert_eq!(reference.len(), TXREF_RECORD_LEN);
    reference[INGRESS_CANONICAL_ID_OFFSET + CANONICAL_ID_LEN] = RT_TXREF_OFFER;
    assert_eq!(frame[..TXREF_RECORD_LEN], reference);
    assert_eq!(frame[TXREF_RECORD_LEN..], envelope);
    assert_eq!(TXREF_RECORD_LEN, 99);
    assert_eq!(
        ingress_deadline(&frame).unwrap(),
        POSITION_TAG | Fixture::POSITION
    );
}

#[test]
fn the_golden_full_reference_reads_back() {
    let frame = golden_frame("txref_offer_ingress");
    let envelope = Fixture::archived(&Fixture::envelope());
    let offer = TxRefOffer::parse(&frame).unwrap();
    assert_eq!(offer, Fixture::txref_offer(&envelope));
    assert_eq!(offer.position(), Some(Fixture::POSITION));
    assert_eq!(offer.canonical().unwrap(), Fixture::tx());
}

/// The sealer relays a full reference as the canonical entry of its
/// transaction: the same frame that a full entry gives.
#[test]
fn the_full_reference_relays_as_the_golden_canonical_entry() {
    let frame = golden_frame("txref_offer_ingress");
    let offer = TxRefOffer::parse(&frame).unwrap();
    assert_eq!(
        encode_egress_record(Fixture::INDEX, &offer.canonical_payload()).unwrap(),
        golden_frame("tx_egress")
    );
}

#[test]
fn the_full_reference_reader_refuses_other_frames() {
    let reference = golden_frame("txref_ingress");
    assert_eq!(
        TxRefOffer::parse(&reference),
        Err(WireError::BadRecordType(RT_TXREF))
    );
    let frame = golden_frame("txref_offer_ingress");
    assert!(matches!(
        TxRefOffer::parse(&frame[..TXREF_RECORD_LEN + ENVELOPE_ROOT_LEN - 1]),
        Err(WireError::TooShort { .. })
    ));
    let mut batch = frame.clone();
    batch[0] = KIND_BATCH;
    assert_eq!(
        TxRefOffer::parse(&batch),
        Err(WireError::BadIngressKind(KIND_BATCH))
    );
    let mut untagged = frame;
    untagged[INGRESS_DEADLINE_OFFSET..INGRESS_TIP_OFFSET].copy_from_slice(&4242_u64.to_le_bytes());
    assert_eq!(TxRefOffer::parse(&untagged).unwrap().position(), None);
}

/// The shortest archived envelope is its root: an empty raw transaction.
#[test]
fn the_shortest_envelope_is_the_root() {
    assert_eq!(
        Fixture::archived(&Fixture::envelope_with(Vec::new())).len(),
        ENVELOPE_ROOT_LEN
    );
}

/// A sealer below decision version 5 relays a full reference as is. The
/// reader decodes it as the same canonical entry as the frame of version 5.
#[test]
fn a_relayed_full_reference_decodes_as_its_canonical_entry() {
    let frame = golden_frame("txref_offer_egress");
    assert_eq!(
        frame,
        encode_egress_record(
            Fixture::INDEX,
            &RelayShape::of(KIND_INGRESS_RECORD).relay(&golden_frame("txref_offer_ingress")),
        )
        .unwrap()
    );
    let item = EgressItem::decode(&frame).unwrap();
    assert_eq!(
        item,
        EgressItem::Tx {
            index: Fixture::INDEX,
            tx: Fixture::tx(),
        }
    );
    assert_eq!(
        item,
        EgressItem::decode(&golden_frame("tx_egress")).unwrap()
    );
}

#[test]
fn the_full_reference_payload_of_an_entry_decodes_to_the_entry() {
    let tx = Fixture::tx();
    let frame = encode_egress_record(Fixture::INDEX, &tx.full_ref_payload().unwrap()).unwrap();
    assert_eq!(
        EgressItem::decode(&frame).unwrap(),
        EgressItem::Tx {
            index: Fixture::INDEX,
            tx,
        }
    );
}

/// The sequencer is trusted to write an id that is the hash in the
/// envelope. A relayed full reference with another id is refused, and the
/// reader drops the frame.
#[test]
fn a_relayed_full_reference_whose_envelope_does_not_hash_to_its_id_is_refused() {
    let mut payload = Fixture::tx().full_ref_payload().unwrap();
    payload[0] ^= 1;
    let frame = encode_egress_record(Fixture::INDEX, &payload).unwrap();
    assert!(matches!(
        EgressItem::decode(&frame),
        Err(WireError::BadTx(_))
    ));
}

#[test]
fn a_relayed_full_reference_with_a_short_envelope_is_refused() {
    let payload = Fixture::tx().full_ref_payload().unwrap();
    let short = &payload[..TXREF_RECORD_LEN - INGRESS_CANONICAL_ID_OFFSET + ENVELOPE_ROOT_LEN - 1];
    let frame = encode_egress_record(Fixture::INDEX, short).unwrap();
    assert!(matches!(
        EgressItem::decode(&frame),
        Err(WireError::TooShort { .. })
    ));
}
