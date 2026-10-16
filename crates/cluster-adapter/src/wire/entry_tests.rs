//! The entry codecs on the golden frames that the sealer tests also read,
//! and the ingress cap against the growth of each relay.

use alloy_primitives::{Address, B256};
use kardamom_cluster_client::message::{MAX_SESSION_MESSAGE_LEN, SESSION_HEADER_LEN};
use kardamom_types::cluster_status::RecordLagStatus;
use kardamom_types::{BPosition, ClusterStatus, TxEnvelope, TxOrderingMessage, TxRef};

use super::relay::{RELAY_SHAPES, RelayShape};
use super::*;

/// The golden frames, one `name hex` line each. The sealer service test
/// `WireGoldenTest` reads the same file.
const GOLDEN: &str = include_str!("../../testdata/wire_golden.txt");

/// The longest application payload of one session message.
const MAX_APP_PAYLOAD_LEN: usize = MAX_SESSION_MESSAGE_LEN - SESSION_HEADER_LEN;

/// The values that the golden frames encode.
struct Fixture;

impl Fixture {
    const INDEX: u64 = 5;
    const LANE: u8 = 3;

    fn guard() -> GuardHeader {
        GuardHeader {
            sender: Address::repeat_byte(0x11),
            nonce: 7,
            max_inclusion_block: 4242,
            tip: 0x0102,
        }
    }

    fn envelope() -> TxEnvelope {
        Self::envelope_with(vec![0x02, 0xf8, 0x6b, 0x01, 0x80])
    }

    fn envelope_with(raw_tx: Vec<u8>) -> TxEnvelope {
        TxEnvelope {
            correlation_id: 0x0102_0304_0506_0708,
            raw_tx: raw_tx.into(),
            sender: Address::repeat_byte(0x11),
            tx_hash: B256::repeat_byte(0xAB),
            max_inclusion_block: 4242,
        }
    }

    fn tx() -> CanonicalTx {
        CanonicalTx {
            canonical_id: B256::repeat_byte(0xAB),
            lane: Self::LANE,
            envelope: Self::envelope(),
        }
    }

    fn archived(envelope: &TxEnvelope) -> Vec<u8> {
        rkyv::to_bytes::<rkyv::rancor::Error>(envelope)
            .unwrap()
            .to_vec()
    }

    fn offer(envelope: &[u8]) -> Vec<u8> {
        TxOffer {
            guard: Self::guard(),
            canonical_id: B256::repeat_byte(0xAB),
            lane: Self::LANE,
            envelope,
        }
        .encode()
    }

    fn relayed_ref() -> RelayedRef {
        RelayedRef {
            index: Self::INDEX,
            canonical_id: B256::repeat_byte(0xAB),
            record_type: RT_TX,
        }
    }

    fn status(retained_bytes: Option<u64>) -> ClusterStatus {
        ClusterStatus {
            posted_head: 100,
            sealed_head: 160,
            budget_blocks: 50,
            halted: false,
            retained_frames: 7000,
            floor_index: 90_000,
            floor_block: 95,
            record_lag: RecordLagStatus::default(),
            retained_bytes,
        }
    }

    fn txref() -> TxRef {
        TxRef::new(
            B256::repeat_byte(0xCD),
            Self::LANE,
            BPosition {
                term_id: 7,
                term_offset: 9_999,
            },
            5,
        )
    }

    /// Each golden frame, by name, from the Rust encoders.
    fn frames() -> Vec<(&'static str, Vec<u8>)> {
        let txref_ingress = encode_ingress_txref(&Self::txref(), Self::guard());
        let txref_egress = encode_egress_record(
            Self::INDEX,
            &RelayShape::of(KIND_INGRESS_RECORD).relay(&txref_ingress),
        )
        .unwrap();
        vec![
            (
                "tx_offer_ingress",
                Self::offer(&Self::archived(&Self::envelope())),
            ),
            (
                "tx_egress",
                encode_egress_record(Self::INDEX, &Self::tx().payload().unwrap()).unwrap(),
            ),
            ("relayed_ref", Self::relayed_ref().encode()),
            ("feed_gap", encode_feed_gap(9, 4)),
            ("status_75", encode_status(&Self::status(Some(1_048_583)))),
            ("status_67", encode_status(&Self::status(None))),
            ("txref_ingress", txref_ingress),
            ("txref_egress", txref_egress),
        ]
    }
}

/// The golden frames of the file, by name.
fn golden() -> Vec<(&'static str, Vec<u8>)> {
    GOLDEN
        .lines()
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(|line| line.split_once(' ').expect("a line is `name hex`"))
        .map(|(name, hex)| (name, alloy_primitives::hex::decode(hex).unwrap()))
        .collect()
}

fn golden_frame(name: &str) -> Vec<u8> {
    golden()
        .into_iter()
        .find_map(|(n, frame)| (n == name).then_some(frame))
        .unwrap_or_else(|| panic!("no golden frame {name}"))
}

#[test]
fn the_encoders_write_the_golden_frames() {
    assert_eq!(golden(), Fixture::frames());
}

#[test]
fn the_full_entry_layout_is_the_guard_header_then_id_type_lane_envelope() {
    let frame = golden_frame("tx_offer_ingress");
    let envelope = Fixture::archived(&Fixture::envelope());
    assert_eq!(frame[0], KIND_INGRESS_RECORD);
    assert_eq!(
        ingress_sender_nonce(&frame).unwrap(),
        (Address::repeat_byte(0x11), 7)
    );
    assert_eq!(ingress_deadline(&frame).unwrap(), 4242);
    assert_eq!(ingress_tip(&frame).unwrap(), 0x0102);
    assert_eq!(&frame[53..85], B256::repeat_byte(0xAB).as_slice());
    assert_eq!(frame[85], RT_TX_OFFER);
    assert_eq!(frame[86], Fixture::LANE);
    assert_eq!(&frame[87..], envelope.as_slice());
    assert_eq!(frame.len(), 87 + envelope.len());
}

/// The sealer relays a full entry as a canonical entry: the payload from
/// the canonical id on, with record type 6 in place of 5.
#[test]
fn the_canonical_entry_is_the_relayed_full_entry_with_type_six() {
    let mut payload = RelayShape::of(KIND_INGRESS_RECORD).relay(&golden_frame("tx_offer_ingress"));
    payload[CANONICAL_ID_LEN] = RT_TX;
    assert_eq!(
        encode_egress_record(Fixture::INDEX, &payload).unwrap(),
        golden_frame("tx_egress")
    );
}

#[test]
fn the_golden_egress_frames_decode() {
    assert_eq!(
        EgressItem::decode(&golden_frame("tx_egress")).unwrap(),
        EgressItem::Tx {
            index: Fixture::INDEX,
            tx: Fixture::tx()
        }
    );
    let relayed_ref = golden_frame("relayed_ref");
    assert_eq!(relayed_ref.len(), RELAYED_REF_LEN);
    assert_eq!(
        EgressItem::decode(&relayed_ref).unwrap(),
        EgressItem::RelayedRef(Fixture::relayed_ref())
    );
    assert_eq!(
        EgressItem::decode(&golden_frame("feed_gap")).unwrap(),
        EgressItem::FeedGap {
            next_index: 9,
            next_block: 4
        }
    );
    let status = golden_frame("status_75");
    assert_eq!(status.len(), STATUS_WITH_RETAINED_BYTES_LEN);
    assert_eq!(
        EgressItem::decode(&status).unwrap(),
        EgressItem::Status(Fixture::status(Some(1_048_583)))
    );
}

/// The frames of today read as today: a 67-byte and a 50-byte status
/// have no retained bytes, and a reference record decodes as before.
#[test]
fn the_frames_of_today_still_decode() {
    let status = golden_frame("status_67");
    assert_eq!(status.len(), STATUS_WITH_RECORD_LAG_LEN);
    assert_eq!(
        EgressItem::decode(&status).unwrap(),
        EgressItem::Status(Fixture::status(None))
    );
    let EgressItem::Status(short) = EgressItem::decode(&status[..STATUS_LEN]).unwrap() else {
        panic!("a status frame decodes as a status");
    };
    assert_eq!(short.retained_bytes, None);
    assert_eq!(
        EgressItem::decode(&golden_frame("txref_egress")).unwrap(),
        EgressItem::Record {
            index: Fixture::INDEX,
            msg: TxOrderingMessage::TxRef(Fixture::txref())
        }
    );
}

#[test]
fn a_partial_byte_tail_is_ignored_and_a_short_reference_is_too_short() {
    let status = golden_frame("status_75");
    assert_eq!(
        EgressItem::decode(&status[..STATUS_WITH_RECORD_LAG_LEN + 3]).unwrap(),
        EgressItem::Status(Fixture::status(None))
    );
    let relayed_ref = golden_frame("relayed_ref");
    assert!(matches!(
        EgressItem::decode(&relayed_ref[..RELAYED_REF_LEN - 1]),
        Err(WireError::TooShort { .. })
    ));
}

#[test]
fn a_relayed_full_entry_is_refused() {
    let payload = RelayShape::of(KIND_INGRESS_RECORD).relay(&golden_frame("tx_offer_ingress"));
    let frame = encode_egress_record(1, &payload).unwrap();
    assert_eq!(
        EgressItem::decode(&frame),
        Err(WireError::UnrelayedRecordType(RT_TX_OFFER))
    );
}

#[test]
fn a_canonical_entry_whose_id_is_not_the_envelope_hash_is_refused() {
    let mut payload = Fixture::tx().payload().unwrap();
    payload[0] ^= 1;
    let frame = encode_egress_record(1, &payload).unwrap();
    assert!(matches!(
        EgressItem::decode(&frame),
        Err(WireError::BadTx(_))
    ));
}

/// Each shape of the table is the relay of its kind: a real frame relays
/// to a payload that decodes, and the egress frame has the length that the
/// table gives.
#[test]
fn each_relay_shape_matches_the_frames_of_its_kind() {
    let epoch = kardamom_types::epoch::EpochRecord {
        l1_number: 77,
        l1_hash: B256::repeat_byte(0x7A),
        deposits: Vec::new(),
    };
    let frames = [
        encode_ingress_txref(&Fixture::txref(), Fixture::guard()),
        encode_ingress_epoch(&epoch).unwrap(),
        encode_ingress_remote_epoch(&super::tests::remote_epoch()).unwrap(),
    ];
    RELAY_SHAPES
        .into_iter()
        .zip(frames)
        .for_each(|(shape, ingress)| {
            assert_eq!(ingress[0], shape.kind);
            let egress = encode_egress_record(0, &shape.relay(&ingress)).unwrap();
            assert!(EgressItem::decode(&egress).is_ok(), "kind {}", shape.kind);
            assert_eq!(Some(egress.len()), shape.egress_len(ingress.len()));
        });
}

/// The relay margins: a kind-0 frame loses 40 bytes, a remote-origin frame
/// 24, and an origin frame none.
#[test]
fn the_relay_margins_are_forty_twenty_four_and_zero() {
    let margins: Vec<(u8, usize)> = RELAY_SHAPES
        .into_iter()
        .map(|shape| (shape.kind, 1000 - shape.egress_len(1000).unwrap()))
        .collect();
    assert_eq!(
        margins,
        [
            (KIND_INGRESS_RECORD, 40),
            (KIND_ORIGIN_RECORD, 0),
            (KIND_REMOTE_ORIGIN_RECORD, 24)
        ]
    );
}

/// An ingress frame at the cap relays to an egress frame that fits one
/// session message, for each shape. The cap is the largest such length: the
/// shape with the largest growth fills the message exactly.
#[test]
fn the_ingress_cap_fits_the_relay_of_each_kind() {
    let egress: Vec<usize> = RELAY_SHAPES
        .into_iter()
        .map(|shape| shape.egress_len(MAX_INGRESS_PAYLOAD_LEN).unwrap())
        .collect();
    assert!(
        egress.iter().all(|len| *len <= MAX_APP_PAYLOAD_LEN),
        "{egress:?}"
    );
    assert!(
        RELAY_SHAPES
            .into_iter()
            .all(|shape| MAX_INGRESS_PAYLOAD_LEN + shape.growth() <= MAX_APP_PAYLOAD_LEN)
    );
    let largest = RELAY_SHAPES.into_iter().map(RelayShape::growth).max();
    assert_eq!(Some(MAX_APP_PAYLOAD_LEN - MAX_INGRESS_PAYLOAD_LEN), largest);
    assert_eq!(
        RelayShape::of(KIND_ORIGIN_RECORD).egress_len(MAX_INGRESS_PAYLOAD_LEN),
        Some(MAX_APP_PAYLOAD_LEN),
        "an origin record at the cap fills one session message"
    );
}

/// A frame at the cap for each record type: the kind byte, the header, and
/// a body that fills the frame. The sealer does not parse the body.
#[test]
fn a_frame_at_the_cap_of_each_record_type_fits_one_egress_message() {
    [
        (KIND_INGRESS_RECORD, RT_TXREF),
        (KIND_INGRESS_RECORD, RT_DEPOSITREF),
        (KIND_INGRESS_RECORD, RT_TX_OFFER),
        (KIND_ORIGIN_RECORD, RT_EPOCH),
        (KIND_REMOTE_ORIGIN_RECORD, RT_REMOTE_EPOCH),
    ]
    .into_iter()
    .map(|(kind, record_type)| (record_type, frame_at_the_cap(kind, record_type)))
    .for_each(|(record_type, egress)| {
        assert!(
            egress.len() <= MAX_APP_PAYLOAD_LEN,
            "record type {record_type}: {} bytes",
            egress.len()
        );
    });
}

/// The egress frame of an ingress frame at the cap of `kind`, with
/// `record_type` and a filler body.
fn frame_at_the_cap(kind: u8, record_type: u8) -> Vec<u8> {
    let shape = RelayShape::of(kind);
    let mut ingress = vec![0x5A; MAX_INGRESS_PAYLOAD_LEN];
    ingress[0] = kind;
    ingress[shape.before_id + CANONICAL_ID_LEN + shape.after_id] = record_type;
    encode_egress_record(0, &shape.relay(&ingress)).unwrap()
}

/// The largest canonical entry under the cap decodes from its egress frame.
#[test]
fn the_largest_canonical_entry_under_the_cap_decodes() {
    // An archived envelope is the raw bytes padded to 8, then the 80-byte
    // root. A full entry adds 87 bytes in front.
    let raw_len = (MAX_INGRESS_PAYLOAD_LEN - 87 - 80) / 8 * 8;
    let envelope = Fixture::envelope_with(vec![0xEE; raw_len]);
    let ingress = Fixture::offer(&Fixture::archived(&envelope));
    assert!(ingress.len() <= MAX_INGRESS_PAYLOAD_LEN);
    assert!(ingress.len() + 8 > MAX_INGRESS_PAYLOAD_LEN);

    let mut payload = RelayShape::of(KIND_INGRESS_RECORD).relay(&ingress);
    payload[CANONICAL_ID_LEN] = RT_TX;
    let egress = encode_egress_record(1, &payload).unwrap();
    assert!(egress.len() <= MAX_APP_PAYLOAD_LEN);
    let EgressItem::Tx { tx, .. } = EgressItem::decode(&egress).unwrap() else {
        panic!("a canonical entry decodes as an entry");
    };
    assert_eq!(tx.envelope, envelope);
}
