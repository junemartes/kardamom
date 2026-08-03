use alloy_primitives::{Address, B256};
use bytes::Bytes;
use kardamom_log::codec::{access, encode, materialize};
use kardamom_types::*;

#[test]
fn log_codec_access_and_materialize() {
    let v = TxEnvelope {
        correlation_id: 7,
        raw_tx: Bytes::from_static(b"raw"),
        sender: Address::repeat_byte(0xAA),
        tx_hash: B256::repeat_byte(0xBB),
    };
    let bytes = encode(&v).unwrap();

    // Zero-copy view.
    let view = access::<TxEnvelope>(&bytes).unwrap();
    assert_eq!(view.correlation_id, 7);

    // Owning view.
    let back: TxEnvelope = materialize(&bytes).unwrap();
    assert_eq!(back, v);
}

#[test]
fn log_codec_tx_ref_roundtrip() {
    // TxData still carries full TxEnvelopes (above). TxOrdering carries
    // tiny TxRef-based messages. Verify both wire shapes encode
    // through the shared codec helpers.
    let r = TxRef {
        tx_hash: alloy_primitives::B256::ZERO,
        shard_id: 2,
        tx_data_position: BPosition {
            term_id: 4,
            term_offset: 8192,
        },
        tx_data_session_id: 0,
    };
    let bytes = encode(&r).unwrap();
    // ~16 B on the wire (TxRef is sequencer_id: u8 + BPosition: i32+i32 + padding).
    // Hard-bound at 64 to give rkyv 0.8 some metadata headroom without letting
    // the type grow silently — a regression in size is a regression in the
    // canonical-orderer's CAS-cursor pressure.
    assert!(
        bytes.len() <= 64,
        "TxRef wire size grew unexpectedly: {}",
        bytes.len()
    );
    let back: TxRef = materialize(&bytes).unwrap();
    assert_eq!(back, r);

    let view = access::<TxRef>(&bytes).unwrap();
    assert_eq!(view.shard_id, 2);
    assert_eq!(view.tx_data_position.term_id, 4);
    assert_eq!(view.tx_data_position.term_offset, 8192);
}

#[test]
fn log_codetx_receipts_channel_b_message_roundtrip() {
    let m = TxOrderingMessage::TxRef(TxRef {
        tx_hash: alloy_primitives::B256::ZERO,
        shard_id: 3,
        tx_data_position: BPosition {
            term_id: 1,
            term_offset: 4096,
        },
        tx_data_session_id: 0,
    });
    let bytes = encode(&m).unwrap();
    let back: TxOrderingMessage = materialize(&bytes).unwrap();
    assert_eq!(back, m);

    let b = TxOrderingMessage::BoundaryStart(BlockBoundaryStart {
        block_number: 17,
        end_tx_idx: BPosition {
            term_id: 1,
            term_offset: 8192,
        },
        l2_timestamp: 1_700_000_000,
        l1_origin: 0,
    });
    let bytes_b = encode(&b).unwrap();
    let back_b: TxOrderingMessage = materialize(&bytes_b).unwrap();
    assert_eq!(back_b, b);
}
