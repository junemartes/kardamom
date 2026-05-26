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
    // Channel A still carries full TxEnvelopes (above). Channel B carries
    // tiny TxRef-based messages (D-Sh12). Verify both wire shapes encode
    // through the shared codec helpers.
    let r = TxRef {
        sequencer_id: 2,
        position_a: BPosition {
            term_id: 4,
            term_offset: 8192,
        },
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
    assert_eq!(view.sequencer_id, 2);
    assert_eq!(view.position_a.term_id, 4);
    assert_eq!(view.position_a.term_offset, 8192);
}

#[test]
fn log_codec_channel_b_message_roundtrip() {
    let m = ChannelBMessage::TxRef(TxRef {
        sequencer_id: 3,
        position_a: BPosition {
            term_id: 1,
            term_offset: 4096,
        },
    });
    let bytes = encode(&m).unwrap();
    let back: ChannelBMessage = materialize(&bytes).unwrap();
    assert_eq!(back, m);

    let b = ChannelBMessage::BoundaryStart(BlockBoundaryStart {
        block_number: 17,
        end_tx_idx: BPosition {
            term_id: 1,
            term_offset: 8192,
        },
        l2_timestamp: 1_700_000_000,
    });
    let bytes_b = encode(&b).unwrap();
    let back_b: ChannelBMessage = materialize(&bytes_b).unwrap();
    assert_eq!(back_b, b);
}
