//! KAR1 framing round-trip tests.

use alloy_primitives::{Address, B256};
use bytes::Bytes;
use kardamom_batcher::frame::{
    BlockCursor, BlockFrame, Kar1Payload, MAGIC, TxFrame, VERSION, VERSION_NO_CURSOR, decode,
    encode,
};

fn sample_payload() -> Kar1Payload {
    Kar1Payload {
        blocks: vec![
            BlockFrame {
                block_number: 42,
                l2_timestamp: 1_700_000_000,
                cursor: None,
                remote_epochs: vec![],
                txs: vec![
                    TxFrame {
                        correlation_id: 1,
                        sender: Address::repeat_byte(0xAA),
                        tx_hash: B256::repeat_byte(0xBB),
                        raw_tx: Bytes::from_static(&[0xDE, 0xAD, 0xBE, 0xEF]),
                    },
                    TxFrame {
                        correlation_id: 2,
                        sender: Address::repeat_byte(0xCC),
                        tx_hash: B256::repeat_byte(0xDD),
                        raw_tx: Bytes::from_static(b""),
                    },
                ],
            },
            BlockFrame {
                block_number: 43,
                l2_timestamp: 1_700_000_250,
                cursor: None,
                remote_epochs: vec![],
                txs: vec![],
            },
        ],
        compressed: false,
    }
}

#[test]
fn roundtrip_preserves_payload() {
    let original = sample_payload();
    let bytes = encode(&original).expect("encode");
    let decoded = decode(&bytes).expect("decode");
    assert_eq!(decoded, original);
}

#[test]
fn header_starts_with_magic_and_version() {
    let bytes = encode(&sample_payload()).expect("encode");
    assert_eq!(&bytes[..4], &MAGIC);
    assert_eq!(bytes[4], 2, "version byte");
    assert_eq!(bytes[5], 0, "uncompressed flag");
}

#[test]
fn empty_payload_roundtrips() {
    let p = Kar1Payload::default();
    let bytes = encode(&p).expect("encode");
    let back = decode(&bytes).expect("decode");
    assert_eq!(back, p);
}

#[test]
fn decode_rejects_bad_magic() {
    let mut bad = encode(&sample_payload()).unwrap();
    bad[0] = b'X';
    assert!(decode(&bad).is_err());
}

#[test]
fn compressed_flag_round_trips() {
    let p = Kar1Payload {
        compressed: true,
        ..sample_payload()
    };
    let bytes = encode(&p).unwrap();
    assert_eq!(bytes[5] & 1, 1, "flag bit 0 reflects compressed");
    let back = decode(&bytes).unwrap();
    assert!(back.compressed);
}

fn block(number: u64, cursor: Option<BlockCursor>) -> BlockFrame {
    BlockFrame {
        block_number: number,
        l2_timestamp: 1_700_000_000 + number,
        cursor,
        ..BlockFrame::default()
    }
}

#[test]
fn a_block_cursor_round_trips_at_version_3() {
    let cursor = BlockCursor {
        end_tx_idx: 115_892,
        l1_origin: 41,
    };
    let payload = Kar1Payload {
        blocks: vec![block(505, Some(cursor))],
        compressed: false,
    };
    let bytes = encode(&payload).unwrap();
    assert_eq!(bytes[4], VERSION);
    assert_eq!(decode(&bytes).unwrap(), payload);
}

/// Blobs posted before the cursor stay on L1 for good, so the decoder
/// must keep reading them; their blocks come back with no cursor.
#[test]
fn a_version_2_payload_decodes_with_no_cursor() {
    let payload = Kar1Payload {
        blocks: vec![block(1, None), block(2, None)],
        compressed: false,
    };
    let bytes = encode(&payload).unwrap();
    assert_eq!(bytes[4], VERSION_NO_CURSOR);
    assert_eq!(decode(&bytes).unwrap(), payload);
}

#[test]
fn a_payload_with_and_without_cursors_is_refused() {
    let payload = Kar1Payload {
        blocks: vec![block(1, Some(BlockCursor::default())), block(2, None)],
        compressed: false,
    };
    let err = encode(&payload).unwrap_err().to_string();
    assert!(err.contains("one version"), "{err}");
}
