//! M8: a block group that overflows the 6-blob ceiling splits into several
//! posts. A block that overflows on its own is a loud `BlockTooLarge`. And
//! one remote-epoch record at the derivation cap fits in 5 blobs.

use alloy_primitives::{Address, B256};
use bytes::Bytes;
use kardamom_batcher::batch::{ClosedBlock, RecordedTx};
use kardamom_batcher::batcher::{
    Batcher, BatcherConfig, MAX_BLOBS_PER_BATCH, MockSender, pack_block_groups, pack_blocks,
};
use kardamom_batcher::blob::USABLE_BYTES_PER_BLOB;
use kardamom_batcher::error::BatcherError;
use kardamom_types::BPosition;
use kardamom_types::xchain::{
    MAX_REMOTE_EPOCH_WIRE_BYTES, REMOTE_EPOCH_FIXED_WIRE_BYTES, RemoteEpochRecord,
    XCHAIN_MSG_FIXED_WIRE_BYTES, XChainMessage, remote_epoch_wire_bytes, remote_source_hash,
};

fn pos(o: i32) -> BPosition {
    BPosition {
        term_id: 0,
        term_offset: o,
    }
}

/// One block with one tx of `raw_len` bytes. Uncompressed packing makes
/// the blob count a function of `raw_len` only.
fn block_of(block_number: u64, raw_len: usize) -> ClosedBlock {
    ClosedBlock {
        block_number,
        l2_timestamp: 1_700_000_000 + block_number,
        end_tx_idx: pos(block_number as i32 * 64),
        remote_epochs: Vec::new(),
        txs: vec![RecordedTx {
            position: pos(0),
            envelope: kardamom_types::TxEnvelope {
                correlation_id: block_number,
                raw_tx: Bytes::from(vec![block_number as u8; raw_len]),
                sender: Address::repeat_byte(0x11),
                tx_hash: B256::repeat_byte(0x22),
            },
        }],
    }
}

fn uncompressed() -> BatcherConfig {
    BatcherConfig {
        compress: false,
        blocks_per_batch: 2,
        ..Default::default()
    }
}

/// About 4 blobs of payload: two such blocks overflow the ceiling.
const FOUR_BLOBS: usize = 4 * USABLE_BYTES_PER_BLOB - 1_000;
/// About 7 blobs of payload: one such block overflows on its own.
const SEVEN_BLOBS: usize = 7 * USABLE_BYTES_PER_BLOB - 1_000;

#[test]
fn group_over_the_ceiling_splits_at_block_boundaries() {
    let cfg = uncompressed();
    let blocks = vec![block_of(10, FOUR_BLOBS), block_of(11, FOUR_BLOBS)];
    assert!(matches!(
        pack_blocks(&cfg, &blocks),
        Err(BatcherError::Blob(_))
    ));

    let batches = pack_block_groups(&cfg, &blocks).unwrap();
    assert_eq!(batches.len(), 2);
    assert_eq!(
        (batches[0].l2_block_start, batches[0].l2_block_end),
        (10, 10)
    );
    assert_eq!(
        (batches[1].l2_block_start, batches[1].l2_block_end),
        (11, 11)
    );
    for b in &batches {
        assert!(b.blobs.len() <= MAX_BLOBS_PER_BATCH);
        assert_eq!(b.blobs.len(), 4);
    }
    // Each split batch equals the batch of that block alone.
    assert_eq!(
        batches[0].records_commitment,
        pack_blocks(&cfg, &blocks[..1]).unwrap().records_commitment
    );
}

#[test]
fn group_takes_the_largest_fitting_prefix() {
    let cfg = uncompressed();
    let small = USABLE_BYTES_PER_BLOB / 2;
    // 0.5 + 0.5 + 4 blobs fit together (5 blobs); the fourth block tips it.
    let blocks = vec![
        block_of(1, small),
        block_of(2, small),
        block_of(3, FOUR_BLOBS),
        block_of(4, FOUR_BLOBS),
        block_of(5, small),
    ];
    let batches = pack_block_groups(&cfg, &blocks).unwrap();
    let ranges: Vec<(u64, u64)> = batches
        .iter()
        .map(|b| (b.l2_block_start, b.l2_block_end))
        .collect();
    assert_eq!(ranges, vec![(1, 3), (4, 5)]);
}

#[test]
fn group_under_the_ceiling_is_one_batch() {
    let cfg = uncompressed();
    let blocks = vec![block_of(1, 100), block_of(2, 100), block_of(3, 100)];
    let batches = pack_block_groups(&cfg, &blocks).unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!((batches[0].l2_block_start, batches[0].l2_block_end), (1, 3));
}

#[test]
fn single_oversize_block_is_a_named_fatal() {
    let cfg = uncompressed();
    let blocks = vec![
        block_of(41, 100),
        block_of(42, SEVEN_BLOBS),
        block_of(43, 100),
    ];
    let err = pack_block_groups(&cfg, &blocks).unwrap_err();
    match err {
        BatcherError::BlockTooLarge {
            block_number,
            blobs,
        } => {
            assert_eq!(block_number, 42);
            assert_eq!(blobs, 7);
        }
        other => panic!("expected BlockTooLarge, got {other:?}"),
    }
    let text = err.to_string();
    assert!(text.contains("block 42"), "{text}");
    assert!(text.contains("7 blobs"), "{text}");

    // The single-block path names the block too.
    assert!(matches!(
        pack_blocks(&cfg, &blocks[1..2]),
        Err(BatcherError::BlockTooLarge {
            block_number: 42,
            ..
        })
    ));
}

#[test]
fn batcher_posts_a_split_group_as_two_batches() {
    let mut batcher = Batcher::new(uncompressed(), MockSender::default());
    batcher.on_closed_block(block_of(10, FOUR_BLOBS)).unwrap();
    assert!(batcher.sender().sent.is_empty());
    batcher.on_closed_block(block_of(11, FOUR_BLOBS)).unwrap();
    let sent = &batcher.sender().sent;
    assert_eq!(sent.len(), 2);
    assert_eq!(sent[0].l2_block_end, 10);
    assert_eq!(sent[1].l2_block_start, 11);
}

/// Pin `MAX_REMOTE_EPOCH_WIRE_BYTES` against the real KAR1 v2 encoder: a
/// record at the cap, alone in a block, packs to 5 blobs, fewer than the
/// ceiling. The fixed per-record and per-message byte counts in
/// `kardamom_types::xchain` must not drift from `frame.rs`.
#[test]
fn record_at_the_derivation_cap_fits_in_five_blobs() {
    const ORIGIN: u64 = 412_346;
    // Nine messages of 65_536 bytes plus one that fills the cap exactly.
    let full = 65_536usize;
    let mut lens = vec![full; 9];
    let used = remote_epoch_wire_bytes(lens.iter().copied()) + XCHAIN_MSG_FIXED_WIRE_BYTES;
    lens.push(MAX_REMOTE_EPOCH_WIRE_BYTES - used);
    assert_eq!(
        remote_epoch_wire_bytes(lens.iter().copied()),
        MAX_REMOTE_EPOCH_WIRE_BYTES
    );
    let messages: Vec<XChainMessage> = lens
        .iter()
        .enumerate()
        .map(|(i, &n)| XChainMessage {
            source_hash: remote_source_hash(ORIGIN, i as u64),
            seq: i as u64,
            origin_sender: Address::repeat_byte(0xA1),
            target: Address::repeat_byte(0xB2),
            value: 0,
            gas_limit: 150_000,
            input: Bytes::from(vec![i as u8; n]),
            callback: Some(kardamom_types::xchain::Callback {
                target: Address::repeat_byte(0xCB),
                gas_limit: 90_000,
                context: B256::repeat_byte(0x42),
            }),
        })
        .collect();
    let record = RemoteEpochRecord {
        origin_chain_id: ORIGIN,
        anchor_number: 100,
        anchor_hash: B256::repeat_byte(0x0B),
        first_seq: 0,
        messages,
    };
    let block = ClosedBlock {
        block_number: 3,
        l2_timestamp: 1_700_000_003,
        end_tx_idx: pos(0),
        remote_epochs: vec![record],
        txs: Vec::new(),
    };
    let cfg = BatcherConfig {
        compress: false,
        ..Default::default()
    };
    let batch = pack_blocks(&cfg, &[block]).unwrap();
    assert_eq!(batch.blobs.len(), 5);
}

/// The fixed byte counts in `kardamom_types::xchain` equal what the KAR1
/// v2 encoder writes: one record with one empty message, with a callback,
/// adds exactly the two constants to the block frame.
#[test]
fn wire_constants_match_the_encoder() {
    use kardamom_batcher::frame::{BlockFrame, Kar1Payload, encode};
    let empty = BlockFrame {
        block_number: 1,
        l2_timestamp: 2,
        remote_epochs: Vec::new(),
        txs: Vec::new(),
    };
    let mut led = empty.clone();
    led.remote_epochs = vec![RemoteEpochRecord {
        origin_chain_id: 412_346,
        anchor_number: 100,
        anchor_hash: B256::repeat_byte(0x0B),
        first_seq: 0,
        messages: vec![XChainMessage {
            source_hash: remote_source_hash(412_346, 0),
            seq: 0,
            origin_sender: Address::repeat_byte(0xA1),
            target: Address::repeat_byte(0xB2),
            value: 0,
            gas_limit: 150_000,
            input: Bytes::new(),
            callback: Some(kardamom_types::xchain::Callback {
                target: Address::repeat_byte(0xCB),
                gas_limit: 90_000,
                context: B256::repeat_byte(0x42),
            }),
        }],
    }];
    let payload = |b: BlockFrame| Kar1Payload {
        blocks: vec![b],
        compressed: false,
    };
    let base = encode(&payload(empty)).unwrap().len();
    let with = encode(&payload(led)).unwrap().len();
    assert_eq!(
        with - base,
        REMOTE_EPOCH_FIXED_WIRE_BYTES + XCHAIN_MSG_FIXED_WIRE_BYTES
    );
}
