//! Reconstruction round-trip: encode → pack → unpack → decode should yield
//! the original block frames. Mirrors the §6 conformance hook.

use alloy_primitives::{Address, B256};
use bytes::Bytes;
use kardamom_batcher::batch::{ClosedBlock, RecordedTx};
use kardamom_batcher::batcher::{BatcherConfig, pack_blocks};
use kardamom_batcher::frame::{BlockFrame, TxFrame};
use kardamom_batcher::recon::reconstruct;
use kardamom_types::{BPosition, TxEnvelope};

fn pos(o: i32) -> BPosition {
    BPosition {
        term_id: 0,
        term_offset: o,
    }
}

fn closed(block_number: u64, n: usize) -> ClosedBlock {
    let txs: Vec<RecordedTx> = (0..n)
        .map(|i| RecordedTx {
            position: pos((i * 64) as i32),
            envelope: TxEnvelope {
                correlation_id: i as u64,
                raw_tx: Bytes::from(vec![0xAB; 100]),
                sender: Address::repeat_byte(i as u8),
                tx_hash: B256::repeat_byte(i as u8),
            },
        })
        .collect();
    ClosedBlock {
        block_number,
        l2_timestamp: 1_700_000_000 + block_number,
        end_tx_idx: pos((n as i32) * 64),
        txs,
    }
}

fn expected_frames(blocks: &[ClosedBlock]) -> Vec<BlockFrame> {
    blocks
        .iter()
        .map(|b| BlockFrame {
            block_number: b.block_number,
            l2_timestamp: b.l2_timestamp,
            txs: b
                .txs
                .iter()
                .map(|t| TxFrame {
                    correlation_id: t.envelope.correlation_id,
                    sender: t.envelope.sender,
                    tx_hash: t.envelope.tx_hash,
                    raw_tx: t.envelope.raw_tx.clone(),
                })
                .collect(),
        })
        .collect()
}

#[test]
fn roundtrip_one_block_compressed() {
    let blocks = vec![closed(1, 3)];
    let batch = pack_blocks(&BatcherConfig::default(), &blocks).unwrap();
    let reconstructed = reconstruct(&batch.blobs).unwrap();
    assert_eq!(reconstructed, expected_frames(&blocks));
}

#[test]
fn roundtrip_one_block_uncompressed() {
    let blocks = vec![closed(2, 3)];
    let cfg = BatcherConfig {
        compress: false,
        ..Default::default()
    };
    let batch = pack_blocks(&cfg, &blocks).unwrap();
    let reconstructed = reconstruct(&batch.blobs).unwrap();
    assert_eq!(reconstructed, expected_frames(&blocks));
}

#[test]
fn roundtrip_five_blocks_grouped() {
    let blocks: Vec<ClosedBlock> = (10..15).map(|i| closed(i as u64, 2)).collect();
    let cfg = BatcherConfig {
        blocks_per_batch: 5,
        ..Default::default()
    };
    let batch = pack_blocks(&cfg, &blocks).unwrap();
    assert_eq!(batch.l2_block_start, 10);
    assert_eq!(batch.l2_block_end, 14);
    let reconstructed = reconstruct(&batch.blobs).unwrap();
    assert_eq!(reconstructed, expected_frames(&blocks));
}
