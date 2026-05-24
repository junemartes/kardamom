//! Property: full pipeline round-trip — encode → pack → reconstruct — for
//! arbitrary block payloads. Catches subtle interactions between zstd, blob
//! padding, and KAR1 framing.

use alloy_primitives::{Address, B256};
use bytes::Bytes;
use kardamom_batcher::batch::{ClosedBlock, RecordedTx};
use kardamom_batcher::batcher::{BatcherConfig, pack_blocks};
use kardamom_batcher::frame::{BlockFrame, TxFrame};
use kardamom_batcher::recon::reconstruct;
use kardamom_types::{BPosition, TxEnvelope};
use proptest::prelude::*;

fn pos(o: i32) -> BPosition {
    BPosition {
        term_id: 0,
        term_offset: o,
    }
}

fn arb_tx(correlation_id: u64, raw_len: usize) -> RecordedTx {
    RecordedTx {
        position: pos((correlation_id as i32) * 64),
        envelope: TxEnvelope {
            correlation_id,
            raw_tx: Bytes::from(vec![correlation_id as u8; raw_len]),
            sender: Address::repeat_byte(correlation_id as u8),
            tx_hash: B256::repeat_byte((correlation_id ^ 0xFF) as u8),
        },
    }
}

fn arb_block(number: u64, n_txs: usize, raw_len: usize) -> ClosedBlock {
    let txs = (0..n_txs as u64).map(|i| arb_tx(i, raw_len)).collect();
    ClosedBlock {
        block_number: number,
        l2_timestamp: 1_700_000_000 + number,
        end_tx_idx: pos(0),
        txs,
    }
}

fn to_block_frame(b: &ClosedBlock) -> BlockFrame {
    BlockFrame {
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
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    /// Compressed round-trip for variable block / tx / payload shapes.
    #[test]
    fn full_pipeline_compressed_roundtrip(
        n_blocks in 1usize..6,
        txs_per_block in 0usize..8,
        raw_len in 0usize..200,
    ) {
        let blocks: Vec<ClosedBlock> = (0..n_blocks as u64)
            .map(|i| arb_block(i, txs_per_block, raw_len))
            .collect();
        let cfg = BatcherConfig {
            blocks_per_batch: n_blocks,
            compress: true,
            ..Default::default()
        };
        let batch = pack_blocks(&cfg, &blocks).unwrap();
        prop_assert!(batch.blobs.len() <= 6, "exceeded 6-blob ceiling");
        let reconstructed = reconstruct(&batch.blobs).unwrap();
        let expected: Vec<BlockFrame> = blocks.iter().map(to_block_frame).collect();
        prop_assert_eq!(reconstructed, expected);
    }

    /// Same, uncompressed.
    #[test]
    fn full_pipeline_uncompressed_roundtrip(
        n_blocks in 1usize..4,
        txs_per_block in 0usize..6,
        raw_len in 0usize..150,
    ) {
        let blocks: Vec<ClosedBlock> = (0..n_blocks as u64)
            .map(|i| arb_block(i, txs_per_block, raw_len))
            .collect();
        let cfg = BatcherConfig {
            blocks_per_batch: n_blocks,
            compress: false,
            ..Default::default()
        };
        let batch = pack_blocks(&cfg, &blocks).unwrap();
        let reconstructed = reconstruct(&batch.blobs).unwrap();
        let expected: Vec<BlockFrame> = blocks.iter().map(to_block_frame).collect();
        prop_assert_eq!(reconstructed, expected);
    }
}
