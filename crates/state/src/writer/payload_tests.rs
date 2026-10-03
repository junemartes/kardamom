//! The block payload store: one row per committed block, in the form the
//! batcher posts, and the retention window the writer keeps.

use alloy_primitives::{Address, B256};
use kardamom_types::kar1::{BlockRecords, TxFrame, decode};
use kardamom_types::{BPosition, BlockBoundary, BlockDelta};

use super::{PayloadRetention, StateWriter, TrieMode, WriteBatch, WriterOptions};
use crate::env::{Durability, StateEnvBuilder};
use crate::snapshot::StateSnapshot;

fn boundary(block: u64) -> BlockBoundary {
    BlockBoundary {
        block_number: block,
        end_tx_idx: BPosition::from_index(block * 3),
        l2_timestamp: 1_700_000_000 + block,
        l1_origin: 10 + block,
    }
}

fn records(block: u64) -> BlockRecords {
    BlockRecords {
        remote_epochs: Vec::new(),
        txs: vec![TxFrame {
            correlation_id: block,
            sender: Address::repeat_byte(0x11),
            tx_hash: B256::repeat_byte(u8::try_from(block).unwrap()),
            raw_tx: bytes::Bytes::from(vec![0xAA; 8]),
        }],
    }
}

/// Every committed block gets a payload row that decodes back to the
/// block's records and its boundary; the row `retention` blocks behind
/// each commit is gone.
#[test]
fn payload_rows_decode_to_the_block_and_fall_out_of_the_retention_window() {
    let dir = tempfile::tempdir().unwrap();
    let env = StateEnvBuilder::new(dir.path())
        .durability(Durability::SafeNoSync)
        .open()
        .unwrap();
    let retention = PayloadRetention::new(std::num::NonZeroU64::new(3).unwrap());
    let mut handle = StateWriter::spawn_with(
        env.clone(),
        WriterOptions {
            trie_mode: TrieMode::Off,
            payload_retention: retention,
        },
    )
    .unwrap();
    for block in 1..=5 {
        handle
            .delta_tx
            .send(WriteBatch::with_records(
                boundary(block),
                BlockDelta::default(),
                records(block),
            ))
            .unwrap();
    }
    handle.shutdown().unwrap();

    let snapshot = StateSnapshot::open(&env).unwrap();
    assert_eq!(snapshot.block_number(), 5);
    assert_eq!(snapshot.block_payload(1).unwrap(), None);
    assert_eq!(snapshot.block_payload(2).unwrap(), None);
    for block in 3..=5 {
        let bytes = snapshot.block_payload(block).unwrap().unwrap();
        let payload = decode(&bytes).unwrap();
        let [frame] = payload.blocks.as_slice() else {
            panic!("one block per row");
        };
        assert_eq!(frame.block_number, block);
        assert_eq!(frame.l2_timestamp, 1_700_000_000 + block);
        let cursor = frame.cursor.unwrap();
        assert_eq!(cursor.end_tx_idx, block * 3);
        assert_eq!(cursor.l1_origin, 10 + block);
        assert_eq!(frame.txs, records(block).txs);
    }
    assert_eq!(snapshot.block_payload(6).unwrap(), None);
}

/// The default writer keeps the default window, so a plain `spawn`
/// never prunes a young chain.
#[test]
fn the_default_retention_keeps_every_block_of_a_young_chain() {
    let dir = tempfile::tempdir().unwrap();
    let env = StateEnvBuilder::new(dir.path())
        .durability(Durability::SafeNoSync)
        .open()
        .unwrap();
    let mut handle = StateWriter::spawn(env.clone()).unwrap();
    for block in 1..=3 {
        handle
            .delta_tx
            .send(WriteBatch::new(boundary(block), BlockDelta::default()))
            .unwrap();
    }
    handle.shutdown().unwrap();
    let snapshot = StateSnapshot::open(&env).unwrap();
    assert_eq!(PayloadRetention::default().blocks(), 100_000);
    for block in 1..=3 {
        let payload = decode(&snapshot.block_payload(block).unwrap().unwrap()).unwrap();
        assert!(payload.blocks[0].txs.is_empty());
    }
}
