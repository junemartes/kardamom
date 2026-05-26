//! `BatchAccumulator` grouping tests.

use alloy_primitives::{Address, B256};
use bytes::Bytes;
use kardamom_batcher::batch::BatchAccumulator;
use kardamom_types::{BPosition, BlockBoundaryStart, TxEnvelope};

fn pos(offset: i32) -> BPosition {
    BPosition {
        term_id: 0,
        term_offset: offset,
    }
}

fn env(correlation_id: u64) -> TxEnvelope {
    TxEnvelope {
        correlation_id,
        raw_tx: Bytes::from_static(b"raw"),
        sender: Address::repeat_byte(0x11),
        tx_hash: B256::repeat_byte(0x22),
    }
}

#[test]
fn boundary_emits_block_with_pending_txs() {
    let mut acc = BatchAccumulator::new();
    acc.observe_tx(env(1), pos(0));
    acc.observe_tx(env(2), pos(64));
    let closed = acc.observe_boundary(BlockBoundaryStart {
        block_number: 7,
        end_tx_idx: pos(128),
        l2_timestamp: 1234,
    });
    assert_eq!(closed.block_number, 7);
    assert_eq!(closed.txs.len(), 2);
    assert_eq!(closed.txs[0].envelope.correlation_id, 1);
    assert_eq!(closed.txs[1].envelope.correlation_id, 2);
    assert_eq!(acc.pending_len(), 0);
}

#[test]
fn back_to_back_boundaries_emit_empty_blocks() {
    let mut acc = BatchAccumulator::new();
    let _ = acc.observe_boundary(BlockBoundaryStart {
        block_number: 1,
        end_tx_idx: pos(0),
        l2_timestamp: 100,
    });
    let next = acc.observe_boundary(BlockBoundaryStart {
        block_number: 2,
        end_tx_idx: pos(0),
        l2_timestamp: 200,
    });
    assert_eq!(next.block_number, 2);
    assert!(next.txs.is_empty());
}

#[test]
fn txs_interleave_correctly_across_three_blocks() {
    let mut acc = BatchAccumulator::new();
    acc.observe_tx(env(1), pos(0));
    let b1 = acc.observe_boundary(BlockBoundaryStart {
        block_number: 1,
        end_tx_idx: pos(64),
        l2_timestamp: 100,
    });
    acc.observe_tx(env(2), pos(64));
    acc.observe_tx(env(3), pos(128));
    let b2 = acc.observe_boundary(BlockBoundaryStart {
        block_number: 2,
        end_tx_idx: pos(192),
        l2_timestamp: 200,
    });
    let b3 = acc.observe_boundary(BlockBoundaryStart {
        block_number: 3,
        end_tx_idx: pos(192),
        l2_timestamp: 300,
    });

    assert_eq!(b1.txs.len(), 1);
    assert_eq!(b2.txs.len(), 2);
    assert_eq!(b3.txs.len(), 0);
}
