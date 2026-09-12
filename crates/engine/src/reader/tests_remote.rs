//! The `tx_ordering` reader's remote-epoch expansion: the marker plus one
//! dispatch per message, and the racing-sequencer duplicate drop. The
//! fixtures live in the sibling `tests` module.

use std::num::NonZeroU64;

use kardamom_types::xchain::{RemoteEpochRecord, XChainMessage};
use kardamom_types::{BlockBoundaryStart, TxOrderingMessage};

use super::tests::{assert_xchain_at, pos, run_ordering};
use super::{JoinBuffer, ReaderConfig, ReaderToExec};
use crate::exec_types::TxIndex;

fn remote_record(origin: u64, first_seq: u64, n: NonZeroU64) -> RemoteEpochRecord {
    let message_at = |seq: u64| XChainMessage {
        source_hash: kardamom_types::xchain::remote_source_hash(origin, seq),
        seq,
        gas_limit: 100_000,
        ..Default::default()
    };
    RemoteEpochRecord {
        origin_chain_id: origin,
        anchor_number: 40,
        anchor_hash: alloy_primitives::B256::repeat_byte(0xAB),
        first_seq,
        messages: kardamom_types::xchain::NonEmptyVec::new(
            message_at(first_seq),
            (first_seq + 1..first_seq + n.get())
                .map(message_at)
                .collect(),
        ),
    }
}

/// A remote epoch expands exactly like an L1 epoch: the marker plus one
/// dispatch per message, `tx_idx` contiguous across the whole range.
#[test]
fn channel_b_reader_expands_a_remote_epoch_into_marker_plus_messages() {
    let origin = 412_346u64;
    let rec = remote_record(origin, 5, NonZeroU64::new(2).expect("2 is nonzero"));
    let out = run_ordering(
        vec![
            Ok((pos(0), TxOrderingMessage::RemoteEpoch(rec.clone()))),
            Ok((
                pos(3),
                TxOrderingMessage::BoundaryStart(BlockBoundaryStart {
                    block_number: 1,
                    // Marker + 2 messages = 3 slots consumed.
                    end_tx_idx: pos(3),
                    l2_timestamp: 1_700_000_000,
                    l1_origin: 0,
                }),
            )),
        ],
        JoinBuffer::new(),
        ReaderConfig::default(),
    )
    .expect("ok");
    assert_eq!(out.len(), 4, "marker + 2 messages + boundary");
    match &out[0] {
        ReaderToExec::RemoteEpoch { tx_idx, record, .. } => {
            assert_eq!(*tx_idx, TxIndex(0));
            assert_eq!(record.origin_chain_id, origin);
            assert_eq!(record.first_seq, 5);
        }
        other => panic!("expected RemoteEpoch marker, got {other:?}"),
    }
    for (i, expected) in rec.messages.iter().enumerate() {
        assert_xchain_at(i, origin, expected, &out[1 + i]);
    }
    match &out[3] {
        ReaderToExec::Boundary(b) => assert_eq!(b.end_tx_idx, pos(3)),
        other => panic!("expected Boundary, got {other:?}"),
    }
}

/// A duplicate remote epoch from a racing sequencer must dispatch
/// nothing. A second expansion would double-deliver every message.
#[test]
fn channel_b_reader_drops_a_duplicate_remote_epoch() {
    let rec = remote_record(412_346, 0, NonZeroU64::new(1).expect("1 is nonzero"));
    let out = run_ordering(
        vec![
            Ok((pos(0), TxOrderingMessage::RemoteEpoch(rec.clone()))),
            Ok((pos(2), TxOrderingMessage::RemoteEpoch(rec))),
        ],
        JoinBuffer::new(),
        ReaderConfig::default(),
    )
    .expect("ok");
    assert_eq!(out.len(), 2, "one marker + one message, not two of each");
}
