//! The batcher writes the same KAR1 bytes from both sources of the
//! canonical stream:
//!
//! - the cluster egress, whose references the reader joins with the
//!   envelopes of the `tx_data` lanes;
//! - the egress (`--egress`), whose entries carry their bytes.
//!
//! The same `tx_ordering` reader thread reads each source. The test closes
//! the blocks of each source as the feed loop does, packs them, and
//! compares the bytes.

use alloy_primitives::Address;
use kardamom_batcher::batch::{BatchAccumulator, ClosedBlock};
use kardamom_batcher::batcher::{BatcherConfig, PostedBatch, pack_blocks};
use kardamom_batcher::frame::MAGIC;
use kardamom_engine::actor::fixtures::{LegacyTx, TestEntry, TestStream, byte_signer};
use kardamom_engine::reader::ReaderToExec;
use kardamom_types::{BPosition, BlockBoundaryStart};

/// The blocks that the batcher closes from the records of one source, and
/// the batch that it packs from them.
struct Packed {
    blocks: Vec<ClosedBlock>,
    batch: PostedBatch,
}

impl Packed {
    /// Close the blocks of `records` as the feed loop does, and pack them
    /// into one uncompressed batch: the KAR1 bytes.
    fn from_records(records: Vec<ReaderToExec>) -> Self {
        let mut acc = BatchAccumulator::new();
        let blocks: Vec<_> = records
            .into_iter()
            .filter_map(|record| match record {
                ReaderToExec::Tx {
                    envelope, position, ..
                } => {
                    acc.observe_tx(envelope, position);
                    None
                }
                ReaderToExec::Boundary(b) => Some(acc.observe_boundary(&b)),
                other => panic!("the stream has no {other:?}"),
            })
            .collect();
        let cfg = BatcherConfig {
            compress: false,
            ..BatcherConfig::default()
        };
        let batch = pack_blocks(&cfg, &blocks).expect("the blocks fit one batch");
        Self { blocks, batch }
    }
}

#[test]
fn the_egress_and_the_join_give_the_same_kar1_bytes() {
    let signer = byte_signer(0x11);
    let transfer = |nonce, correlation_id| {
        TestEntry::Tx(
            LegacyTx {
                to: Address::repeat_byte(0x22),
                nonce,
                value: 1,
                correlation_id,
                ..LegacyTx::default()
            }
            .sign(&signer),
        )
    };
    let boundary = |block_number, end| {
        TestEntry::Boundary(BlockBoundaryStart {
            block_number,
            end_tx_idx: BPosition::from_index(end),
            l2_timestamp: 1_700_000_000 + block_number,
            l1_origin: 0,
        })
    };
    let stream = TestStream(vec![
        transfer(0, 11),
        transfer(1, 12),
        boundary(1, 2),
        boundary(2, 2),
        transfer(2, 13),
        boundary(3, 3),
    ]);
    let rt = tokio::runtime::Runtime::new().unwrap();
    let dir = tempfile::tempdir().unwrap();

    let joined = Packed::from_records(stream.read_joined());
    let carried = Packed::from_records(stream.read_from_egress(&rt, dir.path()));

    let txs: Vec<_> = joined.blocks.iter().map(|b| b.txs.len()).collect();
    assert_eq!(txs, [2, 0, 1], "the join closes the three blocks");
    assert!(
        joined.batch.payload.starts_with(&MAGIC),
        "the batch is KAR1"
    );
    assert_eq!(
        carried.batch.payload, joined.batch.payload,
        "the egress gives the KAR1 bytes of the join"
    );
    assert_eq!(
        carried.batch.records_commitment, joined.batch.records_commitment,
        "the egress gives the records commitment of the join"
    );
}
