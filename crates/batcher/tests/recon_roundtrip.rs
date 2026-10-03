//! Reconstruction round-trip: encode, pack, unpack, then decode should
//! yield the original block frames. This mirrors the section 6 conformance
//! hook.

use std::num::NonZeroUsize;

use alloy_primitives::{Address, B256};
use bytes::Bytes;
use kardamom_batcher::batch::{ClosedBlock, RecordedTx};
use kardamom_batcher::batcher::{BatcherConfig, pack_blocks};
use kardamom_batcher::frame::{BlockCursor, BlockFrame, TxFrame};
use kardamom_batcher::recon::reconstruct;
use kardamom_types::{BPosition, TxEnvelope};

#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    reason = "n and the range below are always small literal test fixtures; the casts here \
              never approach their target types' limits"
)]
fn closed(block_number: u64, n: usize) -> ClosedBlock {
    let txs: Vec<RecordedTx> = (0..n)
        .map(|i| RecordedTx {
            position: BPosition::from_index((i * 64) as u64),
            envelope: TxEnvelope {
                correlation_id: i as u64,
                raw_tx: Bytes::from(vec![0xAB; 100]),
                sender: Address::repeat_byte(i as u8),
                tx_hash: B256::repeat_byte(i as u8),
                max_inclusion_block: u64::MAX,
            },
        })
        .collect();
    ClosedBlock {
        block_number,
        l2_timestamp: 1_700_000_000 + block_number,
        end_tx_idx: BPosition::from_index((n as u64) * 64),
        l1_origin: 0,
        remote_epochs: Vec::new(),
        txs,
    }
}

fn expected_frames(blocks: &[ClosedBlock]) -> Vec<BlockFrame> {
    blocks
        .iter()
        .map(|b| BlockFrame {
            block_number: b.block_number,
            l2_timestamp: b.l2_timestamp,
            cursor: Some(BlockCursor {
                end_tx_idx: b.end_tx_idx.as_index(),
                l1_origin: b.l1_origin,
            }),
            remote_epochs: b.remote_epochs.clone(),
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
    let reconstructed = reconstruct(&batch.payload).unwrap();
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
    let reconstructed = reconstruct(&batch.payload).unwrap();
    assert_eq!(reconstructed, expected_frames(&blocks));
}

#[test]
fn roundtrip_five_blocks_grouped() {
    let blocks: Vec<ClosedBlock> = (10u64..15).map(|i| closed(i, 2)).collect();
    let cfg = BatcherConfig {
        blocks_per_batch: NonZeroUsize::new(5).unwrap(),
        ..Default::default()
    };
    let batch = pack_blocks(&cfg, &blocks).unwrap();
    assert_eq!(batch.l2_block_start, 10);
    assert_eq!(batch.l2_block_end, 14);
    let reconstructed = reconstruct(&batch.payload).unwrap();
    assert_eq!(reconstructed, expected_frames(&blocks));
}

// ---------------------------------------------------------------------------
// Remote-epoch (interop) DA representation: a record's messages travel by
// value, including calldata, so every chain is self-reconstructible with
// no dependency on a peer being alive.
// ---------------------------------------------------------------------------

/// Per-message calldata cap enforced by the origin Outbox
/// (`contracts/src/L2/Outbox.sol` `MAX_DATA_BYTES`).
const MAX_DATA_BYTES: usize = 65_536;

fn remote_epoch(
    origin: u64,
    first_seq: u64,
    inputs: &[&[u8]],
    with_callback: bool,
) -> kardamom_types::xchain::RemoteEpochRecord {
    use kardamom_types::xchain::{
        Callback, NonEmptyVec, RemoteEpochRecord, XChainMessage, remote_source_hash,
    };
    let mut built = inputs.iter().enumerate().map(|(i, input)| {
        let seq = first_seq + i as u64;
        XChainMessage {
            source_hash: remote_source_hash(origin, seq),
            seq,
            origin_sender: Address::repeat_byte(0xA1),
            target: Address::repeat_byte(0xB2),
            value: 0,
            gas_limit: 150_000,
            hops: 0,
            input: Bytes::copy_from_slice(input),
            callback: with_callback.then(|| Callback {
                target: Address::repeat_byte(0xCB),
                gas_limit: 90_000,
                context: B256::repeat_byte(0x42),
            }),
        }
    });
    let first = built.next().expect("fixture always carries a message");
    let messages = NonEmptyVec::new(first, built.collect());
    RemoteEpochRecord {
        origin_chain_id: origin,
        anchor_number: 100 + first_seq,
        anchor_hash: B256::repeat_byte(0x0B),
        first_seq,
        messages,
    }
}

/// Round-trip a batch whose middle block is led by TWO remote-epoch records
/// (a multi-message record with callbacks and a single-message one) — the
/// records must come back byte-identical, attributed to exactly the block
/// they lead, with the surrounding tx-only blocks untouched.
#[test]
fn roundtrip_remote_epochs_multi_message_record() {
    let mut blocks = vec![closed(7, 2), closed(8, 3), closed(9, 1)];
    blocks[1].remote_epochs = vec![
        remote_epoch(412_399, 5, &[&[0xCA, 0xFE], &[], &[0xBE; 33]], true),
        remote_epoch(412_400, 0, &[&[0x01]], false),
    ];

    let batch = pack_blocks(&BatcherConfig::default(), &blocks).unwrap();
    let reconstructed = reconstruct(&batch.payload).unwrap();
    assert_eq!(reconstructed, expected_frames(&blocks));
    assert_eq!(reconstructed[1].remote_epochs, blocks[1].remote_epochs);
    assert_eq!(reconstructed[0].remote_epochs.len(), 0);
    assert_eq!(reconstructed[2].remote_epochs.len(), 0);
}

/// A record whose messages carry the Outbox's `MAX_DATA_BYTES` calldata
/// cap: two such messages make a payload of several hundred kilobytes,
/// and it still round-trips byte-identically.
#[test]
fn roundtrip_max_size_messages() {
    let big_a = vec![0x5A; MAX_DATA_BYTES];
    let big_b = vec![0xA5; MAX_DATA_BYTES];
    let mut block = closed(3, 1);
    block.remote_epochs = vec![remote_epoch(412_399, 0, &[&big_a, &big_b], false)];
    let blocks = vec![block];

    // Uncompressed, so the payload size is the framed size (zstd would
    // collapse the repeated bytes).
    let cfg = BatcherConfig {
        compress: false,
        ..Default::default()
    };
    let batch = pack_blocks(&cfg, &blocks).unwrap();
    assert!(
        batch.payload.len() > 2 * MAX_DATA_BYTES,
        "two max-size messages must be in the payload whole (got {} bytes)",
        batch.payload.len()
    );
    let reconstructed = reconstruct(&batch.payload).unwrap();
    assert_eq!(reconstructed, expected_frames(&blocks));
    assert_eq!(
        reconstructed[0].remote_epochs[0]
            .messages
            .first()
            .input
            .len(),
        MAX_DATA_BYTES
    );
}

/// H7: the records commitment binds the remote epochs the batch posts. The
/// same batch with and without a record has a different commitment, and a
/// reader that recomputes the digest from the reconstructed frames gets
/// the posted commitment back.
#[test]
fn records_commitment_binds_remote_epochs() {
    const CHAIN_ID: u64 = 412_347;
    let cfg = BatcherConfig {
        chain_id: CHAIN_ID,
        ..Default::default()
    };
    let plain = vec![closed(7, 2), closed(8, 3)];
    let mut led = plain.clone();
    led[1].remote_epochs = vec![remote_epoch(412_346, 5, &[&[0xCA, 0xFE], &[]], true)];

    let plain_batch = pack_blocks(&cfg, &plain).unwrap();
    let led_batch = pack_blocks(&cfg, &led).unwrap();
    assert_ne!(
        plain_batch.records_commitment, led_batch.records_commitment,
        "a remote epoch must change the records commitment"
    );

    // The commitment binds the message content and the pair.
    let mut other_msg = led.clone();
    other_msg[1].remote_epochs[0].messages.first_mut().input = Bytes::from_static(&[0xCA, 0xFF]);
    assert_ne!(
        pack_blocks(&cfg, &other_msg).unwrap().records_commitment,
        led_batch.records_commitment
    );
    let other_chain = BatcherConfig {
        chain_id: CHAIN_ID + 1,
        ..Default::default()
    };
    assert_ne!(
        pack_blocks(&other_chain, &led).unwrap().records_commitment,
        led_batch.records_commitment
    );

    // Recompute from the reconstructed frames: remote epochs first, then txs.
    let frames = reconstruct(&led_batch.payload).unwrap();
    let recomputed = kardamom_types::batch_records_commitment(frames.iter().map(|f| {
        let mut d = kardamom_types::BlockRecordsDigest::new(f.block_number);
        for rec in &f.remote_epochs {
            d.add_remote_epoch(CHAIN_ID, rec);
        }
        for tx in &f.txs {
            d.add_tx(&tx.raw_tx);
        }
        d.finish()
    }));
    assert_eq!(recomputed, led_batch.records_commitment);
    assert_eq!(
        recomputed,
        kardamom_types::batch_records_commitment(
            led.iter()
                .map(|b| kardamom_batcher::batcher::block_records_digest(CHAIN_ID, b))
        )
    );
}

/// The accumulator attributes a remote-epoch record to the block it LEADS
/// (the record arrives after a boundary, before the next block's txs), and
/// the buffer drains — the next boundary carries none.
#[test]
fn accumulator_attributes_remote_epochs_to_the_block_they_lead() {
    use kardamom_batcher::batch::BatchAccumulator;
    use kardamom_types::BlockBoundaryStart;

    let mut acc = BatchAccumulator::new();
    let boundary = |n: u64| BlockBoundaryStart {
        block_number: n,
        l2_timestamp: 1_700_000_000 + n,
        end_tx_idx: BPosition::from_index(0),
        l1_origin: 0,
    };

    // Block 1 closes with no interop traffic.
    let b1 = acc.observe_boundary(&boundary(1));
    assert_eq!(b1.remote_epochs.len(), 0);

    // A record leads block 2: observed right after boundary 1, before the
    // block's txs.
    let rec = remote_epoch(412_399, 0, &[&[0xCA]], false);
    acc.observe_remote_epoch(rec.clone());
    let tx = closed(0, 1).txs.remove(0);
    acc.observe_tx(tx.envelope, tx.position);
    let b2 = acc.observe_boundary(&boundary(2));
    assert_eq!(b2.remote_epochs, vec![rec]);
    assert_eq!(b2.txs.len(), 1);

    // Drained: block 3 carries none.
    let b3 = acc.observe_boundary(&boundary(3));
    assert_eq!(b3.remote_epochs.len(), 0);
}

/// The recovery path reads a batch's payload by its certificate from the
/// DA proxy, which is what the batcher posted. A certificate the proxy
/// does not know is an error, not an empty batch.
#[tokio::test(flavor = "multi_thread")]
async fn recover_blocks_reads_the_posted_payload_by_certificate() {
    use kardamom_batcher::da::DaProxy;
    use kardamom_batcher::l1::{BatchDescriptor, recover_blocks};
    use kardamom_batcher::testkit_da::FakeDaProxy;

    let fake = FakeDaProxy::start();
    let da = DaProxy::new(fake.url()).unwrap();
    let blocks = vec![closed(1, 2), closed(2, 1)];
    let batch = pack_blocks(&BatcherConfig::default(), &blocks).unwrap();
    let da_cert = da.put(&batch.payload).await.unwrap();
    assert_eq!(da_cert, FakeDaProxy::cert_of(&batch.payload));

    let d = BatchDescriptor {
        index: 1,
        da_cert,
        l2_block_start: 1,
        l2_block_end: 2,
    };
    let recovered = recover_blocks(std::slice::from_ref(&d), &da).unwrap();
    assert_eq!(recovered, expected_frames(&blocks));

    let unknown = BatchDescriptor {
        da_cert: alloy_primitives::Bytes::from(vec![0x02, 0xBA, 0xD0]),
        ..d
    };
    let err = recover_blocks(&[unknown], &da).unwrap_err().to_string();
    assert!(err.contains("no such certificate"), "{err}");
}
