//! Reconstruction round-trip: encode, pack, unpack, then decode should
//! yield the original block frames. This mirrors the section 6 conformance
//! hook.

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

// ---------------------------------------------------------------------------
// Remote-epoch (interop) DA representation — spec §16 Q8.
// ---------------------------------------------------------------------------

/// Per-message calldata cap enforced by the origin Outbox
/// (`contracts/src/L2/Outbox.sol` MAX_DATA_BYTES).
const MAX_DATA_BYTES: usize = 65_536;

fn remote_epoch(
    origin: u64,
    first_seq: u64,
    inputs: &[&[u8]],
    with_callback: bool,
) -> kardamom_types::xchain::RemoteEpochRecord {
    use kardamom_types::xchain::{Callback, RemoteEpochRecord, XChainMessage, remote_source_hash};
    let messages = inputs
        .iter()
        .enumerate()
        .map(|(i, input)| {
            let seq = first_seq + i as u64;
            XChainMessage {
                source_hash: remote_source_hash(origin, seq),
                seq,
                origin_sender: Address::repeat_byte(0xA1),
                target: Address::repeat_byte(0xB2),
                value: 0,
                gas_limit: 150_000,
                input: Bytes::copy_from_slice(input),
                callback: with_callback.then(|| Callback {
                    target: Address::repeat_byte(0xCB),
                    gas_limit: 90_000,
                    context: B256::repeat_byte(0x42),
                }),
            }
        })
        .collect();
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
    let reconstructed = reconstruct(&batch.blobs).unwrap();
    assert_eq!(reconstructed, expected_frames(&blocks));
    assert_eq!(reconstructed[1].remote_epochs, blocks[1].remote_epochs);
    assert!(reconstructed[0].remote_epochs.is_empty());
    assert!(reconstructed[2].remote_epochs.is_empty());
}

/// A record whose messages carry the Outbox's MAX_DATA_BYTES calldata cap:
/// two such messages exceed one blob's 126 976 usable bytes, so the payload
/// must span blobs — the SAME multi-blob mechanism an oversized tx batch uses
/// (`pack_to_blobs` slicing, `pack_blocks`' 6-blob ceiling as the guard) —
/// and still round-trip byte-identically.
#[test]
fn roundtrip_max_size_messages_span_blobs() {
    let big_a = vec![0x5A; MAX_DATA_BYTES];
    let big_b = vec![0xA5; MAX_DATA_BYTES];
    let mut block = closed(3, 1);
    block.remote_epochs = vec![remote_epoch(412_399, 0, &[&big_a, &big_b], false)];
    let blocks = vec![block];

    // Uncompressed, so the payload size is the framed size and the blob
    // spanning is deterministic (zstd would collapse the repeated bytes).
    let cfg = BatcherConfig {
        compress: false,
        ..Default::default()
    };
    let batch = pack_blocks(&cfg, &blocks).unwrap();
    assert!(
        batch.blobs.len() >= 2,
        "two max-size messages must overflow a single blob (got {} blob(s))",
        batch.blobs.len()
    );
    let reconstructed = reconstruct(&batch.blobs).unwrap();
    assert_eq!(reconstructed, expected_frames(&blocks));
    assert_eq!(
        reconstructed[0].remote_epochs[0].messages[0].input.len(),
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
    other_msg[1].remote_epochs[0].messages[0].input = Bytes::from_static(&[0xCA, 0xFF]);
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
    let frames = reconstruct(&led_batch.blobs).unwrap();
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
        end_tx_idx: pos(0),
        l1_origin: 0,
    };

    // Block 1 closes with no interop traffic.
    let b1 = acc.observe_boundary(boundary(1));
    assert!(b1.remote_epochs.is_empty());

    // A record leads block 2: observed right after boundary 1, before the
    // block's txs.
    let rec = remote_epoch(412_399, 0, &[&[0xCA]], false);
    acc.observe_remote_epoch(rec.clone());
    let tx = closed(0, 1).txs.remove(0);
    acc.observe_tx(tx.envelope, tx.position);
    let b2 = acc.observe_boundary(boundary(2));
    assert_eq!(b2.remote_epochs, vec![rec]);
    assert_eq!(b2.txs.len(), 1);

    // Drained: block 3 carries none.
    let b3 = acc.observe_boundary(boundary(3));
    assert!(b3.remote_epochs.is_empty());
}

/// A DA store that returns wrong bytes of the right length must be caught
/// before reconstruction, not silently rebuilt into a wrong chain. The
/// versioned hash is only a filename to the store. L1's commitment is the
/// authority, so `recover_blocks` recomputes it.
#[test]
fn corrupted_da_blob_is_rejected_against_its_commitment() {
    use kardamom_batcher::da_store::{BlobSource, FsBlobStore};
    use kardamom_batcher::l1::{BatchDescriptor, recover_blocks, verify_blob_against_hash};

    let dir = tempfile::tempdir().unwrap();
    let store = FsBlobStore::open(dir.path()).unwrap();

    // Pack a real payload and register it under its true versioned hash.
    // Use the same helper the post path uses to derive what L1 commits to.
    let blobs = kardamom_batcher::blob::pack_to_blobs(b"kardamom da integrity").unwrap();
    let sidecar = kardamom_batcher::l1::build_sidecar(blobs.clone()).unwrap();
    let vh = sidecar.versioned_hashes().next().unwrap();
    store.put(vh, &blobs[0]).unwrap();

    // Honest bytes verify, and reconstruction proceeds.
    let good = store.fetch_blob(vh).unwrap();
    verify_blob_against_hash(vh, &good).expect("untouched blob must verify");

    // Now corrupt the stored bytes in place, keeping the length the same.
    // This is exactly the shape a size check cannot see. Field elements
    // keep their high byte zero (BLS modulus), so flip a low byte inside
    // the payload region.
    let mut corrupt = good;
    corrupt[1234] ^= 0x01;
    store.put(vh, &corrupt).unwrap();

    let err = verify_blob_against_hash(vh, &store.fetch_blob(vh).unwrap())
        .expect_err("corrupted blob must NOT verify against its commitment");
    let msg = format!("{err}");
    assert!(
        msg.contains("does not match its commitment"),
        "unexpected error: {msg}"
    );

    // And the recovery path itself must refuse, rather than decode garbage.
    let d = BatchDescriptor {
        index: 1,
        versioned_hashes: vec![vh],
        l2_block_start: 1,
        l2_block_end: 1,
    };
    let err = recover_blocks(&[d], &store).expect_err("recover_blocks must reject a corrupt blob");
    assert!(format!("{err}").contains("does not match its commitment"));
}
