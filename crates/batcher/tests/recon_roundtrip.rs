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
