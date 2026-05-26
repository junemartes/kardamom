//! §6 conformance test (post to Anvil → reconstruct from L1).
//!
//! End-to-end exercise of the **new M+1 archive topology**:
//!
//! 1. Build per-sequencer tx_data archives with full `TxEnvelope` bytes.
//! 2. Build the tx_ordering archive with the canonical `TxOrderingMessage`
//!    record stream (`TxRef + BoundaryStart`).
//! 3. Drive the offline `MultiArchiveReader` → `BatchAccumulator` →
//!    `pack_blocks` pipeline to produce the same `PostedBatch` the
//!    lease-holder would broadcast.
//! 4. Deploy `KardamomL2Settlement` via the kardamom factory and call
//!    `postBatch(prevBatchIndex, versionedHashes, l2BlockStart,
//!    l2BlockEnd)` from the batcher EOA. The blob bytes themselves are
//!    *not* sent to anvil (4844 sidecar broadcasting is a future task);
//!    however the calldata path and the `BatchPosted` event are exercised
//!    against the live contract.
//! 5. Reconstruct the L2 stream from the locally-held blob bytes and assert
//!    it matches the inputs — same invariant a real L1-observer client would
//!    use to recover the L2 state.
//!
//! Skips gracefully if anvil is not installed (same convention the deployer
//! and `anvil_e2e.rs` use). The test exists to pin down the §6 conformance
//! invariant under the  input topology: split data and ordering
//! must round-trip through the batcher exactly the same as the pre-split
//! single-archive layout did.

use std::collections::HashMap;
use std::io::Write;

use alloy_node_bindings::Anvil;
use alloy_primitives::{Address, B256, U256, address};
use alloy_provider::{Provider, ProviderBuilder};
use alloy_sol_types::SolEvent;
use batcher::archive_reader::append_frame;
use batcher::batcher::{Batcher, BatcherConfig, MockSender, pack_blocks};
use batcher::multi_archive_reader::{MultiArchiveConfig, MultiArchiveReader, ResolvedRecord};
use batcher::recon::reconstruct;
use batcher::settlement::IKardamomL2Settlement;
use bytes::Bytes;
use kardamom_deployer::addresses::{ERC7955_FACTORY, ERC7955_RUNTIME_HEX};
use kardamom_deployer::{ContractId, Deployer, Op, encode_address_arg};
use tempfile::TempDir;
use types::{BPosition, BlockBoundaryStart, TxEnvelope, TxOrderingMessage, TxRef};

const DEV_OWNER: Address = address!("00000000000000000000000000000000DEAD0001");
const BATCHER: Address = address!("0000000000000000000000000000000000000BA7");
const L2_CHAIN_ID: u64 = 42;

fn pos(o: i32) -> BPosition {
    BPosition {
        term_id: 0,
        term_offset: o,
    }
}

/// Write the M+1 synthetic archives that emulate a  sequencer:
/// 2 sequencers, 3 txs each, 1 boundary on B.
fn write_archives(dir: &TempDir) -> (std::path::PathBuf, HashMap<u8, std::path::PathBuf>) {
    // Per-sequencer envelopes.
    let envs_a0: Vec<TxEnvelope> = (0..3)
        .map(|i| TxEnvelope {
            correlation_id: 1000 + i,
            raw_tx: Bytes::from(vec![0xAA; 80]),
            sender: Address::repeat_byte(0x10),
            tx_hash: B256::repeat_byte(0x10 + i as u8),
        })
        .collect();
    let envs_a1: Vec<TxEnvelope> = (0..3)
        .map(|i| TxEnvelope {
            correlation_id: 2000 + i,
            raw_tx: Bytes::from(vec![0xBB; 80]),
            sender: Address::repeat_byte(0x20),
            tx_hash: B256::repeat_byte(0x20 + i as u8),
        })
        .collect();

    let mut a0_buf = Vec::new();
    let mut a1_buf = Vec::new();
    let a_positions = [pos(0), pos(256), pos(512)];
    for (i, e) in envs_a0.iter().enumerate() {
        append_frame(&mut a0_buf, a_positions[i], e);
    }
    for (i, e) in envs_a1.iter().enumerate() {
        append_frame(&mut a1_buf, a_positions[i], e);
    }

    let a0_path = dir.path().join("a0.rec");
    std::fs::File::create(&a0_path)
        .unwrap()
        .write_all(&a0_buf)
        .unwrap();
    let a1_path = dir.path().join("a1.rec");
    std::fs::File::create(&a1_path)
        .unwrap()
        .write_all(&a1_buf)
        .unwrap();

    // Canonical order on B: round-robin a0/a1 for 6 refs, then a boundary.
    let mut b_buf = Vec::new();
    let mut b_off = 0i32;
    // Use distinct tx_hashes per ref so the executor's dedup doesn't
    // collapse them; this loop drives an offline-reader test so the
    // executor isn't involved, but keep them distinct for hygiene.
    let mut hash_seed: u8 = 0;
    for a_pos in &a_positions {
        hash_seed = hash_seed.wrapping_add(1);
        append_frame(
            &mut b_buf,
            pos(b_off),
            &TxOrderingMessage::TxRef(TxRef::new(
                alloy_primitives::B256::repeat_byte(hash_seed),
                0,
                *a_pos,
            )),
        );
        b_off += 16;
        hash_seed = hash_seed.wrapping_add(1);
        append_frame(
            &mut b_buf,
            pos(b_off),
            &TxOrderingMessage::TxRef(TxRef::new(
                alloy_primitives::B256::repeat_byte(hash_seed),
                1,
                *a_pos,
            )),
        );
        b_off += 16;
    }
    let boundary_pos = pos(b_off);
    append_frame(
        &mut b_buf,
        boundary_pos,
        &TxOrderingMessage::BoundaryStart(BlockBoundaryStart {
            block_number: 42,
            end_tx_idx: boundary_pos,
            l2_timestamp: 1_700_000_042,
        }),
    );
    let b_path = dir.path().join("b.rec");
    std::fs::File::create(&b_path)
        .unwrap()
        .write_all(&b_buf)
        .unwrap();

    let mut a_map = HashMap::new();
    a_map.insert(0u8, a0_path);
    a_map.insert(1u8, a1_path);
    (b_path, a_map)
}

async fn setup_anvil_with_erc7955() -> Option<(
    alloy_node_bindings::AnvilInstance,
    impl alloy_provider::Provider + Clone,
)> {
    let anvil = Anvil::new().try_spawn().ok()?;
    let provider = ProviderBuilder::new()
        .disable_recommended_fillers()
        .connect_http(anvil.endpoint_url());

    let bytes_hex = format!("0x{ERC7955_RUNTIME_HEX}");
    let _: serde_json::Value = provider
        .raw_request("anvil_setCode".into(), (ERC7955_FACTORY, bytes_hex))
        .await
        .ok()?;
    let _: serde_json::Value = provider
        .raw_request(
            "anvil_setBalance".into(),
            (DEV_OWNER, U256::from(1_000_000_000_000_000_000_000u128)),
        )
        .await
        .ok()?;
    let _: serde_json::Value = provider
        .raw_request(
            "anvil_setBalance".into(),
            (BATCHER, U256::from(1_000_000_000_000_000_000_000u128)),
        )
        .await
        .ok()?;
    let _: serde_json::Value = provider
        .raw_request("anvil_impersonateAccount".into(), (DEV_OWNER,))
        .await
        .ok()?;
    let _: serde_json::Value = provider
        .raw_request("anvil_impersonateAccount".into(), (BATCHER,))
        .await
        .ok()?;

    Some((anvil, provider))
}

#[tokio::test]
async fn section6_conformance_m_plus_one_to_l1_and_back() {
    // ----- Stage 1: build the M+1 archives. -----
    let dir = TempDir::new().expect("tempdir");
    let (b_segment, a_segments) = write_archives(&dir);

    // ----- Stage 2: drive the batcher pipeline. -----
    let cfg = BatcherConfig::default();
    let mut batcher = Batcher::new(cfg.clone(), MockSender::default());

    let reader = MultiArchiveReader::open(&MultiArchiveConfig {
        b_segment,
        a_segments,
    })
    .expect("open multi-archive reader");
    assert_eq!(
        reader.a_archive_count(),
        2,
        "test fixture should populate two A archives"
    );

    let mut produced_batches = Vec::new();
    for rec in reader {
        match rec.expect("decode") {
            ResolvedRecord::Tx { position, env, .. } => {
                batcher.accumulator().observe_tx(env, position);
            }
            ResolvedRecord::Boundary { marker, .. } => {
                let closed = batcher.accumulator().observe_boundary(marker);
                let pack = pack_blocks(&cfg, std::slice::from_ref(&closed)).expect("pack");
                // Reconstruct *locally* from the blobs we just packed —
                // mirrors what a §6 L1-observer client does after
                // downloading sidecar bytes from the beacon node.
                let reconstructed = reconstruct(&pack.blobs).expect("reconstruct");
                assert_eq!(reconstructed.len(), 1);
                let block = &reconstructed[0];
                assert_eq!(block.block_number, 42);
                assert_eq!(block.txs.len(), 6, "6 resolved txs in the block");

                // Canonical alternation: a0[0], a1[0], a0[1], a1[1], a0[2], a1[2].
                let want = [1000u64, 2000, 1001, 2001, 1002, 2002];
                for (i, tx) in block.txs.iter().enumerate() {
                    assert_eq!(
                        tx.correlation_id, want[i],
                        "tx {i} canonical order mismatch"
                    );
                }

                produced_batches.push(pack.clone());
                batcher.on_closed_block(closed).expect("on_closed");
            }
        }
    }

    assert_eq!(batcher.sender().sent.len(), 1, "exactly one batch posted");
    let posted = &batcher.sender().sent[0];
    assert_eq!(posted.l2_block_start, 42);
    assert_eq!(posted.l2_block_end, 42);
    assert!(
        !posted.blobs.is_empty(),
        "batch must contain at least one blob"
    );

    // ----- Stage 3: post-batch to anvil + verify event. -----
    let Some((anvil, provider)) = setup_anvil_with_erc7955().await else {
        eprintln!("SKIP: anvil unavailable; offline portion of §6 conformance still asserted");
        return;
    };
    let _ = anvil; // keep alive

    let deployer = Deployer::new(provider.clone(), DEV_OWNER);
    deployer.ensure_factory(DEV_OWNER).await.unwrap();
    deployer
        .apply(
            &[Op::Deploy {
                l2_chain_id: L2_CHAIN_ID,
                id: ContractId::KardamomL2Settlement,
                init_args: encode_address_arg(BATCHER),
            }],
            DEV_OWNER,
        )
        .await
        .expect("deploy KardamomL2Settlement");
    let entries = deployer.addresses(Some(L2_CHAIN_ID)).await.unwrap();
    assert_eq!(entries.len(), 1);
    let settlement_addr = entries[0].proxy;

    // Deterministic stub versioned hashes — the real broadcast path uses
    // `alloy-consensus::BlobTransactionSidecar` which derives KZG commitments
    // from the blobs and then computes the versioned hash. Computing KZG
    // commitments requires the trusted setup; out of scope for this test.
    // The contract only stores the hashes and emits them — it never opens
    // the blob bytes — so a stub is sufficient to exercise the
    // post-batch / event-emission path.
    let versioned_hashes: Vec<B256> = (0..posted.blobs.len())
        .map(|i| B256::repeat_byte(0xA0 + i as u8))
        .collect();
    assert_eq!(versioned_hashes.len(), posted.blobs.len());

    let settlement = IKardamomL2Settlement::new(settlement_addr, provider.clone());
    let receipt = settlement
        .postBatch(
            0,
            versioned_hashes.clone(),
            posted.l2_block_start,
            posted.l2_block_end,
        )
        .from(BATCHER)
        .send()
        .await
        .expect("send postBatch")
        .get_receipt()
        .await
        .expect("await receipt");
    assert!(receipt.status(), "postBatch must succeed");

    let topic0 = IKardamomL2Settlement::BatchPosted::SIGNATURE_HASH;
    let mut found = false;
    for log in receipt.logs() {
        if log.address() == settlement_addr && log.topic0().copied().unwrap_or(B256::ZERO) == topic0
        {
            found = true;
            let topics = log.topics();
            assert_eq!(topics.len(), 2, "BatchPosted indexes only batchIndex");
            let idx = u64::from_be_bytes(topics[1].as_slice()[24..32].try_into().unwrap());
            assert_eq!(idx, 1, "first post advances index 0 -> 1");
        }
    }
    assert!(found, "BatchPosted event missing");

    // ----- Stage 4: reconstruct from the locally-held blob bytes. -----
    // In production a §6 observer fetches the blob bytes from the L1 beacon
    // node by versioned hash; here we use the bytes we already have since
    // the test scope is the offline → on-chain → reconstruct invariant, not
    // beacon-node integration.
    let reconstructed = reconstruct(&posted.blobs).expect("reconstruct posted batch");
    assert_eq!(reconstructed.len(), 1);
    let want = [1000u64, 2000, 1001, 2001, 1002, 2002];
    for (i, tx) in reconstructed[0].txs.iter().enumerate() {
        assert_eq!(
            tx.correlation_id, want[i],
            "reconstructed tx {i} must match the canonical input"
        );
    }
}
