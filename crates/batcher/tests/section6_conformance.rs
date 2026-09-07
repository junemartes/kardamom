//! Section 6 conformance test: post to Anvil, then reconstruct from L1.
//!
//! An end-to-end exercise of the M+1 archive topology:
//!
//! 1. Build per-sequencer `tx_data` archives with full `TxEnvelope` bytes.
//! 2. Build the `tx_ordering` archive with the canonical `TxOrderingMessage`
//!    record stream (`TxRef` and `BoundaryStart`).
//! 3. Drive the offline `MultiArchiveReader`, `BatchAccumulator`, and
//!    `pack_blocks` pipeline to produce the same `PostedBatch` the
//!    lease-holder would broadcast.
//! 4. Deploy `KardamomL2Settlement` through the kardamom factory, and call
//!    `postBatch(prevBatchIndex, versionedHashes, l2BlockStart,
//!    l2BlockEnd)` from the batcher EOA directly, with stub versioned
//!    hashes instead of a real 4844 sidecar (`l1::post_batch` sends the
//!    real sidecar; this test's scope is the calldata path and the
//!    `BatchPosted` event, not sidecar broadcasting).
//! 5. Reconstruct the L2 stream from the locally held blob bytes, and
//!    check it matches the inputs. This is the same invariant a real
//!    L1-observer client would use to recover the L2 state.
//!
//! Skips gracefully if anvil is not installed (the same convention the
//! deployer and `anvil_e2e.rs` use). This test pins down the section 6
//! conformance invariant under the M+1 input topology: split data and
//! ordering must round-trip through the batcher exactly as the pre-split
//! single-archive layout did.

use alloy_primitives::{Address, B256, address};
use alloy_sol_types::SolEvent;
use kardamom_batcher::batcher::{Batcher, BatcherConfig, MockSender, PostedBatch, pack_blocks};
use kardamom_batcher::multi_archive_reader::{
    MultiArchiveConfig, MultiArchiveReader, ResolvedRecord,
};
use kardamom_batcher::recon::reconstruct;
use kardamom_batcher::settlement::IKardamomL2Settlement;
use kardamom_batcher::testkit::{MPlusOneArchives, write_m_plus_one_archives};
use kardamom_deployer::testkit::AnvilRig;
use kardamom_deployer::{ContractId, Deployer, Op, encode_address_arg};
use tempfile::TempDir;

const DEV_OWNER: Address = address!("00000000000000000000000000000000DEAD0001");
const BATCHER: Address = address!("0000000000000000000000000000000000000BA7");
const L2_CHAIN_ID: u64 = 42;

/// Check `txs`' correlation ids match `want`, in order. `want` is the
/// canonical order [`write_m_plus_one_archives`] returned for the fixture
/// under test.
fn assert_canonical_order(txs: &[kardamom_batcher::frame::TxFrame], want: &[u64]) {
    let got: Vec<u64> = txs.iter().map(|tx| tx.correlation_id).collect();
    assert_eq!(got, want, "canonical order mismatch");
}

/// Drive the offline `MultiArchiveReader` -> `BatchAccumulator` ->
/// `pack_blocks` pipeline over the M+1 archives, checking along the way
/// that each closed block reconstructs to the canonical tx order. Returns
/// the one batch the fixture's single boundary produces.
fn drive_batcher_pipeline(archives: &MPlusOneArchives, cfg: &BatcherConfig) -> PostedBatch {
    let mut batcher = Batcher::new(cfg.clone(), MockSender::default());
    let reader = MultiArchiveReader::open(&MultiArchiveConfig {
        b_segment: archives.b_segment.clone(),
        a_segments: archives.a_segments.clone(),
    })
    .expect("open multi-archive reader");
    assert_eq!(
        reader.a_archive_count(),
        2,
        "test fixture should populate two A archives"
    );

    for rec in reader {
        match rec.expect("decode") {
            ResolvedRecord::Tx { position, env, .. } => {
                batcher.accumulator().observe_tx(env, position);
            }
            ResolvedRecord::RemoteEpoch { record, .. } => {
                batcher.accumulator().observe_remote_epoch(record);
            }
            ResolvedRecord::Boundary { marker, .. } => {
                let closed = batcher.accumulator().observe_boundary(&marker);
                let pack = pack_blocks(cfg, std::slice::from_ref(&closed)).expect("pack");

                // Reconstruct locally from the blobs just packed. This
                // mirrors what a section 6 L1-observer client does after
                // downloading sidecar bytes from the beacon node.
                let reconstructed = reconstruct(&pack.blobs).expect("reconstruct");
                assert_eq!(reconstructed.len(), 1);
                let block = &reconstructed[0];
                assert_eq!(block.block_number, 42);
                assert_eq!(
                    block.txs.len(),
                    archives.canonical_order.len(),
                    "resolved tx count should match the fixture"
                );
                assert_canonical_order(&block.txs, &archives.canonical_order);

                batcher.on_closed_block(closed).expect("on_closed");
            }
        }
    }

    assert_eq!(batcher.sender().sent.len(), 1, "exactly one batch posted");
    let posted = batcher.sender().sent[0].clone();
    assert_eq!(posted.l2_block_start, 42);
    assert_eq!(posted.l2_block_end, 42);
    assert!(
        !posted.blobs.is_empty(),
        "batch must contain at least one blob"
    );
    posted
}

/// Deploy `KardamomL2Settlement`, post `posted` with stub versioned
/// hashes, and check the `BatchPosted` event. `None` if anvil is
/// unavailable — the caller skips.
///
/// Deterministic stub versioned hashes stand in for the real 4844
/// sidecar's KZG-derived ones: the real broadcast path uses
/// `alloy-consensus::BlobTransactionSidecar`, which needs the trusted
/// setup to compute, out of scope here. The contract only stores and
/// emits the hashes; it never opens the blob bytes, so a stub is enough to
/// exercise the post-batch and event-emission path.
async fn post_batch_to_anvil_and_verify_event(posted: &PostedBatch) -> Option<()> {
    let rig = AnvilRig::spawn(&[DEV_OWNER, BATCHER]).await?;
    let provider = rig.provider;

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

    let versioned_hashes: Vec<B256> = (0..posted.blobs.len())
        .map(|i| B256::repeat_byte(0xA0 + u8::try_from(i).unwrap()))
        .collect();
    assert_eq!(versioned_hashes.len(), posted.blobs.len());

    let settlement = IKardamomL2Settlement::new(settlement_addr, provider.clone());
    let receipt = settlement
        .postBatch(
            0,
            versioned_hashes.clone(),
            posted.l2_block_start,
            posted.l2_block_end,
            posted.records_commitment,
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
    let log = receipt
        .logs()
        .iter()
        .find(|log| {
            log.address() == settlement_addr
                && log.topic0().copied().unwrap_or(B256::ZERO) == topic0
        })
        .expect("BatchPosted event missing");
    let log_topics = log.topics();
    assert_eq!(log_topics.len(), 2, "BatchPosted indexes only batchIndex");
    let idx = u64::from_be_bytes(log_topics[1].as_slice()[24..32].try_into().unwrap());
    assert_eq!(idx, 1, "first post advances index 0 -> 1");
    Some(())
}

/// Reconstruct `posted`'s locally-held blob bytes and check the result
/// matches the canonical input order. In production, a section 6 observer
/// fetches the blob bytes from the L1 beacon node by versioned hash; this
/// test uses the bytes it already has, because its scope is the offline,
/// on-chain, and reconstruct invariant, not beacon-node integration.
fn assert_reconstructs_to_canonical_order(posted: &PostedBatch, canonical: &[u64]) {
    let reconstructed = reconstruct(&posted.blobs).expect("reconstruct posted batch");
    assert_eq!(reconstructed.len(), 1);
    assert_canonical_order(&reconstructed[0].txs, canonical);
}

#[tokio::test]
async fn section6_conformance_m_plus_one_to_l1_and_back() {
    // ----- setup: build the M+1 archives -----
    let dir = TempDir::new().expect("tempdir");
    let archives = write_m_plus_one_archives(dir.path(), 3, 42, 1_700_000_042);

    // ----- act + assert: offline pipeline (archives -> pack -> reconstruct) -----
    let cfg = BatcherConfig::default();
    let posted = drive_batcher_pipeline(&archives, &cfg);

    // ----- act + assert: post to L1, check the event -----
    if post_batch_to_anvil_and_verify_event(&posted)
        .await
        .is_none()
    {
        eprintln!("SKIP: anvil unavailable; offline portion of §6 conformance still asserted");
        return;
    }

    // ----- assert: reconstruct from the locally-held blob bytes -----
    assert_reconstructs_to_canonical_order(&posted, &archives.canonical_order);
}
