//! E2E test scaffold: real Aeron Media Driver + Aeron Archive in Docker via
//! the [`log::testing::AeronTestCluster`] harness from S3.
//!
//!— mock-based unit and integration tests in this crate stay;
//! this is *additional* coverage that brings up the real Aeron container so we
//! catch wire-format / IPC / back-pressure bugs the in-process reader cannot
//! surface.
//!
//! Gated behind `feature = "docker-e2e"` because it requires a Docker daemon
//! and ~30s startup; default `cargo test` skips it.
//!
//! ** scope:** brings up the Aeron container, writes synthetic
//! segment files in the canonical KAR1-internal frame format that the
//! batcher's offline `TypedSegmentReader` consumes:
//!   - one **tx_ordering** archive carrying `TxOrderingMessage` records
//!     (`TxRef + BoundaryStart`); and
//!   - one or more **per-sequencer tx_data** archives carrying the full
//!     `TxEnvelope` records the refs point at.
//!
//! Then it runs the full batcher pipeline through the
//! [`MultiArchiveReader`] (read → resolve → accumulate → pack → reconstruct
//! → assert blobs), and asserts a `BatchPosted`-shaped `PostBatchParams`
//! could be assembled.
//!
//! The full path "publish to tx_ordering / tx_data[i] with rusteron →
//! recorder writes Aeron-native segment frames → batcher decodes via
//! rusteron-archive replay protocol" lands when the high-level archive
//! wrappers ship from `log`. The harness assertion below is the
//! gating proof that this crate's test target can reach the Aeron container,
//! which is the prerequisite that wrapper landing unblocks.

#![cfg(feature = "docker-e2e")]

use std::collections::HashMap;
use std::io::Write;

use alloy_primitives::{Address, B256};
use batcher::archive_reader::append_frame;
use batcher::batcher::{Batcher, BatcherConfig, MockSender, pack_blocks};
use batcher::multi_archive_reader::{MultiArchiveConfig, MultiArchiveReader, ResolvedRecord};
use batcher::recon::reconstruct;
use bytes::Bytes;
use log::testing::AeronTestCluster;
use tempfile::TempDir;
use types::{BPosition, BlockBoundaryStart, TxEnvelope, TxOrderingMessage, TxRef};

fn pos(o: i32) -> BPosition {
    BPosition {
        term_id: 0,
        term_offset: o,
    }
}

/// Build a 2-sequencer M+1 archive set: A0, A1, B.
///
/// 4 txs alternate between sequencer 0 and 1; B records the canonical order
/// (the alternation) and closes the block with a `BoundaryStart`.
fn write_synthetic_archives(
    dir: &TempDir,
) -> (std::path::PathBuf, HashMap<u8, std::path::PathBuf>) {
    // 2 envelopes per sequencer on its own A archive.
    let mut a0_buf = Vec::new();
    let mut a1_buf = Vec::new();
    let envs_a0 = [
        TxEnvelope {
            correlation_id: 100,
            raw_tx: Bytes::from(vec![0xAB; 64]),
            sender: Address::repeat_byte(0x10),
            tx_hash: B256::repeat_byte(0x10),
        },
        TxEnvelope {
            correlation_id: 101,
            raw_tx: Bytes::from(vec![0xCD; 64]),
            sender: Address::repeat_byte(0x11),
            tx_hash: B256::repeat_byte(0x11),
        },
    ];
    let envs_a1 = [
        TxEnvelope {
            correlation_id: 200,
            raw_tx: Bytes::from(vec![0x12; 64]),
            sender: Address::repeat_byte(0x20),
            tx_hash: B256::repeat_byte(0x20),
        },
        TxEnvelope {
            correlation_id: 201,
            raw_tx: Bytes::from(vec![0x34; 64]),
            sender: Address::repeat_byte(0x21),
            tx_hash: B256::repeat_byte(0x21),
        },
    ];
    append_frame(&mut a0_buf, pos(0), &envs_a0[0]);
    append_frame(&mut a0_buf, pos(128), &envs_a0[1]);
    append_frame(&mut a1_buf, pos(0), &envs_a1[0]);
    append_frame(&mut a1_buf, pos(128), &envs_a1[1]);

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

    // TxOrdering: refs alternate sequencers in the canonical order, then a
    // single boundary closes the block.
    let mut b_buf = Vec::new();
    append_frame(
        &mut b_buf,
        pos(0),
        &TxOrderingMessage::TxRef(TxRef::new(
            alloy_primitives::B256::repeat_byte(0x01),
            0,
            pos(0),
        )),
    );
    append_frame(
        &mut b_buf,
        pos(16),
        &TxOrderingMessage::TxRef(TxRef::new(
            alloy_primitives::B256::repeat_byte(0x02),
            1,
            pos(0),
        )),
    );
    append_frame(
        &mut b_buf,
        pos(32),
        &TxOrderingMessage::TxRef(TxRef::new(
            alloy_primitives::B256::repeat_byte(0x03),
            0,
            pos(128),
        )),
    );
    append_frame(
        &mut b_buf,
        pos(48),
        &TxOrderingMessage::TxRef(TxRef::new(
            alloy_primitives::B256::repeat_byte(0x04),
            1,
            pos(128),
        )),
    );
    append_frame(
        &mut b_buf,
        pos(64),
        &TxOrderingMessage::BoundaryStart(BlockBoundaryStart {
            block_number: 1,
            end_tx_idx: pos(64),
            l2_timestamp: 1_700_000_000,
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker; run with `cargo test -p kardamom-batcher --features docker-e2e -- --ignored`"]
async fn aeron_cluster_starts_and_batcher_round_trips_the_m_plus_one_topology() {
    // 1. Bring up the real Aeron container (Media Driver + Archive).
    let cluster = AeronTestCluster::single_node()
        .await
        .expect("aeron container should start");
    assert_eq!(cluster.len(), 1);
    let archive_endpoint = cluster.archive_control_endpoint(0).await;
    assert!(
        archive_endpoint.starts_with("127.0.0.1:"),
        "unexpected archive endpoint {archive_endpoint}"
    );

    // 2. Write the M+1 synthetic archives using the batcher's own
    //    KAR1-internal frame writer. The full "publish via Aeron →
    //    SegmentReader" round-trip waits on the high-level tx_ordering /
    //    tx_data archive wrappers in `log`.
    let dir = TempDir::new().unwrap();
    let (b_segment, a_segments) = write_synthetic_archives(&dir);

    // 3. Open the M-archive reader and drive the batcher pipeline:
    //    walk B → resolve refs against per-A indexes → accumulate → pack →
    //    reconstruct.
    let cfg = BatcherConfig::default();
    let mut batcher = Batcher::new(cfg.clone(), MockSender::default());

    let reader = MultiArchiveReader::open(&MultiArchiveConfig {
        b_segment,
        a_segments,
    })
    .expect("open multi archive reader");
    assert_eq!(reader.a_archive_count(), 2);

    for rec in reader {
        match rec.expect("decode") {
            ResolvedRecord::Tx { position, env, .. } => {
                batcher.accumulator().observe_tx(env, position);
            }
            ResolvedRecord::Boundary { marker, .. } => {
                let closed = batcher.accumulator().observe_boundary(marker);
                let pack = pack_blocks(&cfg, std::slice::from_ref(&closed)).expect("pack");
                let reconstructed =
                    reconstruct(&pack.blobs).expect("reconstruct round-trips the pipeline");
                assert_eq!(reconstructed.len(), 1);
                assert_eq!(reconstructed[0].block_number, 1);
                assert_eq!(
                    reconstructed[0].txs.len(),
                    4,
                    "block must contain 4 resolved txs"
                );
                // Canonical alternation: a0[0], a1[0], a0[1], a1[1].
                let want = [100u64, 200, 101, 201];
                for (i, tx) in reconstructed[0].txs.iter().enumerate() {
                    assert_eq!(tx.correlation_id, want[i]);
                }
                batcher.on_closed_block(closed).expect("post");
            }
        }
    }

    assert_eq!(
        batcher.sender().sent.len(),
        1,
        "exactly one batch should have been forwarded to the sender"
    );

    drop(cluster);
}
