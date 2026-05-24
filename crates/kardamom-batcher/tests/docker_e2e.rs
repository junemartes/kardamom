//! E2E test scaffold: real Aeron Media Driver + Aeron Archive in Docker via
//! the [`kardamom_log::testing::AeronTestCluster`] harness from S3.
//!
//! Per S0 D-Sh8 — mock-based unit and integration tests in this crate stay;
//! this is *additional* coverage that brings up the real Aeron container so we
//! catch wire-format / IPC / back-pressure bugs the in-process reader cannot
//! surface.
//!
//! Gated behind `feature = "docker-e2e"` because it requires a Docker daemon
//! and ~30s startup; default `cargo test` skips it.
//!
//! **v0 scope:** brings up the Aeron container, writes a *synthetic* segment
//! file in the canonical KAR1-internal frame format that the batcher's
//! offline `SegmentReader` consumes, runs the full batcher pipeline (read →
//! accumulate → pack → assert blobs), and asserts a `BatchPosted`-shaped
//! `PostBatchParams` could be assembled.
//!
//! The full path "publish to channel B with rusteron → recorder writes
//! Aeron-native segment frames → batcher decodes via rusteron-archive replay
//! protocol" lands when the high-level `ChannelBArchive` wrapper ships from
//! `kardamom-log` (same caveat as S6's docker_e2e). The harness assertion
//! below is the gating proof that this crate's test target can reach the
//! Aeron container, which is the prerequisite that wrapper landing unblocks.

#![cfg(feature = "docker-e2e")]

use std::io::Write;

use alloy_primitives::{Address, B256};
use bytes::Bytes;
use kardamom_batcher::archive_reader::{
    STREAM_KIND_BOUNDARY, STREAM_KIND_TX, SegmentReader, SegmentRecord, append_frame,
};
use kardamom_batcher::batcher::{Batcher, BatcherConfig, MockSender, pack_blocks};
use kardamom_batcher::recon::reconstruct;
use kardamom_leases::{Lease, LeaseConfig};
use kardamom_log::testing::AeronTestCluster;
use kardamom_types::{BPosition, BlockBoundaryStart, FsyncWatermark, QuorumWatermark, TxEnvelope};
use tempfile::NamedTempFile;

fn pos(o: i32) -> BPosition {
    BPosition {
        term_id: 0,
        term_offset: o,
    }
}

fn write_synthetic_segment() -> NamedTempFile {
    let mut buf = Vec::new();
    for i in 0..4u64 {
        let env = TxEnvelope {
            correlation_id: i,
            raw_tx: Bytes::from(vec![0xAB; 64]),
            sender: Address::repeat_byte(i as u8),
            tx_hash: B256::repeat_byte(i as u8),
        };
        append_frame(&mut buf, STREAM_KIND_TX, pos((i * 128) as i32), &env);
    }
    let boundary = BlockBoundaryStart {
        block_number: 1,
        end_tx_idx: pos(512),
        l2_timestamp: 1_700_000_000,
    };
    append_frame(&mut buf, STREAM_KIND_BOUNDARY, pos(512), &boundary);

    let mut f = NamedTempFile::new().unwrap();
    f.write_all(&buf).unwrap();
    f
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker; run with `cargo test -p kardamom-batcher --features docker-e2e -- --ignored`"]
async fn aeron_cluster_starts_and_batcher_round_trips_a_synthetic_segment() {
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

    // 2. Write a synthetic segment file using the batcher's own KAR1-internal
    //    frame writer. The full "publish via Aeron → SegmentReader" round-trip
    //    waits on the high-level ChannelBArchive wrapper in kardamom-log.
    let segment_file = write_synthetic_segment();

    // 3. Open the segment via the offline SegmentReader and drive the batcher
    //    pipeline: accumulate → pack → reconstruct.
    let reader = SegmentReader::open(segment_file.path()).expect("open segment");
    let cfg = BatcherConfig::default();
    let mut batcher = Batcher::new(cfg.clone(), MockSender::default());

    let all_ids = vec![0u8];
    let mut lease = Lease::new(LeaseConfig {
        self_id: 0,
        all_ids: all_ids.clone(),
        caught_up_window: 1024 * 1024,
    });
    lease.observe_quorum(QuorumWatermark { position: pos(0) });
    lease.observe_fsync(FsyncWatermark {
        recorder_id: 0,
        position: pos(0),
    });
    assert!(lease.held_by_us(), "single host always holds the lease");

    for rec in reader {
        match rec.expect("decode") {
            SegmentRecord::Tx { position, env } => {
                batcher.accumulator().observe_tx(env, position);
            }
            SegmentRecord::Boundary { marker, .. } => {
                let closed = batcher.accumulator().observe_boundary(marker);
                let pack = pack_blocks(&cfg, std::slice::from_ref(&closed)).expect("pack");
                let reconstructed =
                    reconstruct(&pack.blobs).expect("reconstruct round-trips the pipeline");
                assert_eq!(reconstructed.len(), 1);
                assert_eq!(reconstructed[0].block_number, 1);
                assert_eq!(reconstructed[0].txs.len(), 4);
                batcher.on_closed_block(closed, &lease).expect("post");
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
