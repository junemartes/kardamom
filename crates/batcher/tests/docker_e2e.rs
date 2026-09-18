//! E2E test scaffold: a real Aeron Media Driver and Aeron Archive in
//! Docker, through the [`kardamom_log::testing::AeronTestCluster`] harness
//! from `kardamom-log`.
//!
//! The mock-based unit and integration tests in this crate stay as they
//! are. This is additional coverage that brings up the real Aeron
//! container, so it can catch wire-format, IPC, and back-pressure bugs the
//! in-process reader cannot surface.
//!
//! Gated behind `feature = "docker-e2e"`, because it needs a Docker daemon
//! and about 30 seconds of startup time. The default `cargo test` skips it.
//!
//! Test scope: bring up the Aeron container, and write synthetic segment
//! files in the canonical KAR1-internal frame format that the batcher's
//! offline `TypedSegmentReader` consumes:
//!   - one `tx_ordering` archive, carrying `TxOrderingMessage` records
//!     (`TxRef` and `BoundaryStart`); and
//!   - one or more per-sequencer `tx_data` archives, carrying the full
//!     `TxEnvelope` records the refs point at.
//!
//! Then it runs the full batcher pipeline through the
//! [`MultiArchiveReader`]: read, resolve, accumulate, pack, reconstruct,
//! and check the blobs. It checks that a `BatchPosted`-shaped
//! `PostBatchParams` could be assembled.
//!
//! The synthetic segment files are written directly, with the batcher's
//! own KAR1-internal frame writer, not through a live Aeron publish and
//! record. The harness assertion below proves only that this crate's test
//! target can reach the Aeron container.

#![cfg(feature = "docker-e2e")]

use kardamom_batcher::batcher::{Batcher, BatcherConfig, MockSender, pack_blocks};
use kardamom_batcher::multi_archive_reader::{
    MultiArchiveConfig, MultiArchiveReader, ResolvedRecord,
};
use kardamom_batcher::recon::reconstruct;
use kardamom_batcher::testkit::write_m_plus_one_archives;
use kardamom_log::testing::AeronTestCluster;
use tempfile::TempDir;

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

    // 2. Write the M+1 synthetic archives with the batcher's own
    //    KAR1-internal frame writer. The full "publish through Aeron, then
    //    SegmentReader" round trip waits on the high-level tx_ordering and
    //    tx_data archive wrappers in `log`.
    let dir = TempDir::new().unwrap();
    let archives = write_m_plus_one_archives(dir.path(), 2, 1, 1_700_000_000);

    // 3. Open the M-archive reader and drive the batcher pipeline: walk B,
    //    resolve refs against per-A indexes, accumulate, pack, and
    //    reconstruct.
    let cfg = BatcherConfig::default();
    let mut batcher = Batcher::new(cfg.clone(), MockSender::default());

    let reader = MultiArchiveReader::open(&MultiArchiveConfig {
        b_segment: archives.b_segment,
        a_segments: archives.a_segments,
    })
    .expect("open multi archive reader");
    assert_eq!(reader.a_archive_count(), 2);

    reader.for_each(|rec| {
        apply_resolved_record(
            &mut batcher,
            &cfg,
            &archives.canonical_order,
            rec.expect("decode"),
        );
    });

    assert_eq!(
        batcher.sender().sent.len(),
        1,
        "exactly one batch should have been forwarded to the sender"
    );
}

/// One resolved record from the multi-archive reader: feed it into the
/// accumulator, or, at a boundary, pack the closed block, reconstruct it,
/// and check the canonical alternation (a0\[0\], a1\[0\], a0\[1\], a1\[1\])
/// against `canonical_order`.
fn apply_resolved_record(
    batcher: &mut Batcher<MockSender>,
    cfg: &BatcherConfig,
    canonical_order: &[u64],
    rec: ResolvedRecord,
) {
    let Some(closed) = batcher.accumulator().observe(rec) else {
        return;
    };
    let pack = pack_blocks(cfg, std::slice::from_ref(&closed)).expect("pack");
    let reconstructed = reconstruct(&pack.blobs).expect("reconstruct round-trips the pipeline");
    assert_eq!(reconstructed.len(), 1);
    assert_eq!(reconstructed[0].block_number, 1);
    assert_eq!(
        reconstructed[0].txs.len(),
        canonical_order.len(),
        "resolved tx count should match the fixture"
    );
    let got: Vec<u64> = reconstructed[0]
        .txs
        .iter()
        .map(|tx| tx.correlation_id)
        .collect();
    assert_eq!(got, canonical_order);
    batcher.on_closed_block(closed).expect("post");
}
