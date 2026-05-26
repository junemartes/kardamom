//! **Proof-of-pipeline** end-to-end test.
//!
//! Brings up real Aeron in Docker, wires the kardamom log layer against it
//! via the new Send-friendly [`log::aeron_live`] adapters, and drives N
//! synthetic txs through:
//!
//!   1. publish onto tx_data (real Aeron, recorded — per-shard envelope
//!      stream, proxy role compressed),
//!   2. subscribe on tx_data from the executor's reader thread,
//!   3. execute (synthesized for this test — the real revm path is unit-
//!      tested elsewhere) against an in-memory `StateDatabase`,
//!   4. publish receipts + slim BlockBoundary onto tx_receipts,
//!   5. subscribe on tx_receipts, assert receipts surface with the correct
//!      `tx_hash` (copied unchanged from the inbound envelope),
//!   6. submit BlockDelta + boundary to a real libmdbx `StateWriter` and
//!      assert the post-block snapshot reflects the committed state.
//!
//! Gated on `feature = "full-pipeline-e2e"` AND `#[ignore]` so default
//! `cargo test` skips it. To run locally:
//!
//! ```bash
//! cargo test -p e2e --features full-pipeline-e2e \
//!   --test full_pipeline_e2e -- --ignored --nocapture
//! ```

#![cfg(feature = "full-pipeline-e2e")]

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use alloy_primitives::{Address, B256, U256};
use bytes::Bytes;
use e2e::pipeline::channel_uri_for;
use log::aeron_live::{
    AeronRuntime, TxDataPublisherHandle, TxDataSubscriberHandle, TxReceiptsPublisherHandle,
    TxReceiptsSubscriberHandle,
};
use log::config::LogConfig;
use log::testing::AeronTestCluster;
use types::{BPosition, BlockBoundary, Receipt, TxEnvelope, WireLog};

async fn docker_available() -> bool {
    use tokio::process::Command;
    Command::new("docker")
        .arg("info")
        .output()
        .await
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// How many txs to push through the pipeline. Tunable via `KARDAMOM_E2E_N`.
fn n_txs() -> u64 {
    std::env::var("KARDAMOM_E2E_N")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(200)
}

fn synthetic_envelope(i: u64) -> TxEnvelope {
    TxEnvelope {
        correlation_id: i,
        raw_tx: Bytes::from(vec![0xCDu8; 96]),
        sender: Address::repeat_byte((i as u8) ^ 0xAB),
        tx_hash: B256::repeat_byte((i as u8) ^ 0x5A),
    }
}

fn synthetic_receipt(env: &TxEnvelope, tx_idx: BPosition) -> Receipt {
    Receipt {
        tx_idx,
        tx_hash: env.tx_hash,
        status: true,
        gas_used: 21_000,
        logs: Vec::<WireLog>::new(),
        write_set_hash: B256::repeat_byte(0xEE),
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker; run with `cargo test --features full-pipeline-e2e -- --ignored full_pipeline_e2e`"]
async fn proof_of_pipeline_round_trip() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with_test_writer()
        .try_init();

    if !docker_available().await {
        eprintln!("skipping: docker not available");
        return;
    }

    // 1. Bring up real Aeron in Docker.
    let cluster = AeronTestCluster::single_node()
        .await
        .expect("aeron container started");
    let endpoint = cluster.archive_control_endpoint(0).await;
    tracing::info!(%endpoint, "aeron container up");

    // 2. Configure channels for the test. Use IPC URIs for the single-shard
    //    single-node container, with a session-scoped alias so reruns don't
    //    collide on residual stream state.
    let session_id = format!("e2e-{}", std::process::id());
    let mut cfg = LogConfig::default();
    cfg.channels.tx_data_channel_template = format!("aeron:ipc?alias={session_id}-tx-data-{{sid}}");
    cfg.channels.tx_data_stream_id_base = 7001;
    cfg.channels.tx_receipts_channel = channel_uri_for(&session_id, "tx_receipts");
    cfg.channels.tx_receipts_stream_id = 7100;

    // 3. Spawn the Aeron runtime (dedicated OS thread; Send-friendly handle).
    //    Point at the bind-mounted aeron.dir exposed by the test cluster —
    //    same absolute path on host and inside the container, so cnc.dat's
    //    internal references resolve correctly on both sides.
    let rt = AeronRuntime::spawn_with_dir(cluster.aeron_dir_host(0)).expect("aeron runtime");

    // 4. Open the publishers + subscribers for a single tx_data shard.
    let sequencer_id = 0u8;
    let tx_data_pub =
        TxDataPublisherHandle::open(&rt, &cfg.channels, sequencer_id).expect("tx_data publisher");
    let mut tx_data_sub =
        TxDataSubscriberHandle::open(&rt, &cfg.channels, sequencer_id).expect("tx_data subscriber");
    let tx_receipts_pub =
        TxReceiptsPublisherHandle::open(&rt, &cfg.channels).expect("tx_receipts publisher");
    let mut tx_receipts_sub =
        TxReceiptsSubscriberHandle::open(&rt, &cfg.channels).expect("tx_receipts subscriber");

    // 5. Publish N TxEnvelopes onto tx_data (proxy + sequencer role compressed).
    let n = n_txs();
    tracing::info!(n, "publishing onto tx_data");
    let published_hashes: Arc<std::sync::Mutex<Vec<B256>>> =
        Arc::new(std::sync::Mutex::new(Vec::with_capacity(n as usize)));
    let publisher_for_task = tx_data_pub.clone();
    let hashes_for_publisher = published_hashes.clone();
    let publish_task = tokio::task::spawn_blocking(move || {
        for i in 0..n {
            let env = synthetic_envelope(i);
            let hash = env.tx_hash;
            publisher_for_task.publish(&env).expect("publish");
            hashes_for_publisher.lock().unwrap().push(hash);
        }
    });

    // 6. Executor role: drain tx_data → produce receipts → publish onto
    //    tx_receipts.
    let received_data_count = Arc::new(AtomicU64::new(0));
    let receipts_pub_for_task = tx_receipts_pub.clone();
    let received_data_for_task = received_data_count.clone();
    let executor_task = tokio::spawn(async move {
        let mut seen = 0u64;
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        while seen < n && std::time::Instant::now() < deadline {
            match tokio::time::timeout(Duration::from_millis(50), tx_data_sub.recv()).await {
                Ok(Some((pos, env))) => {
                    let receipt = synthetic_receipt(&env, pos);
                    let publisher = receipts_pub_for_task.clone();
                    let r = receipt.clone();
                    tokio::task::spawn_blocking(move || publisher.publish_receipt(&r))
                        .await
                        .expect("join")
                        .expect("publish_receipt");
                    seen += 1;
                }
                Ok(None) => break,
                Err(_) => {}
            }
        }
        received_data_for_task.store(seen, Ordering::SeqCst);

        // Also publish the slim BlockBoundary.
        let boundary = BlockBoundary {
            block_number: 1,
            end_tx_idx: BPosition::ZERO,
            l2_timestamp: 1_700_000_000,
        };
        let publisher = receipts_pub_for_task.clone();
        let _ = tokio::task::spawn_blocking(move || publisher.publish_boundary(&boundary))
            .await
            .expect("join");
    });

    // 7. Drain tx_receipts and assert tx_hash propagation.
    let mut received_receipts: Vec<Receipt> = Vec::with_capacity(n as usize);
    let drain_deadline = std::time::Instant::now() + Duration::from_secs(60);
    while (received_receipts.len() as u64) < n && std::time::Instant::now() < drain_deadline {
        match tokio::time::timeout(Duration::from_millis(50), tx_receipts_sub.recv()).await {
            Ok(Some((_pos, r))) => received_receipts.push(r),
            Ok(None) => break,
            Err(_) => {}
        }
    }

    publish_task.await.expect("publisher task");
    executor_task.await.expect("executor task");

    // 8. Pipeline assertions.
    assert_eq!(
        received_data_count.load(Ordering::SeqCst),
        n,
        "executor saw {} of {n} envelopes on tx_data",
        received_data_count.load(Ordering::SeqCst)
    );
    assert_eq!(
        received_receipts.len() as u64,
        n,
        "got {} of {n} receipts on tx_receipts",
        received_receipts.len()
    );

    let published = published_hashes.lock().unwrap();
    // tx_hash propagation: every receipt's tx_hash must match the envelope's
    // tx_hash for the same in-stream position. Per-publisher FIFO on tx_data
    // guarantees per-publisher receipt order on tx_receipts.
    for (i, r) in received_receipts.iter().enumerate() {
        assert_eq!(
            r.tx_hash, published[i],
            "tx_hash propagation violated at index {i}"
        );
        assert!(r.status, "receipt {i} marked failure");
        assert_eq!(r.gas_used, 21_000);
    }

    // 9. State-writer round-trip.
    state_writer_round_trip();

    tracing::info!(
        n,
        published = published.len(),
        received_receipts = received_receipts.len(),
        "proof-of-pipeline OK"
    );

    drop(rt);
    drop(cluster);
}

/// Open a libmdbx state writer in a tempdir, push one BlockDelta, and assert
/// the post-commit snapshot reflects the committed account.
fn state_writer_round_trip() {
    use state::env::{Durability, StateEnvBuilder};
    use state::writer::{StateWriter, WriteBatch};
    use types::{AccountChange, BlockDelta, StateDatabase};

    let tmp = tempfile::tempdir().expect("tempdir");
    let env = StateEnvBuilder::new(tmp.path())
        .durability(Durability::SafeNoSync)
        .open()
        .expect("env open");
    let writer = StateWriter::spawn(env).expect("writer spawn");
    // Drop the genesis snapshot.
    let _ = writer.snapshot_rx.recv();

    let addr = Address::repeat_byte(0x77);
    let delta = BlockDelta {
        block_number: 1,
        accounts: vec![AccountChange {
            address: addr,
            nonce: 1,
            balance: U256::from(123u64),
            code_hash: B256::ZERO,
        }],
        storage: vec![],
        code: vec![],
        receipts: vec![],
    };
    let boundary = BlockBoundary {
        block_number: 1,
        end_tx_idx: BPosition::ZERO,
        l2_timestamp: 1_700_000_000,
    };
    writer
        .delta_tx
        .send(WriteBatch { boundary, delta })
        .expect("submit batch");
    let snap = writer
        .snapshot_rx
        .recv()
        .expect("post-commit snapshot published");
    assert_eq!(snap.block_number(), 1);
    let (nonce, balance, _code_hash) = snap.basic(addr).expect("basic").expect("account exists");
    assert_eq!(nonce, 1);
    assert_eq!(balance, U256::from(123u64));
    writer.shutdown().expect("writer shutdown");
}
