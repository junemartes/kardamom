//! E2E test scaffold: real Aeron Media Driver + Aeron Archive in Docker via
//! the [`kardamom_log::testing::AeronTestCluster`] harness from S3.
//!
//!— mock-based unit and integration tests in this crate stay
//! (write_replay, snapshot_mvcc, snapshot_swap, concurrent_readers,
//! recovery_midblock, compaction_smoke); this is *additional* coverage that
//! brings up the real Aeron container so we catch wire-format / IPC /
//! back-pressure bugs the channel-less in-process writer cannot surface.
//!
//! Gated behind `feature = "docker-e2e"` because it requires a Docker daemon
//! and ~30s startup; default `cargo test` skips it.
//!
//! **v0 scope:** brings up the Aeron container, opens a real libmdbx
//! `StateWriter`, and asserts the harness resolves its host ports + the
//! writer can apply at least one local batch. The full tx_receipts subscribe
//! → BlockDelta-build → writer round-trip requires a `TxReceiptsSubscriber`
//! adapter wrapping `log`'s `aeron-live` tx_receipts async wrapper.
//! The high-level tx_receipts async wrapper is not yet shipped by `log`
//! (only the low-level rusteron primitives plus the
//! `testing::AeronTestCluster` harness are exposed). When that wrapper
//! lands, this file gains the full publish → writer → tx_hash_index
//! round-trip; the harness assertion
//! below is the gating proof that this crate's test target can reach the
//! Aeron container, which is the prerequisite the wrapper landing
//! unblocks.

#![cfg(feature = "docker-e2e")]

use alloy_primitives::address;
use kardamom_log::testing::AeronTestCluster;
use kardamom_state::StateWriter;
use kardamom_state::env::{Durability, StateEnvBuilder};
use kardamom_types::StateDatabase;

mod common;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker; run with `cargo test -p kardamom-state --features docker-e2e -- --ignored`"]
async fn aeron_cluster_starts_and_state_writer_applies_batch() {
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

    // 2. Spin up a real libmdbx-backed StateWriter on a tempdir.
    let tmpdir = tempfile::tempdir().expect("tempdir");
    let env = StateEnvBuilder::new(tmpdir.path())
        .durability(Durability::SafeNoSync)
        .open()
        .expect("env open");
    let mut writer = StateWriter::spawn(env).expect("writer spawn");
    // Drop the genesis snapshot.
    let _ = writer.snapshot_rx.recv();

    // 3. Apply one block boundary + delta locally — exercising the writer
    //    end-to-end. The full plumb-through-Aeron flow waits on the high-
    //    level tx_receipts wrapper (see module docs above).
    let addr = address!("0x00000000000000000000000000000000000000aa");
    writer
        .delta_tx
        .send(common::simple_delta(1, addr, 100, 7, 42))
        .expect("submit batch");
    let snap = writer
        .snapshot_rx
        .recv()
        .expect("post-commit snapshot published");
    assert_eq!(snap.block_number(), 1);
    let (_, balance, _) = snap.basic(addr).expect("basic").expect("acct exists");
    assert_eq!(balance, alloy_primitives::U256::from(100u64));

    writer.shutdown().expect("writer shutdown");
}
