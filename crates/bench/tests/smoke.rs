//! Smoke test: run `Benchmark<TransfersWorkflow>` against an in-process
//! kardamom **ingress** (real `IngressProxy` over `MockChannels` + a
//! fake-executor receipt loop) for a short window, and assert the write-path
//! histogram is non-empty.
//!
//! This replaces the former in-process-`Node` smoke test: `kardamom-node` was
//! removed and the bench now targets the cluster ingress. `transfers` is the
//! write-path workflow (`eth_sendRawTransaction` + parked-receipt release),
//! which ingress serves; `eth_call`-based workflows are deferred until ingress
//! grows read-path RPCs.

use std::time::Duration;

use kardamom_bench::harness::spawn_inprocess_ingress;
use kardamom_bench::workflow::BenchWorkflow;
use kardamom_bench::{Benchmark, MixedWorkflow, TransfersWorkflow};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bench_records_samples_against_inprocess_ingress() {
    let chain_id = 1u64;
    let concurrency = 4u32;
    let max_in_flight = 4u32;

    let (client, ingress) = spawn_inprocess_ingress(chain_id, 1, max_in_flight as usize)
        .await
        .expect("spawn in-process ingress");

    let bench = Benchmark {
        workflow: TransfersWorkflow::default(),
        timeout: Duration::from_secs(5),
        concurrency,
        txs_per_task: 50,
        max_in_flight,
    };

    let outputs = bench.run(client).await.expect("bench ran");
    assert!(outputs.counters.sent > 0, "should have sent some requests");

    let send_hist = outputs
        .histograms
        .get("eth_sendRawTransaction")
        .expect("send hist");
    assert!(
        !send_hist.is_empty(),
        "eth_sendRawTransaction samples should be non-empty"
    );

    ingress.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn workflow_genesis_alloc_propagates_bad_mnemonic() {
    // C2 regression test: a workflow constructed with a bogus mnemonic
    // must surface an `Err` from `genesis_alloc`, not panic.
    let workflow = MixedWorkflow {
        mnemonic: "this is not a valid bip39 phrase at all".to_string(),
        ..MixedWorkflow::default()
    };
    let err = workflow
        .genesis_alloc(4)
        .expect_err("bad mnemonic should produce Err, not panic");
    let msg = format!("{err:#}");
    assert!(
        msg.to_lowercase().contains("mnemonic")
            || msg.contains("derivation")
            || msg.contains("bip39"),
        "error should mention the mnemonic / BIP-39 derivation failure, got: {msg}"
    );
}
