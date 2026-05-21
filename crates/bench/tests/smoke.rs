//! Smoke test: run `Benchmark<MixedWorkflow>` against an in-process kardamom
//! node for a short window, assert per-method histograms are non-empty.

use std::net::SocketAddr;
use std::time::Duration;

use jsonrpsee::http_client::HttpClientBuilder;

use kardamom_bench::workflow::BenchWorkflow;
use kardamom_bench::{Benchmark, MixedWorkflow};
use kardamom_node::{Genesis, Node, rpc};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bench_records_samples_against_inprocess_node() {
    let chain_id = 1u64;
    let concurrency = 4u32;

    let workflow = MixedWorkflow::default();
    let alloc = workflow.genesis_alloc(concurrency).expect("genesis_alloc");
    let genesis = Genesis { chain_id, alloc };
    genesis
        .validate()
        .expect("valid genesis from workflow.genesis_alloc");
    let node = Node::new(&genesis);

    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = jsonrpsee::server::Server::builder()
        .build(addr)
        .await
        .unwrap();
    let bound = server.local_addr().unwrap();
    let server_handle = server.start({
        use rpc::EthApiServer;
        rpc::EthHandlers::new(node).into_rpc()
    });

    let url = format!("http://{bound}");
    let client = HttpClientBuilder::default().build(&url).unwrap();

    let bench = Benchmark {
        workflow,
        duration: Duration::from_secs(5),
        warmup: Duration::ZERO,
        concurrency,
        txs_per_task: 50,
        max_in_flight: 4,
    };

    let outputs = bench.run(client).await.expect("bench ran");
    assert!(outputs.counters.sent > 0, "should have sent some requests");

    let call_hist = outputs.histograms.get("eth_call").expect("eth_call hist");
    let send_hist = outputs
        .histograms
        .get("eth_sendRawTransaction")
        .expect("send hist");

    assert!(
        !call_hist.is_empty(),
        "eth_call samples should be non-empty"
    );
    assert!(
        !send_hist.is_empty(),
        "eth_sendRawTransaction samples should be non-empty"
    );

    let _ = server_handle.stop();
    server_handle.stopped().await;
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
