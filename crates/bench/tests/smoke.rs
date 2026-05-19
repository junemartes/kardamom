//! Smoke test: run the bench's generator against an in-process kardamom node
//! for ~1s at 10rps, assert per-method histograms are non-empty.

use std::net::SocketAddr;
use std::time::Duration;

use alloy_primitives::{Address, address};
use jsonrpsee::http_client::HttpClientBuilder;

use kardamom_bench::config::{CallsCfg, ContractEntry, FileConfig, MixCfg, MnemonicCfg, Workload};
use kardamom_bench::{generator, genesis, mnemonic};
use kardamom_node::{Node, rpc};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bench_generator_records_samples_against_inprocess_node() {
    let chain_id = 1u64;
    let concurrency = 4u32;

    let contract: Address = address!("0000000000000000000000000000000000001234");

    let file_cfg = FileConfig {
        rpc: None,
        workload: Some(Workload::Mixed),
        rate: Some(100),
        duration: Some(Duration::from_millis(800)),
        concurrency: Some(concurrency),
        warmup: Some(Duration::from_millis(0)),
        output: None,
        mix: Some(MixCfg {
            transfers: 1,
            calls: 4,
        }),
        calls: Some(CallsCfg { contract }),
        mnemonic: Some(MnemonicCfg {
            phrase: "test test test test test test test test test test test junk".to_string(),
            balance: "1000000000000000000000".to_string(),
            count: None,
        }),
        contracts: vec![ContractEntry {
            address: contract,
            code: "0x604260005260206000f3".to_string(),
            nonce: None,
            balance: None,
        }],
    };

    let genesis_value = genesis::from_config(&file_cfg, chain_id).expect("from_config");
    let node = Node::new(&genesis_value);

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

    let url = format!("http://{}", bound);
    let client = HttpClientBuilder::default().build(&url).unwrap();

    let cli_overrides = FileConfig {
        rpc: Some(url.clone()),
        workload: None,
        rate: None,
        duration: None,
        concurrency: None,
        warmup: None,
        output: None,
        mix: None,
        calls: None,
        mnemonic: None,
        contracts: vec![],
    };
    let cfg = kardamom_bench::config::resolve(Some(file_cfg), cli_overrides).expect("resolve");

    // Sanity: signers derive deterministically and the generator will get the same set.
    let _signers =
        mnemonic::derive_signers(&cfg.mnemonic.phrase, concurrency).expect("derive_signers");

    let outputs = generator::run(client, cfg).await.expect("bench ran");
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
