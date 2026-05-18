//! Smoke test: run the bench's generator against an in-process kardamom node
//! for ~1s at 10rps, assert per-method histograms are non-empty.

use std::net::SocketAddr;
use std::time::Duration;

use alloy_primitives::{Address, Bytes, U256, address, hex};
use jsonrpsee::http_client::HttpClientBuilder;

use kardamom_bench::generator;
use kardamom_bench::signers;
use kardamom_node::{AllocEntry, Genesis, Node, rpc};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bench_generator_records_samples_against_inprocess_node() {
    let chain_id = 1u64;
    let seed = 0xC0FFEE_u64;
    let concurrency = 4u32;

    let derived = signers::derive(seed, concurrency as usize).unwrap();

    let contract: Address = address!("0000000000000000000000000000000000001234");
    // PUSH1 0x42; PUSH1 0x00; MSTORE; PUSH1 0x20; PUSH1 0x00; RETURN
    let code = Bytes::from(hex!("604260005260206000f3").to_vec());

    let one_eth = U256::from(10u64).pow(U256::from(18u64));
    let mut alloc: std::collections::BTreeMap<Address, AllocEntry> = derived
        .iter()
        .map(|s| {
            (
                s.address,
                AllocEntry {
                    balance: one_eth,
                    code: None,
                    nonce: 0,
                },
            )
        })
        .collect();
    alloc.insert(
        contract,
        AllocEntry {
            balance: U256::ZERO,
            code: Some(code),
            nonce: 1,
        },
    );

    let genesis = Genesis { chain_id, alloc };
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

    let url = format!("http://{}", bound);
    let client = HttpClientBuilder::default().build(&url).unwrap();

    let cfg = kardamom_bench::config::Config {
        rpc: url,
        workload: kardamom_bench::config::Workload::Mixed,
        rate: 100,
        duration: Duration::from_millis(800),
        concurrency,
        warmup: Duration::from_millis(0),
        seed,
        output: None,
        mix: kardamom_bench::config::MixCfg {
            transfers: 1,
            calls: 4,
        },
        calls: Some(kardamom_bench::config::CallsCfg { contract }),
    };

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
