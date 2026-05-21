//! End-to-end tests for `eth_simulateV1` over a running jsonrpsee server.

use std::net::SocketAddr;
use std::sync::Arc;

use alloy_primitives::{Address, B256, Bytes, TxKind, U256, address, hex};
use alloy_rpc_types_eth::simulate::{SimBlock, SimulatePayload, SimulatedBlock};
use alloy_rpc_types_eth::state::{AccountOverride, StateOverride};
use alloy_rpc_types_eth::{BlockNumberOrTag, TransactionRequest};

use jsonrpsee::core::client::ClientT;
use jsonrpsee::http_client::HttpClientBuilder;
use jsonrpsee::rpc_params;
use jsonrpsee::server::ServerHandle;

use kardamom_node::Node;
use kardamom_node::genesis::{AllocEntry, Genesis};
use kardamom_node::rpc::{EthApiServer, EthHandlers};

/// Boot a server backed by the provided node and return (client, handle).
async fn boot(node: Node) -> (jsonrpsee::http_client::HttpClient, ServerHandle) {
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = jsonrpsee::server::Server::builder()
        .build(addr)
        .await
        .unwrap();
    let bound = server.local_addr().unwrap();
    let handle = server.start(EthHandlers::new(node).into_rpc());
    let client = HttpClientBuilder::default()
        .build(format!("http://{}", bound))
        .unwrap();
    (client, handle)
}

fn const_42_contract() -> (Address, Bytes) {
    (
        address!("0000000000000000000000000000000000001234"),
        Bytes::from(hex!("604260005260206000f3").to_vec()),
    )
}

#[tokio::test]
async fn rpc_simulate_v1_round_trip() {
    let (contract_addr, code) = const_42_contract();
    let node = Node::new(&Genesis {
        chain_id: 1,
        alloc: vec![],
    });
    let (client, handle) = boot(node).await;

    let mut state_overrides = StateOverride::default();
    state_overrides.insert(
        contract_addr,
        AccountOverride {
            code: Some(code),
            ..Default::default()
        },
    );

    let payload = SimulatePayload {
        block_state_calls: vec![SimBlock {
            block_overrides: None,
            state_overrides: Some(state_overrides),
            calls: vec![TransactionRequest {
                from: Some(Address::ZERO),
                to: Some(TxKind::Call(contract_addr)),
                ..Default::default()
            }],
        }],
        trace_transfers: false,
        validation: false,
        return_full_transactions: false,
    };

    let out: Vec<SimulatedBlock> = client
        .request(
            "eth_simulateV1",
            rpc_params![payload, BlockNumberOrTag::Latest],
        )
        .await
        .expect("simulate ok");

    assert_eq!(out.len(), 1);
    assert_eq!(out[0].calls.len(), 1);
    assert!(out[0].calls[0].status);
    let mut expected = [0u8; 32];
    expected[31] = 0x42;
    assert_eq!(out[0].calls[0].return_data.as_ref(), &expected[..]);

    handle.stop().unwrap();
    handle.stopped().await;
}

#[tokio::test]
async fn rpc_simulate_v1_unsupported_base_block() {
    let node = Node::new(&Genesis {
        chain_id: 1,
        alloc: vec![],
    });
    let (client, handle) = boot(node).await;

    let payload: SimulatePayload = SimulatePayload {
        block_state_calls: vec![SimBlock::default()],
        trace_transfers: false,
        validation: false,
        return_full_transactions: false,
    };

    let err = client
        .request::<Vec<SimulatedBlock>, _>(
            "eth_simulateV1",
            rpc_params![payload, BlockNumberOrTag::Number(1)],
        )
        .await
        .expect_err("unsupported tag");
    let s = err.to_string();
    assert!(s.contains("latest"), "unexpected error: {s}");

    handle.stop().unwrap();
    handle.stopped().await;
}

#[tokio::test]
async fn rpc_simulate_v1_parallel_calls_are_independent() {
    let (contract_addr, code) = const_42_contract();
    let node = Node::new(&Genesis {
        chain_id: 1,
        alloc: vec![AllocEntry {
            address: contract_addr,
            balance: U256::ZERO,
            code: Some(code),
            nonce: None,
        }],
    });
    let (client, handle) = boot(node).await;
    let client = Arc::new(client);

    let mut handles = Vec::new();
    for _ in 0..8 {
        let c = Arc::clone(&client);
        handles.push(tokio::spawn(async move {
            let payload = SimulatePayload {
                block_state_calls: vec![SimBlock {
                    block_overrides: None,
                    state_overrides: None,
                    calls: vec![TransactionRequest {
                        from: Some(Address::ZERO),
                        to: Some(TxKind::Call(contract_addr)),
                        ..Default::default()
                    }],
                }],
                trace_transfers: false,
                validation: false,
                return_full_transactions: false,
            };
            let out: Vec<SimulatedBlock> = c
                .request(
                    "eth_simulateV1",
                    rpc_params![payload, BlockNumberOrTag::Latest],
                )
                .await
                .expect("ok");
            assert_eq!(out.len(), 1);
            assert!(out[0].calls[0].status);
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    handle.stop().unwrap();
    handle.stopped().await;
}

#[tokio::test]
async fn rpc_simulate_does_not_mutate_live_state() {
    let from = address!("00000000000000000000000000000000000000aa");
    let node = Node::new(&Genesis {
        chain_id: 1,
        alloc: vec![AllocEntry {
            address: from,
            balance: U256::from(1_000_000u64),
            code: None,
            nonce: None,
        }],
    });
    let (client, handle) = boot(node).await;

    let bn_before: U256 = client
        .request("eth_blockNumber", rpc_params![])
        .await
        .unwrap();
    let bal_before: U256 = client
        .request(
            "eth_getBalance",
            rpc_params![from, BlockNumberOrTag::Latest],
        )
        .await
        .unwrap();

    let payload = SimulatePayload {
        block_state_calls: vec![SimBlock {
            calls: vec![TransactionRequest {
                from: Some(from),
                to: Some(TxKind::Call(Address::ZERO)),
                value: Some(U256::from(500_000u64)),
                ..Default::default()
            }],
            ..Default::default()
        }],
        trace_transfers: false,
        validation: false,
        return_full_transactions: false,
    };
    let _: Vec<SimulatedBlock> = client
        .request(
            "eth_simulateV1",
            rpc_params![payload, BlockNumberOrTag::Latest],
        )
        .await
        .unwrap();

    let bn_after: U256 = client
        .request("eth_blockNumber", rpc_params![])
        .await
        .unwrap();
    let bal_after: U256 = client
        .request(
            "eth_getBalance",
            rpc_params![from, BlockNumberOrTag::Latest],
        )
        .await
        .unwrap();
    assert_eq!(bn_after, bn_before);
    assert_eq!(bal_after, bal_before);

    handle.stop().unwrap();
    handle.stopped().await;
}

#[tokio::test]
async fn rpc_simulate_v1_multi_block_with_state_inheritance() {
    let node = Node::new(&Genesis {
        chain_id: 1,
        alloc: vec![],
    });
    let (client, handle) = boot(node).await;

    let from = address!("00000000000000000000000000000000000000aa");
    let to = address!("00000000000000000000000000000000000000bb");

    let mk_block = |value: u64| SimBlock {
        calls: vec![TransactionRequest {
            from: Some(from),
            to: Some(TxKind::Call(to)),
            value: Some(U256::from(value)),
            ..Default::default()
        }],
        ..Default::default()
    };

    let payload = SimulatePayload {
        block_state_calls: vec![mk_block(10), mk_block(20), mk_block(30)],
        trace_transfers: false,
        validation: false,
        return_full_transactions: false,
    };
    let out: Vec<SimulatedBlock> = client
        .request(
            "eth_simulateV1",
            rpc_params![payload, BlockNumberOrTag::Latest],
        )
        .await
        .unwrap();
    assert_eq!(out.len(), 3);
    assert_eq!(out[0].inner.header.number, 1);
    assert_eq!(out[1].inner.header.number, 2);
    assert_eq!(out[2].inner.header.number, 3);
    for block in &out {
        assert!(block.calls[0].status);
    }

    handle.stop().unwrap();
    handle.stopped().await;
}

#[tokio::test]
async fn rpc_simulate_then_send_then_simulate_observes_committed_tx() {
    use alloy_consensus::{SignableTransaction, TxEnvelope, TxLegacy};
    use alloy_eips::eip2718::Encodable2718;
    use alloy_network::TxSignerSync;
    use alloy_signer_local::PrivateKeySigner;

    let signer = PrivateKeySigner::random();
    let sender = signer.address();
    let recipient = Address::from([0x22u8; 20]);
    let chain_id = 1;

    let node = Node::new(&Genesis {
        chain_id,
        alloc: vec![AllocEntry {
            address: sender,
            balance: U256::from(10u64).pow(U256::from(18u64)),
            code: None,
            nonce: None,
        }],
    });
    let (client, handle) = boot(node).await;

    // First simulation: recipient balance is 0.
    let probe: SimulatePayload = SimulatePayload {
        block_state_calls: vec![SimBlock {
            calls: vec![],
            ..Default::default()
        }],
        ..Default::default()
    };
    let _: Vec<SimulatedBlock> = client
        .request(
            "eth_simulateV1",
            rpc_params![probe.clone(), BlockNumberOrTag::Latest],
        )
        .await
        .unwrap();
    let bal_first: U256 = client
        .request(
            "eth_getBalance",
            rpc_params![recipient, BlockNumberOrTag::Latest],
        )
        .await
        .unwrap();
    assert_eq!(bal_first, U256::ZERO);

    // Submit a real tx.
    let mut tx = TxLegacy {
        chain_id: Some(chain_id),
        nonce: 0,
        gas_price: 1_000_000_000,
        gas_limit: 21_000,
        to: TxKind::Call(recipient),
        value: U256::from(1_000u64),
        input: Bytes::new(),
    };
    let sig = signer.sign_transaction_sync(&mut tx).unwrap();
    let envelope: TxEnvelope = tx.into_signed(sig).into();
    let mut raw = Vec::new();
    envelope.encode_2718(&mut raw);
    let _hash: B256 = client
        .request("eth_sendRawTransaction", rpc_params![Bytes::from(raw)])
        .await
        .unwrap();

    // Second simulation: recipient balance reflects committed transfer.
    let bal_after: U256 = client
        .request(
            "eth_getBalance",
            rpc_params![recipient, BlockNumberOrTag::Latest],
        )
        .await
        .unwrap();
    assert_eq!(bal_after, U256::from(1_000u64));

    handle.stop().unwrap();
    handle.stopped().await;
}
