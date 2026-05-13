use std::net::SocketAddr;

use alloy_consensus::{SignableTransaction, TxEnvelope, TxLegacy};
use alloy_eips::eip2718::Encodable2718;
use alloy_network::TxSignerSync;
use alloy_primitives::{Address, Bytes, TxKind, U256};
use alloy_rpc_types_eth::{BlockNumberOrTag, TransactionReceipt, TransactionRequest};
use alloy_signer_local::PrivateKeySigner;

use jsonrpsee::core::client::ClientT;
use jsonrpsee::http_client::HttpClientBuilder;
use jsonrpsee::rpc_params;

use kardamom_node::rpc::EthApiServer;
use kardamom_node::Node;

#[tokio::test]
async fn end_to_end_send_and_query() {
    let signer = PrivateKeySigner::random();
    let from = signer.address();
    let to = Address::from([0x22u8; 20]);
    let chain_id = 412346;

    let node = Node::new(
        chain_id,
        &[(from, U256::from(10u64).pow(U256::from(18u64)))],
    );

    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = jsonrpsee::server::Server::builder()
        .build(addr)
        .await
        .unwrap();
    let bound = server.local_addr().unwrap();
    let handle = server.start(
        kardamom_node::rpc::EthHandlers::new(node.clone()).into_rpc(),
    );

    let client = HttpClientBuilder::default()
        .build(format!("http://{}", bound))
        .unwrap();

    // chainId
    let cid: U256 = client.request("eth_chainId", rpc_params![]).await.unwrap();
    assert_eq!(cid, U256::from(chain_id));

    // initial block
    let bn: U256 = client.request("eth_blockNumber", rpc_params![]).await.unwrap();
    assert_eq!(bn, U256::ZERO);

    // balance
    let bal: U256 = client
        .request(
            "eth_getBalance",
            rpc_params![from, BlockNumberOrTag::Latest],
        )
        .await
        .unwrap();
    assert_eq!(bal, U256::from(10u64).pow(U256::from(18u64)));

    // nonce before send
    let nonce_before: U256 = client
        .request(
            "eth_getTransactionCount",
            rpc_params![from, BlockNumberOrTag::Latest],
        )
        .await
        .unwrap();
    assert_eq!(nonce_before, U256::ZERO);

    // send raw tx
    let mut tx = TxLegacy {
        chain_id: Some(chain_id),
        nonce: 0,
        gas_price: 1_000_000_000,
        gas_limit: 21_000,
        to: TxKind::Call(to),
        value: U256::from(5_000u64),
        input: Bytes::new(),
    };
    let sig = signer.sign_transaction_sync(&mut tx).unwrap();
    let envelope: TxEnvelope = tx.into_signed(sig).into();
    let mut raw = Vec::new();
    envelope.encode_2718(&mut raw);

    let hash: alloy_primitives::B256 = client
        .request("eth_sendRawTransaction", rpc_params![Bytes::from(raw)])
        .await
        .unwrap();

    // receipt
    let receipt: Option<TransactionReceipt> = client
        .request("eth_getTransactionReceipt", rpc_params![hash])
        .await
        .unwrap();
    let receipt = receipt.expect("receipt present");
    assert_eq!(receipt.transaction_hash, hash);
    assert_eq!(receipt.from, from);
    assert_eq!(receipt.to, Some(to));

    // post-state
    let bn_after: U256 = client.request("eth_blockNumber", rpc_params![]).await.unwrap();
    assert_eq!(bn_after, U256::from(1u64));
    let bal_to: U256 = client
        .request(
            "eth_getBalance",
            rpc_params![to, BlockNumberOrTag::Latest],
        )
        .await
        .unwrap();
    assert_eq!(bal_to, U256::from(5_000u64));

    // nonce after send
    let nonce_after: U256 = client
        .request(
            "eth_getTransactionCount",
            rpc_params![from, BlockNumberOrTag::Latest],
        )
        .await
        .unwrap();
    assert_eq!(nonce_after, U256::from(1u64));

    // eth_call against a plain EOA — no code, expect empty output
    let mut call_req = TransactionRequest::default();
    call_req.to = Some(TxKind::Call(to));
    let call_result: Bytes = client
        .request("eth_call", rpc_params![call_req, BlockNumberOrTag::Latest])
        .await
        .unwrap();
    assert_eq!(call_result, Bytes::new());

    handle.stop().unwrap();
    handle.stopped().await;
}
