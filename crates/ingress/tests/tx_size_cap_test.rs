//! The size cap on `raw_tx` at the JSON-RPC boundary. A transaction of
//! exactly the cap gets its receipt. One byte more gets a `-32602` error
//! that states the size and the cap.

use std::num::NonZeroUsize;

use alloy_primitives::B256;
use alloy_signer_local::PrivateKeySigner;
use jsonrpsee::core::client::{ClientT, Error as ClientError};
use jsonrpsee::rpc_params;

use kardamom_ingress::config::{DEFAULT_MAX_TX_BYTES, IngressConfig};
use kardamom_ingress::test_support::{
    TestServer, http_client, sign_legacy_sized, spawn_fake_executor, start_test_server,
};

const CAP: NonZeroUsize = DEFAULT_MAX_TX_BYTES;

#[tokio::test(flavor = "multi_thread")]
async fn json_rpc_accepts_the_cap_and_refuses_one_byte_more() {
    let TestServer {
        mock,
        shard_rx,
        addr,
        handle,
    } = start_test_server(IngressConfig::default()).await;
    let executor = spawn_fake_executor(&mock, shard_rx);
    let client = http_client(addr);
    let signer = PrivateKeySigner::random();

    let at_cap = sign_legacy_sized(&signer, 0, CAP.get());
    let hash: B256 = client
        .request("eth_sendRawTransaction", rpc_params![at_cap])
        .await
        .expect("a transaction at the cap lands");
    assert_ne!(hash, B256::ZERO);

    let over_cap = sign_legacy_sized(&signer, 1, CAP.get() + 1);
    let err = client
        .request::<B256, _>("eth_sendRawTransaction", rpc_params![over_cap])
        .await
        .unwrap_err();
    let ClientError::Call(obj) = err else {
        panic!("expected a call error, got {err:?}");
    };
    assert_eq!(obj.code(), -32602);
    assert_eq!(
        obj.message(),
        "oversized data: transaction size 131073 bytes, limit 131072 bytes"
    );

    executor.iter().for_each(tokio::task::JoinHandle::abort);
    handle.stop().unwrap();
}
