use alloy_consensus::Transaction;
use alloy_consensus::transaction::SignerRecoverable;
use alloy_eips::eip2718::Decodable2718;
use alloy_primitives::hex;
use jsonrpsee::server::{RpcModule, Server};
use tokio::sync::mpsc;

use super::*;

/// The next nonce the fake ingress answers for every account.
const NEXT_NONCE: u64 = 5;

/// The smoke account of the test.
const ACCOUNT: u32 = 3;

/// A fake ingress answers an account that already sent five transfers.
/// The smoke signs its transfer at nonce 5, not at nonce 0, so a second
/// smoke on a used chain is not a duplicate.
#[tokio::test]
async fn the_smoke_transfer_signs_at_the_next_nonce() {
    let (sent, mut received) = mpsc::unbounded_channel::<String>();
    let mut module = RpcModule::new(sent);
    module
        .register_method("eth_getTransactionCount", |_, _, _| {
            format!("{NEXT_NONCE:#x}")
        })
        .unwrap();
    module
        .register_method("eth_sendRawTransaction", |params, sent, _| {
            sent.send(params.one::<String>().unwrap()).unwrap();
            format!("{:#x}", alloy_primitives::B256::ZERO)
        })
        .unwrap();
    module
        .register_method(
            "eth_getTransactionReceipt",
            |_, _, _| serde_json::json!({"status": "0x1"}),
        )
        .unwrap();
    let server = Server::builder().build("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", server.local_addr().unwrap());
    let handle = server.start(module);

    Rpc::new(&url, 1)
        .unwrap()
        .transfer_smoke(ACCOUNT, Duration::from_secs(5))
        .await
        .unwrap();

    let raw = hex::decode(received.try_recv().unwrap()).unwrap();
    let envelope = TxEnvelope::decode_2718(&mut raw.as_slice()).unwrap();
    assert_eq!(envelope.nonce(), NEXT_NONCE);
    assert_eq!(
        envelope.recover_signer().unwrap(),
        Rpc::genesis_signer(ACCOUNT).unwrap().address
    );
    handle.stop().unwrap();
}
