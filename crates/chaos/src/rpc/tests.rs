use std::sync::atomic::{AtomicUsize, Ordering};

use alloy_consensus::Transaction;
use alloy_consensus::transaction::SignerRecoverable;
use alloy_eips::eip2718::Decodable2718;
use alloy_primitives::hex;
use jsonrpsee::server::{RpcModule, Server, ServerHandle};
use jsonrpsee::types::ErrorObjectOwned;
use tokio::sync::mpsc;

use std::sync::Arc;

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

/// A fake ingress whose `eth_getTransactionCount` refuses `refusals`
/// times with `refusal`, then answers [`NEXT_NONCE`]. The context counts
/// the calls.
struct RefusingIngress {
    calls: Arc<AtomicUsize>,
    url: String,
    handle: ServerHandle,
}

impl RefusingIngress {
    async fn start(refusals: usize, refusal: ErrorObjectOwned) -> Self {
        let calls = Arc::new(AtomicUsize::new(0));
        let mut module = RpcModule::new(Arc::clone(&calls));
        module
            .register_method("eth_getTransactionCount", move |_, calls, _| {
                let call = calls.fetch_add(1, Ordering::SeqCst);
                if call < refusals {
                    return Err(refusal.clone());
                }
                Ok(format!("{NEXT_NONCE:#x}"))
            })
            .unwrap();
        let server = Server::builder().build("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", server.local_addr().unwrap());
        let handle = server.start(module);
        Self { calls, url, handle }
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

/// The ingress answers `account state unavailable` when no executor
/// answers in its timeout, as on a stall of the whole runner. The read
/// repeats and gets the nonce once an executor answers.
#[tokio::test]
async fn a_nonce_read_repeats_through_account_state_unavailable() {
    let refusal = ErrorObjectOwned::owned::<()>(
        i32::try_from(STATE_UNAVAILABLE_CODE).unwrap(),
        "account state unavailable: executor query failed: http://executor-1:9024: \
         error sending request",
        None,
    );
    let ingress = RefusingIngress::start(2, refusal).await;

    let nonce = Rpc::new(&ingress.url, 1)
        .unwrap()
        .nonce_of(ACCOUNT)
        .await
        .unwrap();

    assert_eq!(nonce, NEXT_NONCE);
    assert_eq!(ingress.calls(), 3);
    ingress.handle.stop().unwrap();
}

/// Any other refusal is not a transient read failure: the read fails at
/// once, with the refusal in the error.
#[tokio::test]
async fn a_nonce_read_fails_at_once_on_another_refusal() {
    let refusal = ErrorObjectOwned::owned::<()>(
        -32602,
        "block 9 not served: the ingress answers only the head",
        None,
    );
    let ingress = RefusingIngress::start(1, refusal).await;

    let err = Rpc::new(&ingress.url, 1)
        .unwrap()
        .nonce_of(ACCOUNT)
        .await
        .unwrap_err();

    assert!(err.to_string().contains("not served"), "{err:#}");
    assert_eq!(ingress.calls(), 1);
    ingress.handle.stop().unwrap();
}
