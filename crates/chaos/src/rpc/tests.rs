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

#[tokio::test]
async fn a_receipted_transfer_waits_for_its_committed_nonce_and_balance() {
    let reads = Arc::new(AtomicUsize::new(0));
    let mut module = RpcModule::new(Arc::clone(&reads));
    module
        .register_method("eth_getTransactionCount", |_, reads, _| {
            if reads.fetch_add(1, Ordering::Relaxed) == 0 {
                "0x5"
            } else {
                "0x6"
            }
        })
        .unwrap();
    module
        .register_method("eth_getBalance", |_, reads, _| {
            if reads.load(Ordering::Relaxed) < 2 {
                "0x64"
            } else {
                "0x63"
            }
        })
        .unwrap();
    let server = Server::builder().build("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", server.local_addr().unwrap());
    let handle = server.start(module);
    let state = Rpc::new(&url, 1)
        .unwrap()
        .await_account_nonce(
            ACCOUNT,
            6,
            Budget::new(Duration::from_secs(1), Duration::from_millis(10)),
        )
        .await
        .unwrap();
    assert_eq!(state.nonce, 6);
    assert_eq!(state.balance, U256::from(99));
    assert!(reads.load(Ordering::Relaxed) >= 2);
    assert!(
        Rpc::new(&url, 1)
            .unwrap()
            .await_account_nonce(ACCOUNT, 7, Budget::new(Duration::ZERO, Duration::ZERO))
            .await
            .is_err(),
        "another nonce cannot satisfy the exact commit fence"
    );
    handle.stop().unwrap();
}

#[tokio::test]
async fn an_account_read_keeps_the_full_balance_quantity() {
    let mut module = RpcModule::new(());
    module
        .register_method("eth_getTransactionCount", |_, &(), _| {
            format!("{NEXT_NONCE:#x}")
        })
        .unwrap();
    module
        .register_method("eth_getBalance", |_, &(), _| format!("{:#x}", U256::MAX))
        .unwrap();
    let server = Server::builder().build("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", server.local_addr().unwrap());
    let handle = server.start(module);
    let account = Rpc::new(&url, 1)
        .unwrap()
        .account_state(ACCOUNT)
        .await
        .unwrap();
    assert_eq!(
        account.address,
        Rpc::genesis_signer(ACCOUNT).unwrap().address
    );
    assert_eq!(account.nonce, NEXT_NONCE);
    assert_eq!(account.balance, U256::MAX);
    handle.stop().unwrap();
}

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

/// A connection that the ingress refuses is no answer: the read repeats
/// within its budget, and the error names the ingress.
#[tokio::test(start_paused = true)]
async fn a_nonce_read_repeats_through_a_refused_connection() {
    // The listener closes at the end of the block, so the port refuses.
    let url = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        format!("http://{}", listener.local_addr().unwrap())
    };
    let err = Rpc::new(&url, 1)
        .unwrap()
        .nonce_of(ACCOUNT)
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("the ingress did not connect"),
        "{err:#}"
    );
    assert!(err.to_string().contains(&url), "{err:#}");
}
