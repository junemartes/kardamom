//! A fake ingress for the unit tests. It holds one account's committed
//! nonce and balance, and it lands each transaction at once. It applies
//! a transaction when the account has funds and the transaction's nonce
//! is the committed nonce. Otherwise it skips the transaction, as the
//! executors do: the receipt has status 0 and the nonce stays. A
//! transaction whose hash the chain saw before does not run again: its
//! old receipt stays.

use std::collections::HashMap;
use std::num::NonZeroU32;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use alloy_consensus::{Transaction, TxEnvelope};
use alloy_eips::eip2718::Decodable2718;
use alloy_primitives::{Address, B256, Bytes, TxKind, U64, U256};
use jsonrpsee::server::{RpcModule, Server, ServerHandle};
use serde_json::{Value, json};
use tokio::sync::watch;

use crate::config::{Endpoint, Timing};
use crate::feed::Board;
use crate::probes::Context;
use crate::ring::{self, Call, Ring};
use crate::rpc::Rpc;
use crate::store::Store;
use crate::wait::Poll;

/// The public test mnemonic.
const MNEMONIC: &str = "test test test test test test test test test test test junk";
const CHAIN_ID: u64 = 1;

/// The state of the fake chain.
#[derive(Debug, Default)]
pub(crate) struct Chain {
    pub(crate) nonce: u64,
    pub(crate) balance: U256,
    /// Whether an applied transaction reverts.
    pub(crate) revert: bool,
    /// The number of submits the chain took.
    pub(crate) sends: usize,
    receipts: HashMap<B256, Value>,
}

impl Chain {
    fn take(&mut self, raw: &Bytes) -> B256 {
        let tx = TxEnvelope::decode_2718(&mut raw.as_ref()).expect("a signed transaction");
        let hash = *tx.tx_hash();
        self.sends += 1;
        if self.receipts.contains_key(&hash) {
            return hash;
        }
        let applied = !self.balance.is_zero() && tx.nonce() == self.nonce;
        self.nonce += u64::from(applied);
        let status = U64::from(u64::from(applied && !self.revert));
        self.receipts
            .insert(hash, json!({ "status": status, "blockNumber": "0x1" }));
        hash
    }
}

/// A fake ingress on a local port, and a client of it.
pub(crate) struct FakeIngress {
    pub(crate) chain: Arc<Mutex<Chain>>,
    pub(crate) rpc: Rpc,
    dir: tempfile::TempDir,
    _server: ServerHandle,
}

impl FakeIngress {
    pub(crate) async fn start() -> Self {
        let chain = Arc::new(Mutex::new(Chain::default()));
        let mut module = RpcModule::new(Arc::clone(&chain));
        module
            .register_method("eth_gasPrice", |_, _, _| json!("0x1"))
            .unwrap();
        module
            .register_method("eth_getTransactionCount", |_, chain, _| {
                json!(U64::from(chain.lock().unwrap().nonce))
            })
            .unwrap();
        module
            .register_method("eth_getBalance", |_, chain, _| {
                json!(chain.lock().unwrap().balance)
            })
            .unwrap();
        module
            .register_method("kardamom_sendRawTransactionAsync", |params, chain, _| {
                let raw: Bytes = params.one().unwrap();
                json!(chain.lock().unwrap().take(&raw))
            })
            .unwrap();
        module
            .register_method("eth_getTransactionReceipt", |params, chain, _| {
                let hash: B256 = params.one().unwrap();
                let receipt = chain.lock().unwrap().receipts.get(&hash).cloned();
                receipt.unwrap_or(Value::Null)
            })
            .unwrap();
        let server = Server::builder().build("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", server.local_addr().unwrap());
        let endpoint: Endpoint = url.parse().unwrap();
        Self {
            chain,
            rpc: Rpc::new(endpoint, Duration::from_secs(5)).unwrap(),
            dir: tempfile::tempdir().unwrap(),
            _server: server.start(module),
        }
    }

    /// The data directory of the test.
    pub(crate) fn dir(&self) -> &Path {
        self.dir.path()
    }

    /// Set the account's balance on the fake chain.
    pub(crate) fn fund(&self, balance: u64) {
        self.chain.lock().unwrap().balance = U256::from(balance);
    }

    /// The ring of the first test account, with its journal under the
    /// test's directory.
    pub(crate) async fn ring(&self) -> Ring {
        let signers = ring::signers(MNEMONIC, 0, NonZeroU32::MIN).unwrap();
        let (board, handle) = Board::new();
        tokio::spawn(board.run());
        Ring::open(&self.dir().join("ring"), signers, CHAIN_ID, handle)
            .await
            .unwrap()
    }

    /// The probes' context over `ring` and this ingress.
    pub(crate) fn context(&self, ring: Ring) -> Arc<Context> {
        let short = Duration::from_millis(300);
        let timing = Timing {
            transfer: short,
            read: short,
            contract: short,
            balance: short,
            receipt_timeout: short,
            feed_grace: short,
            read_timeout: short,
            poll: Self::POLL,
            rwa: short,
            swap: short,
            liquidity: short,
            fees: short,
            safe_timeout: short,
            deposit: short,
            l1_timeout: short,
            credit_timeout: short,
        };
        let (board, handle) = Board::new();
        tokio::spawn(board.run());
        Arc::new(Context {
            ring: Arc::new(ring),
            endpoints: vec![self.rpc.clone()],
            board: handle,
            timing,
            store: Store::new(self.dir()),
            anchor: watch::channel(None).0,
            safe_sample: NonZeroU32::MIN,
        })
    }

    const POLL: Duration = Duration::from_millis(10);

    /// A short poll for the waits of the tests.
    pub(crate) fn wait() -> Poll {
        Poll::within(Duration::from_millis(300), Self::POLL)
    }

    /// A plain transfer.
    pub(crate) fn call() -> Call {
        Call::new(
            TxKind::Call(Address::repeat_byte(9)),
            U256::from(1),
            Bytes::new(),
            21_000,
        )
    }
}
