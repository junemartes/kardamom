//! The client of a case: one sender submits transfers through one
//! ingress, one nonce at a time. A failed submit tries again with the
//! same bytes. While a submit waits, the client reads the receipt from
//! every ingress: a submit that its ingress lost can still have landed.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use alloy_primitives::{B256, Bytes, keccak256};
use alloy_signer_local::PrivateKeySigner;
use jsonrpsee::core::client::ClientT;
use jsonrpsee::http_client::{HttpClient, HttpClientBuilder};
use jsonrpsee::rpc_params;

/// How long one nonce may take before the client gives up on it.
const GIVE_UP: Duration = Duration::from_secs(50);

pub(crate) fn client(url: &str) -> HttpClient {
    HttpClientBuilder::default()
        .request_timeout(Duration::from_secs(40))
        .build(url)
        .unwrap()
}

/// One signed transfer.
#[derive(Clone)]
pub(crate) struct Signed {
    pub(crate) raw: Bytes,
    pub(crate) hash: B256,
}

/// What the client saw.
#[derive(Default, Debug)]
pub(crate) struct Report {
    /// The transfers that landed, in nonce order.
    pub(crate) landed: Vec<B256>,
    /// The nonce that the client gave up on, with its last error.
    pub(crate) lost: Option<(u64, String)>,
    pub(crate) submits: u64,
}

/// A running client.
pub(crate) struct Load {
    pub(crate) sender: PrivateKeySigner,
    landed: Arc<AtomicU64>,
    /// The error of the last failed submit, for a stall report.
    last_error: tokio::sync::watch::Receiver<String>,
    stop: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<Report>,
}

struct Client {
    signer: PrivateKeySigner,
    submit: HttpClient,
    readers: Vec<HttpClient>,
    landed: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
    last_error: tokio::sync::watch::Sender<String>,
}

impl Client {
    async fn run(self) -> Report {
        let mut report = Report::default();
        while !self.stop.load(Ordering::Acquire) && report.lost.is_none() {
            self.deliver(&mut report).await;
        }
        report
    }

    /// Submit the next nonce until it lands or the client gives up.
    async fn deliver(&self, report: &mut Report) {
        let nonce = u64::try_from(report.landed.len()).unwrap();
        let raw = kardamom_ingress::test_support::sign_legacy(&self.signer, nonce);
        let tx = Signed {
            hash: keccak256(&raw),
            raw,
        };
        let end = Instant::now() + GIVE_UP;
        let mut last = String::new();
        let mut landed = false;
        while !landed && Instant::now() < end {
            report.submits += 1;
            landed = self.attempt(&tx, &mut last).await;
        }
        if !landed {
            report.lost = Some((nonce, last));
            return;
        }
        report.landed.push(tx.hash);
        self.landed.fetch_add(1, Ordering::AcqRel);
    }

    /// One submit, raced with receipt reads on every ingress.
    async fn attempt(&self, tx: &Signed, last: &mut String) -> bool {
        let raw = format!("0x{}", alloy_primitives::hex::encode(&tx.raw));
        let sent = tokio::select! {
            sent = self.submit.request::<B256, _>("eth_sendRawTransaction", rpc_params![raw]) => sent,
            () = self.await_receipt(tx.hash) => return true,
        };
        match sent {
            Ok(hash) => hash == tx.hash,
            Err(e) => {
                *last = e.to_string();
                self.last_error.send_replace(last.clone());
                tokio::time::sleep(Duration::from_millis(250)).await;
                self.any_receipt(tx.hash).await
            }
        }
    }

    async fn await_receipt(&self, hash: B256) {
        while !self.any_receipt_after_pause(hash).await {}
    }

    async fn any_receipt_after_pause(&self, hash: B256) -> bool {
        tokio::time::sleep(Duration::from_secs(1)).await;
        self.any_receipt(hash).await
    }

    async fn any_receipt(&self, hash: B256) -> bool {
        let reads = futures::future::join_all(self.readers.iter().map(|r| receipt(r, hash))).await;
        reads.into_iter().any(|r| r)
    }
}

/// Whether `reader` serves a receipt for `hash`.
pub(crate) async fn receipt(reader: &HttpClient, hash: B256) -> bool {
    reader
        .request::<Option<serde_json::Value>, _>(
            "eth_getTransactionReceipt",
            rpc_params![format!("{hash}")],
        )
        .await
        .ok()
        .flatten()
        .is_some()
}

impl Load {
    /// Start a fresh sender that submits through `submit`.
    pub(crate) fn start(submit: &str, readers: &[String]) -> Self {
        let sender = PrivateKeySigner::random();
        let landed = Arc::new(AtomicU64::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let (last_error, last_error_rx) = tokio::sync::watch::channel(String::new());
        let task = tokio::spawn(
            Client {
                signer: sender.clone(),
                submit: client(submit),
                readers: readers.iter().map(|u| client(u)).collect(),
                landed: landed.clone(),
                stop: stop.clone(),
                last_error,
            }
            .run(),
        );
        Self {
            sender,
            landed,
            last_error: last_error_rx,
            stop,
            task,
        }
    }

    pub(crate) fn landed(&self) -> u64 {
        self.landed.load(Ordering::Acquire)
    }

    /// Wait until more than `floor` transfers landed.
    pub(crate) async fn await_above(&self, floor: u64, bound: Duration) -> Duration {
        let started = Instant::now();
        tokio::time::timeout(bound, async {
            while self.landed() <= floor {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap_or_else(|_| {
            panic!(
                "the load lands nothing past {floor} within {bound:?}; last submit error: {:?}",
                *self.last_error.borrow()
            )
        });
        started.elapsed()
    }

    /// Stop after the nonce in flight.
    pub(crate) async fn finish(self) -> Report {
        self.stop.store(true, Ordering::Release);
        self.task.await.unwrap()
    }
}
