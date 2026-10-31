//! The controlled peers of the ingress: one in-process stand-in for the
//! sequencer, the sealer and the executor, on its own media driver, and
//! an HTTP stub for the egress and the executor query. They speak the
//! production codecs: `tx_data` envelopes in, `tx_receipts` batch frames
//! out, and rkyv receipts on the query.
//!
//! The pipeline orders like the sealer: the first copy of a transaction
//! wins, and a sender's nonces run in order with no gap. It keeps a
//! ledger of every executed transfer, so a test can require each one
//! exactly once.
//!
//! The pipeline reads each lane as the sequencer does: a late join of a
//! new ingress session refetches the frames before the join from the
//! archive of that ingress ([`JoinRepair`]).

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use alloy_primitives::{Address, B256, U256};
use kardamom_log::aeron_live::{AeronRuntime, PubHandle, TxDataSubscriberHandle};
use kardamom_log::config::AeronConfig;
use kardamom_log::refetch::{ArchiveRefetcher, EndpointSource, JoinRepair, RefetchConfig};
use kardamom_types::{AccountRow, Receipt, ReceiptBatch, TxEnvelope};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::Channels;

/// One executed transfer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Executed {
    pub(crate) sender: Address,
    pub(crate) nonce: u64,
    pub(crate) hash: B256,
}

/// What the pipeline holds: the executed transfers in order, the receipt
/// of each, the next nonce of each sender, and the transfers that wait
/// for a gap.
#[derive(Default)]
pub(crate) struct Ledger {
    pub(crate) executed: Vec<Executed>,
    pub(crate) receipts: HashMap<B256, Receipt>,
    next: HashMap<Address, u64>,
    parked: BTreeMap<(Address, u64), TxEnvelope>,
    seen: HashSet<B256>,
    /// Copies of a transfer that the pipeline already took.
    pub(crate) copies: u64,
}

impl Ledger {
    /// Take one envelope: drop a copy, park a nonce after a gap, and
    /// return the transfers that run now, in order.
    fn take(&mut self, envelope: TxEnvelope) -> Vec<TxEnvelope> {
        if !self.seen.insert(envelope.tx_hash) {
            self.copies += 1;
            return Vec::new();
        }
        let nonce = kardamom_ingress::test_support::nonce_of(&envelope.raw_tx);
        let sender = envelope.sender;
        self.parked.insert((sender, nonce), envelope);
        let mut run = Vec::new();
        while let Some(next) = self.parked.remove(&(sender, self.next_of(sender))) {
            run.push(self.execute(next));
        }
        run
    }

    fn next_of(&self, sender: Address) -> u64 {
        self.next.get(&sender).copied().unwrap_or(0)
    }

    fn execute(&mut self, envelope: TxEnvelope) -> TxEnvelope {
        let nonce = self.next_of(envelope.sender);
        self.next.insert(envelope.sender, nonce + 1);
        let at = i32::try_from(self.executed.len() + 1).unwrap() * 64;
        let receipt =
            kardamom_ingress::test_support::receipt(envelope.sender, nonce, envelope.tx_hash, at);
        self.receipts.insert(envelope.tx_hash, receipt);
        self.executed.push(Executed {
            sender: envelope.sender,
            nonce,
            hash: envelope.tx_hash,
        });
        envelope
    }

    /// The receipt frame of one executed transfer, with its sender's row.
    fn frame(&self, envelope: &TxEnvelope) -> ReceiptBatch {
        ReceiptBatch {
            receipts: vec![self.receipts[&envelope.tx_hash].clone()],
            accounts: vec![AccountRow {
                address: envelope.sender,
                nonce: self.next_of(envelope.sender),
                balance: U256::from(10u64).pow(U256::from(24u64)),
            }],
        }
    }
}

/// The pipeline: the `tx_data` subscriptions of every ingress, and a
/// receipt publication to every ingress, all on one runtime.
struct Pump {
    lanes: Vec<Lane>,
    receipts: Vec<PubHandle>,
    ledger: Arc<Mutex<Ledger>>,
}

/// The largest late-join gap that the pipeline refetches, as the
/// sequencer's bound.
const MAX_JOIN_GAP: u64 = 64 << 20;

/// The `tx_data` lane 0 of one ingress, with the repair of its late joins.
struct Lane {
    sub: TxDataSubscriberHandle,
    repair: JoinRepair,
    refetcher: ArchiveRefetcher,
}

impl Lane {
    /// Lane 0 of `channels`, whose frames the archive at `archive` records.
    /// The pipeline subscribes before any ingress starts, so it needs no
    /// start grace.
    fn open(
        rt: &AeronRuntime,
        aeron_dir: &std::path::Path,
        channels: &Channels,
        archive: &str,
    ) -> Self {
        let free = || format!("127.0.0.1:{}", super::free_udp_port());
        let refetcher = ArchiveRefetcher::new(RefetchConfig {
            tx_data_endpoints: EndpointSource::Static(vec![archive.to_owned()]),
            tx_deposits_endpoints: EndpointSource::Static(Vec::new()),
            response_endpoint: free(),
            replay_endpoint: free(),
            aeron_dir: Some(aeron_dir.to_path_buf()),
            aeron: AeronConfig::default(),
        });
        Self {
            sub: channels.subscriber(rt),
            repair: JoinRepair::new(
                Channels::defaults().tx_data_stream_id(0),
                Duration::ZERO,
                MAX_JOIN_GAP,
            ),
            refetcher,
        }
    }

    /// The envelopes that arrived, after the repair of a late join.
    fn arrived(&mut self) -> Vec<TxEnvelope> {
        while let Some(item) = self.sub.try_recv_item() {
            self.repair.take(item, &mut self.refetcher);
        }
        std::iter::from_fn(|| self.repair.pop())
            .map(|frame| frame.envelope)
            .collect()
    }
}

impl Pump {
    /// Drain every subscription once, and publish the receipts of what
    /// ran, best effort as the executor's publisher does. A publication to
    /// an ingress that is down drops the frame and never holds the pump;
    /// the ingress reads that receipt from the query stub.
    fn turn(&mut self) {
        let arrived: Vec<TxEnvelope> = self.lanes.iter_mut().flat_map(Lane::arrived).collect();
        let frames: Vec<ReceiptBatch> = {
            let mut ledger = self.ledger.lock().unwrap();
            let ran: Vec<TxEnvelope> = arrived.into_iter().flat_map(|e| ledger.take(e)).collect();
            ran.iter().map(|e| ledger.frame(e)).collect()
        };
        frames
            .iter()
            .flat_map(|frame| self.receipts.iter().map(move |r| (frame, r)))
            .for_each(|(frame, r)| {
                r.publish_best_effort(kardamom_log::codec::encode(frame).unwrap());
            });
    }
}

/// The running pipeline. Its thread stops on drop.
pub(crate) struct Pipeline {
    pub(crate) ledger: Arc<Mutex<Ledger>>,
    stop: Arc<std::sync::atomic::AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Pipeline {
    /// Start the pipeline on the driver at `aeron_dir`, with the channels
    /// of every ingress and the archive control endpoint that records each.
    pub(crate) fn start(
        aeron_dir: &std::path::Path,
        ingresses: &[Channels],
        archives: &[String],
    ) -> Self {
        let ledger = Arc::new(Mutex::new(Ledger::default()));
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let (dir, channels) = (aeron_dir.to_path_buf(), ingresses.to_vec());
        let archives = archives.to_vec();
        let (shared, stopped) = (ledger.clone(), stop.clone());
        let thread = std::thread::spawn(move || {
            let rt = AeronRuntime::spawn_with_dir(&dir).unwrap();
            let mut pump = Pump {
                lanes: channels
                    .iter()
                    .zip(&archives)
                    .map(|(c, archive)| Lane::open(&rt, &dir, c, archive))
                    .collect(),
                receipts: channels.iter().map(|c| c.receipts_publisher(&rt)).collect(),
                ledger: shared,
            };
            while !stopped.load(std::sync::atomic::Ordering::Acquire) {
                Self::pause_then(&mut pump);
            }
        });
        Self {
            ledger,
            stop,
            thread: Some(thread),
        }
    }

    fn pause_then(pump: &mut Pump) {
        std::thread::sleep(Duration::from_millis(5));
        pump.turn();
    }

    /// The executed transfers of `sender`.
    pub(crate) fn executed_by(&self, sender: Address) -> Vec<Executed> {
        let ledger = self.ledger.lock().unwrap();
        ledger
            .executed
            .iter()
            .filter(|e| e.sender == sender)
            .cloned()
            .collect()
    }
}

impl Drop for Pipeline {
    fn drop(&mut self) {
        self.stop.store(true, std::sync::atomic::Ordering::Release);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// An HTTP stub of the egress and the executor query: it answers a
/// receipt query from the ledger, as rkyv hex, and `null` for anything
/// it does not hold.
pub(crate) struct QueryStub {
    pub(crate) url: String,
    task: tokio::task::JoinHandle<()>,
}

impl QueryStub {
    pub(crate) async fn start(ledger: Arc<Mutex<Ledger>>) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            while let Ok((sock, _)) = listener.accept().await {
                tokio::spawn(Self::answer(sock, ledger.clone()));
            }
        });
        Self { url, task }
    }

    async fn answer(mut sock: tokio::net::TcpStream, ledger: Arc<Mutex<Ledger>>) {
        let request = Self::read_request(&mut sock).await;
        let result = {
            let ledger = ledger.lock().unwrap();
            ledger
                .receipts
                .iter()
                .find(|(hash, _)| request.contains(&format!("{hash}")))
                .map_or("null".to_string(), |(_, r)| {
                    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(r).unwrap();
                    format!("\"0x{}\"", alloy_primitives::hex::encode(&bytes))
                })
        };
        let body = format!(r#"{{"jsonrpc":"2.0","id":1,"result":{result}}}"#);
        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\nx-state-block: 9\r\n\
             content-length: {}\r\nconnection: close\r\n\r\n{body}",
            body.len()
        );
        let _ = sock.write_all(response.as_bytes()).await;
    }

    async fn read_request(sock: &mut tokio::net::TcpStream) -> String {
        let mut request = Vec::new();
        while !request.ends_with(b"}") && Self::read_more(sock, &mut request).await {}
        String::from_utf8_lossy(&request).into_owned()
    }

    /// Read one chunk into `request`. False at the end of the stream.
    async fn read_more(sock: &mut tokio::net::TcpStream, request: &mut Vec<u8>) -> bool {
        let mut buf = [0u8; 4096];
        let n = sock.read(&mut buf).await.unwrap_or(0);
        request.extend_from_slice(&buf[..n]);
        n > 0
    }
}

impl Drop for QueryStub {
    fn drop(&mut self) {
        self.task.abort();
    }
}
