//! Full-services end-to-end test.
//!
//! Spins up real Aeron in Docker, then in-process tasks for every kardamom
//! service:
//!
//!   - **Sealer**: publishes `BlockBoundaryStart` every 250 ms onto
//!     tx_ordering.
//!   - **Sequencer**: subscribes to tx_data[0] (warm cache) and publishes
//!     `TxRef`s onto tx_ordering.
//!   - **Executor**: M=1 tx_data subscriber + tx_ordering subscriber +
//!     tx_receipts publisher. Real revm execution against a
//!     `MockStateDatabase` pre-seeded with two funded accounts (genesis).
//!   - **Relay**: in-process bridge that joins `tx_data` (envelope sender +
//!     nonce + tx_hash) with `tx_receipts` (Receipt) and republishes
//!     `CachedReceipt` to the receipt-cache channel. This component is the
//!     missing piece between executor output and the proxy's pending-
//!     receipt waiter; the test ships it inline.
//!   - **Ingress proxy**: TxData[0] publisher + receipt-cache subscriber.
//!     Exposes `submit_raw` (the JSON-RPC `eth_sendRawTransaction` path).
//!
//! The test seeds two accounts (Alice with 1 ETH, Bob with 0), submits N
//! signed transfers Alice→Bob via `IngressProxy::submit_raw`, and asserts
//! each call returns a receipt with the expected `tx_hash` and
//! `status=true`.
//!
//! Gated on `feature = "full-pipeline-e2e"` + `#[ignore]`. To run locally:
//!
//! ```bash
//! cargo test -p e2e --features full-pipeline-e2e \
//!   --test full_services_e2e -- --ignored --nocapture
//! ```

#![cfg(feature = "full-pipeline-e2e")]

use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;
use std::sync::mpsc as sync_mpsc;
use std::time::Duration;

use alloy_consensus::{SignableTransaction, TxEnvelope as ConsensusEnvelope, TxLegacy};
use alloy_network::TxSignerSync;
use alloy_primitives::{Address, B256, Bytes as AlloyBytes, U256};
use alloy_rlp::Encodable;
use alloy_signer_local::PrivateKeySigner;
use anyhow::{Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use kardamom_executor::{
    CMessage, Executor, ExecutorConfig, ExecutorError, MockStateDatabase, MutatingSnapshotSource,
    StateWriterSignal, TxDataSubscription, TxOrderingSubscription, TxReceiptsPublication,
    WriterApplyingQueue,
};
use kardamom_ingress::channels::{InMemoryStateDb, IngressPublication, IngressSubscription};
use kardamom_ingress::config::IngressConfig;
use kardamom_ingress::error::IngressError;
use kardamom_ingress::proxy::IngressProxy;
use kardamom_log::aeron_live::{
    AeronRuntime, FsyncWatermarkSubscriberHandle, PubHandle, QuorumSubscriberHandle,
    ReceiptCachePublisherHandle, ReceiptCacheSubscriberHandle, TxDataPublisherHandle,
    TxDataSubscriberHandle, TxOrderingPublisherHandle, TxOrderingSubscriberHandle,
    TxReceiptsBoundarySubscriberHandle, TxReceiptsPublisherHandle, TxReceiptsSubscriberHandle,
};
use kardamom_log::config::{ChannelsConfig, LogConfig};
use kardamom_log::testing::AeronTestCluster;
use kardamom_sealer::clock::SystemClock;
use kardamom_sealer::emitter::{BoundaryPublisher, PublishError};
use kardamom_sealer::{Sealer, SealerConfig};
use kardamom_sequencer::config::SequencerConfig;
use kardamom_sequencer::duplicate::DuplicateNotification;
use kardamom_sequencer::error::SequencerError;
use kardamom_sequencer::inbound::TxDataSubscriber;
use kardamom_sequencer::outbound::{ReceiptCachePublisher, TxOrderingRefPublisher};
use kardamom_sequencer::sequencer::{Sequencer, Shutdown};
use kardamom_types::{
    BPosition, BlockBoundary, BlockBoundaryStart, CachedReceipt, FsyncWatermark, QuorumWatermark,
    Receipt, TxEnvelope, TxOrderingMessage, TxRef,
};
use std::collections::HashMap;
use std::sync::Mutex;
use tokio::sync::broadcast;

const CHAIN_ID: u64 = 1;

async fn docker_available() -> bool {
    use tokio::process::Command;
    Command::new("docker")
        .arg("info")
        .output()
        .await
        .map(|o| o.status.success())
        .unwrap_or(false)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "requires Docker; run with `cargo test -p e2e --features full-pipeline-e2e --test full_services_e2e -- --ignored --nocapture`"]
async fn full_services_e2e_signed_transfer_round_trip() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with_test_writer()
        .try_init();

    if !docker_available().await {
        eprintln!("skipping: docker not available");
        return;
    }

    let cluster = AeronTestCluster::single_node()
        .await
        .expect("aeron container up");
    let aeron_dir = cluster.aeron_dir_host(0).to_path_buf();
    tracing::info!(aeron_dir = %aeron_dir.display(), "aeron container running");

    // Session-id-scoped URIs so reruns / concurrent runs don't share state.
    let session = format!("svc-e2e-{}", std::process::id());
    let channels = make_channels(&session);

    // Build the genesis state DB. Two funded accounts; revm executes
    // against the snapshot the SnapshotSource hands out.
    let alice = signer_from_seed(0x11);
    let bob = signer_from_seed(0x22);
    let db = MockStateDatabase::builder()
        .account(
            alice.address(),
            U256::from(1_000_000_000_000_000_000_000u128),
            0,
            B256::ZERO,
        )
        .account(bob.address(), U256::ZERO, 0, B256::ZERO)
        .build();
    tracing::info!(
        alice = %alice.address(),
        bob   = %bob.address(),
        "genesis state DB seeded with funded accounts"
    );

    let rt = AeronRuntime::spawn_with_dir(aeron_dir).expect("aeron runtime");

    // ----- Sealer task --------------------------------------------------
    let sealer_pub =
        TxOrderingPublisherHandle::open(&rt, &channels).expect("sealer tx_ordering pub");
    let sealer_cfg = SealerConfig {
        host_id: 0,
        channel_b_uri: channels.tx_ordering_channel.clone(),
        channel_b_tx_stream_id: 1,
        channel_b_boundary_stream_id: channels.tx_ordering_stream_id,
        tick_interval_ms: 250,
    };
    let sealer = Sealer::new(
        sealer_cfg.clone(),
        SystemClock,
        TxOrderingBoundaryAdapter::new(sealer_pub),
        1,
    )
    .expect("sealer ctor");
    let sealer_task = tokio::spawn(async move {
        if let Err(e) = sealer.run_forever().await {
            tracing::error!(error = %e, "sealer exited with error");
        }
    });

    // ----- Sequencer task ----------------------------------------------
    let seq_a_sub = TxDataSubscriberHandle::open(&rt, &channels, 0).expect("sequencer tx_data sub");
    let seq_b_pub =
        TxOrderingPublisherHandle::open(&rt, &channels).expect("sequencer tx_ordering pub");
    let seq_rc_pub = rt
        .open_publication(
            &channels.receipt_cache_channel,
            channels.receipt_cache_stream_id + 1,
        )
        .expect("sequencer receipt-cache pub (duplicate notifications)");
    let seq_shutdown = Shutdown::new();
    let seq_shutdown_handle = seq_shutdown.clone();
    let seq_cfg = SequencerConfig {
        partition_count: 1,
        partition_index: 0,
        sequencer_id: 0,
        max_pending_per_sender: 16,
        core_id: None,
        backpressure_policy: kardamom_sequencer::config::BackpressurePolicy::ReturnImmediately,
    };
    let seq_state_db = Arc::new(kardamom_sequencer::testing::FakeStateDatabase::new());
    let sequencer_task = tokio::task::spawn_blocking(move || {
        let mut sequencer = Sequencer::new(seq_cfg, seq_state_db);
        let mut a = LiveTxDataSubSeq::new(seq_a_sub);
        let mut b = LiveTxOrderingRefPub::new(seq_b_pub);
        let mut rc = LiveSeqReceiptCachePub::new(seq_rc_pub);
        let _ = sequencer.run(&mut a, &mut b, &mut rc, seq_shutdown_handle);
    });

    // ----- Executor task -----------------------------------------------
    let mut exec_a_handle =
        TxDataSubscriberHandle::open(&rt, &channels, 0).expect("executor tx_data sub");
    let (ea_tx, ea_rx) = sync_mpsc::channel::<(BPosition, TxEnvelope)>();
    tokio::spawn(async move {
        while let Some(item) = exec_a_handle.recv().await {
            if ea_tx.send(item).is_err() {
                break;
            }
        }
    });
    let mut exec_b_handle =
        TxOrderingSubscriberHandle::open(&rt, &channels).expect("executor tx_ordering sub");
    let (eb_tx, eb_rx) = sync_mpsc::channel::<(BPosition, TxOrderingMessage)>();
    tokio::spawn(async move {
        while let Some(item) = exec_b_handle.recv().await {
            if eb_tx.send(item).is_err() {
                break;
            }
        }
    });
    let exec_c_handle =
        TxReceiptsPublisherHandle::open(&rt, &channels).expect("executor tx_receipts pub");
    let snapshots = MutatingSnapshotSource(db.clone());
    let sw_queue = WriterApplyingQueue::new(db.clone());
    let exec_cfg = ExecutorConfig {
        chain_id: CHAIN_ID,
        ..ExecutorConfig::default()
    };
    let executor_task = tokio::task::spawn_blocking(move || {
        let a_subs: Vec<Box<dyn TxDataSubscription>> = vec![Box::new(LiveTxDataSub {
            sequencer_id: 0,
            rx: ea_rx,
        })];
        let b_sub: Box<dyn TxOrderingSubscription> = Box::new(LiveTxOrderingSub { rx: eb_rx });
        let c_pub = LiveTxReceiptsPub {
            handle: exec_c_handle,
        };
        if let Err(e) = Executor::run(
            exec_cfg,
            a_subs,
            b_sub,
            c_pub,
            snapshots,
            ImmediateSignal,
            sw_queue,
            0,
        ) {
            tracing::error!(error = %e, "executor exited with error");
        }
    });

    // ----- Relay task: tx_data + tx_receipts join → receipt_cache -------
    let relay_envelopes: Arc<Mutex<HashMap<B256, TxEnvelope>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let relay_pub =
        ReceiptCachePublisherHandle::open(&rt, &channels).expect("relay receipt-cache pub");
    {
        // Subscribe to tx_data to learn (tx_hash → envelope) so we can
        // recover sender + nonce when a Receipt arrives on tx_receipts.
        let mut sub = TxDataSubscriberHandle::open(&rt, &channels, 0).expect("relay tx_data sub");
        let envelopes = relay_envelopes.clone();
        tokio::spawn(async move {
            while let Some((_pos, env)) = sub.recv().await {
                envelopes.lock().unwrap().insert(env.tx_hash, env);
            }
        });
    }
    {
        let mut sub =
            TxReceiptsSubscriberHandle::open(&rt, &channels).expect("relay tx_receipts sub");
        let envelopes = relay_envelopes.clone();
        let relay_pub = relay_pub.clone();
        tokio::spawn(async move {
            while let Some((_pos, receipt)) = sub.recv().await {
                let env = {
                    let mut tries = 0;
                    loop {
                        if let Some(e) = envelopes.lock().unwrap().get(&receipt.tx_hash).cloned() {
                            break Some(e);
                        }
                        if tries > 50 {
                            break None;
                        }
                        tries += 1;
                        // Envelope may arrive slightly after the Receipt
                        // (Aeron stream interleaving); spin briefly.
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                };
                if let Some(env) = env {
                    // Decode nonce from the RLP envelope.
                    if let Ok(nonce) = decode_nonce(&env.raw_tx) {
                        let cached = CachedReceipt {
                            sender: env.sender,
                            nonce,
                            tx_hash: receipt.tx_hash,
                            receipt: receipt.clone(),
                        };
                        let pub_handle = relay_pub.clone();
                        let _ =
                            tokio::task::spawn_blocking(move || pub_handle.publish(&cached)).await;
                    }
                } else {
                    tracing::warn!(
                        tx_hash = ?receipt.tx_hash,
                        "relay: receipt with no matching envelope"
                    );
                }
            }
        });
    }

    // ----- Ingress proxy -----------------------------------------------
    let publication = LiveIngressPublication::open(&rt, &channels, 1).expect("ingress pub");
    let subscription = LiveIngressSubscription::open(&rt, &channels, 0).expect("ingress sub");
    let mut proxy_cfg = IngressConfig::default();
    proxy_cfg.partition_count_m = 1;
    proxy_cfg.chain_id = CHAIN_ID;
    proxy_cfg.binary_tcp_bind = None;
    proxy_cfg.binary_uds_path = None;
    proxy_cfg.pending_receipt_timeout = Duration::from_secs(15);
    let proxy = IngressProxy::new(
        proxy_cfg,
        publication,
        subscription,
        Arc::new(InMemoryStateDb::new()),
    );

    // Give the subscriber tasks a beat to attach so we don't drop early
    // publishes.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // ----- Submit N signed transfers Alice → Bob ------------------------
    const N: u64 = 5;
    let client_ip: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);
    let mut expected_hashes: Vec<B256> = Vec::with_capacity(N as usize);
    for nonce in 0..N {
        let raw = signed_transfer(&alice, bob.address(), U256::from(1u64), nonce);
        let env: ConsensusEnvelope =
            ConsensusEnvelope::decode(&mut raw.as_slice()).expect("decode my own tx");
        let expected_hash: B256 = *env.tx_hash();
        expected_hashes.push(expected_hash);
        let resp = proxy
            .submit_raw(client_ip, AlloyBytes::from(raw))
            .await
            .expect("submit_raw");
        assert_eq!(
            resp.receipt.tx_hash, expected_hash,
            "tx {nonce}: tx_hash mismatch"
        );
        assert!(resp.receipt.status, "tx {nonce}: receipt status=false");
        tracing::info!(
            nonce,
            tx_hash = ?expected_hash,
            gas_used = resp.receipt.gas_used,
            "received receipt"
        );
    }

    tracing::info!("all {N} receipts validated; tearing down");

    // ----- Teardown ----------------------------------------------------
    seq_shutdown.signal();
    sealer_task.abort();
    let _ = sealer_task.await;
    let _ = sequencer_task.await;
    drop(rt);
    let _ = executor_task.await;
    drop(cluster);
}

// ============================================================================
// Helpers
// ============================================================================

fn signer_from_seed(byte: u8) -> PrivateKeySigner {
    let mut bytes = [0u8; 32];
    bytes[31] = byte;
    PrivateKeySigner::from_bytes(&bytes.into()).expect("derive signer")
}

/// Build a session-scoped ChannelsConfig over IPC.
fn make_channels(session: &str) -> ChannelsConfig {
    let mut c = LogConfig::default().channels;
    c.tx_data_channel_template = format!("aeron:ipc?alias={session}-tx-data-{{sid}}");
    c.tx_data_stream_id_base = 5001;
    c.tx_ordering_channel = format!("aeron:ipc?alias={session}-tx-ordering");
    c.tx_ordering_stream_id = 5100;
    c.tx_receipts_channel = format!("aeron:ipc?alias={session}-tx-receipts");
    c.tx_receipts_stream_id = 5200;
    c.receipt_cache_channel = format!("aeron:ipc?alias={session}-receipt-cache");
    c.receipt_cache_stream_id = 5300;
    c.fsync_watermark_channel_template = format!("aeron:ipc?alias={session}-fsync-{{rid}}");
    c.fsync_watermark_stream_id = 5400;
    c.quorum_watermark_channel = format!("aeron:ipc?alias={session}-quorum");
    c.quorum_watermark_stream_id = 5500;
    c
}

/// Encode a signed legacy transfer Alice → `to` for `value` wei at the given
/// nonce. Returns the RLP-encoded tx bytes (what `eth_sendRawTransaction`
/// accepts).
fn signed_transfer(signer: &PrivateKeySigner, to: Address, value: U256, nonce: u64) -> Vec<u8> {
    let mut tx = TxLegacy {
        chain_id: Some(CHAIN_ID),
        nonce,
        gas_price: 1,
        gas_limit: 21_000,
        to: to.into(),
        value,
        input: Default::default(),
    };
    let sig = signer.sign_transaction_sync(&mut tx).expect("sign");
    let env: ConsensusEnvelope = tx.into_signed(sig).into();
    let mut out = Vec::with_capacity(256);
    env.encode(&mut out);
    out
}

fn decode_nonce(raw_tx: &Bytes) -> Result<u64> {
    let env = ConsensusEnvelope::decode(&mut raw_tx.as_ref())
        .map_err(|e| anyhow::anyhow!("decode envelope: {e}"))?;
    Ok(env.nonce())
}

// ============================================================================
// Adapters
// ============================================================================

// --- Sealer ---------------------------------------------------------------

struct TxOrderingBoundaryAdapter {
    pub_handle: TxOrderingPublisherHandle,
    last_pos: Arc<Mutex<BPosition>>,
}

impl TxOrderingBoundaryAdapter {
    fn new(pub_handle: TxOrderingPublisherHandle) -> Self {
        Self {
            pub_handle,
            last_pos: Arc::new(Mutex::new(BPosition::ZERO)),
        }
    }
}

impl BoundaryPublisher for TxOrderingBoundaryAdapter {
    fn publish(&mut self, msg: &BlockBoundaryStart) -> Result<BPosition, PublishError> {
        match self.pub_handle.publish_boundary(msg) {
            Ok(pos) => {
                *self.last_pos.lock().unwrap() = pos;
                Ok(pos)
            }
            Err(e) => Err(PublishError::Fatal(e.to_string())),
        }
    }

    fn current_tx_tail(&self) -> BPosition {
        *self.last_pos.lock().unwrap()
    }
}

// --- Sequencer ------------------------------------------------------------

struct LiveTxDataSubSeq {
    handle: TxDataSubscriberHandle,
}
impl LiveTxDataSubSeq {
    fn new(handle: TxDataSubscriberHandle) -> Self {
        Self { handle }
    }
}
impl TxDataSubscriber for LiveTxDataSubSeq {
    fn poll(&mut self) -> Result<Option<(BPosition, TxEnvelope)>, SequencerError> {
        Ok(self.handle.try_recv())
    }
}

struct LiveTxOrderingRefPub {
    handle: TxOrderingPublisherHandle,
}
impl LiveTxOrderingRefPub {
    fn new(handle: TxOrderingPublisherHandle) -> Self {
        Self { handle }
    }
}
impl TxOrderingRefPublisher for LiveTxOrderingRefPub {
    fn try_publish_ref(&mut self, r: &TxRef) -> Result<(), SequencerError> {
        self.handle
            .publish(&TxOrderingMessage::TxRef(*r))
            .map(|_| ())
            .map_err(|_| SequencerError::Backpressure)
    }
}

struct LiveSeqReceiptCachePub {
    handle: PubHandle,
}
impl LiveSeqReceiptCachePub {
    fn new(handle: PubHandle) -> Self {
        Self { handle }
    }
}
impl ReceiptCachePublisher for LiveSeqReceiptCachePub {
    fn publish_duplicate(&mut self, notification: DuplicateNotification) {
        let _ = self.handle.publish(&notification);
    }
}

// --- Executor -------------------------------------------------------------

struct LiveTxDataSub {
    sequencer_id: u8,
    rx: sync_mpsc::Receiver<(BPosition, TxEnvelope)>,
}
impl TxDataSubscription for LiveTxDataSub {
    fn sequencer_id(&self) -> u8 {
        self.sequencer_id
    }
    fn next(&mut self) -> Result<(BPosition, TxEnvelope), ExecutorError> {
        self.rx.recv().map_err(|_| ExecutorError::TxDataClosed {
            sequencer_id: self.sequencer_id,
        })
    }
}

struct LiveTxOrderingSub {
    rx: sync_mpsc::Receiver<(BPosition, TxOrderingMessage)>,
}
impl TxOrderingSubscription for LiveTxOrderingSub {
    fn next(&mut self) -> Result<(BPosition, TxOrderingMessage), ExecutorError> {
        self.rx.recv().map_err(|_| ExecutorError::TxOrderingClosed)
    }
}

struct LiveTxReceiptsPub {
    handle: TxReceiptsPublisherHandle,
}
impl TxReceiptsPublication for LiveTxReceiptsPub {
    fn publish(&mut self, msg: CMessage) -> Result<(), ExecutorError> {
        match msg {
            CMessage::Receipt(r) => self
                .handle
                .publish_receipt(&r)
                .map(|_| ())
                .map_err(|e| ExecutorError::State(format!("publish_receipt: {e}"))),
            CMessage::BlockBoundary(b) => self
                .handle
                .publish_boundary(&b)
                .map(|_| ())
                .map_err(|e| ExecutorError::State(format!("publish_boundary: {e}"))),
        }
    }
}

struct ImmediateSignal;
impl StateWriterSignal for ImmediateSignal {
    fn wait_committed(&mut self, b: u64) -> Result<u64, ExecutorError> {
        Ok(b)
    }
}

// --- Ingress --------------------------------------------------------------

#[derive(Clone)]
struct LiveIngressPublication {
    tx_data: Vec<TxDataPublisherHandle>,
    receipt_cache: ReceiptCachePublisherHandle,
}

impl LiveIngressPublication {
    fn open(
        rt: &AeronRuntime,
        channels: &ChannelsConfig,
        shards: u8,
    ) -> Result<Self, IngressError> {
        let mut tx_data = Vec::with_capacity(shards as usize);
        for sid in 0..shards {
            let h = TxDataPublisherHandle::open(rt, channels, sid)
                .map_err(|e| IngressError::Internal(format!("open tx_data[{sid}]: {e}")))?;
            tx_data.push(h);
        }
        let receipt_cache = ReceiptCachePublisherHandle::open(rt, channels)
            .map_err(|e| IngressError::Internal(format!("open receipt_cache: {e}")))?;
        Ok(Self {
            tx_data,
            receipt_cache,
        })
    }
}

#[async_trait]
impl IngressPublication for LiveIngressPublication {
    async fn publish_tx_data(
        &self,
        shard: usize,
        envelope: TxEnvelope,
    ) -> Result<(), IngressError> {
        let h = self
            .tx_data
            .get(shard)
            .ok_or_else(|| IngressError::Internal(format!("shard {shard} OOR")))?
            .clone();
        tokio::task::spawn_blocking(move || h.publish(&envelope))
            .await
            .map_err(|e| IngressError::Internal(format!("join: {e}")))?
            .map(|_| ())
            .map_err(|e| IngressError::Internal(format!("publish_tx_data: {e}")))
    }

    async fn publish_receipt_cache(&self, cached: CachedReceipt) -> Result<(), IngressError> {
        let h = self.receipt_cache.clone();
        tokio::task::spawn_blocking(move || h.publish(&cached))
            .await
            .map_err(|e| IngressError::Internal(format!("join: {e}")))?
            .map(|_| ())
            .map_err(|e| IngressError::Internal(format!("publish_receipt_cache: {e}")))
    }
}

#[derive(Clone)]
struct LiveIngressSubscription {
    receipts: broadcast::Sender<Receipt>,
    watermarks: broadcast::Sender<QuorumWatermark>,
    local_fsync: broadcast::Sender<FsyncWatermark>,
    receipt_cache: broadcast::Sender<CachedReceipt>,
    block_boundaries: broadcast::Sender<BlockBoundary>,
}

impl LiveIngressSubscription {
    fn open(
        rt: &AeronRuntime,
        channels: &ChannelsConfig,
        recorder_id: u8,
    ) -> Result<Self, IngressError> {
        let (receipts_tx, _) = broadcast::channel::<Receipt>(1024);
        let (watermarks_tx, _) = broadcast::channel::<QuorumWatermark>(1024);
        let (local_fsync_tx, _) = broadcast::channel::<FsyncWatermark>(1024);
        let (receipt_cache_tx, _) = broadcast::channel::<CachedReceipt>(1024);
        let (block_boundaries_tx, _) = broadcast::channel::<BlockBoundary>(1024);

        let mut r = TxReceiptsSubscriberHandle::open(rt, channels)
            .map_err(|e| IngressError::Internal(format!("open tx_receipts: {e}")))?;
        let tx = receipts_tx.clone();
        tokio::spawn(async move {
            while let Some((_pos, v)) = r.recv().await {
                let _ = tx.send(v);
            }
        });

        let mut q = QuorumSubscriberHandle::open(rt, channels)
            .map_err(|e| IngressError::Internal(format!("open quorum: {e}")))?;
        let tx = watermarks_tx.clone();
        tokio::spawn(async move {
            while let Some((_pos, v)) = q.recv().await {
                let _ = tx.send(v);
            }
        });

        let mut f = FsyncWatermarkSubscriberHandle::open(rt, channels, recorder_id)
            .map_err(|e| IngressError::Internal(format!("open fsync: {e}")))?;
        let tx = local_fsync_tx.clone();
        tokio::spawn(async move {
            while let Some((_pos, v)) = f.recv().await {
                let _ = tx.send(v);
            }
        });

        let mut c = ReceiptCacheSubscriberHandle::open(rt, channels)
            .map_err(|e| IngressError::Internal(format!("open receipt_cache: {e}")))?;
        let tx = receipt_cache_tx.clone();
        tokio::spawn(async move {
            while let Some((_pos, v)) = c.recv().await {
                let _ = tx.send(v);
            }
        });

        let mut bnd = TxReceiptsBoundarySubscriberHandle::open(rt, channels)
            .map_err(|e| IngressError::Internal(format!("open tx_receipts boundaries: {e}")))?;
        let tx = block_boundaries_tx.clone();
        tokio::spawn(async move {
            while let Some((_pos, v)) = bnd.recv().await {
                let _ = tx.send(v);
            }
        });

        Ok(Self {
            receipts: receipts_tx,
            watermarks: watermarks_tx,
            local_fsync: local_fsync_tx,
            receipt_cache: receipt_cache_tx,
            block_boundaries: block_boundaries_tx,
        })
    }
}

impl IngressSubscription for LiveIngressSubscription {
    fn subscribe_receipts(&self) -> broadcast::Receiver<Receipt> {
        self.receipts.subscribe()
    }
    fn subscribe_watermark(&self) -> broadcast::Receiver<QuorumWatermark> {
        self.watermarks.subscribe()
    }
    fn subscribe_local_fsync_watermark(&self) -> broadcast::Receiver<FsyncWatermark> {
        self.local_fsync.subscribe()
    }
    fn subscribe_receipt_cache(&self) -> broadcast::Receiver<CachedReceipt> {
        self.receipt_cache.subscribe()
    }
    fn subscribe_block_boundaries(&self) -> broadcast::Receiver<BlockBoundary> {
        self.block_boundaries.subscribe()
    }
}
