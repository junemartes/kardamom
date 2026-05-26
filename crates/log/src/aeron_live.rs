//! Typed facade over a thread-confined `rusteron_client::Aeron` instance.
//!
//! Exists primarily so downstream crates can ask the log layer "give me the
//! handles I need for role X" without having to spell out the M+1 stream
//! topology by hand on every call site. Each method returns an adapter type
//! that owns its own `Pub`/`Sub` and shares the Aeron client via `Rc`
//! (the C client is `!Send + !Sync` — see the doc on
//! [`crate::publisher::Publishers`]).
//!
//! ## Threading
//!
//! `AeronRuntime` itself is `!Send + !Sync` by construction (it holds
//! `Rc<rusteron_client::Aeron>`). Each role wraps its `AeronRuntime` in a
//! dedicated OS thread; cross-thread coordination uses crossbeam/mpsc
//! channels at the role boundary, never the Aeron client itself. This
//! follows the pattern established by the S3-fix-agent learnings —
//! splitting per-tx_data and tx_ordering publishers into separate threads
//! is *allowed* if the role wants the parallelism, because each thread
//! owns its own `AeronRuntime`/`Aeron` pair.
//!
//! ## Roles
//!
//! - **Proxy (ingress) i:** `tx_data_publisher(i)` (its shard's A); the
//!   proxy fans out validated envelopes to the per-shard tx_data stream.
//! - **Sequencer:** `tx_data_subscriber(i)` (warm cache from its shard's A)
//!   + `tx_ordering_publisher()` (races peers to publish refs).
//! - **Executor / batcher:** `tx_data_subscriber(i)` for i in 0..M +
//!   `tx_ordering_subscriber()`.
//! - **Sealer:** `tx_ordering_publisher()` only (emits boundaries).
//!
//! Gated behind the `aeron-live` cargo feature.

use std::rc::Rc;

use crate::config::ChannelsConfig;
use crate::error::LogError;
use crate::publisher::{
    QuorumPublisher, ReceiptCachePublisher, TxDataPublisher, TxOrderingPublisher,
    TxReceiptsPublisher, WatermarkAPublisher, WatermarkPublisher,
};
use crate::subscriber::{
    QuorumSubscriber, ReceiptCacheSubscriber, Subscribers, TxDataSubscriber, TxOrderingSubscriber,
    TxReceiptsSubscriber, WatermarkSubscriber,
};

type AeronClient = rusteron_client::Aeron;

/// Typed handles into the kardamom log tier, parameterised by the
/// [`ChannelsConfig`] passed at construction. One per OS thread that talks
/// to Aeron.
pub struct AeronRuntime {
    aeron: Rc<AeronClient>,
    channels: ChannelsConfig,
}

impl AeronRuntime {
    /// Build a new runtime over an already-started Aeron client.
    ///
    /// Callers typically own the `Aeron` themselves so they can also wrap
    /// it in their own helper types (e.g. for the fsync sidecar's
    /// `SharedPosition` Refresh). The runtime keeps a clone of the `Rc`
    /// so the client lives as long as any handle this runtime returned.
    pub fn new(aeron: Rc<AeronClient>, channels: ChannelsConfig) -> Self {
        Self { aeron, channels }
    }

    pub fn channels(&self) -> &ChannelsConfig {
        &self.channels
    }

    pub fn aeron(&self) -> &Rc<AeronClient> {
        &self.aeron
    }

    // -----------------------------------------------------------------------
    // TxData (per-sequencer, exclusive publication)
    // -----------------------------------------------------------------------

    /// Open the tx_data publisher for shard `sequencer_id`. Wired by the
    /// proxy (ingress) for its shard, which fans validated envelopes onto
    /// the per-shard tx_data stream for sequencers / executors / batchers
    /// to consume.
    pub fn tx_data_publisher(&self, sequencer_id: u8) -> Result<TxDataPublisher, LogError> {
        TxDataPublisher::open(&self.aeron, &self.channels, sequencer_id)
    }

    /// Open a tx_data subscription. Sequencers open the one matching their
    /// shard (warm cache); executors / batchers open M of these (one per
    /// shard).
    pub fn tx_data_subscriber(&self, sequencer_id: u8) -> Result<TxDataSubscriber, LogError> {
        self.subscribers().tx_data(sequencer_id)
    }

    /// Open the per-A fsync watermark publisher. Opened by the proxy host
    /// that owns the tx_data shard.
    pub fn tx_data_watermark_publisher(
        &self,
        sequencer_id: u8,
    ) -> Result<WatermarkAPublisher, LogError> {
        WatermarkAPublisher::open(&self.aeron, &self.channels, sequencer_id)
    }

    /// Subscribe to a per-A fsync watermark.
    pub fn tx_data_watermark_subscriber(
        &self,
        sequencer_id: u8,
    ) -> Result<WatermarkSubscriber, LogError> {
        self.subscribers().watermark_a(sequencer_id)
    }

    // -----------------------------------------------------------------------
    // TxOrdering (canonical orderer, concurrent publication, tiny payload)
    // -----------------------------------------------------------------------

    pub fn tx_ordering_publisher(&self) -> Result<TxOrderingPublisher, LogError> {
        TxOrderingPublisher::open(&self.aeron, &self.channels)
    }

    pub fn tx_ordering_subscriber(&self) -> Result<TxOrderingSubscriber, LogError> {
        self.subscribers().tx_ordering()
    }

    // -----------------------------------------------------------------------
    // TxReceipts (receipts, RAM only)
    // -----------------------------------------------------------------------

    pub fn tx_receipts_publisher(&self) -> Result<TxReceiptsPublisher, LogError> {
        TxReceiptsPublisher::open(&self.aeron, &self.channels)
    }

    pub fn tx_receipts_subscriber(&self) -> Result<TxReceiptsSubscriber, LogError> {
        self.subscribers().tx_receipts()
    }

    // -----------------------------------------------------------------------
    // Receipt cache (proxy ↔ executor)
    // -----------------------------------------------------------------------

    pub fn receipt_cache_publisher(&self) -> Result<ReceiptCachePublisher, LogError> {
        ReceiptCachePublisher::open(&self.aeron, &self.channels)
    }

    pub fn receipt_cache_subscriber(&self) -> Result<ReceiptCacheSubscriber, LogError> {
        self.subscribers().receipt_cache()
    }

    // -----------------------------------------------------------------------
    // TxOrdering fsync watermarks + quorum
    // -----------------------------------------------------------------------

    pub fn tx_ordering_watermark_publisher(
        &self,
        recorder_id: u8,
    ) -> Result<WatermarkPublisher, LogError> {
        WatermarkPublisher::open(&self.aeron, &self.channels, recorder_id)
    }

    pub fn tx_ordering_watermark_subscriber(
        &self,
        recorder_id: u8,
    ) -> Result<WatermarkSubscriber, LogError> {
        self.subscribers().watermark(recorder_id)
    }

    pub fn quorum_publisher(&self) -> Result<QuorumPublisher, LogError> {
        QuorumPublisher::open(&self.aeron, &self.channels)
    }

    pub fn quorum_subscriber(&self) -> Result<QuorumSubscriber, LogError> {
        self.subscribers().quorum()
    }

    fn subscribers(&self) -> Subscribers {
        Subscribers {
            aeron: self.aeron.clone(),
            ch: self.channels.clone(),
        }
    }
}
