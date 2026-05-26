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
//! splitting per-channel-A and channel-B publishers into separate threads
//! is *allowed* if the role wants the parallelism, because each thread
//! owns its own `AeronRuntime`/`Aeron` pair.
//!
//! ## Roles
//!
//! - **Sequencer i:** `tx_data_publisher(i)` (its own A) +
//!   `tx_ordering_publisher()` (the canonical orderer).
//! - **Executor / batcher:** `tx_data_subscriber(i)` for i in 0..M +
//!   `tx_ordering_subscriber()`.
//! - **Sealer:** `tx_ordering_publisher()` only (emits boundaries).
//!
//! Gated behind the `aeron-live` cargo feature.

use std::rc::Rc;

use crate::config::ChannelsConfig;
use crate::error::LogError;
use crate::publisher::{
    ChannelCPublisher, QuorumPublisher, ReceiptCachePublisher, TxDataPublisher,
    TxOrderingPublisher, WatermarkAPublisher, WatermarkPublisher,
};
use crate::subscriber::{
    ChannelBSubscriber, ChannelCReceiptSubscriber, QuorumSubscriber, ReceiptCacheSubscriber,
    Subscribers, TxDataSubscriber, WatermarkSubscriber,
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

    /// Open the channel-A publisher for sequencer `sequencer_id`. Calling
    /// this on a non-sequencer host is a programmer error — the per-A
    /// stream is exclusive-publisher and the only writer must be the
    /// sequencer that owns the partition.
    pub fn tx_data_publisher(&self, sequencer_id: u8) -> Result<TxDataPublisher, LogError> {
        TxDataPublisher::open(&self.aeron, &self.channels, sequencer_id)
    }

    /// Open a channel-A subscription. Executors / batchers open M of these
    /// (one per sequencer).
    pub fn tx_data_subscriber(&self, sequencer_id: u8) -> Result<TxDataSubscriber, LogError> {
        self.subscribers().a(sequencer_id)
    }

    /// Open the per-A fsync watermark publisher. Each sequencer's fsync
    /// sidecar opens one of these.
    pub fn channel_a_watermark_publisher(
        &self,
        sequencer_id: u8,
    ) -> Result<WatermarkAPublisher, LogError> {
        WatermarkAPublisher::open(&self.aeron, &self.channels, sequencer_id)
    }

    /// Subscribe to a per-A fsync watermark.
    pub fn channel_a_watermark_subscriber(
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

    pub fn tx_ordering_subscriber(&self) -> Result<ChannelBSubscriber, LogError> {
        self.subscribers().b()
    }

    // -----------------------------------------------------------------------
    // Channel C (receipts, RAM only)
    // -----------------------------------------------------------------------

    pub fn channel_c_publisher(&self) -> Result<ChannelCPublisher, LogError> {
        ChannelCPublisher::open(&self.aeron, &self.channels)
    }

    pub fn channel_c_receipt_subscriber(&self) -> Result<ChannelCReceiptSubscriber, LogError> {
        self.subscribers().c_receipts()
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

    pub fn channel_b_watermark_publisher(
        &self,
        recorder_id: u8,
    ) -> Result<WatermarkPublisher, LogError> {
        WatermarkPublisher::open(&self.aeron, &self.channels, recorder_id)
    }

    pub fn channel_b_watermark_subscriber(
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
