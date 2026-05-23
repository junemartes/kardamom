//! Aeron subscribers for channels B, C, per-recorder watermark streams, and
//! the aggregated quorum watermark.
//!
//! Gated behind the `aeron-live` cargo feature; see `publisher.rs` for the
//! same caveats around `rusteron-client` API drift.

use std::sync::Arc;

use rkyv::api::high::{HighDeserializer, HighValidator};
use rkyv::rancor;

use crate::codec;
use crate::config::ChannelsConfig;
use crate::error::LogError;
use kardamom_types::{
    BPosition, CachedReceipt, FsyncWatermark, QuorumWatermark, Receipt, TxEnvelope,
};

type AeronClient = rusteron_client::Aeron;
type Sub = rusteron_client::Subscription;
type Header<'a> = rusteron_client::Header<'a>;

/// Generic single-stream subscriber over a typed message. Materializes each
/// fragment into an owned `T` for ergonomics. Hot-path consumers that want
/// zero-copy use [`TypedSubscriber::poll_zero_copy`] which hands
/// `&Archived<T>` directly to the callback.
pub struct TypedSubscriber<T> {
    sub: Sub,
    _marker: std::marker::PhantomData<T>,
}

impl<T> TypedSubscriber<T>
where
    T: rkyv::Archive + 'static,
    T::Archived: rkyv::Deserialize<T, HighDeserializer<rancor::Error>>
        + for<'a> rkyv::bytecheck::CheckBytes<HighValidator<'a, rancor::Error>>,
{
    pub fn open(aeron: &AeronClient, channel: &str, stream_id: i32) -> Result<Self, LogError> {
        let sub = aeron
            .add_subscription(channel, stream_id)
            .map_err(|e| LogError::Aeron(format!("add_subscription {channel}: {e}")))?;
        Ok(Self {
            sub,
            _marker: std::marker::PhantomData,
        })
    }

    /// Poll once and invoke `f` with an owned `T` on every fragment that
    /// arrived in this poll cycle. Returns the number of fragments processed.
    pub fn poll<F: FnMut(T, BPosition)>(&mut self, mut f: F, fragment_limit: usize) -> usize {
        self.sub.poll(
            |bytes: &[u8], header: Header<'_>| match codec::materialize::<T>(bytes) {
                Ok(v) => f(
                    v,
                    BPosition {
                        term_id: header.term_id(),
                        term_offset: header.term_offset(),
                    },
                ),
                Err(e) => tracing::error!(error = %e, "decode failed"),
            },
            fragment_limit,
        )
    }

    /// Zero-copy poll: invoke `f` with a borrowed `&Archived<T>` view that
    /// lives only for the duration of the callback.
    pub fn poll_zero_copy<F: FnMut(&T::Archived, BPosition)>(
        &mut self,
        mut f: F,
        fragment_limit: usize,
    ) -> usize {
        self.sub.poll(
            |bytes: &[u8], header: Header<'_>| match codec::access::<T>(bytes) {
                Ok(view) => f(
                    view,
                    BPosition {
                        term_id: header.term_id(),
                        term_offset: header.term_offset(),
                    },
                ),
                Err(e) => tracing::error!(error = %e, "access failed"),
            },
            fragment_limit,
        )
    }
}

pub type ChannelBSubscriber = TypedSubscriber<TxEnvelope>;
pub type ChannelCReceiptSubscriber = TypedSubscriber<Receipt>;
pub type ReceiptCacheSubscriber = TypedSubscriber<CachedReceipt>;
pub type WatermarkSubscriber = TypedSubscriber<FsyncWatermark>;
pub type QuorumSubscriber = TypedSubscriber<QuorumWatermark>;

/// Convenience bundle.
pub struct Subscribers {
    pub aeron: Arc<AeronClient>,
    pub ch: ChannelsConfig,
}

impl Subscribers {
    pub fn b(&self) -> Result<ChannelBSubscriber, LogError> {
        TypedSubscriber::open(&self.aeron, &self.ch.b_channel, self.ch.b_stream_id)
    }

    pub fn c_receipts(&self) -> Result<ChannelCReceiptSubscriber, LogError> {
        TypedSubscriber::open(&self.aeron, &self.ch.c_channel, self.ch.c_stream_id)
    }

    pub fn receipt_cache(&self) -> Result<ReceiptCacheSubscriber, LogError> {
        TypedSubscriber::open(
            &self.aeron,
            &self.ch.receipt_cache_channel,
            self.ch.receipt_cache_stream_id,
        )
    }

    pub fn watermark(&self, recorder_id: u8) -> Result<WatermarkSubscriber, LogError> {
        let channel = self
            .ch
            .fsync_watermark_channel_template
            .replace("{rid}", &recorder_id.to_string());
        TypedSubscriber::open(&self.aeron, &channel, self.ch.fsync_watermark_stream_id)
    }

    pub fn quorum(&self) -> Result<QuorumSubscriber, LogError> {
        TypedSubscriber::open(
            &self.aeron,
            &self.ch.quorum_watermark_channel,
            self.ch.quorum_watermark_stream_id,
        )
    }
}
