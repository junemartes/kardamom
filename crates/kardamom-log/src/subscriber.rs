//! Aeron subscribers for channels B, C, per-recorder watermark streams, and
//! the aggregated quorum watermark.
//!
//! Gated behind the `aeron-live` cargo feature; see `publisher.rs` for the
//! same caveats around `rusteron-client` API drift. Channel URIs cross the
//! FFI boundary as `&CStr` (rusteron 0.1.16x) — we own a `CString` per
//! subscription for the duration of the call.

use std::ffi::CString;
use std::rc::Rc;
use std::time::Duration;

use rkyv::api::high::{HighDeserializer, HighValidator};
use rkyv::rancor;

use crate::codec;
use crate::config::ChannelsConfig;
use crate::error::LogError;
use kardamom_types::{
    BPosition, CachedReceipt, FsyncWatermark, QuorumWatermark, Receipt, TxEnvelope,
    TxOrderingMessage,
};

type AeronClient = rusteron_client::Aeron;
type Sub = rusteron_client::AeronSubscription;
type Header = rusteron_client::AeronHeader;

/// Mirror of [`crate::publisher::ADD_PUB_TIMEOUT`].
const ADD_SUB_TIMEOUT: Duration = Duration::from_secs(5);

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
        let c = CString::new(channel)
            .map_err(|e| LogError::Aeron(format!("channel uri contains NUL: {e}")))?;
        // `Handlers::no_*` provides the type-erased "None" the bindgen-generated
        // generic add_subscription requires (a bare `None` cannot infer the
        // callback generic parameter).
        let sub = aeron
            .add_subscription(
                c.as_c_str(),
                stream_id,
                rusteron_client::Handlers::no_available_image_handler(),
                rusteron_client::Handlers::no_unavailable_image_handler(),
                ADD_SUB_TIMEOUT,
            )
            .map_err(|e| LogError::Aeron(format!("add_subscription {channel}: {e}")))?;
        Ok(Self {
            sub,
            _marker: std::marker::PhantomData,
        })
    }

    /// Poll once and invoke `f` with an owned `T` on every fragment that
    /// arrived in this poll cycle. Returns the number of fragments processed.
    pub fn poll<F: FnMut(T, BPosition)>(&mut self, mut f: F, fragment_limit: usize) -> usize {
        let n = self.sub.poll_once(
            |bytes: &[u8], header: Header| match (
                codec::materialize::<T>(bytes),
                header_pos(&header),
            ) {
                (Ok(v), Some(pos)) => f(v, pos),
                (Err(e), _) => tracing::error!(error = %e, "decode failed"),
                (Ok(_), None) => tracing::error!("header values unavailable; dropping fragment"),
            },
            fragment_limit,
        );
        n.unwrap_or(0).max(0) as usize
    }

    /// Zero-copy poll: invoke `f` with a borrowed `&Archived<T>` view that
    /// lives only for the duration of the callback.
    pub fn poll_zero_copy<F: FnMut(&T::Archived, BPosition)>(
        &mut self,
        mut f: F,
        fragment_limit: usize,
    ) -> usize {
        let n = self.sub.poll_once(
            |bytes: &[u8], header: Header| match (codec::access::<T>(bytes), header_pos(&header)) {
                (Ok(view), Some(pos)) => f(view, pos),
                (Err(e), _) => tracing::error!(error = %e, "access failed"),
                (Ok(_), None) => tracing::error!("header values unavailable; dropping fragment"),
            },
            fragment_limit,
        );
        n.unwrap_or(0).max(0) as usize
    }
}

/// Extract `(term_id, term_offset)` from an Aeron header. Returns `None` if
/// the C bindings fail to populate the values struct (should be rare; we log
/// and skip the fragment in that case).
fn header_pos(h: &Header) -> Option<BPosition> {
    let v = h.get_values().ok()?;
    let frame = v.frame();
    Some(BPosition {
        term_id: frame.term_id(),
        term_offset: frame.term_offset(),
    })
}

/// TxData[i]: per-sequencer subscription of full `TxEnvelope` bytes.
/// Executors run M of these (one per sequencer); the per-A reader buffers
/// envelopes keyed by `BPosition` until the corresponding `TxRef` arrives
/// on channel B (spec §2.4).
pub type TxDataSubscriber = TypedSubscriber<TxEnvelope>;

/// TxOrdering: canonical orderer. Yields [`TxOrderingMessage`] records
/// (`TxRef | BoundaryStart`). The `BPosition` handed to the callback is the
/// fragment's canonical L2 position (system invariant I1).
pub type ChannelBSubscriber = TypedSubscriber<TxOrderingMessage>;
pub type ChannelCReceiptSubscriber = TypedSubscriber<Receipt>;
pub type ReceiptCacheSubscriber = TypedSubscriber<CachedReceipt>;
pub type WatermarkSubscriber = TypedSubscriber<FsyncWatermark>;
pub type QuorumSubscriber = TypedSubscriber<QuorumWatermark>;

/// Convenience bundle. Uses `Rc` (not `Arc`) because `AeronClient` is
/// thread-confined (`!Send + !Sync`) and the entire subscriber stack lives
/// on one Aeron-client thread by design.
pub struct Subscribers {
    pub aeron: Rc<AeronClient>,
    pub ch: ChannelsConfig,
}

impl Subscribers {
    /// Open the channel-A subscription for sequencer `sequencer_id`. Run
    /// M of these on each executor host to feed the per-A buffer.
    pub fn a(&self, sequencer_id: u8) -> Result<TxDataSubscriber, LogError> {
        TypedSubscriber::open(
            &self.aeron,
            &self.ch.a_channel(sequencer_id),
            self.ch.a_stream_id(sequencer_id),
        )
    }

    pub fn b(&self) -> Result<ChannelBSubscriber, LogError> {
        TypedSubscriber::open(
            &self.aeron,
            &self.ch.tx_ordering_channel,
            self.ch.tx_ordering_stream_id,
        )
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
        TypedSubscriber::open(
            &self.aeron,
            &self.ch.fsync_watermark_channel(recorder_id),
            self.ch.fsync_watermark_stream_id,
        )
    }

    /// Subscribe to a sequencer-local channel-A fsync watermark stream
    /// (one publisher per sequencer; see `fsync_watermark_tx_data_channel_template`).
    pub fn watermark_a(&self, sequencer_id: u8) -> Result<WatermarkSubscriber, LogError> {
        TypedSubscriber::open(
            &self.aeron,
            &self.ch.fsync_watermark_a_channel(sequencer_id),
            self.ch.fsync_watermark_a_stream_id(sequencer_id),
        )
    }

    pub fn quorum(&self) -> Result<QuorumSubscriber, LogError> {
        TypedSubscriber::open(
            &self.aeron,
            &self.ch.quorum_watermark_channel,
            self.ch.quorum_watermark_stream_id,
        )
    }
}
