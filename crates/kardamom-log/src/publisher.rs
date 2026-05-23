//! Aeron publishers for channel B (canonical, recorded) and channel C
//! (receipts, RAM only).
//!
//! All publishers are concurrent-pub: many publisher handles may offer to the
//! same Aeron stream and Aeron will serialize them into a single byte order.
//! That serialization is the canonical L2 ordering (system invariant I1).
//!
//! **Note:** the exact `rusteron-client` API has drifted between minor
//! releases. The bodies below target the 0.1.16x line; if upstream renames
//! a method, adjust the wrapper to match (see https://docs.rs/rusteron-client).
//! This entire module is gated behind the `aeron-live` cargo feature and is
//! not built in default `cargo test` runs.

use std::sync::Arc;

use rkyv::api::high::HighSerializer;
use rkyv::rancor;
use rkyv::ser::allocator::ArenaHandle;
use rkyv::util::AlignedVec;
use tracing::warn;

use crate::codec;
use crate::config::ChannelsConfig;
use crate::error::LogError;
use kardamom_types::{
    BPosition, BlockBoundary, BlockBoundaryStart, CachedReceipt, FsyncWatermark, QuorumWatermark,
    Receipt, TxEnvelope,
};

// rusteron re-exports we depend on. If the upstream type names drift, this is
// the single point of adjustment.
type AeronClient = rusteron_client::Aeron;
type Pub = rusteron_client::ConcurrentPublication;

/// Channel B: canonical tx log. Concurrent-pub.
pub struct ChannelBPublisher {
    pub_handle: Pub,
}

impl ChannelBPublisher {
    pub fn open(aeron: &AeronClient, ch: &ChannelsConfig) -> Result<Self, LogError> {
        let pub_handle = aeron
            .add_concurrent_publication(&ch.b_channel, ch.b_stream_id)
            .map_err(|e| LogError::Aeron(format!("add_concurrent_publication B: {e}")))?;
        Ok(Self { pub_handle })
    }

    pub fn publish_tx(&self, env: &TxEnvelope) -> Result<BPosition, LogError> {
        offer(&self.pub_handle, env)
    }

    pub fn publish_boundary(&self, b: &BlockBoundaryStart) -> Result<BPosition, LogError> {
        offer(&self.pub_handle, b)
    }
}

/// Channel C: receipts + boundaries. RAM only.
pub struct ChannelCPublisher {
    pub_handle: Pub,
}

impl ChannelCPublisher {
    pub fn open(aeron: &AeronClient, ch: &ChannelsConfig) -> Result<Self, LogError> {
        let pub_handle = aeron
            .add_concurrent_publication(&ch.c_channel, ch.c_stream_id)
            .map_err(|e| LogError::Aeron(format!("add_concurrent_publication C: {e}")))?;
        Ok(Self { pub_handle })
    }

    pub fn publish_receipt(&self, r: &Receipt) -> Result<BPosition, LogError> {
        offer(&self.pub_handle, r)
    }

    pub fn publish_boundary(&self, b: &BlockBoundary) -> Result<BPosition, LogError> {
        offer(&self.pub_handle, b)
    }
}

/// Receipt-cache channel: per-tx `CachedReceipt` messages. RAM only,
/// consumed by short-lived clients (proxy nonce cache, RPC frontends).
pub struct ReceiptCachePublisher {
    pub_handle: Pub,
}

impl ReceiptCachePublisher {
    pub fn open(aeron: &AeronClient, ch: &ChannelsConfig) -> Result<Self, LogError> {
        let pub_handle = aeron
            .add_concurrent_publication(&ch.receipt_cache_channel, ch.receipt_cache_stream_id)
            .map_err(|e| LogError::Aeron(format!("add_concurrent_publication rc: {e}")))?;
        Ok(Self { pub_handle })
    }

    pub fn publish(&self, r: &CachedReceipt) -> Result<BPosition, LogError> {
        offer(&self.pub_handle, r)
    }
}

/// Per-recorder fsync-watermark publisher. Each recorder host opens one of these.
pub struct WatermarkPublisher {
    pub_handle: Pub,
}

impl WatermarkPublisher {
    pub fn open(
        aeron: &AeronClient,
        ch: &ChannelsConfig,
        recorder_id: u8,
    ) -> Result<Self, LogError> {
        let channel = ch
            .fsync_watermark_channel_template
            .replace("{rid}", &recorder_id.to_string());
        let pub_handle = aeron
            .add_concurrent_publication(&channel, ch.fsync_watermark_stream_id)
            .map_err(|e| LogError::Aeron(format!("add_concurrent_publication wm: {e}")))?;
        Ok(Self { pub_handle })
    }

    pub fn publish(&self, w: &FsyncWatermark) -> Result<(), LogError> {
        offer(&self.pub_handle, w).map(|_| ())
    }
}

/// Shared quorum-watermark publisher, used by the aggregator.
pub struct QuorumPublisher {
    pub_handle: Pub,
}

impl QuorumPublisher {
    pub fn open(aeron: &AeronClient, ch: &ChannelsConfig) -> Result<Self, LogError> {
        let pub_handle = aeron
            .add_concurrent_publication(&ch.quorum_watermark_channel, ch.quorum_watermark_stream_id)
            .map_err(|e| LogError::Aeron(format!("add_concurrent_publication qwm: {e}")))?;
        Ok(Self { pub_handle })
    }

    pub fn publish(&self, q: &QuorumWatermark) -> Result<(), LogError> {
        offer(&self.pub_handle, q).map(|_| ())
    }
}

fn offer<T>(p: &Pub, msg: &T) -> Result<BPosition, LogError>
where
    T: for<'a> rkyv::Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>,
{
    let bytes: AlignedVec = codec::encode(msg)?;
    // ConcurrentPublication::offer returns the new stream position (>=0) or
    // a negative back-pressure code. Retry up to 1024 times on back-pressure.
    for attempt in 0..1024 {
        let r = p.offer(bytes.as_slice());
        if r >= 0 {
            return Ok(decode_position(r));
        }
        if attempt % 64 == 63 {
            warn!(attempt, "aeron back-pressure, retrying");
        }
        std::hint::spin_loop();
    }
    Err(LogError::Aeron(
        "back-pressure timeout after 1024 retries".into(),
    ))
}

/// Aeron returns a stream position as `(term_id << 32) | term_offset` packed
/// into i64. Unpack into our `BPosition`.
fn decode_position(p: i64) -> BPosition {
    let term_id = (p >> 32) as i32;
    let term_offset = (p & 0xFFFF_FFFF) as i32;
    BPosition {
        term_id,
        term_offset,
    }
}

/// Bundle of all publishers a single host might need.
#[derive(Clone)]
pub struct Publishers {
    pub aeron: Arc<AeronClient>,
    pub b: Arc<ChannelBPublisher>,
    pub c: Arc<ChannelCPublisher>,
}
