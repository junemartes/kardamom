//! Aeron publishers for channel B (canonical, recorded) and channel C
//! (receipts, RAM only).
//!
//! All publishers use `add_publication` (the shared / concurrent variant in
//! Aeron's data model — `AeronExclusivePublication` is the single-publisher
//! variant). Many publisher handles may offer to the same Aeron stream and
//! Aeron will serialize them into a single byte order. That serialization is
//! the canonical L2 ordering (system invariant I1).
//!
//! Channel URIs in [`crate::config::ChannelsConfig`] are stored as `String` for
//! ergonomics; we convert to `CString`/`&CStr` at the FFI boundary since the
//! rusteron 0.1.16x bindings exclusively take `&CStr`.
//!
//! Gated behind the `aeron-live` cargo feature; not built in default
//! `cargo test` runs.

use std::ffi::CString;
use std::sync::Arc;
use std::time::Duration;

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

// rusteron re-exports we depend on. `AeronPublication` is the shared
// (concurrent) variant; `AeronExclusivePublication` is the single-publisher
// variant. We always want shared so concurrent offers serialize through one
// canonical ordering.
type AeronClient = rusteron_client::Aeron;
type Pub = rusteron_client::AeronPublication;

/// How long `add_publication` is allowed to spin waiting for the Media Driver
/// to acknowledge the registration. 5s is generous for ipc/udp; production
/// deployments tune via env vars.
const ADD_PUB_TIMEOUT: Duration = Duration::from_secs(5);

fn cstring(s: &str) -> Result<CString, LogError> {
    CString::new(s).map_err(|e| LogError::Aeron(format!("channel uri contains NUL: {e}")))
}

fn add_pub(aeron: &AeronClient, uri: &str, stream_id: i32, ctx: &str) -> Result<Pub, LogError> {
    let c = cstring(uri)?;
    aeron
        .add_publication(c.as_c_str(), stream_id, ADD_PUB_TIMEOUT)
        .map_err(|e| LogError::Aeron(format!("add_publication {ctx}: {e}")))
}

/// Channel B: canonical tx log. Concurrent (shared) publisher.
pub struct ChannelBPublisher {
    pub_handle: Pub,
}

impl ChannelBPublisher {
    pub fn open(aeron: &AeronClient, ch: &ChannelsConfig) -> Result<Self, LogError> {
        let pub_handle = add_pub(aeron, &ch.b_channel, ch.b_stream_id, "B")?;
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
        let pub_handle = add_pub(aeron, &ch.c_channel, ch.c_stream_id, "C")?;
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
        let pub_handle = add_pub(
            aeron,
            &ch.receipt_cache_channel,
            ch.receipt_cache_stream_id,
            "rc",
        )?;
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
        let pub_handle = add_pub(aeron, &channel, ch.fsync_watermark_stream_id, "wm")?;
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
        let pub_handle = add_pub(
            aeron,
            &ch.quorum_watermark_channel,
            ch.quorum_watermark_stream_id,
            "qwm",
        )?;
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
    // AeronPublication::offer returns the new stream position (>=0) or
    // a negative back-pressure code. Retry up to 1024 times on back-pressure.
    // We pass no reserved-value supplier — the type-erased "None" lives on
    // the `Handlers` utility struct since `None::<&Handler<_>>` cannot
    // infer the closure-callback generic parameter.
    for attempt in 0..1024 {
        let r = p.offer(
            bytes.as_slice(),
            rusteron_client::Handlers::no_reserved_value_supplier_handler(),
        );
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
