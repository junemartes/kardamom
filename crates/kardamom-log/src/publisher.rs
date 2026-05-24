//! Aeron publishers for the split-architecture log tier (D-Sh12):
//!
//! - **Channel A[i]** — per-sequencer **exclusive** publication of full
//!   `TxEnvelope` bytes. One per sequencer; the sequencer is the only
//!   publisher to its own channel-A stream, so there is no CAS-cursor
//!   contention and writes run at near-`memcpy` speed.
//! - **Channel B** — canonical orderer. Shared (concurrent) multi-publisher
//!   carrying the small [`ChannelBMessage`] enum (`TxRef | BoundaryStart`).
//!   Many publisher handles (M sequencers + the sealer) may offer to the
//!   same Aeron stream; Aeron serialises them into a single byte order, and
//!   that order *is* the canonical L2 ordering (system invariant I1).
//! - **Channel C** — receipts + boundaries. RAM only.
//! - **Watermark / quorum-watermark / receipt-cache** — auxiliary streams.
//!
//! Channel URIs in [`crate::config::ChannelsConfig`] are stored as `String`
//! for ergonomics; we convert to `CString`/`&CStr` at the FFI boundary since
//! the rusteron 0.1.16x bindings exclusively take `&CStr`.
//!
//! Gated behind the `aeron-live` cargo feature; not built in default
//! `cargo test` runs.

use std::ffi::CString;
use std::rc::Rc;
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
    BPosition, BlockBoundary, BlockBoundaryStart, CachedReceipt, ChannelBMessage, FsyncWatermark,
    QuorumWatermark, Receipt, TxEnvelope, TxRef,
};

// rusteron re-exports we depend on. `AeronPublication` is the shared
// (concurrent) variant; `AeronExclusivePublication` is the single-publisher
// variant. Channel B uses shared (concurrent multi-publisher serialised into
// the canonical order); channel A uses exclusive (one writer per stream,
// near-memcpy speed).
type AeronClient = rusteron_client::Aeron;
type Pub = rusteron_client::AeronPublication;
type ExclusivePub = rusteron_client::AeronExclusivePublication;

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

fn add_exclusive_pub(
    aeron: &AeronClient,
    uri: &str,
    stream_id: i32,
    ctx: &str,
) -> Result<ExclusivePub, LogError> {
    let c = cstring(uri)?;
    aeron
        .add_exclusive_publication(c.as_c_str(), stream_id, ADD_PUB_TIMEOUT)
        .map_err(|e| LogError::Aeron(format!("add_exclusive_publication {ctx}: {e}")))
}

// ===========================================================================
// Channel A[i] — per-sequencer exclusive publisher of full TxEnvelopes.
// ===========================================================================

/// Channel A[i]: per-sequencer exclusive publisher of full `TxEnvelope`
/// bytes. The sequencer is the only writer to its own A-stream — no
/// CAS-cursor contention — and the recorded archive is the source of truth
/// for the bulk transaction data referenced from channel B.
///
/// Per-A URIs are derived from
/// [`ChannelsConfig::a_channel_template`] (e.g. `"aeron:ipc?alias=a-{sid}"`)
/// with `{sid}` substituted for the sequencer id. Stream ids are derived as
/// `a_stream_id_base + sequencer_id` so M parallel channel-A streams can
/// coexist on a shared Media Driver.
pub struct ChannelAPublisher {
    sequencer_id: u8,
    pub_handle: ExclusivePub,
}

impl ChannelAPublisher {
    pub fn open(
        aeron: &AeronClient,
        ch: &ChannelsConfig,
        sequencer_id: u8,
    ) -> Result<Self, LogError> {
        let uri = ch
            .a_channel_template
            .replace("{sid}", &sequencer_id.to_string());
        let stream_id = ch.a_stream_id_base + sequencer_id as i32;
        let pub_handle = add_exclusive_pub(aeron, &uri, stream_id, "A")?;
        Ok(Self {
            sequencer_id,
            pub_handle,
        })
    }

    pub fn sequencer_id(&self) -> u8 {
        self.sequencer_id
    }

    /// Publish a full `TxEnvelope` to channel A[i]. Returns the fragment's
    /// position on the channel-A stream — the value the sequencer will hand
    /// to [`kardamom_types::TxRef::new`] when writing the canonical record
    /// to channel B.
    pub fn publish(&self, env: &TxEnvelope) -> Result<BPosition, LogError> {
        offer_exclusive(&self.pub_handle, env)
    }
}

// ===========================================================================
// Channel B — canonical orderer, tiny refs (+ sealer boundaries).
// ===========================================================================

/// Channel B: canonical orderer. Concurrent (shared) publisher carrying
/// [`ChannelBMessage`] records — `TxRef` (~16 B) and `BlockBoundaryStart`.
/// Bulk transaction bytes flow on the per-sequencer channel-A archives;
/// channel B only ever sees small records, so the canonical-orderer CAS
/// cursor stays cheap.
pub struct ChannelBPublisher {
    pub_handle: Pub,
}

impl ChannelBPublisher {
    pub fn open(aeron: &AeronClient, ch: &ChannelsConfig) -> Result<Self, LogError> {
        let pub_handle = add_pub(aeron, &ch.b_channel, ch.b_stream_id, "B")?;
        Ok(Self { pub_handle })
    }

    /// Publish a [`TxRef`] onto channel B. Returns the canonical B-position
    /// of the record (system invariant I1).
    pub fn publish_ref(&self, r: &TxRef) -> Result<BPosition, LogError> {
        offer(&self.pub_handle, &ChannelBMessage::TxRef(*r))
    }

    /// Publish a sealer-emitted boundary onto channel B.
    pub fn publish_boundary(&self, b: &BlockBoundaryStart) -> Result<BPosition, LogError> {
        offer(&self.pub_handle, &ChannelBMessage::BoundaryStart(b.clone()))
    }

    /// Lower-level publish: hand an already-built [`ChannelBMessage`] to
    /// the publisher. Useful when downstream batches refs and boundaries
    /// before emitting.
    pub fn publish_message(&self, m: &ChannelBMessage) -> Result<BPosition, LogError> {
        offer(&self.pub_handle, m)
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

/// Per-channel-B-recorder fsync-watermark publisher. Each B-recorder host
/// opens one of these; the quorum aggregator subscribes to all N.
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

/// Per-channel-A fsync-watermark publisher (D-Sh12). One per sequencer
/// host; downstream consumers (ack-path coordinator, executor) subscribe
/// to whichever A-watermarks they care about. Channel A is single-host
/// durability by default — there is no quorum aggregator for A.
pub struct WatermarkAPublisher {
    pub_handle: Pub,
}

impl WatermarkAPublisher {
    pub fn open(
        aeron: &AeronClient,
        ch: &ChannelsConfig,
        sequencer_id: u8,
    ) -> Result<Self, LogError> {
        let channel = ch
            .fsync_watermark_a_channel_template
            .replace("{sid}", &sequencer_id.to_string());
        let stream_id = ch.fsync_watermark_a_stream_id_base + sequencer_id as i32;
        let pub_handle = add_pub(aeron, &channel, stream_id, "wm-a")?;
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
    let len = bytes.len();
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
            // Aeron returns the position *after* the message; the caller
            // wants the fragment's *start* (so the value it embeds in a
            // TxRef matches what the subscriber later sees as the
            // fragment's term_offset).
            return Ok(decode_position(r - len as i64));
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

/// Same as `offer` but for the exclusive-publisher variant. Channel A
/// publishers go through this path so the data-bulk write avoids the
/// concurrent-pub CAS cursor entirely (spec §2.3).
fn offer_exclusive<T>(p: &ExclusivePub, msg: &T) -> Result<BPosition, LogError>
where
    T: for<'a> rkyv::Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>,
{
    let bytes: AlignedVec = codec::encode(msg)?;
    let len = bytes.len();
    for attempt in 0..1024 {
        let r = p.offer(
            bytes.as_slice(),
            rusteron_client::Handlers::no_reserved_value_supplier_handler(),
        );
        if r >= 0 {
            return Ok(decode_position(r - len as i64));
        }
        if attempt % 64 == 63 {
            warn!(attempt, "aeron back-pressure (channel A), retrying");
        }
        std::hint::spin_loop();
    }
    Err(LogError::Aeron(
        "back-pressure timeout after 1024 retries (channel A)".into(),
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

/// Bundle of all publishers a single host might need. Uses `Rc` (not `Arc`)
/// because `AeronClient` is thread-confined (`!Send + !Sync`) and the whole
/// publisher set lives on one Aeron-client thread by design.
///
/// `a` is the per-sequencer channel-A publisher — only the sequencer hosts
/// populate it; executor / batcher / sealer hosts leave it `None`.
#[derive(Clone)]
pub struct Publishers {
    pub aeron: Rc<AeronClient>,
    pub a: Option<Rc<ChannelAPublisher>>,
    pub b: Rc<ChannelBPublisher>,
    pub c: Rc<ChannelCPublisher>,
}
