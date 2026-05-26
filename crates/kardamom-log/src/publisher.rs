//! Aeron publishers for the split-architecture log tier (D-Sh12):
//!
//! - **TxData[i]** — per-sequencer **exclusive** publication of full
//!   `TxEnvelope` bytes. One per sequencer; the sequencer is the only
//!   publisher to its own tx_data stream, so there is no CAS-cursor
//!   contention and writes run at near-`memcpy` speed.
//! - **TxOrdering** — canonical orderer. Shared (concurrent) multi-publisher
//!   carrying the small [`TxOrderingMessage`] enum (`TxRef | BoundaryStart`).
//!   Many publisher handles (M sequencers + the sealer) may offer to the
//!   same Aeron stream; Aeron serialises them into a single byte order, and
//!   that order *is* the canonical L2 ordering (system invariant I1).
//! - **TxReceipts** — receipts + boundaries. RAM only.
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
    BPosition, BlockBoundary, BlockBoundaryStart, CachedReceipt, FsyncWatermark, QuorumWatermark,
    Receipt, TxEnvelope, TxOrderingMessage, TxRef,
};

// rusteron re-exports we depend on. `AeronPublication` is the shared
// (concurrent) variant; `AeronExclusivePublication` is the single-publisher
// variant. TxOrdering uses shared (concurrent multi-publisher serialised into
// the canonical order); tx_data uses exclusive (one writer per stream,
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
// TxData[i] — per-sequencer exclusive publisher of full TxEnvelopes.
// ===========================================================================

/// TxData[i]: per-sequencer exclusive publisher of full `TxEnvelope`
/// bytes. The sequencer is the only writer to its own A-stream — no
/// CAS-cursor contention — and the recorded archive is the source of truth
/// for the bulk transaction data referenced from tx_ordering.
///
/// Per-A URIs are derived from
/// [`ChannelsConfig::tx_dattx_data_channel_template`] (e.g. `"aeron:ipc?alias=a-{sid}"`)
/// with `{sid}` substituted for the sequencer id. Stream ids are derived as
/// `tx_dattx_data_stream_id_base + sequencer_id` so M parallel tx_data streams can
/// coexist on a shared Media Driver.
pub struct TxDataPublisher {
    sequencer_id: u8,
    pub_handle: ExclusivePub,
}

impl TxDataPublisher {
    pub fn open(
        aeron: &AeronClient,
        ch: &ChannelsConfig,
        sequencer_id: u8,
    ) -> Result<Self, LogError> {
        let pub_handle = add_exclusive_pub(
            aeron,
            &ch.a_channel(sequencer_id),
            ch.a_stream_id(sequencer_id),
            "A",
        )?;
        Ok(Self {
            sequencer_id,
            pub_handle,
        })
    }

    pub fn sequencer_id(&self) -> u8 {
        self.sequencer_id
    }

    /// Publish a full `TxEnvelope` to tx_data[i]. Returns the fragment's
    /// position on the tx_data stream — the value the sequencer will hand
    /// to [`kardamom_types::TxRef::new`] when writing the canonical record
    /// to tx_ordering.
    pub fn publish(&self, env: &TxEnvelope) -> Result<BPosition, LogError> {
        offer(&self.pub_handle, env)
    }
}

// ===========================================================================
// TxOrdering — canonical orderer, tiny refs (+ sealer boundaries).
// ===========================================================================

/// TxOrdering: canonical orderer. Concurrent (shared) publisher carrying
/// [`TxOrderingMessage`] records — `TxRef` (~16 B) and `BlockBoundaryStart`.
/// Bulk transaction bytes flow on the per-sequencer tx_data archives;
/// tx_ordering only ever sees small records, so the canonical-orderer CAS
/// cursor stays cheap.
pub struct TxOrderingPublisher {
    pub_handle: Pub,
}

impl TxOrderingPublisher {
    pub fn open(aeron: &AeronClient, ch: &ChannelsConfig) -> Result<Self, LogError> {
        let pub_handle = add_pub(
            aeron,
            &ch.tx_ordering_channel,
            ch.tx_ordering_stream_id,
            "B",
        )?;
        Ok(Self { pub_handle })
    }

    /// Publish a [`TxRef`] onto tx_ordering. Returns the canonical B-position
    /// of the record (system invariant I1).
    pub fn publish_ref(&self, r: &TxRef) -> Result<BPosition, LogError> {
        offer(&self.pub_handle, &TxOrderingMessage::TxRef(*r))
    }

    /// Publish a sealer-emitted boundary onto tx_ordering.
    pub fn publish_boundary(&self, b: &BlockBoundaryStart) -> Result<BPosition, LogError> {
        offer(
            &self.pub_handle,
            &TxOrderingMessage::BoundaryStart(b.clone()),
        )
    }

    /// Lower-level publish: hand an already-built [`TxOrderingMessage`] to
    /// the publisher. Useful when downstream batches refs and boundaries
    /// before emitting.
    pub fn publish_message(&self, m: &TxOrderingMessage) -> Result<BPosition, LogError> {
        offer(&self.pub_handle, m)
    }
}

/// TxReceipts: receipts + boundaries. RAM only.
pub struct ChannelCPublisher {
    pub_handle: Pub,
}

impl ChannelCPublisher {
    pub fn open(aeron: &AeronClient, ch: &ChannelsConfig) -> Result<Self, LogError> {
        let pub_handle = add_pub(
            aeron,
            &ch.tx_receipts_channel,
            ch.tx_receipts_stream_id,
            "C",
        )?;
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

/// Per-tx_ordering-recorder fsync-watermark publisher. Each B-recorder host
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
        let pub_handle = add_pub(
            aeron,
            &ch.fsync_watermark_channel(recorder_id),
            ch.fsync_watermark_stream_id,
            "wm",
        )?;
        Ok(Self { pub_handle })
    }

    pub fn publish(&self, w: &FsyncWatermark) -> Result<(), LogError> {
        offer(&self.pub_handle, w).map(|_| ())
    }
}

/// Per-tx_data fsync-watermark publisher (D-Sh12). One per sequencer
/// host; downstream consumers (ack-path coordinator, executor) subscribe
/// to whichever A-watermarks they care about. TxData is single-host
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
        let pub_handle = add_pub(
            aeron,
            &ch.fsync_watermark_a_channel(sequencer_id),
            ch.fsync_watermark_a_stream_id(sequencer_id),
            "wm-a",
        )?;
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

/// Unifies the two Aeron publication variants behind a single `offer_bytes`.
/// `Pub` (concurrent) is used by channels B/C/watermark/quorum/receipt-cache;
/// `ExclusivePub` (single-writer) by tx_data[i]. Both expose the same
/// `i64`-returning `offer` over `&[u8]` — the trait just lets us write one
/// retry loop instead of two.
trait OfferBytes {
    fn offer_bytes(&self, bytes: &[u8]) -> i64;
}

impl OfferBytes for Pub {
    #[inline]
    fn offer_bytes(&self, bytes: &[u8]) -> i64 {
        self.offer(
            bytes,
            rusteron_client::Handlers::no_reserved_value_supplier_handler(),
        )
    }
}

impl OfferBytes for ExclusivePub {
    #[inline]
    fn offer_bytes(&self, bytes: &[u8]) -> i64 {
        self.offer(
            bytes,
            rusteron_client::Handlers::no_reserved_value_supplier_handler(),
        )
    }
}

fn offer<P, T>(p: &P, msg: &T) -> Result<BPosition, LogError>
where
    P: OfferBytes,
    T: for<'a> rkyv::Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>,
{
    let bytes: AlignedVec = codec::encode(msg)?;
    let len = bytes.len();
    // `AeronPublication::offer` returns the new stream position (>=0) or
    // a negative back-pressure code. Retry up to 1024 times on back-pressure.
    for attempt in 0..1024 {
        let r = p.offer_bytes(bytes.as_slice());
        if r >= 0 {
            // Aeron returns the position *after* the message; callers
            // want the fragment's *start* (so the value embedded in a
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
/// `a` is the per-sequencer tx_data publisher — only the sequencer hosts
/// populate it; executor / batcher / sealer hosts leave it `None`.
#[derive(Clone)]
pub struct Publishers {
    pub aeron: Rc<AeronClient>,
    pub a: Option<Rc<TxDataPublisher>>,
    pub b: Rc<TxOrderingPublisher>,
    pub c: Rc<ChannelCPublisher>,
}
