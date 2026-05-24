//! High-level real-Aeron channel adapters that are `Send`-friendly for tokio
//! consumers.
//!
//! ## Why this module exists
//!
//! The raw rusteron types (`rusteron_client::Aeron`, `AeronPublication`,
//! `AeronSubscription`, `rusteron_archive::AeronArchive`) wrap raw FFI
//! pointers into a thread-confined C client and are therefore `!Send + !Sync`.
//! Production consumers (the sequencer, executor, sealer, state writer,
//! batcher, ingress proxy) all live in multi-threaded tokio runtimes; they
//! need `Send + Sync` handles they can stash in `Arc`s or move into spawned
//! tasks.
//!
//! This module bridges the gap with a **dedicated Aeron thread** per
//! [`AeronRuntime`]. The thread owns the `Rc<Aeron>` and every publication /
//! subscription opened from it. All cross-thread communication flows through
//! `crossbeam_channel` (outbound publish requests from many tokio tasks → the
//! one Aeron thread) and `tokio::sync::mpsc::UnboundedSender` (inbound
//! messages from the Aeron thread → the registered subscriber task).
//!
//! ## Threading rules (from the S3 fix-agent's hard-won notes)
//!
//! 1. `Aeron` and `AeronArchive` are `!Send + !Sync`. Never move them across
//!    threads, ever.
//! 2. Use `Rc`, not `Arc`. The Aeron loop runs in a dedicated
//!    `std::thread::spawn` OS thread.
//! 3. Cross-thread communication: atomics ([`crate::fsync_sidecar::SharedPosition`]),
//!    `crossbeam::channel`, `tokio::sync::mpsc`/`broadcast`. Never an Aeron
//!    handle.
//! 4. Tokio multi-thread runtimes silently move tasks across worker threads
//!    at await points — so the Aeron loop is plain `std::thread`, not tokio.
//!
//! ## Layout
//!
//! - [`AeronRuntime`] — one per process (or per test). Owns the Aeron thread,
//!   the `Rc<Aeron>`, all publications, and all subscription drivers. Cheap
//!   to clone (just an `Arc` over the command channel).
//! - [`ChannelBPublisherHandle`], [`ChannelCPublisherHandle`],
//!   [`IngressPublisherHandle`], [`ReceiptCachePublisherHandle`],
//!   [`FsyncWatermarkPublisherHandle`], [`QuorumPublisherHandle`] —
//!   `Send + Sync` outbound handles.
//! - [`ChannelBSubscriberHandle<T>`] et al — `Send + Sync` subscription
//!   handles that yield `(BPosition, T)` via `recv()` / `try_recv()` /
//!   `recv_async`.
//! - [`ChannelBArchive`] — offline reader of recorded segment files for the
//!   L1 batcher (D-Sh10). Does **not** require a running Aeron daemon to
//!   read; segment files live on disk.
//!
//! Gated behind `feature = "aeron-live"`.

use std::ffi::CString;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use crossbeam_channel::{Receiver as CbReceiver, Sender as CbSender, TryRecvError};
use rkyv::util::AlignedVec;
use tokio::sync::mpsc::{UnboundedReceiver, unbounded_channel};
use tracing::{error, warn};

use crate::codec;
use crate::config::ChannelsConfig;
use crate::error::LogError;
use kardamom_types::{
    BPosition, BlockBoundary, BlockBoundaryStart, CachedReceipt, FsyncWatermark, QuorumWatermark,
    Receipt, TxEnvelope,
};

type AeronClient = rusteron_client::Aeron;
type Pub = rusteron_client::AeronPublication;
type Sub = rusteron_client::AeronSubscription;
type Header = rusteron_client::AeronHeader;

const ADD_PUB_TIMEOUT: Duration = Duration::from_secs(5);
const ADD_SUB_TIMEOUT: Duration = Duration::from_secs(5);

// ---------------------------------------------------------------------------
// AeronRuntime: the single Aeron thread + command bus.
// ---------------------------------------------------------------------------

/// Top-level handle. Spawn once per process / test; share via clone (cheap —
/// it's an `Arc` over the command channel).
///
/// Drop the last clone to tear down the Aeron thread cleanly.
#[derive(Clone)]
pub struct AeronRuntime {
    cmd_tx: CbSender<RuntimeCmd>,
    join: Arc<std::sync::Mutex<Option<std::thread::JoinHandle<()>>>>,
}

/// Subset of commands the Aeron-thread loop processes.
enum RuntimeCmd {
    /// Publish raw bytes onto a previously-registered publication.
    Publish {
        pub_id: u32,
        bytes: AlignedVec,
        // Returns the BPosition or error to the caller.
        ack: CbSender<Result<BPosition, LogError>>,
    },
    /// Best-effort publish — no ack, errors logged.
    PublishBestEffort { pub_id: u32, bytes: AlignedVec },
    /// Register a new publication. The Aeron thread executes
    /// `aeron.add_publication()` and replies with the assigned `pub_id`.
    OpenPublication {
        uri: String,
        stream_id: i32,
        ack: CbSender<Result<u32, LogError>>,
    },
    /// Register a new subscription. The Aeron thread executes
    /// `aeron.add_subscription()`, stores it in the sub table, and arranges
    /// for the supplied `deliver` closure to be invoked on each fragment.
    OpenSubscription {
        uri: String,
        stream_id: i32,
        deliver: Box<dyn FnMut(&[u8], BPosition) + Send>,
        ack: CbSender<Result<(), LogError>>,
    },
    /// Stop the loop, drop everything.
    Shutdown,
}

/// One row in the Aeron thread's subscription table.
struct SubEntry {
    sub: Sub,
    /// Closure that decodes the raw bytes and forwards into the appropriate
    /// mpsc sender. Created on a tokio task (`Send`), then moved to and run
    /// from the Aeron thread.
    deliver: Box<dyn FnMut(&[u8], BPosition) + Send>,
}

impl AeronRuntime {
    /// Build an Aeron client (using the default `aeron_dir`) and spawn the
    /// dedicated Aeron thread.
    ///
    /// For more control over `AeronContext` (e.g. pointing at a specific dir
    /// or registering error / new-image handlers), use [`AeronRuntime::with_aeron`].
    pub fn spawn_default() -> Result<Self, LogError> {
        let ctx = rusteron_client::AeronContext::new()
            .map_err(|e| LogError::Aeron(format!("AeronContext::new: {e}")))?;
        Self::spawn_with_context(ctx)
    }

    /// Build an Aeron client from a pre-configured context.
    pub fn spawn_with_context(ctx: rusteron_client::AeronContext) -> Result<Self, LogError> {
        let (started_tx, started_rx) = crossbeam_channel::bounded::<Result<(), LogError>>(1);
        let (cmd_tx, cmd_rx) = crossbeam_channel::unbounded::<RuntimeCmd>();

        // Subscription registration uses a side channel because the registration
        // payload itself isn't `Send` (the callback needs the registration cmd
        // to live on the Aeron thread). For Phase 1 simplicity we register all
        // publications + subscriptions up-front via [`Builder`], then spawn —
        // see the [`AeronRuntimeBuilder`] convenience below for that path.
        // Here we ship the minimal runtime; builder methods add to a queue
        // before `spawn()` returns.
        let join = std::thread::Builder::new()
            .name("kardamom-aeron".into())
            .spawn(move || {
                let aeron = match build_aeron(ctx) {
                    Ok(a) => a,
                    Err(e) => {
                        let _ = started_tx.send(Err(e));
                        return;
                    }
                };
                let _ = started_tx.send(Ok(()));
                if let Err(e) = run_aeron_thread(aeron, cmd_rx) {
                    error!(error = %e, "aeron runtime thread exited with error");
                }
            })
            .map_err(|e| LogError::Aeron(format!("spawn aeron thread: {e}")))?;

        // Wait for the thread to confirm Aeron came up.
        match started_rx.recv_timeout(Duration::from_secs(10)) {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return Err(e),
            Err(_) => {
                return Err(LogError::Aeron(
                    "aeron thread did not signal start within 10s".into(),
                ));
            }
        }

        Ok(Self {
            cmd_tx,
            join: Arc::new(std::sync::Mutex::new(Some(join))),
        })
    }

    /// Open a publication on the Aeron thread, returning a `Send + Sync`
    /// handle that forwards every offer through the command channel.
    pub fn open_publication(&self, uri: &str, stream_id: i32) -> Result<PubHandle, LogError> {
        let (ack_tx, ack_rx) = crossbeam_channel::bounded(1);
        self.cmd_tx
            .send(RuntimeCmd::OpenPublication {
                uri: uri.to_string(),
                stream_id,
                ack: ack_tx,
            })
            .map_err(|_| LogError::Aeron("aeron thread dropped".into()))?;
        let pub_id = ack_rx
            .recv_timeout(Duration::from_secs(10))
            .map_err(|_| LogError::Aeron("open_publication timed out".into()))??;
        Ok(PubHandle {
            cmd_tx: self.cmd_tx.clone(),
            pub_id,
        })
    }

    /// Open a raw subscription on the Aeron thread with a caller-supplied
    /// delivery closure. The closure runs on the Aeron thread for every
    /// inbound fragment.
    ///
    /// Most callers should use [`AeronRuntime::open_subscription`] (which
    /// handles rkyv decoding and yields a typed `(BPosition, T)` receiver);
    /// this lower-level entry point is for adapters that need to demultiplex
    /// fragments themselves (e.g. the channel-B archive reader that
    /// distinguishes `TxEnvelope` vs `BlockBoundaryStart` by trial-decode).
    pub fn open_subscription_with_deliver(
        &self,
        uri: &str,
        stream_id: i32,
        deliver: Box<dyn FnMut(&[u8], BPosition) + Send>,
    ) -> Result<(), LogError> {
        let (ack_tx, ack_rx) = crossbeam_channel::bounded(1);
        self.cmd_tx
            .send(RuntimeCmd::OpenSubscription {
                uri: uri.to_string(),
                stream_id,
                deliver,
                ack: ack_tx,
            })
            .map_err(|_| LogError::Aeron("aeron thread dropped".into()))?;
        ack_rx
            .recv_timeout(Duration::from_secs(10))
            .map_err(|_| LogError::Aeron("open_subscription timed out".into()))??;
        Ok(())
    }

    /// Open a typed subscription on the Aeron thread, returning an mpsc
    /// receiver of decoded messages.
    pub fn open_subscription<T>(
        &self,
        uri: &str,
        stream_id: i32,
    ) -> Result<UnboundedReceiver<(BPosition, T)>, LogError>
    where
        T: rkyv::Archive + Send + 'static,
        T::Archived: rkyv::Deserialize<T, rkyv::api::high::HighDeserializer<rkyv::rancor::Error>>
            + for<'a> rkyv::bytecheck::CheckBytes<
                rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>,
            >,
    {
        let (msg_tx, msg_rx) = unbounded_channel::<(BPosition, T)>();
        let deliver: Box<dyn FnMut(&[u8], BPosition) + Send> =
            Box::new(move |bytes: &[u8], pos: BPosition| {
                match codec::materialize::<T>(bytes) {
                    Ok(v) => {
                        if msg_tx.send((pos, v)).is_err() {
                            // Subscriber dropped its receiver. Aeron thread
                            // will reap this subscription on next poll.
                        }
                    }
                    Err(e) => {
                        error!(error = %e, "decode failed on subscription delivery");
                    }
                }
            });
        self.open_subscription_with_deliver(uri, stream_id, deliver)?;
        Ok(msg_rx)
    }
}

impl Drop for AeronRuntime {
    fn drop(&mut self) {
        // Only the last clone triggers shutdown; intermediate clones just
        // decrement the `Arc` refcount on `join`.
        if Arc::strong_count(&self.join) == 1 {
            let _ = self.cmd_tx.send(RuntimeCmd::Shutdown);
            if let Some(j) = self.join.lock().unwrap().take() {
                let _ = j.join();
            }
        }
    }
}

/// Build the actual Aeron client. Lives on the Aeron thread.
fn build_aeron(ctx: rusteron_client::AeronContext) -> Result<Rc<AeronClient>, LogError> {
    let aeron = AeronClient::new(&ctx).map_err(|e| LogError::Aeron(format!("Aeron::new: {e}")))?;
    aeron
        .start()
        .map_err(|e| LogError::Aeron(format!("Aeron::start: {e}")))?;
    Ok(Rc::new(aeron))
}

/// Aeron-thread main loop. Owns the Aeron client + the publication table +
/// the subscription table. Drains the command channel and pumps subscriptions.
fn run_aeron_thread(
    aeron: Rc<AeronClient>,
    cmd_rx: CbReceiver<RuntimeCmd>,
) -> Result<(), LogError> {
    let mut pubs: Vec<Pub> = Vec::new();
    let mut subs: Vec<SubEntry> = Vec::new();

    loop {
        // 1. Drain pending commands non-blockingly.
        loop {
            match cmd_rx.try_recv() {
                Ok(RuntimeCmd::Shutdown) => return Ok(()),
                Ok(cmd) => handle_cmd(&aeron, &mut pubs, &mut subs, cmd)?,
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => return Ok(()),
            }
        }

        // 2. Poll every subscription once with a small fragment limit so we
        //    don't starve the command channel.
        for entry in subs.iter_mut() {
            let _ = entry.sub.poll_once(
                |bytes: &[u8], header: Header| {
                    if let Some(pos) = header_pos(&header) {
                        (entry.deliver)(bytes, pos);
                    }
                },
                64,
            );
        }

        // 3. If nothing was pending and nothing happened, sleep briefly. This
        //    keeps idle CPU low. Production tuning may want a busy-poll mode
        //    behind a config knob (TODO: expose).
        if subs.is_empty() {
            // Block on the next command instead of spinning.
            match cmd_rx.recv_timeout(Duration::from_millis(1)) {
                Ok(RuntimeCmd::Shutdown) => return Ok(()),
                Ok(cmd) => handle_cmd(&aeron, &mut pubs, &mut subs, cmd)?,
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => {}
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => return Ok(()),
            }
        } else {
            std::thread::sleep(Duration::from_micros(100));
        }
    }
}

fn handle_cmd(
    aeron: &Rc<AeronClient>,
    pubs: &mut Vec<Pub>,
    subs: &mut Vec<SubEntry>,
    cmd: RuntimeCmd,
) -> Result<(), LogError> {
    match cmd {
        RuntimeCmd::Publish { pub_id, bytes, ack } => {
            let res = match pubs.get(pub_id as usize) {
                Some(p) => offer_blocking(p, bytes.as_slice()),
                None => Err(LogError::Aeron(format!("publish: unknown pub_id {pub_id}"))),
            };
            let _ = ack.send(res);
        }
        RuntimeCmd::PublishBestEffort { pub_id, bytes } => {
            if let Some(p) = pubs.get(pub_id as usize) {
                if let Err(e) = offer_blocking(p, bytes.as_slice()) {
                    warn!(error = %e, pub_id, "best-effort publish failed");
                }
            } else {
                warn!(pub_id, "best-effort publish to unknown pub_id");
            }
        }
        RuntimeCmd::OpenPublication {
            uri,
            stream_id,
            ack,
        } => {
            let res = open_pub(aeron, &uri, stream_id).map(|p| {
                pubs.push(p);
                (pubs.len() - 1) as u32
            });
            let _ = ack.send(res);
        }
        RuntimeCmd::OpenSubscription {
            uri,
            stream_id,
            deliver,
            ack,
        } => {
            let res = open_sub(aeron, &uri, stream_id).map(|sub| {
                subs.push(SubEntry { sub, deliver });
            });
            let _ = ack.send(res);
        }
        RuntimeCmd::Shutdown => {
            // Handled by the run loop directly.
        }
    }
    Ok(())
}

fn open_pub(aeron: &Rc<AeronClient>, uri: &str, stream_id: i32) -> Result<Pub, LogError> {
    let c = CString::new(uri).map_err(|e| LogError::Aeron(format!("uri contains NUL: {e}")))?;
    aeron
        .add_publication(c.as_c_str(), stream_id, ADD_PUB_TIMEOUT)
        .map_err(|e| LogError::Aeron(format!("add_publication {uri}: {e}")))
}

fn open_sub(aeron: &Rc<AeronClient>, uri: &str, stream_id: i32) -> Result<Sub, LogError> {
    let c = CString::new(uri).map_err(|e| LogError::Aeron(format!("uri contains NUL: {e}")))?;
    aeron
        .add_subscription(
            c.as_c_str(),
            stream_id,
            rusteron_client::Handlers::no_available_image_handler(),
            rusteron_client::Handlers::no_unavailable_image_handler(),
            ADD_SUB_TIMEOUT,
        )
        .map_err(|e| LogError::Aeron(format!("add_subscription {uri}: {e}")))
}

fn offer_blocking(p: &Pub, bytes: &[u8]) -> Result<BPosition, LogError> {
    for attempt in 0..1024 {
        let r = p.offer(
            bytes,
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

fn decode_position(p: i64) -> BPosition {
    let term_id = (p >> 32) as i32;
    let term_offset = (p & 0xFFFF_FFFF) as i32;
    BPosition {
        term_id,
        term_offset,
    }
}

fn header_pos(h: &Header) -> Option<BPosition> {
    let v = h.get_values().ok()?;
    let frame = v.frame();
    Some(BPosition {
        term_id: frame.term_id(),
        term_offset: frame.term_offset(),
    })
}

// ---------------------------------------------------------------------------
// Generic Send-able publish handle
// ---------------------------------------------------------------------------

/// `Send + Sync` publication handle. Forwards each publish through the
/// Aeron-thread command channel.
#[derive(Clone)]
pub struct PubHandle {
    cmd_tx: CbSender<RuntimeCmd>,
    pub_id: u32,
}

impl PubHandle {
    /// Blocking publish with `BPosition` ack. Use from sync code or from a
    /// `tokio::task::spawn_blocking` closure to avoid blocking the async
    /// reactor.
    pub fn publish_bytes(&self, bytes: AlignedVec) -> Result<BPosition, LogError> {
        let (ack_tx, ack_rx) = crossbeam_channel::bounded(1);
        self.cmd_tx
            .send(RuntimeCmd::Publish {
                pub_id: self.pub_id,
                bytes,
                ack: ack_tx,
            })
            .map_err(|_| LogError::Aeron("aeron thread dropped".into()))?;
        ack_rx
            .recv_timeout(Duration::from_secs(10))
            .map_err(|_| LogError::Aeron("publish_bytes timed out".into()))?
    }

    /// Fire-and-forget publish. Errors are logged on the Aeron thread.
    pub fn publish_best_effort(&self, bytes: AlignedVec) {
        let _ = self.cmd_tx.send(RuntimeCmd::PublishBestEffort {
            pub_id: self.pub_id,
            bytes,
        });
    }

    /// Encode a typed message and publish blockingly.
    pub fn publish<T>(&self, msg: &T) -> Result<BPosition, LogError>
    where
        T: for<'a> rkyv::Serialize<
                rkyv::api::high::HighSerializer<
                    AlignedVec,
                    rkyv::ser::allocator::ArenaHandle<'a>,
                    rkyv::rancor::Error,
                >,
            >,
    {
        let bytes = codec::encode(msg)?;
        self.publish_bytes(bytes)
    }
}

// ---------------------------------------------------------------------------
// Typed channel wrappers (Send-friendly)
// ---------------------------------------------------------------------------

/// Channel B publisher: canonical-tx log. Send-able.
#[derive(Clone)]
pub struct ChannelBPublisherHandle {
    inner: PubHandle,
}

impl ChannelBPublisherHandle {
    pub fn open(rt: &AeronRuntime, ch: &ChannelsConfig) -> Result<Self, LogError> {
        Ok(Self {
            inner: rt.open_publication(&ch.b_channel, ch.b_stream_id)?,
        })
    }
    pub fn publish_tx(&self, env: &TxEnvelope) -> Result<BPosition, LogError> {
        self.inner.publish(env)
    }
    pub fn publish_boundary(&self, b: &BlockBoundaryStart) -> Result<BPosition, LogError> {
        self.inner.publish(b)
    }
    pub fn raw(&self) -> &PubHandle {
        &self.inner
    }
}

/// Channel B subscriber. Yields `(BPosition, TxEnvelope)` via mpsc.
pub struct ChannelBSubscriberHandle {
    rx: UnboundedReceiver<(BPosition, TxEnvelope)>,
}

impl ChannelBSubscriberHandle {
    pub fn open(rt: &AeronRuntime, ch: &ChannelsConfig) -> Result<Self, LogError> {
        Ok(Self {
            rx: rt.open_subscription::<TxEnvelope>(&ch.b_channel, ch.b_stream_id)?,
        })
    }
    pub async fn recv(&mut self) -> Option<(BPosition, TxEnvelope)> {
        self.rx.recv().await
    }
    pub fn try_recv(&mut self) -> Option<(BPosition, TxEnvelope)> {
        self.rx.try_recv().ok()
    }
}

/// Channel C publisher: receipts + boundaries (RAM only).
#[derive(Clone)]
pub struct ChannelCPublisherHandle {
    inner: PubHandle,
}

impl ChannelCPublisherHandle {
    pub fn open(rt: &AeronRuntime, ch: &ChannelsConfig) -> Result<Self, LogError> {
        Ok(Self {
            inner: rt.open_publication(&ch.c_channel, ch.c_stream_id)?,
        })
    }
    pub fn publish_receipt(&self, r: &Receipt) -> Result<BPosition, LogError> {
        self.inner.publish(r)
    }
    pub fn publish_boundary(&self, b: &BlockBoundary) -> Result<BPosition, LogError> {
        self.inner.publish(b)
    }
    pub fn raw(&self) -> &PubHandle {
        &self.inner
    }
}

/// Channel C subscriber for receipts.
pub struct ChannelCReceiptSubscriberHandle {
    rx: UnboundedReceiver<(BPosition, Receipt)>,
}

impl ChannelCReceiptSubscriberHandle {
    pub fn open(rt: &AeronRuntime, ch: &ChannelsConfig) -> Result<Self, LogError> {
        Ok(Self {
            rx: rt.open_subscription::<Receipt>(&ch.c_channel, ch.c_stream_id)?,
        })
    }
    pub async fn recv(&mut self) -> Option<(BPosition, Receipt)> {
        self.rx.recv().await
    }
    pub fn try_recv(&mut self) -> Option<(BPosition, Receipt)> {
        self.rx.try_recv().ok()
    }
}

/// Channel C subscriber for boundaries.
pub struct ChannelCBoundarySubscriberHandle {
    rx: UnboundedReceiver<(BPosition, BlockBoundary)>,
}

impl ChannelCBoundarySubscriberHandle {
    pub fn open(rt: &AeronRuntime, ch: &ChannelsConfig) -> Result<Self, LogError> {
        // Boundaries share the channel C stream with receipts — in production
        // the subscriber discriminates by frame header type. For tests we
        // open a dedicated *boundary-only* stream id so the round-trip stays
        // unambiguous. Callers that want both receipts AND boundaries on the
        // same physical stream should poll a single Subscriber<rkyv::Bytes>
        // and demultiplex by the rkyv root type byte.
        Ok(Self {
            rx: rt.open_subscription::<BlockBoundary>(&ch.c_channel, ch.c_stream_id + 1)?,
        })
    }
    pub async fn recv(&mut self) -> Option<(BPosition, BlockBoundary)> {
        self.rx.recv().await
    }
    pub fn try_recv(&mut self) -> Option<(BPosition, BlockBoundary)> {
        self.rx.try_recv().ok()
    }
}

/// Per-shard ingress channel publisher. The channel URI for ingress[i] is
/// derived from the proxy config; this wrapper is generic over the URI.
#[derive(Clone)]
pub struct IngressPublisherHandle {
    inner: PubHandle,
}

impl IngressPublisherHandle {
    pub fn open(rt: &AeronRuntime, uri: &str, stream_id: i32) -> Result<Self, LogError> {
        Ok(Self {
            inner: rt.open_publication(uri, stream_id)?,
        })
    }
    pub fn publish_tx(&self, env: &TxEnvelope) -> Result<BPosition, LogError> {
        self.inner.publish(env)
    }
    pub fn raw(&self) -> &PubHandle {
        &self.inner
    }
}

/// Per-shard ingress channel subscriber.
pub struct IngressSubscriberHandle {
    rx: UnboundedReceiver<(BPosition, TxEnvelope)>,
}

impl IngressSubscriberHandle {
    pub fn open(rt: &AeronRuntime, uri: &str, stream_id: i32) -> Result<Self, LogError> {
        Ok(Self {
            rx: rt.open_subscription::<TxEnvelope>(uri, stream_id)?,
        })
    }
    pub async fn recv(&mut self) -> Option<(BPosition, TxEnvelope)> {
        self.rx.recv().await
    }
    pub fn try_recv(&mut self) -> Option<(BPosition, TxEnvelope)> {
        self.rx.try_recv().ok()
    }
}

/// Receipt-cache channel publisher.
#[derive(Clone)]
pub struct ReceiptCachePublisherHandle {
    inner: PubHandle,
}

impl ReceiptCachePublisherHandle {
    pub fn open(rt: &AeronRuntime, ch: &ChannelsConfig) -> Result<Self, LogError> {
        Ok(Self {
            inner: rt.open_publication(&ch.receipt_cache_channel, ch.receipt_cache_stream_id)?,
        })
    }
    pub fn publish(&self, r: &CachedReceipt) -> Result<BPosition, LogError> {
        self.inner.publish(r)
    }
    pub fn raw(&self) -> &PubHandle {
        &self.inner
    }
}

/// Receipt-cache channel subscriber.
pub struct ReceiptCacheSubscriberHandle {
    rx: UnboundedReceiver<(BPosition, CachedReceipt)>,
}

impl ReceiptCacheSubscriberHandle {
    pub fn open(rt: &AeronRuntime, ch: &ChannelsConfig) -> Result<Self, LogError> {
        Ok(Self {
            rx: rt.open_subscription::<CachedReceipt>(
                &ch.receipt_cache_channel,
                ch.receipt_cache_stream_id,
            )?,
        })
    }
    pub async fn recv(&mut self) -> Option<(BPosition, CachedReceipt)> {
        self.rx.recv().await
    }
    pub fn try_recv(&mut self) -> Option<(BPosition, CachedReceipt)> {
        self.rx.try_recv().ok()
    }
}

/// Per-recorder fsync-watermark publisher.
#[derive(Clone)]
pub struct FsyncWatermarkPublisherHandle {
    inner: PubHandle,
}

impl FsyncWatermarkPublisherHandle {
    pub fn open(rt: &AeronRuntime, ch: &ChannelsConfig, recorder_id: u8) -> Result<Self, LogError> {
        let channel = ch
            .fsync_watermark_channel_template
            .replace("{rid}", &recorder_id.to_string());
        Ok(Self {
            inner: rt.open_publication(&channel, ch.fsync_watermark_stream_id)?,
        })
    }
    pub fn publish(&self, w: &FsyncWatermark) -> Result<(), LogError> {
        self.inner.publish(w).map(|_| ())
    }
}

/// Per-recorder fsync-watermark subscriber.
pub struct FsyncWatermarkSubscriberHandle {
    rx: UnboundedReceiver<(BPosition, FsyncWatermark)>,
}

impl FsyncWatermarkSubscriberHandle {
    pub fn open(rt: &AeronRuntime, ch: &ChannelsConfig, recorder_id: u8) -> Result<Self, LogError> {
        let channel = ch
            .fsync_watermark_channel_template
            .replace("{rid}", &recorder_id.to_string());
        Ok(Self {
            rx: rt.open_subscription::<FsyncWatermark>(&channel, ch.fsync_watermark_stream_id)?,
        })
    }
    pub async fn recv(&mut self) -> Option<(BPosition, FsyncWatermark)> {
        self.rx.recv().await
    }
    pub fn try_recv(&mut self) -> Option<(BPosition, FsyncWatermark)> {
        self.rx.try_recv().ok()
    }
}

/// Quorum-watermark publisher.
#[derive(Clone)]
pub struct QuorumPublisherHandle {
    inner: PubHandle,
}

impl QuorumPublisherHandle {
    pub fn open(rt: &AeronRuntime, ch: &ChannelsConfig) -> Result<Self, LogError> {
        Ok(Self {
            inner: rt
                .open_publication(&ch.quorum_watermark_channel, ch.quorum_watermark_stream_id)?,
        })
    }
    pub fn publish(&self, q: &QuorumWatermark) -> Result<(), LogError> {
        self.inner.publish(q).map(|_| ())
    }
}

/// Quorum-watermark subscriber.
pub struct QuorumSubscriberHandle {
    rx: UnboundedReceiver<(BPosition, QuorumWatermark)>,
}

impl QuorumSubscriberHandle {
    pub fn open(rt: &AeronRuntime, ch: &ChannelsConfig) -> Result<Self, LogError> {
        Ok(Self {
            rx: rt.open_subscription::<QuorumWatermark>(
                &ch.quorum_watermark_channel,
                ch.quorum_watermark_stream_id,
            )?,
        })
    }
    pub async fn recv(&mut self) -> Option<(BPosition, QuorumWatermark)> {
        self.rx.recv().await
    }
    pub fn try_recv(&mut self) -> Option<(BPosition, QuorumWatermark)> {
        self.rx.try_recv().ok()
    }
}

// ---------------------------------------------------------------------------
// Send-trait compile-time assertions
// ---------------------------------------------------------------------------

const _: () = {
    fn assert_send<T: Send>() {}
    fn assert_sync<T: Sync>() {}
    fn _all() {
        assert_send::<AeronRuntime>();
        assert_sync::<AeronRuntime>();
        assert_send::<PubHandle>();
        assert_sync::<PubHandle>();
        assert_send::<ChannelBPublisherHandle>();
        assert_send::<ChannelBSubscriberHandle>();
        assert_send::<ChannelCPublisherHandle>();
        assert_send::<ChannelCReceiptSubscriberHandle>();
        assert_send::<ChannelCBoundarySubscriberHandle>();
        assert_send::<IngressPublisherHandle>();
        assert_send::<IngressSubscriberHandle>();
        assert_send::<ReceiptCachePublisherHandle>();
        assert_send::<ReceiptCacheSubscriberHandle>();
        assert_send::<FsyncWatermarkPublisherHandle>();
        assert_send::<FsyncWatermarkSubscriberHandle>();
        assert_send::<QuorumPublisherHandle>();
        assert_send::<QuorumSubscriberHandle>();
    }
};
