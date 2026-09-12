//! [`AeronRuntime`]: the single Aeron thread's command bus and the
//! `Send + Sync` handles it hands out ([`PubHandle`], typed subscription
//! receivers). All Aeron work happens on the dedicated thread spawned here
//! (see `super::thread`). This module only ships commands to it.

use std::marker::PhantomData;
use std::ops::ControlFlow;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use crossbeam_channel::Sender as CbSender;
use rkyv::util::AlignedVec;
use tokio::sync::mpsc::{UnboundedReceiver, unbounded_channel};
use tracing::error;

use super::thread::run_aeron_thread;
use super::{ACK_TIMEOUT, AeronClient, FrameSink, RawFrame};
use crate::codec;
use crate::error::LogError;
use kardamom_types::{BPosition, TxDataLoc, TxEnvelope};

// ---------------------------------------------------------------------------
// AeronRuntime: the single Aeron thread + command bus.
// ---------------------------------------------------------------------------

/// Top-level handle. Spawn once per process or test; share by cloning
/// (cheap, since it is an `Arc` over the command channel).
///
/// Drop the last clone to tear down the Aeron thread cleanly.
#[derive(Clone)]
pub struct AeronRuntime {
    cmd_tx: CbSender<RuntimeCmd>,
    /// Shared owner of the Aeron thread: the last clone to drop tears it
    /// down (see [`AeronThread`]). Held only for its `Drop`.
    _thread: Arc<AeronThread>,
}

/// RAII owner of the Aeron thread. Dropping it sends `Shutdown` and joins,
/// so teardown runs exactly once — when the last [`AeronRuntime`] clone goes.
struct AeronThread {
    cmd_tx: CbSender<RuntimeCmd>,
    join: Option<std::thread::JoinHandle<()>>,
}

impl Drop for AeronThread {
    fn drop(&mut self) {
        let _ = self.cmd_tx.send(RuntimeCmd::Shutdown);
        if let Some(j) = self.join.take() {
            let _ = j.join();
        }
    }
}

/// Subset of commands the Aeron-thread loop processes.
pub(super) enum RuntimeCmd {
    /// Publish raw bytes onto a previously-registered publication.
    Publish {
        pub_id: u32,
        bytes: AlignedVec,
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
    /// `aeron.add_subscription()`, stores it in the sub table, and sends
    /// each assembled fragment to `sink` as a [`RawFrame`]. Replies with
    /// the assigned `sub_id` (needed to attach MDS destinations).
    OpenSubscription {
        uri: String,
        stream_id: i32,
        sink: FrameSink,
        ack: CbSender<Result<u32, LogError>>,
    },
    /// Attach a source endpoint to a multi-destination
    /// (`control-mode=manual`) subscription. Used to aggregate
    /// per-publisher streams, for example one ingress MDS subscription
    /// pulling receipts from every executor replica.
    SubAddDestination {
        sub_id: u32,
        uri: String,
        ack: CbSender<Result<(), LogError>>,
    },
    /// Detach a previously-attached source endpoint from an MDS subscription.
    SubRemoveDestination {
        sub_id: u32,
        uri: String,
        ack: CbSender<Result<(), LogError>>,
    },
    /// Stop the loop, drop everything.
    Shutdown,
}

/// One command round trip to the Aeron thread: build the command around a
/// fresh ack channel, send it, and wait [`ACK_TIMEOUT`] for the reply.
/// Every control-plane call on [`AeronRuntime`] and
/// [`PubHandle::publish_bytes`] shares this shape, so the two failure
/// modes (the Aeron thread died, or the ack never came) each have exactly
/// one wording. `op` names the caller in the timeout error.
fn request<R>(
    cmd_tx: &CbSender<RuntimeCmd>,
    mk: impl FnOnce(CbSender<Result<R, LogError>>) -> RuntimeCmd,
    op: &str,
) -> Result<R, LogError> {
    let (ack_tx, ack_rx) = crossbeam_channel::bounded(1);
    cmd_tx
        .send(mk(ack_tx))
        .map_err(|_| LogError::Aeron("aeron thread dropped".into()))?;
    ack_rx
        .recv_timeout(ACK_TIMEOUT)
        .map_err(|_| LogError::Aeron(format!("{op} timed out")))?
}

/// The Aeron thread's whole body, run by [`AeronRuntime::spawn_with`] on
/// its dedicated OS thread. Builds the client with `make_ctx`, reports the
/// outcome on `started_tx`, then runs the poll/command loop until it exits.
fn aeron_thread_main<F>(
    make_ctx: F,
    cmd_rx: crossbeam_channel::Receiver<RuntimeCmd>,
    started_tx: &CbSender<Result<(), LogError>>,
) where
    F: FnOnce() -> Result<rusteron_client::AeronContext, LogError>,
{
    let aeron = match make_ctx().and_then(|ctx| build_aeron(&ctx)) {
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
}

impl AeronRuntime {
    /// Calls [`spawn_with_dir`](Self::spawn_with_dir) when a directory is
    /// given, or [`spawn_default`](Self::spawn_default) otherwise. This is
    /// the shape every service binary's optional `--aeron-dir` flag needs.
    ///
    /// # Errors
    ///
    /// Returns an error if the Aeron thread fails to start (see
    /// [`spawn_with_dir`](Self::spawn_with_dir) and
    /// [`spawn_default`](Self::spawn_default)).
    pub fn spawn(aeron_dir: Option<&std::path::Path>) -> Result<Self, LogError> {
        match aeron_dir {
            Some(dir) => Self::spawn_with_dir(dir),
            None => Self::spawn_default(),
        }
    }

    /// Build an Aeron client (using the default `aeron_dir`) and spawn the
    /// dedicated Aeron thread.
    ///
    /// # Errors
    ///
    /// Returns an error if building the default `AeronContext` fails, or
    /// if the Aeron thread fails to start within 10 s (see
    /// [`spawn_with`](Self::spawn_with)).
    pub fn spawn_default() -> Result<Self, LogError> {
        Self::spawn_with(|| {
            rusteron_client::AeronContext::new()
                .map_err(|e| LogError::Aeron(format!("AeronContext::new: {e}")))
        })
    }

    /// Spawn pointing at a specific `aeron.dir` (the Media Driver's
    /// shared-memory directory). Used by e2e tests that bind-mount the
    /// container's aeron.dir into the host.
    ///
    /// # Errors
    ///
    /// Returns an error if `aeron_dir` is not UTF-8 or contains a NUL
    /// byte, or if the Aeron thread fails to start (see
    /// [`spawn_with`](Self::spawn_with)).
    pub fn spawn_with_dir(aeron_dir: impl Into<std::path::PathBuf>) -> Result<Self, LogError> {
        let aeron_dir = aeron_dir.into();
        let aeron_dir_c = crate::ffi::dir_cstring(&aeron_dir)?;
        Self::spawn_with(move || {
            let ctx = rusteron_client::AeronContext::new()
                .map_err(|e| LogError::Aeron(format!("AeronContext::new: {e}")))?;
            ctx.set_dir(aeron_dir_c.as_c_str())
                .map_err(|e| LogError::Aeron(format!("set_dir: {e}")))?;
            Ok(ctx)
        })
    }

    /// Spawn the Aeron thread, building the `AeronContext` inside the
    /// thread with the caller-supplied closure. The closure runs on the
    /// Aeron thread. This is the only way to feed it custom configuration
    /// without crossing the `!Send + !Sync` boundary that `AeronContext`
    /// sits on.
    ///
    /// # Errors
    ///
    /// Returns an error if `make_ctx` fails, if building the `Aeron`
    /// client from the resulting context fails, if the OS thread spawn
    /// fails, or if the Aeron thread does not signal a successful start
    /// within 10 s.
    pub fn spawn_with<F>(make_ctx: F) -> Result<Self, LogError>
    where
        F: FnOnce() -> Result<rusteron_client::AeronContext, LogError> + Send + 'static,
    {
        let (started_tx, started_rx) = crossbeam_channel::bounded::<Result<(), LogError>>(1);
        let (cmd_tx, cmd_rx) = crossbeam_channel::unbounded::<RuntimeCmd>();

        let join = std::thread::Builder::new()
            .name("kardamom-aeron".into())
            .spawn(move || aeron_thread_main(make_ctx, cmd_rx, &started_tx))
            .map_err(|e| LogError::Aeron(format!("spawn aeron thread: {e}")))?;

        match started_rx.recv_timeout(Duration::from_secs(10)) {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return Err(e),
            Err(_) => {
                return Err(LogError::Aeron(
                    "aeron thread did not signal start within 10s".into(),
                ));
            }
        }

        let thread = Arc::new(AeronThread {
            cmd_tx: cmd_tx.clone(),
            join: Some(join),
        });
        Ok(Self {
            cmd_tx,
            _thread: thread,
        })
    }

    /// Open a publication on the Aeron thread, returning a `Send + Sync`
    /// handle that forwards every offer through the command channel.
    ///
    /// # Errors
    ///
    /// Returns an error if the Aeron thread fails to add the publication
    /// (a malformed channel URI, or the driver's `add_publication`
    /// timeout elapsing), or if the command round trip itself times out.
    pub fn open_publication(&self, uri: &str, stream_id: i32) -> Result<PubHandle, LogError> {
        let uri = uri.to_string();
        let pub_id = request(
            &self.cmd_tx,
            |ack| RuntimeCmd::OpenPublication {
                uri,
                stream_id,
                ack,
            },
            "open_publication",
        )?;
        Ok(PubHandle {
            cmd_tx: self.cmd_tx.clone(),
            pub_id,
        })
    }

    /// Open a subscription, returning its raw undecoded fragment stream
    /// (as [`RawFrame`]s) plus the assigned `sub_id` (used to attach MDS
    /// destinations; most callers ignore it). Used by adapters that
    /// decode or demultiplex fragments themselves, on the consumer side
    /// rather than on the Aeron thread — see [`FrameSink`].
    ///
    /// # Errors
    ///
    /// Returns an error if the Aeron thread fails to add the subscription
    /// (a malformed channel URI, or the driver's `add_subscription`
    /// timeout elapsing), or if the command round trip itself times out.
    pub fn open_subscription_raw(
        &self,
        uri: &str,
        stream_id: i32,
    ) -> Result<(u32, UnboundedReceiver<RawFrame>), LogError> {
        let (tx, rx) = unbounded_channel();
        let sub_id = self.open_subscription_sink(uri, stream_id, FrameSink::Tokio(tx))?;
        Ok((sub_id, rx))
    }

    /// Like [`open_subscription_raw`](Self::open_subscription_raw), but
    /// for a plain OS thread that waits on this subscription alongside
    /// other crossbeam channels via `crossbeam_channel::Select`
    /// (`kardamom_cluster_adapter`'s session thread is the one consumer
    /// today). Tokio's channels do not implement crossbeam's
    /// `SelectHandle`, so that consumer needs a crossbeam receiver, not
    /// the tokio one every other subscriber handle in this crate uses.
    ///
    /// # Errors
    ///
    /// Returns an error if the Aeron thread fails to add the subscription,
    /// or if the command round trip itself times out.
    pub fn open_subscription_raw_crossbeam(
        &self,
        uri: &str,
        stream_id: i32,
    ) -> Result<(u32, crossbeam_channel::Receiver<RawFrame>), LogError> {
        let (tx, rx) = crossbeam_channel::unbounded();
        let sub_id = self.open_subscription_sink(uri, stream_id, FrameSink::Crossbeam(tx))?;
        Ok((sub_id, rx))
    }

    /// Shared by every `open_subscription*` method: register the
    /// subscription and point its delivery at `sink`. Several calls can
    /// share one clone of the same [`FrameSink::Tokio`] sender to merge
    /// multiple subscriptions into one raw stream (see
    /// [`open_subscription_merged`](Self::open_subscription_merged)).
    fn open_subscription_sink(
        &self,
        uri: &str,
        stream_id: i32,
        sink: FrameSink,
    ) -> Result<u32, LogError> {
        let uri = uri.to_string();
        request(
            &self.cmd_tx,
            |ack| RuntimeCmd::OpenSubscription {
                uri,
                stream_id,
                sink,
                ack,
            },
            "open_subscription",
        )
    }

    /// Attach a source endpoint to a multi-destination subscription (one
    /// opened `control-mode=manual`). Blocks until the driver confirms the
    /// attach. Idempotent: re-adding an already-attached `uri` is a no-op.
    ///
    /// # Errors
    ///
    /// Returns an error if `sub_id` is unknown, if the driver rejects or
    /// times out the destination attach, or if the command round trip
    /// itself times out.
    pub fn add_destination(&self, sub_id: u32, uri: &str) -> Result<(), LogError> {
        let uri = uri.to_string();
        request(
            &self.cmd_tx,
            |ack| RuntimeCmd::SubAddDestination { sub_id, uri, ack },
            "add_destination",
        )
    }

    /// Detach a previously-attached source endpoint from an MDS subscription.
    ///
    /// # Errors
    ///
    /// Returns an error if the command round trip to the Aeron thread
    /// times out.
    pub fn remove_destination(&self, sub_id: u32, uri: &str) -> Result<(), LogError> {
        let uri = uri.to_string();
        request(
            &self.cmd_tx,
            |ack| RuntimeCmd::SubRemoveDestination { sub_id, uri, ack },
            "remove_destination",
        )
    }

    /// Open a typed subscription, returning a [`TypedSubscription`] that
    /// decodes each fragment as `T` when the consumer calls
    /// `recv`/`try_recv`.
    ///
    /// # Errors
    ///
    /// Returns an error if the Aeron thread fails to add the subscription
    /// (see [`open_subscription_merged`](Self::open_subscription_merged)).
    pub fn open_subscription<T>(
        &self,
        uri: &str,
        stream_id: i32,
    ) -> Result<TypedSubscription<T>, LogError>
    where
        T: crate::codec::WireMessage,
    {
        self.open_subscription_merged(uri, &[], stream_id)
    }

    /// Open one or more subscriptions on the same `stream_id`, all feeding
    /// a single [`TypedSubscription`]. Each URI becomes its own Aeron
    /// subscription (its own `SubEntry`), sharing one clone of the same
    /// raw-frame sender, so fragments from every one merge into the
    /// returned stream in the Aeron thread's poll order.
    ///
    /// This is the `tx_ordering` MDC subscriber primitive: the executor
    /// passes one MDC control URI per publisher (the sealer and each
    /// sequencer), and the downstream reader sees a single ordered
    /// `(BPosition, T)` stream, exactly as before. With no `rest` URIs it
    /// is identical to [`Self::open_subscription`].
    ///
    /// Takes `first` plus `rest` instead of one slice, so the empty-URI
    /// case cannot be constructed and needs no runtime check.
    ///
    /// # Errors
    ///
    /// Returns an error if the Aeron thread fails to add any of the
    /// underlying subscriptions (see
    /// [`open_subscription_raw`](Self::open_subscription_raw)).
    pub fn open_subscription_merged<T>(
        &self,
        first: &str,
        rest: &[&str],
        stream_id: i32,
    ) -> Result<TypedSubscription<T>, LogError>
    where
        T: crate::codec::WireMessage,
    {
        let (frames_tx, frames_rx) = unbounded_channel();
        for uri in std::iter::once(first).chain(rest.iter().copied()) {
            self.open_subscription_sink(uri, stream_id, FrameSink::Tokio(frames_tx.clone()))?;
        }
        Ok(TypedSubscription::new(frames_rx))
    }

    /// Like [`open_subscription`](Self::open_subscription), but also
    /// returns the `sub_id`, so the caller can attach MDS source endpoints
    /// with [`add_destination`](Self::add_destination). Open the
    /// subscription on a `control-mode=manual` channel to make it
    /// multi-destination.
    ///
    /// # Errors
    ///
    /// Returns an error if the Aeron thread fails to add the subscription
    /// (see [`open_subscription_raw`](Self::open_subscription_raw)).
    pub fn open_subscription_with_id<T>(
        &self,
        uri: &str,
        stream_id: i32,
    ) -> Result<(u32, TypedSubscription<T>), LogError>
    where
        T: crate::codec::WireMessage,
    {
        let (sub_id, rx) = self.open_subscription_raw(uri, stream_id)?;
        Ok((sub_id, TypedSubscription::new(rx)))
    }

    /// Open a `tx_data` subscription yielding `(TxDataLoc, TxEnvelope)`,
    /// pairing each envelope with its Aeron publisher `session_id`. The
    /// session id keeps concurrent (active/active) ingress publishers on
    /// one shard distinct. It is what the sequencer stamps into
    /// `TxRef.tx_data_session_id`, and what the executor keys its join
    /// buffer on. With a single publisher, every fragment carries the same
    /// session id.
    ///
    /// # Errors
    ///
    /// Returns an error if the Aeron thread fails to add the subscription
    /// (see [`open_subscription_raw`](Self::open_subscription_raw)).
    pub fn open_tx_data_subscription(
        &self,
        uri: &str,
        stream_id: i32,
    ) -> Result<TxDataSubscription, LogError> {
        let (_sub_id, rx) = self.open_subscription_raw(uri, stream_id)?;
        Ok(TxDataSubscription { rx })
    }
}

/// Drain `rx` until `decode` yields a value or the channel closes.
/// `decode` reports "skip and keep waiting" via `ControlFlow::Continue`
/// for a malformed frame, so one bad fragment never ends the
/// subscription: a decode error is skipped, not surfaced to the caller.
async fn recv_decoded<T>(
    rx: &mut UnboundedReceiver<RawFrame>,
    mut decode: impl FnMut(&RawFrame) -> ControlFlow<T>,
) -> Option<T> {
    let mut out = None;
    while out.is_none() {
        out = decode(&rx.recv().await?).break_value();
    }
    out
}

/// Non-blocking twin of [`recv_decoded`], for `try_recv`. Keeps trying the
/// next already-buffered frame, if any, past one `decode` skips.
fn try_recv_decoded<T>(
    rx: &mut UnboundedReceiver<RawFrame>,
    mut decode: impl FnMut(&RawFrame) -> ControlFlow<T>,
) -> Option<T> {
    std::iter::from_fn(|| rx.try_recv().ok()).find_map(|f| decode(&f).break_value())
}

/// Blocking twin of [`recv_decoded`], for a caller off the tokio runtime.
fn blocking_recv_decoded<T>(
    rx: &mut UnboundedReceiver<RawFrame>,
    mut decode: impl FnMut(&RawFrame) -> ControlFlow<T>,
) -> Option<T> {
    let mut out = None;
    while out.is_none() {
        out = decode(&rx.blocking_recv()?).break_value();
    }
    out
}

/// Decode one [`RawFrame`] as `T`. A malformed frame logs and reports
/// "keep waiting" instead of ending the subscription or surfacing an
/// error to the consumer.
fn decode_typed_frame<T>(frame: &RawFrame) -> ControlFlow<(BPosition, T)>
where
    T: crate::codec::WireMessage,
{
    match codec::materialize::<T>(&frame.bytes) {
        Ok(v) => ControlFlow::Break((frame.pos, v)),
        Err(e) => {
            error!(error = %e, "decode failed on subscription delivery");
            ControlFlow::Continue(())
        }
    }
}

/// Decoded receiver for one subscription (or several merged ones, see
/// [`AeronRuntime::open_subscription_merged`]): wraps the raw frame
/// stream and decodes each frame as `T` on `recv`/`try_recv`.
///
/// `PhantomData<fn() -> T>` rather than `PhantomData<T>`: this type never
/// stores a `T`, so it must not inherit a `T: Send`/`T: Sync` bound it
/// does not need.
pub struct TypedSubscription<T> {
    rx: UnboundedReceiver<RawFrame>,
    _msg: PhantomData<fn() -> T>,
}

impl<T> TypedSubscription<T> {
    fn new(rx: UnboundedReceiver<RawFrame>) -> Self {
        Self {
            rx,
            _msg: PhantomData,
        }
    }
}

impl<T: crate::codec::WireMessage> TypedSubscription<T> {
    pub async fn recv(&mut self) -> Option<(BPosition, T)> {
        recv_decoded(&mut self.rx, decode_typed_frame).await
    }

    pub fn try_recv(&mut self) -> Option<(BPosition, T)> {
        try_recv_decoded(&mut self.rx, decode_typed_frame)
    }
}

/// Decode one `tx_data` fragment as a `TxEnvelope`, pairing it with a
/// [`TxDataLoc`] built from the frame's position and publisher session.
fn decode_tx_data_frame(frame: &RawFrame) -> ControlFlow<(TxDataLoc, TxEnvelope)> {
    match codec::materialize::<TxEnvelope>(&frame.bytes) {
        Ok(v) => ControlFlow::Break((TxDataLoc::new(frame.session, frame.pos), v)),
        Err(e) => {
            error!(error = %e, "decode failed on tx_data subscription delivery");
            ControlFlow::Continue(())
        }
    }
}

/// [`AeronRuntime::open_tx_data_subscription`]'s receiver: like
/// [`TypedSubscription`], but decoding into `(TxDataLoc, TxEnvelope)`
/// instead of `(BPosition, T)`, since `tx_data` also needs the publisher
/// session id.
pub struct TxDataSubscription {
    rx: UnboundedReceiver<RawFrame>,
}

impl TxDataSubscription {
    pub async fn recv(&mut self) -> Option<(TxDataLoc, TxEnvelope)> {
        recv_decoded(&mut self.rx, decode_tx_data_frame).await
    }

    pub fn try_recv(&mut self) -> Option<(TxDataLoc, TxEnvelope)> {
        try_recv_decoded(&mut self.rx, decode_tx_data_frame)
    }

    /// Blocking receive, for a caller off the tokio runtime (for example
    /// a dedicated reader thread). Delegates to
    /// `UnboundedReceiver::blocking_recv`, which parks the OS thread
    /// until a frame arrives or the channel closes; unlike
    /// [`PollRecv::poll_recv`], this has no timeout.
    ///
    /// # Panics
    ///
    /// Panics if called from within a tokio runtime context (the same
    /// restriction `UnboundedReceiver::blocking_recv` itself has).
    pub fn blocking_recv(&mut self) -> Option<(TxDataLoc, TxEnvelope)> {
        blocking_recv_decoded(&mut self.rx, decode_tx_data_frame)
    }

    /// Frames currently queued, not yet received. For a queue-depth
    /// gauge; the channel is unbounded, so this is not a backlog bound.
    #[must_use]
    pub fn len(&self) -> usize {
        self.rx.len()
    }

    /// Whether [`Self::len`] is 0.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.rx.is_empty()
    }
}

/// A subscription receiver that can be polled by hand, for a caller
/// driving its own timeout on a thread with no tokio runtime entered
/// (`crate::refetch`'s replay client is the one consumer today —
/// tokio's `UnboundedReceiver` offers only `blocking_recv`, with no
/// timeout, and async `recv`, which needs a runtime). Implemented by
/// [`TypedSubscription`] and [`TxDataSubscription`].
pub trait PollRecv {
    type Item;
    fn poll_recv(&mut self, cx: &mut std::task::Context<'_>)
    -> std::task::Poll<Option<Self::Item>>;
}

/// Shared poll loop behind both [`PollRecv`] impls: keep polling the raw
/// channel past frames `decode` reports "keep waiting" on, and stop at the
/// first decoded item, end of stream, or pending. This is the manual-poll
/// twin of [`recv_decoded`]/[`try_recv_decoded`].
fn poll_recv_decoded<T>(
    rx: &mut UnboundedReceiver<RawFrame>,
    cx: &mut std::task::Context<'_>,
    mut decode: impl FnMut(&RawFrame) -> ControlFlow<T>,
) -> std::task::Poll<Option<T>> {
    loop {
        if let ControlFlow::Break(outcome) = poll_one(rx, cx, &mut decode) {
            return outcome;
        }
    }
}

/// One raw poll. `Break` carries the outcome [`poll_recv_decoded`] should
/// return: a decoded item, end of stream, or pending. `Continue` means
/// `decode` skipped a malformed frame, so the caller polls again.
fn poll_one<T>(
    rx: &mut UnboundedReceiver<RawFrame>,
    cx: &mut std::task::Context<'_>,
    decode: &mut impl FnMut(&RawFrame) -> ControlFlow<T>,
) -> ControlFlow<std::task::Poll<Option<T>>> {
    match rx.poll_recv(cx) {
        std::task::Poll::Ready(Some(frame)) => match decode(&frame) {
            ControlFlow::Break(v) => ControlFlow::Break(std::task::Poll::Ready(Some(v))),
            ControlFlow::Continue(()) => ControlFlow::Continue(()),
        },
        std::task::Poll::Ready(None) => ControlFlow::Break(std::task::Poll::Ready(None)),
        std::task::Poll::Pending => ControlFlow::Break(std::task::Poll::Pending),
    }
}

impl<T: crate::codec::WireMessage> PollRecv for TypedSubscription<T> {
    type Item = (BPosition, T);

    fn poll_recv(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        poll_recv_decoded(&mut self.rx, cx, decode_typed_frame::<T>)
    }
}

impl PollRecv for TxDataSubscription {
    type Item = (TxDataLoc, TxEnvelope);

    fn poll_recv(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        poll_recv_decoded(&mut self.rx, cx, decode_tx_data_frame)
    }
}

/// Routes Aeron C-client errors through `tracing` instead of the client's
/// default raw-stderr print. The client may follow a fatal error (for
/// example code 1000, a driver keepalive timeout) by ending the process,
/// so this line is often the service's last. It must carry this service's
/// timestamp and land in its own log, to sequence the death against the
/// other services'.
struct TracingErrorHandler;

impl rusteron_client::AeronErrorHandlerCallback for TracingErrorHandler {
    fn handle_aeron_error_handler(&mut self, error_code: std::os::raw::c_int, msg: &str) {
        error!(
            code = error_code,
            msg, "aeron client error; exiting (fail-fast, as aeron's default handler does)"
        );
        // The C default handler this replaces exits the process after its
        // stderr print, and that contract must survive the swap. A client
        // that loses its driver and lingers keeps its /metrics port up
        // while every publication is closed, which reads as a
        // live-but-stuck service to supervisors and probes. Exit here so
        // the supervisor restarts the process.
        std::process::exit(1);
    }
}

fn build_aeron(ctx: &rusteron_client::AeronContext) -> Result<Rc<AeronClient>, LogError> {
    let handler = rusteron_client::Handler::leak(TracingErrorHandler);
    ctx.set_error_handler(Some(&handler))
        .map_err(|e| LogError::Aeron(format!("set_error_handler: {e}")))?;
    // The C side holds the leaked pointer for the client's (the process')
    // lifetime. Forgetting the wrapper suppresses its drop-time
    // release()-was-never-called complaint.
    std::mem::forget(handler);
    let aeron = AeronClient::new(ctx).map_err(|e| LogError::Aeron(format!("Aeron::new: {e}")))?;
    aeron
        .start()
        .map_err(|e| LogError::Aeron(format!("Aeron::start: {e}")))?;
    Ok(Rc::new(aeron))
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
    /// Blocking publish with `BPosition` ack. Waits [`ACK_TIMEOUT`] for the
    /// Aeron thread's reply. See that constant for why the ack always
    /// resolves first.
    ///
    /// # Errors
    ///
    /// Returns an error if the Aeron offer fails or times out, or if the
    /// command round trip to the Aeron thread itself times out.
    pub fn publish_bytes(&self, bytes: AlignedVec) -> Result<BPosition, LogError> {
        let pub_id = self.pub_id;
        request(
            &self.cmd_tx,
            |ack| RuntimeCmd::Publish { pub_id, bytes, ack },
            "publish_bytes",
        )
    }

    /// Fire-and-forget publish. Errors are logged on the Aeron thread.
    pub fn publish_best_effort(&self, bytes: AlignedVec) {
        let _ = self.cmd_tx.send(RuntimeCmd::PublishBestEffort {
            pub_id: self.pub_id,
            bytes,
        });
    }

    /// Encode a typed message and publish blockingly.
    ///
    /// # Errors
    ///
    /// Returns an error if `msg` fails to encode, or if the publish
    /// itself fails (see [`publish_bytes`](Self::publish_bytes)).
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

#[cfg(test)]
mod tests {
    use super::*;

    /// `TypedSubscription` decodes on read, off the Aeron thread. A
    /// malformed frame is skipped, not surfaced as an error or an end of
    /// stream, and the next well-formed frame still decodes. This needs
    /// no Aeron media driver, unlike the rest of this module's behavior.
    #[test]
    fn typed_subscription_skips_a_malformed_frame_and_decodes_the_next() {
        let (tx, rx) = unbounded_channel();
        let mut sub: TypedSubscription<u64> = TypedSubscription::new(rx);

        tx.send(RawFrame {
            bytes: vec![0xFF; 3],
            pos: BPosition::default(),
            session: 0,
        })
        .unwrap();
        tx.send(RawFrame {
            bytes: codec::encode(&42u64).unwrap().to_vec(),
            pos: BPosition::default(),
            session: 0,
        })
        .unwrap();

        let (pos, v) = sub
            .try_recv()
            .expect("skips the malformed frame, decodes the next");
        assert_eq!(v, 42);
        assert_eq!(pos, BPosition::default());
        assert!(
            sub.try_recv().is_none(),
            "only one well-formed frame was sent"
        );
    }
}
