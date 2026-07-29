//! High-level real-Aeron channel adapters that are `Send`-friendly for tokio
//! consumers.
//!
//! ## Why this module exists
//!
//! The raw rusteron types (`rusteron_client::Aeron`, `AeronPublication`,
//! `AeronSubscription`, `rusteron_archive::AeronArchive`) wrap raw FFI
//! pointers into a thread-confined C client and are therefore `!Send + !Sync`.
//! Production consumers (the proxy/ingress, sequencer, executor, sealer,
//! state writer, batcher) all live in multi-threaded tokio runtimes; they
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
//! ## Threading rules
//!
//! 1. `Aeron` and `AeronArchive` are `!Send + !Sync`. Never move them across
//!    threads, ever.
//! 2. Use `Rc`, not `Arc`. The Aeron loop runs in a dedicated
//!    `std::thread::spawn` OS thread.
//! 3. Cross-thread communication: `crossbeam::channel`,
//!    `tokio::sync::mpsc`/`broadcast`. Never an Aeron handle.
//! 4. Tokio multi-thread runtimes silently move tasks across worker threads
//!    at await points — so the Aeron loop is plain `std::thread`, not tokio.
//!
//! ## Handle set
//!
//! Maps the MDS channel topology onto Send-friendly handles:
//! - `TxData{Publisher,Subscriber}Handle` — per-shard envelope channel; the
//!   proxy/ingress publishes, sequencers + executors + batchers subscribe.
//! - `TxOrdering{Publisher,Subscriber}Handle` — canonical orderer of tiny
//!   `TxOrderingMessage` records (`TxRef | BoundaryStart`); sequencers race
//!   to publish, the sealer also publishes boundaries, the executor /
//!   batcher subscribe.
//! - `TxReceipts{Publisher,ReceiptSubscriber,BoundarySubscriber}Handle` —
//!   receipts + slim boundaries (not recorded); executor publishes, proxy /
//!   state writer subscribe.
//! - `ReceiptCache{Publisher,Subscriber}Handle` — proxy ↔ executor receipt
//!   cache (not recorded).
//! - `FsyncWatermark{Publisher,Subscriber}Handle` — per-recorder fsync
//!   watermark streams feeding the quorum aggregator.
//! - `Quorum{Publisher,Subscriber}Handle` — aggregated quorum watermark.
//!
//! (unconditional dep on rusteron.)

use std::collections::VecDeque;
use std::ffi::CString;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver as CbReceiver, Sender as CbSender, TryRecvError};
use rkyv::util::AlignedVec;
use tokio::sync::mpsc::{UnboundedReceiver, unbounded_channel};
use tracing::{error, warn};

use crate::codec;
use crate::config::ChannelsConfig;
use crate::error::LogError;
use crate::offer_retry::{OFFER_TIMEOUT, offer_code_str};
use kardamom_types::{
    BPosition, BlockBoundary, Deposit, FsyncWatermark, QuorumWatermark, Receipt, TxDataLoc,
    TxEnvelope, TxError,
};

type AeronClient = rusteron_client::Aeron;
type Pub = rusteron_client::AeronPublication;
type Sub = rusteron_client::AeronSubscription;
type Header = rusteron_client::AeronHeader;

/// Closure that decodes one Aeron fragment + position + publisher `session_id`
/// and forwards the decoded value (or its raw bytes) somewhere Send-friendly.
/// Boxed so different message types can share the subscription registration
/// path. Most consumers ignore `session_id`; the tx_data subscription uses it to
/// build a [`kardamom_types::TxDataLoc`] so concurrent ingress publishers on one
/// shard stay disambiguated.
pub type DeliverFn = Box<dyn FnMut(&[u8], BPosition, i32) + Send>;

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
    /// Replies with the assigned `sub_id` (needed to attach MDS destinations).
    OpenSubscription {
        uri: String,
        stream_id: i32,
        deliver: DeliverFn,
        ack: CbSender<Result<u32, LogError>>,
    },
    /// Attach a source endpoint to a multi-destination (`control-mode=manual`)
    /// subscription. Used to aggregate per-publisher streams (e.g. one ingress
    /// MDS subscription pulling receipts from every executor replica).
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

/// One row in the Aeron thread's subscription table.
struct SubEntry {
    sub: Sub,
    deliver: DeliverFn,
}

/// Escalating idle wait — the Rust analogue of Aeron's `BackoffIdleStrategy`,
/// which is what keeps the Java sealer stack's duty-cycle threads cheap.
///
/// Poll loops here wait on a channel with a SHORT timeout (the data-plane
/// poll cadence), and profiling showed that cadence dominating the
/// sequencer's CPU: three Aeron runtimes per process waking every 100µs cost
/// ~66% of the process's cycles at 2k tx/s — nearly all of it crossbeam's
/// pre-park spin (`sched_yield` storms), not work. The fix mirrors the
/// sealer's: stay at the base cadence while the loop is DOING something, and
/// once it has observed `grace` consecutive empty iterations, double the
/// wait per iteration up to `cap`; any work snaps it back to base.
///
/// Latency safety: senders wake the channel wait IMMEDIATELY regardless of
/// the timeout, so command/publish latency is untouched. Only the first
/// inbound data-plane fragment after a quiet spell can wait up to `cap`;
/// under steady traffic every poll finds fragments and the cadence never
/// leaves base.
pub struct IdleBackoff {
    base: Duration,
    cap: Duration,
    grace: u32,
    streak: u32,
}

impl IdleBackoff {
    pub fn new(base: Duration, cap: Duration, grace: u32) -> Self {
        Self {
            base,
            cap,
            grace,
            streak: 0,
        }
    }

    /// The loop did work this iteration — snap back to the base cadence.
    pub fn reset(&mut self) {
        self.streak = 0;
    }

    /// Record an empty iteration and return the wait to use for it.
    pub fn idle_wait(&mut self) -> Duration {
        self.streak = self.streak.saturating_add(1);
        if self.streak <= self.grace {
            return self.base;
        }
        let doublings = (self.streak - self.grace).min(10);
        self.base.saturating_mul(1u32 << doublings).min(self.cap)
    }
}

/// A publish awaiting delivery on the Aeron thread.
///
/// Why this queue exists: the Aeron thread is **single-threaded** and shared by
/// every publication *and* subscription on a process. If a publish is offered
/// in a blocking spin/sleep loop (the old `offer_blocking`, up to
/// [`OFFER_TIMEOUT`]), that same thread stops polling its subscriptions for the
/// whole back-pressure window. In the cluster that starves the executor's
/// `tx_ordering` subscription long enough (> Aeron's MIN flow-control receiver
/// timeout, ~2 s) that the sealer drops it from flow control, advances, and the
/// subscription's image develops an unfillable gap and goes end-of-stream — a
/// permanent freeze (the executor uses `no_unavailable_image_handler` and never
/// re-subscribes). A *must-deliver publish* must never starve a *must-deliver
/// subscribe*.
///
/// So a back-pressured offer is parked here and retried **one attempt per loop
/// iteration** instead of blocking: the poll loop keeps draining subscriptions
/// between attempts. Per-publication FIFO order is preserved (a publication with
/// an older pending frame is not skipped ahead).
struct PendingPublish {
    pub_id: u32,
    bytes: AlignedVec,
    /// `Some` for an ack'd publish (`publish_bytes`); `None` for best-effort.
    ack: Option<CbSender<Result<BPosition, LogError>>>,
    /// Give up (ack an error / log) once this instant passes — bounds a publish
    /// to a never-connecting subscriber, matching the old blocking deadline.
    deadline: Instant,
}

impl AeronRuntime {
    /// Build an Aeron client (using the default `aeron_dir`) and spawn the
    /// dedicated Aeron thread.
    pub fn spawn_default() -> Result<Self, LogError> {
        Self::spawn_with(|| {
            rusteron_client::AeronContext::new()
                .map_err(|e| LogError::Aeron(format!("AeronContext::new: {e}")))
        })
    }

    /// Spawn pointing at a specific `aeron.dir` (the Media Driver's shared-
    /// memory directory). Used by e2e tests that bind-mount the container's
    /// aeron.dir into the host.
    pub fn spawn_with_dir(aeron_dir: impl Into<std::path::PathBuf>) -> Result<Self, LogError> {
        let aeron_dir = aeron_dir.into();
        let aeron_dir_str = aeron_dir
            .to_str()
            .ok_or_else(|| LogError::Aeron(format!("aeron.dir is not UTF-8: {aeron_dir:?}")))?
            .to_string();
        let aeron_dir_c = std::ffi::CString::new(aeron_dir_str.clone()).map_err(|_| {
            LogError::Aeron(format!("aeron.dir contains a NUL byte: {aeron_dir_str}"))
        })?;
        Self::spawn_with(move || {
            let ctx = rusteron_client::AeronContext::new()
                .map_err(|e| LogError::Aeron(format!("AeronContext::new: {e}")))?;
            ctx.set_dir(aeron_dir_c.as_c_str())
                .map_err(|e| LogError::Aeron(format!("set_dir: {e}")))?;
            Ok(ctx)
        })
    }

    /// Spawn the Aeron thread, building the `AeronContext` inside the thread
    /// via the caller-supplied closure. The closure runs on the Aeron thread
    /// — this is the only way to feed it custom configuration without
    /// crossing the `!Send + !Sync` boundary that AeronContext sits on.
    pub fn spawn_with<F>(make_ctx: F) -> Result<Self, LogError>
    where
        F: FnOnce() -> Result<rusteron_client::AeronContext, LogError> + Send + 'static,
    {
        let (started_tx, started_rx) = crossbeam_channel::bounded::<Result<(), LogError>>(1);
        let (cmd_tx, cmd_rx) = crossbeam_channel::unbounded::<RuntimeCmd>();

        let join = std::thread::Builder::new()
            .name("kardamom-aeron".into())
            .spawn(move || {
                let aeron = match make_ctx().and_then(build_aeron) {
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

    /// Open a raw subscription with a caller-supplied delivery closure.
    /// Used by adapters that need to demultiplex fragments themselves.
    /// Open a subscription with a raw deliver closure, returning the assigned
    /// `sub_id` (used to attach MDS destinations; most callers ignore it).
    pub fn open_subscription_with_deliver(
        &self,
        uri: &str,
        stream_id: i32,
        deliver: DeliverFn,
    ) -> Result<u32, LogError> {
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
            .map_err(|_| LogError::Aeron("open_subscription timed out".into()))?
    }

    /// Attach a source endpoint to a multi-destination subscription (one opened
    /// `control-mode=manual`). Blocks until the driver confirms the attach.
    /// Idempotent — re-adding an already-attached `uri` is a no-op.
    pub fn add_destination(&self, sub_id: u32, uri: &str) -> Result<(), LogError> {
        let (ack_tx, ack_rx) = crossbeam_channel::bounded(1);
        self.cmd_tx
            .send(RuntimeCmd::SubAddDestination {
                sub_id,
                uri: uri.to_string(),
                ack: ack_tx,
            })
            .map_err(|_| LogError::Aeron("aeron thread dropped".into()))?;
        ack_rx
            .recv_timeout(Duration::from_secs(10))
            .map_err(|_| LogError::Aeron("add_destination timed out".into()))?
    }

    /// Detach a previously-attached source endpoint from an MDS subscription.
    pub fn remove_destination(&self, sub_id: u32, uri: &str) -> Result<(), LogError> {
        let (ack_tx, ack_rx) = crossbeam_channel::bounded(1);
        self.cmd_tx
            .send(RuntimeCmd::SubRemoveDestination {
                sub_id,
                uri: uri.to_string(),
                ack: ack_tx,
            })
            .map_err(|_| LogError::Aeron("aeron thread dropped".into()))?;
        ack_rx
            .recv_timeout(Duration::from_secs(10))
            .map_err(|_| LogError::Aeron("remove_destination timed out".into()))?
    }

    /// Open a typed subscription, returning an mpsc receiver of decoded
    /// messages.
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
        self.open_subscription_merged(std::slice::from_ref(&uri), stream_id)
    }

    /// Open one or more subscriptions on the **same** `stream_id`, all feeding
    /// a single mpsc receiver. Each URI becomes its own Aeron subscription
    /// (its own `SubEntry`); fragments from every one are decoded and merged
    /// into the returned channel in the Aeron thread's poll order — the same
    /// merge the shared-multicast path produced from multiple images of one
    /// subscription.
    ///
    /// This is the tx_ordering MDC subscriber primitive: the executor passes
    /// one MDC control URI per publisher (sealer + each sequencer), and the
    /// downstream reader sees a single ordered `(BPosition, T)` stream exactly
    /// as before. With a single URI it is identical to
    /// [`Self::open_subscription`].
    pub fn open_subscription_merged<T>(
        &self,
        uris: &[&str],
        stream_id: i32,
    ) -> Result<UnboundedReceiver<(BPosition, T)>, LogError>
    where
        T: rkyv::Archive + Send + 'static,
        T::Archived: rkyv::Deserialize<T, rkyv::api::high::HighDeserializer<rkyv::rancor::Error>>
            + for<'a> rkyv::bytecheck::CheckBytes<
                rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>,
            >,
    {
        if uris.is_empty() {
            return Err(LogError::Aeron(
                "open_subscription_merged requires at least one URI".into(),
            ));
        }
        let (msg_tx, msg_rx) = unbounded_channel::<(BPosition, T)>();
        for uri in uris {
            self.open_subscription_with_deliver(uri, stream_id, typed_deliver(msg_tx.clone()))?;
        }
        Ok(msg_rx)
    }

    /// Like [`open_subscription`](Self::open_subscription) but also returns the
    /// `sub_id`, so the caller can attach MDS source endpoints via
    /// [`add_destination`](Self::add_destination). Open the subscription on a
    /// `control-mode=manual` channel to make it multi-destination.
    pub fn open_subscription_with_id<T>(
        &self,
        uri: &str,
        stream_id: i32,
    ) -> Result<(u32, UnboundedReceiver<(BPosition, T)>), LogError>
    where
        T: rkyv::Archive + Send + 'static,
        T::Archived: rkyv::Deserialize<T, rkyv::api::high::HighDeserializer<rkyv::rancor::Error>>
            + for<'a> rkyv::bytecheck::CheckBytes<
                rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>,
            >,
    {
        let (msg_tx, msg_rx) = unbounded_channel::<(BPosition, T)>();
        let sub_id = self.open_subscription_with_deliver(uri, stream_id, typed_deliver(msg_tx))?;
        Ok((sub_id, msg_rx))
    }

    /// Open a **tx_data** subscription yielding `(TxDataLoc, TxEnvelope)`,
    /// pairing each envelope with its Aeron publisher `session_id`. The session
    /// id disambiguates concurrent (active/active) ingress publishers on one
    /// shard: it is what the sequencer stamps into `TxRef.tx_data_session_id`
    /// and the executor keys its join buffer on. With a single publisher every
    /// fragment carries the same session id, so behavior is unchanged.
    pub fn open_tx_data_subscription(
        &self,
        uri: &str,
        stream_id: i32,
    ) -> Result<UnboundedReceiver<(TxDataLoc, TxEnvelope)>, LogError> {
        let (msg_tx, msg_rx) = unbounded_channel::<(TxDataLoc, TxEnvelope)>();
        let deliver: DeliverFn = Box::new(move |bytes: &[u8], pos: BPosition, session: i32| {
            match codec::materialize::<TxEnvelope>(bytes) {
                Ok(v) => {
                    let _ = msg_tx.send((TxDataLoc::new(session, pos), v));
                }
                Err(e) => {
                    error!(error = %e, "decode failed on tx_data subscription delivery");
                }
            }
        });
        self.open_subscription_with_deliver(uri, stream_id, deliver)?;
        Ok(msg_rx)
    }
}

/// Build the standard typed deliver closure: decode each fragment as `T` and
/// forward `(BPosition, T)` into `msg_tx` (a send error means the subscriber
/// dropped its receiver — silently ignored; the sub keeps draining). Shared by
/// [`AeronRuntime::open_subscription_merged`] and
/// [`AeronRuntime::open_subscription_with_id`] so the decode/forward behaviour
/// cannot drift between them.
fn typed_deliver<T>(msg_tx: tokio::sync::mpsc::UnboundedSender<(BPosition, T)>) -> DeliverFn
where
    T: rkyv::Archive + Send + 'static,
    T::Archived: rkyv::Deserialize<T, rkyv::api::high::HighDeserializer<rkyv::rancor::Error>>
        + for<'a> rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>>,
{
    Box::new(move |bytes: &[u8], pos: BPosition, _session: i32| {
        match codec::materialize::<T>(bytes) {
            Ok(v) => {
                let _ = msg_tx.send((pos, v));
            }
            Err(e) => {
                error!(error = %e, "decode failed on subscription delivery");
            }
        }
    })
}

impl Drop for AeronRuntime {
    fn drop(&mut self) {
        if Arc::strong_count(&self.join) == 1 {
            let _ = self.cmd_tx.send(RuntimeCmd::Shutdown);
            if let Some(j) = self.join.lock().unwrap().take() {
                let _ = j.join();
            }
        }
    }
}

fn build_aeron(ctx: rusteron_client::AeronContext) -> Result<Rc<AeronClient>, LogError> {
    let aeron = AeronClient::new(&ctx).map_err(|e| LogError::Aeron(format!("Aeron::new: {e}")))?;
    aeron
        .start()
        .map_err(|e| LogError::Aeron(format!("Aeron::start: {e}")))?;
    Ok(Rc::new(aeron))
}

fn run_aeron_thread(
    aeron: Rc<AeronClient>,
    cmd_rx: CbReceiver<RuntimeCmd>,
) -> Result<(), LogError> {
    let mut pubs: Vec<Pub> = Vec::new();
    let mut subs: Vec<SubEntry> = Vec::new();
    let mut pending: VecDeque<PendingPublish> = VecDeque::new();
    // Live MDS destinations. The rusteron `AeronAsyncDestination` removes its
    // destination when dropped, so we must retain each one for as long as the
    // attachment should stay active; keyed by (sub_id, uri) for removal.
    let mut dests: Vec<(u32, String, rusteron_client::AeronAsyncDestination)> = Vec::new();
    // Escalating idle wait for the busy branch: base 100µs (the established
    // sub-poll/retry cadence), cap 1ms (the empty-branch cadence), grace 10
    // (~1ms of consecutive emptiness before the first escalation).
    let mut backoff = IdleBackoff::new(Duration::from_micros(100), Duration::from_millis(1), 10);

    loop {
        // Whether THIS iteration did anything: handled a command, or polled
        // at least one fragment. Drives the idle backoff — an empty streak
        // escalates the wait, any work snaps it back to base.
        let mut worked = false;

        // 1. Drain all queued commands (non-blocking). Publishes are *enqueued*
        //    onto `pending`, never offered inline, so a back-pressured offer can
        //    never block this loop (see `PendingPublish`).
        loop {
            match cmd_rx.try_recv() {
                Ok(RuntimeCmd::Shutdown) => return Ok(()),
                Ok(cmd) => {
                    worked = true;
                    handle_cmd(&aeron, &mut pubs, &mut subs, &mut pending, &mut dests, cmd)?;
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => return Ok(()),
            }
        }

        // 2. Attempt one offer per pending publish, preserving per-publication
        //    FIFO order. Successful/expired entries are removed.
        drain_pending(&pubs, &mut pending);

        // 3. Poll every subscription. This runs on *every* iteration — even
        //    while a publish is back-pressured in `pending` — so a slow/stalled
        //    publish can never starve a subscription's image. This is the fix
        //    for the cluster `tx_ordering` freeze.
        for entry in subs.iter_mut() {
            let fragments = entry.sub.poll_once(
                |bytes: &[u8], header: Header| {
                    if let Some((pos, session)) = header_loc(&header) {
                        (entry.deliver)(bytes, pos, session);
                    }
                },
                64,
            );
            worked |= fragments.unwrap_or(0) > 0;
        }

        // 4. Idle. Block only when there is genuinely nothing to do — nothing to
        //    poll and nothing pending; otherwise wait at the poll/retry cadence
        //    without busy-spinning a core.
        if subs.is_empty() && pending.is_empty() {
            match cmd_rx.recv_timeout(Duration::from_millis(1)) {
                Ok(RuntimeCmd::Shutdown) => return Ok(()),
                Ok(cmd) => handle_cmd(&aeron, &mut pubs, &mut subs, &mut pending, &mut dests, cmd)?,
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => {}
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => return Ok(()),
            }
        } else {
            // Keep the 100µs sub-poll/retry cadence WHILE TRAFFIC FLOWS, but
            // wake IMMEDIATELY on a new command instead of sleeping through
            // it: with any sub open (always, in the services) this branch is
            // the steady state, and a plain sleep put up to 100µs of latency
            // under every ack-waited publish — a hard ~10k/s cap on any
            // serialized publisher (the sequencer's offer path first among
            // them). When quiet, the wait escalates toward 1ms (IdleBackoff):
            // waking every 100µs regardless of traffic measured as ~66% of
            // the sequencer's CPU, almost all of it crossbeam's pre-park
            // spin. A non-empty `pending` pins the base cadence — the retry
            // timing of a back-pressured offer must not degrade.
            if worked || !pending.is_empty() {
                backoff.reset();
            }
            let wait = if worked || !pending.is_empty() {
                Duration::from_micros(100)
            } else {
                backoff.idle_wait()
            };
            match cmd_rx.recv_timeout(wait) {
                Ok(RuntimeCmd::Shutdown) => return Ok(()),
                Ok(cmd) => {
                    backoff.reset();
                    handle_cmd(&aeron, &mut pubs, &mut subs, &mut pending, &mut dests, cmd)?;
                }
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => {}
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => return Ok(()),
            }
        }
    }
}

fn handle_cmd(
    aeron: &Rc<AeronClient>,
    pubs: &mut Vec<Pub>,
    subs: &mut Vec<SubEntry>,
    pending: &mut VecDeque<PendingPublish>,
    dests: &mut Vec<(u32, String, rusteron_client::AeronAsyncDestination)>,
    cmd: RuntimeCmd,
) -> Result<(), LogError> {
    match cmd {
        // Publishes are never offered here — they are enqueued and retried by
        // `drain_pending` so a back-pressured offer can't block the poll loop.
        RuntimeCmd::Publish { pub_id, bytes, ack } => {
            pending.push_back(PendingPublish {
                pub_id,
                bytes,
                ack: Some(ack),
                deadline: Instant::now() + OFFER_TIMEOUT,
            });
        }
        RuntimeCmd::PublishBestEffort { pub_id, bytes } => {
            pending.push_back(PendingPublish {
                pub_id,
                bytes,
                ack: None,
                deadline: Instant::now() + OFFER_TIMEOUT,
            });
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
                (subs.len() - 1) as u32
            });
            let _ = ack.send(res);
        }
        RuntimeCmd::SubAddDestination { sub_id, uri, ack } => {
            let res = add_sub_destination(aeron, subs, dests, sub_id, &uri);
            let _ = ack.send(res);
        }
        RuntimeCmd::SubRemoveDestination { sub_id, uri, ack } => {
            // Dropping the retained `AeronAsyncDestination` issues the async
            // remove command to the driver. Best-effort: a removed source's
            // image also times out on its own.
            let before = dests.len();
            dests.retain(|(s, u, _)| !(*s == sub_id && *u == uri));
            let res = if dests.len() < before {
                Ok(())
            } else {
                Err(LogError::Aeron(format!(
                    "remove destination: no attached {uri} on sub {sub_id}"
                )))
            };
            let _ = ack.send(res);
        }
        RuntimeCmd::Shutdown => {}
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

/// Attach a source endpoint (`uri`, e.g. `aeron:udp?endpoint=10.0.0.5:9000`) to
/// a `control-mode=manual` MDS subscription and retain the returned
/// `AeronAsyncDestination` so the attachment stays live (dropping it issues the
/// async remove). Idempotent. Blocks the Aeron thread only briefly to poll the
/// driver's async completion — destination changes are infrequent (membership
/// churn), unlike steady-state publishing.
fn add_sub_destination(
    aeron: &Rc<AeronClient>,
    subs: &[SubEntry],
    dests: &mut Vec<(u32, String, rusteron_client::AeronAsyncDestination)>,
    sub_id: u32,
    uri: &str,
) -> Result<(), LogError> {
    let sub = subs
        .get(sub_id as usize)
        .ok_or_else(|| LogError::Aeron(format!("add destination: unknown sub_id {sub_id}")))?;
    if dests.iter().any(|(s, u, _)| *s == sub_id && u == uri) {
        return Ok(()); // already attached
    }
    let c = CString::new(uri).map_err(|e| LogError::Aeron(format!("destination uri NUL: {e}")))?;
    let dest = rusteron_client::AeronAsyncDestination::aeron_subscription_async_add_destination(
        aeron,
        &sub.sub,
        c.as_c_str(),
    )
    .map_err(|e| LogError::Aeron(format!("add destination {uri}: {e}")))?;
    let start = Instant::now();
    loop {
        match dest.aeron_subscription_async_destination_poll() {
            Ok(1) => break,
            Ok(_) => {}
            Err(e) => return Err(LogError::Aeron(format!("destination poll {uri}: {e}"))),
        }
        if start.elapsed() > ADD_SUB_TIMEOUT {
            return Err(LogError::Aeron(format!("add destination {uri} timed out")));
        }
        std::thread::sleep(Duration::from_millis(2));
    }
    dests.push((sub_id, uri.to_string(), dest));
    Ok(())
}

/// Attempt one offer for each pending publish, oldest first, preserving
/// per-publication FIFO order: once a publication back-pressures this pass, its
/// later frames are held too so the stream never reorders. Successful and
/// expired (past-deadline) entries are removed; transiently-failing entries are
/// retained for the next loop iteration.
///
/// Crucially this performs **one** offer attempt per entry and returns — it
/// never spins/sleeps — so the caller ([`run_aeron_thread`]) goes straight back
/// to polling subscriptions. That is what stops a back-pressured publish from
/// starving a subscription image (see [`PendingPublish`]).
fn drain_pending(pubs: &[Pub], pending: &mut VecDeque<PendingPublish>) {
    drain_pending_inner(pending, Instant::now(), |item| {
        match pubs.get(item.pub_id as usize) {
            None => OfferResult::UnknownPub,
            Some(p) => OfferResult::Code(p.offer(
                item.bytes.as_slice(),
                rusteron_client::Handlers::no_reserved_value_supplier_handler(),
            )),
        }
    })
}

/// Outcome of attempting one offer for a [`PendingPublish`].
enum OfferResult {
    /// The publication id is not registered (programming error / use-after-close).
    UnknownPub,
    /// Aeron's raw offer return: `>= 0` is a stream position, `< 0` a status code.
    Code(i64),
}

/// Pure core of [`drain_pending`], with the Aeron offer injected so the FIFO /
/// deadline / back-pressure decisions are unit-testable without a media driver.
/// `now` is threaded in for the same reason (deterministic deadline checks).
fn drain_pending_inner<F>(pending: &mut VecDeque<PendingPublish>, now: Instant, mut offer: F)
where
    F: FnMut(&PendingPublish) -> OfferResult,
{
    if pending.is_empty() {
        return;
    }
    // Publications that already back-pressured this pass; their remaining frames
    // wait so we never deliver a stream out of order.
    let mut blocked: Vec<u32> = Vec::new();
    let mut keep: VecDeque<PendingPublish> = VecDeque::with_capacity(pending.len());

    while let Some(mut item) = pending.pop_front() {
        if blocked.contains(&item.pub_id) {
            // Deadlines are enforced on EVERY retained entry each pass — not
            // only when an entry reaches the head. Frames parked behind a
            // blocked head would otherwise expire one-by-one (~OFFER_TIMEOUT
            // each), so a caller ≥2 deep could hit its ack timeout while its
            // frame was still queued — and then have the frame delivered late
            // once the subscriber connected, after the caller already treated
            // the publish as failed (and possibly re-submitted). Expiring on
            // time here (OFFER_TIMEOUT < publish_bytes's ack timeout) means
            // every ack resolves before its caller gives up, so a
            // reported-failed publish is never delivered afterwards.
            if now >= item.deadline {
                let msg = "aeron offer failed: expired while queued behind a blocked \
                           publication"
                    .to_string();
                match item.ack.take() {
                    Some(ack) => {
                        let _ = ack.send(Err(LogError::Aeron(msg)));
                    }
                    None => warn!(
                        pub_id = item.pub_id,
                        "best-effort publish expired while queued behind a blocked publication"
                    ),
                }
            } else {
                keep.push_back(item);
            }
            continue;
        }
        match offer(&item) {
            OfferResult::UnknownPub => {
                // Fail/log immediately, never retry.
                match item.ack.take() {
                    Some(ack) => {
                        let _ = ack.send(Err(LogError::Aeron(format!(
                            "publish: unknown pub_id {}",
                            item.pub_id
                        ))));
                    }
                    None => warn!(
                        pub_id = item.pub_id,
                        "best-effort publish to unknown pub_id"
                    ),
                }
            }
            OfferResult::Code(code) if code >= 0 => {
                // Delivered. Ack the stream position; best-effort needs no ack.
                // Don't block this pub_id: a later frame for it may also go now.
                if let Some(ack) = item.ack.take() {
                    let _ = ack.send(Ok(decode_position(code)));
                }
            }
            OfferResult::Code(code) if now >= item.deadline => {
                // Gave up (e.g. a subscriber that never joined). Surface the error
                // so an ack'd must-deliver caller can decide to re-submit.
                let msg = format!("aeron offer failed: {} ({code})", offer_code_str(code));
                match item.ack.take() {
                    Some(ack) => {
                        let _ = ack.send(Err(LogError::Aeron(msg)));
                    }
                    None => warn!(
                        code = offer_code_str(code),
                        pub_id = item.pub_id,
                        "best-effort publish failed (deadline)"
                    ),
                }
            }
            OfferResult::Code(_) => {
                // Transient NOT_CONNECTED / BACK_PRESSURED: hold this frame and
                // every later frame on the same publication; retry next iteration.
                blocked.push(item.pub_id);
                keep.push_back(item);
            }
        }
    }
    *pending = keep;
}

fn decode_position(p: i64) -> BPosition {
    let term_id = (p >> 32) as i32;
    let term_offset = (p & 0xFFFF_FFFF) as i32;
    BPosition {
        term_id,
        term_offset,
    }
}

/// Read the fragment-start [`BPosition`] and the Aeron publisher `session_id`
/// from a SINGLE `get_values()` FFI call — the hottest per-fragment path. They
/// must come from the same header read: returning them together guarantees the
/// session belongs to the position's fragment and avoids a divergent
/// second-read error path (where a transient failure could mint a spurious
/// session 0 that collides with a genuine session-0 publisher's join key).
fn header_loc(h: &Header) -> Option<(BPosition, i32)> {
    let v = h.get_values().ok()?;
    let frame = v.frame();
    let pos = BPosition {
        term_id: frame.term_id(),
        term_offset: frame.term_offset(),
    };
    Some((pos, frame.session_id()))
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
    /// How long `publish_bytes` waits for the Aeron thread's ack. Must stay
    /// comfortably ABOVE [`OFFER_TIMEOUT`] (the per-frame queue deadline,
    /// enforced for every queued frame each drain pass): that ordering
    /// guarantees the ack (delivered or expired) always resolves before this
    /// timeout fires, so a caller can never give up on a frame that later gets
    /// delivered behind its back.
    const ACK_TIMEOUT: Duration = Duration::from_secs(10);

    /// Blocking publish with `BPosition` ack.
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
            .recv_timeout(Self::ACK_TIMEOUT)
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
// TxData: per-shard envelope channel (proxy → seq/exec/batcher).
// ---------------------------------------------------------------------------

/// Per-shard TxData publisher. Publishes full `TxEnvelope` bytes.
#[derive(Clone)]
pub struct TxDataPublisherHandle {
    inner: PubHandle,
}

impl TxDataPublisherHandle {
    pub fn open(
        rt: &AeronRuntime,
        ch: &ChannelsConfig,
        sequencer_id: u8,
    ) -> Result<Self, LogError> {
        Ok(Self {
            inner: rt.open_publication(
                &ch.tx_data_channel(sequencer_id),
                ch.tx_data_stream_id(sequencer_id),
            )?,
        })
    }

    pub fn publish(&self, env: &TxEnvelope) -> Result<BPosition, LogError> {
        self.inner.publish(env)
    }

    pub fn raw(&self) -> &PubHandle {
        &self.inner
    }
}

/// Per-shard TxData subscriber. Yields `(TxDataLoc, TxEnvelope)` — the envelope
/// paired with its publisher `session_id` + `BPosition`, so the sequencer can
/// stamp `TxRef.tx_data_session_id` and the executor can key its join buffer on
/// `(shard, session, position)` under concurrent ingress publishers.
pub struct TxDataSubscriberHandle {
    rx: UnboundedReceiver<(TxDataLoc, TxEnvelope)>,
}

impl TxDataSubscriberHandle {
    pub fn open(
        rt: &AeronRuntime,
        ch: &ChannelsConfig,
        sequencer_id: u8,
    ) -> Result<Self, LogError> {
        Ok(Self {
            rx: rt.open_tx_data_subscription(
                &ch.tx_data_channel(sequencer_id),
                ch.tx_data_stream_id(sequencer_id),
            )?,
        })
    }

    pub async fn recv(&mut self) -> Option<(TxDataLoc, TxEnvelope)> {
        self.rx.recv().await
    }

    pub fn try_recv(&mut self) -> Option<(TxDataLoc, TxEnvelope)> {
        self.rx.try_recv().ok()
    }
}

// ---------------------------------------------------------------------------
// TxReceipts: receipts + boundaries (RAM only).
// ---------------------------------------------------------------------------

/// TxReceipts publisher. The executor uses `publish_receipt` and
/// `publish_boundary` on the same channel, but with separate stream ids so
/// subscribers can demultiplex without an in-band tag.
///
/// Two open modes:
/// - [`open`](Self::open): the legacy **single shared channel**
///   (`tx_receipts_channel`) — the lone executor publishes, ingress subscribes
///   directly. This is the single-host IPC default.
/// - [`open_mds`](Self::open_mds): the **multi-destination-subscription**
///   (fan-in) path. Each executor replica publishes to its OWN unicast UDP
///   endpoint (`ch.tx_receipts_endpoint(replica_idx)`), and the single ingress
///   attaches every replica's endpoint to one `control-mode=manual`
///   subscription. Both modes publish receipts on `tx_receipts_stream_id` and
///   boundaries on `tx_receipts_stream_id + 1`; only the channel URI differs,
///   so `publish_receipt`/`publish_boundary` (and the executor commit thread's
///   must-deliver retry that drives them) are identical across modes.
#[derive(Clone)]
pub struct TxReceiptsPublisherHandle {
    inner: PubHandle,
    boundary: PubHandle,
}

impl TxReceiptsPublisherHandle {
    /// Legacy single-shared-channel publisher (IPC default). Use when
    /// `ch.tx_receipts_mds_enabled()` is false.
    pub fn open(rt: &AeronRuntime, ch: &ChannelsConfig) -> Result<Self, LogError> {
        Ok(Self {
            inner: rt.open_publication(&ch.tx_receipts_channel, ch.tx_receipts_stream_id)?,
            boundary: rt.open_publication(&ch.tx_receipts_channel, ch.tx_receipts_stream_id + 1)?,
        })
    }

    /// MDS (fan-in) publisher: this replica publishes BOTH streams to its own
    /// per-replica unicast endpoint `ch.tx_receipts_endpoint(replica_idx)`,
    /// which ingress attaches as a destination on its aggregating subscription.
    /// `replica_idx` is the executor's recorder-id (`NOMAD_ALLOC_INDEX`).
    ///
    /// Errors if MDS is not configured (no `tx_receipts_control_channel`), so a
    /// misconfigured executor fails fast instead of silently using IPC.
    pub fn open_mds(
        rt: &AeronRuntime,
        ch: &ChannelsConfig,
        replica_idx: u32,
    ) -> Result<Self, LogError> {
        let endpoint = ch.tx_receipts_endpoint(replica_idx).ok_or_else(|| {
            LogError::Aeron(format!(
                "open_mds: tx_receipts MDS not configured (replica {replica_idx})"
            ))
        })?;
        // Boundaries publish to a DISTINCT endpoint (port) from receipts, since
        // ingress's two manual subscriptions each bind their destination socket
        // — a shared endpoint collides. See ChannelsConfig::tx_receipts_endpoint.
        let boundary_endpoint = ch
            .tx_receipts_boundary_endpoint(replica_idx)
            .ok_or_else(|| {
                LogError::Aeron(format!(
                    "open_mds: tx_receipts boundary endpoint not configured (replica {replica_idx})"
                ))
            })?;
        Ok(Self {
            inner: rt.open_publication(&endpoint, ch.tx_receipts_stream_id)?,
            boundary: rt.open_publication(&boundary_endpoint, ch.tx_receipts_stream_id + 1)?,
        })
    }

    pub fn publish_receipt(&self, r: &Receipt) -> Result<BPosition, LogError> {
        self.inner.publish(r)
    }

    pub fn publish_boundary(&self, b: &BlockBoundary) -> Result<BPosition, LogError> {
        self.boundary.publish(b)
    }

    /// Fire-and-forget boundary publish. The block-boundary side-stream is a
    /// marker — ingress acks on the receipt / durable watermark, NOT on this —
    /// so it must NEVER block the executor's commit thread. A must-deliver
    /// boundary that can't reach a not-yet-connected ingress (e.g. during
    /// startup before ingress's MDS destinations attach) would back up the
    /// commit→exec channel and freeze ALL state progress. Encodes and hands the
    /// frame to the Aeron thread; delivery is best-effort.
    pub fn publish_boundary_best_effort(&self, b: &BlockBoundary) -> Result<(), LogError> {
        let bytes = codec::encode(b)?;
        self.boundary.publish_best_effort(bytes);
        Ok(())
    }
}

/// TxReceipts subscriber for receipts.
///
/// In the legacy IPC path ([`open`](Self::open)) this is a plain subscription
/// on the shared `tx_receipts_channel`. In the MDS fan-in path
/// ([`open_mds`](Self::open_mds)) it is opened on the `control-mode=manual`
/// `tx_receipts_control_channel`; the caller then attaches each executor
/// replica's endpoint via [`add_destination`](Self::add_destination). The
/// retained `sub_id` is what `add_destination`/`remove_destination` target.
pub struct TxReceiptsSubscriberHandle {
    rx: UnboundedReceiver<(BPosition, Receipt)>,
    /// `Some` only in the MDS path — the subscription id MDS destinations
    /// attach to. `None` for the legacy single-channel subscription.
    sub_id: Option<u32>,
    rt: AeronRuntime,
}

impl TxReceiptsSubscriberHandle {
    /// Legacy single-shared-channel subscriber (IPC default).
    pub fn open(rt: &AeronRuntime, ch: &ChannelsConfig) -> Result<Self, LogError> {
        Ok(Self {
            rx: rt
                .open_subscription::<Receipt>(&ch.tx_receipts_channel, ch.tx_receipts_stream_id)?,
            sub_id: None,
            rt: rt.clone(),
        })
    }

    /// MDS (fan-in) subscriber: one `control-mode=manual` subscription on
    /// `ch.tx_receipts_control_channel` that the caller attaches per-replica
    /// executor endpoints to. Errors if MDS is not configured.
    pub fn open_mds(rt: &AeronRuntime, ch: &ChannelsConfig) -> Result<Self, LogError> {
        if !ch.tx_receipts_mds_enabled() {
            return Err(LogError::Aeron(
                "open_mds: tx_receipts MDS not configured (empty control channel)".into(),
            ));
        }
        let (sub_id, rx) = rt.open_subscription_with_id::<Receipt>(
            &ch.tx_receipts_control_channel,
            ch.tx_receipts_stream_id,
        )?;
        Ok(Self {
            rx,
            sub_id: Some(sub_id),
            rt: rt.clone(),
        })
    }

    /// Attach an executor replica's endpoint as an MDS destination. Only valid
    /// on a handle opened via [`open_mds`](Self::open_mds). Idempotent.
    pub fn add_destination(&self, uri: &str) -> Result<(), LogError> {
        match self.sub_id {
            Some(id) => self.rt.add_destination(id, uri),
            None => Err(LogError::Aeron(
                "add_destination on a non-MDS receipts subscription".into(),
            )),
        }
    }

    /// Detach a previously-attached executor endpoint (membership churn).
    pub fn remove_destination(&self, uri: &str) -> Result<(), LogError> {
        match self.sub_id {
            Some(id) => self.rt.remove_destination(id, uri),
            None => Err(LogError::Aeron(
                "remove_destination on a non-MDS receipts subscription".into(),
            )),
        }
    }

    pub async fn recv(&mut self) -> Option<(BPosition, Receipt)> {
        self.rx.recv().await
    }

    pub fn try_recv(&mut self) -> Option<(BPosition, Receipt)> {
        self.rx.try_recv().ok()
    }

    /// Drop the handle's `AeronRuntime` clone, keeping only the receiver.
    ///
    /// USE THIS when the receiver is moved into a long-lived pump task that
    /// ends on `recv() == None`. Keeping the whole handle there creates an
    /// ownership CYCLE that makes the process unkillable by SIGTERM: the
    /// runtime shuts down only when its LAST clone drops
    /// ([`AeronRuntime::drop`]), the shutdown is what closes subscriptions,
    /// and closing them is what makes `recv()` return `None` — so a pump
    /// task holding a clone waits for a shutdown that its own clone is
    /// preventing. `drop(rt)` in `main` then silently does nothing, every
    /// other subscription stays open, and any thread joining on
    /// end-of-stream hangs forever.
    ///
    /// Destinations can no longer be attached/detached afterwards, so call
    /// it only once MDS membership is established (destinations attached at
    /// open time survive — they live in the driver, not in this handle).
    pub fn into_receiver(self) -> UnboundedReceiver<(BPosition, Receipt)> {
        self.rx
    }
}

/// TxReceipts subscriber for boundaries. Mirrors
/// [`TxReceiptsSubscriberHandle`] but for the `tx_receipts_stream_id + 1`
/// boundary side-stream: [`open`](Self::open) for the legacy shared channel,
/// [`open_mds`](Self::open_mds) + [`add_destination`](Self::add_destination)
/// for the fan-in path.
pub struct TxReceiptsBoundarySubscriberHandle {
    rx: UnboundedReceiver<(BPosition, BlockBoundary)>,
    sub_id: Option<u32>,
    rt: AeronRuntime,
}

impl TxReceiptsBoundarySubscriberHandle {
    pub fn open(rt: &AeronRuntime, ch: &ChannelsConfig) -> Result<Self, LogError> {
        Ok(Self {
            rx: rt.open_subscription::<BlockBoundary>(
                &ch.tx_receipts_channel,
                ch.tx_receipts_stream_id + 1,
            )?,
            sub_id: None,
            rt: rt.clone(),
        })
    }

    /// MDS (fan-in) boundary subscriber on `ch.tx_receipts_control_channel`,
    /// stream `tx_receipts_stream_id + 1`. Errors if MDS is not configured.
    pub fn open_mds(rt: &AeronRuntime, ch: &ChannelsConfig) -> Result<Self, LogError> {
        if !ch.tx_receipts_mds_enabled() {
            return Err(LogError::Aeron(
                "open_mds: tx_receipts MDS not configured (empty control channel)".into(),
            ));
        }
        let (sub_id, rx) = rt.open_subscription_with_id::<BlockBoundary>(
            &ch.tx_receipts_control_channel,
            ch.tx_receipts_stream_id + 1,
        )?;
        Ok(Self {
            rx,
            sub_id: Some(sub_id),
            rt: rt.clone(),
        })
    }

    /// Attach an executor replica's endpoint as an MDS destination. Idempotent.
    pub fn add_destination(&self, uri: &str) -> Result<(), LogError> {
        match self.sub_id {
            Some(id) => self.rt.add_destination(id, uri),
            None => Err(LogError::Aeron(
                "add_destination on a non-MDS boundary subscription".into(),
            )),
        }
    }

    pub fn remove_destination(&self, uri: &str) -> Result<(), LogError> {
        match self.sub_id {
            Some(id) => self.rt.remove_destination(id, uri),
            None => Err(LogError::Aeron(
                "remove_destination on a non-MDS boundary subscription".into(),
            )),
        }
    }

    pub async fn recv(&mut self) -> Option<(BPosition, BlockBoundary)> {
        self.rx.recv().await
    }

    pub fn try_recv(&mut self) -> Option<(BPosition, BlockBoundary)> {
        self.rx.try_recv().ok()
    }
}

// ---------------------------------------------------------------------------
// TxErrors (sequencer → ingress).
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct TxErrorsPublisherHandle {
    inner: PubHandle,
}

impl TxErrorsPublisherHandle {
    pub fn open(rt: &AeronRuntime, ch: &ChannelsConfig) -> Result<Self, LogError> {
        Ok(Self {
            inner: rt.open_publication(&ch.tx_errors_channel, ch.tx_errors_stream_id)?,
        })
    }

    pub fn publish(&self, e: &TxError) -> Result<BPosition, LogError> {
        self.inner.publish(e)
    }

    pub fn raw(&self) -> &PubHandle {
        &self.inner
    }
}

pub struct TxErrorsSubscriberHandle {
    rx: UnboundedReceiver<(BPosition, TxError)>,
}

impl TxErrorsSubscriberHandle {
    pub fn open(rt: &AeronRuntime, ch: &ChannelsConfig) -> Result<Self, LogError> {
        Ok(Self {
            rx: rt.open_subscription::<TxError>(&ch.tx_errors_channel, ch.tx_errors_stream_id)?,
        })
    }

    pub async fn recv(&mut self) -> Option<(BPosition, TxError)> {
        self.rx.recv().await
    }

    pub fn try_recv(&mut self) -> Option<(BPosition, TxError)> {
        self.rx.try_recv().ok()
    }
}

// ---------------------------------------------------------------------------
// TxDeposits (DA watcher → sequencer).
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct TxDepositsPublisherHandle {
    inner: PubHandle,
}

impl TxDepositsPublisherHandle {
    pub fn open(rt: &AeronRuntime, ch: &ChannelsConfig) -> Result<Self, LogError> {
        Ok(Self {
            inner: rt.open_publication(&ch.tx_deposits_channel, ch.tx_deposits_stream_id)?,
        })
    }

    pub fn publish(&self, d: &Deposit) -> Result<BPosition, LogError> {
        self.inner.publish(d)
    }

    pub fn raw(&self) -> &PubHandle {
        &self.inner
    }
}

pub struct TxDepositsSubscriberHandle {
    rx: UnboundedReceiver<(BPosition, Deposit)>,
}

impl TxDepositsSubscriberHandle {
    pub fn open(rt: &AeronRuntime, ch: &ChannelsConfig) -> Result<Self, LogError> {
        Ok(Self {
            rx: rt
                .open_subscription::<Deposit>(&ch.tx_deposits_channel, ch.tx_deposits_stream_id)?,
        })
    }

    pub async fn recv(&mut self) -> Option<(BPosition, Deposit)> {
        self.rx.recv().await
    }

    pub fn try_recv(&mut self) -> Option<(BPosition, Deposit)> {
        self.rx.try_recv().ok()
    }
}

// ---------------------------------------------------------------------------
// Per-recorder fsync watermark.
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct FsyncWatermarkPublisherHandle {
    inner: PubHandle,
}

impl FsyncWatermarkPublisherHandle {
    pub fn open(rt: &AeronRuntime, ch: &ChannelsConfig, recorder_id: u8) -> Result<Self, LogError> {
        let channel = ch.fsync_watermark_channel(recorder_id);
        Ok(Self {
            inner: rt.open_publication(&channel, ch.fsync_watermark_stream_id)?,
        })
    }

    pub fn publish(&self, w: &FsyncWatermark) -> Result<(), LogError> {
        self.inner.publish(w).map(|_| ())
    }
}

pub struct FsyncWatermarkSubscriberHandle {
    rx: UnboundedReceiver<(BPosition, FsyncWatermark)>,
}

impl FsyncWatermarkSubscriberHandle {
    pub fn open(rt: &AeronRuntime, ch: &ChannelsConfig, recorder_id: u8) -> Result<Self, LogError> {
        let channel = ch.fsync_watermark_channel(recorder_id);
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

// ---------------------------------------------------------------------------
// Quorum watermark.
// ---------------------------------------------------------------------------

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
// Send/Sync compile-time assertions.
// ---------------------------------------------------------------------------

#[allow(dead_code)]
fn assert_send_sync<T: Send + Sync>() {}

#[allow(dead_code)]
fn assert_send<T: Send>() {}

const _: fn() = || {
    assert_send_sync::<AeronRuntime>();
    assert_send_sync::<PubHandle>();
    assert_send_sync::<TxDataPublisherHandle>();
    assert_send::<TxDataSubscriberHandle>();
    assert_send_sync::<TxReceiptsPublisherHandle>();
    assert_send::<TxReceiptsSubscriberHandle>();
    assert_send::<TxReceiptsBoundarySubscriberHandle>();
    assert_send_sync::<TxErrorsPublisherHandle>();
    assert_send::<TxErrorsSubscriberHandle>();
    assert_send_sync::<TxDepositsPublisherHandle>();
    assert_send::<TxDepositsSubscriberHandle>();
    assert_send_sync::<FsyncWatermarkPublisherHandle>();
    assert_send::<FsyncWatermarkSubscriberHandle>();
    assert_send_sync::<QuorumPublisherHandle>();
    assert_send::<QuorumSubscriberHandle>();
};

// ---------------------------------------------------------------------------
// Unit tests for the publish-retry scheduler (no media driver required).
//
// These pin the behaviour that fixes the cluster `tx_ordering` freeze: a
// back-pressured publish is *parked and retried*, never blocking the loop, and
// per-publication FIFO order is preserved across retries. The matching real-
// Aeron end-to-end proof (a back-pressured publish must not delay a live
// subscription's delivery) lives in `tests/offer_starvation.rs`.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod drain_pending_tests {
    use super::*;
    use crossbeam_channel::Receiver as CbReceiver;

    /// Build a pending publish with a one-byte payload tagged `marker` (so a test
    /// can identify which frame an offer is being asked about) and a deadline
    /// `dl_ms` from `base`. Returns the entry and its ack receiver.
    fn pending(
        pub_id: u32,
        marker: u8,
        base: Instant,
        dl_ms: u64,
    ) -> (PendingPublish, CbReceiver<Result<BPosition, LogError>>) {
        let (tx, rx) = crossbeam_channel::bounded(1);
        let mut bytes = AlignedVec::new();
        bytes.extend_from_slice(&[marker]);
        (
            PendingPublish {
                pub_id,
                bytes,
                ack: Some(tx),
                deadline: base + Duration::from_millis(dl_ms),
            },
            rx,
        )
    }

    #[test]
    fn delivers_and_acks_then_empties_queue() {
        let now = Instant::now();
        let (p, rx) = pending(0, 0xAA, now, 5_000);
        let mut q = VecDeque::from([p]);
        // Offer always succeeds with a stream position.
        drain_pending_inner(&mut q, now, |_| OfferResult::Code(64));
        assert!(q.is_empty(), "delivered frame must be removed");
        match rx.try_recv() {
            Ok(Ok(_pos)) => {}
            other => panic!("expected an Ok position ack, got {other:?}"),
        }
    }

    #[test]
    fn back_pressure_retains_frame_for_next_iteration() {
        // The crux of the fix: a back-pressured offer (whose deadline has NOT
        // passed) is kept in the queue and retried — never dropped, never
        // blocking. The ack must stay pending.
        let now = Instant::now();
        let (p, rx) = pending(0, 0xAA, now, 5_000);
        let mut q = VecDeque::from([p]);
        drain_pending_inner(&mut q, now, |_| {
            OfferResult::Code(-2 /* BACK_PRESSURED */)
        });
        assert_eq!(q.len(), 1, "back-pressured frame must be retained");
        assert!(rx.try_recv().is_err(), "must not ack a retained frame");

        // Next iteration the subscriber has drained — now it delivers.
        drain_pending_inner(&mut q, now, |_| OfferResult::Code(0));
        assert!(q.is_empty());
        assert!(matches!(rx.try_recv(), Ok(Ok(_))));
    }

    #[test]
    fn preserves_per_publication_fifo_under_back_pressure() {
        // Two frames on pub 0 (A then B). The first attempt back-pressures A; B
        // must NOT be offered ahead of A (would reorder the stream). We assert by
        // recording which markers the offer fn is asked about.
        let now = Instant::now();
        let (a, _ra) = pending(0, 0xA1, now, 5_000);
        let (b, _rb) = pending(0, 0xB2, now, 5_000);
        let mut q = VecDeque::from([a, b]);

        let mut offered: Vec<u8> = Vec::new();
        drain_pending_inner(&mut q, now, |item| {
            offered.push(item.bytes.as_slice()[0]);
            OfferResult::Code(-2) // A back-pressures
        });
        assert_eq!(
            offered,
            vec![0xA1],
            "only the head-of-line frame may be offered; B must not jump ahead"
        );
        assert_eq!(q.len(), 2, "both frames retained, still in order");
        assert_eq!(q[0].bytes.as_slice()[0], 0xA1);
        assert_eq!(q[1].bytes.as_slice()[0], 0xB2);
    }

    #[test]
    fn independent_publications_do_not_block_each_other() {
        // Pub 0 back-pressures but pub 1 is fine — pub 1 must still deliver.
        let now = Instant::now();
        let (a, ra) = pending(0, 0xA1, now, 5_000);
        let (b, rb) = pending(1, 0xB2, now, 5_000);
        let mut q = VecDeque::from([a, b]);

        drain_pending_inner(&mut q, now, |item| {
            if item.pub_id == 0 {
                OfferResult::Code(-2)
            } else {
                OfferResult::Code(0)
            }
        });
        assert_eq!(
            q.len(),
            1,
            "only the back-pressured pub-0 frame is retained"
        );
        assert_eq!(q[0].pub_id, 0);
        assert!(ra.try_recv().is_err(), "pub 0 still pending");
        assert!(matches!(rb.try_recv(), Ok(Ok(_))), "pub 1 delivered");
    }

    #[test]
    fn expired_frame_acks_an_error_and_is_dropped() {
        // A frame still failing once its deadline has passed must error out (so a
        // must-deliver caller can re-submit) rather than spin forever.
        let now = Instant::now();
        let (p, rx) = pending(0, 0xAA, now, 0 /* deadline == now */);
        let mut q = VecDeque::from([p]);
        // `now` already >= deadline, offer still negative.
        drain_pending_inner(&mut q, now, |_| {
            OfferResult::Code(-1 /* NOT_CONNECTED */)
        });
        assert!(q.is_empty(), "expired frame must be dropped");
        match rx.try_recv() {
            Ok(Err(LogError::Aeron(m))) => {
                assert!(
                    m.contains("NOT_CONNECTED"),
                    "error should name the code: {m}"
                )
            }
            other => panic!("expected an Aeron error ack, got {other:?}"),
        }
    }

    #[test]
    fn queued_frame_behind_blocked_head_expires_at_its_own_deadline() {
        // F16.5 regression: a frame parked BEHIND a blocked head must have its
        // deadline evaluated every pass — not only once it reaches the head.
        // Otherwise a caller could hit its ack timeout while the frame is
        // still queued, and the frame would be delivered late after the
        // caller already treated the publish as failed.
        let now = Instant::now();
        let (head, head_rx) = pending(0, 0xA1, now, 5_000); // head: not yet expired
        let (tail, tail_rx) = pending(0, 0xB2, now, 0); // tail: deadline == now
        let mut q = VecDeque::from([head, tail]);

        let mut offered: Vec<u8> = Vec::new();
        drain_pending_inner(&mut q, now, |item| {
            offered.push(item.bytes.as_slice()[0]);
            OfferResult::Code(-2) // head back-pressures
        });
        // Only the head was offered; the expired tail must NOT be offered
        // (that would deliver it out of FIFO order after the caller gave up).
        assert_eq!(offered, vec![0xA1]);
        assert_eq!(q.len(), 1, "only the unexpired head is retained");
        assert_eq!(q[0].bytes.as_slice()[0], 0xA1);
        assert!(head_rx.try_recv().is_err(), "head still pending");
        match tail_rx.try_recv() {
            Ok(Err(LogError::Aeron(m))) => assert!(
                m.contains("expired while queued"),
                "expired-in-queue error should say so: {m}"
            ),
            other => panic!("expected an expired-in-queue error ack, got {other:?}"),
        }
    }

    #[test]
    fn queued_frame_behind_blocked_head_with_live_deadline_is_retained() {
        // Companion to the expiry test: an unexpired frame behind a blocked
        // head stays queued in order (no reordering, no premature error).
        let now = Instant::now();
        let (head, _head_rx) = pending(0, 0xA1, now, 5_000);
        let (tail, tail_rx) = pending(0, 0xB2, now, 5_000);
        let mut q = VecDeque::from([head, tail]);

        drain_pending_inner(&mut q, now, |_| OfferResult::Code(-2));
        assert_eq!(q.len(), 2, "both frames retained, still in order");
        assert_eq!(q[0].bytes.as_slice()[0], 0xA1);
        assert_eq!(q[1].bytes.as_slice()[0], 0xB2);
        assert!(tail_rx.try_recv().is_err(), "tail must not be acked yet");
    }

    #[test]
    fn unknown_publication_id_fails_immediately() {
        let now = Instant::now();
        let (p, rx) = pending(9, 0xAA, now, 5_000);
        let mut q = VecDeque::from([p]);
        drain_pending_inner(&mut q, now, |_| OfferResult::UnknownPub);
        assert!(q.is_empty(), "unknown-pub frame must not be retried");
        match rx.try_recv() {
            Ok(Err(LogError::Aeron(m))) => assert!(m.contains("unknown pub_id")),
            other => panic!("expected unknown-pub error, got {other:?}"),
        }
    }

    #[test]
    fn idle_backoff_escalates_after_grace_and_resets_on_work() {
        let base = Duration::from_micros(100);
        let cap = Duration::from_millis(1);
        let mut b = IdleBackoff::new(base, cap, 3);
        // Within grace: base cadence.
        assert_eq!(b.idle_wait(), base);
        assert_eq!(b.idle_wait(), base);
        assert_eq!(b.idle_wait(), base);
        // Past grace: doubles per idle iteration…
        assert_eq!(b.idle_wait(), Duration::from_micros(200));
        assert_eq!(b.idle_wait(), Duration::from_micros(400));
        assert_eq!(b.idle_wait(), Duration::from_micros(800));
        // …and clamps at the cap, staying there.
        assert_eq!(b.idle_wait(), cap);
        for _ in 0..100 {
            assert_eq!(b.idle_wait(), cap);
        }
        // Any work snaps back to base for a full new grace window.
        b.reset();
        assert_eq!(b.idle_wait(), base);
        assert_eq!(b.idle_wait(), base);
    }
}
