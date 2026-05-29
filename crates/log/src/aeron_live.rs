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
    BPosition, BlockBoundary, BlockBoundaryStart, Deposit, FsyncWatermark, QuorumWatermark,
    Receipt, TxEnvelope, TxError, TxOrderingMessage,
};

type AeronClient = rusteron_client::Aeron;
type Pub = rusteron_client::AeronPublication;
type Sub = rusteron_client::AeronSubscription;
type Header = rusteron_client::AeronHeader;
type Archive = rusteron_archive::AeronArchive;

/// Closure that decodes one Aeron fragment + position and forwards the
/// decoded value (or its raw bytes) somewhere Send-friendly. Boxed so
/// different message types can share the subscription registration path.
pub type DeliverFn = Box<dyn FnMut(&[u8], BPosition) + Send>;

const ADD_PUB_TIMEOUT: Duration = Duration::from_secs(5);
const ADD_SUB_TIMEOUT: Duration = Duration::from_secs(5);

/// Offset added to the source `stream_id` to produce the replay stream id.
/// Callers open a normal subscription on `(replay_channel, stream_id + REPLAY_STREAM_OFFSET)`
/// to consume replayed frames.
pub const REPLAY_STREAM_OFFSET: i32 = 10_000;

/// Metadata returned by [`AeronRuntime::archive_start_replay`]. The caller
/// opens a normal subscription on `(replay_channel, replay_stream_id)` to
/// consume replayed frames.
#[derive(Clone, Debug)]
pub struct ArchiveReplayHandle {
    /// The session id assigned by the archive for this replay.
    pub session_id: i64,
    /// The Aeron channel URI to subscribe on for replayed frames.
    pub replay_channel: String,
    /// The stream id to use for the replay subscription.
    pub replay_stream_id: i32,
}

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
    OpenSubscription {
        uri: String,
        stream_id: i32,
        deliver: DeliverFn,
        ack: CbSender<Result<(), LogError>>,
    },
    /// Query the latest (highest) archived position for `(channel, stream_id)`.
    /// Returns `Ok(None)` if no recording exists yet.
    ArchiveLatestPosition {
        channel: String,
        stream_id: i32,
        reply: CbSender<Result<Option<i64>, LogError>>,
    },
    /// Query the earliest (start) archived position for `(channel, stream_id)`.
    ArchiveEarliestPosition {
        channel: String,
        stream_id: i32,
        reply: CbSender<Result<i64, LogError>>,
    },
    /// Start a replay anchored at `start_position` for `(channel, stream_id)`.
    /// Returns an [`ArchiveReplayHandle`] the caller uses to open a normal
    /// subscription on the replay channel.
    ArchiveStartReplay {
        channel: String,
        stream_id: i32,
        start_position: i64,
        reply: CbSender<Result<ArchiveReplayHandle, LogError>>,
    },
    /// Stop the loop, drop everything.
    Shutdown,
}

/// One row in the Aeron thread's subscription table.
struct SubEntry {
    sub: Sub,
    deliver: DeliverFn,
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
    ///
    /// After the Aeron client is started, a parallel `AeronArchive` connection
    /// is attempted using the default `AeronArchiveContext` (control channel
    /// `aeron:udp?endpoint=localhost:8010`). If the archive is not running, the
    /// runtime still starts successfully but archive commands will return errors.
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
                // Attempt to open a parallel AeronArchive client on this same
                // thread. Archive is `!Send + !Sync`, so it must stay here.
                let archive_client: Option<Archive> = match build_archive() {
                    Ok(a) => Some(a),
                    Err(e) => {
                        tracing::warn!(
                            error = %e,
                            "archive client connect failed; archive commands will return errors"
                        );
                        None
                    }
                };
                let _ = started_tx.send(Ok(()));
                if let Err(e) = run_aeron_thread(aeron, archive_client, cmd_rx) {
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
    pub fn open_subscription_with_deliver(
        &self,
        uri: &str,
        stream_id: i32,
        deliver: DeliverFn,
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

    /// Return the latest archived position for `(channel, stream_id)`.
    ///
    /// Delegates to the archive client on the Aeron thread; returns
    /// `Ok(None)` if no recording for that channel+stream exists yet.
    pub(crate) fn archive_latest_position(
        &self,
        channel: &str,
        stream_id: i32,
    ) -> Result<Option<i64>, LogError> {
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        self.cmd_tx
            .send(RuntimeCmd::ArchiveLatestPosition {
                channel: channel.to_string(),
                stream_id,
                reply: reply_tx,
            })
            .map_err(|_| LogError::Aeron("aeron thread dropped".into()))?;
        reply_rx
            .recv_timeout(Duration::from_secs(10))
            .map_err(|_| LogError::Aeron("archive_latest_position timed out".into()))?
    }

    /// Return the earliest archived position for `(channel, stream_id)`.
    ///
    /// Returns `Err` if no recording for that channel+stream exists.
    pub(crate) fn archive_earliest_position(
        &self,
        channel: &str,
        stream_id: i32,
    ) -> Result<i64, LogError> {
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        self.cmd_tx
            .send(RuntimeCmd::ArchiveEarliestPosition {
                channel: channel.to_string(),
                stream_id,
                reply: reply_tx,
            })
            .map_err(|_| LogError::Aeron("aeron thread dropped".into()))?;
        reply_rx
            .recv_timeout(Duration::from_secs(10))
            .map_err(|_| LogError::Aeron("archive_earliest_position timed out".into()))?
    }

    /// Start a replay from `start_position` for `(channel, stream_id)`.
    ///
    /// Returns an [`ArchiveReplayHandle`] containing the session id and the
    /// replay channel/stream the caller should subscribe on.
    pub(crate) fn archive_start_replay(
        &self,
        channel: &str,
        stream_id: i32,
        start_position: i64,
    ) -> Result<ArchiveReplayHandle, LogError> {
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        self.cmd_tx
            .send(RuntimeCmd::ArchiveStartReplay {
                channel: channel.to_string(),
                stream_id,
                start_position,
                reply: reply_tx,
            })
            .map_err(|_| LogError::Aeron("aeron thread dropped".into()))?;
        reply_rx
            .recv_timeout(Duration::from_secs(10))
            .map_err(|_| LogError::Aeron("archive_start_replay timed out".into()))?
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
        let (msg_tx, msg_rx) = unbounded_channel::<(BPosition, T)>();
        let deliver: DeliverFn = Box::new(move |bytes: &[u8], pos: BPosition| {
            match codec::materialize::<T>(bytes) {
                Ok(v) => {
                    if msg_tx.send((pos, v)).is_err() {
                        // Subscriber dropped its receiver.
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

/// Attempt to connect to the Aeron Archive using the default context.
///
/// The default `AeronArchiveContext` uses `aeron:udp?endpoint=localhost:8010`
/// for control requests and `aeron:udp?endpoint=localhost:8011` for responses.
/// Callers that need custom archive endpoints should extend this via a new
/// `spawn_with_archive` variant; for now the defaults cover the production and
/// Docker-e2e cases.
fn build_archive() -> Result<Archive, LogError> {
    let ctx = rusteron_archive::AeronArchiveContext::new()
        .map_err(|e| LogError::Aeron(format!("AeronArchiveContext::new: {e}")))?;
    // Use a 3-second timeout rather than the full 10 seconds: the outer
    // `started_rx.recv_timeout` in `spawn_with` is also 10 s, and archive
    // failure is non-fatal.  A reachable daemon responds well within 3 s,
    // while an unreachable one will return quickly so the remaining 7 s of
    // budget are available for the runtime to signal started.
    ctx.aeron_archive_connect(Duration::from_secs(3))
        .map_err(|e| LogError::Aeron(format!("aeron_archive_connect: {e}")))
}

fn run_aeron_thread(
    aeron: Rc<AeronClient>,
    archive_client: Option<Archive>,
    cmd_rx: CbReceiver<RuntimeCmd>,
) -> Result<(), LogError> {
    let mut pubs: Vec<Pub> = Vec::new();
    let mut subs: Vec<SubEntry> = Vec::new();
    // Lazy recording-id cache: maps `(channel, stream_id)` → `recording_id`.
    // Populated on first lookup and reused for subsequent calls.
    let mut recording_id_cache: std::collections::HashMap<(String, i32), i64> =
        std::collections::HashMap::new();

    loop {
        loop {
            match cmd_rx.try_recv() {
                Ok(RuntimeCmd::Shutdown) => return Ok(()),
                Ok(cmd) => handle_cmd(
                    &aeron,
                    archive_client.as_ref(),
                    &mut recording_id_cache,
                    &mut pubs,
                    &mut subs,
                    cmd,
                )?,
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => return Ok(()),
            }
        }

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

        if subs.is_empty() {
            match cmd_rx.recv_timeout(Duration::from_millis(1)) {
                Ok(RuntimeCmd::Shutdown) => return Ok(()),
                Ok(cmd) => handle_cmd(
                    &aeron,
                    archive_client.as_ref(),
                    &mut recording_id_cache,
                    &mut pubs,
                    &mut subs,
                    cmd,
                )?,
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
    archive_client: Option<&Archive>,
    recording_id_cache: &mut std::collections::HashMap<(String, i32), i64>,
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
        RuntimeCmd::ArchiveLatestPosition {
            channel,
            stream_id,
            reply,
        } => {
            let res = match archive_client {
                None => Err(LogError::Aeron("archive client not available".into())),
                Some(archive) => {
                    lookup_recording_id(archive, recording_id_cache, &channel, stream_id)
                        .and_then(|recording_id| {
                            archive
                                .get_recording_position(recording_id)
                                .map(|pos| {
                                    // -1 means "no active recording position recorded yet"
                                    if pos < 0 { None } else { Some(pos) }
                                })
                                .map_err(|e| {
                                    LogError::Aeron(format!("get_recording_position: {e}"))
                                })
                        })
                        .or_else(|e| {
                            // If recording not found, return None instead of error
                            if e.to_string().contains("no recording") {
                                Ok(None)
                            } else {
                                Err(e)
                            }
                        })
                }
            };
            let _ = reply.send(res);
        }
        RuntimeCmd::ArchiveEarliestPosition {
            channel,
            stream_id,
            reply,
        } => {
            let res = match archive_client {
                None => Err(LogError::Aeron("archive client not available".into())),
                Some(archive) => {
                    lookup_recording_id(archive, recording_id_cache, &channel, stream_id)
                        .and_then(|recording_id| {
                            fetch_start_position(archive, recording_id)
                        })
                }
            };
            let _ = reply.send(res);
        }
        RuntimeCmd::ArchiveStartReplay {
            channel,
            stream_id,
            start_position,
            reply,
        } => {
            let res = match archive_client {
                None => Err(LogError::Aeron("archive client not available".into())),
                Some(archive) => {
                    lookup_recording_id(archive, recording_id_cache, &channel, stream_id)
                        .and_then(|recording_id| {
                            let replay_channel =
                                format!("aeron:ipc?alias=replay-{recording_id}");
                            let replay_stream_id = stream_id + REPLAY_STREAM_OFFSET;
                            let replay_channel_c =
                                CString::new(replay_channel.as_str()).map_err(|e| {
                                    LogError::Aeron(format!("replay channel NUL: {e}"))
                                })?;
                            let params = rusteron_archive::AeronArchiveReplayParams::new(
                                -1,        // bounding_limit_counter_id: no bound
                                0,         // file_io_max_length: archive default
                                start_position,
                                i64::MAX,  // length: open-ended replay
                                0,         // replay_token: no token
                                -1,        // subscription_registration_id: no binding
                            )
                            .map_err(|e| {
                                LogError::Aeron(format!("AeronArchiveReplayParams::new: {e}"))
                            })?;
                            let session_id = archive
                                .start_replay(
                                    recording_id,
                                    replay_channel_c.as_c_str(),
                                    replay_stream_id,
                                    &params,
                                )
                                .map_err(|e| {
                                    LogError::Aeron(format!("start_replay: {e}"))
                                })?;
                            Ok(ArchiveReplayHandle {
                                session_id,
                                replay_channel,
                                replay_stream_id,
                            })
                        })
                }
            };
            let _ = reply.send(res);
        }
        RuntimeCmd::Shutdown => {}
    }
    Ok(())
}

/// Look up the `recording_id` for `(channel, stream_id)`, using the cache if
/// available or scanning via `list_recordings_for_uri` otherwise.
///
/// The channel is matched as a URI fragment (substring match), which is how
/// the Aeron Archive C API works: pass the alias or channel URI and it does a
/// "contains" match against the original channel stored at recording time.
fn lookup_recording_id(
    archive: &Archive,
    cache: &mut std::collections::HashMap<(String, i32), i64>,
    channel: &str,
    stream_id: i32,
) -> Result<i64, LogError> {
    use std::cell::RefCell;
    use std::rc::Rc;
    use rusteron_archive::{
        AeronArchiveRecordingDescriptor,
        AeronArchiveRecordingDescriptorConsumerFuncCallback, Handler,
    };

    let key = (channel.to_string(), stream_id);
    if let Some(&id) = cache.get(&key) {
        return Ok(id);
    }

    #[derive(Default)]
    struct Found {
        recording_id: i64,
        seen: bool,
    }

    struct Consumer {
        found: Rc<RefCell<Found>>,
    }

    impl AeronArchiveRecordingDescriptorConsumerFuncCallback for Consumer {
        fn handle_aeron_archive_recording_descriptor_consumer_func(
            &mut self,
            desc: AeronArchiveRecordingDescriptor,
        ) {
            let mut g = self.found.borrow_mut();
            // Take the last (most recent) recording that matches.
            g.recording_id = desc.recording_id();
            g.seen = true;
        }
    }

    let channel_c = CString::new(channel)
        .map_err(|e| LogError::Aeron(format!("channel contains NUL: {e}")))?;

    let found: Rc<RefCell<Found>> = Rc::new(RefCell::new(Found::default()));
    let handler = Handler::leak(Consumer {
        found: found.clone(),
    });

    archive
        .list_recordings_for_uri(
            0,             // from_recording_id: scan from the beginning
            i32::MAX,      // record_count: return all matches
            channel_c.as_c_str(),
            stream_id,
            Some(&handler),
        )
        .map_err(|e| LogError::Aeron(format!("list_recordings_for_uri: {e}")))?;

    let g = found.borrow();
    if !g.seen {
        return Err(LogError::Aeron(format!(
            "no recording for channel={channel} stream_id={stream_id}"
        )));
    }
    let recording_id = g.recording_id;
    drop(g);
    cache.insert(key, recording_id);
    Ok(recording_id)
}

/// Fetch the `start_position` for a recording (the earliest position).
fn fetch_start_position(archive: &Archive, recording_id: i64) -> Result<i64, LogError> {
    use std::cell::RefCell;
    use std::rc::Rc;
    use rusteron_archive::{
        AeronArchiveRecordingDescriptor,
        AeronArchiveRecordingDescriptorConsumerFuncCallback, Handler,
    };

    #[derive(Default)]
    struct Captured {
        start_position: i64,
        seen: bool,
    }

    struct Consumer {
        captured: Rc<RefCell<Captured>>,
    }

    impl AeronArchiveRecordingDescriptorConsumerFuncCallback for Consumer {
        fn handle_aeron_archive_recording_descriptor_consumer_func(
            &mut self,
            desc: AeronArchiveRecordingDescriptor,
        ) {
            let mut g = self.captured.borrow_mut();
            g.start_position = desc.start_position();
            g.seen = true;
        }
    }

    let captured: Rc<RefCell<Captured>> = Rc::new(RefCell::new(Captured::default()));
    let handler = Handler::leak(Consumer {
        captured: captured.clone(),
    });
    archive
        .list_recording(recording_id, Some(&handler))
        .map_err(|e| LogError::Aeron(format!("list_recording: {e}")))?;
    let g = captured.borrow();
    if !g.seen {
        return Err(LogError::Aeron(format!(
            "list_recording({recording_id}) returned no descriptor"
        )));
    }
    Ok(g.start_position)
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

/// Per-shard TxData subscriber. Yields `(BPosition, TxEnvelope)`.
pub struct TxDataSubscriberHandle {
    rx: UnboundedReceiver<(BPosition, TxEnvelope)>,
}

impl TxDataSubscriberHandle {
    pub fn open(
        rt: &AeronRuntime,
        ch: &ChannelsConfig,
        sequencer_id: u8,
    ) -> Result<Self, LogError> {
        Ok(Self {
            rx: rt.open_subscription::<TxEnvelope>(
                &ch.tx_data_channel(sequencer_id),
                ch.tx_data_stream_id(sequencer_id),
            )?,
        })
    }

    pub async fn recv(&mut self) -> Option<(BPosition, TxEnvelope)> {
        self.rx.recv().await
    }

    pub fn try_recv(&mut self) -> Option<(BPosition, TxEnvelope)> {
        self.rx.try_recv().ok()
    }
}

// ---------------------------------------------------------------------------
// TxOrdering: canonical orderer of TxOrderingMessage (TxRef | BoundaryStart).
// ---------------------------------------------------------------------------

/// TxOrdering publisher. Both sequencers (publishing `TxRef`) and the sealer
/// (publishing `BoundaryStart`) write here; they share the same Aeron stream.
#[derive(Clone)]
pub struct TxOrderingPublisherHandle {
    inner: PubHandle,
}

impl TxOrderingPublisherHandle {
    pub fn open(rt: &AeronRuntime, ch: &ChannelsConfig) -> Result<Self, LogError> {
        Ok(Self {
            inner: rt.open_publication(&ch.tx_ordering_channel, ch.tx_ordering_stream_id)?,
        })
    }

    pub fn publish(&self, msg: &TxOrderingMessage) -> Result<BPosition, LogError> {
        self.inner.publish(msg)
    }

    /// Publish a boundary marker (convenience over `publish` with the variant
    /// constructor).
    pub fn publish_boundary(&self, b: &BlockBoundaryStart) -> Result<BPosition, LogError> {
        self.inner
            .publish(&TxOrderingMessage::BoundaryStart(b.clone()))
    }

    pub fn raw(&self) -> &PubHandle {
        &self.inner
    }
}

/// TxOrdering subscriber. Yields `(BPosition, TxOrderingMessage)`.
pub struct TxOrderingSubscriberHandle {
    rx: UnboundedReceiver<(BPosition, TxOrderingMessage)>,
}

impl TxOrderingSubscriberHandle {
    pub fn open(rt: &AeronRuntime, ch: &ChannelsConfig) -> Result<Self, LogError> {
        Ok(Self {
            rx: rt.open_subscription::<TxOrderingMessage>(
                &ch.tx_ordering_channel,
                ch.tx_ordering_stream_id,
            )?,
        })
    }

    pub async fn recv(&mut self) -> Option<(BPosition, TxOrderingMessage)> {
        self.rx.recv().await
    }

    pub fn try_recv(&mut self) -> Option<(BPosition, TxOrderingMessage)> {
        self.rx.try_recv().ok()
    }
}

// ---------------------------------------------------------------------------
// TxReceipts: receipts + boundaries (RAM only).
// ---------------------------------------------------------------------------

/// TxReceipts publisher. The executor uses `publish_receipt` and
/// `publish_boundary` on the same channel, but with separate stream ids so
/// subscribers can demultiplex without an in-band tag.
#[derive(Clone)]
pub struct TxReceiptsPublisherHandle {
    inner: PubHandle,
    boundary: PubHandle,
}

impl TxReceiptsPublisherHandle {
    pub fn open(rt: &AeronRuntime, ch: &ChannelsConfig) -> Result<Self, LogError> {
        Ok(Self {
            inner: rt.open_publication(&ch.tx_receipts_channel, ch.tx_receipts_stream_id)?,
            boundary: rt.open_publication(&ch.tx_receipts_channel, ch.tx_receipts_stream_id + 1)?,
        })
    }

    pub fn publish_receipt(&self, r: &Receipt) -> Result<BPosition, LogError> {
        self.inner.publish(r)
    }

    pub fn publish_boundary(&self, b: &BlockBoundary) -> Result<BPosition, LogError> {
        self.boundary.publish(b)
    }
}

/// TxReceipts subscriber for receipts.
pub struct TxReceiptsSubscriberHandle {
    rx: UnboundedReceiver<(BPosition, Receipt)>,
}

impl TxReceiptsSubscriberHandle {
    pub fn open(rt: &AeronRuntime, ch: &ChannelsConfig) -> Result<Self, LogError> {
        Ok(Self {
            rx: rt
                .open_subscription::<Receipt>(&ch.tx_receipts_channel, ch.tx_receipts_stream_id)?,
        })
    }

    pub async fn recv(&mut self) -> Option<(BPosition, Receipt)> {
        self.rx.recv().await
    }

    pub fn try_recv(&mut self) -> Option<(BPosition, Receipt)> {
        self.rx.try_recv().ok()
    }
}

/// TxReceipts subscriber for boundaries.
pub struct TxReceiptsBoundarySubscriberHandle {
    rx: UnboundedReceiver<(BPosition, BlockBoundary)>,
}

impl TxReceiptsBoundarySubscriberHandle {
    pub fn open(rt: &AeronRuntime, ch: &ChannelsConfig) -> Result<Self, LogError> {
        Ok(Self {
            rx: rt.open_subscription::<BlockBoundary>(
                &ch.tx_receipts_channel,
                ch.tx_receipts_stream_id + 1,
            )?,
        })
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
    assert_send_sync::<TxOrderingPublisherHandle>();
    assert_send::<TxOrderingSubscriberHandle>();
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
