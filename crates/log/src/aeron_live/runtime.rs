//! [`AeronRuntime`]: the single Aeron thread's command bus and the
//! `Send + Sync` handles it hands out ([`PubHandle`], typed subscription
//! receivers). All Aeron work happens on the dedicated thread spawned here
//! (see `super::thread`); this module only ships commands to it.

use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use crossbeam_channel::Sender as CbSender;
use rkyv::util::AlignedVec;
use tokio::sync::mpsc::{UnboundedReceiver, unbounded_channel};
use tracing::error;

use super::thread::run_aeron_thread;
use super::{ACK_TIMEOUT, AeronClient, DeliverFn};
use crate::codec;
use crate::error::LogError;
use kardamom_types::{BPosition, TxDataLoc, TxEnvelope};

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

/// One command round trip to the Aeron thread: build the command around a
/// fresh ack channel, send it, and wait [`ACK_TIMEOUT`] for the reply. Every
/// control-plane call on [`AeronRuntime`] and [`PubHandle::publish_bytes`]
/// share this shape, so the two failure modes (the Aeron thread died / the
/// ack never came) have exactly one wording each; `op` names the caller in
/// the timeout error.
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

impl AeronRuntime {
    /// [`spawn_with_dir`](Self::spawn_with_dir) when a directory is given,
    /// [`spawn_default`](Self::spawn_default) otherwise — the shape every
    /// service binary's optional `--aeron-dir` flag needs.
    pub fn spawn(aeron_dir: Option<&std::path::Path>) -> Result<Self, LogError> {
        match aeron_dir {
            Some(dir) => Self::spawn_with_dir(dir),
            None => Self::spawn_default(),
        }
    }

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
        let uri = uri.to_string();
        request(
            &self.cmd_tx,
            |ack| RuntimeCmd::OpenSubscription {
                uri,
                stream_id,
                deliver,
                ack,
            },
            "open_subscription",
        )
    }

    /// Attach a source endpoint to a multi-destination subscription (one opened
    /// `control-mode=manual`). Blocks until the driver confirms the attach.
    /// Idempotent — re-adding an already-attached `uri` is a no-op.
    pub fn add_destination(&self, sub_id: u32, uri: &str) -> Result<(), LogError> {
        let uri = uri.to_string();
        request(
            &self.cmd_tx,
            |ack| RuntimeCmd::SubAddDestination { sub_id, uri, ack },
            "add_destination",
        )
    }

    /// Detach a previously-attached source endpoint from an MDS subscription.
    pub fn remove_destination(&self, sub_id: u32, uri: &str) -> Result<(), LogError> {
        let uri = uri.to_string();
        request(
            &self.cmd_tx,
            |ack| RuntimeCmd::SubRemoveDestination { sub_id, uri, ack },
            "remove_destination",
        )
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
    /// Aeron thread's reply — see that constant for why the ack always
    /// resolves first.
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
