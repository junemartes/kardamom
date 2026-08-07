//! TxReceipts: receipts + boundaries (RAM only). The executor publishes both
//! streams; ingress / the state writer subscribe, either on the legacy shared
//! IPC channel or via MDS fan-in over per-replica unicast endpoints.

use tokio::sync::mpsc::{UnboundedReceiver, unbounded_channel};
use tracing::{error, info, warn};

use super::super::{AeronRuntime, DeliverFn, PubHandle};
use crate::codec;
use crate::config::ChannelsConfig;
use crate::error::LogError;
use kardamom_types::{BPosition, BlockBoundary, Receipt};

/// Attach replicas `0..executor_count` to an MDS fan-in subscription: the
/// shared loop behind the receipts/boundary `open_auto` constructors.
///
/// STATIC MEMBERSHIP (Consul-watch fallback): this runs once at startup over
/// the fixed `0..executor_count` index space. The executor job is a
/// count-based Nomad job with `distinct_hosts`, so replica indices are stable
/// and a restarting replica keeps its index/endpoint — the static attach
/// therefore stays correct across restarts. The full design watches the
/// `executor-receipts` Consul service and add/removes destinations on
/// membership change; see TODO(consul-watch) on
/// `ChannelsConfig::tx_receipts_executor_count`.
fn attach_mds_endpoints(
    kind: &str,
    executor_count: u32,
    endpoint_of: impl Fn(u32) -> Option<String>,
    attach: impl Fn(&str) -> Result<(), LogError>,
) -> Result<(), LogError> {
    if executor_count == 0 {
        warn!(
            kind,
            "tx_receipts MDS enabled but executor_count is 0 — this subscription will \
             receive nothing; set --executor-count / KARDAMOM_EXECUTOR_COUNT or \
             channels.tx_receipts_executor_count"
        );
    }
    for i in 0..executor_count {
        let endpoint = endpoint_of(i).ok_or_else(|| {
            LogError::Aeron(format!(
                "tx_receipts {kind} endpoint({i}) is None (MDS misconfigured)"
            ))
        })?;
        attach(&endpoint)?;
        info!(replica = i, kind, %endpoint, "attached executor endpoint to MDS");
    }
    Ok(())
}

/// Shared MDS destination plumbing for the receipts and boundary subscriber
/// handles: the retained `sub_id` (`Some` only when opened via `open_mds` —
/// the subscription id MDS destinations attach to; `None` for the legacy
/// single-channel subscription) plus the [`AeronRuntime`] clone the
/// add/remove commands go through. Both handles delegate here so the
/// "non-MDS subscription" guard cannot drift between them.
struct MdsSub {
    sub_id: Option<u32>,
    rt: AeronRuntime,
    /// Names the side-stream in error messages ("receipts" / "boundary").
    kind: &'static str,
}

impl MdsSub {
    fn new(sub_id: Option<u32>, rt: &AeronRuntime, kind: &'static str) -> Self {
        Self {
            sub_id,
            rt: rt.clone(),
            kind,
        }
    }

    fn add_destination(&self, uri: &str) -> Result<(), LogError> {
        match self.sub_id {
            Some(id) => self.rt.add_destination(id, uri),
            None => Err(LogError::Aeron(format!(
                "add_destination on a non-MDS {} subscription",
                self.kind
            ))),
        }
    }

    fn remove_destination(&self, uri: &str) -> Result<(), LogError> {
        match self.sub_id {
            Some(id) => self.rt.remove_destination(id, uri),
            None => Err(LogError::Aeron(format!(
                "remove_destination on a non-MDS {} subscription",
                self.kind
            ))),
        }
    }
}

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

    /// Publish a BATCH of receipts as one wire frame (`Vec<Receipt>`,
    /// rkyv-encoded). One encode + one offer + one ack round trip per batch:
    /// the previous receipt-per-frame path paid a blocking cross-thread ack
    /// round trip PER RECEIPT on the executor's commit thread — at thousands
    /// of receipts/s that serialization was the dominant per-tx publish
    /// cost. The subscriber fans batches back out into individual
    /// `(BPosition, Receipt)` deliveries, so consumers are unchanged; every
    /// receipt in a batch shares the frame's stream position (consumers key
    /// on `Receipt.tx_idx`, not the stream position). All receipt frames are
    /// batch frames — a single receipt rides a batch of one.
    pub fn publish_receipts(&self, batch: &Vec<Receipt>) -> Result<BPosition, LogError> {
        self.inner.publish(batch)
    }

    pub fn publish_receipt(&self, r: &Receipt) -> Result<BPosition, LogError> {
        self.publish_receipts(&vec![r.clone()])
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
    mds: MdsSub,
}

impl TxReceiptsSubscriberHandle {
    /// Fan a `Vec<Receipt>` batch frame (the only receipt wire format — see
    /// [`TxReceiptsPublisherHandle::publish_receipts`]) back out into
    /// per-receipt deliveries, preserving in-frame order. Consumers keep the
    /// exact `(BPosition, Receipt)` stream they always had.
    fn batch_fanout_deliver(
        msg_tx: tokio::sync::mpsc::UnboundedSender<(BPosition, Receipt)>,
    ) -> DeliverFn {
        Box::new(move |bytes: &[u8], pos: BPosition, _session: i32| {
            match codec::materialize::<Vec<Receipt>>(bytes) {
                Ok(batch) => {
                    for r in batch {
                        let _ = msg_tx.send((pos, r));
                    }
                }
                Err(e) => {
                    error!(error = %e, "decode failed on tx_receipts batch delivery");
                }
            }
        })
    }

    /// Legacy single-shared-channel subscriber (IPC default).
    pub fn open(rt: &AeronRuntime, ch: &ChannelsConfig) -> Result<Self, LogError> {
        let (msg_tx, rx) = unbounded_channel();
        rt.open_subscription_with_deliver(
            &ch.tx_receipts_channel,
            ch.tx_receipts_stream_id,
            Self::batch_fanout_deliver(msg_tx),
        )?;
        Ok(Self {
            rx,
            mds: MdsSub::new(None, rt, "receipts"),
        })
    }

    /// [`open_mds`](Self::open_mds) + attach replicas `0..executor_count`
    /// when the MDS control channel is configured, plain
    /// [`open`](Self::open) otherwise — the one shape every consumer binary
    /// (ingress, sequencer, validator) needs. A `None` endpoint under MDS is
    /// a misconfiguration and errors rather than silently subscribing to a
    /// subset of executors.
    pub fn open_auto(
        rt: &AeronRuntime,
        ch: &ChannelsConfig,
        executor_count: u32,
    ) -> Result<Self, LogError> {
        if !ch.tx_receipts_mds_enabled() {
            return Self::open(rt, ch);
        }
        let sub = Self::open_mds(rt, ch)?;
        attach_mds_endpoints(
            "receipt",
            executor_count,
            |i| ch.tx_receipts_endpoint(i),
            |uri| sub.add_destination(uri),
        )?;
        Ok(sub)
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
        let (msg_tx, rx) = unbounded_channel();
        let sub_id = rt.open_subscription_with_deliver(
            &ch.tx_receipts_control_channel,
            ch.tx_receipts_stream_id,
            Self::batch_fanout_deliver(msg_tx),
        )?;
        Ok(Self {
            rx,
            mds: MdsSub::new(Some(sub_id), rt, "receipts"),
        })
    }

    /// Attach an executor replica's endpoint as an MDS destination. Only valid
    /// on a handle opened via [`open_mds`](Self::open_mds). Idempotent.
    pub fn add_destination(&self, uri: &str) -> Result<(), LogError> {
        self.mds.add_destination(uri)
    }

    /// Detach a previously-attached executor endpoint (membership churn).
    pub fn remove_destination(&self, uri: &str) -> Result<(), LogError> {
        self.mds.remove_destination(uri)
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
    mds: MdsSub,
}

impl TxReceiptsBoundarySubscriberHandle {
    pub fn open(rt: &AeronRuntime, ch: &ChannelsConfig) -> Result<Self, LogError> {
        Ok(Self {
            rx: rt.open_subscription::<BlockBoundary>(
                &ch.tx_receipts_channel,
                ch.tx_receipts_stream_id + 1,
            )?,
            mds: MdsSub::new(None, rt, "boundary"),
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
            mds: MdsSub::new(Some(sub_id), rt, "boundary"),
        })
    }

    /// Boundary twin of [`TxReceiptsSubscriberHandle::open_auto`]: MDS +
    /// attach `0..executor_count` when configured, plain subscription
    /// otherwise. Boundaries ride distinct per-replica endpoints from
    /// receipts (each manual subscription binds its destination socket) —
    /// see [`ChannelsConfig::tx_receipts_boundary_endpoint`].
    pub fn open_auto(
        rt: &AeronRuntime,
        ch: &ChannelsConfig,
        executor_count: u32,
    ) -> Result<Self, LogError> {
        if !ch.tx_receipts_mds_enabled() {
            return Self::open(rt, ch);
        }
        let sub = Self::open_mds(rt, ch)?;
        attach_mds_endpoints(
            "boundary",
            executor_count,
            |i| ch.tx_receipts_boundary_endpoint(i),
            |uri| sub.add_destination(uri),
        )?;
        Ok(sub)
    }

    /// Attach an executor replica's endpoint as an MDS destination. Idempotent.
    pub fn add_destination(&self, uri: &str) -> Result<(), LogError> {
        self.mds.add_destination(uri)
    }

    pub fn remove_destination(&self, uri: &str) -> Result<(), LogError> {
        self.mds.remove_destination(uri)
    }

    pub async fn recv(&mut self) -> Option<(BPosition, BlockBoundary)> {
        self.rx.recv().await
    }

    pub fn try_recv(&mut self) -> Option<(BPosition, BlockBoundary)> {
        self.rx.try_recv().ok()
    }
}
