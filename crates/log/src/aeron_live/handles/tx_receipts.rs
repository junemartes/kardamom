//! `TxReceipts`: receipts and boundaries (RAM only). The executor publishes
//! both streams. Ingress and the state writer subscribe, either in
//! single-channel IPC mode (one shared channel) or through MDS fan-in
//! over per-replica unicast endpoints.

use std::collections::VecDeque;
use std::num::NonZeroU32;

use tokio::sync::mpsc::UnboundedReceiver;
use tracing::{error, info, warn};

use super::super::{AeronRuntime, PubHandle, RawFrame, TypedSubscription};
use crate::codec;
use crate::config::ChannelsConfig;
use crate::error::LogError;
use kardamom_types::{BPosition, BlockBoundary, Receipt};

/// Attach replicas `0..executor_count` to an MDS fan-in subscription. This
/// is the shared loop behind the receipts/boundary `open_auto`
/// constructors.
///
/// Static membership (Consul-watch fallback): this runs once at startup
/// over the fixed `0..executor_count` index space. The executor job is a
/// count-based Nomad job with `distinct_hosts`, so replica indices stay
/// stable, and a restarting replica keeps its index and endpoint. The
/// static attach therefore stays correct across restarts. The full design
/// watches the `executor-receipts` Consul service and adds or removes
/// destinations on membership change; see
/// `ChannelsConfig::tx_receipts_executor_count` for the static-count
/// limit this fallback has today.
fn attach_mds_endpoints(
    kind: &str,
    executor_count: Option<NonZeroU32>,
    endpoint_of: impl Fn(u32) -> Option<String>,
    attach: impl Fn(&str) -> Result<(), LogError>,
) -> Result<(), LogError> {
    let Some(executor_count) = executor_count else {
        warn!(
            kind,
            "tx_receipts MDS enabled but executor_count is 0 — this subscription will \
             receive nothing; set --executor-count / KARDAMOM_EXECUTOR_COUNT or \
             channels.tx_receipts_executor_count"
        );
        return Ok(());
    };
    for i in 0..executor_count.get() {
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

/// Guard every `open_mds` with one wording: MDS is off unless
/// `tx_receipts_control_channel` names a channel, so a caller that dials
/// `open_mds` directly on an IPC-mode config fails fast with a clear
/// reason, instead of opening a subscription on an empty channel string.
///
/// # Errors
///
/// Returns an error if `ch.tx_receipts_mds_enabled()` is false.
fn require_mds(ch: &ChannelsConfig) -> Result<(), LogError> {
    if ch.tx_receipts_mds_enabled() {
        Ok(())
    } else {
        Err(LogError::Aeron(
            "open_mds: tx_receipts MDS not configured (empty control channel)".into(),
        ))
    }
}

/// Shared MDS destination plumbing for the receipts and boundary
/// subscriber handles: the retained `sub_id` (`Some` only when opened
/// through `open_mds`, the subscription id MDS destinations attach to;
/// `None` for the single-channel IPC subscription) plus the
/// [`AeronRuntime`] clone the add/remove commands go through. Both handles
/// delegate here, so the "non-MDS subscription" guard cannot drift between
/// them.
struct MdsSub {
    sub_id: Option<u32>,
    rt: AeronRuntime,
    /// Names the side-stream in error messages ("receipts" or "boundary").
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

/// Shared shape behind every `open_auto`: open the single-channel IPC
/// subscription when MDS is off, or open the MDS subscription and attach
/// `0..executor_count` replica endpoints when it is on. Both the receipts
/// and boundary subscriber handles are the same shape here, differing only
/// in their open bodies and which per-replica endpoint they attach.
trait MdsSubscriber: Sized {
    /// Names this side-stream in `attach_mds_endpoints`'s log lines
    /// ("receipt" or "boundary").
    const KIND: &'static str;

    fn open(rt: &AeronRuntime, ch: &ChannelsConfig) -> Result<Self, LogError>;
    fn open_mds(rt: &AeronRuntime, ch: &ChannelsConfig) -> Result<Self, LogError>;
    fn endpoint_of(ch: &ChannelsConfig, replica_idx: u32) -> Option<String>;
    fn mds(&self) -> &MdsSub;

    fn open_auto(
        rt: &AeronRuntime,
        ch: &ChannelsConfig,
        executor_count: Option<NonZeroU32>,
    ) -> Result<Self, LogError> {
        if !ch.tx_receipts_mds_enabled() {
            return Self::open(rt, ch);
        }
        let sub = Self::open_mds(rt, ch)?;
        attach_mds_endpoints(
            Self::KIND,
            executor_count,
            |i| Self::endpoint_of(ch, i),
            |uri| sub.mds().add_destination(uri),
        )?;
        Ok(sub)
    }
}

/// `TxReceipts` publisher. The executor uses `publish_receipt` and
/// `publish_boundary` on the same channel, but with separate stream ids,
/// so subscribers can demultiplex without an in-band tag.
///
/// Two open modes:
/// - [`open`](Self::open): the single-channel IPC path
///   (`tx_receipts_channel`). The lone executor publishes, and ingress
///   subscribes directly. This is the single-host IPC default.
/// - [`open_mds`](Self::open_mds): the multi-destination-subscription
///   (fan-in) path. Each executor replica publishes to its own unicast UDP
///   endpoint (`ch.tx_receipts_endpoint(replica_idx)`), and the single
///   ingress attaches every replica's endpoint to one
///   `control-mode=manual` subscription. Both modes publish receipts on
///   `tx_receipts_stream_id` and boundaries on `tx_receipts_stream_id + 1`.
///   Only the channel URI differs, so `publish_receipt`/`publish_boundary`
///   (and the executor commit thread's must-deliver retry that drives
///   them) are identical across modes.
#[derive(Clone)]
pub struct TxReceiptsPublisherHandle {
    inner: PubHandle,
    boundary: PubHandle,
}

impl TxReceiptsPublisherHandle {
    /// The single-shared-channel publisher (IPC default). Use when
    /// `ch.tx_receipts_mds_enabled()` is false.
    ///
    /// # Errors
    ///
    /// Returns an error if the Aeron thread fails to add either
    /// publication.
    pub fn open(rt: &AeronRuntime, ch: &ChannelsConfig) -> Result<Self, LogError> {
        Ok(Self {
            inner: rt.open_publication(&ch.tx_receipts_channel, ch.tx_receipts_stream_id)?,
            boundary: rt
                .open_publication(&ch.tx_receipts_channel, ch.tx_receipts_boundary_stream_id())?,
        })
    }

    /// MDS (fan-in) publisher: this replica publishes both streams to its
    /// own per-replica unicast endpoint
    /// `ch.tx_receipts_endpoint(replica_idx)`, which ingress attaches as a
    /// destination on its aggregating subscription. `replica_idx` is the
    /// executor's recorder-id (`NOMAD_ALLOC_INDEX`).
    ///
    /// # Errors
    ///
    /// Returns an error if MDS is not configured (no
    /// `tx_receipts_control_channel`), so a misconfigured executor fails
    /// fast instead of silently using IPC, or if the Aeron thread fails
    /// to add either publication.
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
        // Boundaries publish to a distinct endpoint (port) from receipts,
        // because ingress's two manual subscriptions each bind their
        // destination socket, and a shared endpoint would collide. See
        // ChannelsConfig::tx_receipts_endpoint.
        let boundary_endpoint = ch
            .tx_receipts_boundary_endpoint(replica_idx)
            .ok_or_else(|| {
                LogError::Aeron(format!(
                    "open_mds: tx_receipts boundary endpoint not configured (replica {replica_idx})"
                ))
            })?;
        Ok(Self {
            inner: rt.open_publication(&endpoint, ch.tx_receipts_stream_id)?,
            boundary: rt
                .open_publication(&boundary_endpoint, ch.tx_receipts_boundary_stream_id())?,
        })
    }

    /// Publish a batch of receipts as one wire frame (`Vec<Receipt>`,
    /// rkyv-encoded). This is one encode, one offer, and one ack round
    /// trip per batch, not per receipt, which keeps the executor's commit
    /// thread off a blocking cross-thread ack round trip on every receipt
    /// at thousands of receipts per second. The subscriber fans a batch
    /// back out into individual `(BPosition, Receipt)` deliveries. Every
    /// receipt in a batch shares the frame's stream position (consumers
    /// key on `Receipt.tx_idx`, not the stream position). All receipt
    /// frames are batch frames; a single receipt
    /// rides a batch of one.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying Aeron offer fails or times
    /// out.
    pub fn publish_receipts(&self, batch: &Vec<Receipt>) -> Result<BPosition, LogError> {
        self.inner.publish(batch)
    }

    /// # Errors
    ///
    /// Returns an error if the underlying Aeron offer fails or times
    /// out (see [`publish_receipts`](Self::publish_receipts)).
    pub fn publish_receipt(&self, r: &Receipt) -> Result<BPosition, LogError> {
        self.publish_receipts(&vec![r.clone()])
    }

    /// # Errors
    ///
    /// Returns an error if the underlying Aeron offer fails or times
    /// out.
    pub fn publish_boundary(&self, b: &BlockBoundary) -> Result<BPosition, LogError> {
        self.boundary.publish(b)
    }

    /// Fire-and-forget boundary publish. The block-boundary side-stream is
    /// a marker; ingress acks on the receipt or durable watermark, not on
    /// this, so it must never block the executor's commit thread. A
    /// must-deliver boundary that cannot reach a not-yet-connected ingress
    /// (for example during startup, before ingress's MDS destinations
    /// attach) would back up the commit-to-exec channel and freeze all
    /// state progress. This encodes and hands the frame to the Aeron
    /// thread; delivery is best effort.
    ///
    /// # Errors
    ///
    /// Returns an error if `b` fails to encode. The publish itself is
    /// best effort and never errors.
    pub fn publish_boundary_best_effort(&self, b: &BlockBoundary) -> Result<(), LogError> {
        let bytes = codec::encode(b)?;
        self.boundary.publish_best_effort(bytes);
        Ok(())
    }
}

/// `TxReceipts` subscriber for receipts.
///
/// In the single-channel IPC path ([`open`](Self::open)) this is a plain
/// subscription on the shared `tx_receipts_channel`. In the MDS fan-in
/// path ([`open_mds`](Self::open_mds)) it is opened on the
/// `control-mode=manual` `tx_receipts_control_channel`. The caller then
/// attaches each executor replica's endpoint with
/// [`add_destination`](Self::add_destination). The retained `sub_id` is
/// what `add_destination`/`remove_destination` target.
pub struct TxReceiptsSubscriberHandle {
    /// The receive path: same fields and `recv`/`try_recv` bodies as the
    /// standalone [`TxReceiptsReceiver`]. Held by composition so the fan-out
    /// loop lives in one place; see [`TxReceiptsReceiver::recv`].
    receiver: TxReceiptsReceiver,
    mds: MdsSub,
}

impl TxReceiptsSubscriberHandle {
    pub async fn recv(&mut self) -> Option<(BPosition, Receipt)> {
        self.receiver.recv().await
    }

    pub fn try_recv(&mut self) -> Option<(BPosition, Receipt)> {
        self.receiver.try_recv()
    }

    /// The single-shared-channel subscriber (IPC default).
    ///
    /// # Errors
    ///
    /// Returns an error if the Aeron thread fails to add the
    /// subscription.
    pub fn open(rt: &AeronRuntime, ch: &ChannelsConfig) -> Result<Self, LogError> {
        <Self as MdsSubscriber>::open(rt, ch)
    }

    /// Calls [`open_mds`](Self::open_mds) and attaches replicas
    /// `0..executor_count` when the MDS control channel is configured, or
    /// plain [`open`](Self::open) otherwise. This is the one shape every
    /// consumer binary (ingress, sequencer, validator) needs. A `None`
    /// endpoint under MDS is a misconfiguration and errors, instead of
    /// silently subscribing to a subset of executors.
    ///
    /// # Errors
    ///
    /// Returns an error if opening the subscription fails, or if any
    /// per-replica endpoint is missing or fails to attach under MDS.
    pub fn open_auto(
        rt: &AeronRuntime,
        ch: &ChannelsConfig,
        executor_count: Option<NonZeroU32>,
    ) -> Result<Self, LogError> {
        <Self as MdsSubscriber>::open_auto(rt, ch, executor_count)
    }

    /// MDS (fan-in) subscriber: one `control-mode=manual` subscription on
    /// `ch.tx_receipts_control_channel` that the caller attaches
    /// per-replica executor endpoints to.
    ///
    /// # Errors
    ///
    /// Returns an error if MDS is not configured, or if the Aeron
    /// thread fails to add the subscription.
    pub fn open_mds(rt: &AeronRuntime, ch: &ChannelsConfig) -> Result<Self, LogError> {
        <Self as MdsSubscriber>::open_mds(rt, ch)
    }

    /// Attach an executor replica's endpoint as an MDS destination. Only
    /// valid on a handle opened with [`open_mds`](Self::open_mds).
    /// Idempotent.
    ///
    /// # Errors
    ///
    /// Returns an error if this handle was not opened with
    /// [`open_mds`](Self::open_mds), or if the driver rejects or times
    /// out the attach.
    pub fn add_destination(&self, uri: &str) -> Result<(), LogError> {
        self.mds.add_destination(uri)
    }

    /// Detach a previously-attached executor endpoint (membership churn).
    ///
    /// # Errors
    ///
    /// Returns an error if this handle was not opened with
    /// [`open_mds`](Self::open_mds), or if the driver rejects or times
    /// out the detach.
    pub fn remove_destination(&self, uri: &str) -> Result<(), LogError> {
        self.mds.remove_destination(uri)
    }

    /// Drop the handle's `AeronRuntime` clone, keeping only the receive
    /// path.
    ///
    /// Use this when the receiver moves into a long-lived pump task that
    /// ends on `recv() == None`. Keeping the whole handle there creates an
    /// ownership cycle that makes the process unkillable by SIGTERM. The
    /// runtime shuts down only when its last clone drops
    /// ([`AeronRuntime::drop`]). That shutdown is what closes
    /// subscriptions, and closing them is what makes `recv()` return
    /// `None`. So a pump task holding a clone waits for a shutdown that
    /// its own clone is preventing. `drop(rt)` in `main` then silently
    /// does nothing, every other subscription stays open, and any thread
    /// joining on end-of-stream hangs forever.
    ///
    /// Destinations can no longer be attached or detached afterwards, so
    /// call this only once MDS membership is established (destinations
    /// attached at open time survive; they live in the driver, not in
    /// this handle).
    #[must_use]
    pub fn into_receiver(self) -> TxReceiptsReceiver {
        self.receiver
    }
}

/// A [`TxReceiptsSubscriberHandle`] with its `AeronRuntime` clone dropped
/// and no MDS operations left, keeping only the receive path. See
/// [`TxReceiptsSubscriberHandle::into_receiver`].
pub struct TxReceiptsReceiver {
    rx: UnboundedReceiver<RawFrame>,
    pending: VecDeque<(BPosition, Receipt)>,
}

impl TxReceiptsReceiver {
    pub async fn recv(&mut self) -> Option<(BPosition, Receipt)> {
        while self.pending.is_empty() {
            self.pending = decode_receipt_batch(&self.rx.recv().await?);
        }
        self.pending.pop_front()
    }

    pub fn try_recv(&mut self) -> Option<(BPosition, Receipt)> {
        while self.pending.is_empty() {
            self.pending = decode_receipt_batch(&self.rx.try_recv().ok()?);
        }
        self.pending.pop_front()
    }
}

/// Decode one `tx_receipts` batch frame into its individual receipts, in
/// frame order. A malformed frame logs and yields no receipts, so the
/// caller's loop tries the next frame instead of ending the subscription.
fn decode_receipt_batch(frame: &RawFrame) -> VecDeque<(BPosition, Receipt)> {
    match codec::materialize::<Vec<Receipt>>(&frame.bytes) {
        Ok(batch) => batch.into_iter().map(|r| (frame.pos, r)).collect(),
        Err(e) => {
            error!(error = %e, "decode failed on tx_receipts batch delivery");
            VecDeque::new()
        }
    }
}

impl MdsSubscriber for TxReceiptsSubscriberHandle {
    const KIND: &'static str = "receipt";

    fn open(rt: &AeronRuntime, ch: &ChannelsConfig) -> Result<Self, LogError> {
        let (_sub_id, rx) =
            rt.open_subscription_raw(&ch.tx_receipts_channel, ch.tx_receipts_stream_id)?;
        Ok(Self {
            receiver: TxReceiptsReceiver {
                rx,
                pending: VecDeque::new(),
            },
            mds: MdsSub::new(None, rt, "receipts"),
        })
    }

    fn open_mds(rt: &AeronRuntime, ch: &ChannelsConfig) -> Result<Self, LogError> {
        require_mds(ch)?;
        let (sub_id, rx) =
            rt.open_subscription_raw(&ch.tx_receipts_control_channel, ch.tx_receipts_stream_id)?;
        Ok(Self {
            receiver: TxReceiptsReceiver {
                rx,
                pending: VecDeque::new(),
            },
            mds: MdsSub::new(Some(sub_id), rt, "receipts"),
        })
    }

    fn endpoint_of(ch: &ChannelsConfig, replica_idx: u32) -> Option<String> {
        ch.tx_receipts_endpoint(replica_idx)
    }

    fn mds(&self) -> &MdsSub {
        &self.mds
    }
}

/// `TxReceipts` subscriber for boundaries. Mirrors
/// [`TxReceiptsSubscriberHandle`], but for the `tx_receipts_stream_id + 1`
/// boundary side-stream: [`open`](Self::open) for the single shared
/// channel, and [`open_mds`](Self::open_mds) plus
/// [`add_destination`](Self::add_destination) for the fan-in path.
pub struct TxReceiptsBoundarySubscriberHandle {
    rx: TypedSubscription<BlockBoundary>,
    mds: MdsSub,
}

impl TxReceiptsBoundarySubscriberHandle {
    /// The single-shared-channel boundary subscriber (IPC default).
    ///
    /// # Errors
    ///
    /// Returns an error if the Aeron thread fails to add the
    /// subscription.
    pub fn open(rt: &AeronRuntime, ch: &ChannelsConfig) -> Result<Self, LogError> {
        <Self as MdsSubscriber>::open(rt, ch)
    }

    /// MDS (fan-in) boundary subscriber on `ch.tx_receipts_control_channel`,
    /// stream `tx_receipts_stream_id + 1`.
    ///
    /// # Errors
    ///
    /// Returns an error if MDS is not configured, or if the Aeron thread
    /// fails to add the subscription.
    pub fn open_mds(rt: &AeronRuntime, ch: &ChannelsConfig) -> Result<Self, LogError> {
        <Self as MdsSubscriber>::open_mds(rt, ch)
    }

    /// Boundary twin of [`TxReceiptsSubscriberHandle::open_auto`]: MDS
    /// plus attaching `0..executor_count` when configured, or a plain
    /// subscription otherwise. Boundaries ride distinct per-replica
    /// endpoints from receipts (each manual subscription binds its
    /// destination socket); see
    /// [`ChannelsConfig::tx_receipts_boundary_endpoint`].
    ///
    /// # Errors
    ///
    /// Returns an error if opening the subscription fails, or if any
    /// per-replica endpoint is missing or fails to attach under MDS.
    pub fn open_auto(
        rt: &AeronRuntime,
        ch: &ChannelsConfig,
        executor_count: Option<NonZeroU32>,
    ) -> Result<Self, LogError> {
        <Self as MdsSubscriber>::open_auto(rt, ch, executor_count)
    }

    /// Attach an executor replica's endpoint as an MDS destination. Idempotent.
    ///
    /// # Errors
    ///
    /// Returns an error if this handle was not opened with
    /// [`open_mds`](Self::open_mds), or if the driver rejects or times
    /// out the attach.
    pub fn add_destination(&self, uri: &str) -> Result<(), LogError> {
        self.mds.add_destination(uri)
    }

    /// Detach a previously-attached executor endpoint (membership churn).
    ///
    /// # Errors
    ///
    /// Returns an error if this handle was not opened with
    /// [`open_mds`](Self::open_mds), or if the driver rejects or times
    /// out the detach.
    pub fn remove_destination(&self, uri: &str) -> Result<(), LogError> {
        self.mds.remove_destination(uri)
    }

    pub async fn recv(&mut self) -> Option<(BPosition, BlockBoundary)> {
        self.rx.recv().await
    }

    pub fn try_recv(&mut self) -> Option<(BPosition, BlockBoundary)> {
        self.rx.try_recv()
    }
}

impl MdsSubscriber for TxReceiptsBoundarySubscriberHandle {
    const KIND: &'static str = "boundary";

    fn open(rt: &AeronRuntime, ch: &ChannelsConfig) -> Result<Self, LogError> {
        Ok(Self {
            rx: rt.open_subscription::<BlockBoundary>(
                &ch.tx_receipts_channel,
                ch.tx_receipts_boundary_stream_id(),
            )?,
            mds: MdsSub::new(None, rt, "boundary"),
        })
    }

    fn open_mds(rt: &AeronRuntime, ch: &ChannelsConfig) -> Result<Self, LogError> {
        require_mds(ch)?;
        let (sub_id, rx) = rt.open_subscription_with_id::<BlockBoundary>(
            &ch.tx_receipts_control_channel,
            ch.tx_receipts_boundary_stream_id(),
        )?;
        Ok(Self {
            rx,
            mds: MdsSub::new(Some(sub_id), rt, "boundary"),
        })
    }

    fn endpoint_of(ch: &ChannelsConfig, replica_idx: u32) -> Option<String> {
        ch.tx_receipts_boundary_endpoint(replica_idx)
    }

    fn mds(&self) -> &MdsSub {
        &self.mds
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One raw batch frame must fan out to every receipt it carries, in
    /// order, before the receiver goes back to the raw channel. This is
    /// consumer-side behavior (`TxReceiptsReceiver`, what
    /// `into_receiver()` returns), so the test needs no `AeronRuntime`.
    #[test]
    fn one_batch_frame_fans_out_to_every_receipt_in_order() {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let mut receiver = TxReceiptsReceiver {
            rx,
            pending: VecDeque::new(),
        };
        let pos = BPosition {
            term_id: 1,
            term_offset: 2,
        };
        let batch = vec![
            Receipt {
                nonce: 0,
                ..Default::default()
            },
            Receipt {
                nonce: 1,
                ..Default::default()
            },
            Receipt {
                nonce: 2,
                ..Default::default()
            },
        ];
        tx.send(RawFrame {
            bytes: codec::encode(&batch).unwrap().to_vec(),
            pos,
            session: 0,
        })
        .unwrap();

        for expected in &batch {
            let (got_pos, got) = receiver.try_recv().expect("fanned-out receipt");
            assert_eq!(got_pos, pos);
            assert_eq!(&got, expected);
        }
        assert!(
            receiver.try_recv().is_none(),
            "every receipt in the one frame was already drained"
        );
    }
}
