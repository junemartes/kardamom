//! The taps: one subscription each on `tx_status`, `tx_receipts` and
//! `tx_errors`, and one task per tap that forwards every record into
//! the hub's ingest queue. With the events path on, the taps also read
//! `tx_events`, take the receipts of the egress as whole frames, and, for
//! the evaluation, the frames of every executor.
//!
//! Every stream is an Aeron publication with multi-destination cast, so
//! a new reader costs the publisher nothing. A lagging tap is paced as
//! the fastest receiver and recovers through the archive refetch.

use std::num::NonZeroU32;

use kardamom_log::LogError;
use kardamom_log::aeron_live::{
    AeronRuntime, RawFrame, TxErrorsSubscriberHandle, TxReceiptsReceiver, TxStatusSubscriberHandle,
};
use kardamom_log::discovery::StreamPlane;
use kardamom_types::TxStatus;
use tokio::sync::mpsc;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::task::JoinHandle;

use crate::hub::Observed;

/// One tap's pull side: the next observed record, or `None` when the
/// subscription closed.
pub trait TapSource: Send + 'static {
    fn next(&mut self) -> impl Future<Output = Option<Observed>> + Send;
}

impl TapSource for TxStatusSubscriberHandle {
    async fn next(&mut self) -> Option<Observed> {
        self.recv().await.map(|(_, s)| Observed::Status(s))
    }
}

impl TapSource for TxErrorsSubscriberHandle {
    async fn next(&mut self) -> Option<Observed> {
        self.recv().await.map(|(_, e)| Observed::Error(e))
    }
}

impl TapSource for TxReceiptsReceiver {
    async fn next(&mut self) -> Option<Observed> {
        self.recv()
            .await
            .map(|(_, r)| Observed::Status(TxStatus::executed(&r)))
    }
}

/// The `tx_events` frames, for the events path.
pub struct EventFrames(UnboundedReceiver<RawFrame>);

impl TapSource for EventFrames {
    async fn next(&mut self) -> Option<Observed> {
        self.0
            .recv()
            .await
            .map(|frame| Observed::Events(frame.bytes))
    }
}

/// The frames of `egress_receipts`, whole, for the events path.
pub struct MergedFrames(TxReceiptsReceiver);

impl TapSource for MergedFrames {
    async fn next(&mut self) -> Option<Observed> {
        self.0
            .recv_frame()
            .await
            .map(|(_, frame)| Observed::Merged(frame))
    }
}

/// The frames of every executor's `tx_receipts`, with their session, for
/// the `executed` stage of the events path.
pub struct CopyFrames(TxReceiptsReceiver);

impl TapSource for CopyFrames {
    async fn next(&mut self) -> Option<Observed> {
        self.0
            .recv_frame()
            .await
            .map(|(session, frame)| Observed::Copy { session, frame })
    }
}

/// The taps of the events path (`KARDAMOM_NOTIFIER_EVENTS`).
#[derive(Clone, Copy, Debug)]
pub struct EventTaps {
    /// Also read the `tx_receipts` of every executor
    /// (`KARDAMOM_NOTIFIER_EXECUTED`).
    pub executed: bool,
    /// The executor count of that fan-in.
    pub executor_count: Option<NonZeroU32>,
}

/// Where the receipts come from.
#[derive(Clone, Copy, Debug)]
pub enum ReceiptSource {
    /// The `tx_receipts` stream of every executor. The count sizes the
    /// fan-in under MDS; discovery ignores it.
    Executors(Option<NonZeroU32>),
    /// The `egress_receipts` stream: each receipt once, merged by the
    /// egress.
    Egress,
}

/// The open subscriptions.
pub struct Taps {
    status: TxStatusSubscriberHandle,
    receipts: Receipts,
    errors: TxErrorsSubscriberHandle,
    events: Option<EventFrames>,
    copies: Option<CopyFrames>,
}

/// The receipt tap: one status for each receipt, or, on the events path,
/// the whole frames of the egress with their rollbacks.
enum Receipts {
    Statuses(TxReceiptsReceiver),
    Merged(MergedFrames),
}

impl Taps {
    /// Open the subscriptions through the plane. The receipts come from
    /// `source`. With `events`, the receipts come from the egress as
    /// whole frames, whatever `source` says: the `receipt` stage is the
    /// merged receipt.
    ///
    /// # Errors
    ///
    /// Returns an error if a subscription fails to open.
    pub fn open(
        rt: &AeronRuntime,
        plane: &mut StreamPlane,
        source: ReceiptSource,
        events: Option<EventTaps>,
    ) -> Result<Self, LogError> {
        // The receivers alone move into the tasks. A runtime clone in a
        // task would hold the Aeron thread open past shutdown.
        let receipts = match (events, source) {
            (Some(_), _) => Receipts::Merged(MergedFrames(
                plane.egress_receipts_subscriber(rt)?.into_receiver(),
            )),
            (None, ReceiptSource::Executors(executor_count)) => Receipts::Statuses(
                plane
                    .tx_receipts_subscriber(rt, executor_count)?
                    .into_receiver(),
            ),
            (None, ReceiptSource::Egress) => {
                Receipts::Statuses(plane.egress_receipts_subscriber(rt)?.into_receiver())
            }
        };
        let copies = match events {
            Some(EventTaps {
                executed: true,
                executor_count,
            }) => Some(CopyFrames(
                plane
                    .tx_receipts_subscriber(rt, executor_count)?
                    .into_receiver(),
            )),
            _ => None,
        };
        let events = match events {
            Some(_) => Some(EventFrames(plane.tx_events_subscription(rt)?)),
            None => None,
        };
        Ok(Self {
            status: plane.subscriber::<TxStatusSubscriberHandle>(rt)?,
            receipts,
            errors: plane.subscriber::<TxErrorsSubscriberHandle>(rt)?,
            events,
            copies,
        })
    }

    /// Start the forwarding tasks. Each ends when its subscription closes,
    /// on `AeronRuntime` shutdown, or when the hub is gone.
    #[must_use]
    pub fn spawn(self, ingest: &mpsc::Sender<Observed>) -> Vec<JoinHandle<()>> {
        let mut tasks = vec![
            tokio::spawn(forward(self.status, ingest.clone())),
            tokio::spawn(forward(self.errors, ingest.clone())),
        ];
        tasks.push(match self.receipts {
            Receipts::Statuses(rx) => tokio::spawn(forward(rx, ingest.clone())),
            Receipts::Merged(rx) => tokio::spawn(forward(rx, ingest.clone())),
        });
        tasks.extend(
            self.events
                .map(|rx| tokio::spawn(forward(rx, ingest.clone()))),
        );
        tasks.extend(
            self.copies
                .map(|rx| tokio::spawn(forward(rx, ingest.clone()))),
        );
        tasks
    }
}

/// Drain `source` into `ingest` until either side closes. The ingest
/// queue is bounded, so a hub that falls behind paces the tap, never
/// the publisher.
async fn forward<S: TapSource>(mut source: S, ingest: mpsc::Sender<Observed>) {
    while let Some(observed) = source.next().await
        && ingest.send(observed).await.is_ok()
    {}
}
