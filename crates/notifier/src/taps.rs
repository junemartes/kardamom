//! The taps: one subscription each on `tx_status`, `tx_receipts` and
//! `tx_errors`, and one task per tap that forwards every record into
//! the hub's ingest queue.
//!
//! Every stream is an Aeron publication with multi-destination cast, so
//! a new reader costs the publisher nothing. A lagging tap is paced as
//! the fastest receiver and recovers through the archive refetch.

use std::num::NonZeroU32;

use kardamom_log::LogError;
use kardamom_log::aeron_live::{
    AeronRuntime, TxErrorsSubscriberHandle, TxReceiptsReceiver, TxStatusSubscriberHandle,
};
use kardamom_log::discovery::StreamPlane;
use kardamom_types::TxStatus;
use tokio::sync::mpsc;
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

/// The three open subscriptions.
pub struct Taps {
    status: TxStatusSubscriberHandle,
    receipts: TxReceiptsReceiver,
    errors: TxErrorsSubscriberHandle,
}

impl Taps {
    /// Open the three subscriptions through the plane. The receipts come
    /// from `source`.
    ///
    /// # Errors
    ///
    /// Returns an error if a subscription fails to open.
    pub fn open(
        rt: &AeronRuntime,
        plane: &mut StreamPlane,
        source: ReceiptSource,
    ) -> Result<Self, LogError> {
        let receipts = match source {
            ReceiptSource::Executors(executor_count) => {
                plane.tx_receipts_subscriber(rt, executor_count)?
            }
            ReceiptSource::Egress => plane.egress_receipts_subscriber(rt)?,
        };
        Ok(Self {
            status: plane.subscriber::<TxStatusSubscriberHandle>(rt)?,
            // The receiver alone moves into the task. A runtime clone in
            // the task would hold the Aeron thread open past shutdown.
            receipts: receipts.into_receiver(),
            errors: plane.subscriber::<TxErrorsSubscriberHandle>(rt)?,
        })
    }

    /// Start the three forwarding tasks. Each ends when its subscription
    /// closes, on `AeronRuntime` shutdown, or when the hub is gone.
    #[must_use]
    pub fn spawn(self, ingest: mpsc::Sender<Observed>) -> [JoinHandle<()>; 3] {
        [
            tokio::spawn(forward(self.status, ingest.clone())),
            tokio::spawn(forward(self.receipts, ingest.clone())),
            tokio::spawn(forward(self.errors, ingest)),
        ]
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
