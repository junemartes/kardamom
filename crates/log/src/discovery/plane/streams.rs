//! The compound and per-lane stream openers of [`StreamPlane`]:
//! `tx_receipts` with its boundary side-stream, and the `tx_data` lanes.

use std::num::NonZeroU32;

use super::{StreamKey, StreamPlane};
use crate::aeron_live::{
    AeronRuntime, TxDataPublisherHandle, TxDataSubscriberHandle, TxDataSubscription,
    TxReceiptsBoundarySubscriberHandle, TxReceiptsPublisherHandle, TxReceiptsSubscriberHandle,
};
use crate::aeron_live::{PubHandle, TypedSubscription};
use crate::discovery::record::Topic;
use crate::error::LogError;
use kardamom_types::BalFrame;

impl StreamPlane {
    /// The `tx_bal` publisher, as the raw handle the executor's BAL
    /// thread drives.
    ///
    /// # Errors
    ///
    /// Returns an error if the publication fails to open or register.
    pub async fn tx_bal_publisher(&mut self, rt: &AeronRuntime) -> Result<PubHandle, LogError> {
        let key = self.tx_bal_key();
        match &mut self.discovered {
            None => rt.open_publication(
                &self.channels.tx_bal_channel,
                self.channels.tx_bal_stream_id,
            ),
            Some(d) => d.open_publication(rt, key).await,
        }
    }

    /// The `tx_bal` subscription the validator's BAL pump reads.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription fails to open.
    pub fn tx_bal_subscription(
        &mut self,
        rt: &AeronRuntime,
    ) -> Result<TypedSubscription<BalFrame>, LogError> {
        let key = self.tx_bal_key();
        match &mut self.discovered {
            None => rt.open_subscription::<BalFrame>(
                &self.channels.tx_bal_channel,
                self.channels.tx_bal_stream_id,
            ),
            Some(d) => d.open_subscription(rt, key),
        }
    }

    fn tx_bal_key(&self) -> StreamKey {
        StreamKey {
            topic: Topic::TxBal,
            stream_id: self.channels.tx_bal_stream_id,
            lane: None,
        }
    }

    /// The `tx_receipts` publisher: the receipt stream and the boundary
    /// side-stream. Static: the per-replica MDS endpoint when the
    /// channels enable MDS, else the shared channel. Discovered: two
    /// dynamic MDC publications, each registered under its own topic.
    ///
    /// # Errors
    ///
    /// Returns an error if either publication fails to open or register.
    pub async fn tx_receipts_publisher(
        &mut self,
        rt: &AeronRuntime,
        replica_idx: u32,
    ) -> Result<TxReceiptsPublisherHandle, LogError> {
        let (receipts_key, boundaries_key) = (self.receipts_key(), self.boundaries_key());
        let Some(d) = &mut self.discovered else {
            return if self.channels.tx_receipts_mds_enabled() {
                TxReceiptsPublisherHandle::open_mds(rt, &self.channels, replica_idx)
            } else {
                TxReceiptsPublisherHandle::open(rt, &self.channels)
            };
        };
        let receipts = d.open_publication(rt, receipts_key).await?;
        let boundaries = d.open_publication(rt, boundaries_key).await?;
        Ok(TxReceiptsPublisherHandle::from_publications(
            receipts, boundaries,
        ))
    }

    /// The `tx_receipts` subscriber. Static: `open_auto` over the
    /// channels, attaching `executor_count` replica endpoints under MDS.
    /// Discovered: one multi-destination subscription the reconcile task
    /// fills from the catalog, so `executor_count` is unused.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription fails to open.
    pub fn tx_receipts_subscriber(
        &mut self,
        rt: &AeronRuntime,
        executor_count: Option<NonZeroU32>,
    ) -> Result<TxReceiptsSubscriberHandle, LogError> {
        let key = self.receipts_key();
        match &mut self.discovered {
            None => TxReceiptsSubscriberHandle::open_auto(rt, &self.channels, executor_count),
            Some(d) => d
                .open_raw_subscription(rt, key)
                .map(|rx| TxReceiptsSubscriberHandle::from_raw(rx, rt)),
        }
    }

    /// The block-boundary twin of [`Self::tx_receipts_subscriber`].
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription fails to open.
    pub fn tx_receipt_boundaries_subscriber(
        &mut self,
        rt: &AeronRuntime,
        executor_count: Option<NonZeroU32>,
    ) -> Result<TxReceiptsBoundarySubscriberHandle, LogError> {
        let key = self.boundaries_key();
        match &mut self.discovered {
            None => {
                TxReceiptsBoundarySubscriberHandle::open_auto(rt, &self.channels, executor_count)
            }
            Some(d) => d
                .open_subscription(rt, key)
                .map(|rx| TxReceiptsBoundarySubscriberHandle::from_subscription(rx, rt)),
        }
    }

    fn receipts_key(&self) -> StreamKey {
        StreamKey {
            topic: Topic::TxReceipts,
            stream_id: self.channels.tx_receipts_stream_id,
            lane: None,
        }
    }

    fn boundaries_key(&self) -> StreamKey {
        StreamKey {
            topic: Topic::TxReceiptBoundaries,
            stream_id: self.channels.tx_receipts_boundary_stream_id(),
            lane: None,
        }
    }

    /// The `tx_data` publisher of `lane`. Discovered: a dynamic MDC
    /// publication registered with its lane id, so a consumer of one lane
    /// joins only that lane's publishers.
    ///
    /// # Errors
    ///
    /// Returns an error if the publication fails to open or register.
    pub async fn tx_data_publisher(
        &mut self,
        rt: &AeronRuntime,
        lane: u8,
    ) -> Result<TxDataPublisherHandle, LogError> {
        let key = self.tx_data_key(lane);
        match &mut self.discovered {
            None => TxDataPublisherHandle::open(rt, &self.channels, lane),
            Some(d) => d
                .open_publication(rt, key)
                .await
                .map(TxDataPublisherHandle::from_publication),
        }
    }

    /// The raw `tx_data` subscription of `lane`, as the engine's reader
    /// threads consume it. Discovered: one multi-destination subscription
    /// the reconcile task fills with every publisher of that lane.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription fails to open.
    pub fn tx_data_subscription(
        &mut self,
        rt: &AeronRuntime,
        lane: u8,
    ) -> Result<TxDataSubscription, LogError> {
        let key = self.tx_data_key(lane);
        match &mut self.discovered {
            None => rt.open_tx_data_subscription(
                &self.channels.tx_data_channel(lane),
                self.channels.tx_data_stream_id(lane),
            ),
            Some(d) => d
                .open_raw_subscription(rt, key)
                .map(TxDataSubscription::from_raw),
        }
    }

    /// [`Self::tx_data_subscription`] wrapped in the typed handle.
    ///
    /// # Errors
    ///
    /// Returns an error if the subscription fails to open.
    pub fn tx_data_subscriber(
        &mut self,
        rt: &AeronRuntime,
        lane: u8,
    ) -> Result<TxDataSubscriberHandle, LogError> {
        self.tx_data_subscription(rt, lane)
            .map(TxDataSubscriberHandle::from_subscription)
    }

    fn tx_data_key(&self, lane: u8) -> StreamKey {
        StreamKey {
            topic: Topic::TxData,
            stream_id: self.channels.tx_data_stream_id(lane),
            lane: Some(lane),
        }
    }
}
