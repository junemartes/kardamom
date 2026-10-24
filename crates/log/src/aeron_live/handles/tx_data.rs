//! `TxData`: per-shard envelope channel (proxy → seq/exec/batcher).

use super::super::{AeronRuntime, PositionedTxData, PubHandle, TxDataSubscription};
use crate::aeron_live::handles::simple::declare_channel_handles;
use crate::config::ChannelsConfig;
use crate::error::LogError;
use kardamom_types::{BPosition, TxDataLoc, TxEnvelope};

declare_channel_handles! {
    /// Per-shard `TxData` publisher. Publishes full `TxEnvelope` bytes.
    publisher TxDataPublisherHandle {
        /// # Errors
        ///
        /// Returns an error if the underlying Aeron offer fails or times out
        /// (see `PubHandle::publish`).
        pub fn publish(&self, env: &TxEnvelope) -> Result<BPosition, LogError> {
            self.inner.publish(env)
        }

        /// [`Self::publish`] for an async caller: the task awaits the ack
        /// and blocks no thread (see `PubHandle::publish_async`).
        ///
        /// # Errors
        ///
        /// Returns an error if the underlying Aeron offer fails or times out.
        pub async fn publish_async(&self, env: &TxEnvelope) -> Result<BPosition, LogError> {
            self.inner.publish_async(env).await
        }
    }
    /// Per-shard `TxData` subscriber. Yields `(TxDataLoc, TxEnvelope)`: the
    /// envelope paired with its publisher `session_id` and `BPosition`, so
    /// the sequencer can stamp `TxRef.tx_data_session_id` and the executor
    /// can key its join buffer on `(shard, session, position)` under
    /// concurrent ingress publishers.
    subscriber TxDataSubscriberHandle(
        item = (TxDataLoc, TxEnvelope),
        rx = TxDataSubscription,
        subscribe = AeronRuntime::open_tx_data_subscription
    );
    open(ch, sequencer_id: u8) = (
        ch.tx_data_channel(sequencer_id),
        ch.tx_data_stream_id(sequencer_id)
    );
}

impl TxDataSubscriberHandle {
    /// [`Self::try_recv`] with the stream position of the envelope in its
    /// image: the position by which racing sequencers name one message.
    pub fn try_recv_positioned(&mut self) -> Option<PositionedTxData> {
        self.rx.try_recv_positioned()
    }

    /// The frames that wait in the receive queue, for a queue-depth gauge.
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
