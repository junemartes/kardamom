//! `TxData`: per-shard envelope channel (proxy → seq/exec/batcher).

use tokio::sync::mpsc::UnboundedReceiver;

use super::super::{AeronRuntime, PubHandle};
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
    }
    /// Per-shard `TxData` subscriber. Yields `(TxDataLoc, TxEnvelope)`: the
    /// envelope paired with its publisher `session_id` and `BPosition`, so
    /// the sequencer can stamp `TxRef.tx_data_session_id` and the executor
    /// can key its join buffer on `(shard, session, position)` under
    /// concurrent ingress publishers.
    subscriber TxDataSubscriberHandle(
        item = (TxDataLoc, TxEnvelope),
        subscribe = AeronRuntime::open_tx_data_subscription
    );
    open(ch, sequencer_id: u8) = (
        ch.tx_data_channel(sequencer_id),
        ch.tx_data_stream_id(sequencer_id)
    );
}
