//! TxData: per-shard envelope channel (proxy → seq/exec/batcher).

use tokio::sync::mpsc::UnboundedReceiver;

use super::super::{AeronRuntime, PubHandle};
use crate::config::ChannelsConfig;
use crate::error::LogError;
use kardamom_types::{BPosition, TxDataLoc, TxEnvelope};

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
