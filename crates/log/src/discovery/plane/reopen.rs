//! Opening the `tx_receipts` publication again. A publication that stays
//! unconnected while its subscribers are attached is dead on the
//! publisher side. The publisher opens a new one and the subscribers
//! follow it.

use std::net::IpAddr;

use tokio::sync::watch;
use tracing::{info, warn};

use super::{Discovered, StreamKey, StreamPlane};
use crate::aeron_live::{AeronRuntime, PubHandle, TxReceiptsPublisherHandle};
use crate::discovery::endpoint::publication_uri;
use crate::discovery::record::{PublisherRecord, Scope, ServiceId};
use crate::discovery::registration::RecordMover;
use crate::discovery::watch::Membership;
use crate::error::LogError;

/// What a discovered publication needs to open again under its own
/// service id: a new dynamic MDC publication on a new control port, and
/// the catalog record of the same id moved to it through the heartbeat
/// of its registration. A consumer that watches the record detaches the
/// old control endpoint and attaches the new one. The shutdown of the
/// plane deregisters the id as before.
///
/// It also watches the subscriber records of the stream: the publisher
/// escalates only while a subscriber is listed. A stream without a
/// subscriber (a cluster bootstrap, an ingress pair down) has nothing to
/// reopen for.
pub struct PublisherReopen {
    mover: RecordMover,
    scope: Scope,
    key: StreamKey,
    id: ServiceId,
    publisher_id: String,
    uri: String,
    subscribers: watch::Receiver<Membership>,
}

impl PublisherReopen {
    /// Open a new publication and move the record to its control
    /// endpoint. Returns the new publication. The caller closes the old
    /// one. A publication whose record cannot move closes again here.
    ///
    /// # Errors
    ///
    /// Returns an error if the publication fails to open, if the driver
    /// reports no bound control address, or if the catalog refuses the
    /// record.
    pub async fn reopen(&self, rt: &AeronRuntime) -> Result<PubHandle, LogError> {
        let (publication, control) = rt.open_mdc_publication(&self.uri, self.key.stream_id)?;
        let record = PublisherRecord {
            id: self.id.clone(),
            control,
            topic: self.key.topic,
            stream_id: self.key.stream_id,
            lane: self.key.lane,
            publisher_id: self.publisher_id.clone(),
            session_id: Some(publication.session_id()),
        };
        if let Err(e) = self.mover.relocate(record.entry(&self.scope)).await {
            if let Err(close) = publication.close() {
                warn!(error = %close, "discovery: the publication of a failed record move did not close");
            }
            return Err(e);
        }
        info!(
            topic = %self.key.topic,
            stream_id = self.key.stream_id,
            %control,
            session_id = publication.session_id(),
            "discovery: publication reopened on a new control port"
        );
        Ok(publication)
    }

    /// Whether the catalog lists a subscriber of the stream.
    #[must_use]
    pub fn subscribers_listed(&self) -> bool {
        !self.subscribers.borrow().entries.is_empty()
    }
}

impl Discovered {
    /// The reopen of the publication of `key`, as
    /// [`Self::open_publication`] registered it.
    ///
    /// # Errors
    ///
    /// Returns an error if the publication of `key` is not open.
    fn reopener(&mut self, key: StreamKey) -> Result<PublisherReopen, LogError> {
        let id = self.instance.service_id(key.topic, key.stream_id);
        let mover = self
            .registrations
            .iter()
            .find(|r| *r.id() == id)
            .map(super::Registration::mover)
            .ok_or_else(|| {
                LogError::Discovery(format!("reopen: the {} publication is not open", key.topic))
            })?;
        Ok(PublisherReopen {
            mover,
            scope: self.scope.clone(),
            key,
            id,
            publisher_id: self.label.clone(),
            uri: publication_uri(IpAddr::V4(self.ip), &self.cfg.flow_control, key.topic),
            subscribers: self.subscribers(key),
        })
    }
}

/// How the receipt stream of `tx_receipts` opens again. A static plane
/// opens an exclusive publication on the same channel: a new session,
/// which the subscriber takes as a new image. A discovered plane opens a
/// new dynamic MDC publication and moves the record to it. The boundary
/// side-stream is best effort and stays as it is.
pub enum TxReceiptsReopen {
    Static { channel: String, stream_id: i32 },
    Discovered(PublisherReopen),
}

impl TxReceiptsReopen {
    /// Open the receipt stream again. Returns the new publication, for
    /// [`TxReceiptsPublisherHandle::replace_receipts`].
    ///
    /// # Errors
    ///
    /// Returns an error if the publication fails to open or register.
    pub async fn reopen(&self, rt: &AeronRuntime) -> Result<PubHandle, LogError> {
        match self {
            Self::Static { channel, stream_id } => {
                rt.open_exclusive_publication(channel, *stream_id)
            }
            Self::Discovered(reopen) => reopen.reopen(rt).await,
        }
    }

    /// Whether a subscriber of the receipt stream is known. A static
    /// plane has no catalog and names its subscribers in its config, so
    /// one always counts as known.
    #[must_use]
    pub fn subscribers_listed(&self) -> bool {
        match self {
            Self::Static { .. } => true,
            Self::Discovered(reopen) => reopen.subscribers_listed(),
        }
    }
}

impl StreamPlane {
    /// The reopen of the receipt stream that
    /// [`Self::tx_receipts_publisher`] opened for `replica_idx`.
    ///
    /// # Errors
    ///
    /// Returns an error on a discovered plane whose receipt stream is not
    /// open, or on a static plane whose replica has no MDS endpoint.
    pub fn tx_receipts_reopen(&mut self, replica_idx: u32) -> Result<TxReceiptsReopen, LogError> {
        let key = self.receipts_key();
        match &mut self.discovered {
            None => Ok(TxReceiptsReopen::Static {
                channel: TxReceiptsPublisherHandle::receipts_channel(&self.channels, replica_idx)?,
                stream_id: key.stream_id,
            }),
            Some(d) => d.reopener(key).map(TxReceiptsReopen::Discovered),
        }
    }
}
