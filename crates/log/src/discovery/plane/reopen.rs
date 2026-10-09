//! Opening the `tx_receipts` publication again. A publication that stays
//! unconnected while its subscribers are attached is dead on the
//! publisher side. The publisher opens a new one and the subscribers
//! follow it.

use std::net::IpAddr;
use std::time::Duration;

use tracing::info;

use super::{Discovered, StreamKey, StreamPlane};
use crate::aeron_live::{AeronRuntime, PubHandle, TxReceiptsPublisherHandle};
use crate::config::ChannelsConfig;
use crate::discovery::catalog::{Catalog, RegistrationSpec};
use crate::discovery::endpoint::publication_uri;
use crate::discovery::record::{PublisherRecord, Scope, ServiceId};
use crate::error::LogError;

/// What a discovered publication needs to open again under its own
/// service id: a new dynamic MDC publication on a new control port, and
/// the catalog record of the same id moved to it. A consumer that watches
/// the record detaches the old control endpoint and attaches the new one.
/// The heartbeat of the first registration keeps passing the check of the
/// id, and the shutdown of the plane deregisters it, so a reopen only
/// moves the record.
pub struct PublisherReopen {
    catalog: Catalog,
    scope: Scope,
    key: StreamKey,
    id: ServiceId,
    publisher_id: String,
    uri: String,
    ttl: Duration,
    deregister_after: Duration,
}

impl PublisherReopen {
    /// Open a new publication and move the record to its control
    /// endpoint. Returns the new publication. The caller closes the old
    /// one.
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
        let spec = RegistrationSpec {
            entry: record.entry(&self.scope),
            ttl: self.ttl,
            deregister_after: self.deregister_after,
        };
        self.catalog.register(&spec).await?;
        self.catalog.pass(&spec.entry.id).await?;
        info!(
            topic = %self.key.topic,
            stream_id = self.key.stream_id,
            %control,
            session_id = publication.session_id(),
            "discovery: publication reopened on a new control port"
        );
        Ok(publication)
    }
}

impl Discovered {
    /// The reopen of the publication of `key`, as
    /// [`Self::open_publication`] registered it.
    fn reopener(&self, key: StreamKey) -> PublisherReopen {
        PublisherReopen {
            catalog: self.catalog.clone(),
            scope: self.scope.clone(),
            key,
            id: self.instance.service_id(key.topic, key.stream_id),
            publisher_id: self.label.clone(),
            uri: publication_uri(IpAddr::V4(self.ip), &self.cfg.flow_control, key.topic),
            ttl: self.cfg.check_ttl(),
            deregister_after: self.cfg.deregister_after(),
        }
    }
}

/// How the receipt stream of `tx_receipts` opens again. A static plane
/// opens an exclusive publication on the same channel: a new session,
/// which the subscriber takes as a new image. A discovered plane opens a
/// new dynamic MDC publication and moves the record to it. The boundary
/// side-stream is best effort and stays as it is.
pub enum TxReceiptsReopen {
    Static {
        channels: ChannelsConfig,
        replica_idx: u32,
    },
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
            Self::Static {
                channels,
                replica_idx,
            } => TxReceiptsPublisherHandle::reopen_receipts(rt, channels, *replica_idx),
            Self::Discovered(reopen) => reopen.reopen(rt).await,
        }
    }
}

impl StreamPlane {
    /// The reopen of the receipt stream that
    /// [`Self::tx_receipts_publisher`] opened for `replica_idx`.
    #[must_use]
    pub fn tx_receipts_reopen(&self, replica_idx: u32) -> TxReceiptsReopen {
        match &self.discovered {
            None => TxReceiptsReopen::Static {
                channels: self.channels.clone(),
                replica_idx,
            },
            Some(d) => TxReceiptsReopen::Discovered(d.reopener(self.receipts_key())),
        }
    }
}
