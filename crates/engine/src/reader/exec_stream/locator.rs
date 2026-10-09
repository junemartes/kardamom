//! The locator query of a consumer: where an executor's archive holds
//! canonical index `i`. The transport is the peer client of the executors
//! (`kardamom_state::ExecPeer`, `kardamom_getExecLocator`). This module
//! names its answer in the shape the consumer's wait matches on.

use std::time::Duration;

use alloy_primitives::B256;
use kardamom_state::{ExecLocatorAnswer, ExecPeer, PeerError};

/// Where one executor's archive holds a record: the archive, the session
/// of the recording, and a raw position at or before the start of the
/// record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArchiveLocator {
    /// The archive record id (`archive_id` in discovery).
    pub archive_id: String,
    /// The Aeron session of the recorded publication.
    pub session_id: i32,
    /// A raw stream position at or before the start of the record.
    pub position: i64,
}

/// One executor's answer for canonical index `i`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LocatorAnswer {
    /// The executor joined `i`, and a retained recording covers it.
    Located(ArchiveLocator),
    /// The executor reached `i` and did not join it.
    NotHeld,
    /// The executor has not reached `i`.
    NotReached,
    /// The executor's state includes `i`, and no retained recording
    /// covers it.
    Lost,
}

impl LocatorAnswer {
    /// The `outcome` label of the refetch metric.
    #[must_use]
    pub fn label(&self) -> &'static str {
        match self {
            Self::Located(_) => "located",
            Self::NotHeld => "not_held",
            Self::NotReached => "not_reached",
            Self::Lost => "lost",
        }
    }
}

impl From<ExecLocatorAnswer> for LocatorAnswer {
    fn from(answer: ExecLocatorAnswer) -> Self {
        match answer {
            ExecLocatorAnswer::Located {
                archive_id,
                session_id,
                position,
            } => Self::Located(ArchiveLocator {
                archive_id,
                session_id,
                position,
            }),
            ExecLocatorAnswer::NotHeld => Self::NotHeld,
            ExecLocatorAnswer::NotReached => Self::NotReached,
            ExecLocatorAnswer::Lost => Self::Lost,
        }
    }
}

/// The locator client: the query endpoints of the executors, in a fixed
/// order, and the bound of one ask. The index of an endpoint names its
/// executor in the wait.
#[derive(Clone, Debug)]
pub(crate) struct LocatorClient {
    endpoints: Vec<ExecPeer>,
    timeout: Duration,
}

impl LocatorClient {
    /// The client of `endpoints` (`http://host:port`), parsed once here.
    ///
    /// # Errors
    ///
    /// Returns the first endpoint that names no `http://host:port`.
    pub(crate) fn new(endpoints: &[String], timeout: Duration) -> Result<Self, String> {
        let endpoints = endpoints
            .iter()
            .map(|raw| raw.parse())
            .collect::<Result<_, _>>()?;
        Ok(Self { endpoints, timeout })
    }

    /// The count of executors the client asks.
    pub(crate) fn len(&self) -> usize {
        self.endpoints.len()
    }

    /// Whether the client asks no executor.
    pub(crate) fn is_empty(&self) -> bool {
        self.endpoints.is_empty()
    }

    /// Ask executor `executor` where its archive holds `index`.
    ///
    /// # Errors
    ///
    /// Returns the failure of the query: no connection, no answer in time,
    /// or an answer that does not parse. An index past the endpoints is no
    /// answer either.
    pub(crate) fn ask(
        &self,
        executor: usize,
        index: u64,
        tx_hash: B256,
    ) -> Result<LocatorAnswer, PeerError> {
        let peer = self
            .endpoints
            .get(executor)
            .ok_or_else(|| PeerError::Response(format!("no executor {executor}")))?;
        Ok(peer.ask(index, tx_hash, self.timeout)?.into())
    }
}
