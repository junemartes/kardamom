//! The miss path of the executor stream: ask each executor where its
//! archive holds index `i`, and replay that archive from the locator.

use alloy_primitives::B256;
use kardamom_log::error::LogError;
use kardamom_log::refetch::{ArchiveRefetcher, ExecRecordsAt, RefetchConfig};
use kardamom_state::PeerError;
use kardamom_types::ExecTxRecord;

use super::locator::{ArchiveLocator, LocatorAnswer, LocatorClient};

/// A refetch step of the executor stream failed.
#[derive(Debug, thiserror::Error)]
pub enum ExecFetchError {
    #[error(transparent)]
    Locator(#[from] PeerError),
    #[error(transparent)]
    Archive(#[from] LogError),
}

/// The executor archives, as the wait of a consumer reaches them.
pub trait ExecArchive {
    /// The count of executors to ask. Executor `k` is `0..executors()`.
    fn executors(&self) -> usize;

    /// Ask executor `executor` where its archive holds `index`.
    ///
    /// # Errors
    ///
    /// Returns a failed query. The wait counts it as no answer.
    fn locate(
        &mut self,
        executor: usize,
        index: u64,
        tx_hash: B256,
    ) -> Result<LocatorAnswer, ExecFetchError>;

    /// Replay the archive that `at` names, from its position, and give
    /// each record to `sink`. Return the count of records.
    ///
    /// # Errors
    ///
    /// Returns a failed replay. The wait counts it as no answer.
    fn replay(
        &mut self,
        at: &ArchiveLocator,
        sink: impl FnMut(ExecTxRecord),
    ) -> Result<u64, ExecFetchError>;
}

/// The part of an [`ExecArchive`] that crosses into the `tx_ordering`
/// reader thread. The archive itself holds Aeron resources that are bound
/// to one thread.
pub trait ExecArchiveSeed: Send + 'static {
    type Archive: ExecArchive;

    fn build(self) -> Self::Archive;
}

/// No archive: a deploy with no executor query endpoint, or no local
/// replay endpoint. The wait then has no miss path, and it waits for the
/// live stream or a void record.
impl<S: ExecArchiveSeed> ExecArchiveSeed for Option<S> {
    type Archive = Option<S::Archive>;

    fn build(self) -> Self::Archive {
        self.map(ExecArchiveSeed::build)
    }
}

impl<A: ExecArchive> ExecArchive for Option<A> {
    fn executors(&self) -> usize {
        self.as_ref().map_or(0, ExecArchive::executors)
    }

    fn locate(
        &mut self,
        executor: usize,
        index: u64,
        tx_hash: B256,
    ) -> Result<LocatorAnswer, ExecFetchError> {
        match self {
            Some(a) => a.locate(executor, index, tx_hash),
            None => Ok(LocatorAnswer::NotReached),
        }
    }

    fn replay(
        &mut self,
        at: &ArchiveLocator,
        sink: impl FnMut(ExecTxRecord),
    ) -> Result<u64, ExecFetchError> {
        match self {
            Some(a) => a.replay(at, sink),
            None => Ok(0),
        }
    }
}

/// The live executor archives: the locator queries over HTTP, and the
/// bounded replays of the archive client.
pub struct LiveExecArchiveSeed {
    pub(crate) locator: LocatorClient,
    pub(crate) refetch: RefetchConfig,
    pub(crate) stream_id: i32,
}

impl ExecArchiveSeed for LiveExecArchiveSeed {
    type Archive = LiveExecArchive;

    fn build(self) -> LiveExecArchive {
        LiveExecArchive {
            locator: self.locator,
            refetcher: ArchiveRefetcher::new(self.refetch),
            stream_id: self.stream_id,
        }
    }
}

/// [`LiveExecArchiveSeed`], built on the reader thread.
pub struct LiveExecArchive {
    locator: LocatorClient,
    refetcher: ArchiveRefetcher,
    stream_id: i32,
}

impl ExecArchive for LiveExecArchive {
    fn executors(&self) -> usize {
        self.locator.len()
    }

    fn locate(
        &mut self,
        executor: usize,
        index: u64,
        tx_hash: B256,
    ) -> Result<LocatorAnswer, ExecFetchError> {
        Ok(self.locator.ask(executor, index, tx_hash)?)
    }

    fn replay(
        &mut self,
        at: &ArchiveLocator,
        sink: impl FnMut(ExecTxRecord),
    ) -> Result<u64, ExecFetchError> {
        let replay = ExecRecordsAt {
            archive_id: &at.archive_id,
            stream_id: self.stream_id,
            session_id: at.session_id,
            position: at.position,
        };
        Ok(self.refetcher.fetch_exec_records(&replay, sink)?)
    }
}
