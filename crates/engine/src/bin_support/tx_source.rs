//! The transaction source of a consumer outside the executors (the
//! validator, the batcher): the `tx_data` lanes or the executor stream, as
//! a flag picks it, and the archives of its miss path.

use std::path::Path;

use anyhow::{Context, Result};

use kardamom_log::aeron_live::{AeronRuntime, ExecTxsSubscriberHandle};
use kardamom_log::config::AeronConfig;
use kardamom_log::discovery::StreamPlane;

use super::{
    ArchiveSources, LiveTxDataSub, LocalRefetch, archive_join_recovery, open_tx_data_subs,
};
use crate::actor::Either;
use crate::reader::{
    ExecRecordSubscription, ExecStreamSource, JoinRecoveryFactory, LiveExecArchiveSeed,
    LocatorClient, TxDataSource,
};

/// Which transaction source a consumer outside the executors reads. Clap
/// renders the variants as `tx-data` and `exec-stream`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum TxSourceArg {
    /// Join the `tx_data` lanes, as the executor does.
    TxData,
    /// Read the executor stream, and refetch a miss from the executor
    /// archives. The consumer never votes.
    ExecStream,
}

impl TxSourceArg {
    /// The flag value, for the log line.
    #[must_use]
    pub fn id(self) -> &'static str {
        match self {
            Self::TxData => "tx-data",
            Self::ExecStream => "exec-stream",
        }
    }
}

/// The live `exec_txs` subscription of a consumer. One destination per
/// executor. The feed thread blocks on it, off the tokio runtime.
pub struct LiveExecTxsSub(ExecTxsSubscriberHandle);

impl ExecRecordSubscription for LiveExecTxsSub {
    fn next(&mut self) -> Option<kardamom_types::ExecTxRecord> {
        self.0.blocking_recv().map(|(_, record)| record)
    }
}

/// The executor stream source of a live consumer: the live subscription,
/// and the executor archives when the locator and the refetch are wired.
pub type LiveExecStreamSource = ExecStreamSource<LiveExecTxsSub, Option<LiveExecArchiveSeed>>;

/// The transaction source a flag picks: the `tx_data` lanes or the
/// executor stream.
pub type LiveTxSource = Either<TxDataSource<LiveTxDataSub>, LiveExecStreamSource>;

/// This node's way to the archives: the plane that lists them, and the
/// local endpoints where a replay lands.
pub struct ArchiveAccess<'a> {
    /// The plane of the subscriptions. Its channels and its archive
    /// records also feed the refetch wiring.
    pub plane: &'a mut StreamPlane,
    pub aeron_cfg: &'a AeronConfig,
    pub aeron_dir: Option<&'a Path>,
    pub archive_control_response_endpoint: Option<&'a str>,
    pub replay_destination_endpoint: Option<&'a str>,
}

impl ArchiveAccess<'_> {
    /// The join-miss refetch from the `tx_data` archives. See
    /// [`archive_join_recovery`].
    pub fn join_recovery(&mut self) -> Option<JoinRecoveryFactory> {
        archive_join_recovery(
            self.plane,
            self.aeron_cfg,
            self.aeron_dir,
            self.archive_control_response_endpoint,
            self.replay_destination_endpoint,
        )
    }

    /// The executor archives: the locator query to each endpoint of
    /// `executor_query_endpoints`, in that order, and the replay from the
    /// archive that a locator names. `None` when no executor query
    /// endpoint or no local refetch endpoint is set.
    ///
    /// # Errors
    ///
    /// Returns `Err` when an executor query endpoint names no `host:port`.
    pub fn exec_archive(
        &mut self,
        executor_query_endpoints: &[String],
    ) -> Result<Option<LiveExecArchiveSeed>> {
        let locator = LocatorClient::new(executor_query_endpoints).map_err(anyhow::Error::msg)?;
        let stream_id = self.plane.channels().exec_txs_stream_id;
        let sources = ArchiveSources::of(self.plane, self.aeron_cfg);
        let local = LocalRefetch::new(
            self.aeron_cfg,
            self.aeron_dir,
            self.archive_control_response_endpoint,
            self.replay_destination_endpoint,
        );
        Ok(local
            .filter(|_| !locator.is_empty())
            .map(|local| LiveExecArchiveSeed {
                locator,
                refetch: local.config(sources),
                stream_id,
            }))
    }
}

/// Everything the transaction source of a consumer needs.
pub struct TxSourceConfig<'a> {
    pub rt: &'a AeronRuntime,
    pub archives: ArchiveAccess<'a>,
    /// The source: the `tx_data` lanes, or the executor stream.
    pub tx_source: TxSourceArg,
    /// The executor query endpoints of the executor stream's miss path.
    /// Unused on the `tx_data` source.
    pub executor_query_endpoints: &'a [String],
    /// Names this binary in the log lines.
    pub bin_name: &'a str,
}

impl TxSourceConfig<'_> {
    /// Open the source that [`Self::tx_source`] names: the M `tx_data`
    /// streams (always live multicast, join-miss gaps recovered in-band by
    /// archive refetch), or the executor stream.
    ///
    /// # Errors
    ///
    /// Returns `Err` when a subscription fails to open, or an executor
    /// query endpoint names no `host:port`.
    pub fn open(mut self) -> Result<LiveTxSource> {
        tracing::info!(
            tx_source = self.tx_source.id(),
            "{}: transaction source {}",
            self.bin_name,
            self.tx_source.id()
        );
        match self.tx_source {
            TxSourceArg::TxData => {
                let tx_data = open_tx_data_subs(self.rt, self.archives.plane)?;
                let recovery = self.archives.join_recovery();
                Ok(Either::Left(TxDataSource::new(tx_data, recovery)))
            }
            TxSourceArg::ExecStream => self.open_exec_stream().map(Either::Right),
        }
    }

    /// The live `exec_txs` subscription, and the miss path when the
    /// executor query endpoints and the local refetch endpoints are set.
    /// Without the miss path a lost live record waits for a void record,
    /// and a warning says so.
    fn open_exec_stream(mut self) -> Result<LiveExecStreamSource> {
        let sub = self
            .archives
            .plane
            .subscriber::<ExecTxsSubscriberHandle>(self.rt)
            .context("open exec_txs subscription")?;
        let archive = self.archives.exec_archive(self.executor_query_endpoints)?;
        if archive.is_none() {
            tracing::warn!(
                "executor stream refetch DISABLED (needs --executor-query-endpoints, \
                 --archive-control-response-endpoint and --replay-destination-endpoint); \
                 a lost exec_txs record waits for the live stream or a void record"
            );
        }
        Ok(ExecStreamSource::new(LiveExecTxsSub(sub), archive))
    }
}
