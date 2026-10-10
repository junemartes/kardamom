//! The executor stream source of a consumer outside the executors: the
//! flag that picks the transaction source, the live `exec_txs`
//! subscription, and the miss path through the executor archives.

use std::path::Path;

use anyhow::{Context, Result};

use kardamom_log::aeron_live::AeronRuntime;
use kardamom_log::config::AeronConfig;

use crate::actor::Either;
use crate::reader::{
    ExecRecordSubscription, ExecStreamSource, LiveExecArchiveSeed, LocatorClient, TxDataSource,
};

use super::{ArchiveSources, LiveTxDataSub, LocalRefetch};

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
pub struct LiveExecTxsSub(kardamom_log::aeron_live::ExecTxsSubscriberHandle);

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

/// Everything [`open_exec_stream`] needs.
pub struct ExecStreamConfig<'a> {
    pub rt: &'a AeronRuntime,
    pub plane: &'a mut kardamom_log::discovery::StreamPlane,
    pub aeron_cfg: &'a AeronConfig,
    pub aeron_dir: Option<&'a Path>,
    pub archive_control_response_endpoint: Option<&'a str>,
    pub replay_destination_endpoint: Option<&'a str>,
    /// The query endpoints of the executors, as `http://host:port`. The
    /// locator query of a miss asks them in this order.
    pub executor_query_endpoints: &'a [String],
}

/// Open the executor stream source: the live `exec_txs` subscription, and
/// the miss path when the executor query endpoints and the local refetch
/// endpoints are set. Without the miss path a lost live record waits for
/// a void record, and a warning says so.
///
/// # Errors
///
/// Returns `Err` when the subscription fails to open, or an executor
/// query endpoint names no `host:port`.
pub fn open_exec_stream(cfg: ExecStreamConfig<'_>) -> Result<LiveExecStreamSource> {
    let ExecStreamConfig {
        rt,
        plane,
        aeron_cfg,
        aeron_dir,
        archive_control_response_endpoint,
        replay_destination_endpoint,
        executor_query_endpoints,
    } = cfg;
    let sub = plane
        .subscriber::<kardamom_log::aeron_live::ExecTxsSubscriberHandle>(rt)
        .context("open exec_txs subscription")?;
    let locator = LocatorClient::new(
        executor_query_endpoints,
        kardamom_state::exec_peers::DEFAULT_PEER_TIMEOUT,
    )
    .map_err(anyhow::Error::msg)?;
    let stream_id = plane.channels().exec_txs_stream_id;
    let sources = ArchiveSources::of(plane, aeron_cfg);
    let local = LocalRefetch::new(
        aeron_cfg,
        aeron_dir,
        archive_control_response_endpoint,
        replay_destination_endpoint,
    );
    let archive = if let (Some(local), false) = (local, locator.is_empty()) {
        Some(LiveExecArchiveSeed {
            locator,
            refetch: local.config(sources),
            stream_id,
        })
    } else {
        tracing::warn!(
            "executor stream refetch DISABLED (needs --executor-query-endpoints, \
             --archive-control-response-endpoint and --replay-destination-endpoint); \
             a lost exec_txs record waits for the live stream or a void record"
        );
        None
    };
    Ok(ExecStreamSource::new(LiveExecTxsSub(sub), archive))
}
