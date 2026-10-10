//! The live [`BlockFeed`]: the `l1_blocks` subscription, the stream's
//! archives, and the follower's own archive as the backstop.
//!
//! A history read replays every recording of the stream from the
//! archives of the follower nodes and keeps the records from the wanted
//! block on. Where the replay does not reach the wanted block, or stops
//! short (a long recording outlasts the drain cap), the follower's archive
//! serves the missing records by number (`indexer_l1_block`). Neither
//! source is L1: the watcher has no L1 access.

use std::ops::ControlFlow;
use std::path::PathBuf;
use std::sync::mpsc;
use std::thread;

use anyhow::Context;

use alloy_primitives::Bytes;
use async_trait::async_trait;
use jsonrpsee::core::client::ClientT;
use jsonrpsee::http_client::{HttpClient, HttpClientBuilder};
use jsonrpsee::rpc_params;
use kardamom_da_watcher::BlockFeed;
use kardamom_log::aeron_live::{
    AeronRuntime, L1BlocksSubscriberHandle, ServiceEventsSubscriberHandle,
};
use kardamom_log::config::AeronConfig;
use kardamom_log::discovery::{StreamPlane, Topic};
use kardamom_log::refetch::{ArchiveRefetcher, EndpointSource, RefetchConfig};
use kardamom_obs::events::BoardView;
use kardamom_types::L1Block;
use tokio::sync::{oneshot, watch};

/// The most records one history read takes from the follower's archive:
/// about four days of Sepolia. A longer gap fills over several reads.
const STORE_READ_CAP: u64 = 32_768;

/// One history request to the archive thread: the first block, and where
/// the records go.
type Request = (u64, oneshot::Sender<Vec<L1Block>>);

/// The stream's archives, read on a thread of their own: the archive
/// session and its replay runtime are bound to the thread that made them.
pub(crate) struct ArchiveHistory {
    requests: mpsc::Sender<Request>,
}

impl ArchiveHistory {
    /// Start the archive thread. Its Aeron resources open on the first
    /// request.
    pub(crate) fn spawn(
        cfg: RefetchConfig,
        endpoints: EndpointSource,
        stream_id: i32,
    ) -> std::io::Result<Self> {
        let (requests, rx) = mpsc::channel::<Request>();
        thread::Builder::new()
            .name("l1-blocks-history".into())
            .spawn(move || {
                let mut refetcher = ArchiveRefetcher::new(cfg);
                for (from, reply) in rx {
                    let _ = reply.send(Self::replay(
                        &mut refetcher,
                        &endpoints.current(),
                        stream_id,
                        from,
                    ));
                }
            })?;
        Ok(Self { requests })
    }

    /// Every record from `from` on that one archive holds. A failure is
    /// logged and reads as no record: the watcher asks again later.
    fn replay(
        refetcher: &mut ArchiveRefetcher,
        endpoints: &[String],
        stream_id: i32,
        from: u64,
    ) -> Vec<L1Block> {
        let mut records = Vec::new();
        let outcome = refetcher.fetch_whole::<L1Block>(endpoints, stream_id, |record| {
            if record.number >= from {
                records.push(record);
            }
        });
        if let Err(error) = outcome {
            tracing::warn!(%error, from, "l1_blocks archive replay failed");
        }
        records
    }

    /// The records from `from` on, from the archive thread.
    async fn read(&self, from: u64) -> Vec<L1Block> {
        let (reply, records) = oneshot::channel();
        if self.requests.send((from, reply)).is_err() {
            return Vec::new();
        }
        records.await.unwrap_or_default()
    }
}

/// The follower's own archive, by block number, over its API.
pub(crate) struct FollowerStore {
    client: HttpClient,
}

impl FollowerStore {
    pub(crate) fn connect(url: &str) -> anyhow::Result<Self> {
        Ok(Self {
            client: HttpClientBuilder::default().build(url)?,
        })
    }

    /// The record of block `number`, or `None` when the archive does not
    /// hold it or the follower does not answer.
    async fn record(&self, number: u64) -> Option<L1Block> {
        let bytes: Option<Bytes> = self
            .client
            .request("indexer_l1_block", rpc_params![number])
            .await
            .inspect_err(|error| tracing::debug!(%error, number, "indexer_l1_block failed"))
            .ok()?;
        rkyv::from_bytes::<L1Block, rkyv::rancor::Error>(&bytes?)
            .inspect_err(
                |error| tracing::warn!(%error, number, "an l1_blocks record does not decode"),
            )
            .ok()
    }

    /// The records from `from` on, while the archive holds them, up to
    /// [`STORE_READ_CAP`].
    async fn read(&self, from: u64) -> Vec<L1Block> {
        let mut records = Vec::new();
        for number in from..from.saturating_add(STORE_READ_CAP) {
            if self.read_one(number, &mut records).await.is_break() {
                break;
            }
        }
        records
    }

    /// Append the record of `number`. `Break` when the archive does not
    /// hold it.
    async fn read_one(&self, number: u64, records: &mut Vec<L1Block>) -> ControlFlow<()> {
        let Some(record) = self.record(number).await else {
            return ControlFlow::Break(());
        };
        records.push(record);
        ControlFlow::Continue(())
    }
}

/// The stream as the watcher reads it.
pub(crate) struct StreamFeed {
    pub(crate) live: L1BlocksSubscriberHandle,
    pub(crate) archive: Option<ArchiveHistory>,
    pub(crate) store: Option<FollowerStore>,
}

impl StreamFeed {
    /// The first block from `from` on that `records` (sorted) do not
    /// hold without a hole.
    fn first_missing(records: &[L1Block], from: u64) -> u64 {
        records.iter().fold(from, |next, record| {
            next.saturating_add(u64::from(record.number == next))
        })
    }
}

#[async_trait]
impl BlockFeed for StreamFeed {
    async fn history(&mut self, from: u64) -> Vec<L1Block> {
        let mut records = match &self.archive {
            Some(archive) => archive.read(from).await,
            None => Vec::new(),
        };
        records.sort_by_key(|record| record.number);
        let missing = Self::first_missing(&records, from);
        if let Some(store) = &self.store {
            records.extend(store.read(missing).await);
            records.sort_by_key(|record| record.number);
        }
        tracing::info!(
            from,
            records = records.len(),
            first_missing = Self::first_missing(&records, from),
            "l1_blocks history read"
        );
        records
    }

    async fn next(&mut self) -> Option<L1Block> {
        self.live.recv().await.map(|(_, record)| record)
    }

    fn try_next(&mut self) -> Option<L1Block> {
        self.live.try_recv().map(|(_, record)| record)
    }
}

/// What opening the stream needs from the process.
pub(crate) struct StreamParts<'a> {
    pub(crate) rt: &'a AeronRuntime,
    pub(crate) plane: &'a mut StreamPlane,
    pub(crate) aeron_cfg: &'a AeronConfig,
    pub(crate) aeron_dir: Option<PathBuf>,
    pub(crate) indexer_url: Option<&'a str>,
    /// The node-local endpoints of the archive replay.
    pub(crate) response_endpoint: Option<&'a str>,
    pub(crate) replay_endpoint: Option<&'a str>,
}

impl StreamParts<'_> {
    /// Subscribe to `l1_blocks` and to the `events` board, and set up the
    /// two history sources the parts allow.
    pub(crate) fn open(self) -> anyhow::Result<(StreamFeed, watch::Receiver<BoardView>)> {
        let live = self
            .plane
            .subscriber::<L1BlocksSubscriberHandle>(self.rt)
            .context("open the l1_blocks subscription")?;
        let board = self
            .plane
            .subscriber::<ServiceEventsSubscriberHandle>(self.rt)
            .context("open the events subscription")?
            .spawn_board();
        let store = self.indexer_url.map(FollowerStore::connect).transpose()?;
        let archive = self.archive()?;
        if archive.is_none() && store.is_none() {
            tracing::warn!(
                "no l1_blocks archive and no --indexer-url: a start resumes only from the live \
                 stream, and waits for any record it missed"
            );
        }
        Ok((
            StreamFeed {
                live,
                archive,
                store,
            },
            board,
        ))
    }

    /// The archive history, when the archives and the node-local replay
    /// endpoints are known.
    fn archive(self) -> anyhow::Result<Option<ArchiveHistory>> {
        let endpoints = match self.plane.watch_archives() {
            Some(archives) => EndpointSource::Discovered {
                topic: Topic::L1Blocks,
                archives,
            },
            None => EndpointSource::Static(self.aeron_cfg.l1_blocks_archive_endpoints.clone()),
        };
        let (Some(response), Some(replay)) = (self.response_endpoint, self.replay_endpoint) else {
            tracing::warn!(
                "no --archive-control-response-endpoint or --replay-destination-endpoint: the \
                 l1_blocks archives are not read; the follower's archive is the only history"
            );
            return Ok(None);
        };
        if !endpoints.is_configured() {
            return Ok(None);
        }
        let cfg = RefetchConfig {
            tx_data_endpoints: EndpointSource::Static(Vec::new()),
            tx_deposits_endpoints: EndpointSource::Static(Vec::new()),
            response_endpoint: response.to_string(),
            replay_endpoint: replay.to_string(),
            aeron_dir: self.aeron_dir,
            aeron: self.aeron_cfg.clone(),
        };
        let stream_id = self.plane.channels().l1_blocks_stream_id;
        Ok(Some(
            ArchiveHistory::spawn(cfg, endpoints, stream_id).context("spawn the history thread")?,
        ))
    }
}
