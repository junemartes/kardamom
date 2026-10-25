//! `kardamom-egress`: the stage after the executors. It merges the
//! `exec_frames` copies of every executor into one canonical stream, keeps
//! it on disk, publishes it live on `egress_order`, and answers the range
//! reads of the readers that resume. It merges the receipts of every
//! executor too, publishes them on `egress_receipts`, and answers
//! `eth_getTransactionReceipt`. It keeps the drops that the sequencers
//! report on `tx_drops`, and answers `kardamom_getTransactionStatus`. It
//! reads the posted head from the `l1_blocks` stream of the L1 follower,
//! and, with an oracle, the final head from L1, and keeps every block above
//! the lower of the two. See the library docs.

use std::net::SocketAddr;
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::PathBuf;
use std::time::Duration;

use alloy_primitives::Address;
use anyhow::{Context, Result};
use clap::Parser;
use kardamom_egress::catchup::CatchUp;
use kardamom_egress::drops::Drops;
use kardamom_egress::final_head::FinalHead;
use kardamom_egress::http::Http;
use kardamom_egress::l1_heads::L1Heads;
use kardamom_egress::merge::{Head, Merge, MergeConfig};
use kardamom_egress::owner::{FramePublisher, Input, Join, Owner, OwnerConfig, Parts};
use kardamom_egress::receipts::Receipts;
use kardamom_egress::store::Store;
use kardamom_log::aeron_live::{
    AeronRuntime, L1BlocksSubscriberHandle, PubHandle, TxDropsSubscriberHandle,
};
use kardamom_log::config::LogConfig;
use kardamom_log::discovery::StreamPlane;
use kardamom_obs::bin::wait_for_shutdown;

/// The inputs that may wait for the owner thread.
const INPUTS: usize = 64 * 1024;

/// The head of an empty chain: the first record index and the first block.
const GENESIS: Head = Head {
    next_index: 0,
    next_block: 1,
    last: None,
};

#[derive(Debug, Parser)]
#[command(
    name = "kardamom-egress",
    version,
    about = "kardamom egress: the canonical stream after the executors"
)]
struct Args {
    /// Optional `LogConfig` TOML supplying the Aeron `[channels]` config.
    #[arg(long, env = "KARDAMOM_LOG_CONFIG")]
    log_config: Option<PathBuf>,
    /// Aeron Media Driver directory (`aeron.dir`).
    #[arg(long)]
    aeron_dir: Option<PathBuf>,
    /// The directory of the stored stream.
    #[arg(
        long,
        env = "KARDAMOM_EGRESS_DIR",
        default_value = "/opt/kardamom/egress"
    )]
    dir: PathBuf,
    /// The address of the range read and the health check.
    #[arg(long, env = "KARDAMOM_EGRESS_BIND", default_value = "127.0.0.1:8548")]
    bind: SocketAddr,
    /// The blocks in one segment file.
    #[arg(long, env = "KARDAMOM_EGRESS_SEGMENT_BLOCKS", default_value = "1000")]
    segment_blocks: NonZeroU64,
    /// The blocks the store keeps below its head, whatever the L1 heads
    /// say: the window of the readers that resume. The default is one
    /// challenge window: 1 day at 2 s a block. The store also keeps every
    /// block above the lower of the posted head and the final head.
    #[arg(long, env = "KARDAMOM_EGRESS_KEEP_BLOCKS", default_value = "43200")]
    keep_blocks: NonZeroU64,
    /// The L1 JSON-RPC endpoint of the output oracle. Point it at the
    /// light client. It needs `--withdrawal-oracle`.
    #[arg(long, env = "KARDAMOM_L1_RPC_URL", requires = "withdrawal_oracle")]
    l1_rpc_url: Option<reqwest::Url>,
    /// The L1 address of `WithdrawalOutputOracle`. With it, the store keeps
    /// every block above the lower of the posted head and the final head.
    /// Without it, the store keeps every block above the posted head. It
    /// needs `--l1-rpc-url`.
    #[arg(long, env = "KARDAMOM_WITHDRAWAL_ORACLE", requires = "l1_rpc_url")]
    withdrawal_oracle: Option<Address>,
    /// The period of the disk sync, in milliseconds.
    #[arg(long, env = "KARDAMOM_EGRESS_SYNC_MS", default_value_t = 1_000)]
    sync_ms: u64,
    /// The frames of the copies ahead of the head that may wait.
    #[arg(long, env = "KARDAMOM_EGRESS_MAX_WAITING", default_value = "1000000")]
    max_waiting: NonZeroUsize,
    /// The delivered frames whose digests the egress keeps, to compare
    /// the copies.
    #[arg(long, env = "KARDAMOM_EGRESS_COMPARED", default_value = "1000000")]
    compared: NonZeroUsize,
    /// The newest receipts that the egress keeps for
    /// `eth_getTransactionReceipt`.
    #[arg(long, env = "KARDAMOM_EGRESS_RECEIPTS", default_value = "1000000")]
    receipts: NonZeroUsize,
    /// The newest drops of the sequencers that the egress keeps for
    /// `kardamom_getTransactionStatus`.
    #[arg(long, env = "KARDAMOM_EGRESS_DROPS", default_value = "1000000")]
    drops: NonZeroUsize,
    /// The executor replicas whose `tx_receipts` endpoints to attach under
    /// MDS. Falls back to `channels.tx_receipts_executor_count`.
    #[arg(long, env = "KARDAMOM_EXECUTOR_COUNT")]
    executor_count: Option<std::num::NonZeroU32>,
    /// Address for the Prometheus /metrics HTTP listener.
    #[arg(long, env = "KARDAMOM_METRICS_ADDR", default_value = "127.0.0.1:9013")]
    metrics_addr: SocketAddr,
    /// Host identifier. Stamped on every metric.
    #[arg(long, env = "KARDAMOM_HOST_ID", default_value = "local")]
    host_id: String,
    /// The other egress instances (`http://host:port`, comma-separated).
    /// After a start, the egress reads the frames between its head and
    /// the copies of the executors from one of them. The list may hold
    /// this instance too. Empty: the stream starts again after the next
    /// boundary of a copy.
    #[arg(long, env = "KARDAMOM_EGRESS_PEERS", value_delimiter = ',')]
    peers: Vec<String>,
    /// How long the peers may bring no frame before the stream starts
    /// again after the next boundary of a copy, in seconds.
    #[arg(long, env = "KARDAMOM_EGRESS_JOIN_TIMEOUT_S", default_value = "60")]
    join_timeout_secs: NonZeroU64,
}

impl Args {
    /// The watch of the final head, with an oracle, and the L1 heads that
    /// the owner starts from.
    fn final_head(
        &self,
        inputs: &crossbeam_channel::Sender<Input>,
    ) -> (Option<FinalHead>, L1Heads) {
        match (&self.l1_rpc_url, self.withdrawal_oracle) {
            (Some(url), Some(oracle)) => (
                Some(FinalHead::new(url.clone(), oracle, inputs.clone())),
                L1Heads::with_oracle(),
            ),
            _ => (None, L1Heads::posted_only()),
        }
    }
}

/// A live publication of the egress.
struct Live {
    handle: PubHandle,
    /// Whether every reader repairs a gap with the range read of the
    /// egress. Then a refused offer drops the frame at once: with no
    /// reader, the retry queue of the Aeron thread would hold each frame
    /// until its deadline, and each loop pass would check them all.
    repaired: bool,
}

impl Live {
    /// `egress_order`: a carried reader repairs a gap with the range read.
    fn order(handle: PubHandle) -> Self {
        Self {
            handle,
            repaired: true,
        }
    }

    /// `egress_receipts`: a receipt reader repairs no gap, so a frame
    /// waits for its subscriber until the offer deadline.
    fn receipts(handle: PubHandle) -> Self {
        Self {
            handle,
            repaired: false,
        }
    }
}

impl FramePublisher for Live {
    fn publish(&self, frame: &[u8]) {
        let mut bytes = rkyv::util::AlignedVec::with_capacity(frame.len());
        bytes.extend_from_slice(frame);
        if self.repaired {
            self.handle.publish_lossy(bytes);
        } else {
            self.handle.publish_best_effort(bytes);
        }
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<()> {
    kardamom_obs::bin::init_tracing();
    let args = Args::parse();
    kardamom_obs::init_service!("egress", args.metrics_addr, args.host_id.as_str()).await?;
    kardamom_egress::metrics::describe();

    let log_cfg = LogConfig::resolve(args.log_config.as_deref()).context("resolve log config")?;
    let mut plane = StreamPlane::from_config(&log_cfg, &format!("egress-{}", args.host_id))
        .context("build the stream plane")?;
    let rt = AeronRuntime::spawn(args.aeron_dir.as_deref()).context("spawn AeronRuntime")?;
    let frames = plane
        .exec_frames_subscription(&rt)
        .context("open exec_frames")?;
    let live = plane
        .egress_order_publisher(&rt)
        .await
        .context("open egress_order")?;
    let executor_count = args
        .executor_count
        .or(plane.channels().tx_receipts_executor_count);
    let receipt_frames = plane
        .tx_receipts_subscriber(&rt, executor_count)
        .context("open tx_receipts")?
        .into_receiver();
    let receipts_live = plane
        .egress_receipts_publisher(&rt)
        .await
        .context("open egress_receipts")?;
    let drops = plane.tx_drops_subscriber(&rt).context("open tx_drops")?;
    let l1_blocks = plane
        .subscriber::<L1BlocksSubscriberHandle>(&rt)
        .context("open l1_blocks")?;

    let store = Store::open(&args.dir, args.segment_blocks, GENESIS).context("open the store")?;
    tracing::info!(head = ?store.head(), mark = ?store.mark(), dir = %args.dir.display(), "egress: store open");
    let merge = Merge::new(
        MergeConfig {
            max_waiting: args.max_waiting,
            compared: args.compared,
        },
        store.head(),
    );
    let (inputs, rx) = crossbeam_channel::bounded(INPUTS);
    let (final_head, heads) = args.final_head(&inputs);
    let catch_up = CatchUp::new(
        args.peers,
        inputs.clone(),
        Duration::from_secs(args.join_timeout_secs.get()),
    );
    let join = if catch_up.is_some() {
        Join::FromPeer
    } else {
        Join::AtBoundary
    };
    let owner = Owner::new(
        OwnerConfig {
            sync_every: Duration::from_millis(args.sync_ms),
            keep_blocks: args.keep_blocks,
            join,
        },
        Parts {
            merge,
            store,
            receipts: Receipts::new(args.receipts),
            drops: Drops::new(args.drops),
            live: Live::order(live),
            receipts_live: Live::receipts(receipts_live),
        },
    )
    .with_heads(heads);
    let owner_thread = std::thread::Builder::new()
        .name("egress-owner".into())
        .spawn(move || owner.run(&rx))
        .context("spawn the owner thread")?;
    Feeds {
        frames,
        receipt_frames,
        drops,
        l1_blocks,
    }
    .forward(&inputs)?;

    if let Some(task) = catch_up {
        tokio::spawn(task.run());
    }
    if let Some(task) = final_head {
        tokio::spawn(task.run());
    }
    let http = Http::bind(args.bind, inputs)
        .await
        .context("bind the range read")?;
    tracing::info!(addr = %http.local_addr()?, "kardamom-egress starting");
    let server = tokio::spawn(http.serve());

    wait_for_shutdown().await;
    tracing::info!("kardamom-egress: shutdown signal received");
    server.abort();
    plane.shutdown().await;
    drop_runtime(rt);
    match owner_thread.join() {
        Ok(result) => result.context("the owner thread"),
        Err(_) => anyhow::bail!("the owner thread panicked"),
    }
}

/// The streams that feed the owner thread.
struct Feeds {
    frames: tokio::sync::mpsc::UnboundedReceiver<kardamom_log::aeron_live::RawFrame>,
    receipt_frames: kardamom_log::aeron_live::TxReceiptsReceiver,
    drops: TxDropsSubscriberHandle,
    l1_blocks: L1BlocksSubscriberHandle,
}

impl Feeds {
    /// Start one thread for each stream. Each thread puts the inputs of
    /// its stream on `inputs`.
    fn forward(self, inputs: &crossbeam_channel::Sender<Input>) -> Result<()> {
        let Self {
            frames,
            receipt_frames,
            drops,
            l1_blocks,
        } = self;
        let handle = tokio::runtime::Handle::current();
        let to = inputs.clone();
        Self::spawn("egress-frames", move || forward_frames(frames, &to))?;
        let (to, rt) = (inputs.clone(), handle.clone());
        Self::spawn("egress-receipts", move || {
            forward_receipts(receipt_frames, &to, &rt);
        })?;
        let (to, rt) = (inputs.clone(), handle.clone());
        Self::spawn("egress-drops", move || forward_drops(drops, &to, &rt))?;
        let to = inputs.clone();
        Self::spawn("egress-posted", move || {
            forward_posted(l1_blocks, &to, &handle);
        })
    }

    fn spawn(name: &str, body: impl FnOnce() + Send + 'static) -> Result<()> {
        std::thread::Builder::new()
            .name(name.into())
            .spawn(body)
            .with_context(|| format!("spawn the thread {name}"))?;
        Ok(())
    }
}

/// Put each `exec_frames` message on the owner's queue, until the
/// subscription closes or the owner is gone. A full queue holds this
/// thread, and Aeron then holds the executors' frames.
fn forward_frames(
    mut frames: tokio::sync::mpsc::UnboundedReceiver<kardamom_log::aeron_live::RawFrame>,
    inputs: &crossbeam_channel::Sender<Input>,
) {
    let _ = std::iter::from_fn(|| frames.blocking_recv())
        .map(|frame| Input::Frame {
            session: frame.session,
            message: frame.bytes,
        })
        .try_for_each(|input| inputs.send(input));
}

/// Put each receipt frame of the executors on the owner's queue, until the
/// subscription closes or the owner is gone.
fn forward_receipts(
    mut frames: kardamom_log::aeron_live::TxReceiptsReceiver,
    inputs: &crossbeam_channel::Sender<Input>,
    handle: &tokio::runtime::Handle,
) {
    let _ = std::iter::from_fn(|| handle.block_on(frames.recv_frame()))
        .try_for_each(|(_, frame)| inputs.send(Input::Receipts(frame)));
}

/// Put each drop of the sequencers on the owner's queue, until the
/// subscription closes or the owner is gone.
fn forward_drops(
    mut drops: TxDropsSubscriberHandle,
    inputs: &crossbeam_channel::Sender<Input>,
    handle: &tokio::runtime::Handle,
) {
    let _ = std::iter::from_fn(|| handle.block_on(drops.recv()))
        .try_for_each(|(_, drop)| inputs.send(Input::Drop(drop)));
}

/// Put the posted head of each `l1_blocks` record that carries a post on
/// the owner's queue, until the subscription closes or the owner is gone.
/// The follower publishes only finalized L1 blocks.
fn forward_posted(
    mut blocks: L1BlocksSubscriberHandle,
    inputs: &crossbeam_channel::Sender<Input>,
    handle: &tokio::runtime::Handle,
) {
    let _ = std::iter::from_fn(|| handle.block_on(blocks.recv()))
        .filter_map(|(_, record)| record.batches.iter().map(|b| b.l2_block_end).max())
        .try_for_each(|block| inputs.send(Input::Posted(block)));
}

/// End the Aeron runtime: the subscription closes, so the frames thread
/// ends, and with it the last sender of the owner's queue.
fn drop_runtime(_rt: AeronRuntime) {}
