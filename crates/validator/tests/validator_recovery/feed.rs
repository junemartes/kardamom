//! The live feed of one test, on the shared media driver: a real egress that
//! serves the canonical stream and publishes it on `egress_order`, and the
//! `tx_bal` and `tx_receipts` publications of one executor replica. A
//! feeder thread publishes one block of the chain at a time, at a fixed
//! pace, up to a target that the test moves.
//!
//! The feed has a subscriber of its own on each result stream. A deployed
//! executor does not wait for the validator: the ingresses and the
//! sequencers also read its receipts, and its BAL thread drops a frame
//! that has no subscriber. So the chain here keeps its pace while the
//! validator is frozen or gone, and a validator that starts joins each
//! stream at its live position: the results of the outage are lost to it.

use std::num::NonZeroU16;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;

use tokio::sync::mpsc::UnboundedReceiver;

use kardamom_log::aeron_live::{AeronRuntime, PubHandle, RawFrame, TxReceiptsPublisherHandle};
use kardamom_log::config::ChannelsConfig;
use kardamom_types::BalFrame;

use super::chain::Chain;
use super::egress::{Egress, Live};

/// The pace of the chain: one block per tick.
const TICK: Duration = Duration::from_millis(100);
/// The publications of one executor replica, and the egress input.
struct Outputs {
    bal: PubHandle,
    receipts: TxReceiptsPublisherHandle,
    egress: Egress,
}

impl Outputs {
    /// Publish block `index` of `chain`, as an executor replica does after
    /// it ran the block: the receipts, the BAL, then the canonical frames
    /// into the egress. While no validator subscribes, a publish fails as
    /// not connected; the executor then drops the frame after its
    /// deadline, and a later validator counts the block as unverified.
    fn block(&mut self, chain: &Chain, index: usize) {
        let block = &chain.blocks[index];
        block.receipts.iter().for_each(|batch| {
            let _ = self.receipts.publish_receipts(batch);
        });
        let _ = self.bal.publish(&BalFrame {
            delta: block.delta.clone(),
            bal_rlp: vec![alloy_rlp::EMPTY_LIST_CODE],
            granularity: NonZeroU16::MIN,
        });
        block
            .frames
            .iter()
            .for_each(|frame| self.egress.push(frame));
    }
}

/// The other subscribers of the result streams. Nothing reads the
/// frames: the subscriptions only keep each publication connected and
/// its position at the live end.
struct Taps {
    _bal: UnboundedReceiver<RawFrame>,
    _receipts: UnboundedReceiver<RawFrame>,
}

impl Taps {
    fn open(rt: &AeronRuntime, channels: &ChannelsConfig) -> Self {
        Self {
            _bal: Self::tap(rt, &channels.tx_bal_channel, channels.tx_bal_stream_id),
            _receipts: Self::tap(
                rt,
                &channels.tx_receipts_channel,
                channels.tx_receipts_stream_id,
            ),
        }
    }

    fn tap(rt: &AeronRuntime, channel: &str, stream: i32) -> UnboundedReceiver<RawFrame> {
        rt.open_subscription_raw(channel, stream).unwrap().1
    }
}

/// The running feed.
pub(crate) struct Feed {
    target: Arc<AtomicU64>,
    published: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
    pub(crate) egress_base: String,
    /// They close before the client of the test.
    _taps: Taps,
    /// The client of the test; it closes when the feeder has stopped.
    aeron: Option<AeronRuntime>,
    /// The runtime of the egress HTTP side; held for its life.
    _http: tokio::runtime::Runtime,
}

impl Feed {
    /// Start a client of the driver at `aeron_dir`, an egress with its
    /// store under `dir`, and a feeder of `chain`.
    pub(crate) fn start(dir: &Path, chain: Arc<Chain>, aeron_dir: &Path) -> Self {
        let http = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let rt = AeronRuntime::spawn_with_dir(aeron_dir).unwrap();
        let channels = ChannelsConfig::default();
        let live = rt
            .open_publication(
                &channels.egress_order_channel,
                channels.egress_order_stream_id,
            )
            .unwrap();
        let taps = Taps::open(&rt, &channels);
        let egress = Egress::start(&http, &dir.join("egress"), Live::Aeron(live));
        let egress_base = egress.base.clone();
        let outputs = Outputs {
            bal: rt
                .open_publication(&channels.tx_bal_channel, channels.tx_bal_stream_id)
                .unwrap(),
            receipts: TxReceiptsPublisherHandle::open(&rt, &channels).unwrap(),
            egress,
        };
        let target = Arc::new(AtomicU64::new(0));
        let published = Arc::new(AtomicU64::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let worker = Feeder {
            chain,
            outputs,
            target: target.clone(),
            published: published.clone(),
            stop: stop.clone(),
        }
        .spawn();
        Self {
            target,
            published,
            stop,
            worker: Some(worker),
            egress_base,
            _taps: taps,
            aeron: Some(rt),
            _http: http,
        }
    }

    /// Publish blocks up to `block`, at the pace of the chain.
    pub(crate) fn run_to(&self, block: u64) {
        self.target.store(block, Ordering::Release);
    }

    /// The client of the test closes here.
    fn close(_aeron: Option<AeronRuntime>) {}

    /// The newest block the feed published.
    pub(crate) fn head(&self) -> u64 {
        self.published.load(Ordering::Acquire)
    }
}

impl Drop for Feed {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
        Self::close(self.aeron.take());
    }
}

/// The feeder thread.
struct Feeder {
    chain: Arc<Chain>,
    outputs: Outputs,
    target: Arc<AtomicU64>,
    published: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
}

impl Feeder {
    fn spawn(self) -> JoinHandle<()> {
        std::thread::spawn(move || self.run())
    }

    fn run(mut self) {
        while !self.stop.load(Ordering::Acquire) {
            self.tick();
            std::thread::sleep(TICK);
        }
    }

    /// Publish the next block when the target is ahead of the head.
    fn tick(&mut self) {
        let head = self.published.load(Ordering::Acquire);
        if head >= self.target.load(Ordering::Acquire) {
            return;
        }
        self.outputs
            .block(&self.chain, usize::try_from(head).unwrap());
        self.published.store(head + 1, Ordering::Release);
    }
}
