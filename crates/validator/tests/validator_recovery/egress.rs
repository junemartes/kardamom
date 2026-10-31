//! A real egress of the tests: its store, its owner thread and its HTTP
//! side on a free port. With a live publication, the owner also publishes
//! the merged stream on `egress_order`, as the deployed egress does.

use std::num::{NonZeroU64, NonZeroUsize};
use std::path::Path;
use std::time::Duration;

use crossbeam_channel::Sender;
use kardamom_egress::drops::Drops;
use kardamom_egress::http::Http;
use kardamom_egress::merge::{Head, Merge, MergeConfig};
use kardamom_egress::owner::{FramePublisher, Input, Join, Owner, OwnerConfig, Parts};
use kardamom_egress::receipts::Receipts;
use kardamom_egress::store::Store;
use kardamom_log::aeron_live::PubHandle;

/// The head of an empty chain.
const GENESIS: Head = Head {
    next_index: 0,
    next_block: 1,
    last: None,
};

/// Where the egress publishes its merged stream live.
pub(crate) enum Live {
    /// The `egress_order` stream on the media driver.
    Aeron(PubHandle),
    /// No live reader: the receipt publication of these tests.
    Nobody,
}

impl FramePublisher for Live {
    fn publish(&self, frame: &[u8]) {
        match self {
            Self::Aeron(handle) => {
                let mut bytes = rkyv::util::AlignedVec::with_capacity(frame.len());
                bytes.extend_from_slice(frame);
                handle.publish_lossy(bytes);
            }
            Self::Nobody => {}
        }
    }
}

/// A running egress: its input queue and its base URL.
pub(crate) struct Egress {
    inputs: Sender<Input>,
    pub(crate) base: String,
    /// The sequence number of the next frame of the one executor copy.
    seq: u64,
}

impl Egress {
    /// Start an egress with its store under `dir`, its HTTP side on
    /// `http`, and its live publication `live`.
    pub(crate) fn start(http: &tokio::runtime::Runtime, dir: &Path, live: Live) -> Self {
        let store = Store::open(dir, NonZeroU64::new(10).unwrap(), GENESIS).unwrap();
        let merge = Merge::new(
            MergeConfig {
                max_waiting: NonZeroUsize::new(64).unwrap(),
                compared: NonZeroUsize::new(64).unwrap(),
            },
            store.head(),
        );
        let owner = Owner::new(
            OwnerConfig {
                sync_every: Duration::from_millis(10),
                keep_blocks: NonZeroU64::new(1_000).unwrap(),
                join: Join::FromPeer,
            },
            Parts {
                merge,
                store,
                receipts: Receipts::new(NonZeroUsize::new(16).unwrap()),
                drops: Drops::new(NonZeroUsize::new(16).unwrap()),
                live,
                receipts_live: Live::Nobody,
            },
        );
        let (inputs, rx) = crossbeam_channel::bounded(1024);
        std::thread::spawn(move || owner.run(&rx));
        let server = http
            .block_on(Http::bind("127.0.0.1:0".parse().unwrap(), inputs.clone()))
            .unwrap();
        let base = format!("http://{}", server.local_addr().unwrap());
        http.spawn(server.serve());
        Self {
            inputs,
            base,
            seq: 0,
        }
    }

    /// Hand one canonical frame of the executor copy to the owner.
    pub(crate) fn push(&mut self, frame: &[u8]) {
        let message = [self.seq.to_le_bytes().as_slice(), frame].concat();
        self.seq += 1;
        self.inputs
            .send(Input::Frame {
                session: 1,
                message,
            })
            .unwrap();
    }
}
