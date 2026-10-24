//! The egress of the tests: an owner thread and its HTTP side on a free
//! port.

use std::net::SocketAddr;
use std::num::{NonZeroU64, NonZeroUsize};
use std::time::Duration;

use crossbeam_channel::Sender;

use crate::drops::Drops;
use crate::http::Http;
use crate::merge::{Head, Merge, MergeConfig};
use crate::owner::{FramePublisher, Input, Join, Owner, OwnerConfig, Parts};
use crate::receipts::Receipts;
use crate::store::Store;

/// The head of an empty chain.
pub(crate) const GENESIS: Head = Head {
    next_index: 0,
    next_block: 1,
    last: None,
};

struct NoLive;

impl FramePublisher for NoLive {
    fn publish(&self, _frame: &[u8]) {}
}

/// A running egress of a test: its address and its input queue.
pub(crate) struct TestEgress {
    pub(crate) addr: SocketAddr,
    pub(crate) inputs: Sender<Input>,
}

impl TestEgress {
    /// An egress on the store in `dir`, which joins the copies by `join`.
    pub(crate) async fn spawn(dir: &std::path::Path, join: Join) -> Self {
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
                join,
            },
            Parts {
                merge,
                store,
                receipts: Receipts::new(NonZeroUsize::new(16).unwrap()),
                drops: Drops::new(NonZeroUsize::new(16).unwrap()),
                live: NoLive,
                receipts_live: NoLive,
            },
        );
        let (inputs, rx) = crossbeam_channel::bounded(64);
        std::thread::spawn(move || owner.run(&rx));
        let http = Http::bind("127.0.0.1:0".parse().unwrap(), inputs.clone())
            .await
            .unwrap();
        let addr = http.local_addr().unwrap();
        tokio::spawn(http.serve());
        Self { addr, inputs }
    }

    /// Send `frames` as the copy `session`, from sequence `seq`.
    pub(crate) fn send_copy(&self, session: i32, seq: u64, frames: &[Vec<u8>]) {
        frames.iter().zip(seq..).for_each(|(frame, seq)| {
            self.inputs
                .send(Input::Frame {
                    session,
                    message: [seq.to_le_bytes().as_slice(), frame].concat(),
                })
                .unwrap();
        });
    }
}
