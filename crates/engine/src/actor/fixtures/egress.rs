//! [`TestEgress`]: an egress on a free port, and a reader of it as a binary
//! with `--egress` opens it.

use std::cell::Cell;
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::Path;
use std::time::Duration;

use crossbeam_channel::Sender;
use kardamom_cluster_adapter::wire::{CanonicalTx, encode_egress_boundary, encode_egress_record};
use kardamom_egress::drops::Drops;
use kardamom_egress::http::Http;
use kardamom_egress::merge::{Head, Merge, MergeConfig};
use kardamom_egress::owner::{FramePublisher, Input, Join, Owner, OwnerConfig, Parts, ReadRequest};
use kardamom_egress::receipts::Receipts;
use kardamom_egress::store::{Range, Store};
use kardamom_types::{BlockBoundaryStart, TxEnvelope};

use crate::reader::carried::{CarriedSubscription, EgressTransport, NoSource};
use crate::reader::cluster::ReplayCursor;

/// The live frames of the egress go to no reader.
struct NoLive;

impl FramePublisher for NoLive {
    fn publish(&self, _frame: &[u8]) {}
}

/// How long a [`TestEgress`] keeps its blocks. The store keeps every block
/// above the posted head, and the reader window below its head.
#[derive(Clone, Copy, Debug)]
pub struct Retention {
    /// The blocks of the reader window below the head of the store.
    pub keep_blocks: NonZeroU64,
    /// The blocks of one store segment. A prune removes whole segments.
    pub segment_blocks: NonZeroU64,
}

impl Retention {
    /// A window longer than the chain of any test, in 10-block segments.
    pub const LONG: Self = Self {
        keep_blocks: NonZeroU64::new(1_000).unwrap(),
        segment_blocks: NonZeroU64::new(10).unwrap(),
    };
}

/// An egress on a free port: a store, its owner thread, and its HTTP side.
/// It takes the frames of one executor copy.
pub struct TestEgress {
    /// The base URL, `http://host:port`.
    base: String,
    /// The input queue of the owner thread.
    inputs: Sender<Input>,
    /// The sequence number of the next frame of the copy.
    next_seq: Cell<u64>,
}

impl TestEgress {
    /// The head of an empty chain.
    pub const GENESIS: Head = Head {
        next_index: 0,
        next_block: 1,
        last: None,
    };

    /// Serve on `rt` a store in `dir` that starts at `genesis` and holds
    /// `frames` in order.
    ///
    /// # Panics
    ///
    /// Panics if the store does not open or the HTTP side does not bind.
    #[must_use]
    pub fn serve(
        rt: &tokio::runtime::Runtime,
        dir: &Path,
        genesis: Head,
        frames: &[Vec<u8>],
    ) -> Self {
        Self::serve_with(rt, dir, genesis, Retention::LONG, frames)
    }

    /// Serve on `rt` a store in `dir` that starts at `genesis`, keeps its
    /// blocks by `retention`, and holds `frames` in order. The store syncs
    /// and prunes every 10 ms.
    ///
    /// # Panics
    ///
    /// Panics if the store does not open or the HTTP side does not bind.
    #[must_use]
    pub fn serve_with(
        rt: &tokio::runtime::Runtime,
        dir: &Path,
        genesis: Head,
        retention: Retention,
        frames: &[Vec<u8>],
    ) -> Self {
        let store = Store::open(dir, retention.segment_blocks, genesis).unwrap();
        let merge = Merge::new(
            MergeConfig {
                max_waiting: NonZeroUsize::new(64).unwrap(),
                compared: NonZeroUsize::new(64).unwrap(),
            },
            store.head(),
        );
        let cfg = OwnerConfig {
            sync_every: Duration::from_millis(10),
            keep_blocks: retention.keep_blocks,
            join: Join::FromPeer,
        };
        let parts = Parts {
            merge,
            store,
            receipts: Receipts::new(NonZeroUsize::new(16).unwrap()),
            drops: Drops::new(NonZeroUsize::new(16).unwrap()),
            live: NoLive,
            receipts_live: NoLive,
        };
        let owner = Owner::new(cfg, parts);
        let (inputs, rx) = crossbeam_channel::bounded(64);
        std::thread::spawn(move || owner.run(&rx));
        let http = rt
            .block_on(Http::bind("127.0.0.1:0".parse().unwrap(), inputs.clone()))
            .unwrap();
        let base = format!("http://{}", http.local_addr().unwrap());
        rt.spawn(http.serve());
        let egress = Self {
            base,
            inputs,
            next_seq: Cell::new(0),
        };
        egress.append(frames);
        egress
    }

    /// The base URL of the HTTP side, `http://host:port`.
    #[must_use]
    pub fn base(&self) -> &str {
        &self.base
    }

    /// Deliver `frames` from the copy, after the frames before them.
    ///
    /// # Panics
    ///
    /// Panics if the owner thread is gone.
    pub fn append(&self, frames: &[Vec<u8>]) {
        frames.iter().for_each(|frame| {
            let seq = self.next_seq.get();
            self.next_seq.set(seq + 1);
            let message = [seq.to_le_bytes().as_slice(), frame].concat();
            self.inputs
                .send(Input::Frame {
                    session: 1,
                    message,
                })
                .unwrap();
        });
    }

    /// A post on L1 covers the blocks through `block`.
    ///
    /// # Panics
    ///
    /// Panics if the owner thread is gone.
    pub fn post(&self, block: u64) {
        self.inputs.send(Input::Posted(block)).unwrap();
    }

    /// The floor of the store: the oldest cursor that it serves, or `None`
    /// while it still serves the genesis cursor.
    ///
    /// # Panics
    ///
    /// Panics if the owner thread is gone or the store fails.
    #[must_use]
    pub fn floor(&self) -> Option<Head> {
        let (reply, range) = tokio::sync::oneshot::channel();
        self.inputs
            .send(Input::Read(ReadRequest {
                from_index: Self::GENESIS.next_index,
                from_block: Self::GENESIS.next_block,
                limit: 1,
                reply,
            }))
            .unwrap();
        match range.blocking_recv().unwrap().unwrap() {
            Range::Frames { .. } => None,
            Range::Unavailable { floor, .. } => Some(floor),
        }
    }

    /// A transport of this egress, and the live `egress_order` stream that
    /// it reads after a replay. Its range reads run on `rt`.
    pub(crate) fn transport(
        &self,
        rt: &tokio::runtime::Runtime,
    ) -> (EgressTransport, Sender<Vec<u8>>) {
        let (live, frames) = crossbeam_channel::unbounded();
        let transport = EgressTransport::new(self.base.clone(), frames, rt.handle().clone());
        (transport, live)
    }

    /// A reader of this egress from `cursor`, as a binary with `--egress`
    /// opens it: the egress is its only source. Its range reads run on
    /// `rt`.
    #[must_use]
    pub fn reader(&self, rt: &tokio::runtime::Runtime, cursor: ReplayCursor) -> EgressReader {
        let (transport, live) = self.transport(rt);
        EgressReader {
            sub: CarriedSubscription::new(transport, NoSource, cursor),
            live,
        }
    }

    /// The frame of `envelope` at the canonical index `index`, as the
    /// sealer relays it: the entry with its bytes, on lane 0.
    ///
    /// # Panics
    ///
    /// Panics if the envelope does not serialize.
    pub(super) fn tx_frame(index: u64, envelope: &TxEnvelope) -> Vec<u8> {
        let entry = CanonicalTx {
            canonical_id: envelope.tx_hash,
            lane: 0,
            envelope: envelope.clone(),
        };
        encode_egress_record(index, &entry.payload().unwrap()).unwrap()
    }

    /// The frame of the boundary `b`.
    pub(super) fn boundary_frame(b: &BlockBoundaryStart) -> Vec<u8> {
        encode_egress_boundary(
            b.block_number,
            b.end_tx_idx.as_index(),
            b.l2_timestamp,
            b.l1_origin,
        )
    }
}

/// A reader of a [`TestEgress`], and the live `egress_order` stream of the
/// egress. After its replay, the reader reads the live stream. When the
/// live stream closes, the reader stops with `TxOrderingClosed`.
pub struct EgressReader {
    pub sub: CarriedSubscription<EgressTransport, NoSource>,
    pub live: Sender<Vec<u8>>,
}
