//! [`TestEgress`]: an egress on a free port, and a reader of it as a binary
//! with `--egress` opens it.

use std::num::{NonZeroU64, NonZeroUsize};
use std::path::Path;
use std::time::Duration;

use crossbeam_channel::Sender;
use kardamom_cluster_adapter::wire::{CanonicalTx, encode_egress_boundary, encode_egress_record};
use kardamom_egress::drops::Drops;
use kardamom_egress::http::Http;
use kardamom_egress::merge::{Head, Merge, MergeConfig};
use kardamom_egress::owner::{FramePublisher, Input, Join, Owner, OwnerConfig, Parts};
use kardamom_egress::receipts::Receipts;
use kardamom_egress::store::Store;
use kardamom_types::{BlockBoundaryStart, TxEnvelope};

use crate::reader::carried::{CarriedSubscription, EgressTransport, NoSource};
use crate::reader::cluster::ReplayCursor;

/// The live frames of the egress go to no reader.
struct NoLive;

impl FramePublisher for NoLive {
    fn publish(&self, _frame: &[u8]) {}
}

/// An egress on a free port: a store, its owner thread, and its HTTP side.
pub struct TestEgress {
    /// The base URL, `http://host:port`.
    base: String,
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
        let store = Store::open(dir, NonZeroU64::new(10).unwrap(), genesis).unwrap();
        let merge = Merge::new(
            MergeConfig {
                max_waiting: NonZeroUsize::new(64).unwrap(),
                compared: NonZeroUsize::new(64).unwrap(),
            },
            store.head(),
        );
        let cfg = OwnerConfig {
            sync_every: Duration::from_millis(10),
            keep_blocks: NonZeroU64::new(1_000).unwrap(),
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
        frames.iter().zip(0u64..).for_each(|(frame, seq)| {
            let message = [seq.to_le_bytes().as_slice(), frame].concat();
            inputs
                .send(Input::Frame {
                    session: 1,
                    message,
                })
                .unwrap();
        });
        std::thread::spawn(move || owner.run(&rx));
        let http = rt
            .block_on(Http::bind("127.0.0.1:0".parse().unwrap(), inputs))
            .unwrap();
        let base = format!("http://{}", http.local_addr().unwrap());
        rt.spawn(http.serve());
        Self { base }
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
