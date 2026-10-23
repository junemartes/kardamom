use std::num::{NonZeroU64, NonZeroUsize};
use std::time::Duration;

use kardamom_cluster_adapter::wire::{
    EGRESS_KIND_REPLAY_DONE, encode_egress_boundary, encode_egress_record, encode_feed_answer,
};
use kardamom_egress::http::Http;
use kardamom_egress::merge::{Head, Merge, MergeConfig};
use kardamom_egress::owner::{FramePublisher, Input, Owner, OwnerConfig, Parts};
use kardamom_egress::receipts::Receipts;
use kardamom_egress::store::Store;

use super::EgressTransport;
use crate::ExecutorError;
use crate::reader::carried::CarriedSubscription;
use crate::reader::carried::memory::{MemoryRemote, MemoryTransport};
use crate::reader::carried::tests::{TIMING, boundary, label, tx_at, unavailable};
use crate::reader::carried::transport::{Poll, Request, Transport};
use crate::reader::cluster::ReplayCursor;

struct NoLive;

impl FramePublisher for NoLive {
    fn publish(&self, _frame: &[u8]) {}
}

/// The head of an empty chain.
const GENESIS: Head = Head {
    next_index: 0,
    next_block: 1,
    last: None,
};

/// An egress whose store starts at `genesis` and holds `frames`, served on
/// `rt`. Returns its base URL.
fn egress(
    rt: &tokio::runtime::Runtime,
    dir: &std::path::Path,
    genesis: Head,
    frames: &[Vec<u8>],
) -> String {
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
    };
    let parts = Parts {
        merge,
        store,
        receipts: Receipts::new(NonZeroUsize::new(16).unwrap()),
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
    base
}

/// Poll `t` until it sends `n` frames.
fn frames(t: &mut EgressTransport, n: usize) -> Vec<Vec<u8>> {
    std::iter::repeat_with(|| t.poll(Duration::from_millis(50)))
        .take(200)
        .filter_map(|poll| match poll {
            Poll::Frame(frame) => Some(frame),
            Poll::Idle | Poll::Closed => None,
        })
        .take(n)
        .collect()
}

#[test]
fn a_request_replays_the_store_answers_done_then_reads_live() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let stored = vec![
        encode_egress_record(0, &[1; 40]).unwrap(),
        encode_egress_boundary(1, 1, 0, 0),
        encode_egress_record(1, &[2; 40]).unwrap(),
    ];
    let base = egress(&rt, dir.path(), GENESIS, &stored);
    let (live_tx, live) = crossbeam_channel::unbounded();
    let next_live = encode_egress_record(2, &[3; 40]).unwrap();
    live_tx.send(next_live.clone()).unwrap();
    let mut t = EgressTransport::new(base, live, rt.handle().clone());
    assert_eq!(t.connection(), Some(1));
    t.request(Request {
        from_index: 0,
        from_block: 1,
        request_id: 7,
    });
    let mut want = stored;
    want.push(encode_feed_answer(EGRESS_KIND_REPLAY_DONE, 2, 2, 7));
    want.push(next_live);
    assert_eq!(frames(&mut t, 5), want, "the replay, its answer, then live");
}

#[test]
fn a_failed_read_counts_a_new_connection() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (_live_tx, live) = crossbeam_channel::unbounded();
    let mut t = EgressTransport::new("http://127.0.0.1:1".into(), live, rt.handle().clone());
    t.request(Request {
        from_index: 0,
        from_block: 1,
        request_id: 1,
    });
    let changed = std::iter::repeat_with(|| {
        t.poll(Duration::from_millis(50));
        t.connection()
    })
    .take(100)
    .any(|c| c == Some(2));
    assert!(changed, "the reader sees a new connection and asks again");
}

/// A reader of a member feed that refuses every cursor below its window,
/// with the egress as its second source.
struct Recovery {
    sub: CarriedSubscription<MemoryTransport, EgressTransport>,
    feed: MemoryRemote,
    /// The live `egress_order` stays open while the reader runs.
    _live: crossbeam_channel::Sender<Vec<u8>>,
}

impl Recovery {
    /// The window of the member starts at index 20, block 8.
    const WINDOW: (u64, u64) = (20, 8);

    fn new(rt: &tokio::runtime::Runtime, base: String, cursor: ReplayCursor) -> Self {
        let (feed, remote) = MemoryTransport::pair();
        let (live_tx, live) = crossbeam_channel::unbounded();
        let egress = EgressTransport::new(base, live, rt.handle().clone());
        Self {
            sub: CarriedSubscription::with_timing(feed, egress, cursor, TIMING),
            feed: remote,
            _live: live_tx,
        }
    }

    /// Run the reader until it delivers `n` items, or fails. The member
    /// refuses each request.
    fn run(&mut self, n: usize) -> Result<Vec<String>, ExecutorError> {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        let mut got = Vec::new();
        while got.len() < n {
            self.step(&mut got, deadline)?;
        }
        Ok(got)
    }

    /// Answer the requests to the member, take one step, and label what the
    /// reader delivers.
    fn step(
        &mut self,
        got: &mut Vec<String>,
        deadline: std::time::Instant,
    ) -> Result<(), ExecutorError> {
        assert!(std::time::Instant::now() < deadline, "the reader stalled");
        let (index, block) = Self::WINDOW;
        self.feed
            .requests()
            .iter()
            .for_each(|r| self.feed.send(unavailable(r.request_id, index, block)));
        self.sub.step()?;
        let delivered =
            std::iter::from_fn(|| self.sub.deliver().transpose()).collect::<Result<Vec<_>, _>>()?;
        got.extend(delivered.into_iter().map(label));
        Ok(())
    }
}

#[test]
fn a_cursor_below_the_window_recovers_from_the_egress() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let stored = [tx_at(0), boundary(1, 1), tx_at(1), boundary(2, 2)];
    let base = egress(&rt, dir.path(), GENESIS, &stored);
    let mut reader = Recovery::new(&rt, base, ReplayCursor::new(0, 1));
    assert_eq!(
        reader.run(4).expect("no peer checkpoint"),
        ["t0", "b1", "t1", "b2"],
        "the egress serves the cursor that the member refuses"
    );
}

#[test]
fn a_cursor_below_the_floor_of_the_egress_goes_to_a_peer_checkpoint() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let floor = Head {
        next_index: 10,
        next_block: 5,
        last: None,
    };
    let base = egress(&rt, dir.path(), floor, &[]);
    let mut reader = Recovery::new(&rt, base, ReplayCursor::new(3, 2));
    match reader.run(1) {
        Err(ExecutorError::ClusterReplayUnavailable {
            from_index,
            oldest_index,
            oldest_block,
        }) => assert_eq!(
            (from_index, oldest_index, oldest_block),
            (3, 10, 5),
            "the peer checkpoint starts at the lower floor: the floor of the egress"
        ),
        other => panic!("expected the peer-checkpoint error, got {other:?}"),
    }
}
