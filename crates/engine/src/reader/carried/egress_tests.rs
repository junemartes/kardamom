use std::time::Duration;

use kardamom_cluster_adapter::wire::{
    EGRESS_KIND_REPLAY_DONE, encode_egress_boundary, encode_egress_record, encode_feed_answer,
};
use kardamom_egress::merge::Head;

use super::EgressTransport;
use crate::ExecutorError;
use crate::actor::fixtures::TestEgress;
use crate::reader::carried::answer::Timing;
use crate::reader::carried::memory::{MemoryRemote, MemoryTransport};
use crate::reader::carried::tests::{TIMING, boundary, label, tx_at, unavailable};
use crate::reader::carried::transport::{Poll, Request, Transport};
use crate::reader::carried::{CarriedSubscription, NoSource};
use crate::reader::cluster::ReplayCursor;

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
    let (mut t, live_tx) =
        TestEgress::serve(&rt, dir.path(), TestEgress::GENESIS, &stored).transport(&rt);
    let next_live = encode_egress_record(2, &[3; 40]).unwrap();
    live_tx.send(next_live.clone()).unwrap();
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

    fn new(rt: &tokio::runtime::Runtime, egress: &TestEgress, cursor: ReplayCursor) -> Self {
        let (feed, remote) = MemoryTransport::pair();
        let (service, live) = egress.transport(rt);
        Self {
            sub: CarriedSubscription::with_timing(feed, service, cursor, TIMING),
            feed: remote,
            _live: live,
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
    let egress = TestEgress::serve(&rt, dir.path(), TestEgress::GENESIS, &stored);
    let mut reader = Recovery::new(&rt, &egress, ReplayCursor::new(0, 1));
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
    let egress = TestEgress::serve(&rt, dir.path(), floor, &[]);
    let mut reader = Recovery::new(&rt, &egress, ReplayCursor::new(3, 2));
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

/// A reader of the egress alone, as the validator reads it.
struct Alone {
    sub: CarriedSubscription<EgressTransport, NoSource>,
    live: crossbeam_channel::Sender<Vec<u8>>,
}

impl Alone {
    fn new(rt: &tokio::runtime::Runtime, egress: &TestEgress, cursor: ReplayCursor) -> Self {
        Self::with_timing(rt, egress, cursor, TIMING)
    }

    fn with_timing(
        rt: &tokio::runtime::Runtime,
        egress: &TestEgress,
        cursor: ReplayCursor,
        timing: Timing,
    ) -> Self {
        let (transport, live) = egress.transport(rt);
        Self {
            sub: CarriedSubscription::with_timing(transport, NoSource, cursor, timing),
            live,
        }
    }

    /// Run the reader until it delivers `n` items, and label them.
    fn deliver(&mut self, n: usize) -> Vec<String> {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        let mut got = Vec::new();
        while got.len() < n {
            self.step(&mut got, deadline);
        }
        got
    }

    /// Take one step, and label what the reader delivers.
    fn step(&mut self, got: &mut Vec<String>, deadline: std::time::Instant) {
        assert!(std::time::Instant::now() < deadline, "got only {got:?}");
        self.sub.step().expect("a step of the reader");
        got.extend(std::iter::from_fn(|| self.sub.deliver().unwrap()).map(label));
    }
}

#[test]
fn a_live_record_of_the_egress_waits_for_its_boundary() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let stored = [tx_at(0), boundary(1, 1)];
    let egress = TestEgress::serve(&rt, dir.path(), TestEgress::GENESIS, &stored);
    let mut reader = Alone::new(&rt, &egress, ReplayCursor::new(0, 1));
    assert_eq!(reader.deliver(2), ["t0", "b1"]);
    reader.live.send(tx_at(1)).unwrap();
    (0..20).try_for_each(|_| reader.sub.step()).unwrap();
    assert_eq!(
        reader.sub.deliver().unwrap().map(label),
        None,
        "the live record waits for the boundary of its block"
    );
    reader.live.send(boundary(2, 2)).unwrap();
    assert_eq!(reader.deliver(2), ["t1", "b2"]);
}

/// The reader delivered the records of block 2 before the boundary of
/// block 1. Each range read from that cursor starts with the boundary of
/// block 1, so the run starts again from the state DB at the first range
/// read. The stall window of this reader is one hour, so the restart
/// cannot come from the stall watchdog.
#[test]
fn a_cursor_past_an_undelivered_boundary_restarts_the_run() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let stored = [
        tx_at(0),
        boundary(1, 1),
        tx_at(1),
        boundary(2, 2),
        tx_at(2),
        boundary(3, 3),
    ];
    let egress = TestEgress::serve(&rt, dir.path(), TestEgress::GENESIS, &stored);
    let timing = Timing {
        stall: Duration::from_secs(3600),
        ..TIMING
    };
    let mut reader = Alone::with_timing(&rt, &egress, ReplayCursor::new(2, 1), timing);
    match reader.sub.error() {
        ExecutorError::SourceRestarted {
            next_index,
            next_block,
        } => assert_eq!((next_index, next_block), (2, 1)),
        other => panic!("expected a restart from the state DB, got {other:?}"),
    }
}
