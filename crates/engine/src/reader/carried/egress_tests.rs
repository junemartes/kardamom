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
use crate::reader::carried::transport::{Poll, Request, Transport};

struct NoLive;

impl FramePublisher for NoLive {
    fn publish(&self, _frame: &[u8]) {}
}

/// An egress that holds `frames`, served on `rt`. Returns its base URL.
fn egress(rt: &tokio::runtime::Runtime, dir: &std::path::Path, frames: &[Vec<u8>]) -> String {
    let genesis = Head {
        next_index: 0,
        next_block: 1,
        last: None,
    };
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
        join: kardamom_egress::owner::Join::FromPeer,
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
    let base = egress(&rt, dir.path(), &stored);
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
