use std::num::{NonZeroU64, NonZeroUsize};
use std::time::Duration;

use kardamom_cluster_adapter::wire::{encode_egress_boundary, encode_egress_record};

use super::Http;
use crate::merge::{Head, Merge, MergeConfig};
use crate::owner::{FramePublisher, Input, Owner, OwnerConfig};
use crate::store::Store;

struct NoLive;

impl FramePublisher for NoLive {
    fn publish(&self, _frame: &[u8]) {}
}

/// An egress with `frames` stored, on a free port. Returns its address.
async fn egress(dir: &std::path::Path, frames: &[Vec<u8>]) -> std::net::SocketAddr {
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
    let owner = Owner::new(
        OwnerConfig {
            sync_every: Duration::from_millis(10),
            keep_blocks: NonZeroU64::new(1_000).unwrap(),
        },
        merge,
        store,
        NoLive,
    );
    let (inputs, rx) = crossbeam_channel::bounded(64);
    frames.iter().zip(0u64..).for_each(|(frame, seq)| {
        inputs
            .send(Input::Frame {
                session: 1,
                message: [seq.to_le_bytes().as_slice(), frame].concat(),
            })
            .unwrap();
    });
    std::thread::spawn(move || owner.run(&rx));
    let http = Http::bind("127.0.0.1:0".parse().unwrap(), inputs)
        .await
        .unwrap();
    let addr = http.local_addr().unwrap();
    tokio::spawn(http.serve());
    addr
}

/// Split a body of `[len:u32][frame]` records.
fn frames_of(mut body: &[u8]) -> Vec<Vec<u8>> {
    std::iter::from_fn(|| {
        let (len, rest) = body.split_first_chunk::<4>()?;
        let (frame, rest) = rest.split_at(usize::try_from(u32::from_le_bytes(*len)).ok()?);
        body = rest;
        Some(frame.to_vec())
    })
    .collect()
}

#[tokio::test]
async fn a_range_read_answers_the_frames_after_the_cursor_and_the_head() {
    let dir = tempfile::tempdir().unwrap();
    let frames = vec![
        encode_egress_record(0, &[1; 40]).unwrap(),
        encode_egress_boundary(1, 1, 0, 0),
        encode_egress_record(1, &[2; 40]).unwrap(),
    ];
    let addr = egress(dir.path(), &frames).await;
    let response = reqwest::get(format!("http://{addr}/v1/frames?from_index=1&from_block=1"))
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let header = |name: &str| response.headers()[name].to_str().unwrap().to_owned();
    assert_eq!(header("x-egress-next-index"), "2");
    assert_eq!(header("x-egress-next-block"), "2");
    assert_eq!(header("x-egress-after-index"), "2");
    assert_eq!(header("x-egress-after-block"), "2");
    let body = response.bytes().await.unwrap();
    assert_eq!(frames_of(&body), frames[1..]);
}

#[tokio::test]
async fn a_bad_query_is_400_and_health_is_ok() {
    let dir = tempfile::tempdir().unwrap();
    let addr = egress(dir.path(), &[]).await;
    let bad = reqwest::get(format!("http://{addr}/v1/frames?from_index=x"))
        .await
        .unwrap();
    assert_eq!(bad.status(), 400);
    let health = reqwest::get(format!("http://{addr}/health")).await.unwrap();
    assert_eq!(health.status(), 200);
}
