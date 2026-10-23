use std::time::Duration;

use kardamom_cluster_adapter::wire::{encode_egress_boundary, encode_egress_record};
use tokio::sync::oneshot;

use super::CatchUp;
use crate::merge::Head;
use crate::owner::{Input, Join, JoinState};
use crate::test_support::TestEgress;

fn record(index: u64) -> Vec<u8> {
    encode_egress_record(index, &[u8::try_from(index).unwrap(); 40]).unwrap()
}

/// The canonical stream of the tests: two blocks of two records.
fn stream() -> Vec<Vec<u8>> {
    vec![
        record(0),
        record(1),
        encode_egress_boundary(1, 2, 0, 0),
        record(2),
        record(3),
        encode_egress_boundary(2, 4, 0, 0),
    ]
}

async fn state(egress: &TestEgress) -> JoinState {
    let (reply, answer) = oneshot::channel();
    egress.inputs.send(Input::State(reply)).unwrap();
    answer.await.unwrap()
}

/// The frames after genesis that `egress` serves.
async fn served(egress: &TestEgress) -> Vec<Vec<u8>> {
    let body = reqwest::get(format!(
        "http://{}/v1/frames?from_index=0&from_block=1",
        egress.addr
    ))
    .await
    .unwrap()
    .bytes()
    .await
    .unwrap();
    super::split(&body)
}

/// A restarted egress with an empty store sees only the late frames of a
/// copy. The task reads the place before them from a peer, the copy
/// chains behind it, and the task ends.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restarted_egress_joins_the_copies_from_a_peer() {
    let (peer_dir, dir) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let peer = TestEgress::spawn(peer_dir.path(), Join::FromPeer).await;
    peer.send_copy(1, 0, &stream());
    let restarted = TestEgress::spawn(dir.path(), Join::FromPeer).await;
    restarted.send_copy(1, 50, &stream()[3..]);
    assert!(!state(&restarted).await.joined);

    let task = CatchUp::new(
        vec![format!("http://{}", peer.addr)],
        restarted.inputs.clone(),
        Duration::from_secs(10),
    )
    .unwrap();
    tokio::time::timeout(Duration::from_secs(10), task.run())
        .await
        .expect("the task ends once the copies join");

    assert!(state(&restarted).await.joined);
    assert_eq!(served(&restarted).await, stream());
    restarted.send_copy(1, 53, &[record(4)]);
    assert_eq!(
        state(&restarted).await.head.next_index,
        5,
        "the copy chains"
    );
}

/// A peer that does not answer brings no frame. After the timeout the
/// task gives up, and the next boundary of a copy starts the stream.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn with_no_answer_the_task_gives_up_and_a_boundary_starts_the_stream() {
    let dir = tempfile::tempdir().unwrap();
    let restarted = TestEgress::spawn(dir.path(), Join::FromPeer).await;
    let task = CatchUp::new(
        vec!["http://127.0.0.1:9".to_string()],
        restarted.inputs.clone(),
        Duration::from_millis(300),
    )
    .unwrap();
    tokio::time::timeout(Duration::from_secs(10), task.run())
        .await
        .expect("the task gives up");
    restarted.send_copy(1, 50, &[record(10), encode_egress_boundary(5, 11, 0, 0)]);
    let start = Head::after_boundary(&encode_egress_boundary(5, 11, 0, 0)).unwrap();
    assert_eq!(state(&restarted).await.head, start);
}

#[test]
fn no_peer_makes_no_task() {
    let (inputs, _rx) = crossbeam_channel::bounded(1);
    assert!(CatchUp::new(Vec::new(), inputs, Duration::from_secs(1)).is_none());
}
