//! Real-Aeron check of the executor-stream recording:
//!
//! 1. two exclusive IPC publications on one driver have their own sessions;
//! 2. the archive records the session of one of them, and the recorder
//!    reports recording positions that reach the end of every frame of
//!    that session, with no frame of the other;
//! 3. when the publication closes, the recorder ends with an error.
//!
//! Gated on the `docker-e2e` feature and on Docker availability.

#![cfg(feature = "docker-e2e")]

use std::sync::mpsc;
use std::time::{Duration, Instant};

use kardamom_log::recorder::{
    ARCHIVE_CONNECT_TIMEOUT, PositionReport, RecordedStream, RecorderKind, record_stream_reporting,
};
use kardamom_log::testing::{AeronTestCluster, SingleNodeRig};
use rkyv::util::AlignedVec;
use tokio_util::sync::CancellationToken;

/// Stream id 5401 keeps this test apart from the other e2e tests.
const STREAM: i32 = 5401;
const CHANNEL: &str = "aeron:ipc?alias=exec-txs-e2e";
const FRAMES: usize = 50;

fn frame(fill: u8) -> AlignedVec {
    let mut v = AlignedVec::new();
    v.extend_from_slice(&[fill; 200]);
    v
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker; run with `cargo test -p kardamom-log --features docker-e2e --test exec_txs_recording -- --ignored`"]
async fn the_recording_position_reaches_the_end_of_every_offered_frame() {
    // A short driver timeout gives the recorder the floor of its loss
    // wait, 10 s, so the loss check below stays short.
    // SAFETY: no thread of this test binary reads the environment yet.
    unsafe { std::env::set_var("AERON_DRIVER_TIMEOUT", "5000") };
    kardamom_log::testing::require_docker().await;
    let SingleNodeRig { cluster, rt, cfg } = AeronTestCluster::single_node_runtime(5401).await;
    let aeron_dir = cluster.aeron_dir_host(0).to_path_buf();
    let publication = rt
        .open_exclusive_publication(CHANNEL, STREAM)
        .expect("publication");
    let other = rt
        .open_exclusive_publication(CHANNEL, STREAM)
        .expect("a second publication");
    let session_id = publication.session_id();
    assert_ne!(session_id, other.session_id(), "each has its own session");

    let stop = CancellationToken::new();
    let (ready_tx, ready_rx) = mpsc::channel();
    let (positions_tx, positions_rx) = mpsc::channel();
    let recorder = {
        let stop = stop.clone();
        let aeron_cfg = cfg.aeron.clone();
        std::thread::spawn(move || {
            record_stream_reporting(
                Some(&aeron_dir),
                &aeron_cfg,
                RecordedStream {
                    channel: CHANNEL,
                    stream_id: STREAM,
                    kind: RecorderKind::ExecTxs { session_id },
                    connect_timeout: ARCHIVE_CONNECT_TIMEOUT,
                },
                &stop,
                |outcome| ready_tx.send(outcome).expect("ready"),
                PositionReport {
                    every: Duration::from_millis(20),
                    send: |position| {
                        let _ = positions_tx.send(position);
                    },
                },
            )
        })
    };
    ready_rx
        .recv_timeout(Duration::from_secs(30))
        .expect("a readiness report")
        .expect("the recording of this session starts");

    let ends: Vec<i64> = (0..FRAMES)
        .map(|i| {
            let fill = u8::try_from(i).expect("below 256");
            other
                .publish_bytes(frame(0xEE))
                .expect("publish on the other");
            let end = publication.publish_bytes(frame(fill)).expect("publish");
            publication.stream_position(end).expect("a raw position")
        })
        .collect();
    assert!(ends.is_sorted(), "ends rise: {ends:?}");
    let last = *ends.last().expect("frames");

    let deadline = Instant::now() + Duration::from_secs(10);
    let reached = std::iter::from_fn(|| {
        let left = deadline.saturating_duration_since(Instant::now());
        positions_rx.recv_timeout(left).ok()
    })
    .inspect(|&position| assert!(position <= last, "{position} passes the last end {last}"))
    .any(|position| position == last);
    assert!(reached, "the recording position never reached {last}");

    // Closing the runtime closes the publications, so the recording stops.
    // The recorder waits its loss budget (the driver timeout plus a
    // margin, at least 10 s) before it gives up.
    drop((publication, other, rt));
    let closed = Instant::now();
    let outcome = recorder.join().expect("no panic");
    assert!(
        closed.elapsed() >= Duration::from_secs(10),
        "the recorder waited only {:?}",
        closed.elapsed()
    );
    let err = outcome.expect_err("a stopped recording ends the recorder");
    assert!(err.to_string().contains("is lost"), "got {err}");
    stop.cancel();
}
