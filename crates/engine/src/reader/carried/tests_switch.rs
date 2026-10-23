//! The order of the sources and the switch between them: the silence, the
//! lost connection, the hash mismatch, the floors, and the return to the
//! feed.

use std::time::Duration;

use kardamom_cluster_adapter::wire::{EGRESS_KIND_REPLAY_AHEAD, encode_feed_answer};

use super::tests::{Rig, TIMING, boundary, done, entry, forged_at, tx_at, unavailable};
use super::*;

/// Serve one block on `source` from its newest request: the entry at
/// `index` and the boundary of `block`, then the replay end.
fn serve_block(rig: &mut Rig, source: Source, index: u64, block: u64) -> Request {
    let request = rig.request(source);
    let remote = rig.remote(source);
    remote.send(tx_at(index));
    remote.send(boundary(block, index + 1));
    remote.send(done(request.request_id, index + 1, block + 1));
    request
}

/// Run `sub` until it fails, and answer each request of `remote` with the
/// frame that `answer` makes from the request id: a member that refuses
/// each request, also the probe of the return check.
fn fail_answering<S: Transport>(
    sub: &mut CarriedSubscription<MemoryTransport, S>,
    remote: &MemoryRemote,
    answer: impl Fn(u64) -> Vec<u8>,
) -> ExecutorError {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    std::iter::repeat_with(|| {
        assert!(
            std::time::Instant::now() < deadline,
            "the reader did not fail"
        );
        remote
            .requests()
            .iter()
            .for_each(|request| remote.send(answer(request.request_id)));
        sub.step()
    })
    .find_map(Result::err)
    .expect("an error")
}

#[test]
fn a_silent_feed_switches_to_the_service_at_a_frame_boundary() {
    let mut rig = Rig::new(0, 1);
    serve_block(&mut rig, Source::Feed, 0, 1);
    assert_eq!(rig.deliver(5), ["t0", "b1"]);
    rig.feed.send(tx_at(1));
    let request = rig.request(Source::Service);
    assert_eq!(rig.sub.source(), Source::Service);
    assert_eq!(
        (request.from_index, request.from_block),
        (1, 2),
        "the new source replays from the cursor"
    );
    rig.service.send(tx_at(1));
    rig.service.send(boundary(2, 2));
    rig.service.send(done(request.request_id, 2, 3));
    assert_eq!(
        rig.deliver(5),
        ["t1", "b2"],
        "the record of the old source waits for its boundary and goes with the switch"
    );
}

#[test]
fn a_lost_feed_switches_at_once() {
    let mut rig = Rig::new(0, 1);
    rig.request(Source::Feed);
    rig.feed.set_connected(false);
    let started = std::time::Instant::now();
    rig.run_until(|rig| rig.sub.source() == Source::Service);
    assert!(
        started.elapsed() < TIMING.silence,
        "a lost image does not wait for the silence"
    );
    assert_eq!(rig.sub.source(), Source::Service);
}

#[test]
fn the_reader_comes_back_to_the_feed_after_the_ready_window() {
    let mut rig = Rig::new(0, 1);
    rig.request(Source::Feed);
    rig.feed.set_connected(false);
    serve_block(&mut rig, Source::Service, 0, 1);
    assert_eq!(rig.deliver(5), ["t0", "b1"]);
    rig.feed.resume();
    let probe = rig.request(Source::Feed);
    assert_eq!(
        rig.sub.source(),
        Source::Service,
        "the probe does not switch"
    );
    assert_eq!((probe.from_index, probe.from_block), (1, 2));
    rig.feed.send(tx_at(1));
    rig.feed.send(done(probe.request_id, 2, 2));
    let request = rig.request(Source::Feed);
    assert_ne!(request.request_id, probe.request_id);
    assert_eq!(rig.sub.source(), Source::Feed);
    assert_eq!((request.from_index, request.from_block), (1, 2));
    assert_eq!(
        rig.deliver(1),
        [] as [String; 0],
        "probe frames are dropped"
    );
}

#[test]
fn a_break_in_the_readiness_starts_the_window_again() {
    let mut rig = Rig::new(0, 1);
    rig.request(Source::Feed);
    rig.feed.set_connected(false);
    serve_block(&mut rig, Source::Service, 0, 1);
    rig.feed.set_connected(true);
    rig.run_for(TIMING.ready * 2 / 3);
    rig.feed.set_connected(false);
    rig.run_for(TIMING.poll * 4);
    rig.feed.set_connected(true);
    rig.run_for(TIMING.ready * 2 / 3);
    assert_eq!(rig.feed.requests(), [], "no probe before a whole window");
    assert_eq!(rig.sub.source(), Source::Service);
}

#[test]
fn a_hash_mismatch_replays_the_index_from_the_other_source() {
    let mut rig = Rig::new(0, 1);
    rig.request(Source::Feed);
    rig.feed.send(forged_at(0));
    let request = serve_block(&mut rig, Source::Service, 0, 1);
    assert_eq!((request.from_index, request.from_block), (0, 1));
    assert_eq!(rig.sub.source(), Source::Service);
    assert_eq!(
        rig.next_item(),
        (
            kardamom_types::BPosition::from_index(0),
            Ordered::Tx(entry(0))
        ),
        "the reader executes the entry of the other source, not the forged one"
    );
}

#[test]
fn both_sources_below_their_floor_go_to_a_peer_checkpoint() {
    let mut rig = Rig::new(3, 2);
    let request = rig.request(Source::Feed);
    rig.feed.send(unavailable(request.request_id, 10, 5));
    let request = rig.request(Source::Service);
    rig.service.send(unavailable(request.request_id, 8, 4));
    match rig.error() {
        ExecutorError::ClusterReplayUnavailable {
            from_index,
            oldest_index,
            oldest_block,
        } => assert_eq!((from_index, oldest_index, oldest_block), (3, 8, 4)),
        other => panic!("expected the peer-checkpoint error, got {other:?}"),
    }
}

/// Run a reader of a feed below its floor, with `service` as the second
/// source that never connects, until it fails.
fn refused_with_no_service<S: Transport>(service: S) -> ExecutorError {
    let (feed, remote) = MemoryTransport::pair();
    let mut sub = CarriedSubscription::with_timing(feed, service, ReplayCursor::new(3, 2), TIMING);
    fail_answering(&mut sub, &remote, |id| unavailable(id, 10, 5))
}

#[test]
fn a_feed_below_its_floor_with_no_service_goes_to_a_peer_checkpoint() {
    let no_egress: Option<super::EgressTransport> = None;
    for error in [
        refused_with_no_service(NoSource),
        refused_with_no_service(no_egress),
    ] {
        assert!(
            matches!(
                error,
                ExecutorError::ClusterReplayUnavailable {
                    oldest_block: 5,
                    ..
                }
            ),
            "{error:?}"
        );
    }
}

#[test]
fn the_service_drops_its_frames_while_the_reader_reads_the_feed() {
    let mut rig = Rig::new(0, 1);
    serve_block(&mut rig, Source::Feed, 0, 1);
    assert_eq!(rig.deliver(5), ["t0", "b1"]);
    (0..8).for_each(|index| rig.service.send(tx_at(index)));
    rig.run_for(TIMING.poll * 4);
    assert_eq!(rig.sub.source(), Source::Feed);
    assert_eq!(
        rig.service.pending(),
        0,
        "a live service that the reader does not read keeps no backlog"
    );
}

#[test]
fn a_service_that_never_connects_sends_the_reader_back_to_the_feed() {
    let mut rig = Rig::new(0, 1);
    rig.request(Source::Feed);
    rig.service.set_connected(false);
    rig.feed.set_connected(false);
    rig.run_until(|rig| rig.sub.source() == Source::Service);
    rig.run_until(|rig| rig.sub.source() == Source::Feed);
    rig.feed.set_connected(true);
    let request = rig.request(Source::Feed);
    assert_eq!((request.from_index, request.from_block), (0, 1));
    assert_eq!(rig.service.requests(), []);
}

#[test]
fn a_silent_feed_and_a_service_below_its_floor_try_the_feed_again() {
    let mut rig = Rig::new(3, 2);
    let first = rig.request(Source::Feed);
    let request = rig.request(Source::Service);
    let resends = rig.feed.requests();
    assert!(
        !resends.is_empty(),
        "the feed got resends before the silence"
    );
    rig.service.send(unavailable(request.request_id, 8, 4));
    let again = rig.request(Source::Feed);
    assert_eq!(rig.sub.source(), Source::Feed, "no peer checkpoint");
    assert_ne!(again.request_id, first.request_id);
    assert_eq!((again.from_index, again.from_block), (3, 2));
}

#[test]
fn a_service_that_never_connected_counts_as_absent() {
    let mut rig = Rig::new(3, 2);
    rig.service.set_connected(false);
    let error = fail_answering(&mut rig.sub, &rig.feed, |id| unavailable(id, 10, 5));
    match error {
        ExecutorError::ClusterReplayUnavailable { oldest_block, .. } => {
            assert_eq!(oldest_block, 5);
        }
        other => panic!("expected the peer-checkpoint error, got {other:?}"),
    }
    assert_eq!(rig.service.requests(), []);
}

#[test]
fn a_large_feed_backlog_does_not_stop_the_return_to_the_feed() {
    let mut rig = Rig::new(0, 1);
    rig.request(Source::Feed);
    rig.feed.set_connected(false);
    serve_block(&mut rig, Source::Service, 0, 1);
    assert_eq!(rig.deliver(5), ["t0", "b1"]);
    let old = tx_at(0);
    (0..200_000).for_each(|_| rig.feed.send(old.clone()));
    rig.feed.resume();
    let started = std::time::Instant::now();
    let probe = rig.request(Source::Feed);
    (0..20_000).for_each(|_| rig.feed.send(old.clone()));
    rig.feed.send(done(probe.request_id, 1, 2));
    rig.run_until(|rig| rig.sub.source() == Source::Feed);
    assert!(
        started.elapsed() < TIMING.ready + Duration::from_secs(1),
        "the return took {:?}",
        started.elapsed()
    );
}

#[test]
fn a_new_request_clears_the_old_floor_of_the_source() {
    let mut rig = Rig::new(3, 2);
    let request = rig.request(Source::Feed);
    rig.feed.send(unavailable(request.request_id, 10, 5));
    rig.run_until(|rig| rig.sub.source() == Source::Service);
    let service = rig.request(Source::Service);
    rig.service.send(tx_at(3));
    rig.service.send(boundary(2, 4));
    rig.service.send(done(service.request_id, 4, 3));
    assert_eq!(rig.deliver(5), ["t3", "b2"]);
    assert_eq!(rig.feed.requests(), [], "no probe before the ready window");
    rig.request(Source::Feed);
    rig.feed.send(tx_at(4));
    rig.service.send(unavailable(service.request_id, 8, 4));
    rig.run_until(|rig| rig.sub.source() == Source::Feed);
    let again = rig.request(Source::Feed);
    assert_eq!(
        (again.from_index, again.from_block),
        (4, 3),
        "no peer checkpoint at the old floor of the feed"
    );
}

/// Both sources have a head below the delivery cursor: they lost entries
/// that the reader delivered. The run starts again from the state DB, and
/// the comparison at the start finds the divergence.
#[test]
fn both_heads_below_the_cursor_restart_the_run_from_the_state_db() {
    let mut rig = Rig::new(20, 10);
    let request = rig.request(Source::Feed);
    rig.feed.send(encode_feed_answer(
        EGRESS_KIND_REPLAY_AHEAD,
        5,
        3,
        request.request_id,
    ));
    let request = rig.request(Source::Service);
    rig.service.send(encode_feed_answer(
        EGRESS_KIND_REPLAY_AHEAD,
        9,
        6,
        request.request_id,
    ));
    match rig.error() {
        ExecutorError::SourceRestarted {
            next_index,
            next_block,
        } => assert_eq!((next_index, next_block), (20, 10)),
        other => panic!("expected the restart from the state DB, got {other:?}"),
    }
}

/// The feed comes back on a new image after the reader delivered entries
/// of the service. The stream of the new image can differ below the
/// cursor, so the run starts again from the state DB.
#[test]
fn a_return_to_a_new_image_of_the_feed_restarts_the_run() {
    let mut rig = Rig::new(0, 1);
    rig.request(Source::Feed);
    rig.feed.set_connected(false);
    serve_block(&mut rig, Source::Service, 0, 1);
    assert_eq!(rig.deliver(5), ["t0", "b1"]);
    rig.feed.set_connected(true);
    let probe = rig.request(Source::Feed);
    rig.feed.send(done(probe.request_id, 1, 2));
    match rig.error() {
        ExecutorError::SourceRestarted {
            next_index,
            next_block,
        } => assert_eq!((next_index, next_block), (1, 2)),
        other => panic!("expected the restart from the state DB, got {other:?}"),
    }
}
