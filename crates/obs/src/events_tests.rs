use std::time::Duration;

use tokio::time::Instant;

use kardamom_types::service::{Halt, HaltCause, HaltRef, PauseReason, ServiceEvent, ServiceState};

use super::{Beacon, Board, BoardFeed, EXPIRY, FORGET, HEARTBEAT, Identity};
use crate::lifecycle::Lifecycle;

fn identity() -> Identity {
    Identity {
        service: "ingress".into(),
        instance: "host-1".into(),
    }
}

fn event(service: &str, instance: &str, seq: u64, state: ServiceState) -> ServiceEvent {
    ServiceEvent {
        service: service.into(),
        instance: instance.into(),
        seq,
        state,
    }
}

fn halted(cause: HaltCause) -> ServiceState {
    ServiceState::Halted(Halt::new(cause, "detail"))
}

#[test]
fn the_beacon_says_resumed_once_after_a_pause() {
    let life = Lifecycle::new(Some("ingress"));
    let mut beacon = Beacon::new(identity(), life.subscribe());
    let first = beacon.next_event();
    assert_eq!(first.state, ServiceState::Running, "a fresh start runs");
    assert_eq!(first.seq, 1);
    life.follow(Some(HaltRef {
        service: "sealer".into(),
        instance: "cluster".into(),
        cause: HaltCause::DaLag,
    }));
    let paused = beacon.next_event();
    assert!(matches!(&paused.state, ServiceState::Paused(p)
        if matches!(p.reason, PauseReason::Upstream(_))));
    life.follow(None);
    assert_eq!(beacon.next_event().state, ServiceState::Resumed);
    let heartbeat = beacon.next_event();
    assert_eq!(heartbeat.state, ServiceState::Running);
    assert_eq!(heartbeat.seq, 4);
}

#[tokio::test(start_paused = true)]
async fn the_beacon_publishes_on_a_change_and_on_every_heartbeat() {
    let life = std::sync::Arc::new(Lifecycle::new(Some("ingress")));
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let beacon = Beacon::new(identity(), life.subscribe());
    let task = tokio::spawn(beacon.run(move |event| {
        let _ = tx.send(event.clone());
    }));
    assert_eq!(rx.recv().await.unwrap().state, ServiceState::Running);
    tokio::time::sleep(HEARTBEAT + Duration::from_millis(1)).await;
    assert_eq!(
        rx.recv().await.unwrap().state,
        ServiceState::Running,
        "the heartbeat"
    );
    life.raise(Halt::new(HaltCause::L1Unreachable, "down"));
    let change = rx.recv().await.unwrap();
    assert!(
        matches!(change.state, ServiceState::Halted(_)),
        "at once on a change"
    );
    task.abort();
}

#[test]
fn a_record_without_a_heartbeat_is_gone_and_then_forgotten() {
    let start = Instant::now();
    let mut board = Board::default();
    board.observe(event("batcher", "aux", 1, ServiceState::Running), start);
    let view = board.view(start + EXPIRY - Duration::from_millis(1));
    assert!(view.services[0].live);
    assert_eq!(view.services[0].state_id(), "running");
    let view = board.view(start + EXPIRY);
    assert!(!view.services[0].live);
    assert_eq!(view.services[0].state_id(), "gone");
    board.forget(start + FORGET);
    assert!(board.view(start + FORGET).services.is_empty());
}

#[test]
fn the_latest_record_wins_and_a_restart_is_taken() {
    let now = Instant::now();
    let mut board = Board::default();
    board.observe(
        event("executor", "e1", 9, halted(HaltCause::ReplayUnavailable)),
        now,
    );
    board.observe(event("executor", "e1", 1, ServiceState::Running), now);
    let view = board.view(now);
    assert_eq!(view.services.len(), 1);
    assert_eq!(view.services[0].event.state, ServiceState::Running);
    board.observe(event("executor", "e1", 2, ServiceState::Resumed), now);
    assert_eq!(board.view(now).services[0].state_id(), "running");
}

#[test]
fn every_executor_halted_names_a_root_and_one_serving_executor_names_none() {
    let now = Instant::now();
    let mut board = Board::default();
    assert_eq!(
        board.view(now).all_halted("executor"),
        None,
        "no executor is known"
    );
    board.observe(
        event("executor", "e1", 1, halted(HaltCause::ReplayUnavailable)),
        now,
    );
    board.observe(event("executor", "e2", 1, ServiceState::Running), now);
    assert_eq!(board.view(now).all_halted("executor"), None, "e2 serves");
    board.observe(
        event("executor", "e2", 2, halted(HaltCause::ReplayUnavailable)),
        now,
    );
    let root = board.view(now).all_halted("executor").unwrap();
    assert_eq!(root.service, "executor");
    assert_eq!(root.cause, HaltCause::ReplayUnavailable);
    // A gone executor does not count as serving: the live ones decide.
    let later = now + EXPIRY;
    board.observe(
        event("executor", "e1", 2, halted(HaltCause::ReplayUnavailable)),
        later,
    );
    assert_eq!(
        board.view(later).all_halted("executor").unwrap().instance,
        "e1"
    );
}

#[test]
fn the_roots_are_the_live_halts_only() {
    let now = Instant::now();
    let mut board = Board::default();
    board.observe(
        event("validator", "v1", 1, halted(HaltCause::ValidatorDivergence)),
        now,
    );
    board.observe(
        event("da-watcher", "aux", 1, halted(HaltCause::L1Unreachable)),
        now - EXPIRY,
    );
    board.observe(event("ingress", "i1", 1, ServiceState::Running), now);
    let view = board.view(now);
    let roots = view.roots();
    assert_eq!(roots.len(), 1);
    assert_eq!(roots[0].service, "validator");
    assert!(
        view.halted_on("validator", HaltCause::ValidatorDivergence)
            .is_some()
    );
    assert!(
        view.halted_on("validator", HaltCause::L1Unreachable)
            .is_none()
    );
    assert!(
        view.any_halted("da-watcher").is_none(),
        "a gone record is no root"
    );
    let json = view.to_json();
    assert_eq!(json.as_array().unwrap().len(), 3);
}

#[tokio::test(start_paused = true)]
async fn the_feed_publishes_the_view_and_ages_it() {
    let (feed, tx, mut view) = BoardFeed::new();
    let task = tokio::spawn(feed.run());
    tx.send(event("batcher", "aux", 1, ServiceState::Running))
        .await
        .unwrap();
    view.wait_for(|v| v.services.len() == 1).await.unwrap();
    assert!(view.borrow().services[0].live);
    tokio::time::sleep(EXPIRY + Duration::from_secs(2)).await;
    view.wait_for(|v| v.services.iter().all(|s| !s.live))
        .await
        .unwrap();
    task.abort();
}
