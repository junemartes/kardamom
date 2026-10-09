use std::sync::Arc;
use std::time::Duration;

use kardamom_obs::events::{Board, BoardView};
use kardamom_obs::halt::{Halt, HaltCause, ServiceEvent, ServiceState};
use kardamom_obs::lifecycle::{Lifecycle, Slots};
use kardamom_types::ClusterStatus;
use kardamom_types::cluster_status::RecordLagStatus;
use tokio::sync::watch;
use tokio::time::Instant;

use super::{ChainStatus, ChainWatch, EXECUTOR, SEALER, SEALER_SILENCE};

fn event(service: &str, instance: &str, state: ServiceState) -> ServiceEvent {
    ServiceEvent {
        service: service.into(),
        instance: instance.into(),
        seq: 1,
        state,
    }
}

fn halted(cause: HaltCause) -> ServiceState {
    ServiceState::Halted(Halt::new(cause, "detail"))
}

fn board(events: Vec<ServiceEvent>) -> BoardView {
    let now = Instant::now();
    let mut board = Board::default();
    for event in events {
        board.observe(event, now);
    }
    board.view(now)
}

fn watch_over(sealer: &Arc<Lifecycle>) -> ChainWatch {
    ChainWatch::new(
        watch::channel(ClusterStatus::default()).1,
        watch::channel(BoardView::default()).1,
        sealer.clone(),
    )
}

#[test]
fn the_da_lag_flag_halts_the_sealer_and_the_next_status_clears_it() {
    let sealer = Arc::new(Lifecycle::new(Some(SEALER)));
    let mut chain = watch_over(&sealer);
    let now = Instant::now();
    chain.on_status(
        &ClusterStatus {
            posted_head: 100,
            sealed_head: 160,
            budget_blocks: 50,
            halted: true,
            ..ClusterStatus::default()
        },
        now,
    );
    let halt = sealer.slots().halt.unwrap();
    assert_eq!(halt.cause, HaltCause::DaLag);
    assert!(
        halt.detail.contains("sealed head 160 is 60 blocks"),
        "{}",
        halt.detail
    );
    let root = ChainWatch::submit_root(&sealer.slots(), &BoardView::default()).unwrap();
    assert_eq!(
        (root.service.as_str(), root.cause),
        (SEALER, HaltCause::DaLag)
    );

    chain.on_status(&ClusterStatus::default(), now);
    assert_eq!(sealer.slots().halt, None);
    assert_eq!(
        ChainWatch::submit_root(&sealer.slots(), &BoardView::default()),
        None
    );
}

/// The record-lag flag halts the sealer on `record_lag`. With both flags
/// up, the record lag is the root; when it clears, the DA lag stands; when
/// both clear, the halt ends.
#[test]
fn the_record_lag_flag_comes_before_the_da_lag_flag_and_each_clears() {
    let sealer = Arc::new(Lifecycle::new(Some(SEALER)));
    let mut chain = watch_over(&sealer);
    let now = Instant::now();
    let da_lag = ClusterStatus {
        posted_head: 100,
        sealed_head: 160,
        budget_blocks: 50,
        halted: true,
        ..ClusterStatus::default()
    };
    let both = ClusterStatus {
        record_lag: RecordLagStatus {
            best_recorded: Some(3_000),
            budget: 16_384,
            halted: true,
        },
        ..da_lag
    };

    chain.on_status(&both, now);
    let halt = sealer.slots().halt.unwrap();
    assert_eq!(halt.cause, HaltCause::RecordLag);
    assert!(
        halt.detail.contains("best recorded index is 3000"),
        "{}",
        halt.detail
    );
    let root = ChainWatch::submit_root(&sealer.slots(), &BoardView::default()).unwrap();
    assert_eq!(
        (root.service.as_str(), root.cause),
        (SEALER, HaltCause::RecordLag)
    );

    chain.on_status(&da_lag, now);
    assert_eq!(sealer.slots().halt.unwrap().cause, HaltCause::DaLag);

    chain.on_status(
        &ClusterStatus {
            record_lag: both.record_lag,
            ..ClusterStatus::default()
        },
        now,
    );
    assert_eq!(sealer.slots().halt.unwrap().cause, HaltCause::RecordLag);

    chain.on_status(&ClusterStatus::default(), now);
    assert_eq!(sealer.slots().halt, None);
    assert_eq!(
        ChainWatch::submit_root(&sealer.slots(), &BoardView::default()),
        None
    );
}

#[test]
fn a_silent_cluster_halts_the_sealer_on_a_lost_quorum_and_a_status_ends_it() {
    let sealer = Arc::new(Lifecycle::new(Some(SEALER)));
    let mut chain = watch_over(&sealer);
    let start = Instant::now();
    chain.check_silence(start + SEALER_SILENCE * 2);
    assert_eq!(sealer.slots().halt, None, "no session yet: never silent");

    chain.on_status(&ClusterStatus::default(), start);
    chain.check_silence(start + SEALER_SILENCE - Duration::from_millis(1));
    assert_eq!(sealer.slots().halt, None);
    chain.check_silence(start + SEALER_SILENCE);
    assert_eq!(
        sealer.slots().halt.unwrap().cause,
        HaltCause::SealerNoQuorum
    );

    chain.on_status(&ClusterStatus::default(), start + SEALER_SILENCE * 2);
    assert_eq!(sealer.slots().halt, None, "a status proves the quorum");
}

#[test]
fn every_executor_halted_pauses_submits_and_one_serving_executor_does_not() {
    let sealer = Slots::default();
    let one = board(vec![
        event(EXECUTOR, "e1", halted(HaltCause::ReplayUnavailable)),
        event(EXECUTOR, "e2", ServiceState::Running),
    ]);
    assert_eq!(ChainWatch::submit_root(&sealer, &one), None);
    let all = board(vec![
        event(EXECUTOR, "e1", halted(HaltCause::ReplayUnavailable)),
        event(EXECUTOR, "e2", halted(HaltCause::ReplayUnavailable)),
    ]);
    let root = ChainWatch::submit_root(&sealer, &all).unwrap();
    assert_eq!(root.service, EXECUTOR);
}

#[test]
fn the_sealer_root_comes_before_the_executors() {
    let sealer = Slots {
        halt: Some(Halt::new(HaltCause::SealerNoQuorum, "silent")),
        pause: None,
    };
    let all = board(vec![event(
        EXECUTOR,
        "e1",
        halted(HaltCause::ReplayUnavailable),
    )]);
    assert_eq!(
        ChainWatch::submit_root(&sealer, &all).unwrap().cause,
        HaltCause::SealerNoQuorum
    );
}

#[test]
fn the_chain_status_names_the_roots_and_the_rows_that_only_report() {
    let sealer = Slots {
        halt: Some(Halt::new(HaltCause::DaLag, "lag")),
        pause: None,
    };
    let view = board(vec![
        event("batcher", "aux", halted(HaltCause::L1Unreachable)),
        event("da-watcher", "aux", halted(HaltCause::L1ChainBreak)),
        event("ingress", "i1", ServiceState::Running),
    ]);
    let json = ChainStatus {
        cluster: ClusterStatus {
            posted_head: 5,
            sealed_head: 9,
            budget_blocks: 3,
            ..ClusterStatus::default()
        },
        sealer: &sealer,
        ingress: &Slots::default(),
        board: &view,
    }
    .to_json();
    assert_eq!(json["posted_head"], 5);
    assert_eq!(json["sealed_head"], 9);
    assert_eq!(json["sealer"]["state"], "halted");
    assert_eq!(json["roots"][0]["service"], "sealer");
    assert_eq!(json["roots"][0]["runbook"], "docs/runbooks/da_lag.md");
    assert_eq!(json["roots"].as_array().unwrap().len(), 3);
    assert_eq!(json["batcher_halted"]["cause"], "l1_unreachable");
    assert_eq!(json["deposits_delayed"], true);
    assert_eq!(json["services"].as_array().unwrap().len(), 3);
    assert_eq!(
        json["best_recorded"],
        serde_json::Value::Null,
        "no cursor yet"
    );
    assert_eq!(json["record_lag_budget"], 0);
    assert_eq!(json["record_lag_halted"], false);
}

#[test]
fn the_chain_status_shows_the_record_lag_guard() {
    let json = ChainStatus {
        cluster: ClusterStatus {
            record_lag: RecordLagStatus {
                best_recorded: Some(70),
                budget: 16_384,
                halted: true,
            },
            ..ClusterStatus::default()
        },
        sealer: &Slots::default(),
        ingress: &Slots::default(),
        board: &board(vec![]),
    }
    .to_json();
    assert_eq!(json["best_recorded"], 70);
    assert_eq!(json["record_lag_budget"], 16_384);
    assert_eq!(json["record_lag_halted"], true);
}
