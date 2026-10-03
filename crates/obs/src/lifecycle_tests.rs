use kardamom_types::service::{Halt, HaltCause, HaltRef, Pause, PauseReason, ServiceState};

use super::Lifecycle;

fn root(cause: HaltCause) -> HaltRef {
    HaltRef {
        service: "sealer".into(),
        instance: "cluster".into(),
        cause,
    }
}

fn operator(note: &str) -> Pause {
    Pause::new(PauseReason::Operator { note: note.into() })
}

#[test]
fn a_halt_has_priority_over_a_pause() {
    let life = Lifecycle::new(Some("test"));
    assert_eq!(life.state(), ServiceState::Running);
    life.follow(Some(root(HaltCause::DaLag)));
    assert!(matches!(life.state(), ServiceState::Paused(_)));
    life.raise(Halt::new(HaltCause::L1Unreachable, "own fault"));
    assert!(matches!(life.state(), ServiceState::Halted(_)));
    life.clear();
    assert!(
        matches!(life.state(), ServiceState::Paused(_)),
        "the pause still stands"
    );
    life.follow(None);
    assert_eq!(life.state(), ServiceState::Running);
}

#[test]
fn an_upstream_pause_never_replaces_or_ends_an_operator_pause() {
    let life = Lifecycle::new(Some("test"));
    life.pause(operator("maintenance"));
    life.follow(Some(root(HaltCause::SealerNoQuorum)));
    let pause = life.slots().pause.unwrap();
    assert_eq!(
        pause.reason,
        PauseReason::Operator {
            note: "maintenance".into()
        }
    );
    life.follow(None);
    assert!(life.slots().pause.is_some(), "an upstream resume keeps it");
    assert!(life.resume().is_some(), "the operator's resume ends it");
    assert_eq!(life.state(), ServiceState::Running);
}

#[test]
fn an_operator_pause_replaces_an_upstream_pause() {
    let life = Lifecycle::new(Some("test"));
    life.follow(Some(root(HaltCause::DaLag)));
    life.pause(operator("drain"));
    assert_eq!(life.slots().pause.unwrap().reason.id(), "operator");
}

#[test]
fn a_repeated_pause_keeps_its_start_and_wakes_nobody() {
    let life = Lifecycle::new(Some("test"));
    life.follow(Some(root(HaltCause::DaLag)));
    let first = life.slots().pause.unwrap();
    let mut rx = life.subscribe();
    rx.mark_unchanged();
    life.follow(Some(root(HaltCause::DaLag)));
    assert!(!rx.has_changed().unwrap(), "the same root wakes no watcher");
    assert_eq!(life.slots().pause.unwrap(), first);
    life.follow(Some(root(HaltCause::SealerNoQuorum)));
    assert!(rx.has_changed().unwrap(), "a new root is a change");
}

#[test]
fn a_clear_with_no_halt_wakes_nobody() {
    let life = Lifecycle::new(Some("test"));
    let mut rx = life.subscribe();
    rx.mark_unchanged();
    assert!(life.clear().is_none());
    assert!(life.resume_upstream().is_none());
    assert!(!rx.has_changed().unwrap());
}

#[test]
fn the_summary_names_the_root_and_its_runbook() {
    let life = Lifecycle::new(Some("test"));
    assert_eq!(life.slots().summary(), None);
    life.follow(Some(root(HaltCause::DaLag)));
    let summary = life.slots().summary().unwrap();
    assert!(
        summary.contains("upstream sealer cluster halted (da_lag)"),
        "{summary}"
    );
    assert!(summary.contains("docs/runbooks/da_lag.md"), "{summary}");
}
