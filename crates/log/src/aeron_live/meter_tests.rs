use kardamom_obs::pipeline::LoopState;

use super::Backlog;

#[test]
fn a_pass_that_worked_is_busy_with_or_without_a_backlog() {
    assert_eq!(Backlog(0).pass_state(true), LoopState::Busy);
    assert_eq!(Backlog(3).pass_state(true), LoopState::Busy);
}

#[test]
fn a_pass_with_no_work_is_blocked_only_while_a_refused_offer_waits() {
    assert_eq!(Backlog(0).pass_state(false), LoopState::Idle);
    assert_eq!(Backlog(1).pass_state(false), LoopState::Blocked);
}

#[test]
fn a_wait_is_blocked_only_while_a_refused_offer_waits() {
    assert_eq!(Backlog(0).quiet_state(), LoopState::Idle);
    assert_eq!(Backlog(2).quiet_state(), LoopState::Blocked);
}
