use super::*;

#[test]
fn a_regular_case_has_the_three_minute_deadline_and_its_load_covers_it() {
    let knobs = Knobs::read(&[]).unwrap();
    assert_eq!(knobs.case_deadline, Duration::from_mins(3));
    for case in [Case::GracefulExecutor, Case::NodeFailureExecutor, Case::L1Liar] {
        assert_eq!(case.deadline(&knobs), Duration::from_mins(3), "{}", case.name());
        assert_eq!(case.window(&knobs), case.deadline(&knobs), "{}", case.name());
    }
    let shorter = Knobs::read(&[("CHAOS_CASE_S", "90")]).unwrap();
    assert_eq!(Case::HardIngress.deadline(&shorter), Duration::from_secs(90));
}

#[test]
fn only_the_nightly_cases_keep_a_longer_bound() {
    let knobs = Knobs::read(&[]).unwrap();
    for case in [
        Case::ExecutorRestartStorm,
        Case::SealerFleetTotalWipeRecover,
        Case::ResizeScaleOutIn,
    ] {
        assert!(case.deadline(&knobs) > knobs.case_deadline, "{}", case.name());
    }
}
