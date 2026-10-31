use super::*;

#[test]
fn a_regular_case_has_the_three_minute_deadline_and_its_load_covers_it() {
    let knobs = Knobs::read(&[]).unwrap();
    assert_eq!(knobs.case_deadline, Duration::from_mins(3));
    for case in [Case::HardExecutor, Case::NodeFailureExecutor, Case::L1Liar] {
        assert_eq!(
            case.deadline(&knobs),
            Duration::from_mins(3),
            "{}",
            case.name()
        );
        assert_eq!(
            case.window(&knobs),
            case.deadline(&knobs),
            "{}",
            case.name()
        );
    }
    let shorter = Knobs::read(&[("CHAOS_CASE_S", "90")]).unwrap();
    assert_eq!(
        Case::HardIngress.deadline(&shorter),
        Duration::from_secs(90)
    );
}

#[test]
fn only_the_nightly_cases_keep_a_longer_bound() {
    let knobs = Knobs::read(&[]).unwrap();
    for case in [
        Case::ExecutorRestartStorm,
        Case::SealerFleetTotalWipeRecover,
        Case::ResizeScaleOutIn,
        Case::NodeReplaceExecutor,
        Case::TwoDayOutage,
    ] {
        assert!(
            case.deadline(&knobs) > knobs.case_deadline,
            "{}",
            case.name()
        );
    }
}

#[test]
fn a_carry_step_without_the_check_play_has_the_case_deadline() {
    let knobs = Knobs::read(&[]).unwrap();
    let step = crate::cases::carry::Step::EgressRestart;
    assert_eq!(Case::Carry(step).deadline(&knobs), knobs.case_deadline);
}

#[test]
fn the_deploy_tooling_cases_have_the_case_deadline() {
    let knobs = Knobs::read(&[]).unwrap();
    for case in [Case::DeployBrokenImage, Case::NodeReplaceSealer] {
        assert_eq!(
            case.deadline(&knobs),
            knobs.case_deadline,
            "{}",
            case.name()
        );
    }
}
