use super::*;
use crate::cases::carry::Step;

#[test]
fn replacement_traffic_covers_provisioning_and_the_recovered_fleet() {
    let knobs = Knobs::read(&[]).unwrap();
    let provisioning = Duration::from_secs(41 * 60 + 14);
    let recovered = knobs.inject_delay + provisioning + knobs.rejoin_slo;
    for case in [Case::NodeReplaceExecutor, Case::NodeReplaceSealer] {
        assert!(case.window(&knobs) > recovered, "{}", case.name());
    }
}

#[test]
fn carry_traffic_covers_each_full_deployment_and_recovery() {
    let knobs = Knobs::read(&[]).unwrap();
    let deployment = Duration::from_secs(43 * 60 + 9);
    let observed = knobs.inject_delay + deployment + knobs.restart_slo;
    for step in [Step::ReadersOnEgress, Step::ExecutorFirst] {
        assert!(Case::Carry(step).window(&knobs) > observed);
    }
    let two_deployments = knobs.inject_delay + deployment * 2 + knobs.restart_slo;
    assert!(Case::Carry(Step::ExecutorsRest).window(&knobs) > two_deployments);
}

#[test]
fn broken_image_traffic_covers_the_observed_failed_deploy_and_healing_timeline() {
    let knobs = Knobs::read(&[]).unwrap();
    // Both plays run under traffic. The failed rollout takes 20m32s;
    // healing takes 18m13s, before the case-owned recovery observation.
    let broken = Duration::from_secs(20 * 60 + 32);
    let healing = Duration::from_secs(18 * 60 + 13);
    let recovery = knobs.inject_delay + broken + healing + knobs.restart_slo;
    assert!(Case::DeployBrokenImage.window(&knobs) > recovery);
    let mut longer = knobs.clone();
    longer.case_window = Case::DeployBrokenImage.window(&knobs) + Duration::from_mins(1);
    assert_eq!(Case::DeployBrokenImage.window(&longer), longer.case_window);
}
