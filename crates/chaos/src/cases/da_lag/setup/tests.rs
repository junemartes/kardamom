use std::num::NonZeroU64;

use super::GuardReadings;

#[test]
fn an_absent_or_disabled_guard_does_not_become_a_small_budget() {
    for body in [
        "",
        "kardamom_ingress_cluster_da_lag_budget_blocks 0\n",
        "kardamom_ingress_cluster_da_lag_budget_blocks -1\n",
    ] {
        assert_eq!(GuardReadings::parse(body), None);
    }
}

#[test]
fn missing_mixed_or_empty_observations_cannot_certify_the_guard() {
    let expected = NonZeroU64::new(8).unwrap();
    for readings in [
        vec![],
        vec![Some(expected), None],
        vec![Some(expected), NonZeroU64::new(10_000)],
    ] {
        assert!(!GuardReadings(readings).matches(expected));
    }
}

#[test]
fn every_ingress_observes_the_configured_guard() {
    let observed = GuardReadings::parse("kardamom_ingress_cluster_da_lag_budget_blocks 8\n");
    assert!(GuardReadings(vec![observed, observed]).matches(NonZeroU64::new(8).unwrap()));
}
