use kardamom_chaos::Shard;

use super::{Guard, Phases};

fn cases(names: &[&str]) -> Vec<String> {
    names.iter().map(|name| (*name).to_owned()).collect()
}

#[test]
fn retention_runs_every_case_in_order_on_two_fixed_configurations() {
    let selected = cases(Shard::Retention.cases());
    let phases = Phases::read(Shard::Retention, &selected, false).unwrap();
    assert_eq!(phases.0.len(), 2);
    assert_eq!(phases.0[0].guard, Guard::Standard);
    assert_eq!(phases.0[0].vars.da_lag_budget_blocks, None);
    assert_eq!(phases.0[1].guard, Guard::Short);
    assert_eq!(phases.0[1].vars.da_lag_budget_blocks, Some(8));
    assert_eq!(phases.0[1].knobs.da_lag_budget_blocks.unwrap().get(), 8);
    assert!(phases.0.iter().all(|phase| phase.vars.genesis_carry));
    assert_eq!(
        phases
            .0
            .iter()
            .flat_map(|phase| phase.cases.clone())
            .collect::<Vec<_>>(),
        selected
    );
}

#[test]
fn overrides_keep_their_order_and_never_change_a_live_guard() {
    let selected = cases(&["da-lag-halt", "prune-floor", "canary-da-lag"]);
    let phases = Phases::read(Shard::Retention, &selected, false).unwrap();
    assert_eq!(phases.0.len(), 3);
    assert_eq!(
        phases
            .0
            .iter()
            .flat_map(|phase| phase.cases.clone())
            .collect::<Vec<_>>(),
        selected
    );
    assert_eq!(
        phases.0.iter().map(|phase| phase.guard).collect::<Vec<_>>(),
        [Guard::Short, Guard::Standard, Guard::Short]
    );
}

#[test]
fn reuse_rejects_a_plan_that_needs_a_different_replicated_configuration() {
    assert!(Phases::read(Shard::Retention, &cases(Shard::Retention.cases()), true).is_err());
    let one = Phases::read(
        Shard::Retention,
        &cases(&["da-lag-halt", "canary-da-lag"]),
        true,
    )
    .unwrap();
    assert_eq!(one.0.len(), 1);
    assert!(one.0[0].reuse);
}

#[test]
fn other_shards_keep_one_configuration_and_their_complete_case_list() {
    let selected = cases(Shard::Sequencer.cases());
    let phases = Phases::read(Shard::Sequencer, &selected, false).unwrap();
    assert_eq!(phases.0.len(), 1);
    assert_eq!(phases.0[0].cases, selected);
    assert_eq!(phases.0[0].vars.da_lag_budget_blocks, None);
}

#[test]
fn each_phase_finishes_with_its_validator_evidence_still_live() {
    let phases = Phases::read(Shard::Retention, &cases(Shard::Retention.cases()), false).unwrap();
    assert!(!phases.0[0].audit(2, "prune-floor"));
    assert!(!phases.0[1].audit(1, "canary-da-lag"));
}

#[test]
fn an_empty_override_cannot_skip_every_fault_and_state_verdict() {
    assert!(Phases::read(Shard::Retention, &[], false).is_err());
}
