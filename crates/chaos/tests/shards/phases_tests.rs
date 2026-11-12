use kardamom_chaos::Shard;

use super::Phase;

fn cases(names: &[&str]) -> Vec<String> {
    names.iter().map(|name| (*name).to_owned()).collect()
}

#[test]
fn the_retention_shard_runs_every_case_on_one_chain_born_with_the_da_guard() {
    let selected = cases(Shard::Retention.cases());
    let phase = Phase::read(Shard::Retention, &selected, false).unwrap();
    assert_eq!(phase.vars.da_lag_budget_blocks, Some(8));
    assert_eq!(phase.knobs.da_lag_budget_blocks.unwrap().get(), 8);
    assert!(phase.vars.genesis_carry);
    assert_eq!(phase.cases, selected);
}

#[test]
fn a_shard_without_the_guard_deploys_none_and_an_empty_plan_is_refused() {
    let phase = Phase::read(Shard::Cache, &cases(Shard::Cache.cases()), true).unwrap();
    assert_eq!(phase.vars.da_lag_budget_blocks, None);
    assert!(phase.reuse);
    assert!(Phase::read(Shard::Cache, &[], false).is_err());
}
