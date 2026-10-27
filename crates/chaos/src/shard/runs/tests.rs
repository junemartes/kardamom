use super::*;
use crate::knobs::Knobs;

#[test]
fn retention_keeps_all_cases_and_separates_the_two_replay_budgets() {
    let cases: Vec<String> = Shard::Retention
        .cases()
        .iter()
        .map(|c| (*c).into())
        .collect();
    let runs = Shard::Retention.runs(&cases, false);
    assert_eq!(runs.len(), 2);
    assert_eq!(runs[0].cases, cases[..3]);
    assert_eq!(runs[1].cases, cases[3..]);
    assert_eq!(runs[0].vars.da_lag_budget_blocks, None);
    assert_eq!(runs[1].vars.da_lag_budget_blocks, Some(8));
    assert_eq!(
        runs.iter().flat_map(|run| &run.cases).collect::<Vec<_>>(),
        cases.iter().collect::<Vec<_>>()
    );
    let knobs = Knobs::deployed(Shard::Retention.env(), &runs[1].vars).unwrap();
    assert_eq!(knobs.da_lag_budget_blocks.unwrap().get(), 8);
    assert!(runs.iter().all(|run| run.vars.genesis_carry
        && run.vars.canary_local == Some(true)
        && run.vars.checkpoint_log));
}

#[test]
fn custom_case_order_starts_another_chain_instead_of_restoring_a_live_budget() {
    let cases = ["da-lag-halt", "prune-floor", "canary-da-lag"].map(str::to_string);
    let runs = Shard::Retention.runs(&cases, false);
    assert_eq!(runs.len(), 3);
    assert_eq!(
        runs.iter()
            .map(|run| run.vars.da_lag_budget_blocks)
            .collect::<Vec<_>>(),
        [Some(8), None, Some(8)]
    );
    assert_eq!(
        runs.iter().flat_map(|run| &run.cases).collect::<Vec<_>>(),
        cases.iter().collect::<Vec<_>>()
    );
}

#[test]
fn ordinary_shards_keep_one_deployment_and_reuse_never_reconfigures_the_guard() {
    let cases: Vec<_> = Shard::Cache
        .cases()
        .iter()
        .map(|c| (*c).to_string())
        .collect();
    assert_eq!(Shard::Cache.runs(&cases, false).len(), 1);
    let cases = ["prune-floor", "da-lag-halt"].map(str::to_string);
    let runs = Shard::Retention.runs(&cases, true);
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].cases, cases);
    assert!(runs[0].vars.da_lag_budget_blocks.is_none());
    assert_eq!(Shard::Cache.runs(&[], false).len(), 1);
}
