use serde_json::json;

use super::{BudgetJob, SMALL_BUDGET};

#[test]
fn every_cluster_group_changes_without_mutating_the_saved_job() {
    let task = json!({"Name":"cluster", "Env":{"JAVA_TOOL_OPTIONS":"-Xmx1g  -Dkardamom.cluster.daLagBudgetBlocks=10000\t-Dother=\"quoted value\" -Dembedded=-Dkardamom.cluster.daLagBudgetBlocks=10000"}, "Config":{"image":"sealer:original"}});
    let sidecar = json!({"Name":"other", "Env":{"JAVA_TOOL_OPTIONS":"-Dkardamom.cluster.daLagBudgetBlocks=10000"}});
    let saved = json!({"ID":"cluster", "Meta":{"source":"original"}, "TaskGroups":[
        {"Name":"first", "Count":2, "Tasks":[task.clone(), sidecar.clone()]},
        {"Name":"second", "Count":1, "Tasks":[task.clone()]}
    ]});
    let configured = BudgetJob::new(&saved, SMALL_BUDGET).unwrap();
    assert_eq!(configured.tasks.get(), 2);
    let mut expected = saved.clone();
    expected["TaskGroups"][0]["Tasks"][0]["Env"]["JAVA_TOOL_OPTIONS"] = json!(
        "-Xmx1g  -Dkardamom.cluster.daLagBudgetBlocks=8\t-Dother=\"quoted value\" -Dembedded=-Dkardamom.cluster.daLagBudgetBlocks=10000"
    );
    expected["TaskGroups"][1]["Tasks"][0]["Env"]["JAVA_TOOL_OPTIONS"] =
        expected["TaskGroups"][0]["Tasks"][0]["Env"]["JAVA_TOOL_OPTIONS"].clone();
    assert_eq!(configured.definition, expected);
    assert_eq!(saved["TaskGroups"][0]["Tasks"][0], task);
    assert_eq!(saved["TaskGroups"][0]["Tasks"][1], sidecar);
}

#[test]
fn an_absent_or_ambiguous_guard_is_rejected_before_registration() {
    let definitions = [
        json!({}),
        json!({"TaskGroups":[{"Tasks":[]}]}),
        json!({"TaskGroups":[{"Tasks":[{"Name":"cluster", "Env":{"JAVA_TOOL_OPTIONS":"-Xmx1g"}}]}]}),
        json!({"TaskGroups":[{"Tasks":[{"Name":"cluster", "Env":{"JAVA_TOOL_OPTIONS":"-Dkardamom.cluster.daLagBudgetBlocks=8 -Dkardamom.cluster.daLagBudgetBlocks=9"}}]}]}),
    ];
    assert!(
        definitions
            .iter()
            .all(|saved| BudgetJob::new(saved, SMALL_BUDGET).is_err())
    );
}
