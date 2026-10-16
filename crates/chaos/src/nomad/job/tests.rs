use super::*;

fn job() -> Job {
    serde_json::from_value(serde_json::json!({
        "Version": 3, "TaskGroups": [{"Name":"seq-0","Count":2},{"Name":"seq-1","Count":2}]
    }))
    .unwrap()
}

fn alloc(id: &str, group: &str, version: u64, desired: &str, status: &str) -> Alloc {
    serde_json::from_value(serde_json::json!({
        "ID":id,"TaskGroup":group,"JobVersion":version,"DesiredStatus":desired,
        "ClientStatus":status,"NodeName":"sequencer-0","NodeID":"node","TaskStates":{}
    }))
    .unwrap()
}

fn complete() -> Vec<Alloc> {
    vec![
        alloc("a", "seq-0", 3, "run", "running"),
        alloc("b", "seq-0", 3, "run", "running"),
        alloc("c", "seq-1", 3, "run", "running"),
        alloc("d", "seq-1", 3, "run", "running"),
    ]
}

#[test]
fn every_group_needs_its_full_current_count() {
    let job = job();
    let mut allocs = complete();
    assert_eq!(job.running(&allocs).unwrap().len(), 4);
    allocs.pop();
    assert!(job.running(&allocs).is_none());
    allocs.push(alloc("extra", "seq-0", 3, "run", "running"));
    assert!(
        job.running(&allocs).is_none(),
        "another group's replica cannot replace the missing one"
    );
}

#[test]
fn stale_stopping_failed_and_pending_allocations_cannot_satisfy_a_replica() {
    for (version, desired, status) in [
        (2, "run", "running"),
        (3, "stop", "running"),
        (3, "run", "failed"),
        (3, "run", "pending"),
    ] {
        let mut allocs = complete();
        allocs[3] = alloc("d", "seq-1", version, desired, status);
        assert!(job().running(&allocs).is_none());
    }
}

#[test]
fn historical_failures_do_not_block_a_complete_replacement_set() {
    let mut allocs = complete();
    allocs.push(alloc("old", "seq-1", 3, "run", "failed"));
    assert_eq!(job().running(&allocs).unwrap().len(), 4);
}

#[test]
fn empty_jobs_and_unknown_groups_cannot_pass() {
    let empty: Job =
        serde_json::from_value(serde_json::json!({"Version":3,"TaskGroups":[]})).unwrap();
    assert!(empty.running(&[]).is_none());
    assert!(!job().has_group("seq-2"));
    let mut allocs = complete();
    allocs.push(alloc("extra", "seq-2", 3, "run", "running"));
    assert!(job().running(&allocs).is_none());
}

/// One executor allocation per node. The allocation on `executor-2` is
/// `pending`: it waits in a restart delay.
fn executors() -> (Job, Vec<Alloc>) {
    let job = serde_json::from_value(serde_json::json!({
        "Version": 6, "TaskGroups": [{"Name":"executor","Count":3}]
    }))
    .unwrap();
    let allocs = [
        ("executor-0", "running"),
        ("executor-1", "running"),
        ("executor-2", "pending"),
    ]
    .into_iter()
    .map(|(node, status)| Alloc {
        node_name: node.into(),
        ..alloc(node, "executor", 6, "run", status)
    })
    .collect();
    (job, allocs)
}

fn nodes<const N: usize>(names: [&str; N]) -> Nodes {
    Nodes::new(names.map(String::from)).unwrap()
}

#[test]
fn a_node_set_holds_at_least_one_node() {
    assert!(Nodes::new(Vec::new()).is_none());
}

#[test]
fn a_replica_that_restarts_by_design_does_not_hold_back_the_named_nodes() {
    let (job, allocs) = executors();
    assert!(job.running(&allocs).is_none());
    assert!(job.runs_on(&allocs, &nodes(["executor-0", "executor-1"])));
    assert!(!job.runs_on(&allocs, &nodes(["executor-0", "executor-2"])));
}

#[test]
fn a_named_node_needs_a_current_running_allocation() {
    let (job, mut allocs) = executors();
    allocs[1].client_status = "pending".into();
    assert!(!job.runs_on(&allocs, &nodes(["executor-0", "executor-1"])));
    allocs[1] = Alloc {
        node_name: "executor-1".into(),
        ..alloc("old", "executor", 5, "run", "running")
    };
    assert!(!job.runs_on(&allocs, &nodes(["executor-0", "executor-1"])));
}
