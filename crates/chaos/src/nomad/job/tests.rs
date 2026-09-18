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
