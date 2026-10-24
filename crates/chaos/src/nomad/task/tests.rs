use super::{Alloc, FailedExits};

/// An allocation of the executor job whose task lists `events`.
fn alloc(events: &serde_json::Value) -> Alloc {
    serde_json::from_value(serde_json::json!({
        "JobVersion": 3, "DesiredStatus": "run", "ID": "df280978-aaaa-bbbb-cccc-000000000000",
        "TaskGroup": "executor", "ClientStatus": "running", "NodeName": "executor-1",
        "NodeID": "n1",
        "TaskStates": {"executor": {
            "State": "running", "Restarts": 1, "StartedAt": "2026-10-24T10:30:45Z",
            "FinishedAt": null, "Events": events}}
    }))
    .unwrap()
}

fn terminated(code: i64) -> serde_json::Value {
    serde_json::json!({"Type": "Terminated", "Time": 1_761_301_845_000_000_000_i64,
        "DisplayMessage": format!("Exit Code: {code}"), "ExitCode": code, "Signal": 0,
        "Details": {"oom_killed": "false"}})
}

const RESTARTING: &str = r#"{"Type": "Restarting", "Time": 1761301846000000000,
    "DisplayMessage": "Task restarting in 15s"}"#;

/// A failed exit passes only when the log shows a designed recovery
/// for it. A clean exit and a restart event are no failed exits.
#[test]
fn a_failed_exit_passes_only_with_its_designed_recovery_line() {
    let restarting: serde_json::Value = serde_json::from_str(RESTARTING).unwrap();
    let failed = alloc(&serde_json::json!([terminated(1), restarting]));
    let exits = FailedExits::of(&failed).unwrap();
    let consul = "Error: discovery: query kardamom-cluster-member: 500 Internal Server Error: No known Consul servers\n";
    let notes = exits.unexplained(consul);
    assert_eq!(notes.len(), 1, "{notes:?}");
    assert!(
        notes[0].starts_with("df280978 task executor (0 designed recovery lines in its log):"),
        "{}",
        notes[0]
    );
    assert!(notes[0].contains("Terminated exit=1"), "{}", notes[0]);
    assert!(notes[0].contains("Exit Code: 1"), "{}", notes[0]);
    let join = "WARN kardamom_executor::reader: join timeout: TxRef has no envelope on tx_data (archive refetch exhausted); aborting\n";
    assert_eq!(exits.unexplained(join), Vec::<String>::new());
    let clean = alloc(&serde_json::json!([terminated(0)]));
    assert!(FailedExits::of(&clean).unwrap().exits.is_empty());
    let none = alloc(&serde_json::Value::Null);
    assert!(FailedExits::of(&none).unwrap().exits.is_empty());
}

/// Two failed exits need two recovery lines.
#[test]
fn each_failed_exit_needs_its_own_recovery_line() {
    let failed = alloc(&serde_json::json!([terminated(1), terminated(1)]));
    let exits = FailedExits::of(&failed).unwrap();
    let join = "join timeout: TxRef has no envelope on tx_data\n";
    assert_eq!(exits.unexplained(join).len(), 2);
    assert_eq!(exits.unexplained(&join.repeat(2)), Vec::<String>::new());
}
