use super::{Alloc, Restarts};

/// An allocation of the executor job whose task lists `events`.
fn alloc(events: &serde_json::Value) -> Alloc {
    restarted(events, 1)
}

/// An allocation of the executor job that restarted `restarts` times
/// and whose task lists `events`.
fn restarted(events: &serde_json::Value, restarts: u64) -> Alloc {
    serde_json::from_value(serde_json::json!({
        "JobVersion": 3, "DesiredStatus": "run", "ID": "df280978-aaaa-bbbb-cccc-000000000000",
        "TaskGroup": "executor", "ClientStatus": "running", "NodeName": "executor-1",
        "NodeID": "n1",
        "TaskStates": {"executor": {
            "State": "running", "Restarts": restarts, "StartedAt": "2026-10-24T10:30:45Z",
            "FinishedAt": null, "Events": events}}
    }))
    .unwrap()
}

fn terminated(code: i64) -> serde_json::Value {
    terminated_at(code, 1_761_301_845_000_000_000_i64)
}

fn terminated_at(code: i64, time: i64) -> serde_json::Value {
    serde_json::json!({"Type": "Terminated", "Time": time,
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
    let exits = Restarts::of(&failed).unwrap();
    let consul = "Error: discovery: query kardamom-cluster-member: 500 Internal Server Error: No known Consul servers\n";
    let notes = exits.unexplained(consul);
    assert_eq!(notes.len(), 2, "{notes:?}");
    assert!(
        notes[0].starts_with("df280978 task executor (no designed recovery line in its process):"),
        "{}",
        notes[0]
    );
    assert!(notes[0].contains("Terminated exit=1"), "{}", notes[0]);
    assert!(notes[0].contains("Exit Code: 1"), "{}", notes[0]);
    let join = "WARN kardamom_executor::reader: join timeout: TxRef has no envelope on tx_data (archive refetch exhausted); aborting\n";
    assert_eq!(exits.unexplained(join), Vec::<String>::new());
    let none = restarted(&serde_json::Value::Null, 0);
    assert!(Restarts::of(&none).unwrap().is_clean());
}

/// Two failed exits need two recovery lines.
#[test]
fn each_failed_exit_needs_its_own_recovery_line() {
    let failed = restarted(&serde_json::json!([terminated(1), terminated(1)]), 2);
    let exits = Restarts::of(&failed).unwrap();
    let join = "join timeout: TxRef has no envelope on tx_data\n";
    assert_eq!(exits.unexplained(join).len(), 2);
    assert_eq!(exits.unexplained(&join.repeat(2)), Vec::<String>::new());
}

/// The exit of a sealer member's join watchdog is a designed recovery.
/// The deferred follower stall line ends no process, so it explains no
/// exit.
#[test]
fn a_join_watchdog_exit_of_a_member_is_a_designed_recovery() {
    let failed = alloc(&serde_json::json!([terminated(8)]));
    let exits = Restarts::of(&failed).unwrap();
    let truncated = "cluster FOLLOWER STALL memberId=1 commitPosition=18339136 state=FOLLOWER_LOG_AWAIT truncated log: the service applied the log past 18338816, the position that a relaunch reaches, for 5s; a new leader cut the log tail, so no join can succeed; exiting for a relaunch that keeps the recording log\n";
    assert_eq!(exits.unexplained(truncated), Vec::<String>::new());
    let stall = "cluster LEADER STALL memberId=0 commitPosition=9 in LEADER_READY with no progress on this member or its peers for 160s (limit 160s): a member that voted cannot join this term; exiting for a relaunch from the own state so the members elect again\n";
    assert_eq!(exits.unexplained(stall), Vec::<String>::new());
    let deferred = "cluster FOLLOWER STALL memberId=1 deferred: peer memberId=2 has a closed election, so a seed can recover this member; the CATCHUP STALL rule decides at 160s\n";
    assert_eq!(exits.unexplained(deferred).len(), 2);
}

/// A task that ends with exit code 0 and restarts did not wait. Its
/// restart needs a recovery line too.
#[test]
fn a_clean_exit_that_restarts_needs_a_recovery_line() {
    let ended = alloc(&serde_json::json!([terminated(0)]));
    let exits = Restarts::of(&ended).unwrap();
    assert_eq!(exits.unexplained("ingress: no sealer\n").len(), 2);
    let unlisted = restarted(&serde_json::Value::Null, 3);
    let notes = Restarts::of(&unlisted).unwrap().unexplained("");
    assert_eq!(
        notes,
        vec!["df280978 restarted 3 times with 0 designed recovery lines in its log"]
    );
}

/// A stamped recovery line explains only the end of its own process:
/// the line of an earlier process does not excuse a later crash.
#[test]
fn a_recovery_line_of_an_earlier_process_explains_no_later_crash() {
    const SECOND: i64 = 1_000_000_000;
    let started = |time: i64| serde_json::json!({"Type": "Started", "Time": time});
    let t0 = 1_761_301_800 * SECOND;
    let events = serde_json::json!([
        started(t0),
        terminated_at(1, t0 + 20 * SECOND),
        started(t0 + 35 * SECOND),
        terminated_at(101, t0 + 90 * SECOND),
        started(t0 + 105 * SECOND),
    ]);
    let crashed = restarted(&events, 2);
    let exits = Restarts::of(&crashed).unwrap();
    let early = "2025-10-24T10:30:10.000000Z  WARN kardamom_executor::reader: join timeout: TxRef has no envelope on tx_data\n";
    let notes = exits.unexplained(early);
    assert_eq!(notes.len(), 2, "{notes:?}");
    assert!(notes[0].contains("exit=101"), "{}", notes[0]);
    let late = "2025-10-24T10:31:20.000000Z  WARN kardamom_executor::reader: join timeout: TxRef has no envelope on tx_data\n";
    assert_eq!(
        exits.unexplained(&format!("{early}{late}")),
        Vec::<String>::new()
    );
}
