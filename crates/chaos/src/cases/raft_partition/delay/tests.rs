use std::process::Command;

use serde_json::json;

use super::Delay;

#[test]
fn exact_delay_controller_preserves_scope_ownership_and_cleanup() {
    let output = Command::new("python3")
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/src/cases/raft_partition/delay/test_fault.py"
        ))
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn every_peer_must_deliver_more_packets_without_drops() {
    let before = json!({"queues":[
        {"peer":"192.0.2.2","packets":10,"drops":0},
        {"peer":"192.0.2.3","packets":20,"drops":0}
    ]});
    let after = json!({"queues":[
        {"peer":"192.0.2.2","packets":11,"drops":0},
        {"peer":"192.0.2.3","packets":22,"drops":0}
    ]});
    Delay::require_traffic(&before, &after).unwrap();
    assert!(Delay::require_traffic(&before, &before).is_err());
    let mut wrong = after.clone();
    wrong["queues"][1]["packets"] = 20.into();
    assert!(Delay::require_traffic(&before, &wrong).is_err());
    wrong = after.clone();
    wrong["queues"][0]["drops"] = 1.into();
    assert!(Delay::require_traffic(&before, &wrong).is_err());
    wrong = after.clone();
    wrong["queues"][1]["peer"] = "192.0.2.9".into();
    assert!(Delay::require_traffic(&before, &wrong).is_err());
    assert!(Delay::require_traffic(&before, &json!({})).is_err());
}
