use std::cell::RefCell;
use std::future::ready;

use super::*;

impl WriteBlock {
    fn fixture() -> Self {
        Self {
            lease: "test-lease".into(),
            targets: ["primary", "replica"].map(|name| DataNode {
                allocation: name.into(),
                node: format!("node-{name}"),
                ip: Ipv4Addr::LOCALHOST,
                cid: format!("cid-{name}"),
                started_at: "start".into(),
            }),
        }
    }
}

impl Alloc {
    fn redis_fixture(group: &str, node: &str) -> Self {
        serde_json::from_value(serde_json::json!({
            "ID": format!("{group}-{node}"), "JobVersion": 1,
            "TaskGroup":group, "NodeName":node, "NodeID":node,
            "ClientStatus":"running", "DesiredStatus":"run"
        }))
        .unwrap()
    }
}

#[test]
fn both_data_groups_are_selected_independent_of_the_sentinel_role() {
    let mut stopped = Alloc::redis_fixture("primary", "old-primary");
    stopped.desired_status = "stop".into();
    let allocations = [
        Alloc::redis_fixture("sentinel", "s0"),
        Alloc::redis_fixture("replica", "promoted-node"),
        Alloc::redis_fixture("primary", "demoted-node"),
        stopped,
    ];
    assert_eq!(
        WriteBlock::data_nodes(&allocations)
            .unwrap()
            .map(|a| a.node_name.as_str()),
        ["demoted-node", "promoted-node"]
    );
}

#[test]
fn missing_duplicate_or_colocated_data_nodes_cannot_arm_the_fault() {
    let primary = Alloc::redis_fixture("primary", "p");
    let replica = Alloc::redis_fixture("replica", "r");
    for allocations in [
        vec![primary.clone()],
        vec![replica.clone()],
        vec![primary.clone(), primary.clone(), replica],
        vec![primary, Alloc::redis_fixture("replica", "p")],
    ] {
        assert!(WriteBlock::data_nodes(&allocations).is_err());
    }
}

#[tokio::test]
async fn every_destination_is_blocked_and_restored_even_after_the_first_error() {
    let pause = WriteBlock::fixture();
    let calls = RefCell::new(Vec::new());
    let armed = pause
        .command_with(ScriptAccess::Blocked, |target, script| {
            calls.borrow_mut().push((target.node.clone(), script));
            ready(if target.allocation == "primary" {
                Err(anyhow::anyhow!("first destination fails"))
            } else {
                Ok("OK".into())
            })
        })
        .await;
    assert!(armed.is_err());
    pause
        .command_with(ScriptAccess::Allowed, |target, script| {
            calls.borrow_mut().push((target.node.clone(), script));
            ready(Ok("OK".into()))
        })
        .await
        .unwrap();
    let calls = calls.borrow();
    assert_eq!(
        calls
            .iter()
            .map(|(node, _)| node.as_str())
            .collect::<Vec<_>>(),
        [
            "node-primary",
            "node-replica",
            "node-primary",
            "node-replica"
        ]
    );
    calls[..2].iter().for_each(|(_, script)| {
        assert!(script.contains("sleep 600"));
        assert!(script.contains("if [ -e /local/kardamom-chaos-lua-test-lease ]"));
        assert!(script.ends_with("ACL SETUSER default -eval -evalsha"));
        assert!(!script.contains("CLIENT PAUSE"));
    });
    calls[2..].iter().for_each(|(_, script)| {
        assert!(script.contains("rm -f /local/kardamom-chaos-lua-test-lease"));
        assert!(script.ends_with("ACL SETUSER default +eval +evalsha"));
    });
    assert!(calls[0].1.contains("docker exec cid-primary redis-cli"));
    assert!(calls[1].1.contains("docker exec cid-replica redis-cli"));
}

#[tokio::test]
async fn an_error_reply_from_either_data_node_rejects_the_fault() {
    let pause = WriteBlock::fixture();
    for failed in ["primary", "replica"] {
        let result = pause
            .command_with(ScriptAccess::Blocked, |target, _| {
                ready(Ok(if target.allocation == failed {
                    "ERR denied"
                } else {
                    "OK"
                }
                .into()))
            })
            .await;
        assert!(result.is_err());
    }
}

#[test]
fn both_lua_permissions_must_be_allowed_before_the_fault() {
    DataNode::require_script_access("OK\nOK\n").unwrap();
    for answer in ["", "OK", "OK\nNOPERM", "NOPERM\nOK", "OK\nOK\nOK"] {
        assert!(DataNode::require_script_access(answer).is_err());
    }
}

#[test]
fn both_lua_entry_points_must_remain_denied_before_the_kill() {
    DataNode::require_denied_scripts("NOPERM eval denied\nNOPERM evalsha denied\n").unwrap();
    for answer in [
        "",
        "NOPERM eval denied",
        "1\nNOPERM evalsha denied",
        "NOPERM eval denied\nNOSCRIPT",
    ] {
        assert!(DataNode::require_denied_scripts(answer).is_err());
    }
}
