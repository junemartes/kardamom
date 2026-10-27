use std::cell::RefCell;
use std::future::ready;

use super::*;

impl WritePause {
    fn fixture() -> Self {
        Self {
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
        WritePause::data_nodes(&allocations)
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
        assert!(WritePause::data_nodes(&allocations).is_err());
    }
}

#[tokio::test]
async fn every_destination_is_paused_and_unpaused_even_after_the_first_error() {
    let pause = WritePause::fixture();
    let calls = RefCell::new(Vec::new());
    let armed = pause
        .command_with("CLIENT PAUSE 600000 WRITE", |target, script| {
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
        .command_with("CLIENT UNPAUSE", |target, script| {
            calls.borrow_mut().push((target.node.clone(), script));
            ready(Ok("OK".into()))
        })
        .await
        .unwrap();
    assert_eq!(*calls.borrow(), [
        ("node-primary".into(), "docker exec cid-primary redis-cli --raw -h 127.0.0.1 -p 6379 CLIENT PAUSE 600000 WRITE".into()),
        ("node-replica".into(), "docker exec cid-replica redis-cli --raw -h 127.0.0.1 -p 6379 CLIENT PAUSE 600000 WRITE".into()),
        ("node-primary".into(), "docker exec cid-primary redis-cli --raw -h 127.0.0.1 -p 6379 CLIENT UNPAUSE".into()),
        ("node-replica".into(), "docker exec cid-replica redis-cli --raw -h 127.0.0.1 -p 6379 CLIENT UNPAUSE".into()),
    ]);
}

#[tokio::test]
async fn an_error_reply_from_either_data_node_rejects_the_fault() {
    let pause = WritePause::fixture();
    for failed in ["primary", "replica"] {
        let result = pause
            .command_with("CLIENT PAUSE 600000 WRITE", |target, _| {
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
