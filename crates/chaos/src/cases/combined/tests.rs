use std::collections::BTreeSet;

use super::checks::{Refills, leaders_by_term, split_terms};
use super::*;
use crate::Shard;

/// Every combined case, in the shard's run order.
const ALL: [Combined; 5] = [
    INGRESS_SEQUENCER,
    INGRESS_SEALER,
    SEQUENCER_SEALER,
    ALL_THREE,
    ALL_THREE_REVERSE,
];

fn classes(plan: &[Step]) -> Vec<Class> {
    plan.iter().map(|s| s.class).collect()
}

fn pauses(plan: &[Step]) -> Vec<u64> {
    plan.iter().map(|s| s.after.as_secs()).collect()
}

#[test]
fn the_return_follows_the_dependency_order_and_the_stagger() {
    let down = [Class::Ingress, Class::Sequencer, Class::Sealer];
    let ordered = Recovery::Ordered(Duration::from_secs(30)).plan(&down);
    assert_eq!(
        classes(&ordered),
        [Class::Sealer, Class::Sequencer, Class::Ingress]
    );
    assert_eq!(pauses(&ordered), [0, 30, 30]);
    let reverse = Recovery::Reverse(Duration::from_secs(45)).plan(&down);
    assert_eq!(
        classes(&reverse),
        [Class::Ingress, Class::Sequencer, Class::Sealer]
    );
    assert_eq!(pauses(&reverse), [0, 45, 45]);
    // The fault order is the return order when every class returns at once.
    let at_once = Recovery::AllAtOnce.plan(&[Class::Sequencer, Class::Sealer]);
    assert_eq!(classes(&at_once), [Class::Sequencer, Class::Sealer]);
    assert_eq!(pauses(&at_once), [0, 0]);
    // A pair in reverse: the ingresses a minute before the sealers.
    let pair = Recovery::Reverse(Duration::from_secs(60)).plan(&[Class::Ingress, Class::Sealer]);
    assert_eq!(classes(&pair), [Class::Ingress, Class::Sealer]);
    assert_eq!(pauses(&pair), [0, 60]);
}

#[test]
fn the_expectations_judge_two_readings() {
    let flat = Progress {
        block: 100,
        applied: 5000,
    };
    let sealed = Progress {
        block: 103,
        applied: 5000,
    };
    let applied = Progress {
        block: 103,
        applied: 5010,
    };
    assert!(Expect::Stall.judge(flat, flat).is_ok());
    assert!(
        Expect::Stall
            .judge(flat, sealed)
            .unwrap_err()
            .contains("UNEXPECTEDLY progressed")
    );
    assert!(Expect::SealOnly.judge(flat, sealed).is_ok());
    assert!(
        Expect::SealOnly
            .judge(flat, flat)
            .unwrap_err()
            .contains("no block was sealed")
    );
    assert!(
        Expect::SealOnly
            .judge(flat, applied)
            .unwrap_err()
            .contains("a user transaction was applied")
    );
    assert!(Expect::Stall.observed(flat, flat).contains("STALLED"));
}

#[test]
fn one_leader_per_term() {
    let logs = "2026-09-20T14:27:56Z cluster TERM memberId=1 leadershipTermId=7 leaderMemberId=1 logPosition=1 role=LEADER block=10\n\
        2026-09-20T14:27:56Z cluster TERM memberId=2 leadershipTermId=7 leaderMemberId=1 logPosition=1 role=FOLLOWER block=10\n\
        2026-09-20T14:30:00Z cluster TERMINATION memberId=2 reason=shutdown\n\
        2026-09-20T14:31:00Z cluster TERM memberId=0 leadershipTermId=8 leaderMemberId=2 logPosition=9 role=FOLLOWER block=40\n";
    let terms = leaders_by_term(logs);
    assert_eq!(terms.len(), 2);
    assert_eq!(terms[&7], BTreeSet::from([1]));
    assert_eq!(terms[&8], BTreeSet::from([2]));
    assert_eq!(split_terms(&terms), Vec::<String>::new());
    let split = format!(
        "{logs}2026-09-20T14:31:00Z cluster TERM memberId=1 leadershipTermId=8 leaderMemberId=1 logPosition=9 role=LEADER block=40\n"
    );
    assert_eq!(
        split_terms(&leaders_by_term(&split)),
        ["term 8: members {1, 2}"]
    );
    assert!(leaders_by_term("cluster role=LEADER memberId=1").is_empty());
}

#[test]
fn a_refill_is_a_rise_of_either_counter() {
    let before = Refills {
        republished: Some(3),
        origin_gaps: Some(0),
    };
    assert!(!before.rose_to(before));
    assert!(before.rose_to(Refills {
        republished: Some(4),
        origin_gaps: Some(0),
    }));
    assert!(before.rose_to(Refills {
        republished: Some(3),
        origin_gaps: Some(1),
    }));
    // A dark exporter is not a rise.
    assert!(!before.rose_to(Refills {
        republished: None,
        origin_gaps: None,
    }));
    assert_eq!(before.describe(), "republished 3, origin gaps 0");
}

#[test]
fn the_restarts_of_an_allocation_sum_over_its_tasks() {
    let body = r#"{"ID":"788914fb-2aa0","TaskGroup":"ingress","ClientStatus":"running",
        "JobVersion":2,"DesiredStatus":"run","NodeName":"ingress-0","NodeID":"2d99",
        "TaskStates":{"ingress":{"State":"running","Restarts":2,"Failed":false},
        "sidecar":{"State":"running","Restarts":1,"Failed":false}}}"#;
    let alloc: Alloc = serde_json::from_str(body).unwrap();
    assert_eq!(task_restarts(&alloc), 3);
    let fresh = r#"{"ID":"0f89a7f1-2222","TaskGroup":"ingress","ClientStatus":"pending",
        "JobVersion":2,"DesiredStatus":"run","NodeName":"ingress-1","NodeID":"3653","TaskStates":null}"#;
    let alloc: Alloc = serde_json::from_str(fresh).unwrap();
    assert_eq!(task_restarts(&alloc), 0);
}

#[test]
fn the_table_is_the_shard() {
    let names: Vec<&str> = ALL.iter().map(|c| c.case.name()).collect();
    assert_eq!(names, Shard::CombinedOrdering.cases());
    for c in &ALL {
        // A stall needs the sealers down; a seal-only case keeps them.
        assert_eq!(
            c.expect == Expect::Stall,
            c.down.contains(&Class::Sealer),
            "{}",
            c.case.name()
        );
        assert!(!c.checks.is_empty(), "{}", c.case.name());
    }
    assert_eq!(Class::Sealer.job(), "cluster");
    assert_eq!(
        [Class::Sealer, Class::Sequencer, Class::Ingress].map(Class::count),
        [3, 4, 2]
    );
}
