use std::collections::{BTreeMap, BTreeSet};

use super::checks::{Refills, Terms};
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

fn classes(plan: &[Wave]) -> Vec<Vec<Class>> {
    plan.iter().map(|w| w.classes.clone()).collect()
}

fn pauses(plan: &[Wave]) -> Vec<u64> {
    plan.iter().map(|w| w.after.as_secs()).collect()
}

#[test]
fn the_return_follows_the_dependency_order_and_the_stagger() {
    let down = [Class::Ingress, Class::Sequencer, Class::Sealer];
    let ordered = Recovery::Ordered(Duration::from_secs(30)).plan(&down);
    assert_eq!(
        classes(&ordered),
        [
            vec![Class::Sealer],
            vec![Class::Sequencer],
            vec![Class::Ingress]
        ]
    );
    assert_eq!(pauses(&ordered), [0, 30, 30]);
    let reverse = Recovery::Reverse(Duration::from_secs(45)).plan(&down);
    assert_eq!(
        classes(&reverse),
        [
            vec![Class::Ingress],
            vec![Class::Sequencer],
            vec![Class::Sealer]
        ]
    );
    assert_eq!(pauses(&reverse), [0, 45, 45]);
    // A pair in reverse: the ingresses a minute before the sealers.
    let pair = Recovery::Reverse(Duration::from_secs(60)).plan(&[Class::Ingress, Class::Sealer]);
    assert_eq!(classes(&pair), [vec![Class::Ingress], vec![Class::Sealer]]);
    assert_eq!(pauses(&pair), [0, 60]);
}

#[test]
fn all_at_once_posts_every_class_before_it_waits() {
    // One wave holds every class, so every job is posted and every node
    // is started before the first wait on any of them.
    let at_once = Recovery::AllAtOnce.plan(&[Class::Sequencer, Class::Sealer]);
    assert_eq!(classes(&at_once), [vec![Class::Sequencer, Class::Sealer]]);
    assert_eq!(pauses(&at_once), [0]);
    assert_eq!(
        at_once[0].describe(),
        "pause 0s, then sequencer and cluster"
    );
}

#[test]
fn started_nodes_count_as_back_when_their_job_reaches_its_count() {
    assert!(matches!(
        Pending::for_nodes(Class::Sealer),
        Pending::Count {
            job: "cluster",
            count: 3
        }
    ));
    assert_eq!(Class::Sealer.fault_kind(), FaultKind::KillNodes);
    assert_eq!(Class::Sequencer.fault_kind(), FaultKind::KillTasks);
    assert_eq!(Class::Ingress.fault_kind(), FaultKind::KillTasks);
}

#[test]
fn every_job_that_returned_is_checked_for_a_restart_at_the_end() {
    assert_eq!(
        ALL_THREE.restart_proofs(),
        [Class::Ingress, Class::Sequencer]
    );
    assert_eq!(INGRESS_SEALER.restart_proofs(), [Class::Ingress]);
    assert_eq!(
        INGRESS_SEQUENCER.restart_proofs(),
        [Class::Ingress, Class::Sequencer]
    );
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
    let terms = Terms::parse(logs);
    assert_eq!(terms.len(), 2);
    assert_eq!(
        terms,
        Terms(BTreeMap::from([
            (7, BTreeSet::from([1])),
            (8, BTreeSet::from([2]))
        ]))
    );
    assert_eq!(terms.split(), Vec::<String>::new());
    let split = format!(
        "{logs}2026-09-20T14:31:00Z cluster TERM memberId=1 leadershipTermId=8 leaderMemberId=1 logPosition=9 role=LEADER block=40\n"
    );
    assert_eq!(Terms::parse(&split).split(), ["term 8: members {1, 2}"]);
    assert_eq!(Terms::parse("cluster role=LEADER memberId=1").len(), 0);
}

#[test]
fn a_refill_is_a_rise_of_the_republish_counter() {
    let before = Refills {
        republished: Some(3),
    };
    assert!(!before.rose_to(before));
    assert!(before.rose_to(Refills {
        republished: Some(4),
    }));
    // A dark exporter is not a rise.
    assert!(!before.rose_to(Refills { republished: None }));
    assert_eq!(before.describe(), "republished 3");
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
