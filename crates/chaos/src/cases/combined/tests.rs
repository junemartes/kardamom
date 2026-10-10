use std::collections::{BTreeMap, BTreeSet};

use super::checks::{Lookups, Refills, Terms};
use super::*;
use crate::Shard;

/// Every ordering case, in the shard's run order.
const ALL: [Combined; 5] = [
    INGRESS_SEQUENCER,
    INGRESS_SEALER,
    SEQUENCER_SEALER,
    ALL_THREE,
    ALL_THREE_REVERSE,
];

/// Every exec case, in the shard's run order.
const ALL_EXEC: [Combined; 5] = [
    EXECUTOR_SEALER,
    EXECUTOR_SEALER_VALIDATOR,
    INGRESS_EXECUTOR,
    READ_PATH,
    SEQUENCER_EXECUTOR_REDIS,
];

fn classes(plan: &[Wave]) -> Vec<Vec<Class>> {
    plan.iter().map(|w| w.classes.clone()).collect()
}

fn pauses(plan: &[Wave]) -> Vec<u64> {
    plan.iter().map(|w| w.after.as_secs()).collect()
}

/// The classes of a case's return, one wave after the other.
fn order(c: Combined) -> Vec<Class> {
    c.recovery
        .plan(c.down)
        .into_iter()
        .flat_map(|w| w.classes)
        .collect()
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
fn the_exec_rows_return_in_the_order_the_plan_names() {
    // The executors a minute before the sealers.
    assert_eq!(
        order(EXECUTOR_SEALER),
        [Class::ExecutorNodes, Class::Sealer]
    );
    // The sealers, then the executors, then the validator.
    assert_eq!(
        order(EXECUTOR_SEALER_VALIDATOR),
        [Class::Sealer, Class::Executor, Class::Validator]
    );
    // The executors, then the ingresses.
    assert_eq!(
        order(INGRESS_EXECUTOR),
        [Class::ExecutorNodes, Class::Ingress]
    );
    // Redis, then the mirrors, then the executors.
    assert_eq!(
        order(READ_PATH),
        [Class::Redis, Class::StateMirror, Class::Executor]
    );
    // The sequencers, then Redis, then the executors.
    assert_eq!(
        order(SEQUENCER_EXECUTOR_REDIS),
        [Class::Sequencer, Class::Redis, Class::Executor]
    );
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
    assert_eq!(Class::ExecutorNodes.fault_kind(), FaultKind::KillNodes);
    assert_eq!(Class::Redis.fault_kind(), FaultKind::StopJob);
    for class in [
        Class::Executor,
        Class::Validator,
        Class::StateMirror,
        Class::Sequencer,
        Class::Ingress,
    ] {
        assert_eq!(class.fault_kind(), FaultKind::KillTasks, "{class:?}");
    }
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
    // A stopped job is a restored job, so it is checked too; killed
    // nodes are judged by their count.
    assert_eq!(
        READ_PATH.restart_proofs(),
        [Class::Executor, Class::Redis, Class::StateMirror]
    );
    assert_eq!(EXECUTOR_SEALER.restart_proofs(), Vec::<Class>::new());
    assert_eq!(INGRESS_EXECUTOR.restart_proofs(), [Class::Ingress]);
}

#[test]
fn the_expectations_judge_two_readings() {
    let flat = Progress {
        block: 100,
        applied: Some(5000),
    };
    let sealed = Progress {
        block: 103,
        applied: Some(5000),
    };
    let applied = Progress {
        block: 103,
        applied: Some(5010),
    };
    // No executor answers: nothing is applied, and the head still tells
    // a stall from a seal.
    let dark = Progress {
        block: 103,
        applied: None,
    };
    assert!(Expect::Stall.judge(flat, flat).is_ok());
    assert!(Expect::SealOnly.judge(flat, dark).is_ok());
    assert!(Expect::Stall.judge(dark, dark).is_ok());
    assert!(Expect::Stall.judge(flat, dark).is_err());
    // A head that reads lower is not a seal: a source of it went dark.
    let lower = Progress {
        block: 99,
        applied: None,
    };
    assert!(
        Expect::SealOnly
            .judge(flat, lower)
            .unwrap_err()
            .contains("no block was sealed")
    );
    assert_eq!(dark.describe(), "head 103, applied ?");
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
    assert_eq!(
        terms.leaders,
        BTreeMap::from([(7, BTreeSet::from([1])), (8, BTreeSet::from([2]))])
    );
    assert_eq!(terms.reporters, BTreeSet::from([0, 1, 2]));
    assert_eq!(terms.split(), Vec::<String>::new());
    assert_eq!(terms.silent(3), Vec::<u64>::new());
    assert!(terms.assert_one_leader("case", 3).is_ok());
    let split = format!(
        "{logs}2026-09-20T14:31:00Z cluster TERM memberId=1 leadershipTermId=8 leaderMemberId=1 logPosition=9 role=LEADER block=40\n"
    );
    assert_eq!(Terms::parse(&split).split(), ["term 8: members {1, 2}"]);
    assert!(
        Terms::parse("cluster role=LEADER memberId=1")
            .leaders
            .is_empty()
    );
}

#[test]
fn a_member_whose_log_reads_empty_fails_the_term_check() {
    // Member 2's log is empty: no split shows, but its leaders are not
    // known, so the check must not pass.
    let logs = "cluster TERM memberId=0 leadershipTermId=7 leaderMemberId=0 role=LEADER\n\
        cluster TERM memberId=1 leadershipTermId=7 leaderMemberId=0 role=FOLLOWER\n";
    let terms = Terms::parse(logs);
    assert_eq!(terms.silent(3), [2]);
    let err = terms.assert_one_leader("case", 3).unwrap_err().to_string();
    assert!(
        err.contains("members [2] logged no leadership term"),
        "{err}"
    );
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
fn a_floor_is_looked_up_when_a_park_asked_and_a_source_answered() {
    let asked_and_answered = Lookups {
        asked: 2,
        answered: 1,
    };
    assert!(asked_and_answered.looked_up());
    // Never asked: the sender never parked, or the replica guessed.
    assert!(
        !Lookups {
            asked: 0,
            answered: 0
        }
        .looked_up()
    );
    // Asked, no source answered yet.
    assert!(
        !Lookups {
            asked: 3,
            answered: 0
        }
        .looked_up()
    );
}

#[test]
fn the_tables_are_the_shards() {
    let names = |table: &[Combined]| -> Vec<&str> { table.iter().map(|c| c.case.name()).collect() };
    assert_eq!(names(&ALL), Shard::CombinedOrdering.cases());
    // The read-path and the sequencer-and-Redis rows run by name only,
    // until the defects they found are fixed.
    assert_eq!(names(&ALL_EXEC[..3]), Shard::CombinedExec.cases());
    for c in ALL.iter().chain(&ALL_EXEC) {
        // A stall needs the sealers down; a seal-only case keeps them.
        assert_eq!(
            c.expect == Expect::Stall,
            c.down.contains(&Class::Sealer),
            "{}",
            c.case.name()
        );
        assert!(!c.checks.is_empty(), "{}", c.case.name());
    }
    // The mirrors live on the executor nodes, so a case that brings them
    // back before the executors stops the executor job and keeps the
    // nodes.
    assert!(!READ_PATH.down.contains(&Class::ExecutorNodes));
    assert_eq!(Class::Sealer.job(), "cluster");
    assert_eq!(Class::ExecutorNodes.job(), Class::Executor.job());
    assert_eq!(
        [
            Class::Sealer,
            Class::Executor,
            Class::Validator,
            Class::StateMirror,
            Class::Redis,
            Class::Sequencer,
            Class::Ingress
        ]
        .map(Class::count),
        [3, 3, 1, 3, 5, 4, 2]
    );
}
