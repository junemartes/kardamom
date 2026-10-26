use std::collections::{BTreeMap, BTreeSet};

use super::checks::{Lookups, Terms};
use super::refill::{Refill, RefillWatch, Refills, WatcherRuns};
use super::*;
use crate::Shard;
use crate::probes::Boundaries;

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
        ALL_THREE.placed(ExecutorNodes::Executor).restart_proofs(),
        [Class::Ingress, Class::Sequencer]
    );
    assert_eq!(
        INGRESS_SEALER
            .placed(ExecutorNodes::Executor)
            .restart_proofs(),
        [Class::Ingress]
    );
    assert_eq!(
        INGRESS_SEQUENCER
            .placed(ExecutorNodes::Executor)
            .restart_proofs(),
        [Class::Ingress, Class::Sequencer]
    );
    // A stopped job is a restored job, so it is checked too; killed
    // nodes are judged by their count.
    assert_eq!(
        READ_PATH.placed(ExecutorNodes::Executor).restart_proofs(),
        [Class::Executor, Class::Redis, Class::StateMirror]
    );
    assert_eq!(
        EXECUTOR_SEALER
            .placed(ExecutorNodes::Executor)
            .restart_proofs(),
        Vec::<Class>::new()
    );
    assert_eq!(
        INGRESS_EXECUTOR
            .placed(ExecutorNodes::Executor)
            .restart_proofs(),
        [Class::Ingress]
    );
}

#[test]
fn on_the_sealer_nodes_the_executors_and_the_members_go_down_by_task() {
    let carry = |c: Combined| c.placed(ExecutorNodes::Sealer);
    // The executors return a minute before the members, and wait.
    let placed = carry(EXECUTOR_SEALER);
    assert_eq!(placed.down, [Class::Executor, Class::SealerTasks]);
    assert_eq!(
        Recovery::Reverse(Duration::from_secs(60))
            .plan(&placed.down)
            .into_iter()
            .flat_map(|w| w.classes)
            .collect::<Vec<_>>(),
        [Class::Executor, Class::SealerTasks]
    );
    assert_eq!(
        placed.restart_proofs(),
        [Class::Executor, Class::SealerTasks]
    );
    assert!(placed.executors_down() && placed.sealers_down());
    assert_eq!(
        carry(EXECUTOR_SEALER_VALIDATOR).down,
        [Class::Executor, Class::SealerTasks, Class::Validator]
    );
    assert_eq!(
        carry(INGRESS_EXECUTOR).down,
        [Class::Executor, Class::Ingress]
    );
    // A node kill of the members alone also takes the executors on the
    // nodes, so the case waits for their exporters.
    let sealers = carry(INGRESS_SEALER);
    assert_eq!(sealers.down, [Class::Ingress, Class::Sealer]);
    assert!(sealers.executors_down());
    assert!(
        !INGRESS_SEALER
            .placed(ExecutorNodes::Executor)
            .executors_down()
    );
    assert_eq!(Class::SealerTasks.job(), "cluster");
    assert_eq!(Class::SealerTasks.fault_kind(), FaultKind::KillTasks);
}

/// A reading with no member answer.
fn progress(block: i64, applied: Option<i64>) -> Progress {
    Progress {
        block,
        boundaries: None,
        applied,
    }
}

#[test]
fn the_expectations_judge_two_readings() {
    let flat = progress(100, Some(5000));
    let sealed = progress(103, Some(5000));
    let applied = progress(103, Some(5010));
    // No executor answers: nothing is applied, and the head still tells
    // a stall from a seal.
    let dark = progress(103, None);
    assert!(Expect::Stall.judge(flat, flat).is_ok());
    assert!(Expect::SealOnly.judge(flat, dark).is_ok());
    assert!(Expect::Stall.judge(dark, dark).is_ok());
    assert!(Expect::Stall.judge(flat, dark).is_err());
    // A head that reads lower is not a seal: a source of it went dark.
    let lower = progress(99, None);
    assert!(
        Expect::SealOnly
            .judge(flat, lower)
            .unwrap_err()
            .contains("no block was sealed")
    );
    assert_eq!(dark.describe(), "head 103, sealer boundaries ?, applied ?");
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
fn a_seal_only_outage_with_no_client_is_judged_by_the_members() {
    // The executors and the ingresses are down, and the validator reads a
    // feed that only the executors fill: the head stays flat.
    let at = |count: Option<i64>| Progress {
        block: 931,
        boundaries: count.map(Boundaries::Members),
        applied: None,
    };
    assert!(
        Expect::SealOnly
            .judge(at(Some(4810)), at(Some(4870)))
            .is_ok()
    );
    let why = Expect::SealOnly
        .judge(at(Some(4810)), at(Some(4810)))
        .unwrap_err();
    assert_eq!(
        why,
        "no block was sealed while the sealers were up (head 931, sealer boundaries 4810, applied ? -> head 931, sealer boundaries 4810, applied ?)"
    );
    // One reading with no member answer proves no seal.
    assert!(Expect::SealOnly.judge(at(None), at(Some(4870))).is_err());
    assert!(Expect::SealOnly.judge(at(Some(4810)), at(None)).is_err());
    // A user transaction still fails the case while the members tick.
    let applied = |boundaries, n| Progress {
        applied: Some(n),
        ..at(Some(boundaries))
    };
    assert!(
        Expect::SealOnly
            .judge(applied(4810, 5000), applied(4870, 5010))
            .unwrap_err()
            .contains("a user transaction was applied")
    );
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

/// The counters of the da-watcher.
fn refills(republished: Option<i64>, published: Option<i64>) -> Refills {
    Refills {
        republished,
        published,
    }
}

/// The refills that a watch from `base` shows for each reading.
fn watched(base: Refills, readings: &[Refills]) -> Vec<Option<Refill>> {
    let mut watch = RefillWatch::new(base);
    readings.iter().map(|now| watch.step(*now)).collect()
}

#[test]
fn a_refill_is_a_rise_of_the_republish_counter() {
    let before = refills(Some(3), Some(200));
    assert_eq!(
        watched(before, &[before, refills(Some(4), Some(201))]),
        [None, Some(Refill::Republished)]
    );
    // A dark exporter is not a rise, and not a drop.
    assert_eq!(
        watched(before, &[refills(None, None), refills(None, Some(230))]),
        [None, None]
    );
    // Publishes with no republish and no drop show no refill.
    assert_eq!(watched(before, &[refills(Some(3), Some(230))]), [None]);
    assert_eq!(before.describe(), "republished 3, published 200");
    assert_eq!(refills(None, None).describe(), "republished ?, published ?");
}

#[test]
fn a_restarted_da_watcher_needs_a_rise_after_the_drop() {
    let before = refills(Some(5), Some(200));
    // The new process published 3 epochs before the first reading,
    // perhaps into no queue. The drop moves the baseline, so they count
    // for nothing.
    let drop = refills(Some(0), Some(3));
    assert_eq!(watched(before, &[drop, drop, drop]), [None, None, None]);
    // A publish after the drop is the refill.
    assert_eq!(
        watched(before, &[drop, refills(Some(0), Some(4))]),
        [None, Some(Refill::Restarted)]
    );
    // A republish after the drop is a republish.
    assert_eq!(
        watched(before, &[drop, refills(Some(1), Some(3))]),
        [None, Some(Refill::Republished)]
    );
    // The published counter shows the drop also with no republish before
    // it.
    assert_eq!(
        watched(
            refills(Some(0), Some(200)),
            &[drop, refills(Some(0), Some(4))]
        ),
        [None, Some(Refill::Restarted)]
    );
}

/// A fail-fast exit line of the da-watcher at `at`, with `msg`.
fn exit_line(at: &str, msg: &str) -> String {
    format!(
        "{at} ERROR kardamom_log::fail_fast: aeron client error; exiting (fail-fast) code=1000 msg=\"{msg}\""
    )
}

#[test]
fn a_restart_is_judged_by_its_driver_loss() {
    let alloc = || "c683773a-aac6-9a63-af79-0d4889810fca".to_string();
    let base = WatcherRuns::new([(alloc(), 0)], "start\n");
    assert_eq!(base.judge(&base), Ok(None));
    let loss = exit_line("22:24:10", "MediaDriver has been shutdown");
    let after_loss = WatcherRuns::new([(alloc(), 1)], &format!("start\n{loss}\n"));
    let note = after_loss.judge(&base).unwrap().unwrap();
    assert!(note.contains("restarted 1 times"), "{note}");
    assert!(note.contains("c683773a restarted 1 times"), "{note}");
    assert!(note.contains("MediaDriver has been shutdown"), "{note}");
    // A restart with no driver loss fails.
    let stall = exit_line("22:24:10", "service interval exceeded");
    let stalled = WatcherRuns::new([(alloc(), 1)], &format!("start\n{stall}\n"));
    let err = stalled.judge(&base).unwrap_err();
    assert!(
        err.contains("a driver loss explains 0 of the 1 restarts"),
        "{err}"
    );
    // An exit before the baseline explains no restart after it.
    let old = WatcherRuns::new([(alloc(), 1)], &format!("{loss}\n"));
    assert!(
        WatcherRuns::new([(alloc(), 2)], &format!("{loss}\n"))
            .judge(&old)
            .is_err()
    );
    // A new allocation is a restart.
    let moved = WatcherRuns::new([("6479b28f-0000".to_string(), 0)], &format!("{loss}\n"));
    assert!(moved.judge(&base).unwrap().is_some());
}

/// The starts that find no live driver after a driver loss are part of
/// that loss. With no driver loss, they explain nothing.
#[test]
fn a_start_with_no_live_driver_after_a_loss_is_part_of_the_loss() {
    let alloc = || "c683773a-aac6-9a63-af79-0d4889810fca".to_string();
    let base = WatcherRuns::new([(alloc(), 0)], "start\n");
    let loss = exit_line(
        "08:24:33",
        "MediaDriver keepalive: age=30009ms > timeout=30000ms",
    );
    let no_cnc = "Error: spawn AeronRuntime\n    [aeron_client_connect_to_driver, aeronc.c:59] CnC file not created: /opt/kardamom/aeron-mount/dir/cnc.dat\n";
    let no_beat = "Error: spawn events runtime\n    [aeron_client_connect_to_driver, aeronc.c:119] no driver heartbeat detected after: 30000ms\n";
    let after_loss = format!("start\n{loss}\n{no_cnc}{no_cnc}{no_beat}");
    let note = WatcherRuns::new([(alloc(), 4)], &after_loss)
        .judge(&base)
        .unwrap()
        .unwrap();
    assert!(note.contains("starts with no live driver: 3"), "{note}");
    let one_more = WatcherRuns::new([(alloc(), 5)], &after_loss)
        .judge(&base)
        .unwrap_err();
    assert!(
        one_more.contains("a driver loss explains 4 of the 5 restarts"),
        "{one_more}"
    );
    let no_loss = WatcherRuns::new([(alloc(), 1)], &format!("start\n{no_cnc}"))
        .judge(&base)
        .unwrap_err();
    assert!(
        no_loss.contains("a driver loss explains 0 of the 1 restarts"),
        "{no_loss}"
    );
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
    assert_eq!(names(&ALL_EXEC), Shard::CombinedExec.cases());
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

#[test]
fn floor_answers_include_both_versions_of_the_committed_live_layer() {
    for source in ["local", "live", "redis", "ok"] {
        let metrics = format!(
            "kardamom_sequencer_nonce_lookup_requests_total{{partition=\"0\"}} 2\n\
             kardamom_sequencer_nonce_lookups_total{{partition=\"0\",outcome=\"{source}\"}} 1\n"
        );
        assert!(Lookups::parse(&metrics).looked_up(), "{source}");
    }
    let failed = "kardamom_sequencer_nonce_lookup_requests_total 3\n\
        kardamom_sequencer_nonce_lookups_total{outcome=\"error\"} 2\n\
        kardamom_sequencer_nonce_lookups_total{outcome=\"timeout\"} 1\n";
    assert!(!Lookups::parse(failed).looked_up());
}
