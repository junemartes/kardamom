use super::*;

const EVERY_META: [&str; 10] = [
    "nonce_burn",
    "chain_state_install",
    "sealer_v4",
    "first_receipt",
    "sealer_version_kept",
    "v2_loop",
    "reads_egress",
    "ref_mark",
    "relayed_full_ref",
    "exec_frames",
];

fn job(version: u64) -> JobFacts {
    JobFacts {
        version,
        metas: EVERY_META.iter().map(|m| (*m).to_string()).collect(),
        live: vec![version, version],
    }
}

fn member(version: i64) -> MemberStatus {
    MemberStatus {
        reads_max: 18,
        version,
        store: "heap".into(),
    }
}

/// A cluster on which every upgrade passes.
fn healthy() -> Facts {
    let groups: Vec<String> = (0..3).map(|i| format!("cluster-{i}")).collect();
    Facts {
        members: groups
            .iter()
            .map(|g| (g.clone(), Some(member(5))))
            .collect(),
        groups: Some(groups),
        jobs: [
            "sequencer",
            "executor",
            "validator",
            "ingress",
            "batcher",
            "egress",
        ]
        .into_iter()
        .map(|name| (name, Some(job(4))))
        .collect(),
        held: vec![Some(-1); 3],
        window: WindowFacts {
            marks: vec![Some((40, 900)), Some((42, 910))],
            batcher: vec![Some(43)],
            validator: vec![Some(50)],
        },
    }
}

fn refuses(facts: &Facts, upgrade: Upgrade, fragment: &str) -> bool {
    facts
        .refusals(upgrade)
        .iter()
        .any(|refusal| refusal.contains(fragment))
}

#[test]
fn a_healthy_cluster_passes_every_upgrade_of_the_carry_shard() {
    let facts = healthy();
    for upgrade in [
        Upgrade::on(3),
        Upgrade::on(4),
        Upgrade::off(4),
        Upgrade::on(5),
        Upgrade::off(5),
    ] {
        assert_eq!(facts.refusals(upgrade), Vec::<String>::new(), "{upgrade}");
    }
}

#[test]
fn feature_two_takes_only_the_activation_time_zero() {
    assert!(refuses(&healthy(), Upgrade::off(2), "feature 2"));
    assert!(!refuses(&healthy(), Upgrade::on(2), "feature 2"));
}

#[test]
fn an_unknown_sealer_job_refuses() {
    let facts = Facts {
        groups: None,
        ..healthy()
    };
    assert!(refuses(&facts, Upgrade::on(3), "sealer job"));
}

#[test]
fn a_configured_member_that_does_not_run_or_answer_refuses() {
    let mut down = healthy();
    down.members.remove("cluster-1");
    assert!(refuses(&down, Upgrade::on(5), "do not run"));
    let mut silent = healthy();
    silent.members.insert("cluster-2".into(), None);
    assert!(refuses(&silent, Upgrade::on(3), "do not answer"));
}

#[test]
fn each_feature_needs_its_snapshot_version_on_every_member() {
    for (reads, feature, refused) in [
        (13, 2, true),
        (14, 3, true),
        (15, 3, false),
        (15, 4, true),
        (16, 4, false),
        (17, 5, true),
    ] {
        let mut facts = healthy();
        let status = facts
            .members
            .get_mut("cluster-0")
            .unwrap()
            .as_mut()
            .unwrap();
        status.reads_max = reads;
        assert_eq!(
            refuses(&facts, Upgrade::on(feature), "snapshot version"),
            refused,
            "reads {reads}, feature {feature}"
        );
    }
}

#[test]
fn feature_five_needs_version_five_and_the_heap_store_on_every_member() {
    let mut early = healthy();
    early.members.insert("cluster-1".into(), Some(member(4)));
    assert!(refuses(&early, Upgrade::on(5), "decision version 5"));
    let mut file = healthy();
    file.members
        .get_mut("cluster-2")
        .unwrap()
        .as_mut()
        .unwrap()
        .store = "file".into();
    assert!(refuses(&file, Upgrade::on(5), "on the heap"));
    assert!(!refuses(&file, Upgrade::on(3), "on the heap"));
}

#[test]
fn feature_five_needs_every_egress_mark_and_every_reader_past_it() {
    let mut silent = healthy();
    silent.window.marks.push(None);
    assert!(refuses(&silent, Upgrade::on(5), "reference mark"));
    let mut none = healthy();
    none.window.marks.clear();
    assert!(refuses(&none, Upgrade::on(5), "reference mark"));
    let mut behind = healthy();
    behind.window.batcher = vec![Some(42)];
    assert!(refuses(&behind, Upgrade::on(5), "batcher cursors"));
    let mut dark = healthy();
    dark.window.validator = vec![Some(-1)];
    assert!(refuses(&dark, Upgrade::on(5), "validator cursors"));
    let mut absent = healthy();
    absent.window.validator.clear();
    assert!(refuses(&absent, Upgrade::on(5), "validator cursors"));
    assert!(!refuses(&behind, Upgrade::on(4), "batcher cursors"));
}

#[test]
fn feature_three_or_four_off_refuses_while_a_member_holds_feature_five_or_is_silent() {
    for held in [Some(1), Some(2), None] {
        let mut facts = healthy();
        facts.held[1] = held;
        assert!(
            refuses(&facts, Upgrade::off(4), "holds feature 5"),
            "{held:?}"
        );
        assert!(
            refuses(&facts, Upgrade::off(3), "holds feature 5"),
            "{held:?}"
        );
        assert!(
            !refuses(&facts, Upgrade::on(4), "holds feature 5"),
            "{held:?}"
        );
    }
}

#[test]
fn a_service_without_its_meta_or_on_an_older_version_refuses() {
    let mut meta = healthy();
    let ingress = meta.jobs.get_mut("ingress").unwrap().as_mut().unwrap();
    ingress.metas.remove("nonce_burn");
    assert!(refuses(&meta, Upgrade::on(4), "ingress job"));
    let mut older = healthy();
    older
        .jobs
        .get_mut("executor")
        .unwrap()
        .as_mut()
        .unwrap()
        .live = vec![4, 3];
    assert!(refuses(&older, Upgrade::on(4), "executor job"));
    let mut idle = healthy();
    idle.jobs
        .get_mut("validator")
        .unwrap()
        .as_mut()
        .unwrap()
        .live = Vec::new();
    assert!(refuses(&idle, Upgrade::on(4), "validator job"));
    let mut unknown = healthy();
    unknown.jobs.insert("sequencer", None);
    assert!(refuses(&unknown, Upgrade::on(4), "sequencer job"));
}

#[test]
fn the_services_of_version_four_and_of_the_window_count_only_for_their_feature() {
    let mut v4 = healthy();
    let executor = v4.jobs.get_mut("executor").unwrap().as_mut().unwrap();
    executor.metas.remove("first_receipt");
    assert!(refuses(&v4, Upgrade::on(3), "executor job"));
    assert!(!refuses(&v4, Upgrade::on(4), "executor job"));
    let mut window = healthy();
    let batcher = window.jobs.get_mut("batcher").unwrap().as_mut().unwrap();
    batcher.metas.remove("reads_egress");
    assert!(refuses(&window, Upgrade::on(5), "batcher job"));
    assert!(!refuses(&window, Upgrade::on(4), "batcher job"));
    let mut mark = healthy();
    let egress = mark.jobs.get_mut("egress").unwrap().as_mut().unwrap();
    egress.metas.remove("ref_mark");
    assert!(refuses(&mark, Upgrade::on(5), "egress job"));
}

#[test]
fn the_job_facts_keep_only_true_metas_and_live_allocations() {
    let definition = serde_json::json!({
        "Version": 7,
        "Meta": {"nonce_burn": "true", "v2_loop": "false"},
    });
    let alloc = |status: &str, version: u64| -> Alloc {
        serde_json::from_value(serde_json::json!({
            "ID": "a", "TaskGroup": "g", "ClientStatus": status, "DesiredStatus": "run",
            "JobVersion": version, "NodeName": "n", "NodeID": "i",
        }))
        .unwrap()
    };
    let facts = JobFacts::of(
        &definition,
        &[
            alloc("running", 7),
            alloc("pending", 7),
            alloc("complete", 6),
        ],
    );
    assert_eq!(facts.version, 7);
    assert_eq!(facts.metas, BTreeSet::from(["nonce_burn".to_string()]));
    assert_eq!(facts.live, vec![7, 7]);
}

#[test]
fn a_member_status_without_a_field_takes_the_default_of_the_play() {
    let status = MemberStatus::of(&serde_json::json!({"decisionVersion": 5}));
    assert_eq!(
        (status.reads_max, status.version, status.store.as_str()),
        (0, 5, "none")
    );
}
