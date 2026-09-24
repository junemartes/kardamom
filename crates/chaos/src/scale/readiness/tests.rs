use super::*;

#[test]
fn missing_exporters_and_missing_metrics_are_not_zero() {
    assert!(!LaneReadiness::idle(None, &["pending"]));
    assert!(!LaneReadiness::idle(Some("unrelated 0\n"), &["pending"]));
    assert!(!LaneReadiness::idle(
        Some("pending 0\n"),
        &["pending", "resync"]
    ));
    assert!(!LaneReadiness::idle(
        Some("pending 0\nresync 1\n"),
        &["pending", "resync"]
    ));
    assert!(LaneReadiness::idle(
        Some("pending 0\nresync 0\n"),
        &["pending", "resync"]
    ));
}

#[test]
fn every_vslot_must_be_idle() {
    assert!(!LaneReadiness::idle(
        Some("pending{vslot=\"0\"} 0\npending{vslot=\"1\"} 1\n"),
        &["pending"]
    ));
    assert!(LaneReadiness::idle(
        Some("pending{vslot=\"0\"} 0\npending{vslot=\"1\"} 0\n"),
        &["pending"]
    ));
}

#[tokio::test]
async fn a_replacement_during_sampling_must_supply_its_own_evidence() {
    let resize = Resize {
        cluster_dir: std::path::PathBuf::new(),
        nomad_addr: "http://127.0.0.1:1".into(),
        sequencers: std::collections::BTreeMap::new(),
        tx_ttl: std::time::Duration::from_secs(30),
        target: 1,
        dry_run: false,
        scrape: crate::metrics::Scrape::new(),
    };
    let ready = LaneReadiness {
        resize: &resize,
        nomad: Nomad::new(&resize.nomad_addr).unwrap(),
        job: serde_json::from_value(serde_json::json!({
            "Version":3,"TaskGroups":[{"Name":"seq-0","Count":1}]
        }))
        .unwrap(),
        groups: BTreeSet::from(["seq-0".into()]),
        metrics: &["pending"],
    };
    let before: Vec<Alloc> = serde_json::from_value(serde_json::json!([{
        "ID":"original","TaskGroup":"seq-0","JobVersion":3,"DesiredStatus":"run",
        "ClientStatus":"running","NodeName":"sequencer-0","NodeID":"node","TaskStates":{}
    }]))
    .unwrap();
    assert!(ready.same_allocations(&before, &before));
    let mut replaced = before.clone();
    replaced[0].id = "replacement".into();
    assert!(!ready.same_allocations(&before, &replaced));
    replaced[0].id = "original".into();
    replaced[0].job_version = 4;
    assert!(!ready.same_allocations(&before, &replaced));
    assert!(!ready.same_allocations(&before, &[]));
}
