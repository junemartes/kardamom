use super::*;

#[test]
fn broken_image_traffic_covers_both_full_deployments_and_the_recovery_tail() {
    let knobs = crate::knobs::Knobs::read(&[]).unwrap();
    let window = Case::DeployBrokenImage.window(&knobs);
    // The fault and healing passes take 1232 and 1093 seconds. The
    // common tail must still observe case-owned traffic afterwards.
    let observed = std::time::Duration::from_secs(1232 + 1093);
    assert!(window >= knobs.inject_delay + observed + knobs.reschedule_slo);
    let mut longer = knobs.clone();
    longer.case_window = window + std::time::Duration::from_mins(1);
    assert_eq!(Case::DeployBrokenImage.window(&longer), longer.case_window);
}

#[test]
fn every_shard_case_parses_and_names_round_trip() {
    let shards = [
        crate::Shard::Executor,
        crate::Shard::Ingress,
        crate::Shard::Sequencer,
        crate::Shard::Cluster,
        crate::Shard::Fleet,
        crate::Shard::Coordinated,
        crate::Shard::CombinedOrdering,
        crate::Shard::CombinedExec,
        crate::Shard::Retention,
        crate::Shard::Cache,
        crate::Shard::L1,
        crate::Shard::Follower,
        crate::Shard::Integrity,
        crate::Shard::Carry,
    ];
    for shard in shards {
        shard
            .cases()
            .iter()
            .for_each(|name| assert_eq!(Case::parse(name).unwrap().name(), *name));
    }
    let unscheduled: Vec<_> = ALL
        .iter()
        .filter(|case| {
            !shards
                .iter()
                .any(|shard| shard.cases().contains(&case.name()))
        })
        .copied()
        .collect();
    assert!(unscheduled.is_empty());
    shards
        .iter()
        .try_for_each(|shard| {
            let mut accounts = crate::accounts::Accounts::new(7, false);
            shard.cases().iter().try_for_each(|name| {
                let case = Case::parse(name)?;
                accounts.take(case.pin(), name, |_| true).map(|_| ())
            })
        })
        .unwrap();
    assert!(Case::parse("sealer-hard").is_err());
}
