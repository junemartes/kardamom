use super::*;

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
    // These cases need a small DA budget or the local canary, set for their own fault window.
    assert_eq!(
        unscheduled,
        [Case::DaLagHalt, Case::CanaryDaLag, Case::PruneFloor]
    );
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
