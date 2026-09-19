use super::*;

#[test]
fn raft_faults_cannot_hide_bad_receipts_or_replica_failures() {
    let mut verdict = Verdict {
        pass: false,
        missing: 0,
        failures: vec!["receipt had non-0x1 status".into()],
        offered: 10,
        accepted: 10,
        seq_dropped: Some(2),
    };
    assert!(Case::ClusterLeaderKill.judge_load(&verdict).is_err());
    verdict.failures = vec!["executor unreachable".into()];
    assert!(Case::ClusterQuorumLossRecover.judge_load(&verdict).is_err());
    verdict.pass = true;
    verdict.failures.clear();
    assert!(Case::ClusterLeaderKill.judge_load(&verdict).is_ok());
    verdict.missing = 1;
    assert!(Case::ClusterLeaderKill.judge_load(&verdict).is_err());
    assert!(Case::HardExecutor.judge_load(&verdict).is_err());
}

#[test]
fn the_recovery_probe_needs_every_receipt_and_a_rate() {
    let mut verdict = Verdict {
        pass: true,
        missing: 0,
        failures: Vec::new(),
        offered: 6000,
        accepted: 6000,
        seq_dropped: None,
    };
    assert!(
        Case::ClusterTotalLossRecover
            .judge_probe(&verdict, 1500)
            .is_ok()
    );
    verdict.accepted = 242;
    let err = Case::ClusterTotalLossRecover
        .judge_probe(&verdict, 1500)
        .unwrap_err()
        .to_string();
    assert!(err.contains("accepted 242"), "{err}");
    verdict.accepted = 6000;
    verdict.missing = 1;
    verdict.pass = false;
    assert!(
        Case::ClusterTotalLossRecover
            .judge_probe(&verdict, 1500)
            .is_err()
    );
}
