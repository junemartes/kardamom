use super::*;

#[test]
fn raft_faults_cannot_hide_bad_receipts_or_replica_failures() {
    let mut verdict = Verdict {
        pass: false,
        missing: 0,
        failures: vec!["receipt had non-0x1 status".into()],
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
