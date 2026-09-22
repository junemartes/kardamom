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

fn spec() -> LoadSpec {
    LoadSpec {
        rpc_url: "http://ingress-1:8545".into(),
        receipt_rpcs: Vec::new(),
        chain_id: 1,
        account: 0,
        nonce_start: 0,
        completeness: Completeness::Offered,
        fixed_rate: true,
        duration: PROBE_WINDOW,
        tps: NonZeroU32::MIN,
        retry_submit: 0,
        max_gap: 0,
        drain_timeout: Duration::ZERO,
        report_path: PathBuf::new(),
        executor_nodes: Vec::new(),
        ingress_node: String::new(),
        sequencer_nodes: Vec::new(),
    }
}

#[test]
fn a_probe_leg_needs_every_receipt_and_a_rate_and_names_its_meaning() {
    let leg = ProbeLeg {
        who: "the fresh sender #0".into(),
        meaning: "the pipeline does not accept a fresh sender",
        floor: 750,
        spec: spec(),
    };
    let mut verdict = Verdict {
        pass: true,
        missing: 0,
        failures: Vec::new(),
        offered: 3000,
        accepted: 3000,
        seq_dropped: None,
    };
    let case = Case::ClusterTotalLossRecover;
    assert!(leg.judge(case, &verdict).is_ok());
    verdict.accepted = 242;
    let err = leg.judge(case, &verdict).unwrap_err().to_string();
    assert!(
        err.contains("accepted 242") && err.contains("fresh sender"),
        "{err}"
    );
    verdict.accepted = 3000;
    verdict.missing = 1;
    verdict.pass = false;
    assert!(leg.judge(case, &verdict).is_err());
}
