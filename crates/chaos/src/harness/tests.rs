use std::net::Ipv4Addr;

use kardamom_bench::load::{MetricsTarget, MetricsTargets};

use super::*;
use crate::load::Latency;

#[test]
fn raft_faults_cannot_hide_bad_receipts_or_replica_failures() {
    let mut verdict = Verdict {
        pass: false,
        missing: 0,
        unlanded: 0,
        failures: vec!["receipt had non-0x1 status".into()],
        offered: 10,
        accepted: 10,
        seq_dropped: Some(2),
        latency: Latency::default(),
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
        max_in_flight: NonZeroU32::MIN,
        retry_submit: 0,
        pause_wait: PROBE_PAUSE_WAIT,
        max_gap: 0,
        drain_timeout: Duration::ZERO,
        report_path: PathBuf::new(),
        metrics: MetricsTargets {
            executors: Vec::new(),
            ingress: MetricsTarget::bridged(Ipv4Addr::LOCALHOST, "ingress-1", 9006),
            sequencers: Vec::new(),
        },
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
        unlanded: 0,
        failures: Vec::new(),
        offered: 3000,
        accepted: 3000,
        seq_dropped: None,
        latency: Latency {
            lat_p50_us: 19_841_000,
            ..Latency::default()
        },
    };
    let case = Case::ClusterTotalLossRecover;
    assert!(leg.judge(case, &verdict).is_ok());
    verdict.accepted = 242;
    let err = leg.judge(case, &verdict).unwrap_err().to_string();
    assert!(
        err.contains("accepted 242") && err.contains("fresh sender") && err.contains("p50=19841ms"),
        "{err}"
    );
    verdict.accepted = 3000;
    verdict.missing = 1;
    verdict.unlanded = 1;
    verdict.pass = false;
    let err = leg.judge(case, &verdict).unwrap_err().to_string();
    assert!(
        err.contains("missing=1 of 3000 offered") && err.contains("unlanded=1"),
        "{err}"
    );
}

/// The probe waits out a sealer halt that clears by itself: longer
/// than the 10 s silence budget that starts it.
#[test]
fn the_probe_waits_out_a_paused_ingress_past_the_sealer_silence() {
    assert!(PROBE_PAUSE_WAIT > Duration::from_secs(10));
    assert_eq!(spec().pause_wait, PROBE_PAUSE_WAIT);
}
