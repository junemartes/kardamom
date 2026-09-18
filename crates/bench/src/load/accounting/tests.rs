use super::*;

fn snap(exec: &[(&str, u64)], sealer: u64) -> MetricsSnapshot {
    MetricsSnapshot {
        executor_blocks: exec
            .iter()
            .map(|(n, b)| ((*n).to_string(), Some(*b)))
            .collect(),
        sealer_block: Some(sealer),
        ..Default::default()
    }
}

fn counts(offered: u64, accepted: u64, receipted: u64, bad: u64) -> Counts {
    Counts {
        offered,
        accepted,
        receipted,
        bad_status: bad,
    }
}

/// The clean-run baseline `EvalInput`: 300 offered, accepted, and
/// receipted with no bad statuses, no missing or unlanded
/// transactions, no recheck sample, `max_gap` 5, strict delivery,
/// no feed-ack trust, and soak (non-chaos) mode. Every test below
/// overrides only the fields its scenario varies, with
/// `..base_input(&base, &fin)`.
pub(crate) fn base_input<'a>(base: &'a MetricsSnapshot, fin: &'a MetricsSnapshot) -> EvalInput<'a> {
    EvalInput {
        counts: counts(300, 300, 300, 0),
        missing: 0,
        unlanded: 0,
        base,
        fin,
        recheck: None,
        max_gap: 5,
        assert_all_delivered: true,
        ack_proves_receipt: false,
        chaos_mode: false,
    }
}

#[test]
fn clean_run_passes() {
    let base = snap(&[("exec-0", 10), ("exec-1", 10)], 10);
    let fin = snap(&[("exec-0", 50), ("exec-1", 49)], 51);
    let v = evaluate(&base_input(&base, &fin));
    assert!(v.pass, "expected pass, failures: {:?}", v.failures);
}

#[test]
fn missing_receipt_fails_under_assert() {
    let base = snap(&[("exec-0", 10)], 10);
    let fin = snap(&[("exec-0", 50)], 50);
    let v = evaluate(&EvalInput {
        counts: counts(300, 300, 299, 0),
        missing: 1,
        ..base_input(&base, &fin)
    });
    assert!(!v.pass);
    assert!(v.failures.iter().any(|f| f.contains("must-deliver")));
}

#[test]
fn frozen_executor_fails_even_in_chaos() {
    let base = snap(&[("exec-0", 10), ("exec-1", 10)], 10);
    // exec-1 never advances, while the sealer does.
    let fin = snap(&[("exec-0", 50), ("exec-1", 10)], 50);
    let v = evaluate(&EvalInput {
        chaos_mode: true,
        ..base_input(&base, &fin)
    });
    assert!(!v.pass);
    assert!(v.failures.iter().any(|f| f.contains("FROZEN")));
}

#[test]
fn gap_is_soft_in_chaos_hard_in_soak() {
    let base = snap(&[("exec-0", 10)], 10);
    // The executor advances but lags the sealer by 40, above max_gap of 5.
    let fin = snap(&[("exec-0", 20)], 60);
    let soak = evaluate(&base_input(&base, &fin));
    assert!(!soak.pass, "gap should fail in soak mode");
    let chaos = evaluate(&EvalInput {
        chaos_mode: true,
        ..base_input(&base, &fin)
    });
    assert!(
        chaos.pass,
        "gap should be soft in chaos mode: {:?}",
        chaos.failures
    );
}

#[test]
fn restarted_executor_with_moving_recheck_is_recovering_not_frozen() {
    // exec-1 was hard-killed: its gauge reset from 10 to 3, so advanced
    // is negative. The recheck sample shows it replaying, from 3 to 8,
    // so the verdict is RECOVERING, not FROZEN.
    let base = snap(&[("exec-0", 10), ("exec-1", 10)], 10);
    let fin = snap(&[("exec-0", 50), ("exec-1", 3)], 50);
    let recheck = snap(&[("exec-0", 51), ("exec-1", 8)], 51);
    let v = evaluate(&EvalInput {
        recheck: Some(&recheck),
        chaos_mode: true,
        ..base_input(&base, &fin)
    });
    assert!(v.pass, "expected pass, failures: {:?}", v.failures);
    let kp = v.keep_pace.iter().find(|k| k.node == "exec-1").unwrap();
    assert_eq!(kp.verdict, "RECOVERING");
}

#[test]
fn restarted_executor_with_stalled_recheck_is_frozen() {
    // The gauge reset, and did not move by the recheck sample: still FROZEN.
    let base = snap(&[("exec-0", 10), ("exec-1", 10)], 10);
    let fin = snap(&[("exec-0", 50), ("exec-1", 3)], 50);
    let recheck = snap(&[("exec-0", 51), ("exec-1", 3)], 51);
    let v = evaluate(&EvalInput {
        recheck: Some(&recheck),
        chaos_mode: true,
        ..base_input(&base, &fin)
    });
    assert!(!v.pass);
    assert!(v.failures.iter().any(|f| f.contains("FROZEN")));
}

#[test]
fn sequencer_drop_fails() {
    let mut base = snap(&[("exec-0", 10)], 10);
    base.seq_dropped_past = Some(0);
    let mut fin = snap(&[("exec-0", 50)], 50);
    fin.seq_dropped_past = Some(2);
    let v = evaluate(&base_input(&base, &fin));
    assert!(!v.pass);
    assert!(v.failures.iter().any(|f| f.contains("sequencer dropped")));
}

#[test]
fn sequencer_drop_is_soft_in_chaos() {
    // Retry noise across an ingress restart can double-submit. The drop
    // delta is reported, but must not fail a chaos run.
    let mut base = snap(&[("exec-0", 10)], 10);
    base.seq_dropped_past = Some(0);
    let mut fin = snap(&[("exec-0", 50)], 50);
    fin.seq_dropped_past = Some(2);
    let v = evaluate(&EvalInput {
        chaos_mode: true,
        ..base_input(&base, &fin)
    });
    assert!(v.pass, "expected pass, failures: {:?}", v.failures);
    assert_eq!(v.seq_dropped, Some(2), "delta still reported");
}
