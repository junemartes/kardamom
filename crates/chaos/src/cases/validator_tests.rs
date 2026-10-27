use std::time::Duration;

use super::Warm;

#[test]
fn an_advancing_validator_without_an_executor_head_cannot_prove_catchup() {
    let observed = Warm::observed([Some(200), Some(1_000), None], Duration::from_secs(6));
    assert!(
        !observed.is_some_and(|warm| warm.verifies_after(Some(199), 15)),
        "an unavailable executor is not a zero-height chain"
    );
}

#[test]
fn complete_samples_still_require_verified_progress_and_bounded_lag() {
    let live = Warm::observed([Some(200), Some(1_000), Some(1_005)], Duration::ZERO).unwrap();
    assert!(!live.verifies_after(None, 15));
    assert!(live.verifies_after(Some(199), 15));
    assert!(!live.verifies_after(Some(200), 15));
    assert!(!live.verifies_after(Some(201), 15));
    assert!(!live.verifies_after(Some(199), 4));
    let zero = Warm::observed([Some(200), Some(1_000), Some(0)], Duration::ZERO).unwrap();
    assert!(!zero.verifies_after(Some(199), 15));
}

#[test]
fn every_required_source_must_answer() {
    assert!(
        [
            [None, Some(1_000), Some(1_005)],
            [Some(200), None, Some(1_005)],
            [Some(200), Some(1_000), None],
        ]
        .into_iter()
        .all(|sample| Warm::observed(sample, Duration::ZERO).is_none())
    );
}
