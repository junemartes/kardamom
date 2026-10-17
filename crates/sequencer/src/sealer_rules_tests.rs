use std::time::{Duration, Instant};

use super::SealerRules;
use crate::resync::SharedWatermark;

fn rules() -> (SealerRules, SharedWatermark, Instant) {
    let shared = SharedWatermark::new();
    let now = Instant::now();
    (SealerRules::new(0, shared.clone(), now), shared, now)
}

#[test]
fn a_report_after_its_session_start_counts() {
    let (mut r, shared, now) = rules();
    r.on_session_start(7, now);
    r.on_sealer_info(7, 3, now);
    assert!(shared.sealer_takes_releases());
}

#[test]
fn a_report_before_its_session_start_counts() {
    let (mut r, shared, now) = rules();
    r.on_sealer_info(7, 3, now);
    assert!(!shared.sealer_takes_releases(), "no session yet");
    r.on_session_start(7, now);
    assert!(shared.sealer_takes_releases());
}

#[test]
fn a_new_session_forgets_the_version_and_an_old_report_never_counts() {
    let (mut r, shared, now) = rules();
    r.on_session_start(7, now);
    r.on_sealer_info(7, 3, now);
    r.on_session_start(8, now);
    assert!(!shared.sealer_takes_releases());
    r.on_sealer_info(7, 3, now);
    assert!(!shared.sealer_takes_releases(), "a report of session 7");
    r.on_sealer_info(8, 2, now);
    assert!(
        !shared.sealer_takes_releases(),
        "version 2 takes no release"
    );
}

#[test]
fn an_early_report_of_another_session_does_not_count() {
    let (mut r, shared, now) = rules();
    r.on_sealer_info(6, 3, now);
    r.on_session_start(7, now);
    assert!(!shared.sealer_takes_releases());
}

#[test]
fn the_check_warns_once_and_a_report_ends_the_wait() {
    let (mut r, _shared, now) = rules();
    r.on_session_start(7, now);
    r.check(now + Duration::from_secs(30));
    assert!(r.missing_since.is_some());
    r.check(now + Duration::from_secs(61));
    assert!(r.missing_since.is_none(), "warned once");
    r.on_session_start(8, now);
    r.on_sealer_info(8, 3, now);
    assert!(r.missing_since.is_none());
}
