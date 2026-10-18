use super::SealerRules;
use crate::resync::SharedWatermark;

fn rules() -> (SealerRules, SharedWatermark) {
    let shared = SharedWatermark::new();
    (SealerRules::new(0, shared.clone()), shared)
}

#[test]
fn a_report_after_its_session_start_counts() {
    let (mut r, shared) = rules();
    r.on_session_start(7);
    r.on_sealer_info(7, 3);
    assert!(shared.sealer_takes_releases());
}

#[test]
fn a_report_before_its_session_start_counts() {
    let (mut r, shared) = rules();
    r.on_sealer_info(7, 3);
    assert!(!shared.sealer_takes_releases(), "no session yet");
    r.on_session_start(7);
    assert!(shared.sealer_takes_releases());
}

#[test]
fn a_new_session_forgets_the_version_and_an_old_report_never_counts() {
    let (mut r, shared) = rules();
    r.on_session_start(7);
    r.on_sealer_info(7, 3);
    r.on_session_start(8);
    assert!(!shared.sealer_takes_releases());
    r.on_sealer_info(7, 3);
    assert!(!shared.sealer_takes_releases(), "a report of session 7");
    r.on_sealer_info(8, 2);
    assert!(
        !shared.sealer_takes_releases(),
        "version 2 takes no release"
    );
}

#[test]
fn an_early_report_of_another_session_does_not_count() {
    let (mut r, shared) = rules();
    r.on_sealer_info(6, 3);
    r.on_session_start(7);
    assert!(!shared.sealer_takes_releases());
}

/// A sealer under version 2 sends no report. Its switch to version 3
/// sends one in the middle of the session.
#[test]
fn a_session_with_no_report_takes_no_release_until_the_switch() {
    let (mut r, shared) = rules();
    r.on_session_start(7);
    assert!(!shared.sealer_takes_releases());
    r.on_sealer_info(7, 3);
    assert!(shared.sealer_takes_releases());
}
