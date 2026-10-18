use std::time::{Duration, Instant};

use kardamom_cluster_adapter::wire::FeatureActivation;

use super::{ActivationWatch, RULES_AFTER, SealerRules};
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
    assert!(shared.sealer_runs_v3());
}

#[test]
fn a_report_before_its_session_start_counts() {
    let (mut r, shared) = rules();
    r.on_sealer_info(7, 3);
    assert!(!shared.sealer_runs_v3(), "no session yet");
    r.on_session_start(7);
    assert!(shared.sealer_runs_v3());
}

#[test]
fn a_new_session_forgets_the_version_and_an_old_report_never_counts() {
    let (mut r, shared) = rules();
    r.on_session_start(7);
    r.on_sealer_info(7, 3);
    r.on_session_start(8);
    assert!(!shared.sealer_runs_v3());
    r.on_sealer_info(7, 3);
    assert!(!shared.sealer_runs_v3(), "a report of session 7");
    r.on_sealer_info(8, 2);
    assert!(!shared.sealer_runs_v3(), "a report of version 2");
}

#[test]
fn an_early_report_of_another_session_does_not_count() {
    let (mut r, shared) = rules();
    r.on_sealer_info(6, 3);
    r.on_session_start(7);
    assert!(!shared.sealer_runs_v3());
}

/// A sealer under version 2 sends no report. Its switch to version 3
/// sends one in the middle of the session.
#[test]
fn a_session_with_no_report_runs_version_two_until_the_switch() {
    let (mut r, shared) = rules();
    r.on_session_start(7);
    assert!(!shared.sealer_runs_v3());
    r.on_sealer_info(7, 3);
    assert!(shared.sealer_runs_v3());
}

fn activation(activation_ms: u64) -> FeatureActivation {
    FeatureActivation {
        l1_origin: 9,
        feature_id: kardamom_types::upgrades::FEATURE_SEALER_V3,
        activation_ms,
    }
}

#[test]
fn the_watch_warns_once_when_no_report_follows_an_activation() {
    let shared = SharedWatermark::new();
    let mut watch = ActivationWatch::new(0, shared);
    let now = Instant::now();
    watch.check(now + RULES_AFTER * 2);
    assert!(watch.warn_at.is_none(), "no activation, no watch");
    watch.on_offered(activation(0), now);
    watch.check(now + Duration::from_secs(30));
    assert!(watch.warn_at.is_some(), "the time has not passed yet");
    watch.check(now + RULES_AFTER + Duration::from_secs(1));
    assert!(watch.warn_at.is_none(), "warned once");
}

#[test]
fn a_report_of_version_three_ends_the_watch() {
    let shared = SharedWatermark::new();
    let mut watch = ActivationWatch::new(0, shared.clone());
    let now = Instant::now();
    watch.on_offered(activation(0), now);
    shared.set_sealer_version(3);
    watch.check(now + Duration::from_secs(1));
    assert!(watch.warn_at.is_none());
}

#[test]
fn a_scheduled_activation_waits_for_its_time() {
    let mut far = ActivationWatch::new(0, SharedWatermark::new());
    let now = Instant::now();
    far.on_offered(activation(u64::MAX), now);
    far.check(now + RULES_AFTER * 2);
    assert!(far.warn_at.is_some(), "a time far ahead has not passed");
    let mut watch = ActivationWatch::new(0, SharedWatermark::new());
    let in_an_hour = kardamom_types::time::duration_to_ms_saturating(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap(),
    ) + 3_600_000;
    watch.on_offered(activation(in_an_hour), now);
    watch.check(now + RULES_AFTER * 2);
    assert!(
        watch.warn_at.is_some(),
        "the activation time is an hour away"
    );
}
