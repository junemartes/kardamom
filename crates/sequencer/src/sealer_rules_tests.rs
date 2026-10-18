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

fn activation(l1_origin: u64, activation_ms: u64) -> FeatureActivation {
    FeatureActivation {
        l1_origin,
        feature_id: kardamom_types::upgrades::FEATURE_SEALER_V3,
        activation_ms,
    }
}

fn watch() -> (ActivationWatch, SharedWatermark) {
    let shared = SharedWatermark::new();
    (ActivationWatch::new(0, shared.clone()), shared)
}

/// The last switch that the watch settled.
fn settled(watch: &ActivationWatch) -> Option<Instant> {
    watch.watched.as_ref().and_then(|w| w.settled)
}

fn unix_ms() -> u64 {
    kardamom_types::time::duration_to_ms_saturating(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap(),
    )
}

#[test]
fn the_watch_warns_once_when_no_report_follows_an_activation() {
    let (mut watch, _shared) = watch();
    let now = Instant::now();
    watch.check(now + RULES_AFTER * 2);
    assert!(watch.watched.is_none(), "no activation, no watch");
    watch.on_offered(activation(9, 0), now);
    watch.check(now + Duration::from_secs(30));
    assert_eq!(settled(&watch), None, "the time has not passed yet");
    watch.check(now + RULES_AFTER + Duration::from_secs(1));
    assert_eq!(settled(&watch), Some(now), "warned once for the switch");
}

#[test]
fn a_report_of_version_three_settles_the_switch_on() {
    let (mut watch, shared) = watch();
    let now = Instant::now();
    watch.on_offered(activation(9, 0), now);
    shared.set_sealer_version(3);
    watch.check(now + Duration::from_secs(1));
    assert_eq!(settled(&watch), Some(now));
}

/// An activation with a time ahead switches the rules off soon after the offer.
/// A sealer that still reports version 3 past the grace gets a warning.
#[test]
fn the_watch_warns_when_the_sealer_keeps_version_three_after_a_switch_off() {
    let (mut watch, shared) = watch();
    let now = Instant::now();
    shared.set_sealer_version(3);
    watch.on_offered(activation(9, u64::MAX), now);
    watch.check(now + Duration::from_secs(30));
    assert_eq!(settled(&watch), None);
    watch.check(now + RULES_AFTER + Duration::from_secs(1));
    assert_eq!(settled(&watch), Some(now), "warned for the switch off");
}

/// A report of version 2 settles the switch off. At its time the rules
/// switch on again, and the watch waits for version 3.
#[test]
fn a_switch_off_settles_on_version_two_and_its_time_switches_on_again() {
    let (mut watch, shared) = watch();
    let now = Instant::now();
    shared.set_sealer_version(2);
    watch.on_offered(activation(9, unix_ms() + 3_600_000), now);
    watch.check(now + Duration::from_secs(1));
    assert_eq!(
        settled(&watch),
        Some(now),
        "version 2 matches the switch off"
    );
    let on_again = watch.watched.as_ref().and_then(|w| w.due).unwrap();
    watch.check(on_again + Duration::from_secs(30));
    assert_eq!(
        settled(&watch),
        Some(now),
        "the grace of the switch on runs"
    );
    watch.check(on_again + RULES_AFTER + Duration::from_secs(1));
    assert_eq!(settled(&watch), Some(on_again), "warned for the switch on");
}

#[test]
fn an_offer_of_the_same_epoch_again_keeps_the_watch() {
    let (mut watch, _shared) = watch();
    let now = Instant::now();
    watch.on_offered(activation(9, 0), now);
    watch.on_offered(activation(9, 0), now + Duration::from_secs(50));
    watch.check(now + RULES_AFTER + Duration::from_secs(1));
    assert_eq!(settled(&watch), Some(now), "the first offer counts");
}

#[test]
fn a_newer_activation_replaces_the_watch() {
    let (mut watch, shared) = watch();
    let now = Instant::now();
    shared.set_sealer_version(3);
    watch.on_offered(activation(9, 0), now);
    watch.check(now);
    let later = now + Duration::from_secs(5);
    watch.on_offered(activation(10, u64::MAX), later);
    assert_eq!(settled(&watch), None, "the switch off waits for its report");
    shared.set_sealer_version(2);
    watch.check(later + Duration::from_secs(1));
    assert_eq!(settled(&watch), Some(later));
}
