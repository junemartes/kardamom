use std::time::{Duration, SystemTime};

use super::{Readiness, SERVICE_UP, unix_seconds};

const RENDERED: &str = "\
# HELP kardamom_service_up 1 while the service's exporter is live.
# TYPE kardamom_service_up gauge
kardamom_service_up{service=\"x\",host_id=\"h\"} 1
kardamom_executor_block_number{service=\"x\",host_id=\"h\"} 90
kardamom_sealer_block_number{service=\"x\",host_id=\"h\"} 100
kardamom_sequencer_resync_mode{service=\"x\",host_id=\"h\",partition=\"0\"} 0
kardamom_sequencer_resync_mode{service=\"x\",host_id=\"h\",partition=\"1\"} 1
";

fn now() -> SystemTime {
    SystemTime::now()
}

#[test]
fn the_default_rule_needs_the_liveness_gauge() {
    assert!(Readiness::up().check(RENDERED, now()).is_ok());
    assert!(Readiness::up().check("", now()).is_err());
    assert!(Readiness::up().reads(SERVICE_UP));
}

#[test]
fn equals_applies_to_every_series() {
    let rule = Readiness::default().equals("kardamom_sequencer_resync_mode", 0.0);
    let failed = rule.check(RENDERED, now()).unwrap_err();
    assert_eq!(failed.len(), 1, "{failed:?}");
    assert!(failed[0].contains("resync_mode [1.0]"), "{failed:?}");
}

#[test]
fn within_compares_against_the_head() {
    let gauge = "kardamom_executor_block_number";
    let head = "kardamom_sealer_block_number";
    assert!(
        Readiness::default()
            .within(gauge, head, 10.0)
            .check(RENDERED, now())
            .is_ok()
    );
    assert!(
        Readiness::default()
            .within(gauge, head, 9.0)
            .check(RENDERED, now())
            .is_err()
    );
    let missing_head = Readiness::default().within(gauge, "kardamom_absent", 9.0);
    let failed = missing_head.check(RENDERED, now()).unwrap_err();
    assert!(failed[0].contains("not exported yet"), "{failed:?}");
}

#[test]
fn fresh_reads_a_unix_time() {
    let at = now();
    let rendered = format!("kardamom_last_tick {}\n", unix_seconds(at));
    let rule = Readiness::default().fresh("kardamom_last_tick", Duration::from_secs(5));
    assert!(rule.check(&rendered, at).is_ok());
    assert!(rule.check(&rendered, at + Duration::from_secs(6)).is_err());
    assert!(rule.check("", at).is_err(), "an unset gauge is not fresh");
}

#[test]
fn a_missing_gauge_fails_and_comments_are_skipped() {
    let rule = Readiness::default().equals("kardamom_absent", 1.0);
    assert!(rule.check(RENDERED, now()).is_err());
    assert!(
        Readiness::default()
            .present("kardamom_absent")
            .check(RENDERED, now())
            .is_err()
    );
    assert!(
        Readiness::default()
            .present("kardamom_sealer_block_number")
            .check(RENDERED, now())
            .is_ok()
    );
    let only_comments = "# TYPE kardamom_absent gauge\n";
    assert!(rule.check(only_comments, now()).is_err());
}
