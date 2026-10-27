use std::time::Duration;

use metrics_exporter_prometheus::PrometheusBuilder;

use super::{Depths, SessionMeters};
use crate::live::endpoints::now_ms;

/// The text that `record` gives on a fresh recorder.
fn render(record: impl FnOnce()) -> String {
    let recorder = PrometheusBuilder::new().build_recorder();
    metrics::with_local_recorder(&recorder, record);
    recorder.handle().render()
}

/// The value of the one series that starts with `series`, or `None`.
fn value(text: &str, series: &str) -> Option<f64> {
    text.lines()
        .find(|line| line.starts_with(series))
        .and_then(|line| line.rsplit(' ').next())
        .map(|v| v.parse().unwrap())
}

#[test]
fn the_age_is_the_clock_difference_and_a_leader_ahead_reads_zero() {
    assert_eq!(
        SessionMeters::age(1_000_250, 1_000_000),
        Duration::from_millis(250)
    );
    assert_eq!(SessionMeters::age(1_000_000, 1_000_250), Duration::ZERO);
    assert_eq!(SessionMeters::age(5, i64::MIN), Duration::ZERO);
}

#[test]
fn a_drain_records_the_age_of_its_oldest_message_once() {
    let now = i64::try_from(now_ms()).unwrap();
    let mut observed = None;
    let text = render(|| {
        let mut meters = SessionMeters::new(false);
        meters.note_egress(now - 20);
        meters.note_egress(now - 2_000);
        meters.note_egress(now - 30);
        let before = now_ms();
        meters.drain_done();
        observed = Some((before, now_ms()));
        // A drain with no app message records nothing.
        meters.drain_done();
    });
    assert_eq!(
        value(&text, "kardamom_cluster_client_egress_age_seconds_count"),
        Some(1.0)
    );
    let sum = value(&text, "kardamom_cluster_client_egress_age_seconds_sum").unwrap();
    let (before, after) = observed.unwrap();
    let oldest = u64::try_from(now - 2_000).unwrap();
    let bounds = Duration::from_millis(before.checked_sub(oldest).unwrap()).as_secs_f64()
        ..=Duration::from_millis(after.checked_sub(oldest).unwrap()).as_secs_f64();
    assert!(bounds.contains(&sum), "sum {sum}, expected {bounds:?}");
}

#[test]
fn a_session_sets_only_the_depths_of_its_live_seams() {
    let text = render(|| {
        let mut meters = SessionMeters::new(true);
        meters.duties_done(
            true,
            &Depths {
                requests: None,
                egress_out: Some(3),
            },
        );
    });
    assert_eq!(
        value(
            &text,
            r#"kardamom_queue_depth{queue="cluster_client.stream_egress_out"}"#
        ),
        Some(3.0)
    );
    let requests = value(
        &text,
        r#"kardamom_queue_depth{queue="cluster_client.requests"}"#,
    );
    assert_eq!(requests.unwrap_or(0.0), 0.0);
}

#[test]
fn a_cycle_splits_its_time_into_busy_and_idle() {
    let text = render(|| {
        let mut meters = SessionMeters::new(false);
        let depths = Depths {
            requests: Some(0),
            egress_out: Some(0),
        };
        // Each charge crosses the 100 ms flush interval by itself. A
        // delayed first span cannot leave the second charge unpublished.
        std::thread::sleep(Duration::from_millis(120));
        meters.duties_done(true, &depths);
        std::thread::sleep(Duration::from_millis(120));
        meters.waited();
    });
    let state = |s: &str| {
        value(
            &text,
            &format!(
                r#"kardamom_loop_nanoseconds_total{{loop="cluster_client.session",state="{s}"}}"#
            ),
        )
        .unwrap()
    };
    assert!(state("busy") >= 120_000_000.0, "busy {}", state("busy"));
    assert!(state("idle") >= 120_000_000.0, "idle {}", state("idle"));
    assert_eq!(state("blocked"), 0.0);
}
