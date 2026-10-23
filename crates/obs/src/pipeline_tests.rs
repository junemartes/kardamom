use std::time::{Duration, Instant};

use metrics_exporter_prometheus::PrometheusRecorder;

use super::{Batch, LoopMeter, LoopState, Queue, Step};
use crate::{Exporter, Readiness};

/// A recorder with the service bucket layout.
fn recorder() -> PrometheusRecorder {
    Exporter::new("x", "127.0.0.1:0".parse().unwrap(), "h", Readiness::up())
        .recorder()
        .unwrap()
}

/// The text that `record` gives on a fresh recorder.
fn render(record: impl FnOnce()) -> String {
    let recorder = recorder();
    metrics::with_local_recorder(&recorder, record);
    recorder.handle().render()
}

/// The value of the one series that starts with `series`.
fn value(text: &str, series: &str) -> f64 {
    text.lines()
        .find(|line| line.starts_with(series))
        .and_then(|line| line.rsplit(' ').next())
        .unwrap_or_else(|| panic!("no {series} in\n{text}"))
        .parse()
        .unwrap()
}

#[test]
fn a_step_of_three_microseconds_lands_in_the_five_microsecond_bucket() {
    let text = render(|| {
        Step::new("s").since(
            Instant::now()
                .checked_sub(Duration::from_micros(3))
                .unwrap(),
        );
    });
    let bucket = r#"kardamom_step_seconds_bucket{service="x",host_id="h",step="s",le="#;
    assert_eq!(value(&text, &format!("{bucket}\"0.0000025\"}}")), 0.0);
    assert_eq!(value(&text, &format!("{bucket}\"0.000005\"}}")), 1.0);
}

#[test]
fn a_batch_and_a_queue_record_their_counts() {
    let text = render(|| {
        Batch::new("b").record(3);
        Queue::new("q").set(7);
    });
    let bucket = r#"kardamom_batch_items_bucket{service="x",host_id="h",batch="b",le="#;
    assert_eq!(value(&text, &format!("{bucket}\"2\"}}")), 0.0);
    assert_eq!(value(&text, &format!("{bucket}\"4\"}}")), 1.0);
    assert_eq!(
        value(
            &text,
            r#"kardamom_queue_depth{service="x",host_id="h",queue="q"}"#
        ),
        7.0
    );
}

#[test]
fn the_bal_frame_histogram_has_byte_buckets() {
    let text = render(|| {
        metrics::histogram!("kardamom_executor_bal_frame_bytes").record(5000.0);
    });
    let bucket = r#"kardamom_executor_bal_frame_bytes_bucket{service="x",host_id="h",le="#;
    assert_eq!(value(&text, &format!("{bucket}\"4096\"}}")), 0.0);
    assert_eq!(value(&text, &format!("{bucket}\"16384\"}}")), 1.0);
}

#[test]
fn a_meter_charges_each_span_to_its_state() {
    let text = render(|| {
        let mut meter = LoopMeter::new("l");
        std::thread::sleep(Duration::from_millis(60));
        meter.mark(LoopState::Idle);
        std::thread::sleep(Duration::from_millis(60));
        meter.pass(true);
    });
    let series = |state: &str| {
        value(
            &text,
            &format!(
                r#"kardamom_loop_nanoseconds_total{{service="x",host_id="h",loop="l",state="{state}"}}"#
            ),
        )
    };
    assert!(series("idle") >= 60e6, "{text}");
    assert!(series("busy") >= 60e6, "{text}");
    assert_eq!(series("blocked"), 0.0);
}

#[test]
fn a_meter_keeps_short_spans_until_the_flush() {
    let text = render(|| {
        let mut meter = LoopMeter::new("l");
        meter.pass(true);
    });
    let busy = r#"kardamom_loop_nanoseconds_total{service="x",host_id="h",loop="l",state="busy"}"#;
    assert_eq!(
        value(&text, busy),
        0.0,
        "a span under the flush interval stays in the meter"
    );
}
