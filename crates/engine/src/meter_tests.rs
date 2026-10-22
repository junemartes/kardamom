use std::time::Duration;

use kardamom_obs::pipeline::LoopMeter;
use metrics_exporter_prometheus::PrometheusBuilder;

use super::OutputWait;

/// The text that `record` gives on a fresh local recorder.
pub(crate) fn render(record: impl FnOnce()) -> String {
    let recorder = PrometheusBuilder::new().build_recorder();
    metrics::with_local_recorder(&recorder, record);
    recorder.handle().render()
}

/// The value of the one series that starts with `series`, or 0 when the
/// text has no such series.
pub(crate) fn value(text: &str, series: &str) -> f64 {
    text.lines()
        .find(|line| line.starts_with(series))
        .and_then(|line| line.rsplit(' ').next())
        .map_or(0.0, |v| v.parse().unwrap())
}

/// The nanoseconds of the loop `name` in `state`.
fn loop_nanos(text: &str, name: &str, state: &str) -> f64 {
    value(
        text,
        &format!(r#"kardamom_loop_nanoseconds_total{{loop="{name}",state="{state}"}}"#),
    )
}

#[test]
fn a_send_to_a_full_output_is_blocked_and_a_free_one_is_not() {
    let text = render(|| {
        let mut meter = LoopMeter::new("l");
        let (tx, rx) = crossbeam_channel::bounded::<u8>(1);
        meter.output_wait(tx.is_full(), || tx.send(1)).unwrap();
        let drain = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(120));
            rx.recv().unwrap();
            rx.recv().unwrap();
        });
        meter.output_wait(tx.is_full(), || tx.send(2)).unwrap();
        drain.join().unwrap();
    });
    assert!(loop_nanos(&text, "l", "blocked") >= 100_000_000.0, "{text}");
    assert!(loop_nanos(&text, "l", "busy") < 100_000_000.0, "{text}");
}
