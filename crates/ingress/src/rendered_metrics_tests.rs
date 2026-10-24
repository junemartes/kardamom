//! The text of a Prometheus recorder after a test records on it.

use metrics_exporter_prometheus::PrometheusBuilder;

/// The Prometheus text of the metrics that one closure recorded.
pub(crate) struct Rendered(String);

impl Rendered {
    /// Run `record` on a fresh local recorder and render its text.
    pub(crate) fn of(record: impl FnOnce()) -> Self {
        let recorder = PrometheusBuilder::new().build_recorder();
        metrics::with_local_recorder(&recorder, record);
        Self(recorder.handle().render())
    }

    /// The value of the one series whose line starts with `series`.
    pub(crate) fn value(&self, series: &str) -> f64 {
        self.0
            .lines()
            .find(|line| line.starts_with(series))
            .and_then(|line| line.rsplit(' ').next())
            .unwrap_or_else(|| panic!("no {series} in\n{}", self.0))
            .parse()
            .unwrap()
    }
}
