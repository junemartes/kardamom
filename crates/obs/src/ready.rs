//! The readiness rule of a service: conditions over the gauges the
//! service exports. The exporter's `/ready` route answers 200 when every
//! condition holds and 503 with the failed conditions when one does not.
//! A rule reads the rendered `/metrics` text, so a condition names a
//! gauge the dashboards show, and nothing else.

use std::time::{Duration, SystemTime};

/// One condition over the exported gauges. A condition names a gauge by
/// its metric name and applies to every series of that name. At least
/// one series must exist: a gauge the service has not set yet fails.
#[derive(Debug, Clone)]
pub enum Condition {
    /// At least one series of `gauge` exists.
    Present { gauge: &'static str },
    /// Every series of `gauge` equals `value`.
    Equals { gauge: &'static str, value: f64 },
    /// The highest series of `head` minus every series of `gauge` is at
    /// most `max_gap`.
    Within {
        gauge: &'static str,
        head: &'static str,
        max_gap: f64,
    },
    /// Every series of `gauge` holds a unix time in seconds that is at
    /// most `max_age` before `now`.
    Fresh {
        gauge: &'static str,
        max_age: Duration,
    },
}

/// The readiness rule: the conjunction of its conditions.
#[derive(Debug, Clone, Default)]
pub struct Readiness {
    conditions: Vec<Condition>,
}

/// The exporter's liveness gauge. The default rule requires it, so a
/// service with no rule of its own is ready once its exporter is live.
pub const SERVICE_UP: &str = "kardamom_service_up";

/// One rendered sample: the metric name without its labels, and the
/// value.
#[derive(Debug, Clone, Copy, PartialEq)]
struct Sample<'a> {
    name: &'a str,
    value: f64,
}

impl<'a> Sample<'a> {
    /// Parse one line of the Prometheus text format. A comment, a blank
    /// line, or a line without a numeric value is `None`.
    fn parse(line: &'a str) -> Option<Self> {
        if line.starts_with('#') {
            return None;
        }
        let (series, value) = line.rsplit_once(' ')?;
        let name = series.split('{').next()?;
        Some(Self {
            name,
            value: value.parse().ok()?,
        })
    }
}

/// The values of the samples that carry the metric name `name`.
fn values<'a>(samples: &'a [Sample<'a>], name: &'a str) -> impl Iterator<Item = f64> + 'a {
    samples
        .iter()
        .filter(move |s| s.name == name)
        .map(|s| s.value)
}

impl Condition {
    /// The gauge this condition reads.
    fn gauge(&self) -> &'static str {
        match self {
            Self::Present { gauge }
            | Self::Equals { gauge, .. }
            | Self::Within { gauge, .. }
            | Self::Fresh { gauge, .. } => gauge,
        }
    }

    /// The reason this condition fails for `samples` at `now`, or `None`
    /// when it holds.
    fn failure(&self, samples: &[Sample<'_>], now: f64) -> Option<String> {
        let gauge = self.gauge();
        let mut series = values(samples, gauge).peekable();
        if series.peek().is_none() {
            return Some(format!("{gauge} is not exported yet"));
        }
        let bad: Vec<f64> = match self {
            Self::Present { .. } => Vec::new(),
            #[allow(
                clippy::float_cmp,
                reason = "the gauges a rule compares hold small integers; exact equality is the meaning"
            )]
            Self::Equals { value, .. } => series.filter(|v| v != value).collect(),
            Self::Within {
                head: head_gauge,
                max_gap,
                ..
            } => {
                let head = values(samples, head_gauge).reduce(f64::max);
                let Some(head) = head else {
                    return Some(format!("{head_gauge} is not exported yet"));
                };
                series.filter(|v| head - v > *max_gap).collect()
            }
            Self::Fresh { max_age, .. } => {
                series.filter(|v| now - v > max_age.as_secs_f64()).collect()
            }
        };
        (!bad.is_empty()).then(|| format!("{gauge} {bad:?} fails {self:?}"))
    }
}

impl Readiness {
    /// The default rule: the exporter is live.
    #[must_use]
    pub fn up() -> Self {
        Self::default().equals(SERVICE_UP, 1.0)
    }

    /// Require `gauge` to be exported, with any value.
    #[must_use]
    pub fn present(mut self, gauge: &'static str) -> Self {
        self.conditions.push(Condition::Present { gauge });
        self
    }

    /// Require every series of `gauge` to equal `value`.
    #[must_use]
    pub fn equals(mut self, gauge: &'static str, value: f64) -> Self {
        self.conditions.push(Condition::Equals { gauge, value });
        self
    }

    /// Require every series of `gauge` to be within `max_gap` of `head`.
    #[must_use]
    pub fn within(mut self, gauge: &'static str, head: &'static str, max_gap: f64) -> Self {
        self.conditions.push(Condition::Within {
            gauge,
            head,
            max_gap,
        });
        self
    }

    /// Require every series of `gauge`, a unix time in seconds, to be at
    /// most `max_age` old.
    #[must_use]
    pub fn fresh(mut self, gauge: &'static str, max_age: Duration) -> Self {
        self.conditions.push(Condition::Fresh { gauge, max_age });
        self
    }

    /// Check the rule against the rendered `/metrics` text at `now`.
    ///
    /// # Errors
    ///
    /// Returns the reason of every failed condition.
    pub fn check(&self, rendered: &str, now: SystemTime) -> Result<(), Vec<String>> {
        let samples: Vec<Sample<'_>> = rendered.lines().filter_map(Sample::parse).collect();
        let now = unix_seconds(now);
        let failed: Vec<String> = self
            .conditions
            .iter()
            .filter_map(|c| c.failure(&samples, now))
            .collect();
        if failed.is_empty() {
            Ok(())
        } else {
            Err(failed)
        }
    }

    /// Does the rule name `gauge`?
    #[must_use]
    pub fn reads(&self, gauge: &str) -> bool {
        self.conditions.iter().any(|c| c.gauge() == gauge)
    }
}

/// `at` as seconds since the unix epoch. A clock before the epoch reads
/// as 0, which fails every freshness condition.
#[must_use]
pub fn unix_seconds(at: SystemTime) -> f64 {
    at.duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0.0, |d| d.as_secs_f64())
}

/// Set `gauge` to the current unix time in seconds. A follower calls
/// this when it completes a tick, so its readiness rule can require a
/// recent one.
pub fn mark_now(gauge: &'static str) {
    metrics::gauge!(gauge).set(unix_seconds(SystemTime::now()));
}

#[cfg(test)]
#[path = "ready_tests.rs"]
mod tests;
