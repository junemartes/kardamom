//! Prometheus scrape and parse. A scrape tries the bridge address first
//! and falls back to `docker exec` on the node, since a hard kill of a
//! privileged sibling can stall the host's dockerd and take every exec
//! probe with it. A failed scrape is `None`, never zero: a metric that
//! is absent, or a node that does not answer, must not read as a value.

use std::net::Ipv4Addr;
use std::time::Duration;

use crate::nodes::Nodes;

/// Where one exporter is reached.
#[derive(Debug, Clone)]
pub struct Target {
    /// The bridge address, when the exporter binds beyond loopback.
    pub ip: Option<Ipv4Addr>,
    /// The node container, for the `docker exec` fallback.
    pub node: String,
    pub port: u16,
}

impl Target {
    /// An exporter reached over the bridge, with the exec fallback.
    #[must_use]
    pub fn bridged(ip: Ipv4Addr, node: &str, port: u16) -> Self {
        Self {
            ip: Some(ip),
            node: node.to_string(),
            port,
        }
    }

    /// A loopback-only exporter, reached through the node.
    #[must_use]
    pub fn loopback(node: &str, port: u16) -> Self {
        Self {
            ip: None,
            node: node.to_string(),
            port,
        }
    }
}

/// The scraper.
#[derive(Debug, Clone)]
pub struct Scrape {
    http: reqwest::Client,
    nodes: Nodes,
}

impl Default for Scrape {
    fn default() -> Self {
        Self::new()
    }
}

impl Scrape {
    /// A scraper with a five-second HTTP budget per probe.
    ///
    /// # Panics
    ///
    /// Panics if the HTTP client cannot be built, which only a broken
    /// TLS backend causes.
    #[must_use]
    pub fn new() -> Self {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .expect("build the scrape HTTP client");
        Self { http, nodes: Nodes }
    }

    /// One `/metrics` body, or `None` when no probe answers.
    pub async fn fetch(&self, target: &Target) -> Option<String> {
        if let Some(body) = self.fetch_direct(target).await {
            return Some(body);
        }
        self.fetch_via_node(target).await
    }

    async fn fetch_direct(&self, target: &Target) -> Option<String> {
        let ip = target.ip?;
        let url = format!("http://{ip}:{}/metrics", target.port);
        self.http
            .get(&url)
            .send()
            .await
            .ok()?
            .error_for_status()
            .ok()?
            .text()
            .await
            .ok()
    }

    async fn fetch_via_node(&self, target: &Target) -> Option<String> {
        let script = format!(
            "curl -fsS --max-time 5 http://127.0.0.1:{}/metrics",
            target.port
        );
        self.nodes.exec(&target.node, &script).await.ok()
    }

    /// Whether the exporter answers at all.
    pub async fn answers(&self, target: &Target) -> bool {
        self.fetch(target).await.is_some()
    }
}

/// Whether a line is a sample of `metric`: the name, then a label
/// brace, a space, or the end of the name.
fn is_sample_of(line: &str, metric: &str) -> bool {
    let Some(rest) = line.strip_prefix(metric) else {
        return false;
    };
    !line.starts_with('#') && (rest.is_empty() || rest.starts_with('{') || rest.starts_with(' '))
}

/// The value of a sample line, truncated to an integer as `awk %d` does.
/// Gauges may render as floats or in scientific notation.
fn sample_value(line: &str) -> Option<i64> {
    let raw = line.rsplit(' ').next()?;
    let float: f64 = raw.parse().ok()?;
    // `%d` truncation: the fractional part is dropped.
    #[allow(
        clippy::cast_possible_truncation,
        reason = "awk %d semantics: truncate a finite exposition value toward zero"
    )]
    Some(float.trunc() as i64)
}

/// The integer value of the first sample of `metric`, for gauges. `None`
/// when no sample matches.
#[must_use]
pub fn first(body: &str, metric: &str) -> Option<i64> {
    body.lines()
        .find(|l| is_sample_of(l, metric))
        .and_then(sample_value)
}

/// The integer sum of every sample of `metric`, for per-label counters.
/// `None` when no sample matches: a scrape failure, or a metric not yet
/// registered, is not zero.
#[must_use]
pub fn sum(body: &str, metric: &str) -> Option<i64> {
    let values: Vec<i64> = body
        .lines()
        .filter(|l| is_sample_of(l, metric))
        .filter_map(sample_value)
        .collect();
    (!values.is_empty()).then(|| values.iter().sum())
}

/// The integer sum of the samples of `metric` whose label set contains
/// `label_fragment`, such as `outcome="ok"`. Zero when the body has no
/// such sample; `None` only when the body is empty.
#[must_use]
pub fn sum_where(body: &str, metric: &str, label_fragment: &str) -> Option<i64> {
    if body.is_empty() {
        return None;
    }
    Some(
        body.lines()
            .filter(|l| is_sample_of(l, metric) && l.contains(label_fragment))
            .filter_map(sample_value)
            .sum(),
    )
}

/// The number of sample lines whose name starts with `prefix`.
#[must_use]
pub fn count_with_prefix(body: &str, prefix: &str) -> usize {
    body.lines()
        .filter(|l| l.starts_with(prefix) && !l.starts_with('#'))
        .count()
}

#[cfg(test)]
mod tests {
    use super::*;

    const BODY: &str = "# HELP x\nkardamom_executor_block_number 41.0\n\
        kardamom_sequencer_nonce_lookups_total{outcome=\"ok\"} 3\n\
        kardamom_sequencer_nonce_lookups_total{outcome=\"error\"} 2\n\
        kardamom_sequencer_nonce_lookups_total_created 1.7e9\n\
        validator_committed_block 1.2e2\n";

    #[test]
    fn first_reads_a_gauge_and_ignores_prefix_collisions() {
        assert_eq!(first(BODY, "kardamom_executor_block_number"), Some(41));
        assert_eq!(first(BODY, "validator_committed_block"), Some(120));
        assert_eq!(first(BODY, "kardamom_sequencer_nonce_lookups"), None);
        assert_eq!(first(BODY, "validator_divergence_total"), None);
    }

    #[test]
    fn sum_adds_labeled_samples_only() {
        assert_eq!(sum(BODY, "kardamom_sequencer_nonce_lookups_total"), Some(5));
        assert_eq!(
            sum_where(
                BODY,
                "kardamom_sequencer_nonce_lookups_total",
                "outcome=\"error\""
            ),
            Some(2)
        );
        assert_eq!(
            sum_where(
                BODY,
                "kardamom_sequencer_nonce_lookups_total",
                "outcome=\"timeout\""
            ),
            Some(0)
        );
        assert_eq!(sum_where("", "any", "x"), None);
        assert_eq!(sum("", "any"), None);
        assert_eq!(count_with_prefix(BODY, "kardamom_sequencer_"), 3);
    }
}
