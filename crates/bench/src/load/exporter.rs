//! One Prometheus exporter and the read of its `/metrics`.
//!
//! A target that has a URL is read over that URL first. When that read
//! fails, `docker exec <node> curl 127.0.0.1:<port>/metrics` reads the
//! same exporter from inside its node, and the read counts as a
//! fallback. A target with no URL is read through its node only, and
//! that read is not a fallback. Every read has a time bound: a killed
//! `DinD` node can stall `docker exec` on the host for minutes.
//!
//! The load harness and the chaos probes share this read.

use std::net::Ipv4Addr;
use std::time::Duration;

use tokio::process::Command;

/// The default metrics port of the executor (`--metrics-addr`).
const EXECUTOR_METRICS_PORT: u16 = 9004;
/// The default metrics port of the ingress.
const INGRESS_METRICS_PORT: u16 = 9006;
/// The default metrics port of the lane-0 sequencer replica.
const SEQUENCER_METRICS_PORT: u16 = 9001;

/// The time bound of one direct read.
const DIRECT_TIMEOUT: Duration = Duration::from_secs(5);
/// The time bound of one read through the node: the 5 s `curl` inside
/// the node, plus the start of `docker exec`.
const EXEC_TIMEOUT: Duration = Duration::from_secs(10);

/// One exporter: the node container, for the read through the node, the
/// URL of a direct read, if the exporter has one, and its port.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricsTarget {
    /// The node container name.
    pub name: String,
    /// The URL of the direct read. `None` for an exporter that only the
    /// node reaches.
    url: Option<reqwest::Url>,
    port: u16,
}

impl MetricsTarget {
    /// The exporter on `port` of the bridge address `ip`, in the node
    /// container `name`.
    ///
    /// # Panics
    ///
    /// Never in practice: an IPv4 address and a port always form a valid
    /// URL.
    #[must_use]
    pub fn bridged(ip: Ipv4Addr, name: &str, port: u16) -> Self {
        Self::direct(name, &ip.to_string(), port).expect("an IPv4 address and a port form a URL")
    }

    /// The exporter on `port` of the node container `name`, read only
    /// through the node.
    #[must_use]
    pub fn loopback(name: &str, port: u16) -> Self {
        Self {
            name: name.to_string(),
            url: None,
            port,
        }
    }

    /// The exporter on `port` of the node container `name`, with the
    /// container name as the host of the direct read.
    ///
    /// # Errors
    ///
    /// Returns an error if `name` is not a valid host name.
    fn named(name: &str, port: u16) -> anyhow::Result<Self> {
        Self::direct(name, name, port)
    }

    fn direct(name: &str, host: &str, port: u16) -> anyhow::Result<Self> {
        let url = reqwest::Url::parse(&format!("http://{host}:{port}/metrics"))?;
        Ok(Self {
            name: name.to_string(),
            url: Some(url),
            port,
        })
    }

    /// The port of the exporter.
    #[must_use]
    pub fn port(&self) -> u16 {
        self.port
    }

    /// The URL that the node reads inside its own network: the same
    /// port on loopback.
    fn loopback_url(&self) -> String {
        format!("http://127.0.0.1:{}/metrics", self.port)
    }
}

/// The exporters one load reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricsTargets {
    /// The executor exporters.
    pub executors: Vec<MetricsTarget>,
    /// The exporter of the ingress the load submits through.
    pub ingress: MetricsTarget,
    /// The lane-0 sequencer exporters.
    pub sequencers: Vec<MetricsTarget>,
}

impl MetricsTargets {
    /// The exporters on the default ports of the named node containers.
    /// With `via_docker`, every exporter is read through its node only.
    /// Otherwise the container name is the host of the direct read.
    ///
    /// # Errors
    ///
    /// Returns an error if a name is not a valid host name.
    pub fn named(
        executors: &[String],
        ingress: &str,
        sequencers: &[String],
        via_docker: bool,
    ) -> anyhow::Result<Self> {
        let target = |name: &str, port| {
            if via_docker {
                Ok(MetricsTarget::loopback(name, port))
            } else {
                MetricsTarget::named(name, port)
            }
        };
        let targets = |names: &[String], port| {
            names
                .iter()
                .map(|n| target(n, port))
                .collect::<anyhow::Result<Vec<_>>>()
        };
        Ok(Self {
            executors: targets(executors, EXECUTOR_METRICS_PORT)?,
            ingress: target(ingress, INGRESS_METRICS_PORT)?,
            sequencers: targets(sequencers, SEQUENCER_METRICS_PORT)?,
        })
    }
}

/// The result of one read: the body, and whether the read fell back to
/// `docker exec`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExporterRead {
    /// The `/metrics` body. `None` when no read answers.
    pub body: Option<String>,
    /// The direct read failed, and the read went through the node.
    pub fell_back: bool,
}

/// The reader of exporters.
#[derive(Debug, Clone)]
pub struct ExporterReader {
    http: reqwest::Client,
    /// The program of the read through the node: `docker`. It runs as
    /// `<program> exec <node> curl ... <loopback url>`.
    exec_program: &'static str,
    /// The time bound of one read through the node.
    exec_timeout: Duration,
}

impl ExporterReader {
    /// A reader with the `docker exec` path through the node.
    ///
    /// # Errors
    ///
    /// Returns an error if the HTTP client cannot be built.
    pub fn new() -> anyhow::Result<Self> {
        Self::with_exec("docker", EXEC_TIMEOUT)
    }

    pub(crate) fn with_exec(
        exec_program: &'static str,
        exec_timeout: Duration,
    ) -> anyhow::Result<Self> {
        let http = reqwest::Client::builder().timeout(DIRECT_TIMEOUT).build()?;
        Ok(Self {
            http,
            exec_program,
            exec_timeout,
        })
    }

    /// Read `target`: directly when it has a URL, then through the node
    /// when the direct read fails.
    pub async fn read(&self, target: &MetricsTarget) -> ExporterRead {
        let Some(url) = &target.url else {
            return ExporterRead {
                body: self.read_via_node(target).await,
                fell_back: false,
            };
        };
        match self.read_direct(url).await {
            Some(body) => ExporterRead {
                body: Some(body),
                fell_back: false,
            },
            None => ExporterRead {
                body: self.read_via_node(target).await,
                fell_back: true,
            },
        }
    }

    async fn read_direct(&self, url: &reqwest::Url) -> Option<String> {
        self.http
            .get(url.clone())
            .send()
            .await
            .ok()?
            .error_for_status()
            .ok()?
            .text()
            .await
            .ok()
    }

    /// Read `target` through its node within the exec time bound. A
    /// command that runs past the bound is killed when its future drops.
    async fn read_via_node(&self, target: &MetricsTarget) -> Option<String> {
        let url = target.loopback_url();
        let run = Command::new(self.exec_program)
            .args([
                "exec",
                &target.name,
                "curl",
                "-fsS",
                "--max-time",
                "5",
                &url,
            ])
            .kill_on_drop(true)
            .output();
        let out = tokio::time::timeout(self.exec_timeout, run)
            .await
            .ok()?
            .ok()?;
        out.status
            .success()
            .then(|| String::from_utf8_lossy(&out.stdout).into_owned())
    }
}

#[cfg(test)]
mod tests;
