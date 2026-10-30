//! The read path of the local stack: one Redis container and one state
//! mirror next to the executor. Redis keeps nothing on disk, as in the
//! deployment, so a lost Redis returns empty, and the mirror rebuilds it
//! from the executor's newest checkpoint.

use std::net::SocketAddr;
use std::process::Command;
use std::time::Duration;

use alloy_primitives::Address;
use anyhow::{Context, Result};
use kardamom_obs::testkit::free_port;

use super::LocalStack;
use super::proc::Proc;
use super::services::{self, ServiceSpec, SpawnPlan, Spawned};

/// The readiness gauge of the mirror: 1 while it serves.
const SERVING: &str = "kardamom_state_mirror_serving";

/// One Redis container, removed on drop.
pub struct Redis {
    name: String,
    pub port: u16,
}

impl Redis {
    /// Start a Redis 7 container with no persistence on a free port.
    ///
    /// # Errors
    /// Returns an error when Docker fails or Redis does not answer.
    pub fn start(name: &str) -> Result<Self> {
        let port = free_port().port();
        let _ = Command::new("docker").args(["rm", "-f", name]).output();
        Self::docker(&[
            "run",
            "-d",
            "--name",
            name,
            "-p",
            &format!("127.0.0.1:{port}:6379"),
            "redis:7-alpine",
            "redis-server",
            "--save",
            "",
            "--appendonly",
            "no",
        ])?;
        let redis = Self {
            name: name.to_string(),
            port,
        };
        redis.await_ping()?;
        Ok(redis)
    }

    fn docker(args: &[&str]) -> Result<String> {
        let out = Command::new("docker")
            .args(args)
            .output()
            .context("run docker")?;
        anyhow::ensure!(
            out.status.success(),
            "docker {args:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
    }

    /// Wait until Redis answers PING.
    fn await_ping(&self) -> Result<()> {
        super::metrics::poll_sync(
            "redis answers PING",
            Duration::from_secs(30),
            Duration::from_millis(200),
            || Ok(self.ping().then_some(())),
        )
    }

    /// Whether Redis answers PING.
    #[must_use]
    pub fn ping(&self) -> bool {
        Self::docker(&["exec", &self.name, "redis-cli", "ping"]).is_ok_and(|out| out == "PONG")
    }

    /// The number of keys in Redis.
    ///
    /// # Errors
    /// Returns an error when Redis does not answer.
    pub fn keys(&self) -> Result<u64> {
        Ok(Self::docker(&["exec", &self.name, "redis-cli", "dbsize"])?.parse()?)
    }

    /// Whether Redis holds `key`.
    #[must_use]
    pub fn has(&self, key: &str) -> bool {
        Self::docker(&["exec", &self.name, "redis-cli", "exists", key]).is_ok_and(|out| out == "1")
    }

    /// SIGKILL the container.
    ///
    /// # Errors
    /// Returns an error when Docker fails.
    pub fn kill(&self) -> Result<()> {
        Self::docker(&["kill", &self.name]).map(|_| ())
    }

    /// Start the killed container again. It returns empty.
    ///
    /// # Errors
    /// Returns an error when Docker fails or Redis does not answer.
    pub fn restart(&self) -> Result<()> {
        Self::docker(&["start", &self.name])?;
        self.await_ping()
    }

    fn url(&self) -> String {
        format!("redis://127.0.0.1:{}", self.port)
    }
}

impl Drop for Redis {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["rm", "-f", &self.name])
            .output();
    }
}

/// The running read path.
pub struct ReadPath {
    pub(crate) mirror: Spawned,
    pub redis: Redis,
}

impl ReadPath {
    /// [`Self::start`] when `wanted`.
    ///
    /// # Errors
    /// Returns an error when Redis or the mirror fails to start.
    pub(crate) fn start_if(wanted: bool, spec: &ServiceSpec<'_>) -> Result<Option<Self>> {
        wanted.then(|| Self::start(spec)).transpose()
    }

    /// Start Redis and the mirror of executor 0.
    ///
    /// # Errors
    /// Returns an error when Redis or the mirror fails to start.
    pub(crate) fn start(spec: &ServiceSpec<'_>) -> Result<Self> {
        let suffix = spec
            .root
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("stack")
            .to_ascii_lowercase();
        let redis = Redis::start(&format!("kardamom-e2e-redis-{suffix}"))?;
        let mirror = Self::spawn_mirror(spec, &redis, None)?;
        Ok(Self { mirror, redis })
    }

    /// Spawn the mirror, on `metrics_port` when it returns.
    fn spawn_mirror(
        spec: &ServiceSpec<'_>,
        redis: &Redis,
        metrics_port: Option<u16>,
    ) -> Result<Spawned> {
        let config = spec.root.join("mirror.toml");
        std::fs::write(&config, format!("[cache]\nurl = \"{}\"\n", redis.url()))?;
        let metrics_port = metrics_port.unwrap_or_else(|| free_port().port());
        let mut cmd = Command::new(services::bin("kardamom-state-mirror")?);
        cmd.arg("--config")
            .arg(&config)
            .arg("--aeron-dir")
            .arg(spec.aeron_dir)
            .args(["--executor-count", "1", "--mirror-id", "0"])
            .arg("--checkpoints-dir")
            .arg(spec.root.join("checkpoints"))
            .arg("--mirror-dir")
            .arg(spec.root.join("mirror"))
            .args(["--metrics-addr", &format!("127.0.0.1:{metrics_port}")])
            .args(["--host-id", "e2e-mirror"]);
        services::with_log_config(&mut cmd, spec);
        services::common_service_env(&mut cmd);
        SpawnPlan {
            name: "state-mirror".to_string(),
            cmd,
            log: spec.root.join(format!("state-mirror-{metrics_port}.log")),
            metrics_port,
            state_dir: None,
        }
        .spawn()
    }

    fn metrics(&self) -> SocketAddr {
        self.mirror.metrics_addr
    }

    /// The mirror's process, for the failure dump.
    pub(crate) fn proc(&self) -> &Proc {
        &self.mirror.proc
    }
}

impl LocalStack {
    /// The read path, when the stack runs one.
    #[must_use]
    pub fn read_path(&self) -> Option<&ReadPath> {
        self.read_path.as_ref()
    }

    /// Kill Redis and the mirror, as a loss of their nodes does.
    ///
    /// # Errors
    /// Returns an error when Docker fails.
    pub fn kill_read_path(&mut self) -> Result<()> {
        let Some(path) = self.read_path.as_mut() else {
            return Ok(());
        };
        path.mirror.proc.kill();
        path.redis.kill()
    }

    /// Start Redis empty and the mirror again. The mirror rebuilds Redis
    /// from the executor's newest checkpoint.
    ///
    /// # Errors
    /// Returns an error when Redis or the mirror does not start.
    pub fn restart_read_path(&mut self) -> Result<()> {
        let Some(port) = self.read_path.as_ref().map(|p| p.metrics().port()) else {
            return Ok(());
        };
        let spec = self.service_spec();
        let path = self.read_path.as_ref().context("read path")?;
        path.redis.restart()?;
        let mirror = ReadPath::spawn_mirror(&spec, &path.redis, Some(port))?;
        self.read_path.as_mut().context("read path")?.mirror = mirror;
        Ok(())
    }

    /// Wait until the mirror serves and Redis holds the row of `account`.
    ///
    /// # Errors
    /// Returns an error when the stack runs no read path, or when the
    /// mirror does not serve within `bound`.
    pub async fn await_read_path(&self, bound: Duration, account: Address) -> Result<u64> {
        let path = self
            .read_path
            .as_ref()
            .context("the stack runs no read path")?;
        let metrics = path.metrics();
        super::metrics::poll_until(
            &format!("the state mirror serves and Redis holds the row of {account}"),
            bound,
            Duration::from_millis(250),
            async || {
                let serving = super::metrics::scrape(metrics)
                    .await
                    .ok()
                    .and_then(|s| s.value(SERVING))
                    .is_some_and(|v| v > 0.0);
                let keys = path.redis.keys().unwrap_or(0);
                let row = path.redis.has(&format!("acct:{account}"));
                Ok((serving && row).then_some(keys))
            },
        )
        .await
    }
}
