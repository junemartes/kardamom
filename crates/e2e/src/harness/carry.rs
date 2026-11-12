//! The carry window parts of the local stack: the egress, the live
//! batcher, and the restarts that the rollout does one service at a time.
//! [`super::CarryOptions`] turns them on.

use std::net::SocketAddr;
use std::process::Command;
use std::time::Duration;

use anyhow::{Context, Result};
use kardamom_batcher::testkit_da::FakeDaProxy;
use kardamom_obs::testkit::{free_port, free_udp_port};

use super::LocalStack;
use super::l1::{BATCHER_KEY, L1};
use super::services::{
    self, ExecutorRun, ServiceSpec, SpawnPlan, Spawned, ValidatorOptions, common_service_env,
    with_log_config,
};

/// A running egress: the process, and the ports that a restart keeps.
pub(super) struct SpawnedEgress {
    pub(super) service: Spawned,
    http_port: u16,
}

impl SpawnedEgress {
    /// The base URL of the range read and the JSON-RPC of the egress.
    #[must_use]
    pub(super) fn url(&self) -> String {
        format!("http://127.0.0.1:{}", self.http_port)
    }
}

/// The live batcher, and the fake DA proxy that keeps its payloads. The
/// proxy outlives every batcher process of the stack.
pub(super) struct BatcherSide {
    process: Option<Spawned>,
    metrics_port: u16,
    l1_rpc: String,
    settlement: String,
    da: FakeDaProxy,
}

/// The seams of the carry window that a scenario reads: the egress, the
/// batcher, and the admin endpoint of each sealer member.
pub struct CarrySeams {
    /// The base URL of the egress.
    pub egress_url: String,
    pub egress_metrics: SocketAddr,
    /// Present when the stack runs a batcher (`StackConfig::l1`).
    pub batcher_metrics: Option<SocketAddr>,
    /// The admin endpoint of each member, in member order.
    pub sealer_admin: Vec<SocketAddr>,
    /// The base URL of the fake DA proxy that keeps the payloads of the
    /// batcher. Present when the stack runs a batcher.
    pub da_proxy: Option<String>,
}

impl ServiceSpec<'_> {
    /// Spawn `kardamom-egress` on the shared media driver. Its store is
    /// `<root>/egress`, so a respawn on the same ports keeps its frames and
    /// its reference mark.
    ///
    /// # Errors
    /// Returns an error when the binary is not built or the process fails
    /// to spawn.
    pub(super) fn spawn_egress(&self, ports: Option<(u16, u16)>) -> Result<SpawnedEgress> {
        let (metrics_port, http_port) =
            ports.unwrap_or_else(|| (free_port().port(), free_port().port()));
        let mut cmd = Command::new(services::bin("kardamom-egress")?);
        cmd.arg("--aeron-dir")
            .arg(self.aeron_dir)
            .arg("--dir")
            .arg(self.root.join("egress"))
            .args(["--bind", &format!("127.0.0.1:{http_port}")])
            .args(["--metrics-addr", &format!("127.0.0.1:{metrics_port}")])
            .args(["--host-id", "e2e-egress"]);
        with_log_config(&mut cmd, self);
        common_service_env(&mut cmd);
        let log = if ports.is_some() {
            "egress-restarted.log"
        } else {
            "egress.log"
        };
        let service = SpawnPlan {
            name: "egress".to_string(),
            cmd,
            log: self.root.join(log),
            metrics_port,
            state_dir: None,
        }
        .spawn()?;
        Ok(SpawnedEgress { service, http_port })
    }

    /// Spawn the live batcher. With `egress`, it reads the canonical
    /// stream from the egress at that base URL. Its cursor file and spool
    /// are under `<root>/batcher`, so a respawn resumes where the last
    /// process stopped.
    ///
    /// # Errors
    /// Returns an error when the binary is not built, when the config file
    /// or the directory cannot be written, or when the process fails to
    /// spawn.
    fn spawn_batcher(&self, side: &BatcherSide, egress: Option<&str>) -> Result<Spawned> {
        let dir = self.root.join("batcher");
        std::fs::create_dir_all(&dir)?;
        let cfg_path = self.write_cluster_config("batcher", "")?;
        let mut cmd = Command::new(services::bin("kardamom-batcher")?);
        cmd.args(["--live", "--dry-run=false"])
            .args(["--l1-rpc", &side.l1_rpc])
            .args(["--l1-key", BATCHER_KEY])
            .args(["--settlement", &side.settlement])
            .args(["--da-proxy", &side.da.url()])
            .arg("--config")
            .arg(&cfg_path)
            .arg("--aeron-dir")
            .arg(self.aeron_dir)
            .args([
                "--cluster-egress-endpoint",
                &format!("127.0.0.1:{}", free_udp_port().port()),
            ])
            .arg("--cursor-file")
            .arg(dir.join("cursor"))
            .args(["--chain-id", &self.chain_id.to_string()])
            // A test chain seals four blocks a second, and one post takes
            // a few L1 blocks. So a group holds every block that waits, and
            // posts within a second or two.
            .args(["--blocks-per-batch", "100000"])
            .args(["--flush-ms", "1000", "--idle-flush-ms", "2000"])
            .args([
                "--metrics-addr",
                &format!("127.0.0.1:{}", side.metrics_port),
            ])
            .args(["--host-id", "e2e-batcher"]);
        if let Some(url) = egress {
            cmd.args(["--egress", url]);
        }
        with_log_config(&mut cmd, self);
        common_service_env(&mut cmd);
        let log = if egress.is_some() {
            "batcher-egress.log"
        } else {
            "batcher.log"
        };
        SpawnPlan {
            name: "batcher".to_string(),
            cmd,
            log: self.root.join(log),
            metrics_port: side.metrics_port,
            state_dir: None,
        }
        .spawn()
    }
}

impl BatcherSide {
    /// Start the fake DA proxy and the first batcher: on the egress at
    /// `egress` when present, otherwise on the cluster egress.
    ///
    /// # Errors
    /// Returns an error when the batcher fails to spawn.
    pub(super) fn launch(spec: &ServiceSpec<'_>, l1: &L1, egress: Option<&str>) -> Result<Self> {
        let mut side = Self {
            process: None,
            metrics_port: free_port().port(),
            l1_rpc: l1.rpc_url(),
            settlement: l1.settlement.to_string(),
            da: FakeDaProxy::start(),
        };
        side.process = Some(spec.spawn_batcher(&side, egress)?);
        Ok(side)
    }

    pub(super) fn metrics_addr(&self) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], self.metrics_port))
    }

    pub(super) fn proc(&self) -> Option<&super::proc::Proc> {
        self.process.as_ref().map(|p| &p.proc)
    }
}

impl LocalStack {
    /// The seams of the carry window, when the stack runs it.
    #[must_use]
    pub fn carry_seams(&self) -> Option<CarrySeams> {
        let egress = self.egress.as_ref()?;
        Some(CarrySeams {
            egress_url: egress.url(),
            egress_metrics: egress.service.metrics_addr,
            batcher_metrics: self.batcher.as_ref().map(BatcherSide::metrics_addr),
            sealer_admin: self.sealer.admin.clone(),
            da_proxy: self.batcher.as_ref().map(|b| b.da.url()),
        })
    }

    fn egress_url(&self) -> Result<String> {
        self.egress
            .as_ref()
            .map(SpawnedEgress::url)
            .context("the stack runs no egress (StackConfig::carry)")
    }

    /// Kill the executor (SIGKILL) and start it again in carry mode:
    /// `--carry-bytes` on the member feed of member 0, then the egress. It
    /// keeps its state directory and its metrics port.
    ///
    /// The egress here has one copy of the frames, and it chains a copy
    /// that starts again only from a frame that it holds, or from the
    /// boundary of the next empty block. A killed executor resumes at or
    /// below the blocks that it executed last: a commit settles at a later
    /// boundary. So a caller that kills it on a frozen chain, once the
    /// egress holds every frame, gets a new copy that the egress chains.
    ///
    /// # Errors
    /// Returns an error when the stack runs no egress, or when the executor
    /// fails to spawn.
    pub fn restart_executor_in_carry_mode(&mut self) -> Result<()> {
        let url = self.egress_url()?;
        self.executor.proc.kill();
        let respawned = services::spawn_executor_at(
            &self.service_spec(),
            &ExecutorRun {
                metrics_port: Some(self.executor.metrics_addr.port()),
                carry_egress: Some(&url),
            },
        )?;
        self.executor = respawned;
        Ok(())
    }

    /// Stop the validator with SIGTERM and start it again with `--egress`.
    /// It keeps its state directory and its metrics port.
    ///
    /// # Errors
    /// Returns an error when the stack runs no egress or no validator, or
    /// when the validator fails to spawn.
    pub fn restart_validator_on_egress(&mut self) -> Result<()> {
        let url = self.egress_url()?;
        let validator = self
            .validator
            .as_mut()
            .context("the stack runs no validator")?;
        validator.proc.terminate(Duration::from_secs(20));
        let port = validator.metrics_addr.port();
        let feed_addr_file = self.root.path().join("validator-feed.addr");
        let respawned = services::spawn_validator(
            &self.service_spec(),
            &ValidatorOptions {
                trie_shadow_check: self.cfg.trie_shadow_check,
                attester: self.validator_l1.as_ref(),
                parallel: self.cfg.validator_parallel,
                serve_feed_addr_file: self
                    .cfg
                    .validator_serve_feed
                    .then_some(feed_addr_file.as_path()),
                egress: Some(&url),
                metrics_port: Some(port),
            },
        )?;
        self.validator = Some(respawned);
        Ok(())
    }

    /// Stop the batcher with SIGTERM. It posts nothing until
    /// [`Self::start_batcher_on_egress`].
    ///
    /// # Errors
    /// Returns an error when the stack runs no batcher.
    pub fn stop_batcher(&mut self) -> Result<()> {
        let side = self.batcher.as_mut().context("the stack runs no batcher")?;
        if let Some(mut p) = side.process.take() {
            p.proc.terminate(Duration::from_secs(20));
        }
        Ok(())
    }

    /// Stop the batcher, then start it again with `--egress`. It resumes
    /// from its cursor file and keeps its metrics port.
    ///
    /// # Errors
    /// Returns an error when the stack runs no batcher or no egress, or when
    /// the batcher fails to spawn.
    pub fn start_batcher_on_egress(&mut self) -> Result<()> {
        let url = self.egress_url()?;
        self.stop_batcher()?;
        let mut side = self.batcher.take().context("the stack runs no batcher")?;
        side.process = Some(self.service_spec().spawn_batcher(&side, Some(&url))?);
        self.batcher = Some(side);
        Ok(())
    }

    /// Stop the egress with SIGTERM and start it again on the same store
    /// and ports.
    ///
    /// # Errors
    /// Returns an error when the stack runs no egress, or when the egress
    /// fails to spawn.
    pub fn restart_egress(&mut self) -> Result<()> {
        let egress = self.egress.as_mut().context("the stack runs no egress")?;
        egress.service.proc.terminate(Duration::from_secs(20));
        let ports = (egress.service.metrics_addr.port(), egress.http_port);
        let respawned = self.service_spec().spawn_egress(Some(ports))?;
        self.egress = Some(respawned);
        Ok(())
    }

    /// SIGSTOP every sealer member: the chain seals no block until
    /// [`Self::thaw_sealer`]. A member that stops for less than the leader
    /// heartbeat timeout (10 s) starts no election.
    pub fn freeze_sealer(&self) {
        self.sealer
            .procs
            .iter()
            .for_each(super::proc::Proc::suspend);
    }

    /// SIGCONT every sealer member after [`Self::freeze_sealer`].
    pub fn thaw_sealer(&self) {
        self.sealer.procs.iter().for_each(super::proc::Proc::resume);
    }

    /// Kill sealer member `id` and start it again from its own snapshot
    /// and log.
    ///
    /// # Errors
    /// Returns an error when the member does not come up within 60s.
    pub fn restart_sealer_member(&mut self, id: usize) -> Result<()> {
        self.sealer.restart_member(id)
    }

    /// The logs of every sealer process of the stack, as `(file name,
    /// text)`. Unlike [`Self::sealer_logs`], this includes the log of each
    /// process that a restart replaced.
    ///
    /// # Errors
    /// Returns an error when the stack root or a log cannot be read.
    pub fn every_sealer_log(&self) -> Result<Vec<(String, String)>> {
        let names = std::fs::read_dir(self.root.path())?
            .map(|entry| Ok(entry?.file_name().to_string_lossy().into_owned()))
            .collect::<Result<Vec<String>>>()?;
        names
            .into_iter()
            .filter(|name| {
                name.starts_with("sealer-")
                    && std::path::Path::new(name)
                        .extension()
                        .is_some_and(|ext| ext == "log")
            })
            .map(|name| {
                let text = std::fs::read_to_string(self.root.path().join(&name))?;
                Ok((name, text))
            })
            .collect()
    }

    /// The genesis TOML that every service of the stack reads.
    #[must_use]
    pub fn genesis_path(&self) -> &std::path::Path {
        &self.genesis
    }

    /// The tail of the log of the executor in carry mode.
    #[must_use]
    pub fn carry_executor_log(&self) -> Option<String> {
        std::fs::read_to_string(self.root.path().join("executor-carry.log")).ok()
    }
}
