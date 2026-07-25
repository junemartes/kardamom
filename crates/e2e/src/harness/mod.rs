//! Target-L: the single-host local stack for the chain-semantics suite.
//!
//! One `LocalStack::launch` brings up, on per-test temp dirs and OS-assigned
//! ports (concurrent stacks never collide):
//!
//! 1. a host-native `ArchivingMediaDriver` (the Rust services' transport),
//! 2. a 1-member Java Aeron Cluster sealer (`ClusterNode`, canonical order),
//! 3. `kardamom-sequencer` × shards, `kardamom-executor`, `kardamom-ingress`
//!    as real child processes wired via `--aeron-dir` + `[cluster]` config,
//!
//! then hands scenarios a [`crate::scenarios::Target`] (RPC + metrics seams
//! only). Drop kills everything; set `KARDAMOM_E2E_KEEP=1` to keep the temp
//! root (its path is printed) for post-mortem digging.

pub mod aeron;
pub mod inject;
pub mod l2;
pub mod metrics;
pub mod proc;
pub mod sealer;
pub mod services;

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};

use crate::scenarios::Target;
use aeron::MediaDriver;
use sealer::SealerCluster;
use services::{IngressOptions, ServiceSpec, Spawned, SpawnedIngress};

/// The dev chain id (`deploy/cluster/config/genesis/dev.toml`).
pub const DEV_CHAIN_ID: u64 = 412_346;

/// Stack knobs a scenario can tune; defaults mirror the deployed shape where
/// it matters (shards=2 like the cluster) and the test-friendly value where
/// it doesn't (250 ms boundary ticks).
pub struct StackConfig {
    pub shards: u32,
    pub sealer_members: usize,
    pub cluster_tick_ms: u64,
    pub ingress: IngressOptions,
    /// Also run `kardamom-validator` (S6/S7). Off by default — the nonce/RPC
    /// scenarios don't need it and stacks stay lighter without it.
    pub validator: bool,
    /// Trie shadow-check cadence for the validator (`Some(1)` = every block,
    /// the semantics-suite default; the cluster runs 8).
    pub trie_shadow_check: Option<u64>,
}

impl Default for StackConfig {
    fn default() -> Self {
        Self {
            shards: 2,
            sealer_members: 1,
            cluster_tick_ms: 250,
            ingress: IngressOptions::default(),
            validator: false,
            trie_shadow_check: Some(1),
        }
    }
}

/// Which services exited on SIGTERM (vs. needing the SIGKILL fallback) during
/// [`LocalStack::shutdown_graceful`]. `validator` is `None` when the stack
/// runs without one.
#[derive(Debug, Default)]
pub struct ShutdownReport {
    pub executor: bool,
    pub validator: Option<bool>,
}

pub struct LocalStack {
    // Drop order matters: services die before the sealer/driver they attach
    // to, and the temp root outlives everything. Fields drop in declaration
    // order.
    ingress: SpawnedIngress,
    executor: Spawned,
    validator: Option<Spawned>,
    sequencers: Vec<Spawned>,
    sealer: SealerCluster,
    driver: MediaDriver,
    root: tempfile::TempDir,
    keep: bool,
    shutdown_report: ShutdownReport,
    pub cfg: StackConfig,
}

impl LocalStack {
    pub async fn launch(cfg: StackConfig) -> Result<Self> {
        let repo = services::repo_root();
        let genesis = repo.join("deploy/cluster/config/genesis/dev.toml");
        anyhow::ensure!(
            genesis.is_file(),
            "genesis not found at {}",
            genesis.display()
        );
        let keep = std::env::var("KARDAMOM_E2E_KEEP").is_ok_and(|v| v == "1");
        let root = tempfile::Builder::new()
            .prefix("kardamom-e2e-")
            .tempdir()
            .context("create stack temp root")?;
        eprintln!("stack root: {}", root.path().display());

        // Bring-up is ordered by dependency, each step gated on readiness:
        // driver → sealer (LEADER) → sequencers/executor → ingress.
        let rootp = root.path().to_path_buf();
        let (driver, sealer) = {
            let repo = repo.clone();
            let tick = cfg.cluster_tick_ms;
            let members = cfg.sealer_members;
            tokio::task::spawn_blocking(move || -> Result<(MediaDriver, SealerCluster)> {
                let driver = MediaDriver::launch(&rootp)?;
                let sealer = SealerCluster::launch(&rootp, &repo, members, tick)?;
                Ok((driver, sealer))
            })
            .await
            .context("bring-up join")??
        };

        let spec = ServiceSpec {
            root: root.path(),
            aeron_dir: &driver.aeron_dir,
            cluster_ingress_endpoints: &sealer.ingress_endpoints,
            shards: cfg.shards,
            chain_id: DEV_CHAIN_ID,
            genesis: &genesis,
        };

        let mut sequencers = Vec::with_capacity(cfg.shards as usize);
        for i in 0..cfg.shards {
            sequencers.push(services::spawn_sequencer(&spec, i)?);
        }
        let executor = services::spawn_executor(&spec)?;
        let validator = if cfg.validator {
            Some(services::spawn_validator(&spec, cfg.trie_shadow_check)?)
        } else {
            None
        };
        let ingress = services::spawn_ingress(&spec, &cfg.ingress)?;

        let stack = Self {
            ingress,
            executor,
            validator,
            sequencers,
            sealer,
            driver,
            root,
            keep,
            shutdown_report: ShutdownReport::default(),
            cfg,
        };

        // Readiness: every metrics endpoint answers, and the ingress RPC
        // serves eth_chainId. (Metrics exporters run on dedicated threads, so
        // this proves process liveness; the chainId probe proves the RPC
        // server is accepting.)
        for (name, addr) in stack.metric_addrs() {
            metrics::poll_until(
                &format!("{name} /metrics"),
                Duration::from_secs(30),
                Duration::from_millis(200),
                || async move { Ok(metrics::scrape(addr).await.ok().map(|_| ())) },
            )
            .await?;
        }
        let probe = l2::L2Client::new(&stack.ingress.rpc_url, Duration::from_secs(2))?;
        metrics::poll_until(
            "ingress eth_chainId",
            Duration::from_secs(30),
            Duration::from_millis(200),
            || {
                let probe = probe.clone();
                async move { Ok(probe.chain_id().await.result.ok().map(|_| ())) }
            },
        )
        .await?;
        Ok(stack)
    }

    fn metric_addrs(&self) -> Vec<(String, std::net::SocketAddr)> {
        let mut v = vec![
            ("ingress".to_string(), self.ingress.metrics_addr),
            ("executor".to_string(), self.executor.metrics_addr),
        ];
        if let Some(val) = &self.validator {
            v.push(("validator".to_string(), val.metrics_addr));
        }
        for (i, s) in self.sequencers.iter().enumerate() {
            v.push((format!("sequencer-{i}"), s.metrics_addr));
        }
        v
    }

    /// The scenario-facing seam. `rpc_timeout` is the client-side request
    /// bound; keep it ABOVE the ingress pending-receipt timeout so scenarios
    /// observe the server-side `-32000`, not a client abort.
    pub fn target(&self, rpc_timeout: Duration) -> Result<Target> {
        Ok(Target {
            rpc: l2::L2Client::new(&self.ingress.rpc_url, rpc_timeout)?,
            chain_id: DEV_CHAIN_ID,
            pending_receipt_timeout: self.cfg.ingress.pending_receipt_timeout,
            ingress_metrics: self.ingress.metrics_addr,
            executor_metrics: self.executor.metrics_addr,
            sequencer_metrics: self.sequencers.iter().map(|s| s.metrics_addr).collect(),
            validator_metrics: self.validator.as_ref().map(|v| v.metrics_addr),
        })
    }

    /// Path of the stack's temp root (log files, state dirs, configs).
    pub fn root(&self) -> PathBuf {
        self.root.path().to_path_buf()
    }

    /// The shared media driver's `aeron.dir` (for test-side stream injection).
    pub fn aeron_dir(&self) -> PathBuf {
        self.driver.aeron_dir.clone()
    }

    pub fn executor_state_dir(&self) -> Option<PathBuf> {
        self.executor.state_dir.clone()
    }

    pub fn validator_state_dir(&self) -> Option<PathBuf> {
        self.validator.as_ref().and_then(|v| v.state_dir.clone())
    }

    /// Freeze the executor (SIGSTOP): its genuine BAL/receipt publications
    /// stop so an injected corrupt frame faces no competition (S7).
    pub fn suspend_executor(&self) {
        self.executor.proc.suspend();
    }

    pub fn resume_executor(&self) {
        self.executor.proc.resume();
    }

    /// Wait for the validator process to exit on its own; returns its exit
    /// code (S7 expects the divergence fail-stop's exit 2).
    pub fn wait_validator_exit(&mut self, timeout: Duration) -> Option<Option<i32>> {
        self.validator.as_mut()?.proc.wait_exit(timeout)
    }

    pub fn validator_log(&self) -> Option<String> {
        let v = self.validator.as_ref()?;
        std::fs::read_to_string(&v.proc.log_path).ok()
    }

    /// Stop the pipeline so both state DBs freeze at the SAME final block,
    /// then shut the executor + validator down cleanly (mdbx envs closed) for
    /// offline inspection.
    ///
    /// Order matters twice over:
    /// 1. ingress + sequencers die first, so no new transactions enter;
    /// 2. the sealer is **SIGSTOPped, not killed** — that halts the boundary
    ///    clock (otherwise it keeps stamping empty blocks and the two
    ///    consumers never settle on a common head) while leaving the cluster
    ///    sessions intact. Killing it instead wedges shutdown: the Rust
    ///    cluster client treats a vanished sealer as a reconnect-with-backoff
    ///    case and retries forever, so the reader never sees end-of-stream
    ///    and the process ignores SIGTERM until the harness SIGKILLs it.
    /// 3. only then SIGTERM the consumers, which close their mdbx envs.
    pub async fn shutdown_graceful(&mut self) -> Result<()> {
        self.ingress.proc.terminate(Duration::from_secs(10));
        for s in &mut self.sequencers {
            s.proc.terminate(Duration::from_secs(10));
        }
        for p in &self.sealer.procs {
            p.suspend();
        }

        // Drain: executor's block freezes (no more boundaries) and the
        // validator catches up to the same block.
        let exec_addr = self.executor.metrics_addr;
        let val_addr = self.validator.as_ref().map(|v| v.metrics_addr);
        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        let mut last_exec = -1.0f64;
        loop {
            let exec_block = metrics::scrape(exec_addr)
                .await?
                .value(crate::scenarios::EXEC_BLOCK_NUMBER)
                .unwrap_or(0.0);
            let val_ok = match val_addr {
                None => true,
                Some(addr) => {
                    let committed = metrics::scrape(addr)
                        .await?
                        .value(crate::scenarios::VALIDATOR_COMMITTED_BLOCK)
                        .unwrap_or(0.0);
                    committed == exec_block
                }
            };
            if val_ok && exec_block == last_exec {
                break; // stable across one interval AND validator caught up
            }
            last_exec = exec_block;
            anyhow::ensure!(
                std::time::Instant::now() < deadline,
                "drain: executor/validator did not settle on a common block in 60s"
            );
            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        // Both consumers are committed through the same block now, so SIGTERM
        // them and require a CLEAN exit from each. This doubles as the
        // regression test for the receipts-pump ownership cycle that made the
        // validator ignore SIGTERM entirely (fixed by
        // `TxReceiptsSubscriberHandle::into_receiver`): before the fix the
        // validator sat through 90s+ of SIGTERM, so a 20s bound catches any
        // return of that deadlock. `terminate` SIGKILLs on overrun, which
        // keeps the DBs comparable either way (commits are atomic), but the
        // assertions below make a regression loud rather than silent.
        let honored = ShutdownReport {
            executor: self.executor.proc.terminate(Duration::from_secs(20)),
            validator: self
                .validator
                .as_mut()
                .map(|v| v.proc.terminate(Duration::from_secs(20))),
        };
        self.shutdown_report = honored;
        anyhow::ensure!(
            self.shutdown_report.executor,
            "executor did not exit on SIGTERM within 20s"
        );
        anyhow::ensure!(
            self.shutdown_report.validator != Some(false),
            "validator did not exit on SIGTERM within 20s — the Aeron-runtime \
             ownership cycle in the receipts pump is back (see \
             TxReceiptsSubscriberHandle::into_receiver)"
        );
        Ok(())
    }

    /// Which services honored SIGTERM during [`Self::shutdown_graceful`].
    pub fn shutdown_report(&self) -> &ShutdownReport {
        &self.shutdown_report
    }

    fn dump_tails(&self) {
        eprintln!("=== stack log tails ({}) ===", self.root.path().display());
        let procs: Vec<&proc::Proc> = std::iter::once(&self.ingress.proc)
            .chain(std::iter::once(&self.executor.proc))
            .chain(self.validator.as_ref().map(|v| &v.proc))
            .chain(self.sequencers.iter().map(|s| &s.proc))
            .chain(self.sealer.procs.iter())
            .chain(std::iter::once(&self.driver.proc))
            .collect();
        for p in procs {
            eprintln!("--- {} ---\n{}", p.name, p.log_tail(25));
        }
    }
}

impl Drop for LocalStack {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.dump_tails();
        }
        if self.keep || std::thread::panicking() {
            // Keep the temp root for post-mortem (explicit opt-in, or any
            // failing test). TempDir::keep consumes; disable cleanup instead.
            let path = self.root.path().to_path_buf();
            eprintln!("keeping stack root at {}", path.display());
            self.root.disable_cleanup(true);
        }
    }
}
