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
}

impl Default for StackConfig {
    fn default() -> Self {
        Self {
            shards: 2,
            sealer_members: 1,
            cluster_tick_ms: 250,
            ingress: IngressOptions::default(),
        }
    }
}

pub struct LocalStack {
    // Drop order matters: services die before the sealer/driver they attach
    // to, and the temp root outlives everything. Fields drop in declaration
    // order.
    ingress: SpawnedIngress,
    executor: Spawned,
    sequencers: Vec<Spawned>,
    sealer: SealerCluster,
    driver: MediaDriver,
    root: tempfile::TempDir,
    keep: bool,
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
        let ingress = services::spawn_ingress(&spec, &cfg.ingress)?;

        let stack = Self {
            ingress,
            executor,
            sequencers,
            sealer,
            driver,
            root,
            keep,
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
        })
    }

    /// Path of the stack's temp root (log files, state dirs, configs).
    pub fn root(&self) -> PathBuf {
        self.root.path().to_path_buf()
    }

    fn dump_tails(&self) {
        eprintln!("=== stack log tails ({}) ===", self.root.path().display());
        let procs: Vec<&proc::Proc> = std::iter::once(&self.ingress.proc)
            .chain(std::iter::once(&self.executor.proc))
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
