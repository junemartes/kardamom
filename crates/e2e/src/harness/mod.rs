//! Target-L: the single-host local stack for the chain-semantics suite.
//!
//! One `LocalStack::launch` call brings up the following, on per-test temp
//! directories and OS-assigned ports (so concurrent stacks never collide):
//!
//! 1. a host-native `ArchivingMediaDriver` (the transport for the Rust
//!    services).
//! 2. a 1-member Java Aeron Cluster sealer (`ClusterNode`, canonical
//!    order).
//! 3. `kardamom-sequencer` (one per shard), `kardamom-executor`, and
//!    `kardamom-ingress`, as real child processes wired with `--aeron-dir`
//!    and a `[cluster]` config.
//!
//! It then hands scenarios a [`crate::scenarios::Target`] (only the RPC
//! and metrics seams). Drop kills everything. Set `KARDAMOM_E2E_KEEP=1` to
//! keep the temp root (its path is printed) for later inspection.

pub mod aeron;
pub mod inject;
pub mod l1;
pub mod l1_verified;
pub mod l2;
pub mod metrics;
pub mod proc;
pub mod sealer;
pub mod services;

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result};

use crate::scenarios::Target;
use aeron::MediaDriver;
use sealer::SealerCluster;
use services::{IngressOptions, ServiceSpec, Spawned, SpawnedIngress};

/// The dev chain id (`deploy/cluster/config/genesis/dev.toml`).
pub const DEV_CHAIN_ID: u64 = 412_346;

/// Stack settings a scenario can tune. The defaults match the deployed
/// shape where it matters (shards=2, like the cluster), and use a
/// test-friendly value where it does not (250 ms boundary ticks).
pub struct StackConfig {
    pub shards: u32,
    pub sealer_members: usize,
    pub cluster_tick_ms: u64,
    pub ingress: IngressOptions,
    /// Also run `kardamom-validator` (used by the consistency and
    /// divergence-detection tests). Off by default: the nonce and RPC
    /// scenarios do not need it, and stacks stay lighter without it.
    pub validator: bool,
    /// Trie shadow-check cadence for the validator. `Some(1)` checks every
    /// block, the semantics-suite default. The cluster runs with 8.
    pub trie_shadow_check: Option<u64>,
    /// Route the validator's L1 reads through the mock verified endpoint
    /// (`l1_verified`), the way production routes them through a light
    /// client. The da-watcher keeps talking to anvil directly. Only the
    /// verifier's view is interposed, so a fault isolates to verification
    /// and does not also corrupt the epochs being produced.
    pub verified_l1: bool,
    /// Which L2 genesis to run. Bridge scenarios need
    /// [`Genesis::DevWithdrawals`] for the `L2ToL1MessagePasser` predeploy.
    pub genesis: Genesis,
    /// Bring up anvil, the bridge contracts, the da-watcher, and, when a
    /// validator runs, its L1 output attester.
    pub l1: bool,
    /// Record tx_data into the shared Aeron archive (the ingress
    /// `--archive-durability` flag) and turn on the consumers' join-miss
    /// refetch. Crash recovery needs this: a restarted consumer replays
    /// canonical records from its persisted cursor, but the envelopes for
    /// those records were published while it was down, so only the
    /// archive still has them. This costs a recorder-startup barrier at
    /// bring-up, so it is opt-in.
    pub archive_durability: bool,
}

/// The L2 genesis a stack boots from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Genesis {
    /// `deploy/cluster/config/genesis/dev.toml`: 18 prefunded dev accounts,
    /// with no withdrawal predeploy. This is what the deployed cluster
    /// runs.
    ClusterDev,
    /// `chains/dev-withdrawals.toml`: the `L2ToL1MessagePasser` predeploy
    /// at `0x42…16`, but only account #0 is prefunded.
    DevWithdrawals,
}

impl Genesis {
    fn path(self, repo: &std::path::Path) -> PathBuf {
        match self {
            Genesis::ClusterDev => repo.join("deploy/cluster/config/genesis/dev.toml"),
            Genesis::DevWithdrawals => repo.join("chains/dev-withdrawals.toml"),
        }
    }
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
            genesis: Genesis::ClusterDev,
            l1: false,
            archive_durability: false,
            verified_l1: false,
        }
    }
}

/// Which services exited on SIGTERM, instead of needing the SIGKILL
/// fallback, during [`LocalStack::shutdown_graceful`]. `validator` is
/// `None` when the stack runs without one.
#[derive(Debug, Default)]
pub struct ShutdownReport {
    pub executor: bool,
    pub validator: Option<bool>,
}

pub struct LocalStack {
    // Drop order matters. Services must die before the sealer and driver
    // they attach to, and the temp root must outlive everything. Fields
    // drop in declaration order.
    ingress: SpawnedIngress,
    da_watcher: Option<Spawned>,
    verified_l1: Option<crate::harness::l1_verified::VerifiedL1>,
    executor: Spawned,
    validator: Option<Spawned>,
    sequencers: Vec<Spawned>,
    sealer: SealerCluster,
    driver: MediaDriver,
    /// Samples `/proc/loadavg` into `<root>/host-load.log` while the stack
    /// is up. Driver-stall flakes correlate with host load, so a kept
    /// failure root carries the load timeline next to the service logs.
    _load_sampler: LoadSampler,
    /// Anvil and the bridge contracts (`StackConfig::l1`). This drops
    /// last among the services, so a scenario's L1 queries stay live
    /// until teardown.
    l1: Option<l1::L1>,
    /// Resolved at launch and reused by [`Self::service_spec`]. A restarted
    /// service then runs from the same genesis as the original bring-up.
    genesis: PathBuf,
    /// The same `--log-config` as the original bring-up (the
    /// archive-durability `channels.toml`, written once at launch). Also
    /// reused by [`Self::service_spec`] for a restarted service.
    log_config: Option<PathBuf>,
    root: tempfile::TempDir,
    keep: bool,
    shutdown_report: ShutdownReport,
    pub cfg: StackConfig,
}

/// The one place that assembles a service `ServiceSpec`. Bring-up
/// (`launch_with_l1`) and executor restart ([`LocalStack::service_spec`])
/// both go through here, so the two cannot drift apart.
fn assemble_spec<'a>(
    root: &'a Path,
    driver: &'a MediaDriver,
    sealer: &'a SealerCluster,
    shards: u32,
    genesis: &'a Path,
    log_config: Option<&'a Path>,
) -> ServiceSpec<'a> {
    ServiceSpec {
        root,
        aeron_dir: &driver.aeron_dir,
        cluster_ingress_endpoints: &sealer.ingress_endpoints,
        shards,
        chain_id: DEV_CHAIN_ID,
        genesis,
        log_config,
    }
}

impl LocalStack {
    /// Bring up a stack. Returns `Ok(None)` only when `cfg.l1` is set and
    /// the `anvil` binary is missing. Bridge scenarios then skip, instead
    /// of failing on a machine with no Foundry, matching the convention of
    /// the crate-level anvil tests.
    pub async fn launch_opt(cfg: StackConfig) -> Result<Option<Self>> {
        let l1 = if cfg.l1 {
            match l1::L1::launch(DEV_CHAIN_ID).await? {
                Some(l1) => Some(l1),
                None => return Ok(None),
            }
        } else {
            None
        };
        Self::launch_with_l1(cfg, l1).await.map(Some)
    }

    pub async fn launch(cfg: StackConfig) -> Result<Self> {
        anyhow::ensure!(
            !cfg.l1,
            "use LocalStack::launch_opt for L1-backed stacks (it skips cleanly without anvil)"
        );
        Self::launch_with_l1(cfg, None).await
    }

    async fn launch_with_l1(cfg: StackConfig, l1: Option<l1::L1>) -> Result<Self> {
        let repo = services::repo_root();
        let genesis = cfg.genesis.path(&repo);
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

        // Bring-up follows a dependency order, and each step waits for
        // readiness: driver, then sealer (leader), then sequencers and
        // executor, then ingress.
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

        // The archive-durability variant: a log config that keeps the
        // built-in IPC `[channels]` defaults (omitted fields inherit
        // them), and adds only the `[aeron]` archive settings the
        // recorder and refetch need.
        let log_config = if cfg.archive_durability {
            let p = root.path().join("channels.toml");
            std::fs::write(
                &p,
                format!(
                    "[aeron]\narchive_dir = \"{}\"\n\
                     tx_data_archive_endpoints = [\"{}\"]\n\
                     tx_deposits_archive_endpoints = [\"{}\"]\n",
                    driver.archive_dir.display(),
                    driver.archive_control_endpoint,
                    driver.archive_control_endpoint,
                ),
            )
            .context("write archive log-config")?;
            Some(p)
        } else {
            None
        };

        let spec = assemble_spec(
            root.path(),
            &driver,
            &sealer,
            cfg.shards,
            &genesis,
            log_config.as_deref(),
        );

        let mut sequencers = Vec::with_capacity(cfg.shards as usize);
        for i in 0..cfg.shards {
            sequencers.push(services::spawn_sequencer(&spec, i)?);
        }
        let executor = services::spawn_executor(&spec)?;
        let wiring = l1.as_ref().map(|l| services::L1Wiring {
            rpc_url: l.rpc_url(),
            lockbox: l.lockbox.to_string(),
            oracle: l.oracle.to_string(),
            attester_key: l1::ATTESTER_KEY.to_string(),
        });
        // Interpose the mock verified endpoint on the validator's L1 view
        // only. The da-watcher keeps reading anvil directly, so a fault
        // isolates to verification and does not also corrupt the epochs
        // under test.
        let verified_l1 = match (cfg.verified_l1, l1.as_ref()) {
            (true, Some(l)) => {
                Some(crate::harness::l1_verified::VerifiedL1::spawn(&l.rpc_url()).await?)
            }
            _ => None,
        };
        let validator_wiring = match (&wiring, &verified_l1) {
            (Some(w), Some(v)) => Some(services::L1Wiring {
                rpc_url: v.url(),
                ..w.clone()
            }),
            _ => wiring.clone(),
        };
        let validator = if cfg.validator {
            Some(services::spawn_validator(
                &spec,
                cfg.trie_shadow_check,
                validator_wiring.as_ref(),
            )?)
        } else {
            None
        };
        let da_watcher = match &wiring {
            Some(w) => Some(services::spawn_da_watcher(&spec, w)?),
            None => None,
        };
        let ingress = services::spawn_ingress(&spec, &cfg.ingress)?;

        let load_sampler = LoadSampler::start(root.path().join("host-load.log"));
        let stack = Self {
            ingress,
            da_watcher,
            verified_l1,
            executor,
            validator,
            sequencers,
            sealer,
            driver,
            _load_sampler: load_sampler,
            l1,
            genesis,
            log_config,
            root,
            keep,
            shutdown_report: ShutdownReport::default(),
            cfg,
        };

        // Readiness check: every metrics endpoint answers, and the ingress
        // RPC serves eth_chainId. (Metrics exporters run on dedicated
        // threads, so this proves the process is alive. The chainId probe
        // proves the RPC server accepts requests.)
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
        if let Some(w) = &self.da_watcher {
            v.push(("da-watcher".to_string(), w.metrics_addr));
        }
        for (i, s) in self.sequencers.iter().enumerate() {
            v.push((format!("sequencer-{i}"), s.metrics_addr));
        }
        v
    }

    /// The seam that scenarios use. `rpc_timeout` is the client-side
    /// request limit. Keep it above the ingress pending-receipt timeout,
    /// so scenarios see the server's `-32000` error, not a client abort.
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

    /// Path of the stack's temp root (log files, state directories,
    /// configs).
    pub fn root(&self) -> PathBuf {
        self.root.path().to_path_buf()
    }

    /// The anvil L1 and bridge contracts (`StackConfig::l1`).
    pub fn l1(&self) -> Option<&l1::L1> {
        self.l1.as_ref()
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

    /// Liveness probe for the validator process, for failure diagnostics.
    /// The bridge-withdrawal-test mdbx-read-only failure family depends on
    /// whether the validator was already dead at probe time (an
    /// Aeron driver-timeout abort leaves the environment unsteadily
    /// closed). `None` when the stack runs no validator.
    pub fn validator_alive(&mut self) -> Option<bool> {
        self.validator.as_mut().map(|v| v.proc.is_alive())
    }

    /// SIGSTOP the sealer, so it stops stamping block boundaries. The
    /// chain settles on one final head and stays there. Scenarios that
    /// must reason about "the current block" with no race (the
    /// bridge-withdrawal test matches a withdrawal to the attested output
    /// for its block) freeze the clock first. This suspends the sealer
    /// instead of killing it. Killing it would make the consumers'
    /// cluster clients retry forever and wedge shutdown.
    pub fn freeze_block_clock(&self) {
        for p in &self.sealer.procs {
            p.suspend();
        }
    }

    /// SIGKILL the executor: an unclean crash, with no shutdown hooks and
    /// no final flush. What survives is exactly what mdbx committed.
    pub fn crash_executor(&mut self) {
        self.executor.proc.kill();
    }

    /// The [`ServiceSpec`] this stack launched its services from, rebuilt
    /// from the launch-time state (same genesis, same `--log-config`).
    fn service_spec(&self) -> ServiceSpec<'_> {
        assemble_spec(
            self.root.path(),
            &self.driver,
            &self.sealer,
            self.cfg.shards,
            &self.genesis,
            self.log_config.as_deref(),
        )
    }

    /// Restart the executor against the same state directory and metrics
    /// port. This makes it resume from its persisted cursor, instead of
    /// re-syncing from genesis, and lets scenarios keep the address they
    /// already hold.
    pub fn restart_executor(&mut self) -> Result<()> {
        let port = self.executor.metrics_addr.port();
        let respawned = services::spawn_executor_at(&self.service_spec(), Some(port))?;
        self.executor = respawned;
        Ok(())
    }

    /// Tail of the restarted executor's log. The crash-recovery scenario
    /// checks its resume line.
    pub fn restarted_executor_log(&self) -> Option<String> {
        std::fs::read_to_string(self.root.path().join("executor-restarted.log")).ok()
    }

    /// The mock verified L1 endpoint, when `StackConfig::verified_l1` is on.
    pub fn verified_l1(&self) -> Option<&crate::harness::l1_verified::VerifiedL1> {
        self.verified_l1.as_ref()
    }

    /// SIGSTOP the DA watcher, so no honest epoch competes with an
    /// injected one. This is the same determinism trick that
    /// `suspend_executor` gives the BAL drill. Returns false when the
    /// stack has no watcher (no L1).
    pub fn suspend_da_watcher(&self) -> bool {
        match &self.da_watcher {
            Some(w) => {
                w.proc.suspend();
                true
            }
            None => false,
        }
    }

    /// Freeze the executor (SIGSTOP). Its genuine BAL and receipt
    /// publications stop, so an injected corrupt frame faces no
    /// competition (the divergence-detection test).
    pub fn suspend_executor(&self) {
        self.executor.proc.suspend();
    }

    pub fn resume_executor(&self) {
        self.executor.proc.resume();
    }

    /// Wait for the validator process to exit on its own. Returns its
    /// exit code (exit code 2 is the divergence fail-stop).
    pub fn wait_validator_exit(&mut self, timeout: Duration) -> Option<Option<i32>> {
        self.validator.as_mut()?.proc.wait_exit(timeout)
    }

    pub fn validator_log(&self) -> Option<String> {
        let v = self.validator.as_ref()?;
        std::fs::read_to_string(&v.proc.log_path).ok()
    }

    /// Stop the pipeline so both state databases freeze at the same final
    /// block, then shut the executor and validator down cleanly (with
    /// mdbx environments closed) for offline inspection.
    ///
    /// Order matters here, in two ways:
    /// 1. ingress and sequencers die first, so no new transactions enter.
    /// 2. the sealer is SIGSTOPped, not killed. This halts the boundary
    ///    clock (otherwise it keeps stamping empty blocks, and the two
    ///    consumers never settle on a common head), while leaving the
    ///    cluster sessions intact. Killing it instead would wedge
    ///    shutdown: the Rust cluster client treats a vanished sealer as a
    ///    case to reconnect with backoff, and retries forever. So the
    ///    reader never sees the end of the stream, and the process
    ///    ignores SIGTERM until the harness sends SIGKILL.
    /// 3. only then does SIGTERM go to the consumers, which closes their
    ///    mdbx environments.
    pub async fn shutdown_graceful(&mut self) -> Result<()> {
        self.ingress.proc.terminate(Duration::from_secs(10));
        if let Some(w) = &mut self.da_watcher {
            w.proc.terminate(Duration::from_secs(10));
        }
        for s in &mut self.sequencers {
            s.proc.terminate(Duration::from_secs(10));
        }
        for p in &self.sealer.procs {
            p.suspend();
        }

        self.drain_until_settled().await?;

        // Both consumers are caught up now, so send SIGTERM to them and
        // require a clean exit from each. This also serves as the
        // regression test for the receipts-pump ownership cycle that made
        // the validator ignore SIGTERM entirely (fixed by
        // `TxReceiptsSubscriberHandle::into_receiver`). Before the fix,
        // the validator sat through SIGTERM for over 90 seconds, so a
        // 20-second limit catches that deadlock if it returns.
        // `terminate` sends SIGKILL on overrun, which keeps the databases
        // comparable either way (commits are atomic), but the checks
        // below make a regression loud instead of silent.
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

    /// Drain: wait until both consumers stop advancing and the validator
    /// is no longer behind. [`Self::shutdown_graceful`] calls this after
    /// the producers are gone and the sealer is SIGSTOPped.
    ///
    /// This does not wait for "the two gauges are equal". `EXEC_BLOCK_NUMBER`
    /// is the newest durable block, set in the inflight sweep as the state
    /// writer settles, and commits are pipelined several blocks deep. A
    /// commit settles "at a later boundary's sweep, or at end of stream".
    /// The caller has SIGSTOPped the sealer, so no later boundary is
    /// coming: the last few executed blocks stay unsettled until the
    /// SIGTERM that follows this drain ends the stream. Requiring
    /// equality here would wait for something that cannot happen until
    /// after this loop, so it would always burn the full 60-second
    /// timeout.
    ///
    /// So the validator legitimately sits ahead of the executor's
    /// durability gauge, and it can never run ahead of what the executor
    /// actually executed, since it verifies off that output. "Both
    /// stable, validator >= executor" is therefore the honest settled
    /// condition. The executor flushes its pipeline on the clean exit
    /// that follows, and the offline phase then compares equal final
    /// blocks from the two persisted databases.
    async fn drain_until_settled(&self) -> Result<()> {
        let exec_addr = self.executor.metrics_addr;
        let val_addr = self.validator.as_ref().map(|v| v.metrics_addr);
        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        let mut last_exec = -1.0f64;
        let mut last_val = -1.0f64;
        loop {
            let exec_block = metrics::scrape(exec_addr)
                .await?
                .value(crate::scenarios::EXEC_BLOCK_NUMBER)
                .unwrap_or(0.0);
            let val_block = match val_addr {
                None => exec_block, // no validator: treat its half as already settled
                Some(addr) => metrics::scrape(addr)
                    .await?
                    .value(crate::scenarios::VALIDATOR_COMMITTED_BLOCK)
                    .unwrap_or(0.0),
            };
            if exec_block == last_exec && val_block == last_val && val_block >= exec_block {
                // Both stable across one interval, validator not behind.
                return Ok(());
            }
            last_exec = exec_block;
            last_val = val_block;
            anyhow::ensure!(
                std::time::Instant::now() < deadline,
                "drain: executor/validator did not settle in 60s                  (executor durable {exec_block}, validator committed {val_block})"
            );
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
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
            .chain(self.da_watcher.as_ref().map(|w| &w.proc))
            .chain(self.sequencers.iter().map(|s| &s.proc))
            .chain(self.sealer.procs.iter())
            .chain(std::iter::once(&self.driver.proc))
            .collect();
        for p in procs {
            eprintln!("--- {} ---\n{}", p.name, p.log_tail(25));
        }
    }
}

/// Background `/proc/loadavg` sampler, at a 500 ms cadence. It writes
/// epoch-stamped lines to a file in the stack root. One sampler per
/// stack; it stops when dropped.
struct LoadSampler {
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
    join: Option<std::thread::JoinHandle<()>>,
}

impl LoadSampler {
    fn start(path: PathBuf) -> Self {
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stop2 = stop.clone();
        let join = std::thread::Builder::new()
            .name("load-sampler".into())
            .spawn(move || {
                use std::io::Write;
                let Ok(mut f) = std::fs::File::create(&path) else {
                    return;
                };
                while !stop2.load(std::sync::atomic::Ordering::Relaxed) {
                    let load = std::fs::read_to_string("/proc/loadavg").unwrap_or_default();
                    let epoch_ms = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_millis())
                        .unwrap_or(0);
                    let _ = writeln!(f, "{epoch_ms} {}", load.trim());
                    std::thread::sleep(Duration::from_millis(500));
                }
            })
            .ok();
        Self { stop, join }
    }
}

impl Drop for LoadSampler {
    fn drop(&mut self) {
        self.stop.store(true, std::sync::atomic::Ordering::Relaxed);
        if let Some(j) = self.join.take() {
            let _ = j.join();
        }
    }
}

impl Drop for LocalStack {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.dump_tails();
        }
        if self.keep || std::thread::panicking() {
            // Keep the temp root for inspection later (an explicit opt-in,
            // or any failing test). `TempDir::keep` consumes the value, so
            // disable cleanup instead.
            let path = self.root.path().to_path_buf();
            eprintln!("keeping stack root at {}", path.display());
            self.root.disable_cleanup(true);
        }
    }
}
