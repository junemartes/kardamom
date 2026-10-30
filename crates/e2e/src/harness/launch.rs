//! Bring-up: `LocalStack::launch`, `launch_opt`, and `launch_with_l1`.
//!
//! Bring-up follows a dependency order, and each step waits for
//! readiness: driver, then sealer (leader), then sequencers and executor,
//! then ingress, then the readiness barriers in
//! [`LocalStack::await_ready`].

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result};

use super::aeron::MediaDriver;
use super::config::{DEV_CHAIN_ID, StackConfig};
use super::sealer::SealerCluster;
use super::services::{self, ServiceSpec};
use super::{LoadSampler, LocalStack, ShutdownReport};

/// [`StackLaunch::build_l1_wiring`]'s result: the da-watcher's L1 wiring
/// (when L1 is up), the interposed mock verified-L1 endpoint (when
/// [`StackConfig::verified_l1`] is set), and the validator's own L1
/// wiring (routed through the mock endpoint when both are present).
struct L1Wirings {
    da_watcher: Option<services::L1Wiring>,
    verified_l1: Option<super::l1_verified::VerifiedL1>,
    validator: Option<services::L1Wiring>,
}

/// Bring-up state shared by the steps that assemble one stack: the
/// config, the stack root, and the driver and sealer already launched.
/// Each step reads this as state instead of taking it as loose
/// parameters, so the steps that build on each other (the spec needs the
/// genesis path and log config the earlier steps produce) cannot drift.
pub(super) struct StackLaunch<'a> {
    cfg: &'a StackConfig,
    root: &'a Path,
    driver: &'a MediaDriver,
    sealer: &'a SealerCluster,
    /// The executor's nonce query port. Picked once at bring-up and kept
    /// for every respawn, so a restarted executor serves the address the
    /// sequencers already query.
    executor_query_port: u16,
}

impl<'a> StackLaunch<'a> {
    pub(super) fn new(
        cfg: &'a StackConfig,
        root: &'a Path,
        driver: &'a MediaDriver,
        sealer: &'a SealerCluster,
    ) -> Self {
        Self {
            cfg,
            root,
            driver,
            sealer,
            executor_query_port: kardamom_obs::testkit::free_port().port(),
        }
    }

    /// Reuse the nonce query port of an earlier bring-up.
    pub(super) fn with_executor_query_port(mut self, port: u16) -> Self {
        self.executor_query_port = port;
        self
    }

    /// The nonce query port this launch hands the services.
    pub(super) fn executor_query_port(&self) -> u16 {
        self.executor_query_port
    }

    /// Materialise the genesis for `cfg.chain_id`: the canonical TOML
    /// as-is at the default id with fees off, or a patched copy in the
    /// stack root (the `chain_id` line rewritten, the canary's accounts
    /// and the fee schedule fragment appended when on — alloc + predeploy
    /// bytecode ride along byte-identical, so the deployer's drift guard
    /// keeps watching the ONE canonical copy). In carry mode from genesis,
    /// the copy starts with the `genesis_features` line: a top-level key
    /// must come before the first table of the TOML.
    fn materialise_genesis(&self, canonical: &Path) -> Result<PathBuf> {
        let chain_id = self.cfg.chain_id;
        let carry_genesis = self.cfg.carry_genesis();
        if chain_id == DEV_CHAIN_ID
            && !self.cfg.priority_fees
            && !self.cfg.canary_genesis
            && !carry_genesis
        {
            return Ok(canonical.to_path_buf());
        }
        let features = if carry_genesis {
            format!("genesis_features = [{}]\n", super::config::GENESIS_FEATURES)
        } else {
            String::new()
        };
        let src = std::fs::read_to_string(canonical)
            .map(|body| format!("{features}{body}"))
            .with_context(|| format!("read genesis {}", canonical.display()))?;
        // The order of the deploy: the canary's accounts, then the fees.
        let repo = services::repo_root();
        let fragments = [
            (
                self.cfg.canary_genesis,
                super::config::Genesis::canary_fragment(&repo),
            ),
            (
                self.cfg.priority_fees,
                super::config::Genesis::fees_fragment(&repo),
            ),
        ];
        let src = std::iter::once(Ok(src))
            .chain(fragments.iter().filter(|(on, _)| *on).map(|(_, fragment)| {
                std::fs::read_to_string(fragment)
                    .with_context(|| format!("read genesis fragment {}", fragment.display()))
            }))
            .collect::<Result<Vec<_>>>()?
            .join("\n");
        let i = src
            .lines()
            .position(|l| l.trim_start().starts_with("chain_id"))
            .with_context(|| {
                format!(
                    "genesis {} has no chain_id line to override",
                    canonical.display()
                )
            })?;
        let patched: String = src
            .lines()
            .enumerate()
            .map(|(n, line)| {
                if n == i {
                    format!("chain_id = {chain_id}\n")
                } else {
                    format!("{line}\n")
                }
            })
            .collect();
        let path = self.root.join(format!("genesis-chain-{chain_id}.toml"));
        std::fs::write(&path, patched).context("write patched genesis")?;
        Ok(path)
    }

    /// The archive-durability variant: a log config that keeps the
    /// built-in IPC `[channels]` defaults (omitted fields inherit them),
    /// and adds only the `[aeron]` archive settings the recorder and
    /// refetch need.
    fn write_archive_log_config(&self) -> Result<Option<PathBuf>> {
        if !self.cfg.archive_durability {
            return Ok(None);
        }
        let p = self.root.join("channels.toml");
        std::fs::write(
            &p,
            format!(
                "[aeron]\narchive_dir = \"{}\"\n\
                 tx_data_archive_endpoints = [\"{}\"]\n\
                 tx_deposits_archive_endpoints = [\"{}\"]\n",
                self.driver.archive_dir.display(),
                self.driver.archive_control_endpoint,
                self.driver.archive_control_endpoint,
            ),
        )
        .context("write archive log-config")?;
        Ok(Some(p))
    }

    /// L1 wiring for the services that need it: the da-watcher always
    /// (when L1 is up), the validator through
    /// [`super::l1_verified::VerifiedL1`] when
    /// [`StackConfig::verified_l1`] interposes the mock endpoint.
    /// Interposing only the validator's view means a fault isolates to
    /// verification and does not also corrupt the epochs under test.
    async fn build_l1_wiring(&self, l1: Option<&super::l1::L1>) -> Result<L1Wirings> {
        let da_watcher = l1.map(|l| services::L1Wiring {
            rpc_url: l.rpc_url(),
            lockbox: l.lockbox.to_string(),
            oracle: l.oracle.to_string(),
            attester_key: super::l1::ATTESTER_KEY.to_string(),
        });
        let verified_l1 = match (self.cfg.verified_l1, l1) {
            (true, Some(l)) => Some(
                super::l1_verified::VerifiedL1::spawn(&l.rpc_url(), "127.0.0.1:0".parse()?).await?,
            ),
            _ => None,
        };
        let validator = match (&da_watcher, &verified_l1) {
            (Some(w), Some(v)) => Some(services::L1Wiring {
                rpc_url: v.url(),
                ..w.clone()
            }),
            _ => da_watcher.clone(),
        };
        Ok(L1Wirings {
            da_watcher,
            verified_l1,
            validator,
        })
    }

    /// Assemble the service `ServiceSpec`. Bring-up (`launch_with_l1`) and
    /// executor restart ([`LocalStack::service_spec`]) both go through
    /// here (directly, or through this builder), so the two cannot drift
    /// apart.
    pub(super) fn assemble_spec(
        &self,
        genesis: &'a Path,
        log_config: Option<&'a Path>,
    ) -> ServiceSpec<'a> {
        ServiceSpec {
            root: self.root,
            aeron_dir: &self.driver.aeron_dir,
            cluster_ingress_endpoints: &self.sealer.ingress_endpoints,
            shards: self.cfg.shards,
            priority_fees: self.cfg.priority_fees,
            tx_ttl: self.cfg.ingress.pending_receipt_timeout.as_duration(),
            executor_query: std::net::SocketAddr::from(([127, 0, 0, 1], self.executor_query_port)),
            chain_id: self.cfg.chain_id.get(),
            genesis,
            log_config,
            admission: self.v2_loop(),
            // KARDAMOM_E2E_VERDICTS=1 adds the verdict path to the v2 loop.
            // The carry window restarts a sequencer on the v1 loop, which
            // sends no verdict, so it keeps the receipts path.
            verdicts: self.v2_loop()
                && !self.cfg.ingress_rows
                && self.cfg.carry.is_none()
                && std::env::var("KARDAMOM_E2E_VERDICTS").is_ok_and(|v| v == "1"),
            exec_frames: self.cfg.carry.is_some(),
        }
    }

    /// Spawn one sequencer per shard. In carry mode from genesis each one
    /// starts at the floor 5, so that no reference without bytes goes out
    /// before the first report of the sealer.
    fn spawn_sequencers(&self, spec: &ServiceSpec<'_>) -> Result<Vec<services::Spawned>> {
        let opts = services::SequencerOptions {
            sealer_version_floor: if self.cfg.carry_genesis() { 5 } else { 0 },
            ..services::SequencerOptions::default()
        };
        (0..self.cfg.shards.get())
            .map(|i| services::spawn_sequencer_with(spec, i, &opts))
            .collect()
    }

    /// Spawn the validator when the stack runs one: on the egress at
    /// `egress` when present, otherwise on the cluster egress.
    fn spawn_validator(
        &self,
        spec: &ServiceSpec<'_>,
        attester: Option<&services::L1Wiring>,
        egress: Option<&str>,
    ) -> Result<Option<services::Spawned>> {
        let feed_addr_file = self.root.join("validator-feed.addr");
        self.cfg
            .validator
            .then(|| {
                services::spawn_validator(
                    spec,
                    &services::ValidatorOptions {
                        trie_shadow_check: self.cfg.trie_shadow_check,
                        attester,
                        parallel: self.cfg.validator_parallel,
                        serve_feed_addr_file: self
                            .cfg
                            .validator_serve_feed
                            .then_some(feed_addr_file.as_path()),
                        egress,
                        metrics_port: None,
                    },
                )
            })
            .transpose()
    }

    /// `KARDAMOM_E2E_ADMISSION=1` runs the sequencers on the v2 loop, so the
    /// suite covers both loops. A scenario that needs transactions that
    /// cannot pay keeps the v1 loop: the v2 filter refuses them. The carry
    /// window always runs the v2 loop: only the v2 loop offers a
    /// transaction with its bytes.
    fn v2_loop(&self) -> bool {
        !self.cfg.unpayable_spends
            && (self.cfg.carry.is_some()
                || self.cfg.admission
                || std::env::var("KARDAMOM_E2E_ADMISSION").is_ok_and(|v| v == "1"))
    }
}

impl LocalStack {
    /// Wait for every metrics endpoint to answer, the ingress RPC to
    /// serve `eth_chainId`, and the executor to commit its first block.
    /// The chain is LIVE only once that last barrier clears: without it
    /// every scenario pays the sealer/executor cold start out of its own
    /// (much tighter) budget, and under runner contention that cost alone
    /// can blow a scenario's timeout. The bound below is generous on
    /// purpose — a warm host passes in about 1s, and only a genuinely
    /// wedged stack waits it out. Idle chains still seal boundary ticks,
    /// so no traffic is needed.
    async fn await_ready(&self) -> Result<()> {
        for (name, addr) in self.metric_addrs() {
            super::metrics::poll_until(
                &format!("{name} /metrics"),
                Duration::from_secs(90),
                Duration::from_millis(200),
                async || Ok(super::metrics::scrape(addr).await.ok().map(|_| ())),
            )
            .await?;
        }
        let probe = super::l2::L2Client::new(&self.ingress.rpc_url, Duration::from_secs(2))?;
        super::metrics::poll_until(
            "ingress eth_chainId",
            Duration::from_secs(90),
            Duration::from_millis(200),
            async || Ok(probe.chain_id().await.result.ok().map(|_| ())),
        )
        .await?;
        let exec_addr = self.executor.metrics_addr;
        super::metrics::poll_until(
            "the executor's first committed block",
            Duration::from_secs(90),
            Duration::from_millis(250),
            async || {
                // A bring-up poll: the executor's HTTP server may not be
                // listening yet, so a scrape error here means "not ready",
                // the same as a genuinely absent counter. The 90s timeout
                // below still catches a stack that never comes up.
                let v = super::metrics::scrape(exec_addr)
                    .await
                    .ok()
                    .and_then(|s| s.value(crate::scenarios::EXEC_BLOCK_NUMBER))
                    .unwrap_or(0.0);
                Ok((v >= 1.0).then_some(()))
            },
        )
        .await?;
        Ok(())
    }
}

impl LocalStack {
    /// Bring up a stack. Returns `Ok(None)` only when `cfg.l1` is set and
    /// the `anvil` binary is missing. Bridge scenarios then skip, instead
    /// of failing on a machine with no Foundry, matching the convention of
    /// the crate-level anvil tests.
    ///
    /// # Errors
    /// Returns an error when any bring-up step fails: writing the
    /// materialised genesis, launching the driver, sealer, services, or
    /// clearing the readiness barriers.
    pub async fn launch_opt(cfg: StackConfig) -> Result<Option<Self>> {
        let l1 = if cfg.l1 {
            match super::l1::L1::launch(cfg.chain_id.get()).await? {
                Some(l1) => Some(l1),
                None => return Ok(None),
            }
        } else {
            None
        };
        Self::launch_with_l1(cfg, l1).await.map(Some)
    }

    /// # Errors
    /// Returns an error when `cfg.l1` is set (use [`Self::launch_opt`]
    /// instead), or under the same conditions as [`Self::launch_opt`].
    pub async fn launch(cfg: StackConfig) -> Result<Self> {
        anyhow::ensure!(
            !cfg.l1,
            "use LocalStack::launch_opt for L1-backed stacks (it skips cleanly without anvil)"
        );
        Self::launch_with_l1(cfg, None).await
    }

    async fn launch_with_l1(cfg: StackConfig, l1: Option<super::l1::L1>) -> Result<Self> {
        let repo = services::repo_root();
        let genesis = super::proc::ExistingFile::new(
            cfg.genesis.path(&repo),
            "confirm the genesis TOML path",
        )?;
        let keep = std::env::var("KARDAMOM_E2E_KEEP").is_ok_and(|v| v == "1");
        let root = tempfile::Builder::new()
            .prefix("kardamom-e2e-")
            .tempdir()
            .context("create stack temp root")?;
        eprintln!("stack root: {}", root.path().display());

        // The driver and the sealer must be spawned on a thread that lives
        // as long as the stack. Every child gets PR_SET_PDEATHSIG(SIGKILL)
        // (see `Proc::spawn`), and the kernel sends it when the spawning
        // THREAD exits, not the process. A spawn_blocking thread exits after
        // 10s idle, so a stack whose blocking pool went quiet lost its media
        // driver and sealer mid-test, and every client then aborted on the
        // driver keepalive (the s16 and chain-semantics "MediaDriver
        // keepalive" flake). block_in_place runs the launch on this runtime
        // thread, which lives until the runtime shuts down.
        let rootp = root.path().to_path_buf();
        let (driver, sealer) =
            tokio::task::block_in_place(|| -> Result<(MediaDriver, SealerCluster)> {
                let driver = MediaDriver::launch(&rootp)?;
                let sealer = SealerCluster::launch(&rootp, &repo, &cfg, &driver.aeron_dir)?;
                Ok((driver, sealer))
            })?;

        let stack_launch = StackLaunch::new(&cfg, root.path(), &driver, &sealer);
        let executor_query_port = stack_launch.executor_query_port();
        let genesis = stack_launch.materialise_genesis(genesis.path())?;
        let log_config = stack_launch.write_archive_log_config()?;
        let spec = stack_launch.assemble_spec(&genesis, log_config.as_deref());

        let sequencers = stack_launch.spawn_sequencers(&spec)?;
        let egress = cfg.carry.map(|_| spec.spawn_egress(None)).transpose()?;
        let genesis_egress = egress
            .as_ref()
            .filter(|_| cfg.carry_genesis())
            .map(super::carry::SpawnedEgress::url);
        let executor = services::spawn_executor_at(
            &spec,
            &services::ExecutorRun {
                metrics_port: None,
                carry_egress: genesis_egress.as_deref(),
            },
        )?;
        let wirings = stack_launch.build_l1_wiring(l1.as_ref()).await?;
        let validator = stack_launch.spawn_validator(
            &spec,
            wirings.validator.as_ref(),
            genesis_egress.as_deref(),
        )?;
        let l1_follower = wirings
            .da_watcher
            .as_ref()
            .map(|w| services::spawn_l1_follower(&spec, w))
            .transpose()?;
        let da_watcher = l1_follower
            .as_ref()
            .map(|f| services::spawn_da_watcher(&spec, &f.api_url))
            .transpose()?;
        let batcher = cfg
            .carry
            .and(l1.as_ref())
            .map(|l| super::carry::BatcherSide::launch(&spec, l, genesis_egress.as_deref()))
            .transpose()?;
        let ingress = services::spawn_ingress(&spec, &cfg.ingress)?;
        let notifier = cfg
            .notifier
            .then(|| services::spawn_notifier(&spec))
            .transpose()?;

        let load_sampler = LoadSampler::start(root.path().join("host-load.log"));
        let stack = Self {
            notifier,
            ingress,
            extra_ingresses: Vec::new(),
            da_watcher,
            l1_follower,
            verified_l1: wirings.verified_l1,
            batcher,
            egress,
            executor,
            validator,
            sequencers,
            sequencer_twins: Vec::new(),
            sealer,
            driver,
            _load_sampler: load_sampler,
            l1,
            genesis,
            log_config,
            follower_l1: wirings.da_watcher,
            validator_l1: wirings.validator,
            executor_query_port,
            root,
            keep,
            shutdown_report: ShutdownReport::default(),
            cfg,
        };

        if let Err(e) = stack.await_ready().await {
            // The readiness barrier is the one place a wedged bring-up
            // shows. Without the tails, a CI failure here names a timeout
            // and nothing else.
            stack.dump_tails();
            return Err(e);
        }
        Ok(stack)
    }

    fn metric_addrs(&self) -> Vec<(String, std::net::SocketAddr)> {
        let named = [
            ("ingress".to_string(), self.ingress.metrics_addr),
            ("executor".to_string(), self.executor.metrics_addr),
        ];
        named
            .into_iter()
            .chain(
                self.validator
                    .as_ref()
                    .map(|v| ("validator".to_string(), v.metrics_addr)),
            )
            .chain(
                self.da_watcher
                    .as_ref()
                    .map(|w| ("da-watcher".to_string(), w.metrics_addr)),
            )
            .chain(
                self.l1_follower
                    .as_ref()
                    .map(|f| ("l1-follower".to_string(), f.service.metrics_addr)),
            )
            .chain(
                self.notifier
                    .as_ref()
                    .map(|n| ("notifier".to_string(), n.service.metrics_addr)),
            )
            .chain(
                self.egress
                    .as_ref()
                    .map(|e| ("egress".to_string(), e.service.metrics_addr)),
            )
            .chain(
                self.batcher
                    .as_ref()
                    .map(|b| ("batcher".to_string(), b.metrics_addr())),
            )
            .chain(
                self.sequencers
                    .iter()
                    .enumerate()
                    .map(|(i, s)| (format!("sequencer-{i}"), s.metrics_addr)),
            )
            .collect()
    }
}
