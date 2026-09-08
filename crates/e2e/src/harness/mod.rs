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
mod config;
mod control;
pub mod inject;
pub mod l1;
pub mod l1_verified;
pub mod l2;
mod launch;
mod load_sampler;
pub mod metrics;
pub mod proc;
pub mod sealer;
pub mod services;
mod shutdown;

use std::path::PathBuf;
use std::time::Duration;

use anyhow::Result;

pub use config::{DEV_CHAIN_ID, Genesis, StackConfig};
use load_sampler::LoadSampler;
use sealer::SealerCluster;
use services::{ServiceSpec, Spawned, SpawnedIngress};
use shutdown::ShutdownReport;

use crate::scenarios::Target;
use aeron::MediaDriver;

pub struct LocalStack {
    // Drop order matters. Services must die before the sealer and driver
    // they attach to, and the temp root must outlive everything. Fields
    // drop in declaration order.
    ingress: SpawnedIngress,
    da_watcher: Option<Spawned>,
    verified_l1: Option<l1_verified::VerifiedL1>,
    executor: Spawned,
    validator: Option<Spawned>,
    sequencers: Vec<Spawned>,
    sealer: SealerCluster,
    driver: MediaDriver,
    /// Samples `/proc/loadavg` into `<root>/host-load.log` while the stack
    /// is up.
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

impl LocalStack {
    /// The seam that scenarios use. `rpc_timeout` is the client-side
    /// request limit. Keep it above the ingress pending-receipt timeout,
    /// so scenarios see the server's `-32000` error, not a client abort.
    ///
    /// # Errors
    /// Returns an error when the RPC client fails to build.
    pub fn target(&self, rpc_timeout: Duration) -> Result<Target> {
        Ok(Target {
            rpc: l2::L2Client::new(&self.ingress.rpc_url, rpc_timeout)?,
            chain_id: self.cfg.chain_id.get(),
            pending_receipt_timeout: self.cfg.ingress.pending_receipt_timeout.as_duration(),
            ingress_metrics: self.ingress.metrics_addr,
            executor_metrics: self.executor.metrics_addr,
            sequencer_metrics: self.sequencers.iter().map(|s| s.metrics_addr).collect(),
            validator_metrics: self.validator.as_ref().map(|v| v.metrics_addr),
        })
    }

    /// Path of the stack's temp root (log files, state directories,
    /// configs).
    #[must_use]
    pub fn root(&self) -> PathBuf {
        self.root.path().to_path_buf()
    }

    /// The anvil L1 and bridge contracts (`StackConfig::l1`).
    #[must_use]
    pub fn l1(&self) -> Option<&l1::L1> {
        self.l1.as_ref()
    }

    /// The shared media driver's `aeron.dir` (for test-side stream injection).
    #[must_use]
    pub fn aeron_dir(&self) -> PathBuf {
        self.driver.aeron_dir.clone()
    }

    #[must_use]
    pub fn executor_state_dir(&self) -> Option<PathBuf> {
        self.executor.state_dir.clone()
    }

    #[must_use]
    pub fn validator_state_dir(&self) -> Option<PathBuf> {
        self.validator.as_ref().and_then(|v| v.state_dir.clone())
    }

    /// Spawn the interop watcher (`kardamom-da-watcher` in interop mode)
    /// against `feed_url`, publishing remote epochs into this stack's Aeron
    /// dir. `cursor_reconcile` says whether (and where) it reconciles its
    /// startup cursor against this stack's own JSON-RPC (the validator
    /// feed URL) — building the pair at the caller, instead of taking a
    /// raw `Option<&str>` here, means the invalid "a URL that is somehow
    /// also skip" state cannot exist at this boundary. The caller owns the
    /// returned process — the xchain scenario observes its exit (the
    /// pair-scoped fail-stop) directly — and it still dies with the test
    /// via `PR_SET_PDEATHSIG`, so nothing leaks.
    ///
    /// # Errors
    /// Returns an error when the binary is not built or the process fails
    /// to spawn.
    pub fn spawn_interop_watcher(
        &self,
        origin_chain_id: u64,
        feed_url: &str,
        cursor_file: &std::path::Path,
        cursor_reconcile: &kardamom_da_watcher::interop::CursorReconcile,
    ) -> Result<services::Spawned> {
        services::spawn_interop_watcher(
            &self.service_spec(),
            origin_chain_id,
            feed_url,
            cursor_file,
            cursor_reconcile,
        )
    }

    /// Log files of the sealer cluster members, for scenarios that grep
    /// the sealer's stdout signals (`cluster REMOTE-ORIGIN-REJECT …`).
    #[must_use]
    pub fn sealer_logs(&self) -> Vec<PathBuf> {
        self.sealer
            .procs
            .iter()
            .map(|p| p.log_path.clone())
            .collect()
    }

    /// The [`ServiceSpec`] this stack launched its services from, rebuilt
    /// from the launch-time state (same genesis, same `--log-config`).
    fn service_spec(&self) -> ServiceSpec<'_> {
        launch::StackLaunch::new(&self.cfg, self.root.path(), &self.driver, &self.sealer)
            .assemble_spec(&self.genesis, self.log_config.as_deref())
    }

    /// The WS URL of this stack's validator feed
    /// (`StackConfig::validator_serve_feed`), polled from the addr file the
    /// binary writes once its server is bound (port 0 at spawn, so the real
    /// port is only known to the process).
    ///
    /// # Errors
    /// Returns an error when the addr file does not appear within 30s.
    pub async fn validator_feed_url(&self) -> Result<String> {
        let path = self.root.path().join("validator-feed.addr");
        metrics::poll_until(
            "validator feed addr file",
            Duration::from_secs(30),
            Duration::from_millis(200),
            async || {
                Ok(std::fs::read_to_string(&path)
                    .ok()
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty()))
            },
        )
        .await
        .map(|addr| format!("ws://{addr}"))
    }

    /// Tail of the restarted executor's log. The crash-recovery scenario
    /// checks its resume line.
    #[must_use]
    pub fn restarted_executor_log(&self) -> Option<String> {
        std::fs::read_to_string(self.root.path().join("executor-restarted.log")).ok()
    }

    /// The mock verified L1 endpoint, when `StackConfig::verified_l1` is on.
    #[must_use]
    pub fn verified_l1(&self) -> Option<&l1_verified::VerifiedL1> {
        self.verified_l1.as_ref()
    }
}
