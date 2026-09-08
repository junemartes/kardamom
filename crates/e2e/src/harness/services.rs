//! Spawners for the Rust service binaries.
//!
//! Binaries are found in the same cargo target directory the test
//! executable ran from (`target/<profile>/deps/<test>` leads to
//! `target/<profile>/`). So the whole contract is: run `cargo build
//! --bins -p kardamom-{ingress,sequencer,executor,validator,state,
//! da-watcher}` before `cargo test -p e2e`. (`just test-e2e-local` and
//! the chain-semantics CI job build exactly that set.) Services attach
//! to the shared media driver with `--aeron-dir`, and use the built-in
//! single-host IPC channel defaults (no `--log-config`), the documented
//! known-good local topology.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use anyhow::{Context, Result};
use kardamom_da_watcher::interop::CursorReconcile;

use super::proc::{ExistingFile, Proc};
use kardamom_obs::testkit::{free_port, free_udp_port};

/// How long an Aeron client waits for a media driver to update its
/// keepalive before it decides the driver is dead and shuts down.
///
/// Aeron's default is 10 s, which is too short for a test stack. A
/// chain-semantics run is 2 JVMs plus 4 to 6 service processes on a
/// 2-core CI runner. When the machine is oversubscribed, the driver
/// conductor may simply not get scheduled for a while. That is not a
/// dead driver, but at the default, every client decides it is dead at
/// the same time, and the whole stack tears itself down. This shows up
/// as unrelated-looking scenario failures: a validator that "made no
/// progress", or a state database left unsteady so a read-only open
/// hits `MDBX_WANNA_RECOVERY`. The cause is CPU scheduling starvation on
/// the driver JVM, not garbage collection.
///
/// A 30 s timeout rides out those stalls, while still catching a
/// genuinely dead driver well inside any scenario timeout. This does not
/// change deployments, since it is test-harness only, and an outer
/// `AERON_DRIVER_TIMEOUT` still wins, so CI or a developer can tune it
/// with no rebuild.
const DEFAULT_DRIVER_TIMEOUT_MS: &str = "30000";

/// The driver timeout the harness gives its children, in milliseconds.
/// Honors an inherited `AERON_DRIVER_TIMEOUT`, if one is already set.
#[must_use]
pub fn driver_timeout_ms() -> String {
    std::env::var("AERON_DRIVER_TIMEOUT").unwrap_or_else(|_| DEFAULT_DRIVER_TIMEOUT_MS.to_string())
}

/// Environment variables every spawned Rust service shares. This is the
/// one place for them, so the services cannot drift apart.
fn common_service_env(cmd: &mut Command) {
    cmd.env("RUST_LOG", "info");
    // The Aeron C client reads this directly in `aeron_context_init`. This
    // code never calls `set_driver_timeout_ms`, so nothing overrides it.
    cmd.env("AERON_DRIVER_TIMEOUT", driver_timeout_ms());
}

/// `target/<profile>` directory containing the prebuilt service binaries.
fn bin_dir() -> Result<PathBuf> {
    let exe = std::env::current_exe().context("current_exe")?;
    // target/<profile>/deps/<test-bin> leads to target/<profile>
    let dir = exe
        .parent() // deps
        .and_then(|p| p.parent()) // <profile>
        .map(Path::to_path_buf)
        .context("derive target dir from test executable path")?;
    Ok(dir)
}

/// # Errors
/// Returns an error when the binary is not built yet.
pub fn bin(name: &str) -> Result<ExistingFile> {
    ExistingFile::new(
        bin_dir()?.join(name),
        "build the service binaries first: `cargo build --bins -p kardamom-ingress \
         -p kardamom-sequencer -p kardamom-executor -p kardamom-validator -p kardamom-state \
         -p kardamom-da-watcher -p kardamom-reconstruct` (or just `just test-e2e-local`)",
    )
}

/// Repo root (this crate lives at `<repo>/crates/e2e`).
#[must_use]
pub fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .unwrap_or_else(|_| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../.."))
}

/// The `[cluster]` TOML block every Rust cluster client parses.
fn cluster_toml(ingress_endpoints: &str) -> String {
    format!(
        r#"[cluster]
ingress_endpoints = "{ingress_endpoints}"
initial_leader_member_id = 0
ingress_stream_id = 101
egress_stream_id = 102
keep_alive_interval_ms = 1000
"#
    )
}

pub struct ServiceSpec<'a> {
    pub root: &'a Path,
    pub aeron_dir: &'a Path,
    pub cluster_ingress_endpoints: &'a str,
    pub shards: std::num::NonZeroU32,
    pub chain_id: u64,
    pub genesis: &'a Path,
    /// `--log-config` for every service. `None` uses the built-in
    /// single-host IPC defaults, the accepted local path. Set this only
    /// for the archive-durability variant, which needs `[aeron]` archive
    /// settings. The `[channels]` defaults are inherited, so the topology
    /// does not change.
    pub log_config: Option<&'a Path>,
}

/// The parts of a stateful service's (executor, validator) bring-up that
/// vary per spawn: the binary name (also the config file's name and the
/// registry's `kardamom-<bin_name>` lookup), the state directory, the
/// reserved ports, and the `--host-id`.
pub struct StateService<'a> {
    pub bin_name: &'a str,
    pub state_dir: &'a Path,
    pub metrics_port: u16,
    pub egress_port: u16,
    pub host_id: &'a str,
}

impl ServiceSpec<'_> {
    /// Write `<root>/<name>.toml`: `prefix` (a service's own settings,
    /// empty for services that have none) followed by the `[cluster]`
    /// block every Rust cluster client parses at `--config`. Shared by
    /// every spawner that needs a config file.
    fn write_cluster_config(&self, name: &str, prefix: &str) -> Result<PathBuf> {
        let cfg_path = self.root.join(format!("{name}.toml"));
        std::fs::write(
            &cfg_path,
            format!("{prefix}{}", cluster_toml(self.cluster_ingress_endpoints)),
        )?;
        Ok(cfg_path)
    }

    /// Build the `Command` shared by every stateful service (executor,
    /// validator): the config file (`<root>/<svc.bin_name>.toml`, no
    /// prefix), aeron dir, shard/chain wiring, state dir, durability
    /// mode, cluster egress endpoint, and metrics address. The caller
    /// adds its own flags (e.g. `--parallel-validation`) after this
    /// returns.
    ///
    /// # Errors
    /// Returns an error when the binary is not built, or when writing
    /// the config file fails.
    fn state_service_cmd(&self, svc: &StateService<'_>) -> Result<Command> {
        let cfg_path = self.write_cluster_config(svc.bin_name, "")?;
        let mut cmd = Command::new(bin(&format!("kardamom-{}", svc.bin_name))?);
        cmd.arg("--config")
            .arg(&cfg_path)
            .arg("--aeron-dir")
            .arg(self.aeron_dir)
            .args(["--shards", &self.shards.get().to_string()])
            .args(["--chain-id", &self.chain_id.to_string()])
            .arg("--chain")
            .arg(self.genesis)
            .arg("--state-dir")
            .arg(svc.state_dir)
            // Ephemeral test DBs: skip the per-block fsync.
            .args(["--state-durability", "safe-no-sync"])
            .args([
                "--cluster-egress-endpoint",
                &format!("127.0.0.1:{}", svc.egress_port),
            ])
            .args(["--metrics-addr", &format!("127.0.0.1:{}", svc.metrics_port)])
            .args(["--host-id", svc.host_id]);
        Ok(cmd)
    }
}

/// Add `--log-config` when the spec carries one.
fn with_log_config(cmd: &mut Command, spec: &ServiceSpec<'_>) {
    if let Some(p) = spec.log_config {
        cmd.arg("--log-config").arg(p);
    }
}

/// L1 wiring for the bridge scenarios. The da-watcher needs the lockbox
/// to watch, and the validator's attester needs the oracle and a key.
#[derive(Clone)]
pub struct L1Wiring {
    pub rpc_url: String,
    pub lockbox: String,
    pub oracle: String,
    pub attester_key: String,
}

/// Spawn `kardamom-da-watcher` against the anvil L1.
/// `--poll-interval-secs 1` (the production default is 12 s) keeps
/// deposit latency inside a test's patience.
///
/// # Errors
/// Returns an error when the binary is not built or the process fails to
/// spawn.
pub fn spawn_da_watcher(spec: &ServiceSpec<'_>, l1: &L1Wiring) -> Result<Spawned> {
    let metrics_port = free_port().port();
    let mut cmd = Command::new(bin("kardamom-da-watcher")?);
    cmd.args(["--l1-rpc", &l1.rpc_url])
        .args(["--lockbox", &l1.lockbox])
        .args(["--poll-interval-secs", "1"])
        .arg("--aeron-dir")
        .arg(spec.aeron_dir);
    with_log_config(&mut cmd, spec);
    cmd.args(["--metrics-addr", &format!("127.0.0.1:{metrics_port}")])
        .args(["--host-id", "e2e-da-watcher"]);
    common_service_env(&mut cmd);
    SpawnPlan {
        name: "da-watcher".to_string(),
        cmd,
        log: spec.root.join("da-watcher.log"),
        metrics_port,
        state_dir: None,
    }
    .spawn()
}

/// Spawn `kardamom-da-watcher` in INTEROP mode: no L1 flags, one peer pair
/// sourced from `feed_url` (the scenario's `MockInteropFeed`), remote epochs
/// published into the stack's Aeron dir for the sequencers to relay.
///
/// Spawning the real binary rather than driving the watcher in-process is
/// deliberate: it exercises the CLI's cursor-file bootstrap, the Aeron
/// `tx_remote_epochs` publication, and the process-level fail-stop contract
/// (the interop path running ALONE means a pair fault ends the process
/// nonzero — the exact seam the gap scenario asserts on).
///
/// # Errors
/// Returns an error when the binary is not built or the process fails to
/// spawn.
pub fn spawn_interop_watcher(
    spec: &ServiceSpec<'_>,
    origin_chain_id: u64,
    feed_url: &str,
    cursor_file: &Path,
    cursor_reconcile: &CursorReconcile,
) -> Result<Spawned> {
    let metrics_port = free_port().port();
    let mut cmd = Command::new(bin("kardamom-da-watcher")?);
    cmd.args(["--interop-peer-chain-id", &origin_chain_id.to_string()])
        .args(["--interop-feed-url", feed_url])
        .args(["--self-chain-id", &spec.chain_id.to_string()])
        .arg("--interop-cursor-file")
        .arg(cursor_file);
    // The startup cursor reconcile needs a destination JSON-RPC that
    // serves eth_getStorageAt (the validator's --serve-feed endpoint). A
    // stack without one skips the reconcile, which is the test-only path.
    // `CursorReconcile::cli_args` is the same flag-building code the
    // binary itself parses back, so the harness and the binary name one
    // thing one way. Taking `CursorReconcile` here, instead of
    // `Option<&str>`, means the caller builds the pair, and the invalid
    // "a URL that is somehow also skip" state cannot exist at this
    // boundary.
    cmd.args(cursor_reconcile.cli_args());
    cmd
        // 1 s (vs the 2 s default) keeps feed-retry latency inside a test's
        // patience, mirroring the L1 watcher's tightened poll interval.
        .args(["--interop-retry-interval-secs", "1"])
        .arg("--aeron-dir")
        .arg(spec.aeron_dir);
    with_log_config(&mut cmd, spec);
    cmd.args(["--metrics-addr", &format!("127.0.0.1:{metrics_port}")])
        .args(["--host-id", "e2e-interop-watcher"]);
    cmd.env("RUST_LOG", "info");
    SpawnPlan {
        name: "interop-watcher".to_string(),
        cmd,
        log: spec.root.join("interop-watcher.log"),
        metrics_port,
        state_dir: None,
    }
    .spawn()
}

pub struct Spawned {
    pub proc: Proc,
    pub metrics_addr: SocketAddr,
    /// The service's libmdbx state dir (executor/validator only).
    pub state_dir: Option<PathBuf>,
}

/// Everything needed to spawn one service process and wrap it as a
/// [`Spawned`]: the process, its metrics address
/// (`127.0.0.1:<metrics_port>`), and its state dir (if any).
struct SpawnPlan {
    name: String,
    cmd: Command,
    log: PathBuf,
    metrics_port: u16,
    state_dir: Option<PathBuf>,
}

impl SpawnPlan {
    /// # Errors
    /// Returns an error when the process fails to spawn.
    fn spawn(self) -> Result<Spawned> {
        let proc = Proc::spawn(&self.name, self.cmd, self.log)?;
        Ok(Spawned {
            proc,
            metrics_addr: format!("127.0.0.1:{}", self.metrics_port).parse()?,
            state_dir: self.state_dir,
        })
    }
}

/// # Errors
/// Returns an error when the binary is not built, when the config file
/// cannot be written, or when the process fails to spawn.
pub fn spawn_sequencer(spec: &ServiceSpec<'_>, index: u32) -> Result<Spawned> {
    // partition_index and sequencer_id are placeholders here; the CLI
    // flags below (`--partition-index`, `--sequencer-id`) are what the
    // binary actually uses. max_pending_per_sender and
    // backpressure_policy have no CLI flag, so they must live in the file.
    let prefix = format!(
        "partition_count = {}\npartition_index = 0\nsequencer_id = 0\n\
         max_pending_per_sender = 512\nbackpressure_policy = \"return_immediately\"\n\n",
        spec.shards
    );
    let cfg_path = spec.write_cluster_config(&format!("sequencer-{index}"), &prefix)?;
    let metrics_port = free_port().port();
    let egress_port = free_udp_port().port();
    let mut cmd = Command::new(bin("kardamom-sequencer")?);
    cmd.arg("--config")
        .arg(&cfg_path)
        .arg("--aeron-dir")
        .arg(spec.aeron_dir)
        .args(["--partition-index", &index.to_string()])
        .args(["--partition-count", &spec.shards.get().to_string()])
        .args(["--sequencer-id", &index.to_string()])
        .args([
            "--cluster-egress-endpoint",
            &format!("127.0.0.1:{egress_port}"),
        ])
        .args(["--metrics-addr", &format!("127.0.0.1:{metrics_port}")])
        .args(["--host-id", &format!("e2e-seq-{index}")]);
    with_log_config(&mut cmd, spec);
    common_service_env(&mut cmd);
    SpawnPlan {
        name: format!("sequencer-{index}"),
        cmd,
        log: spec.root.join(format!("sequencer-{index}.log")),
        metrics_port,
        state_dir: None,
    }
    .spawn()
}

/// # Errors
/// Returns an error under the same conditions as [`spawn_executor_at`].
pub fn spawn_executor(spec: &ServiceSpec<'_>) -> Result<Spawned> {
    spawn_executor_at(spec, None)
}

/// Join-miss refetch only matters alongside a log config that lists
/// durability-archive endpoints (the archive-durability variant). Without
/// it, a restarted executor cannot get envelopes for canonical records
/// replayed from before the crash, and aborts by design.
fn add_archive_endpoints(cmd: &mut Command, spec: &ServiceSpec<'_>) {
    if spec.log_config.is_some() {
        cmd.args([
            "--replay-destination-endpoint",
            &format!("127.0.0.1:{}", free_udp_port().port()),
        ])
        .args([
            "--archive-control-response-endpoint",
            &format!("127.0.0.1:{}", free_udp_port().port()),
        ]);
    }
}

/// Spawn the executor, or respawn it. `fixed_metrics_port` reuses a
/// previous instance's port, so a restarted executor keeps the address
/// scenarios already hold. The crash-recovery scenario restarts it
/// against the same state directory, which drives the resume-from-cursor
/// path.
///
/// # Errors
/// Returns an error when the binary is not built, when the state
/// directory or config file cannot be created, or when the process fails
/// to spawn.
pub fn spawn_executor_at(
    spec: &ServiceSpec<'_>,
    fixed_metrics_port: Option<u16>,
) -> Result<Spawned> {
    let state_dir = spec.root.join("executor-state");
    std::fs::create_dir_all(&state_dir)?;
    let metrics_port = match fixed_metrics_port {
        Some(p) => p,
        None => free_port().port(),
    };
    let egress_port = free_udp_port().port();
    let mut cmd = spec.state_service_cmd(&StateService {
        bin_name: "executor",
        state_dir: &state_dir,
        metrics_port,
        egress_port,
        host_id: "e2e-exec",
    })?;
    // The footprint shadow is on for every e2e executor. It only
    // measures: execution stays sequential, and the handoff never blocks.
    // Running it here gives the shadow thread real multi-process pipeline
    // coverage in CI. Its per-block summary lines land in executor.log.
    cmd.env("KARDAMOM_FOOTPRINT_SHADOW", "1");
    with_log_config(&mut cmd, spec);
    add_archive_endpoints(&mut cmd, spec);
    common_service_env(&mut cmd);
    // A respawn logs to its own file, so the pre-crash log survives for
    // later inspection (`Proc::spawn` truncates its log file).
    let log = if fixed_metrics_port.is_some() {
        "executor-restarted.log"
    } else {
        "executor.log"
    };
    SpawnPlan {
        name: "executor".to_string(),
        cmd,
        log: spec.root.join(log),
        metrics_port,
        state_dir: Some(state_dir),
    }
    .spawn()
}

/// Validator spawn knobs beyond the shared [`ServiceSpec`].
pub struct ValidatorOptions<'a> {
    /// Trie shadow-check cadence. `Some(1)` checks every block, the
    /// semantics-suite default. The production cluster runs 8.
    pub trie_shadow_check: Option<std::num::NonZeroU64>,
    /// L1 output attestation + epoch verification wiring.
    pub attester: Option<&'a L1Wiring>,
    /// `--parallel-validation` (the deployed cluster's mode).
    pub parallel: bool,
    /// `Some(path)` enables the egress-E1 serving role: `--serve-feed
    /// 127.0.0.1:0` with the bound address written to `path`.
    pub serve_feed_addr_file: Option<&'a Path>,
}

/// The egress-E1 serving role: port 0 (OS-assigned, so concurrent stacks
/// never collide), real address discovered through the addr file.
fn add_serve_feed_args(cmd: &mut Command, opts: &ValidatorOptions<'_>) {
    if let Some(addr_file) = opts.serve_feed_addr_file {
        cmd.args(["--serve-feed", "127.0.0.1:0"])
            .arg("--serve-feed-addr-file")
            .arg(addr_file);
    }
}

/// L1 output attestation needs all three flags together, or none of them
/// (the binary rejects a partial set). `--attester-post-interval 1` posts
/// an output per block, so a withdrawal becomes finalizable promptly.
fn add_attester_args(cmd: &mut Command, opts: &ValidatorOptions<'_>) {
    if let Some(l1) = opts.attester {
        cmd.args(["--l1-rpc-url", &l1.rpc_url])
            .args(["--output-oracle", &l1.oracle])
            .args(["--attester-key", &l1.attester_key])
            .args(["--attester-post-interval", "1"])
            // With the RPC URL, this turns on epoch verification. Every
            // epoch is re-derived from L1, and a mismatch counts as a
            // divergence. So the whole L1-backed suite runs with
            // verification on. An honest producer that quietly stopped
            // matching L1 would then surface as a failure in every bridge
            // scenario, not only the forged-epoch one.
            .args(["--lockbox", &l1.lockbox]);
    }
}

/// Spawn `kardamom-validator` with its own state directory.
///
/// # Errors
/// Returns an error when the binary is not built, when the state
/// directory or config file cannot be created, or when the process fails
/// to spawn.
pub fn spawn_validator(spec: &ServiceSpec<'_>, opts: &ValidatorOptions<'_>) -> Result<Spawned> {
    let state_dir = spec.root.join("validator-state");
    std::fs::create_dir_all(&state_dir)?;
    let metrics_port = free_port().port();
    let egress_port = free_udp_port().port();
    let mut cmd = spec.state_service_cmd(&StateService {
        bin_name: "validator",
        state_dir: &state_dir,
        metrics_port,
        egress_port,
        host_id: "e2e-validator",
    })?;
    with_log_config(&mut cmd, spec);
    if let Some(n) = opts.trie_shadow_check {
        cmd.args(["--trie-shadow-check", &n.to_string()]);
    }
    if opts.parallel {
        cmd.arg("--parallel-validation");
    }
    add_serve_feed_args(&mut cmd, opts);
    add_attester_args(&mut cmd, opts);
    common_service_env(&mut cmd);
    SpawnPlan {
        name: "validator".to_string(),
        cmd,
        log: spec.root.join("validator.log"),
        metrics_port,
        state_dir: Some(state_dir),
    }
    .spawn()
}

/// A pending-receipt park duration that must be nonzero: a zero here
/// would make several scenario timing assertions vacuously true instead
/// of testing anything (for example the `out.elapsed >= park / 2` lower
/// bound in `scenarios::nonce_gap`). Parsed once at construction; every
/// consumer reads a real `Duration` back with [`Self::as_duration`].
#[derive(Debug, Clone, Copy)]
pub struct ParkTimeout(std::num::NonZeroU64);

impl ParkTimeout {
    /// A park timeout of `ms` milliseconds. The `NonZeroU64` argument
    /// carries the nonzero guarantee, so this constructor cannot fail.
    #[must_use]
    pub const fn from_millis(ms: std::num::NonZeroU64) -> Self {
        Self(ms)
    }

    /// A park timeout of `secs` seconds. `NonZeroU64::saturating_mul` is
    /// a `const fn`: it cannot panic, and the result is already
    /// `NonZeroU64`, so this constructor cannot fail either.
    #[must_use]
    pub const fn from_secs(secs: std::num::NonZeroU64) -> Self {
        const MILLIS_PER_SEC: std::num::NonZeroU64 = std::num::NonZeroU64::new(1000).unwrap();
        Self(secs.saturating_mul(MILLIS_PER_SEC))
    }

    #[must_use]
    pub fn as_duration(self) -> Duration {
        Duration::from_millis(self.0.get())
    }
}

pub struct IngressOptions {
    pub pending_receipt_timeout: ParkTimeout,
    pub rpc_max_connections: std::num::NonZeroU32,
}

impl Default for IngressOptions {
    fn default() -> Self {
        const THIRTY_SECS: std::num::NonZeroU64 = std::num::NonZeroU64::new(30).unwrap();
        const DEFAULT_RPC_MAX_CONNECTIONS: std::num::NonZeroU32 =
            std::num::NonZeroU32::new(8192).unwrap();
        Self {
            pending_receipt_timeout: ParkTimeout::from_secs(THIRTY_SECS),
            rpc_max_connections: DEFAULT_RPC_MAX_CONNECTIONS,
        }
    }
}

pub struct SpawnedIngress {
    pub proc: Proc,
    pub metrics_addr: SocketAddr,
    pub rpc_url: String,
}

/// # Errors
/// Returns an error when the binary is not built, when the config file
/// cannot be written, or when the process fails to spawn.
pub fn spawn_ingress(spec: &ServiceSpec<'_>, opts: &IngressOptions) -> Result<SpawnedIngress> {
    let cfg_path = spec.write_cluster_config("ingress", "")?;
    let metrics_port = free_port().port();
    let rpc_port = free_port().port();
    let mut cmd = Command::new(bin("kardamom-ingress")?);
    cmd.arg("--config")
        .arg(&cfg_path)
        .arg("--aeron-dir")
        .arg(spec.aeron_dir)
        .args(["--jsonrpc-bind", &format!("127.0.0.1:{rpc_port}")])
        .args(["--shards", &spec.shards.get().to_string()])
        .args(["--ack-policy", "on-offer"])
        .args(["--chain-id", &spec.chain_id.to_string()])
        .args([
            "--pending-receipt-timeout-ms",
            &opts
                .pending_receipt_timeout
                .as_duration()
                .as_millis()
                .to_string(),
        ])
        .args([
            "--rpc-max-connections",
            &opts.rpc_max_connections.to_string(),
        ])
        .args(["--metrics-addr", &format!("127.0.0.1:{metrics_port}")])
        .args(["--host-id", "e2e-ingress"]);
    with_log_config(&mut cmd, spec);
    // The ingress is the tx_data recorder. With this flag, the shared
    // archive records every shard's publication. This is what makes a
    // crashed consumer's join-miss refetch possible, along with the
    // fsync watermark that drives its resume cursor.
    if spec.log_config.is_some() {
        cmd.arg("--archive-durability");
    }
    common_service_env(&mut cmd);
    let proc = Proc::spawn("ingress", cmd, spec.root.join("ingress.log"))?;
    Ok(SpawnedIngress {
        proc,
        metrics_addr: format!("127.0.0.1:{metrics_port}").parse()?,
        rpc_url: format!("http://127.0.0.1:{rpc_port}"),
    })
}
