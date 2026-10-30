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

pub use super::consumers::{
    ExecutorRun, ValidatorOptions, spawn_executor, spawn_executor_at, spawn_validator,
};
pub use super::ingress::{IngressOptions, IngressSlot, ParkTimeout, SpawnedIngress};
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
pub(super) fn common_service_env(cmd: &mut Command) {
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
         -p kardamom-da-watcher -p kardamom-l1-indexer -p kardamom-reconstruct -p kardamom-notifier -p kardamom-canary \
         -p kardamom-batcher -p kardamom-egress` (or just `just test-e2e-local`)",
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

#[allow(
    clippy::struct_excessive_bools,
    reason = "each bool is an independent setting that a stack passes to its services"
)]
pub struct ServiceSpec<'a> {
    pub root: &'a Path,
    pub aeron_dir: &'a Path,
    pub cluster_ingress_endpoints: &'a str,
    /// The active shard count (M). The sequencers and the ingress take it.
    /// The consumers open the fixed lane plane and take no count.
    pub shards: std::num::NonZeroU32,
    /// `--priority-fees` on every sequencer. The genesis the spec names
    /// carries the matching fee schedule, and the sealer the matching
    /// window: one value, as in the deploy.
    pub priority_fees: bool,
    /// The transaction lifetime. The sequencers take it as `--tx-ttl-ms`.
    /// The ingress takes the same value as `--pending-receipt-timeout-ms`
    /// (see [`IngressOptions`]). One value drives both, as in the deploy.
    pub tx_ttl: Duration,
    /// The executor's nonce query address. The executor serves it, and
    /// every sequencer queries it. The stack picks the port before the
    /// sequencers spawn, so a restarted executor keeps it.
    pub executor_query: SocketAddr,
    pub chain_id: u64,
    pub genesis: &'a Path,
    /// `--log-config` for every service. `None` uses the built-in
    /// single-host IPC defaults, the accepted local path. Set this only
    /// for the archive-durability variant, which needs `[aeron]` archive
    /// settings. The `[channels]` defaults are inherited, so the topology
    /// does not change.
    pub log_config: Option<&'a Path>,
    /// Run every sequencer on the v2 loop (the `[admission]` section).
    pub admission: bool,
    /// With `admission`: the sequencers send the verdict of each
    /// transaction to its ingress, and `eth_sendRawTransaction` answers on
    /// it, at admission. The ingress reads its `tx_verdicts` stream and no
    /// receipts.
    pub verdicts: bool,
    /// `--exec-frames` on the executor: it forwards its frames to the
    /// egress.
    pub exec_frames: bool,
    /// The executor writes checkpoints to `<root>/checkpoints` every two
    /// seconds, for the state mirror's rebuild.
    pub checkpoints: bool,
}

impl ServiceSpec<'_> {
    /// Write `<root>/<name>.toml`: `prefix` (a service's own settings,
    /// empty for services that have none) followed by the `[cluster]`
    /// block every Rust cluster client parses at `--config`. Shared by
    /// every spawner that needs a config file.
    pub(super) fn write_cluster_config(&self, name: &str, prefix: &str) -> Result<PathBuf> {
        let cfg_path = self.root.join(format!("{name}.toml"));
        std::fs::write(
            &cfg_path,
            format!("{prefix}{}", cluster_toml(self.cluster_ingress_endpoints)),
        )?;
        Ok(cfg_path)
    }
}

/// Add `--log-config` when the spec carries one.
pub(super) fn with_log_config(cmd: &mut Command, spec: &ServiceSpec<'_>) {
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

/// A running L1 follower: the process, and the URL of its API.
pub struct SpawnedFollower {
    pub service: Spawned,
    pub api_url: String,
}

/// Spawn `kardamom-l1-indexer`, the L1 follower, against the anvil L1. It
/// publishes `l1_blocks` into the stack's Aeron dir and serves its archive
/// on its API, the da-watcher's history. No batch is posted in a test
/// stack, so the DA proxy is never asked; the settlement is the zero
/// address. `--poll-interval-secs 1` keeps deposit latency inside a
/// test's patience: anvil has no beacon API, so the follower reads every
/// slot.
///
/// # Errors
/// Returns an error when the binary is not built or the process fails to
/// spawn.
pub fn spawn_l1_follower(spec: &ServiceSpec<'_>, l1: &L1Wiring) -> Result<SpawnedFollower> {
    let metrics_port = free_port().port();
    let api_port = free_port().port();
    let mut cmd = Command::new(bin("kardamom-l1-indexer")?);
    cmd.args(["--l1-rpc", &l1.rpc_url])
        .args(["--lockbox", &l1.lockbox])
        .args(["--settlement", "0x0000000000000000000000000000000000000000"])
        .args(["--da-proxy", "http://127.0.0.1:9"])
        .args(["--poll-interval-secs", "1"])
        .arg("--data-dir")
        .arg(spec.root.join("l1-follower"))
        .args(["--listen", &format!("127.0.0.1:{api_port}")])
        .arg("--aeron-dir")
        .arg(spec.aeron_dir);
    with_log_config(&mut cmd, spec);
    cmd.args(["--metrics-addr", &format!("127.0.0.1:{metrics_port}")])
        .args(["--host-id", "e2e-l1-follower"]);
    common_service_env(&mut cmd);
    let service = SpawnPlan {
        name: "l1-follower".to_string(),
        cmd,
        log: spec.root.join("l1-follower.log"),
        metrics_port,
        state_dir: None,
    }
    .spawn()?;
    Ok(SpawnedFollower {
        service,
        api_url: format!("http://127.0.0.1:{api_port}"),
    })
}

/// Spawn `kardamom-da-watcher` on the follower's `l1_blocks` stream, with
/// the follower's API (`indexer_url`) as its history.
/// `--poll-interval-secs 1` (the production default is 12 s) is its
/// housekeeping tick. The watcher follows the sealer's boundaries through
/// the stack's cluster, as in the deploy.
///
/// # Errors
/// Returns an error when the binary is not built, when writing the config
/// file fails, or when the process fails to spawn.
pub fn spawn_da_watcher(spec: &ServiceSpec<'_>, indexer_url: &str) -> Result<Spawned> {
    let metrics_port = free_port().port();
    let egress_port = free_udp_port().port();
    let cfg_path = spec.write_cluster_config("da-watcher", "")?;
    let mut cmd = Command::new(bin("kardamom-da-watcher")?);
    // The tests mine bursts of L1 blocks. At the 2 s default pace, the
    // deposits of a burst wait too long, so the harness uses 100 ms.
    cmd.arg("--l1-blocks")
        .args(["--indexer-url", indexer_url])
        .args(["--poll-interval-secs", "1"])
        .args(["--epoch-pace-ms", "100"])
        .arg("--config")
        .arg(&cfg_path)
        .args([
            "--cluster-egress-endpoint",
            &format!("127.0.0.1:{egress_port}"),
        ])
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

/// A running notifier: the process, and the WebSocket URL of its feed.
pub struct SpawnedNotifier {
    pub service: Spawned,
    pub ws_url: String,
}

/// Spawn `kardamom-notifier`: the status feed over the stack's Aeron
/// dir.
///
/// # Errors
/// Returns an error when the binary is not built or the process fails to
/// spawn.
pub fn spawn_notifier(spec: &ServiceSpec<'_>) -> Result<SpawnedNotifier> {
    let metrics_port = free_port().port();
    let feed_port = free_port().port();
    let mut cmd = Command::new(bin("kardamom-notifier")?);
    cmd.arg("--aeron-dir")
        .arg(spec.aeron_dir)
        .args(["--bind", &format!("127.0.0.1:{feed_port}")]);
    with_log_config(&mut cmd, spec);
    cmd.args(["--metrics-addr", &format!("127.0.0.1:{metrics_port}")])
        .args(["--host-id", "e2e-notifier"]);
    common_service_env(&mut cmd);
    let service = SpawnPlan {
        name: "notifier".to_string(),
        cmd,
        log: spec.root.join("notifier.log"),
        metrics_port,
        state_dir: None,
    }
    .spawn()?;
    Ok(SpawnedNotifier {
        service,
        ws_url: format!("ws://127.0.0.1:{feed_port}"),
    })
}

/// What a spawned canary probes, and its ring.
pub struct CanarySpec<'a> {
    /// The directory of the canary's data and log.
    pub root: &'a Path,
    pub ingress_url: &'a str,
    pub notifier_ws: &'a str,
    /// The first dev-mnemonic account of the ring, and the ring size.
    pub ring_offset: u32,
    pub ring_size: u32,
    /// The `deposit` probe's L1: the endpoint, the key, and the lockbox.
    pub l1: Option<CanaryL1>,
}

/// The L1 side of a spawned canary.
pub struct CanaryL1 {
    pub rpc_url: String,
    pub key: String,
    pub lockbox: alloy_primitives::Address,
}

/// Spawn `kardamom-canary` against one ingress and the notifier, on the
/// dev mnemonic, with short cadences so a scenario sees every probe run
/// within seconds.
///
/// # Errors
/// Returns an error when the binary is not built or the process fails to
/// spawn.
pub fn spawn_canary(spec: &CanarySpec<'_>) -> Result<Spawned> {
    let metrics_port = free_port().port();
    let mut cmd = Command::new(bin("kardamom-canary")?);
    cmd.args(["--ingress", &format!("e2e-ingress={}", spec.ingress_url)])
        .args(["--notifier-ws", spec.notifier_ws])
        .args(["--ring-offset", &spec.ring_offset.to_string()])
        .args(["--ring-size", &spec.ring_size.to_string()])
        .arg("--dir")
        .arg(spec.root.join("canary"))
        .args([
            "--transfer-ms",
            "1000",
            "--read-ms",
            "3000",
            "--contract-ms",
            "2000",
        ])
        .args([
            "--balance-ms",
            "2000",
            "--fees-ms",
            "3000",
            "--rwa-ms",
            "3000",
        ])
        .args(["--swap-ms", "2000", "--liquidity-ms", "4000"])
        .args(["--deposit-ms", "600000"])
        // The local stack runs no batcher, so the safe head stays put.
        .args(["--safe-sample", "1000000000"])
        .args(["--metrics-addr", &format!("127.0.0.1:{metrics_port}")])
        .args(["--host-id", "e2e-canary"])
        .env("KARDAMOM_CANARY_MNEMONIC", super::l2::DEV_MNEMONIC);
    if let Some(l1) = &spec.l1 {
        cmd.env("KARDAMOM_L1_RPC", &l1.rpc_url)
            .env("KARDAMOM_CANARY_L1_KEY", &l1.key)
            .args(["--lockbox", &l1.lockbox.to_string()])
            .args(["--l1-timeout-ms", "120000", "--credit-timeout-ms", "120000"]);
    }
    common_service_env(&mut cmd);
    SpawnPlan {
        name: "canary".to_string(),
        cmd,
        log: spec.root.join("canary.log"),
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
pub(super) struct SpawnPlan {
    pub(super) name: String,
    pub(super) cmd: Command,
    pub(super) log: PathBuf,
    pub(super) metrics_port: u16,
    pub(super) state_dir: Option<PathBuf>,
}

impl SpawnPlan {
    /// # Errors
    /// Returns an error when the process fails to spawn.
    pub(super) fn spawn(self) -> Result<Spawned> {
        let proc = Proc::spawn(&self.name, self.cmd, self.log)?;
        Ok(Spawned {
            proc,
            metrics_addr: format!("127.0.0.1:{}", self.metrics_port).parse()?,
            state_dir: self.state_dir,
        })
    }
}

/// The lane-plane knobs of one sequencer process. The default is the
/// identity map: lane `index`, the slots of partition `index` under
/// `partition_count = shards`. A resize sets the rest. The text forms
/// go to the binary's flags as they are; the binary parses them. See
/// `docs/specs/dynamic-sequencer-sizing.md`, section 3.5.
#[derive(Clone, Debug, Default)]
pub struct SequencerOptions {
    /// `--partition-index`, for a second replica of a lane. Defaults to the
    /// process index.
    pub partition: Option<u32>,
    /// `--partition-count`. Defaults to the stack's shard count.
    pub partition_count: Option<std::num::NonZeroU32>,
    /// `--lane`.
    pub lane: Option<u8>,
    /// `--vslots`, in the text form `0-7,16`.
    pub vslots: Option<String>,
    /// `--extra-lanes`.
    pub extra_lanes: Vec<u8>,
    /// `--shadow-vslots`.
    pub shadow_vslots: Option<String>,
    /// `--shadow-warm-ms`.
    pub shadow_warm: Option<Duration>,
    /// A suffix for the log file name, so a restarted process does not
    /// overwrite the log of the one it replaced.
    pub log_tag: String,
    /// `--sealer-version-floor`: the decision version that the process
    /// keeps until the sealer reports its own. 0 is the default.
    pub sealer_version_floor: u8,
    /// Run the v1 loop, also on a stack whose sequencers run the v2 loop.
    /// The v1 loop offers every transaction as a reference without its
    /// bytes (record type 0), as a sequencer of an older release does.
    pub v1_loop: bool,
    /// The `[cache]` URL of the Redis that the nonce lookup reads first.
    /// `None` runs the lookup on the executor alone.
    pub cache_url: Option<String>,
}

impl SequencerOptions {
    /// Add the lane-plane flags this option set names to `cmd`.
    fn add_lane_args(&self, cmd: &mut Command) {
        if let Some(lane) = self.lane {
            cmd.args(["--lane", &lane.to_string()]);
        }
        if let Some(v) = self.vslots.as_deref() {
            cmd.args(["--vslots", v]);
        }
        if !self.extra_lanes.is_empty() {
            let lanes: Vec<String> = self.extra_lanes.iter().map(u8::to_string).collect();
            cmd.args(["--extra-lanes", &lanes.join(",")]);
        }
        if let Some(v) = self.shadow_vslots.as_deref() {
            cmd.args(["--shadow-vslots", v]);
        }
        if let Some(warm) = self.shadow_warm {
            cmd.args(["--shadow-warm-ms", &warm.as_millis().to_string()]);
        }
        if self.sealer_version_floor > 0 {
            cmd.args([
                "--sealer-version-floor",
                &self.sealer_version_floor.to_string(),
            ]);
        }
    }
}

/// # Errors
/// Returns an error under the same conditions as [`spawn_sequencer_with`].
pub fn spawn_sequencer(spec: &ServiceSpec<'_>, index: u32) -> Result<Spawned> {
    spawn_sequencer_with(spec, index, &SequencerOptions::default())
}

/// # Errors
/// Returns an error when the binary is not built, when the config file
/// cannot be written, or when the process fails to spawn.
pub fn spawn_sequencer_with(
    spec: &ServiceSpec<'_>,
    index: u32,
    opts: &SequencerOptions,
) -> Result<Spawned> {
    let partition_count = opts.partition_count.unwrap_or(spec.shards);
    // partition_index and sequencer_id are placeholders here; the CLI
    // flags below (`--partition-index`, `--sequencer-id`) are what the
    // binary actually uses. max_pending_per_sender and
    // backpressure_policy have no CLI flag, so they must live in the file.
    let mut prefix = format!(
        "partition_count = {partition_count}\npartition_index = 0\nsequencer_id = 0\n\
         max_pending_per_sender = 512\nbackpressure_policy = \"return_immediately\"\n\n",
    );
    if spec.admission && !opts.v1_loop {
        prefix.push_str("[admission]\nenabled = true\nmax_parked = 512\n");
        if spec.verdicts {
            prefix.push_str("verdicts = true\n");
        }
        prefix.push('\n');
    }
    if let Some(url) = opts.cache_url.as_deref() {
        prefix.push_str("[cache]\nurl = \"");
        prefix.push_str(url);
        prefix.push_str("\"\n\n");
    }
    let name = format!("sequencer-{index}{}", opts.log_tag);
    let cfg_path = spec.write_cluster_config(&name, &prefix)?;
    let metrics_port = free_port().port();
    let egress_port = free_udp_port().port();
    let mut cmd = Command::new(bin("kardamom-sequencer")?);
    cmd.arg("--config")
        .arg(&cfg_path)
        .arg("--aeron-dir")
        .arg(spec.aeron_dir)
        .args([
            "--partition-index",
            &opts.partition.unwrap_or(index).to_string(),
        ])
        .args(["--partition-count", &partition_count.to_string()])
        .args(["--sequencer-id", &index.to_string()]);
    opts.add_lane_args(&mut cmd);
    cmd.args(["--tx-ttl-ms", &spec.tx_ttl.as_millis().to_string()])
        .args([
            "--executor-query-endpoints",
            &format!("http://{}", spec.executor_query),
        ])
        .args([
            "--cluster-egress-endpoint",
            &format!("127.0.0.1:{egress_port}"),
        ])
        .args(["--metrics-addr", &format!("127.0.0.1:{metrics_port}")])
        .args(["--host-id", &format!("e2e-seq-{index}")])
        .args(["--priority-fees", &spec.priority_fees.to_string()]);
    with_log_config(&mut cmd, spec);
    common_service_env(&mut cmd);
    SpawnPlan {
        name: format!("sequencer-{index}"),
        cmd,
        log: spec.root.join(format!("{name}.log")),
        metrics_port,
        state_dir: None,
    }
    .spawn()
}

/// # Errors
/// Returns an error when the binary is not built, when the config file
/// cannot be written, or when the process fails to spawn.
pub fn spawn_ingress(spec: &ServiceSpec<'_>, opts: &IngressOptions) -> Result<SpawnedIngress> {
    spawn_ingress_at(spec, opts, &IngressSlot::first(), None)
}

/// Spawn an ingress replica, or respawn it. The ports of `slot` keep the
/// URLs that scenarios already hold. `shard_map` is the `--shard-map`
/// file of a resize; `None` is the identity map over the shard count.
///
/// # Errors
/// Returns an error when the binary is not built, when the config file
/// cannot be written, or when the process fails to spawn.
pub fn spawn_ingress_at(
    spec: &ServiceSpec<'_>,
    opts: &IngressOptions,
    slot: &IngressSlot,
    shard_map: Option<&Path>,
) -> Result<SpawnedIngress> {
    let cfg_path = spec.write_cluster_config("ingress", "")?;
    let metrics_port = slot.metrics_port.unwrap_or_else(|| free_port().port());
    let rpc_port = slot.rpc_port.unwrap_or_else(|| free_port().port());
    // The cluster egress port of the `Sealed` status tap.
    let egress_port = free_udp_port().port();
    let mut cmd = Command::new(bin("kardamom-ingress")?);
    cmd.arg("--config")
        .arg(&cfg_path)
        .arg("--aeron-dir")
        .arg(spec.aeron_dir)
        .args([
            "--cluster-egress-endpoint",
            &format!("127.0.0.1:{egress_port}"),
        ])
        .args(["--jsonrpc-bind", &format!("127.0.0.1:{rpc_port}")])
        .args(["--ingress-id", &slot.id.to_string()])
        .args(["--shards", &spec.shards.get().to_string()]);
    if let Some(map) = shard_map {
        cmd.arg("--shard-map").arg(map);
    }
    cmd.args(["--ack-policy", "on-offer"])
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
        .args(["--host-id", &format!("e2e-ingress-{}", slot.id)])
        // The account RPCs ask the executor on a local miss, as the
        // sequencers' nonce lookup does.
        .args([
            "--executor-query-endpoints",
            &format!("http://{}", spec.executor_query),
        ]);
    // The v2 sequencers check each sender, and the ingress reads no
    // receipts: its local account layer gets no rows.
    if spec.verdicts {
        cmd.args(["--verdicts", "true", "--admission-checks", "false"]);
    }
    with_log_config(&mut cmd, spec);
    // The ingress is the tx_data recorder. With this flag, the shared
    // archive records every shard's publication. This is what makes a
    // crashed consumer's join-miss refetch possible, along with the
    // fsync watermark that drives its resume cursor.
    if spec.log_config.is_some() {
        cmd.arg("--archive-durability");
    }
    common_service_env(&mut cmd);
    let proc = Proc::spawn(
        &slot.log_name,
        cmd,
        spec.root.join(format!("{}.log", slot.log_name)),
    )?;
    Ok(SpawnedIngress {
        proc,
        metrics_addr: format!("127.0.0.1:{metrics_port}").parse()?,
        rpc_url: format!("http://127.0.0.1:{rpc_port}"),
    })
}
