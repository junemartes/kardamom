//! Spawners for the Rust service binaries.
//!
//! Binaries are resolved from the same cargo target dir the test executable
//! ran from (`target/<profile>/deps/<test>` → `target/<profile>/`), so
//! `cargo build --bins -p kardamom-{ingress,sequencer,executor}` before
//! `cargo test -p e2e` is the whole contract — the mechanism the deleted
//! multiprocess e2e used. Services attach to the shared media driver via
//! `--aeron-dir` and use the built-in single-host IPC channel defaults (no
//! `--log-config`), the documented known-good local topology.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use anyhow::{Context, Result};

use super::proc::{Proc, free_tcp_port, free_udp_port};

/// `target/<profile>` directory containing the prebuilt service binaries.
pub fn bin_dir() -> Result<PathBuf> {
    let exe = std::env::current_exe().context("current_exe")?;
    // target/<profile>/deps/<test-bin> → target/<profile>
    let dir = exe
        .parent() // deps
        .and_then(|p| p.parent()) // <profile>
        .map(Path::to_path_buf)
        .context("derive target dir from test executable path")?;
    Ok(dir)
}

pub fn bin(name: &str) -> Result<PathBuf> {
    let p = bin_dir()?.join(name);
    anyhow::ensure!(
        p.is_file(),
        "{} not found — build the service binaries first: \
         `cargo build -p kardamom-ingress -p kardamom-sequencer -p kardamom-executor --bins`",
        p.display()
    );
    Ok(p)
}

/// Repo root (this crate lives at `<repo>/crates/e2e`).
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
    pub shards: u32,
    pub chain_id: u64,
    pub genesis: &'a Path,
}

/// L1 wiring for the bridge scenarios: the da-watcher needs the lockbox to
/// watch, and the validator's attester needs the oracle + a key.
pub struct L1Wiring {
    pub rpc_url: String,
    pub lockbox: String,
    pub oracle: String,
    pub attester_key: String,
}

/// Spawn `kardamom-da-watcher` against the anvil L1. `--poll-interval-secs 1`
/// (vs the 12 s production default) keeps deposit latency inside a test's
/// patience.
pub fn spawn_da_watcher(spec: &ServiceSpec<'_>, l1: &L1Wiring) -> Result<Spawned> {
    let metrics_port = free_tcp_port()?;
    let mut cmd = Command::new(bin("kardamom-da-watcher")?);
    cmd.args(["--l1-rpc", &l1.rpc_url])
        .args(["--lockbox", &l1.lockbox])
        .args(["--poll-interval-secs", "1"])
        .arg("--aeron-dir")
        .arg(spec.aeron_dir)
        .args(["--metrics-addr", &format!("127.0.0.1:{metrics_port}")])
        .args(["--host-id", "e2e-da-watcher"]);
    cmd.env("RUST_LOG", "info");
    let proc = Proc::spawn("da-watcher", cmd, spec.root.join("da-watcher.log"))?;
    Ok(Spawned {
        proc,
        metrics_addr: format!("127.0.0.1:{metrics_port}").parse()?,
        state_dir: None,
    })
}

pub struct Spawned {
    pub proc: Proc,
    pub metrics_addr: SocketAddr,
    /// The service's libmdbx state dir (executor/validator only).
    pub state_dir: Option<PathBuf>,
}

pub fn spawn_sequencer(spec: &ServiceSpec<'_>, index: u32) -> Result<Spawned> {
    let cfg_path = spec.root.join(format!("sequencer-{index}.toml"));
    std::fs::write(
        &cfg_path,
        format!(
            "partition_count = {}\npartition_index = 0\nsequencer_id = 0\n\
             max_pending_per_sender = 512\nbackpressure_policy = \"return_immediately\"\n\n{}",
            spec.shards,
            cluster_toml(spec.cluster_ingress_endpoints)
        ),
    )?;
    let metrics_port = free_tcp_port()?;
    let egress_port = free_udp_port()?;
    let mut cmd = Command::new(bin("kardamom-sequencer")?);
    cmd.arg("--config")
        .arg(&cfg_path)
        .arg("--aeron-dir")
        .arg(spec.aeron_dir)
        .args(["--partition-index", &index.to_string()])
        .args(["--partition-count", &spec.shards.to_string()])
        .args(["--sequencer-id", &index.to_string()])
        .args([
            "--cluster-egress-endpoint",
            &format!("127.0.0.1:{egress_port}"),
        ])
        .args(["--metrics-addr", &format!("127.0.0.1:{metrics_port}")])
        .args(["--host-id", &format!("e2e-seq-{index}")]);
    cmd.env("RUST_LOG", "info");
    let proc = Proc::spawn(
        &format!("sequencer-{index}"),
        cmd,
        spec.root.join(format!("sequencer-{index}.log")),
    )?;
    Ok(Spawned {
        proc,
        metrics_addr: format!("127.0.0.1:{metrics_port}").parse()?,
        state_dir: None,
    })
}

pub fn spawn_executor(spec: &ServiceSpec<'_>) -> Result<Spawned> {
    let cfg_path = spec.root.join("executor.toml");
    std::fs::write(&cfg_path, cluster_toml(spec.cluster_ingress_endpoints))?;
    let state_dir = spec.root.join("executor-state");
    std::fs::create_dir_all(&state_dir)?;
    let metrics_port = free_tcp_port()?;
    let egress_port = free_udp_port()?;
    let mut cmd = Command::new(bin("kardamom-executor")?);
    cmd.arg("--config")
        .arg(&cfg_path)
        .arg("--aeron-dir")
        .arg(spec.aeron_dir)
        .args(["--shards", &spec.shards.to_string()])
        .args(["--chain-id", &spec.chain_id.to_string()])
        .arg("--chain")
        .arg(spec.genesis)
        .arg("--state-dir")
        .arg(&state_dir)
        // Ephemeral test DBs: skip the per-block fsync.
        .args(["--state-durability", "safe-no-sync"])
        .args([
            "--cluster-egress-endpoint",
            &format!("127.0.0.1:{egress_port}"),
        ])
        .args(["--metrics-addr", &format!("127.0.0.1:{metrics_port}")])
        .args(["--host-id", "e2e-exec"]);
    cmd.env("RUST_LOG", "info");
    let proc = Proc::spawn("executor", cmd, spec.root.join("executor.log"))?;
    Ok(Spawned {
        proc,
        metrics_addr: format!("127.0.0.1:{metrics_port}").parse()?,
        state_dir: Some(state_dir),
    })
}

/// Spawn `kardamom-validator` with its own state dir and the trie
/// shadow-check at the given cadence (`Some(1)` = every block, the
/// semantics-suite default; the production cluster runs 8).
pub fn spawn_validator(
    spec: &ServiceSpec<'_>,
    trie_shadow_check: Option<u64>,
    attester: Option<&L1Wiring>,
) -> Result<Spawned> {
    let cfg_path = spec.root.join("validator.toml");
    std::fs::write(&cfg_path, cluster_toml(spec.cluster_ingress_endpoints))?;
    let state_dir = spec.root.join("validator-state");
    std::fs::create_dir_all(&state_dir)?;
    let metrics_port = free_tcp_port()?;
    let egress_port = free_udp_port()?;
    let mut cmd = Command::new(bin("kardamom-validator")?);
    cmd.arg("--config")
        .arg(&cfg_path)
        .arg("--aeron-dir")
        .arg(spec.aeron_dir)
        .args(["--shards", &spec.shards.to_string()])
        .args(["--chain-id", &spec.chain_id.to_string()])
        .arg("--chain")
        .arg(spec.genesis)
        .arg("--state-dir")
        .arg(&state_dir)
        .args(["--state-durability", "safe-no-sync"])
        .args([
            "--cluster-egress-endpoint",
            &format!("127.0.0.1:{egress_port}"),
        ])
        .args(["--metrics-addr", &format!("127.0.0.1:{metrics_port}")])
        .args(["--host-id", "e2e-validator"]);
    if let Some(n) = trie_shadow_check {
        cmd.args(["--trie-shadow-check", &n.to_string()]);
    }
    // L1 output attestation: all three flags together or none (the binary
    // rejects a partial set). `--attester-post-interval 1` posts an output
    // per block so a withdrawal becomes finalizable promptly.
    if let Some(l1) = attester {
        cmd.args(["--l1-rpc-url", &l1.rpc_url])
            .args(["--output-oracle", &l1.oracle])
            .args(["--attester-key", &l1.attester_key])
            .args(["--attester-post-interval", "1"]);
    }
    cmd.env("RUST_LOG", "info");
    let proc = Proc::spawn("validator", cmd, spec.root.join("validator.log"))?;
    Ok(Spawned {
        proc,
        metrics_addr: format!("127.0.0.1:{metrics_port}").parse()?,
        state_dir: Some(state_dir),
    })
}

pub struct IngressOptions {
    pub pending_receipt_timeout: Duration,
    pub rpc_max_connections: u32,
}

impl Default for IngressOptions {
    fn default() -> Self {
        Self {
            pending_receipt_timeout: Duration::from_secs(30),
            rpc_max_connections: 8192,
        }
    }
}

pub struct SpawnedIngress {
    pub proc: Proc,
    pub metrics_addr: SocketAddr,
    pub rpc_url: String,
}

pub fn spawn_ingress(spec: &ServiceSpec<'_>, opts: &IngressOptions) -> Result<SpawnedIngress> {
    let cfg_path = spec.root.join("ingress.toml");
    std::fs::write(&cfg_path, cluster_toml(spec.cluster_ingress_endpoints))?;
    let metrics_port = free_tcp_port()?;
    let rpc_port = free_tcp_port()?;
    let mut cmd = Command::new(bin("kardamom-ingress")?);
    cmd.arg("--config")
        .arg(&cfg_path)
        .arg("--aeron-dir")
        .arg(spec.aeron_dir)
        .args(["--jsonrpc-bind", &format!("127.0.0.1:{rpc_port}")])
        .args(["--shards", &spec.shards.to_string()])
        .args(["--ack-policy", "on-offer"])
        .args(["--chain-id", &spec.chain_id.to_string()])
        .args([
            "--pending-receipt-timeout-ms",
            &opts.pending_receipt_timeout.as_millis().to_string(),
        ])
        .args([
            "--rpc-max-connections",
            &opts.rpc_max_connections.to_string(),
        ])
        .args(["--metrics-addr", &format!("127.0.0.1:{metrics_port}")])
        .args(["--host-id", "e2e-ingress"]);
    cmd.env("RUST_LOG", "info");
    let proc = Proc::spawn("ingress", cmd, spec.root.join("ingress.log"))?;
    Ok(SpawnedIngress {
        proc,
        metrics_addr: format!("127.0.0.1:{metrics_port}").parse()?,
        rpc_url: format!("http://127.0.0.1:{rpc_port}"),
    })
}
